"""
TradingLoop: the main background task that drives the bot.

One asyncio task. Runs every poll_seconds. Owns nothing — depends on
injected services. Writes to StatusCache after each cycle so routes
stay fast.
"""
from __future__ import annotations

import asyncio
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..market.coinbase_ws import CoinbaseSpotFeed
from ..market.kalshi_stream import KalshiMarketStream
from ..market.snapshot import SnapshotStore
from ..paper.engine import PaperEngine
from ..paper.accounting import PaperAccounting
from ..observability.health import HealthRegistry, HealthStatus

logger = logging.getLogger(__name__)


class StatusCache:
    """
    Pre-computed status blob written by the loop, read by /api/status.
    Routes never compute this themselves.
    """

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {"status": "starting"}
        self._updated_at: Optional[datetime] = None

    def update(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._updated_at = datetime.now(timezone.utc)

    def get(self) -> Dict[str, Any]:
        return {
            **self._data,
            "cache_updated_at": self._updated_at.isoformat() if self._updated_at else None,
        }

    @property
    def age_seconds(self) -> float:
        if self._updated_at is None:
            return float("inf")
        return (datetime.now(timezone.utc) - self._updated_at).total_seconds()


class TradingLoop:
    def __init__(
        self,
        *,
        store: SnapshotStore,
        coinbase: CoinbaseSpotFeed,
        kalshi_stream: KalshiMarketStream,
        paper_engine: PaperEngine,
        accounting: PaperAccounting,
        status_cache: StatusCache,
        health: HealthRegistry,
        poll_seconds: int = 15,
    ) -> None:
        self._store = store
        self._coinbase = coinbase
        self._kalshi = kalshi_stream
        self._paper = paper_engine
        self._accounting = accounting
        self._status_cache = status_cache
        self._health = health
        self._poll_seconds = poll_seconds
        self._loop_health = health.register("trading_loop")
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._cycle_count = 0

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._run(), name="trading-loop")
        logger.info("TradingLoop started (poll=%ds)", self._poll_seconds)

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _run(self) -> None:
        while not self._stop_evt.is_set():
            start = datetime.now(timezone.utc)
            try:
                await self._tick()
                elapsed = (datetime.now(timezone.utc) - start).total_seconds()
                self._loop_health.mark_ok({"last_elapsed_s": round(elapsed, 2)})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                tb = traceback.format_exc()
                self._loop_health.mark_degraded(repr(exc), {"traceback": tb[-2000:]})
                logger.error("TradingLoop error: %s", exc)

            # Sleep until next tick
            try:
                await asyncio.wait_for(
                    self._stop_evt.wait(), timeout=self._poll_seconds
                )
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        self._cycle_count += 1
        spot_prices = self._coinbase.all_prices()

        # Run paper engine cycle
        cycle_stats = await self._paper.run_cycle(spot_prices)

        # Recompute accounting
        snapshots = self._store.snapshot()
        top_edge: Optional[float] = None
        enriched_edges = [
            snap.best_exec_edge
            for snap in snapshots.values()
            if snap.best_exec_edge is not None and snap.best_exec_edge > 0
        ]
        if enriched_edges:
            top_edge = max(enriched_edges)

        session = await self._accounting.compute(top_edge=top_edge)

        # Build status cache
        markets_list = [
            {
                "ticker": snap.ticker,
                "title": snap.title,
                "status": snap.status,
                "market_family": snap.market_family,
                "spot_symbol": snap.spot_symbol,
                "fair_yes": snap.fair_yes,
                "yes_ask": snap.yes_ask,
                "no_ask": snap.no_ask,
                "yes_bid": snap.yes_bid,
                "no_bid": snap.no_bid,
                "mid": snap.mid,
                "spread": snap.spread,
                "best_side": snap.best_side,
                "best_exec_edge": snap.best_exec_edge,
                "execution_edge_yes": snap.execution_edge_yes,
                "execution_edge_no": snap.execution_edge_no,
                "hours_to_expiry": snap.hours_to_expiry,
                "distance_from_spot_pct": snap.distance_from_spot_pct,
                "distance_bucket": snap.distance_bucket,
                "floor_strike": snap.floor_strike,
                "cap_strike": snap.cap_strike,
                "rationale": snap.rationale,
            }
            for snap in sorted(
                snapshots.values(),
                key=lambda s: (s.best_exec_edge or -999),
                reverse=True,
            )
        ]

        self._status_cache.update({
            "mode": "paper",
            "live_armed": False,
            "cycle": self._cycle_count,
            "spot_prices": spot_prices,
            "market_count": len(snapshots),
            "markets": markets_list[:50],  # cap to 50 for payload size
            "session": session.to_dict(),
            "top_edge": round(top_edge, 4) if top_edge is not None else None,
            "maker_queue": self._paper.maker_stats(),
            "paper_cycle": cycle_stats,
            "health": self._health.to_dict(),
            "stream": {
                "coinbase": self._coinbase.health_dict(),
                "kalshi": self._kalshi.health_dict(),
            },
            "strategies": {
                "crypto_lag": {
                    "enabled": self._paper.enabled,
                    "live_enabled": False,
                }
            },
        })
