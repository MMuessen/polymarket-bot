"""
OutcomeResolverJob: periodically checks for expired markets and settles
any open shadow positions that resolved YES or NO.

Runs as a low-priority background task. Does not interfere with the
main trading loop.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from ..market.snapshot import SnapshotStore
from ..persistence.repos.trades import ShadowTradeRepository
from ..persistence.repos.positions import ShadowPositionRepository
from ..observability.health import HealthRegistry

logger = logging.getLogger(__name__)


class OutcomeResolverJob:
    def __init__(
        self,
        *,
        store: SnapshotStore,
        trades_repo: ShadowTradeRepository,
        positions_repo: ShadowPositionRepository,
        health: HealthRegistry,
        interval_seconds: int = 60,
    ) -> None:
        self._store = store
        self._trades = trades_repo
        self._positions = positions_repo
        self._health_sub = health.register("outcome_resolver")
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._run(), name="outcome-resolver")

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
            try:
                await self._resolve_pass()
                self._health_sub.mark_ok()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._health_sub.mark_degraded(repr(exc))
                logger.error("OutcomeResolver error: %s", exc)

            try:
                await asyncio.wait_for(
                    self._stop_evt.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                pass

    async def _resolve_pass(self) -> None:
        """
        Find open positions whose market has expired and force-close them.
        In a real deployment this would also check settlement prices.
        For paper trading, we close at the last known bid when market expires.
        """
        now = datetime.now(timezone.utc)
        positions = await self._positions.list_all()
        snapshots = self._store.snapshot()

        for pos in positions:
            ticker = pos["ticker"]
            variant = pos["variant_name"]
            side = str(pos.get("side") or "yes").lower()
            snap = snapshots.get(ticker)

            # If market no longer in store (expired/delisted), force close at 0
            if snap is None:
                await self._force_close(pos, exit_price=0.0, reason="market_delisted")
                continue

            # If market expired
            hours = snap.hours_until_expiry(now)
            if hours is not None and hours <= 0:
                # Close at current bid (likely 0 or 1 for settled markets)
                mark = snap.side_bid(side) or 0.0
                await self._force_close(pos, exit_price=float(mark), reason="market_expired")

    async def _force_close(
        self,
        pos: dict,
        exit_price: float,
        reason: str,
    ) -> None:
        ticker = pos["ticker"]
        variant = pos["variant_name"]
        entry = float(pos.get("entry_price") or 0.0)
        qty = int(pos.get("qty") or 0)
        realized = (exit_price - entry) * qty

        closed = await self._trades.close_trade(
            ticker=ticker,
            variant_name=variant,
            exit_price=exit_price,
            realized_pnl=realized,
            close_reason=reason,
        )
        if closed:
            await self._positions.close_position(ticker, variant)
            logger.info(
                "OutcomeResolver closed %s %s reason=%s pnl=%.4f",
                variant, ticker, reason, realized,
            )
