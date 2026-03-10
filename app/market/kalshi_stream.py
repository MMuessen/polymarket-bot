"""
KalshiMarketStream: clean adapter over KalshiMarketStreamV40.

Converts the legacy SnapshotRow dict objects into MarketSnapshot dataclasses
and pushes them into SnapshotStore. Decouples the rest of the new system
from the legacy stream implementation completely.

The legacy stream handles all the WebSocket mechanics (auth, reconnect,
merge, orderbook updates). We wrap it and translate its output.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from ..market.snapshot import MarketSnapshot, SnapshotStore

logger = logging.getLogger(__name__)


def _parse_dt(v: Any) -> Optional[datetime]:
    """Parse an ISO string or datetime into a tz-aware datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _f(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _row_to_snapshot(row: Dict[str, Any]) -> Optional[MarketSnapshot]:
    """
    Convert a SnapshotRow (dict-like) from kalshi_stream_v40 into a
    clean MarketSnapshot dataclass.

    Key fix: raw_close_time is always stored as a datetime here,
    never as a string. This prevents the string-minus-datetime error
    that plagued the legacy system.
    """
    ticker = str(row.get("ticker") or "").strip()
    if not ticker:
        return None

    raw_close_time = _parse_dt(
        row.get("raw_close_time")
        or row.get("close_dt")
        or row.get("close_time")
    )
    if raw_close_time is None or raw_close_time <= datetime.now(timezone.utc):
        return None

    yes_bid = _f(row.get("yes_bid"))
    yes_ask = _f(row.get("yes_ask"))
    no_bid = _f(row.get("no_bid"))
    no_ask = _f(row.get("no_ask"))
    mid = _f(row.get("mid"))
    spread = _f(row.get("spread"))

    # Recompute mid/spread from quotes if missing
    if mid is None and yes_bid is not None and yes_ask is not None:
        mid = round((yes_bid + yes_ask) / 2.0, 4)
    if spread is None:
        if yes_bid is not None and yes_ask is not None:
            spread = round(max(yes_ask - yes_bid, 0.0), 4)
        elif no_bid is not None and no_ask is not None:
            spread = round(max(no_ask - no_bid, 0.0), 4)

    return MarketSnapshot(
        ticker=ticker,
        title=str(row.get("title") or ""),
        subtitle=str(row.get("subtitle") or ""),
        category=str(row.get("category") or ""),
        market_family=row.get("market_family"),
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        last_price=_f(row.get("last_price")),
        mid=mid,
        spread=spread,
        status=str(row.get("status") or "unknown").lower(),
        raw_close_time=raw_close_time,       # always datetime, never string
        volume_24h=_f(row.get("volume_24h")),
        liquidity=_f(row.get("liquidity")),
        open_interest=_f(row.get("open_interest")),
        spot_symbol=str(row.get("spot_symbol") or row.get("asset") or "").upper() or None,
        threshold=_f(row.get("threshold")),
        direction=row.get("direction"),
        floor_strike=_f(row.get("floor_strike")),
        cap_strike=_f(row.get("cap_strike")),
        distance_from_spot_pct=_f(row.get("distance_from_spot_pct")),
    )


class KalshiMarketStream:
    """
    Runs the legacy KalshiMarketStreamV40 and bridges its output into
    the new SnapshotStore.

    Polling pattern: after each seed/update from the legacy stream, we
    translate all current rows into MarketSnapshot objects and replace the
    store contents atomically.
    """

    def __init__(
        self,
        store: SnapshotStore,
        api_key_id: str,
        private_key_path: str,
        poll_interval: float = 15.0,
        ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2",
        public_base: str = "https://api.elections.kalshi.com/trade-api/v2",
    ) -> None:
        self._store = store
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._poll_interval = poll_interval
        self._ws_url = ws_url
        self._public_base = public_base
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._status: str = "stopped"
        self._last_update: Optional[datetime] = None
        self._snapshot_count: int = 0

    async def start(self, session: aiohttp.ClientSession) -> None:
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(
            self._run(session), name="kalshi-market-stream"
        )
        logger.info("KalshiMarketStream started")

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._status = "stopped"

    async def _run(self, session: aiohttp.ClientSession) -> None:
        """
        Seed from public REST on startup, then poll periodically to
        pick up any updates the WS may have missed.

        The legacy stream's WebSocket handling runs inside a
        bot-coupled object; here we operate in a decoupled mode:
        seed via REST, refresh on interval.
        """
        self._status = "starting"

        # Initial seed
        await self._seed(session)

        # Periodic refresh
        while not self._stop_evt.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_evt.wait(), timeout=self._poll_interval
                )
            except asyncio.TimeoutError:
                pass
            if not self._stop_evt.is_set():
                await self._seed(session)

    async def _seed(self, session: aiohttp.ClientSession) -> None:
        """Fetch current market snapshot from the Kalshi public events API."""
        import urllib.parse

        self._status = "seeding"
        rows: list[Dict[str, Any]] = []
        series_list = ["KXBTC", "KXETH", "KXSOL"]

        for series in series_list:
            params = {
                "status": "open",
                "series_ticker": series,
                "with_nested_markets": "true",
                "limit": "200",
            }
            url = f"{self._public_base}/events?{urllib.parse.urlencode(params)}"
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "Kalshi events API returned %d for series %s",
                            resp.status, series,
                        )
                        continue
                    data = await resp.json()
                    events = data.get("events") or []
                    for event in events:
                        for market in event.get("markets") or []:
                            row = self._market_to_row(market, event)
                            if row:
                                rows.append(row)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Kalshi seed error for %s: %s", series, exc)

        if rows:
            await self._apply_rows(rows)
            logger.debug(
                "KalshiMarketStream: %d snapshots loaded", self._snapshot_count
            )

        self._status = "live" if rows else "degraded"

    def _market_to_row(
        self, market: Dict[str, Any], event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Convert a raw Kalshi market dict into a normalised row dict."""
        import re

        ticker = str(market.get("ticker") or "").strip()
        if not ticker:
            return None

        close_dt = None
        for key in ("close_dt", "raw_close_time", "close_time", "expiration_time"):
            close_dt = _parse_dt(market.get(key))
            if close_dt:
                break

        if close_dt is None or close_dt <= datetime.now(timezone.utc):
            return None

        def prob(v: Any, dv: Any = None) -> Optional[float]:
            d = _f(dv)
            if d is not None:
                return max(0.0, min(1.0, round(d, 4)))
            n = _f(v)
            if n is None:
                return None
            if n > 1:
                n /= 100.0
            return max(0.0, min(1.0, round(n, 4)))

        yes_bid = prob(market.get("yes_bid"), market.get("yes_bid_dollars"))
        yes_ask = prob(market.get("yes_ask"), market.get("yes_ask_dollars"))
        no_bid = prob(market.get("no_bid"), market.get("no_bid_dollars"))
        no_ask = prob(market.get("no_ask"), market.get("no_ask_dollars"))

        spread = None
        if yes_bid is not None and yes_ask is not None:
            spread = round(max(yes_ask - yes_bid, 0.0), 4)
        mid = None
        if yes_bid is not None and yes_ask is not None:
            mid = round((yes_bid + yes_ask) / 2.0, 4)

        floor_strike = _f(market.get("floor_strike"))
        cap_strike = _f(market.get("cap_strike"))

        # Detect asset from ticker prefix
        asset = ""
        for prefix, sym in [("KXBTC-", "BTC"), ("KXETH-", "ETH"), ("KXSOL-", "SOL")]:
            if ticker.startswith(prefix):
                asset = sym
                break

        # Detect range_bucket family
        title = str(market.get("title") or event.get("title") or "").lower()
        family = market.get("market_family")
        if not family:
            if (
                floor_strike is not None
                and cap_strike is not None
                and asset
            ) or "price range" in title:
                family = "range_bucket"

        return {
            "ticker": ticker,
            "title": market.get("title") or event.get("title") or "",
            "subtitle": market.get("subtitle") or market.get("yes_sub_title") or "",
            "status": str(market.get("status") or "active").lower(),
            "spot_symbol": asset,
            "asset": asset,
            "market_family": family,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "mid": mid,
            "spread": spread,
            "floor_strike": floor_strike,
            "cap_strike": cap_strike,
            "threshold": _f(market.get("threshold")),
            "direction": market.get("direction"),
            "raw_close_time": close_dt,      # already a datetime
            "volume_24h": _f(market.get("volume_24h") or market.get("volume")),
            "liquidity": _f(market.get("liquidity")),
            "open_interest": _f(market.get("open_interest")),
        }

    async def _apply_rows(self, rows: list[Dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc)
        snapshots: Dict[str, MarketSnapshot] = {}
        for row in rows:
            snap = _row_to_snapshot(row)
            if snap is not None:
                snapshots[snap.ticker] = snap

        await self._store.replace_all(snapshots)
        self._last_update = now
        self._snapshot_count = len(snapshots)

    def health_dict(self) -> dict:
        return {
            "status": self._status,
            "snapshot_count": self._snapshot_count,
            "last_update": self._last_update.isoformat() if self._last_update else None,
        }
