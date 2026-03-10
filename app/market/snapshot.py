"""
MarketSnapshot: the canonical data model for a single market.
SnapshotStore: thread/async-safe in-memory store of all live market snapshots.

Design:
- MarketSnapshot is a frozen(-ish) dataclass — enrichment produces a new instance
  or writes to designated mutable fields (fair_yes, edge, etc.)
- SnapshotStore holds the current universe; updated by the stream, read by strategy
- Writes to the store use an asyncio.Lock; reads do not (copy-on-read for safety)
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class MarketSnapshot:
    # --- Core identity ---
    ticker: str
    title: str = ""
    subtitle: str = ""
    category: str = ""
    market_family: Optional[str] = None  # "range_bucket" | "threshold" | None

    # --- Quotes ---
    yes_bid: Optional[float] = None
    yes_ask: Optional[float] = None
    no_bid: Optional[float] = None
    no_ask: Optional[float] = None
    last_price: Optional[float] = None
    mid: Optional[float] = None
    spread: Optional[float] = None

    # --- Market metadata ---
    status: str = "unknown"
    raw_close_time: Optional[datetime] = None     # always a datetime, never a string
    volume_24h: Optional[float] = None
    liquidity: Optional[float] = None
    open_interest: Optional[float] = None

    # --- Classification ---
    spot_symbol: Optional[str] = None            # "BTC" | "ETH" | "SOL"
    threshold: Optional[float] = None
    direction: Optional[str] = None              # "above" | "below"
    floor_strike: Optional[float] = None
    cap_strike: Optional[float] = None
    distance_from_spot_pct: Optional[float] = None
    distance_bucket: Optional[str] = None

    # --- Fair value (set by enrichment, never by stream) ---
    fair_yes: Optional[float] = None
    execution_edge_yes: Optional[float] = None
    execution_edge_no: Optional[float] = None
    edge: Optional[float] = None                 # max(exec_edge_yes, exec_edge_no)
    best_side: Optional[str] = None              # "yes" | "no"
    best_exec_edge: Optional[float] = None

    # --- Fees ---
    entry_fee_yes: Optional[float] = None
    entry_fee_no: Optional[float] = None

    # --- Microstructure (set by gate layer) ---
    obi: Optional[float] = None                  # order book imbalance
    vpin: Optional[float] = None

    # --- Timing ---
    hours_to_expiry: Optional[float] = None

    # --- Diagnostics ---
    rationale: str = ""
    last_enriched_at: Optional[datetime] = None

    def hours_until_expiry(self, now: Optional[datetime] = None) -> Optional[float]:
        if self.raw_close_time is None:
            return None
        from datetime import timezone
        _now = now or datetime.now(timezone.utc)
        delta = (self.raw_close_time - _now).total_seconds()
        return max(delta / 3600.0, 0.0)

    def side_ask(self, side: str) -> Optional[float]:
        """Return the executable ask price for the given side."""
        return self.yes_ask if side == "yes" else self.no_ask

    def side_bid(self, side: str) -> Optional[float]:
        """Return the current bid (mark) for the given side."""
        return self.yes_bid if side == "yes" else self.no_bid

    def is_tradeable(self) -> bool:
        return self.status.lower() in {"active", "open", "initialized"}

    def to_dict(self) -> Dict[str, Any]:
        from dataclasses import asdict
        d = asdict(self)
        if self.raw_close_time is not None:
            d["raw_close_time"] = self.raw_close_time.isoformat()
        if self.last_enriched_at is not None:
            d["last_enriched_at"] = self.last_enriched_at.isoformat()
        return d


class SnapshotStore:
    """
    Async-safe in-memory store for market snapshots.

    - Writes (from stream) acquire the lock.
    - Reads return a shallow copy of the current dict (safe for iteration).
    - Enrichment runs on copies; enriched snapshots are written back with lock.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._store: Dict[str, MarketSnapshot] = {}

    async def put(self, snapshot: MarketSnapshot) -> None:
        async with self._lock:
            self._store[snapshot.ticker] = snapshot

    async def put_many(self, snapshots: Dict[str, MarketSnapshot]) -> None:
        async with self._lock:
            self._store.update(snapshots)

    async def replace_all(self, snapshots: Dict[str, MarketSnapshot]) -> None:
        async with self._lock:
            self._store = dict(snapshots)

    def get(self, ticker: str) -> Optional[MarketSnapshot]:
        return self._store.get(ticker)

    def snapshot(self) -> Dict[str, MarketSnapshot]:
        """Return a shallow copy for safe iteration outside lock."""
        return dict(self._store)

    def count(self) -> int:
        return len(self._store)
