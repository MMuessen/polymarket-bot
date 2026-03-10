"""
MakerQueue: in-memory pending maker order queue.

Logic: instead of taking the ask immediately, we queue an order at
ask - price_improve (the "maker" price). Each loop iteration we check
if current_ask <= target_price (i.e. the market moved to fill us).
Orders expire after max_wait_seconds.

This is owned exclusively by PaperEngine. No external mutation.
All access is synchronous within the async event loop (no threading needed
since SQLite writes are the only blocking ops, and they're in repos).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Composite key: (ticker, variant_name)
_Key = Tuple[str, str]


@dataclass
class PendingOrder:
    ticker: str
    variant_name: str
    side: str                    # "yes" | "no" — set at queue time, never changes
    target_price: float          # fill when current_ask <= this
    ask_at_queue: float          # ask price when order was created
    dollars: float               # target notional
    spread_at_queue: Optional[float]
    edge_at_queue: Optional[float]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def key(self) -> _Key:
        return (self.ticker, self.variant_name)

    def age_seconds(self, now: Optional[datetime] = None) -> float:
        _now = now or datetime.now(timezone.utc)
        return (_now - self.created_at).total_seconds()

    def is_expired(self, max_wait_seconds: float, now: Optional[datetime] = None) -> bool:
        return self.age_seconds(now) >= max_wait_seconds

    def qty(self) -> int:
        """Contracts at the target price."""
        return max(1, int(self.dollars / max(self.target_price, 0.01)))

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "variant_name": self.variant_name,
            "side": self.side,
            "target_price": self.target_price,
            "ask_at_queue": self.ask_at_queue,
            "dollars": self.dollars,
            "spread_at_queue": self.spread_at_queue,
            "edge_at_queue": self.edge_at_queue,
            "created_at": self.created_at.isoformat(),
            "age_seconds": round(self.age_seconds(), 1),
        }


class MakerQueue:
    """
    Thread-safe pending order queue (within a single async context).

    Invariant: at most one pending order per (ticker, variant_name).
    """

    def __init__(
        self,
        price_improve: float = 0.01,
        max_wait_seconds: float = 300.0,
    ) -> None:
        self.price_improve = price_improve
        self.max_wait_seconds = max_wait_seconds
        self._orders: Dict[_Key, PendingOrder] = {}

    def has(self, ticker: str, variant_name: str) -> bool:
        return (ticker, variant_name) in self._orders

    def queue(
        self,
        *,
        ticker: str,
        variant_name: str,
        side: str,
        current_ask: float,
        dollars: float,
        spread_at_queue: Optional[float] = None,
        edge_at_queue: Optional[float] = None,
    ) -> PendingOrder:
        """
        Add a maker order. The target fill price is ask - price_improve.
        If an order already exists for this key, it is replaced.
        """
        target = round(max(0.01, current_ask - self.price_improve), 2)
        order = PendingOrder(
            ticker=ticker,
            variant_name=variant_name,
            side=side,
            target_price=target,
            ask_at_queue=current_ask,
            dollars=dollars,
            spread_at_queue=spread_at_queue,
            edge_at_queue=edge_at_queue,
        )
        self._orders[order.key] = order
        logger.debug(
            "MakerQueue: queued %s %s side=%s target=%.4f",
            variant_name, ticker, side, target,
        )
        return order

    def check_fill(
        self,
        ticker: str,
        variant_name: str,
        current_ask: float,
    ) -> Optional[PendingOrder]:
        """
        Return the order if it should fill now (current_ask <= target_price).
        Does NOT remove from queue — caller must call .remove() after processing.
        """
        order = self._orders.get((ticker, variant_name))
        if order is None:
            return None
        if current_ask <= order.target_price:
            return order
        return None

    def remove(self, ticker: str, variant_name: str) -> None:
        self._orders.pop((ticker, variant_name), None)

    def expire_old(self, now: Optional[datetime] = None) -> List[PendingOrder]:
        """Remove and return all expired orders."""
        expired = [
            o for o in self._orders.values()
            if o.is_expired(self.max_wait_seconds, now)
        ]
        for o in expired:
            self._orders.pop(o.key, None)
            logger.debug(
                "MakerQueue: expired %s %s (age=%.0fs)",
                o.variant_name, o.ticker, o.age_seconds(now),
            )
        return expired

    def all_orders(self) -> List[PendingOrder]:
        return list(self._orders.values())

    def to_list(self) -> List[dict]:
        return [o.to_dict() for o in self._orders.values()]

    def stats(self) -> dict:
        return {
            "pending_count": len(self._orders),
            "orders": self.to_list(),
        }
