"""
PaperAccounting: single source of truth for session PnL and equity.

Computed from the DB on demand (called after each loop cycle) and cached
for route reads. No in-memory mutable balance — the DB is the truth.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..persistence.repos.trades import ShadowTradeRepository
from ..persistence.repos.positions import ShadowPositionRepository

logger = logging.getLogger(__name__)


@dataclass
class SessionSnapshot:
    """Point-in-time accounting snapshot."""
    computed_at: datetime
    starting_balance: float
    realized_pnl: float
    unrealized_pnl: float
    total_trades: int
    open_positions: int
    wins: int
    losses: int
    win_rate: float
    equity: float                   # starting_balance + realized_pnl + unrealized_pnl
    avg_pnl: float
    top_edge: Optional[float]       # best current executable edge across open positions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "computed_at": self.computed_at.isoformat(),
            "starting_balance": self.starting_balance,
            "realized_pnl": round(self.realized_pnl, 4),
            "unrealized_pnl": round(self.unrealized_pnl, 4),
            "equity": round(self.equity, 4),
            "total_trades": self.total_trades,
            "open_positions": self.open_positions,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 4),
            "avg_pnl": round(self.avg_pnl, 4),
            "top_edge": round(self.top_edge, 4) if self.top_edge is not None else None,
        }


class PaperAccounting:
    def __init__(
        self,
        trades_repo: ShadowTradeRepository,
        positions_repo: ShadowPositionRepository,
        starting_balance: float = 1000.0,
    ) -> None:
        self._trades = trades_repo
        self._positions = positions_repo
        self._starting_balance = starting_balance
        self._last_snapshot: Optional[SessionSnapshot] = None

    async def compute(self, top_edge: Optional[float] = None) -> SessionSnapshot:
        """
        Recompute accounting from DB. Called by the trading loop after each cycle.
        """
        stats = await self._trades.session_stats()
        positions = await self._positions.list_all()

        unrealized = sum(
            float(p.get("unrealized_pnl") or 0.0) for p in positions
        )
        realized = float(stats.get("total_pnl") or 0.0)

        snap = SessionSnapshot(
            computed_at=datetime.now(timezone.utc),
            starting_balance=self._starting_balance,
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            total_trades=int(stats.get("total_trades") or 0),
            open_positions=len(positions),
            wins=int(stats.get("wins") or 0),
            losses=int(stats.get("losses") or 0),
            win_rate=float(stats.get("win_rate") or 0.0),
            equity=self._starting_balance + realized + unrealized,
            avg_pnl=float(stats.get("avg_pnl") or 0.0),
            top_edge=top_edge,
        )
        self._last_snapshot = snap
        return snap

    @property
    def last(self) -> Optional[SessionSnapshot]:
        return self._last_snapshot

    async def open_positions_detail(self) -> List[Dict[str, Any]]:
        return await self._positions.list_all()

    async def recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        return await self._trades.list_recent(limit=limit)
