"""
ShadowPositionRepository: live open positions (shadow_positions table).

This table acts as a live view of what is currently held.
It is always consistent with shadow_trades: every open trade has a position row;
every closed trade has no position row.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiosqlite

from ..db import Database


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ShadowPositionRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def open_position(
        self,
        *,
        ticker: str,
        variant_name: str,
        strategy_name: str,
        regime_name: Optional[str],
        side: str,
        qty: int,
        entry_price: float,
        entry_edge: Optional[float],
        initial_mark: float,
    ) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                """
                INSERT OR REPLACE INTO shadow_positions (
                    ticker, variant_name, opened_at, strategy_name, regime_name,
                    side, qty, entry_price,
                    last_mark_price, unrealized_pnl,
                    best_mark_price, worst_mark_price,
                    entry_edge, last_edge, last_spread
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticker,
                    variant_name,
                    _utc_now(),
                    strategy_name,
                    regime_name,
                    side,
                    qty,
                    float(entry_price),
                    float(initial_mark),
                    0.0,
                    float(initial_mark),
                    float(initial_mark),
                    float(entry_edge) if entry_edge is not None else None,
                    float(entry_edge) if entry_edge is not None else None,
                    None,
                ),
            )
            await conn.commit()

    async def close_position(self, ticker: str, variant_name: str) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                "DELETE FROM shadow_positions WHERE ticker=? AND variant_name=?",
                (ticker, variant_name),
            )
            await conn.commit()

    async def update_mark(
        self,
        *,
        ticker: str,
        variant_name: str,
        mark: float,
        edge: Optional[float] = None,
        spread: Optional[float] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Update mark price + unrealized PnL. Returns the updated row or None.
        """
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM shadow_positions WHERE ticker=? AND variant_name=?",
                    (ticker, variant_name),
                )
            ).fetchone()
            if row is None:
                return None

            entry = float(row["entry_price"] or 0.0)
            qty = int(row["qty"] or 0)
            unrealized = (mark - entry) * qty
            best = max(float(row["best_mark_price"] or mark), mark)
            worst = min(float(row["worst_mark_price"] or mark), mark)

            await conn.execute(
                """
                UPDATE shadow_positions
                SET last_mark_price=?, unrealized_pnl=?,
                    best_mark_price=?, worst_mark_price=?,
                    last_edge=COALESCE(?, last_edge),
                    last_spread=COALESCE(?, last_spread)
                WHERE ticker=? AND variant_name=?
                """,
                (mark, unrealized, best, worst, edge, spread, ticker, variant_name),
            )
            await conn.commit()

            return {
                "ticker": ticker,
                "variant_name": variant_name,
                "side": row["side"],
                "entry": entry,
                "qty": qty,
                "mark": mark,
                "unrealized_pnl": unrealized,
                "best_mark": best,
                "worst_mark": worst,
                "mfe": max((best - entry) * qty, 0.0),
                "mae": min((worst - entry) * qty, 0.0),
            }

    async def is_open(self, ticker: str, variant_name: str) -> bool:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT 1 FROM shadow_positions WHERE ticker=? AND variant_name=? LIMIT 1",
                    (ticker, variant_name),
                )
            ).fetchone()
            return row is not None

    async def count_all(self) -> int:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute("SELECT COUNT(*) FROM shadow_positions")
            ).fetchone()
            return int(row[0] or 0)

    async def count_by_variant(self, variant_name: str) -> int:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT COUNT(*) FROM shadow_positions WHERE variant_name=?",
                    (variant_name,),
                )
            ).fetchone()
            return int(row[0] or 0)

    async def count_by_ticker(self, ticker: str) -> int:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT COUNT(*) FROM shadow_positions WHERE ticker=?", (ticker,)
                )
            ).fetchone()
            return int(row[0] or 0)

    async def list_all(self) -> List[Dict[str, Any]]:
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM shadow_positions ORDER BY opened_at DESC"
                )
            ).fetchall()
            return [dict(r) for r in rows]

    async def tickers_for_cluster(self, cluster_key: int) -> List[str]:
        """Return tickers of all open positions in a cluster (floor rounded to 1000)."""
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute("SELECT ticker FROM shadow_positions")
            ).fetchall()
            return [r["ticker"] for r in rows]
