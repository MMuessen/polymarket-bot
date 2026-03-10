"""
ShadowTradeRepository: all reads and writes for the shadow_trades table.

Single source of truth for trade lifecycle:
  open → (mark updates via shadow_positions) → close → outcome set

No business logic here — only SQL.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiosqlite

from ..db import Database


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(obj: Any) -> str:
    return json.dumps(obj, default=str, separators=(",", ":"))


class ShadowTradeRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------ #
    # Write                                                                 #
    # ------------------------------------------------------------------ #

    async def open_trade(
        self,
        *,
        strategy_name: str,
        variant_name: str,
        regime_name: Optional[str],
        ticker: str,
        side: str,
        qty: int,
        entry_price: float,
        edge_at_entry: Optional[float],
        spread_at_entry: Optional[float],
        hours_to_expiry_at_entry: Optional[float],
        distance_from_spot_pct_at_entry: Optional[float],
        meta: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Insert a new open trade. Returns the new row id."""
        async with self._db.connection() as conn:
            cursor = await conn.execute(
                """
                INSERT INTO shadow_trades (
                    opened_at, strategy_name, variant_name, regime_name,
                    ticker, side, qty, entry_price,
                    edge_at_entry, spread_at_entry,
                    hours_to_expiry_at_entry, distance_from_spot_pct_at_entry,
                    entry_edge,
                    max_favorable_excursion, max_adverse_excursion,
                    best_mark_price, worst_mark_price,
                    meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, 0.0, ?, ?, ?)
                """,
                (
                    _utc_now(),
                    strategy_name,
                    variant_name,
                    regime_name,
                    ticker,
                    side,
                    qty,
                    float(entry_price),
                    float(edge_at_entry) if edge_at_entry is not None else None,
                    float(spread_at_entry) if spread_at_entry is not None else None,
                    float(hours_to_expiry_at_entry) if hours_to_expiry_at_entry is not None else None,
                    float(distance_from_spot_pct_at_entry) if distance_from_spot_pct_at_entry is not None else None,
                    float(edge_at_entry) if edge_at_entry is not None else None,
                    float(entry_price),
                    float(entry_price),
                    _json(meta or {}),
                ),
            )
            await conn.commit()
            return cursor.lastrowid  # type: ignore[return-value]

    async def close_trade(
        self,
        *,
        ticker: str,
        variant_name: str,
        exit_price: float,
        realized_pnl: float,
        close_reason: str,
        edge_at_exit: Optional[float] = None,
        spread_at_exit: Optional[float] = None,
    ) -> bool:
        """Close the open trade for (ticker, variant_name). Returns True if a row was updated."""
        now = _utc_now()
        async with self._db.connection() as conn:
            # Compute minutes_open from opened_at
            row = await (
                await conn.execute(
                    "SELECT opened_at, best_mark_price, worst_mark_price "
                    "FROM shadow_trades WHERE ticker=? AND variant_name=? AND closed_at IS NULL "
                    "ORDER BY id DESC LIMIT 1",
                    (ticker, variant_name),
                )
            ).fetchone()

            if row is None:
                return False

            try:
                opened = datetime.fromisoformat(row["opened_at"])
                minutes_open = (
                    datetime.now(timezone.utc) - opened
                ).total_seconds() / 60.0
            except Exception:
                minutes_open = None

            entry_price_row = await (
                await conn.execute(
                    "SELECT entry_price, qty FROM shadow_trades "
                    "WHERE ticker=? AND variant_name=? AND closed_at IS NULL "
                    "ORDER BY id DESC LIMIT 1",
                    (ticker, variant_name),
                )
            ).fetchone()

            outcome: Optional[str] = None
            if entry_price_row is not None:
                entry = float(entry_price_row["entry_price"] or 0.0)
                outcome = "win" if exit_price > entry else "loss"

            await conn.execute(
                """
                UPDATE shadow_trades
                SET closed_at=?, exit_price=?, realized_pnl=?, close_reason=?,
                    outcome=?, edge_at_exit=?, spread_at_exit=?, minutes_open=?
                WHERE ticker=? AND variant_name=? AND closed_at IS NULL
                """,
                (
                    now,
                    float(exit_price),
                    float(realized_pnl),
                    close_reason,
                    outcome,
                    float(edge_at_exit) if edge_at_exit is not None else None,
                    float(spread_at_exit) if spread_at_exit is not None else None,
                    minutes_open,
                    ticker,
                    variant_name,
                ),
            )
            await conn.commit()
            return True

    async def update_excursions(
        self,
        *,
        ticker: str,
        variant_name: str,
        best_mark: float,
        worst_mark: float,
        mfe: float,
        mae: float,
    ) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                """
                UPDATE shadow_trades
                SET best_mark_price=?, worst_mark_price=?,
                    max_favorable_excursion=?, max_adverse_excursion=?
                WHERE ticker=? AND variant_name=? AND closed_at IS NULL
                """,
                (best_mark, worst_mark, mfe, mae, ticker, variant_name),
            )
            await conn.commit()

    # ------------------------------------------------------------------ #
    # Read                                                                  #
    # ------------------------------------------------------------------ #

    async def get_open(self, ticker: str, variant_name: str) -> Optional[Dict[str, Any]]:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM shadow_trades WHERE ticker=? AND variant_name=? "
                    "AND closed_at IS NULL ORDER BY id DESC LIMIT 1",
                    (ticker, variant_name),
                )
            ).fetchone()
            return dict(row) if row else None

    async def get_recent_closed(
        self,
        ticker: str,
        variant_name: str,
        within_minutes: float,
    ) -> Optional[Dict[str, Any]]:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    SELECT * FROM shadow_trades
                    WHERE ticker=? AND variant_name=? AND closed_at IS NOT NULL
                      AND closed_at >= datetime('now', ?)
                    ORDER BY id DESC LIMIT 1
                    """,
                    (ticker, variant_name, f"-{int(within_minutes)} minutes"),
                )
            ).fetchone()
            return dict(row) if row else None

    async def count_today(self, ticker: str, variant_name: str) -> int:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT COUNT(*) FROM shadow_trades "
                    "WHERE ticker=? AND variant_name=? AND opened_at >= datetime('now','-1 day')",
                    (ticker, variant_name),
                )
            ).fetchone()
            return int(row[0] or 0)

    async def list_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM shadow_trades ORDER BY id DESC LIMIT ?", (limit,)
                )
            ).fetchall()
            return [dict(r) for r in rows]

    async def session_stats(self) -> Dict[str, Any]:
        """Aggregate PnL and win-rate across all closed trades."""
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    """
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as wins,
                        SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END) as losses,
                        SUM(realized_pnl) as total_pnl,
                        AVG(realized_pnl) as avg_pnl
                    FROM shadow_trades WHERE closed_at IS NOT NULL
                    """
                )
            ).fetchone()
            total = int(row["total"] or 0)
            wins = int(row["wins"] or 0)
            return {
                "total_trades": total,
                "wins": wins,
                "losses": int(row["losses"] or 0),
                "win_rate": round(wins / total, 4) if total > 0 else 0.0,
                "total_pnl": round(float(row["total_pnl"] or 0.0), 4),
                "avg_pnl": round(float(row["avg_pnl"] or 0.0), 4),
            }
