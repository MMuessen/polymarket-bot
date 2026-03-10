"""
CandidateEventRepository: audit log of every opportunity the strategy evaluated.

This table is append-only. No updates. Used for calibration and diagnostics.
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


class CandidateEventRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def log(
        self,
        *,
        strategy_name: str,
        variant_name: str,
        ticker: str,
        market_family: Optional[str],
        status: Optional[str],
        spot_symbol: Optional[str],
        spot_price: Optional[float],
        floor_strike: Optional[float],
        cap_strike: Optional[float],
        threshold: Optional[float],
        direction: Optional[str],
        yes_bid: Optional[float],
        yes_ask: Optional[float],
        no_bid: Optional[float],
        no_ask: Optional[float],
        mid: Optional[float],
        spread: Optional[float],
        fair_yes: Optional[float],
        edge: Optional[float],
        hours_to_expiry: Optional[float],
        distance_from_spot_pct: Optional[float],
        liquidity: Optional[float],
        volume_24h: Optional[float],
        open_interest: Optional[float],
        eligible: bool,
        rejection_reason: Optional[str],
        regime_name: Optional[str],
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                """
                INSERT INTO candidate_events (
                    ts, strategy_name, variant_name, ticker,
                    market_family, status, spot_symbol, spot_price,
                    floor_strike, cap_strike, threshold, direction,
                    yes_bid, yes_ask, no_bid, no_ask, mid, spread,
                    fair_yes, edge, hours_to_expiry, distance_from_spot_pct,
                    liquidity, volume_24h, open_interest,
                    eligible, rejection_reason, regime_name, meta_json
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    _utc_now(),
                    strategy_name,
                    variant_name,
                    ticker,
                    market_family,
                    status,
                    spot_symbol,
                    float(spot_price) if spot_price is not None else None,
                    float(floor_strike) if floor_strike is not None else None,
                    float(cap_strike) if cap_strike is not None else None,
                    float(threshold) if threshold is not None else None,
                    direction,
                    float(yes_bid) if yes_bid is not None else None,
                    float(yes_ask) if yes_ask is not None else None,
                    float(no_bid) if no_bid is not None else None,
                    float(no_ask) if no_ask is not None else None,
                    float(mid) if mid is not None else None,
                    float(spread) if spread is not None else None,
                    float(fair_yes) if fair_yes is not None else None,
                    float(edge) if edge is not None else None,
                    float(hours_to_expiry) if hours_to_expiry is not None else None,
                    float(distance_from_spot_pct) if distance_from_spot_pct is not None else None,
                    float(liquidity) if liquidity is not None else None,
                    float(volume_24h) if volume_24h is not None else None,
                    float(open_interest) if open_interest is not None else None,
                    1 if eligible else 0,
                    rejection_reason,
                    regime_name,
                    _json(meta or {}),
                ),
            )
            await conn.commit()

    async def list_recent(self, limit: int = 200) -> List[Dict[str, Any]]:
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM candidate_events ORDER BY id DESC LIMIT ?", (limit,)
                )
            ).fetchall()
            return [dict(r) for r in rows]

    async def rejection_summary(self, minutes: int = 10) -> List[Dict[str, Any]]:
        """Count rejections by reason in the last N minutes."""
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute(
                    """
                    SELECT rejection_reason, COUNT(*) as n
                    FROM candidate_events
                    WHERE eligible=0 AND ts >= datetime('now', ?)
                    GROUP BY rejection_reason
                    ORDER BY n DESC
                    """,
                    (f"-{minutes} minutes",),
                )
            ).fetchall()
            return [dict(r) for r in rows]
