"""
SummarizerJob: periodically rebuilds strategy_summaries and
parameter_suggestions from closed shadow_trades.

Low-priority background task. Output is read-only by the dashboard.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..persistence.db import Database
from ..observability.health import HealthRegistry

logger = logging.getLogger(__name__)

_WINDOWS = [
    ("1h",  "-1 hours"),
    ("6h",  "-6 hours"),
    ("24h", "-1 day"),
    ("7d",  "-7 days"),
    ("all", None),
]


class SummarizerJob:
    def __init__(
        self,
        *,
        db: Database,
        health: HealthRegistry,
        interval_seconds: int = 120,
    ) -> None:
        self._db = db
        self._health_sub = health.register("summarizer")
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(self._run(), name="summarizer")

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
                await self._rebuild()
                self._health_sub.mark_ok()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._health_sub.mark_degraded(repr(exc))
                logger.error("Summarizer error: %s", exc)

            try:
                await asyncio.wait_for(
                    self._stop_evt.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                pass

    async def _rebuild(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._db.connection() as conn:
            await conn.execute("DELETE FROM strategy_summaries")

            for window_name, since in _WINDOWS:
                where = (
                    f"AND opened_at >= datetime('now', '{since}')"
                    if since else ""
                )
                rows = await (
                    await conn.execute(
                        f"""
                        SELECT variant_name,
                            COUNT(*) as trades,
                            SUM(CASE WHEN outcome='win' THEN 1 ELSE 0 END) as wins,
                            SUM(CASE WHEN outcome='loss' THEN 1 ELSE 0 END) as losses,
                            SUM(realized_pnl) as total_pnl,
                            AVG(realized_pnl) as avg_pnl,
                            AVG(edge_at_entry) as avg_edge,
                            AVG(spread_at_entry) as avg_spread,
                            AVG(hours_to_expiry_at_entry) as avg_hours,
                            AVG(distance_from_spot_pct_at_entry) as avg_dist
                        FROM shadow_trades
                        WHERE closed_at IS NOT NULL {where}
                        GROUP BY variant_name
                        """
                    )
                ).fetchall()

                for r in rows:
                    trades = int(r["trades"] or 0)
                    wins = int(r["wins"] or 0)
                    losses = int(r["losses"] or 0)
                    win_rate = wins / trades if trades > 0 else 0.0
                    total_pnl = float(r["total_pnl"] or 0.0)
                    expectancy = total_pnl / trades if trades > 0 else 0.0

                    await conn.execute(
                        """
                        INSERT INTO strategy_summaries (
                            ts, strategy_name, variant_name, window_name,
                            trade_count, win_count, loss_count, win_rate,
                            realized_pnl, avg_edge, avg_spread,
                            avg_hours_to_expiry, avg_distance_from_spot_pct,
                            expectancy
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now, "crypto_lag", r["variant_name"], window_name,
                            trades, wins, losses, round(win_rate, 4),
                            round(total_pnl, 4),
                            r["avg_edge"], r["avg_spread"],
                            r["avg_hours"], r["avg_dist"],
                            round(expectancy, 4),
                        ),
                    )

            await conn.commit()
            logger.debug("Summarizer rebuilt strategy_summaries")
