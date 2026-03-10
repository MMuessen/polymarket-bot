"""
StrategySettingsRepository: operator-controlled strategy parameters.

Wraps the strategy_settings and operator_settings tables.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..db import Database


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StrategySettingsRepository:
    def __init__(self, db: Database) -> None:
        self._db = db

    async def get(
        self,
        strategy_name: str,
        variant_name: str,
        setting_name: str,
    ) -> Optional[str]:
        async with self._db.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT setting_value FROM strategy_settings "
                    "WHERE strategy_name=? AND variant_name=? AND setting_name=?",
                    (strategy_name, variant_name, setting_name),
                )
            ).fetchone()
            return row["setting_value"] if row else None

    async def set(
        self,
        strategy_name: str,
        variant_name: str,
        setting_name: str,
        value: str,
    ) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                """
                INSERT INTO strategy_settings
                    (strategy_name, variant_name, setting_name, setting_value, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (strategy_name, variant_name, setting_name)
                DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """,
                (strategy_name, variant_name, setting_name, value, _utc_now()),
            )
            await conn.commit()

    async def get_all(self, strategy_name: str, variant_name: str) -> Dict[str, str]:
        async with self._db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT setting_name, setting_value FROM strategy_settings "
                    "WHERE strategy_name=? AND variant_name=?",
                    (strategy_name, variant_name),
                )
            ).fetchall()
            return {r["setting_name"]: r["setting_value"] for r in rows}

    async def log_operator_action(
        self,
        strategy_name: str,
        variant_name: Optional[str],
        parameter_name: str,
        value_text: str,
        source: str = "operator",
    ) -> None:
        async with self._db.connection() as conn:
            await conn.execute(
                """
                INSERT INTO operator_settings
                    (ts, strategy_name, variant_name, parameter_name, value_text, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (_utc_now(), strategy_name, variant_name, parameter_name, value_text, source),
            )
            await conn.commit()
