from __future__ import annotations
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

from ...persistence.db import Database
from ...persistence.repos.settings import StrategySettingsRepository


class SettingUpdate(BaseModel):
    strategy_name: str
    variant_name: Optional[str] = None
    parameter_name: str
    value_text: str
    source: str = "manual"


def make_strategy_lab_router(
    db: Database,
    settings_repo: StrategySettingsRepository,
) -> APIRouter:
    r = APIRouter()

    @r.get("/api/strategy-lab/settings")
    async def get_settings():
        all_settings = await settings_repo.get_all("crypto_lag", "crypto_lag_aggressive")
        return JSONResponse({"settings": all_settings})

    @r.post("/api/strategy-lab/settings")
    async def update_setting(body: SettingUpdate):
        await settings_repo.set(
            body.strategy_name,
            body.variant_name or "",
            body.parameter_name,
            body.value_text,
        )
        await settings_repo.log_operator_action(
            body.strategy_name,
            body.variant_name,
            body.parameter_name,
            body.value_text,
            body.source,
        )
        return JSONResponse({"ok": True})

    @r.get("/api/strategy-lab/summaries")
    async def get_summaries():
        async with db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM strategy_summaries ORDER BY ts DESC LIMIT 100"
                )
            ).fetchall()
            return JSONResponse({"summaries": [dict(r) for r in rows]})

    @r.get("/api/strategy-lab/suggestions")
    async def get_suggestions():
        async with db.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT * FROM parameter_suggestions ORDER BY ts DESC LIMIT 50"
                )
            ).fetchall()
            return JSONResponse({"suggestions": [dict(r) for r in rows]})

    return r
