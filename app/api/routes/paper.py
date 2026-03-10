from __future__ import annotations
from typing import Any, Dict
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ...paper.accounting import PaperAccounting
from ...paper.engine import PaperEngine
from ...persistence.db import Database


class PaperToggle(BaseModel):
    enabled: bool


def make_paper_router(
    accounting: PaperAccounting,
    engine: PaperEngine,
    db: Database,
) -> APIRouter:
    r = APIRouter()

    @r.get("/api/paper-session")
    async def get_paper_session():
        snap = accounting.last
        positions = await accounting.open_positions_detail()
        trades = await accounting.recent_trades(limit=50)
        return JSONResponse({
            "session": snap.to_dict() if snap else {},
            "open_positions": positions,
            "recent_trades": trades,
        })

    @r.get("/api/paper-controls")
    def get_paper_controls():
        return JSONResponse({
            "enabled": engine.enabled,
            "mode": "paper",
        })

    @r.post("/api/paper-controls")
    async def set_paper_controls(body: PaperToggle):
        engine.enabled = body.enabled
        return JSONResponse({"ok": True, "enabled": engine.enabled})

    @r.post("/api/paper/reset")
    async def reset_paper():
        """
        Reset paper trading: clear all open positions and trades.
        Used at cutover to start fresh.
        """
        async with db.connection() as conn:
            await conn.execute("DELETE FROM shadow_positions")
            await conn.execute("DELETE FROM shadow_trades")
            await conn.execute("DELETE FROM candidate_events")
            await conn.execute("DELETE FROM strategy_summaries")
            await conn.commit()
        engine._maker._orders.clear()
        return JSONResponse({"ok": True, "message": "Paper trading data reset"})

    return r
