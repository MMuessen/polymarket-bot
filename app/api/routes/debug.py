from __future__ import annotations
from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from ...market.snapshot import SnapshotStore
from ...paper.engine import PaperEngine
from ...persistence.repos.candidates import CandidateEventRepository
from ...jobs.loop import StatusCache


def make_debug_router(
    store: SnapshotStore,
    engine: PaperEngine,
    candidates: CandidateEventRepository,
    cache: StatusCache,
) -> APIRouter:
    r = APIRouter()

    @r.get("/api/debug/snapshots")
    def get_snapshots():
        """Current market snapshots — for diagnostics only."""
        snaps = store.snapshot()
        return JSONResponse({
            "count": len(snaps),
            "tickers": sorted(snaps.keys()),
            "sample": [
                {
                    "ticker": s.ticker,
                    "status": s.status,
                    "fair_yes": s.fair_yes,
                    "best_side": s.best_side,
                    "best_exec_edge": s.best_exec_edge,
                    "hours_to_expiry": s.hours_to_expiry,
                    "rationale": s.rationale,
                }
                for s in list(snaps.values())[:20]
            ],
        })

    @r.get("/api/debug/maker-queue")
    def get_maker_queue():
        return JSONResponse(engine.maker_stats())

    @r.get("/api/debug/candidate-events")
    async def get_candidates():
        summary = await candidates.rejection_summary(minutes=10)
        recent = await candidates.list_recent(limit=30)
        return JSONResponse({
            "rejection_summary_10m": summary,
            "recent": recent,
        })

    @r.get("/api/debug/cache-age")
    def get_cache_age():
        return JSONResponse({
            "age_seconds": round(cache.age_seconds, 2),
            "stale": cache.age_seconds > 60,
        })

    return r
