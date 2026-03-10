from __future__ import annotations
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ...jobs.loop import StatusCache


def make_status_router(cache: StatusCache) -> APIRouter:
    r = APIRouter()

    @r.get("/api/status")
    def get_status():
        """
        Returns the pre-computed status blob. Never triggers recomputation.
        Always fast (<1ms).
        """
        return JSONResponse(cache.get())

    @r.get("/architect/data")
    def get_architect_data():
        """Architect-layer view consumed by the dashboard."""
        data = cache.get()
        markets = data.get("markets") or []
        session = data.get("session") or {}
        return JSONResponse({
            "version": "2026.rewrite.v1",
            "runtime": {
                "mode": data.get("mode"),
                "live_armed": data.get("live_armed"),
                "top_edge": data.get("top_edge"),
                "cycle": data.get("cycle"),
                "market_count": data.get("market_count"),
            },
            "session": session,
            "spot_prices": data.get("spot_prices"),
            "health": data.get("health"),
            "stream": data.get("stream"),
            "markets": markets,
            "maker_queue": data.get("maker_queue"),
        })

    return r
