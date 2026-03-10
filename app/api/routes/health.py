from __future__ import annotations
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from ...observability.health import HealthRegistry, HealthStatus

router = APIRouter()


def make_health_router(health: HealthRegistry) -> APIRouter:
    r = APIRouter()

    @r.get("/health")
    def get_health():
        data = health.to_dict()
        overall = health.overall_status()
        status_code = 200 if overall in (HealthStatus.OK, HealthStatus.STARTING) else 503
        return JSONResponse(data, status_code=status_code)

    @r.get("/ready")
    def get_ready():
        overall = health.overall_status()
        if overall == HealthStatus.DOWN:
            return JSONResponse({"ready": False, "status": overall.value}, status_code=503)
        return JSONResponse({"ready": True, "status": overall.value})

    return r
