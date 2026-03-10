"""
HealthRegistry: a single place where all subsystems report their health.

Subsystems write their state here; API routes read it.
No subsystem health check ever blocks a route.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class HealthStatus(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"
    STARTING = "starting"
    UNKNOWN = "unknown"


@dataclass
class SubsystemHealth:
    name: str
    status: HealthStatus = HealthStatus.STARTING
    last_ok_at: Optional[datetime] = None
    last_error_at: Optional[datetime] = None
    last_error: Optional[str] = None
    detail: Dict[str, Any] = field(default_factory=dict)

    def mark_ok(self, detail: Optional[Dict[str, Any]] = None) -> None:
        self.status = HealthStatus.OK
        self.last_ok_at = datetime.now(timezone.utc)
        self.last_error = None
        if detail:
            self.detail.update(detail)

    def mark_degraded(self, error: str, detail: Optional[Dict[str, Any]] = None) -> None:
        self.status = HealthStatus.DEGRADED
        self.last_error_at = datetime.now(timezone.utc)
        self.last_error = error
        if detail:
            self.detail.update(detail)

    def mark_down(self, error: str, detail: Optional[Dict[str, Any]] = None) -> None:
        self.status = HealthStatus.DOWN
        self.last_error_at = datetime.now(timezone.utc)
        self.last_error = error
        if detail:
            self.detail.update(detail)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "last_ok_at": self.last_ok_at.isoformat() if self.last_ok_at else None,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "last_error": self.last_error,
            "detail": self.detail,
        }


class HealthRegistry:
    """Thread-safe (asyncio-safe) health state registry."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._subsystems: Dict[str, SubsystemHealth] = {}

    def register(self, name: str) -> SubsystemHealth:
        """Register a subsystem. Returns the SubsystemHealth handle."""
        h = SubsystemHealth(name=name)
        self._subsystems[name] = h
        return h

    def get(self, name: str) -> Optional[SubsystemHealth]:
        return self._subsystems.get(name)

    def overall_status(self) -> HealthStatus:
        statuses = {s.status for s in self._subsystems.values()}
        if HealthStatus.DOWN in statuses:
            return HealthStatus.DOWN
        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED
        if HealthStatus.STARTING in statuses:
            return HealthStatus.STARTING
        if all(s == HealthStatus.OK for s in statuses):
            return HealthStatus.OK
        return HealthStatus.UNKNOWN

    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall": self.overall_status().value,
            "subsystems": {
                name: h.to_dict() for name, h in self._subsystems.items()
            },
        }
