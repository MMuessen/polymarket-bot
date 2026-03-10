"""
Structured logging configuration.

Uses stdlib logging with a JSON-ready formatter for production
and a human-readable formatter for development.
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from typing import Optional


class _ISOFormatter(logging.Formatter):
    """Formats log records as ISO-timestamp lines: 2026-01-01T00:00:00Z [LEVEL] name: message"""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:  # noqa: N802
        return datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3] + "Z"

    def format(self, record: logging.LogRecord) -> str:
        record.asctime = self.formatTime(record)
        return (
            f"{record.asctime} [{record.levelname:<8}] {record.name}: {record.getMessage()}"
        )


def configure_logging(level: str = "INFO") -> None:
    """
    Configure root logger. Call once at process startup.

    Uses a clean ISO-timestamp format.
    Does not configure any external logging libraries — only stdlib.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_ISOFormatter())
    handler.setLevel(numeric_level)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove any existing handlers (uvicorn may have added its own)
    root.handlers.clear()
    root.addHandler(handler)

    # Quiet noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
