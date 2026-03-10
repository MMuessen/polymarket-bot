"""
Database layer: aiosqlite connection factory + schema migrations.

Design decisions:
- One aiosqlite connection per call site is fine for SQLite (no separate pool needed).
  SQLite supports concurrent reads; writes serialize automatically via WAL mode.
- WAL mode is enabled on every connection for improved read/write concurrency.
- All migrations are additive (ALTER TABLE ADD COLUMN IF NOT EXISTS pattern).
  Existing data is never touched.
- Schema creation is idempotent (CREATE TABLE IF NOT EXISTS).
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiosqlite

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Schema: base tables (must match strategy_lab/schema.sql)                    #
# Extended columns added by legacy patches are handled in migrations below.   #
# --------------------------------------------------------------------------- #

_BASE_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS candidate_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    ticker TEXT NOT NULL,
    market_family TEXT,
    status TEXT,
    spot_symbol TEXT,
    spot_price REAL,
    floor_strike REAL,
    cap_strike REAL,
    threshold REAL,
    direction TEXT,
    yes_bid REAL,
    yes_ask REAL,
    no_bid REAL,
    no_ask REAL,
    mid REAL,
    spread REAL,
    fair_yes REAL,
    edge REAL,
    hours_to_expiry REAL,
    distance_from_spot_pct REAL,
    liquidity REAL,
    volume_24h REAL,
    open_interest REAL,
    eligible INTEGER NOT NULL,
    rejection_reason TEXT,
    regime_name TEXT,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS shadow_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    regime_name TEXT,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    exit_price REAL,
    outcome TEXT,
    realized_pnl REAL,
    max_favorable_excursion REAL,
    max_adverse_excursion REAL,
    edge_at_entry REAL,
    spread_at_entry REAL,
    hours_to_expiry_at_entry REAL,
    distance_from_spot_pct_at_entry REAL,
    meta_json TEXT,
    close_reason TEXT,
    best_mark_price REAL,
    worst_mark_price REAL,
    entry_edge REAL,
    edge_at_exit REAL,
    spread_at_exit REAL,
    distance_from_spot_pct_at_exit REAL,
    minutes_open REAL
);

CREATE TABLE IF NOT EXISTS shadow_positions (
    ticker TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    regime_name TEXT,
    side TEXT NOT NULL,
    qty INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    last_mark_price REAL,
    unrealized_pnl REAL,
    best_mark_price REAL,
    worst_mark_price REAL,
    entry_edge REAL,
    last_edge REAL,
    last_spread REAL,
    PRIMARY KEY (ticker, variant_name)
);

CREATE TABLE IF NOT EXISTS strategy_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    window_name TEXT NOT NULL,
    trade_count INTEGER NOT NULL,
    win_count INTEGER NOT NULL,
    loss_count INTEGER NOT NULL,
    win_rate REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    avg_edge REAL,
    avg_spread REAL,
    avg_hours_to_expiry REAL,
    avg_distance_from_spot_pct REAL,
    expectancy REAL,
    max_drawdown REAL,
    score REAL,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS parameter_suggestions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    parameter_name TEXT NOT NULL,
    current_value TEXT,
    suggested_value TEXT,
    confidence TEXT,
    sample_size INTEGER,
    basis_window TEXT,
    metric_name TEXT,
    explanation TEXT,
    applied INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS strategy_settings (
    strategy_name TEXT NOT NULL,
    variant_name TEXT NOT NULL,
    setting_name TEXT NOT NULL,
    setting_value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (strategy_name, variant_name, setting_name)
);

CREATE TABLE IF NOT EXISTS market_microstructure_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    ticker TEXT NOT NULL,
    best_yes_bid REAL,
    best_no_bid REAL,
    implied_yes_ask REAL,
    implied_no_ask REAL,
    spread REAL,
    trade_burst_score REAL,
    quote_persistence_score REAL,
    replenishment_score REAL,
    liquidity_withdrawal_score REAL,
    meta_json TEXT
);

CREATE TABLE IF NOT EXISTS operator_settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    variant_name TEXT,
    parameter_name TEXT NOT NULL,
    value_text TEXT NOT NULL,
    source TEXT NOT NULL,
    applied INTEGER NOT NULL DEFAULT 0
);
"""

# Additive migrations: columns added by legacy patches.
# Each entry: (table, column, type, default).
_MIGRATIONS: list[tuple[str, str, str, str]] = [
    ("shadow_trades", "close_reason", "TEXT", ""),
    ("shadow_trades", "exit_price", "REAL", ""),
    ("shadow_trades", "closed_at", "TEXT", ""),
    ("shadow_trades", "realized_pnl", "REAL", ""),
    ("shadow_trades", "outcome", "TEXT", ""),
    ("shadow_trades", "best_mark_price", "REAL", ""),
    ("shadow_trades", "worst_mark_price", "REAL", ""),
    ("shadow_trades", "entry_edge", "REAL", ""),
    ("shadow_trades", "edge_at_exit", "REAL", ""),
    ("shadow_trades", "spread_at_exit", "REAL", ""),
    ("shadow_trades", "distance_from_spot_pct_at_exit", "REAL", ""),
    ("shadow_trades", "minutes_open", "REAL", ""),
    ("shadow_positions", "best_mark_price", "REAL", ""),
    ("shadow_positions", "worst_mark_price", "REAL", ""),
    ("shadow_positions", "entry_edge", "REAL", ""),
    ("shadow_positions", "last_edge", "REAL", ""),
    ("shadow_positions", "last_spread", "REAL", ""),
]


class Database:
    """
    Async database facade.

    Usage:
        db = Database(path)
        await db.init()               # once at startup
        async with db.connection() as conn:
            await conn.execute(...)
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    async def init(self) -> None:
        """Create schema and run additive migrations. Safe to call repeatedly."""
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.executescript(_BASE_SCHEMA)
            await conn.commit()
            await self._run_migrations(conn)

        logger.info("Database initialized: %s", path)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[aiosqlite.Connection]:
        """Yield a configured aiosqlite connection. Caller manages transactions."""
        async with aiosqlite.connect(self.path) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA foreign_keys=ON")
            try:
                yield conn
            except Exception:
                await conn.rollback()
                raise

    async def _run_migrations(self, conn: aiosqlite.Connection) -> None:
        """Apply additive column migrations idempotently."""
        for table, column, col_type, default in _MIGRATIONS:
            existing = await conn.execute(f"PRAGMA table_info({table})")
            cols = {row[1] async for row in existing}
            if column not in cols:
                default_clause = f" DEFAULT {default}" if default else ""
                await conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {column} {col_type}{default_clause}"
                )
                logger.info("Migration: added %s.%s %s", table, column, col_type)
        await conn.commit()
