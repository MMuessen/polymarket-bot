from __future__ import annotations
import csv
import io
from typing import Iterable, Dict, Any
from app.strategy_lab.storage import get_conn

def init_research_store() -> None:
    conn = get_conn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS market_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                ticker TEXT NOT NULL,
                asset TEXT,
                market_family TEXT,
                status TEXT,
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
                open_interest REAL,
                volume REAL,
                close_time TEXT,
                spot REAL,
                fair_yes REAL,
                edge REAL,
                distance_from_spot_pct REAL
            );
            CREATE INDEX IF NOT EXISTS idx_market_observations_ts ON market_observations(ts);
            CREATE INDEX IF NOT EXISTS idx_market_observations_ticker_ts ON market_observations(ticker, ts);
            """
        )
        conn.commit()
    finally:
        conn.close()

def _ensure():
    init_research_store()

def record_market_observations(rows: Iterable[Dict[str, Any]]) -> int:
    _ensure()
    rows = list(rows or [])
    if not rows:
        return 0

    conn = get_conn()
    try:
        conn.executemany(
            """
            INSERT INTO market_observations(
                ts, ticker, asset, market_family, status,
                floor_strike, cap_strike, threshold, direction,
                yes_bid, yes_ask, no_bid, no_ask, mid, spread,
                open_interest, volume, close_time, spot, fair_yes, edge, distance_from_spot_pct
            ) VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                (
                    r.get("ts"),
                    r.get("ticker"),
                    r.get("asset"),
                    r.get("market_family"),
                    r.get("status"),
                    r.get("floor_strike"),
                    r.get("cap_strike"),
                    r.get("threshold"),
                    r.get("direction"),
                    r.get("yes_bid"),
                    r.get("yes_ask"),
                    r.get("no_bid"),
                    r.get("no_ask"),
                    r.get("mid"),
                    r.get("spread"),
                    r.get("open_interest"),
                    r.get("volume"),
                    r.get("close_time"),
                    r.get("spot"),
                    r.get("fair_yes"),
                    r.get("edge"),
                    r.get("distance_from_spot_pct"),
                )
                for r in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()

def get_research_store_stats() -> Dict[str, Any]:
    _ensure()
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS observation_count,
                COUNT(DISTINCT ticker) AS distinct_tickers,
                MIN(ts) AS first_ts,
                MAX(ts) AS last_ts
            FROM market_observations
            """
        ).fetchone()
        return dict(row) if row else {
            "observation_count": 0,
            "distinct_tickers": 0,
            "first_ts": None,
            "last_ts": None,
        }
    finally:
        conn.close()

def export_market_observations_csv(limit: int = 5000) -> str:
    _ensure()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM market_observations
            ORDER BY ts DESC, ticker ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        cols = [
            "id", "ts", "ticker", "asset", "market_family", "status",
            "floor_strike", "cap_strike", "threshold", "direction",
            "yes_bid", "yes_ask", "no_bid", "no_ask", "mid", "spread",
            "open_interest", "volume", "close_time", "spot", "fair_yes", "edge", "distance_from_spot_pct"
        ]

        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(cols)
        for r in rows:
            writer.writerow([r[c] if c in r.keys() else None for c in cols])
        return out.getvalue()
    finally:
        conn.close()
