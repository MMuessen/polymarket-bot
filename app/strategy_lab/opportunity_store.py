from __future__ import annotations

import csv
import io
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List

DB_PATH = Path("data/strategy_lab.db")


def _row_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            return {}


def _conn(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_opportunity_schema(db_path: Path | str = DB_PATH) -> None:
    conn = _conn(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS market_opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_ts TEXT NOT NULL,
            ts TEXT NOT NULL,
            ticker TEXT NOT NULL,
            asset TEXT,
            market_family TEXT,
            yes_bid REAL,
            yes_ask REAL,
            no_bid REAL,
            no_ask REAL,
            mid REAL,
            spread REAL,
            spot REAL,
            fair_yes REAL,
            execution_edge_yes REAL,
            execution_edge_no REAL,
            best_side TEXT,
            best_exec_edge REAL,
            distance_from_spot_pct REAL,
            hours_to_expiry REAL,
            floor_strike REAL,
            cap_strike REAL,
            threshold REAL,
            close_time TEXT,
            rationale TEXT,
            market_result TEXT,
            result_yes INTEGER,
            pnl_if_yes REAL,
            pnl_if_no REAL,
            best_side_correct INTEGER,
            best_side_realized_pnl REAL,
            resolved_at TEXT,
            extra_json TEXT,
            UNIQUE(batch_ts, ticker)
        );

        CREATE INDEX IF NOT EXISTS idx_market_opportunities_ticker
            ON market_opportunities(ticker);

        CREATE INDEX IF NOT EXISTS idx_market_opportunities_close_time
            ON market_opportunities(close_time);

        CREATE INDEX IF NOT EXISTS idx_market_opportunities_unresolved
            ON market_opportunities(resolved_at, close_time);

        CREATE INDEX IF NOT EXISTS idx_market_opportunities_best_side
            ON market_opportunities(best_side, best_exec_edge);
        """
    )
    conn.commit()
    conn.close()


def record_market_opportunities(
    rows: Iterable[Dict[str, Any]],
    db_path: Path | str = DB_PATH,
) -> int:
    ensure_opportunity_schema(db_path)

    cols = [
        "batch_ts",
        "ts",
        "ticker",
        "asset",
        "market_family",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "mid",
        "spread",
        "spot",
        "fair_yes",
        "execution_edge_yes",
        "execution_edge_no",
        "best_side",
        "best_exec_edge",
        "distance_from_spot_pct",
        "hours_to_expiry",
        "floor_strike",
        "cap_strike",
        "threshold",
        "close_time",
        "rationale",
        "market_result",
        "result_yes",
        "pnl_if_yes",
        "pnl_if_no",
        "best_side_correct",
        "best_side_realized_pnl",
        "resolved_at",
        "extra_json",
    ]

    cleaned: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            row = _row_dict(row)
        if not row:
            continue
        if not row.get("ticker") or not row.get("batch_ts"):
            continue
        cleaned.append(row)

    if not cleaned:
        return 0

    placeholders = ",".join(["?"] * len(cols))
    sql = f"""
        INSERT OR REPLACE INTO market_opportunities
        ({",".join(cols)})
        VALUES ({placeholders})
    """

    payload = [tuple(r.get(c) for c in cols) for r in cleaned]

    conn = _conn(db_path)
    conn.executemany(sql, payload)
    conn.commit()
    conn.close()
    return len(payload)


def list_unresolved_expired_opportunities(
    limit: int = 200,
    db_path: Path | str = DB_PATH,
) -> List[Dict[str, Any]]:
    ensure_opportunity_schema(db_path)
    conn = _conn(db_path)
    rows = conn.execute(
        """
        SELECT *
        FROM market_opportunities
        WHERE resolved_at IS NULL
          AND close_time IS NOT NULL
          AND close_time <= datetime('now')
        ORDER BY close_time ASC, id ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    conn.close()
    return [_row_dict(r) for r in rows]


def resolve_market_opportunity_outcomes(
    updates: Iterable[Dict[str, Any]],
    db_path: Path | str = DB_PATH,
) -> int:
    ensure_opportunity_schema(db_path)

    cleaned = []
    for row in updates:
        if not isinstance(row, dict):
            row = _row_dict(row)
        if not row or not row.get("id"):
            continue
        cleaned.append(row)

    if not cleaned:
        return 0

    conn = _conn(db_path)
    conn.executemany(
        """
        UPDATE market_opportunities
        SET market_result = ?,
            result_yes = ?,
            pnl_if_yes = ?,
            pnl_if_no = ?,
            best_side_correct = ?,
            best_side_realized_pnl = ?,
            resolved_at = ?
        WHERE id = ?
        """,
        [
            (
                row.get("market_result"),
                row.get("result_yes"),
                row.get("pnl_if_yes"),
                row.get("pnl_if_no"),
                row.get("best_side_correct"),
                row.get("best_side_realized_pnl"),
                row.get("resolved_at"),
                int(row.get("id")),
            )
            for row in cleaned
        ],
    )
    conn.commit()
    conn.close()
    return len(cleaned)


def summarize_market_opportunities(
    limit_recent: int = 20,
    db_path: Path | str = DB_PATH,
) -> Dict[str, Any]:
    ensure_opportunity_schema(db_path)
    conn = _conn(db_path)

    totals = _row_dict(
        conn.execute(
            """
            SELECT
                COUNT(*) AS observations,
                COALESCE(SUM(CASE WHEN execution_edge_yes > 0 THEN 1 ELSE 0 END), 0) AS positive_yes_count,
                COALESCE(SUM(CASE WHEN execution_edge_no > 0 THEN 1 ELSE 0 END), 0) AS positive_no_count,
                COALESCE(SUM(CASE WHEN best_exec_edge > 0 THEN 1 ELSE 0 END), 0) AS positive_best_count,
                COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS resolved_count,
                COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL AND best_exec_edge > 0 THEN 1 ELSE 0 END), 0) AS resolved_positive_best_count,
                ROUND(COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL AND best_exec_edge > 0 THEN COALESCE(best_side_realized_pnl, 0) ELSE 0 END), 0), 4) AS realized_pnl_follow_best
            FROM market_opportunities
            """
        ).fetchone()
    )

    by_best_side = [
        _row_dict(r)
        for r in conn.execute(
            """
            SELECT
                COALESCE(best_side, 'unknown') AS best_side,
                COUNT(*) AS observations,
                COALESCE(SUM(CASE WHEN best_exec_edge > 0 THEN 1 ELSE 0 END), 0) AS positive_count,
                COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS resolved_count,
                ROUND(AVG(CASE WHEN resolved_at IS NOT NULL THEN best_side_correct END), 4) AS resolved_win_rate,
                ROUND(COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL THEN COALESCE(best_side_realized_pnl, 0) ELSE 0 END), 0), 4) AS realized_pnl
            FROM market_opportunities
            GROUP BY COALESCE(best_side, 'unknown')
            ORDER BY observations DESC
            """
        ).fetchall()
    ]

    by_distance_bucket = [
        _row_dict(r)
        for r in conn.execute(
            """
            SELECT
                CASE
                    WHEN distance_from_spot_pct IS NULL THEN 'unknown'
                    WHEN distance_from_spot_pct <= 0.005 THEN '0-0.5%'
                    WHEN distance_from_spot_pct <= 0.015 THEN '0.5-1.5%'
                    WHEN distance_from_spot_pct <= 0.03 THEN '1.5-3.0%'
                    ELSE '>3.0%'
                END AS distance_bucket,
                COUNT(*) AS observations,
                COALESCE(SUM(CASE WHEN best_exec_edge > 0 THEN 1 ELSE 0 END), 0) AS positive_best_count,
                COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS resolved_count,
                ROUND(AVG(CASE WHEN resolved_at IS NOT NULL THEN best_side_correct END), 4) AS resolved_win_rate,
                ROUND(COALESCE(SUM(CASE WHEN resolved_at IS NOT NULL THEN COALESCE(best_side_realized_pnl, 0) ELSE 0 END), 0), 4) AS realized_pnl
            FROM market_opportunities
            GROUP BY distance_bucket
            ORDER BY
                CASE distance_bucket
                    WHEN '0-0.5%' THEN 1
                    WHEN '0.5-1.5%' THEN 2
                    WHEN '1.5-3.0%' THEN 3
                    WHEN '>3.0%' THEN 4
                    ELSE 5
                END
            """
        ).fetchall()
    ]

    recent_positive = [
        _row_dict(r)
        for r in conn.execute(
            """
            SELECT
                ts, ticker, best_side, best_exec_edge, fair_yes,
                yes_ask, no_ask, spread, distance_from_spot_pct,
                hours_to_expiry, market_result, best_side_correct,
                best_side_realized_pnl
            FROM market_opportunities
            WHERE best_exec_edge > 0
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit_recent),),
        ).fetchall()
    ]

    unresolved_expired = _row_dict(
        conn.execute(
            """
            SELECT COUNT(*) AS n
            FROM market_opportunities
            WHERE resolved_at IS NULL
              AND close_time IS NOT NULL
              AND close_time <= datetime('now')
            """
        ).fetchone()
    )

    conn.close()

    return {
        "totals": totals,
        "by_best_side": by_best_side,
        "by_distance_bucket": by_distance_bucket,
        "recent_positive": recent_positive,
        "unresolved_expired": int(unresolved_expired.get("n") or 0),
    }


def export_market_opportunities_csv(
    limit: int = 500,
    db_path: Path | str = DB_PATH,
) -> str:
    ensure_opportunity_schema(db_path)
    conn = _conn(db_path)
    rows = conn.execute(
        """
        SELECT *
        FROM market_opportunities
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    conn.close()

    rows = [_row_dict(r) for r in rows]

    fieldnames = [
        "id",
        "batch_ts",
        "ts",
        "ticker",
        "asset",
        "market_family",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "mid",
        "spread",
        "spot",
        "fair_yes",
        "execution_edge_yes",
        "execution_edge_no",
        "best_side",
        "best_exec_edge",
        "distance_from_spot_pct",
        "hours_to_expiry",
        "floor_strike",
        "cap_strike",
        "threshold",
        "close_time",
        "market_result",
        "result_yes",
        "pnl_if_yes",
        "pnl_if_no",
        "best_side_correct",
        "best_side_realized_pnl",
        "resolved_at",
        "rationale",
        "extra_json",
    ]

    sio = io.StringIO()
    writer = csv.DictWriter(sio, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k) for k in fieldnames})
    return sio.getvalue()
