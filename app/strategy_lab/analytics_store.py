from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

DB_PATH = Path("data/strategy_lab.db")


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _row_dict(row: Any) -> dict:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            try:
                return dict(row)
            except Exception:
                pass
    try:
        return dict(row)
    except Exception:
        return {}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def ensure_trade_analytics_table() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_analytics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table TEXT NOT NULL,
            source_rowid INTEGER NOT NULL,
            strategy_name TEXT,
            variant_name TEXT,
            ticker TEXT,
            side TEXT,
            contracts REAL,
            entry_price REAL,
            exit_price REAL,
            realized_pnl REAL,
            close_reason TEXT,
            outcome TEXT,
            opened_at TEXT,
            closed_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_table, source_rowid)
        )
        """
    )
    conn.commit()
    conn.close()


def _pick_expr(cols: set[str], candidates: list[str], alias: str) -> str:
    for c in candidates:
        if c in cols:
            return f"{c} AS {alias}"
    return f"NULL AS {alias}"


def backfill_from_shadow_trades(strategy_name: str | None = None) -> dict:
    ensure_trade_analytics_table()
    conn = _connect()

    if not _table_exists(conn, "shadow_trades"):
        conn.close()
        return {
            "ok": False,
            "source_table": "shadow_trades",
            "reason": "missing_table",
            "inserted": 0,
            "seen": 0,
        }

    cols = _table_columns(conn, "shadow_trades")

    query = f"""
        SELECT
            rowid AS source_rowid,
            {_pick_expr(cols, ['strategy_name', 'strategy'], 'strategy_name')},
            {_pick_expr(cols, ['variant_name', 'variant'], 'variant_name')},
            {_pick_expr(cols, ['ticker'], 'ticker')},
            {_pick_expr(cols, ['side'], 'side')},
            {_pick_expr(cols, ['contracts', 'qty', 'quantity'], 'contracts')},
            {_pick_expr(cols, ['entry_price', 'requested_price', 'price'], 'entry_price')},
            {_pick_expr(cols, ['exit_price', 'mark_price'], 'exit_price')},
            {_pick_expr(cols, ['realized_pnl', 'pnl', 'profit_loss'], 'realized_pnl')},
            {_pick_expr(cols, ['close_reason'], 'close_reason')},
            {_pick_expr(cols, ['outcome', 'result'], 'outcome')},
            {_pick_expr(cols, ['opened_at', 'created_at', 'ts'], 'opened_at')},
            {_pick_expr(cols, ['closed_at', 'settled_at', 'resolved_at', 'updated_at'], 'closed_at')}
        FROM shadow_trades
        ORDER BY rowid ASC
    """

    rows = conn.execute(query).fetchall()
    seen = 0
    inserted = 0

    for raw in rows:
        row = _row_dict(raw)
        seen += 1

        strat = _row_dict(row).get("strategy_name")
        if strategy_name and strat != strategy_name:
            continue

        realized_pnl = float(_row_dict(row).get("realized_pnl") or 0.0)
        close_reason = _row_dict(row).get("close_reason")
        outcome = _row_dict(row).get("outcome")
        closed_at = _row_dict(row).get("closed_at")

        looks_closed = any(
            x not in (None, "", "null")
            for x in [closed_at, close_reason, outcome]
        ) or abs(realized_pnl) > 1e-12

        if not looks_closed:
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO trade_analytics (
                source_table,
                source_rowid,
                strategy_name,
                variant_name,
                ticker,
                side,
                contracts,
                entry_price,
                exit_price,
                realized_pnl,
                close_reason,
                outcome,
                opened_at,
                closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "shadow_trades",
                int(_row_dict(row).get("source_rowid")),
                _row_dict(row).get("strategy_name"),
                _row_dict(row).get("variant_name"),
                _row_dict(row).get("ticker"),
                str(_row_dict(row).get("side") or "").lower() or None,
                _row_dict(row).get("contracts"),
                _row_dict(row).get("entry_price"),
                _row_dict(row).get("exit_price"),
                realized_pnl,
                _row_dict(row).get("close_reason"),
                _row_dict(row).get("outcome"),
                _row_dict(row).get("opened_at"),
                _row_dict(row).get("closed_at"),
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "source_table": "shadow_trades",
        "seen": seen,
        "inserted": inserted,
    }


def _summarize(rows: list[dict]) -> dict:
    trades = len(rows)
    wins = sum(1 for r in rows if float(r.get("realized_pnl") or 0.0) > 0)
    losses = sum(1 for r in rows if float(r.get("realized_pnl") or 0.0) < 0)
    flat = trades - wins - losses
    realized = round(sum(float(r.get("realized_pnl") or 0.0) for r in rows), 4)
    avg = round((realized / trades), 4) if trades else 0.0
    win_rate = round((wins / trades), 4) if trades else 0.0
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "win_rate": win_rate,
        "realized_pnl": realized,
        "avg_pnl_per_trade": avg,
    }


def get_trade_analytics_summary(strategy_name: str = "crypto_lag") -> dict:
    ensure_trade_analytics_table()
    conn = _connect()

    rows = [
        _row_dict(r)
        for r in conn.execute(
            """
            SELECT *
            FROM trade_analytics
            WHERE strategy_name = ?
            ORDER BY COALESCE(closed_at, opened_at, created_at) DESC, id DESC
            """,
            (strategy_name,),
        ).fetchall()
    ]
    conn.close()

    by_side = defaultdict(list)
    by_variant = defaultdict(list)
    by_close_reason = defaultdict(list)

    for r in rows:
        side = str(r.get("side") or "").lower().strip() or "unknown"
        variant = str(r.get("variant_name") or "").strip() or "unknown"
        reason = str(r.get("close_reason") or "").strip() or "unknown"

        by_side[side].append(r)
        by_variant[variant].append(r)
        by_close_reason[reason].append(r)

    return {
        "strategy_name": strategy_name,
        "totals": _summarize(rows),
        "by_side": {k: _summarize(v) for k, v in sorted(by_side.items())},
        "by_variant": {k: _summarize(v) for k, v in sorted(by_variant.items())},
        "by_close_reason": {k: _summarize(v) for k, v in sorted(by_close_reason.items())},
        "row_count": len(rows),
    }


# --- dict/object field helper ---
def _field(obj, name, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    try:
        return getattr(obj, name)
    except Exception:
        pass
    try:
        return obj[name]
    except Exception:
        return default

