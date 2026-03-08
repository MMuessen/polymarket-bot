from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path("data/strategy_lab.db")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "null"):
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v in (None, "", "null"):
            return default
        return int(v)
    except Exception:
        return default


def _row_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if hasattr(v, "keys"):
        try:
            return {k: v[k] for k in v.keys()}
        except Exception:
            pass
    try:
        return dict(v)
    except Exception:
        return {}


def _parse_dt(v: Any) -> Optional[datetime]:
    try:
        if not v:
            return None
        s = str(v).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
    except Exception:
        return []


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _twap_for_window(conn: sqlite3.Connection, symbol: str, end_dt: datetime, seconds: int = 60):
    start_dt = end_dt - timedelta(seconds=seconds)
    rows = conn.execute(
        """
        SELECT spot FROM spot_samples
        WHERE symbol=? AND ts >= ? AND ts <= ?
        ORDER BY ts ASC
        """,
        (symbol, start_dt.isoformat(), end_dt.isoformat()),
    ).fetchall()
    vals = [_safe_float(r["spot"], 0.0) for r in rows]
    vals = [v for v in vals if v > 0]
    if not vals:
        return None, 0
    return round(sum(vals) / len(vals), 4), len(vals)


def ensure_execution_compare_resolution_columns(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "execution_compare_cohorts"):
        return

    cols = set(_table_columns(conn, "execution_compare_cohorts"))
    needed = {
        "resolved_at": "TEXT",
        "settlement_spot_twap_60s": "REAL",
        "settlement_proxy_points": "INTEGER",
        "market_result": "TEXT",
        "maker_side_correct": "INTEGER",
        "taker_side_correct": "INTEGER",
        "maker_realized_pnl": "REAL",
        "taker_realized_pnl": "REAL",
        "outcome_source": "TEXT",
    }
    for col, typ in needed.items():
        if col not in cols:
            conn.execute(f"ALTER TABLE execution_compare_cohorts ADD COLUMN {col} {typ}")
    conn.commit()


def resolve_execution_compare_cohorts(limit: int = 1000) -> Dict[str, Any]:
    conn = get_conn()
    try:
        if not _table_exists(conn, "execution_compare_cohorts"):
            return {
                "ok": False,
                "reason": "execution_compare_cohorts_missing",
                "seen": 0,
                "resolved": 0,
                "skipped_no_twap": 0,
            }

        ensure_execution_compare_resolution_columns(conn)
        cols = set(_table_columns(conn, "execution_compare_cohorts"))

        required = {
            "id", "ticker", "close_time",
            "maker_best_side", "maker_best_edge",
            "taker_best_side", "taker_best_edge",
            "yes_ask", "no_ask", "yes_bid", "no_bid",
            "floor_strike", "cap_strike",
        }
        if not required.issubset(cols):
            return {
                "ok": False,
                "reason": "execution_compare_cohorts_missing_required_columns",
                "seen": 0,
                "resolved": 0,
                "skipped_no_twap": 0,
            }

        rows = conn.execute(
            """
            SELECT *
            FROM execution_compare_cohorts
            WHERE close_time IS NOT NULL
              AND close_time <= ?
              AND (resolved_at IS NULL OR resolved_at = '')
            ORDER BY close_time ASC
            LIMIT ?
            """,
            (_now_iso(), int(limit)),
        ).fetchall()

        seen = 0
        resolved = 0
        skipped_no_twap = 0

        for raw in rows:
            row = _row_dict(raw)
            seen += 1

            close_dt = _parse_dt(row.get("close_time"))
            if close_dt is None:
                continue

            twap, pts = _twap_for_window(conn, "BTC", close_dt, 60)
            if twap is None or pts < 3:
                skipped_no_twap += 1
                continue

            low = _safe_float(row.get("floor_strike"), 0.0)
            high = _safe_float(row.get("cap_strike"), 0.0)
            result_yes = 1 if (low <= twap <= high) else 0
            market_result = "yes" if result_yes == 1 else "no"

            maker_side = str(row.get("maker_best_side") or "").lower().strip()
            taker_side = str(row.get("taker_best_side") or "").lower().strip()

            maker_price = None
            taker_price = None

            if maker_side == "yes":
                maker_price = _safe_float(row.get("yes_bid"), 0.0)
            elif maker_side == "no":
                maker_price = _safe_float(row.get("no_bid"), 0.0)

            if taker_side == "yes":
                taker_price = _safe_float(row.get("yes_ask"), 0.0)
            elif taker_side == "no":
                taker_price = _safe_float(row.get("no_ask"), 0.0)

            maker_side_correct = None
            taker_side_correct = None
            maker_realized_pnl = None
            taker_realized_pnl = None

            if maker_side in {"yes", "no"} and maker_price > 0:
                maker_side_correct = 1 if maker_side == market_result else 0
                maker_realized_pnl = round((1.0 - maker_price) if maker_side_correct else (-maker_price), 4)

            if taker_side in {"yes", "no"} and taker_price > 0:
                taker_side_correct = 1 if taker_side == market_result else 0
                taker_realized_pnl = round((1.0 - taker_price) if taker_side_correct else (-taker_price), 4)

            conn.execute(
                """
                UPDATE execution_compare_cohorts
                SET resolved_at=?,
                    settlement_spot_twap_60s=?,
                    settlement_proxy_points=?,
                    market_result=?,
                    maker_side_correct=?,
                    taker_side_correct=?,
                    maker_realized_pnl=?,
                    taker_realized_pnl=?,
                    outcome_source=?
                WHERE id=?
                """,
                (
                    _now_iso(),
                    twap,
                    int(pts),
                    market_result,
                    maker_side_correct,
                    taker_side_correct,
                    maker_realized_pnl,
                    taker_realized_pnl,
                    "rolling_60s_twap_proxy",
                    int(row["id"]),
                ),
            )
            resolved += 1

        conn.commit()

        resolved_total = conn.execute(
            """
            SELECT COUNT(*) FROM execution_compare_cohorts
            WHERE resolved_at IS NOT NULL AND resolved_at != ''
            """
        ).fetchone()[0]

        pending_total = conn.execute(
            """
            SELECT COUNT(*) FROM execution_compare_cohorts
            WHERE close_time IS NOT NULL
              AND close_time <= ?
              AND (resolved_at IS NULL OR resolved_at = '')
            """,
            (_now_iso(),),
        ).fetchone()[0]

        return {
            "ok": True,
            "seen": int(seen),
            "resolved": int(resolved),
            "skipped_no_twap": int(skipped_no_twap),
            "resolved_total": int(resolved_total),
            "pending_expired_unresolved": int(pending_total),
        }
    finally:
        conn.close()


def build_lookup(limit_rows: int = 50000) -> Dict[str, Any]:
    conn = get_conn()
    try:
        if not _table_exists(conn, "execution_compare_cohorts"):
            return {
                "source_table": None,
                "row_count_used": 0,
                "maker_lookup": [],
                "taker_lookup": [],
            }

        cols = set(_table_columns(conn, "execution_compare_cohorts"))
        needed = {
            "distance_bucket", "hours_to_expiry",
            "maker_best_side", "taker_best_side",
            "maker_side_correct", "taker_side_correct",
            "maker_realized_pnl", "taker_realized_pnl",
            "resolved_at",
        }
        if not needed.issubset(cols):
            return {
                "source_table": "execution_compare_cohorts",
                "row_count_used": 0,
                "maker_lookup": [],
                "taker_lookup": [],
            }

        rows = conn.execute(
            """
            SELECT *
            FROM execution_compare_cohorts
            WHERE resolved_at IS NOT NULL AND resolved_at != ''
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit_rows),),
        ).fetchall()
    finally:
        conn.close()

    def hours_bucket(v: Any) -> str:
        h = _safe_float(v, -1.0)
        if h < 0:
            return "unknown"
        if h < 2:
            return "lt_2h"
        if h < 6:
            return "2_6h"
        if h < 12:
            return "6_12h"
        if h < 24:
            return "12_24h"
        return "24h_plus"

    maker_agg: Dict[tuple, Dict[str, Any]] = {}
    taker_agg: Dict[tuple, Dict[str, Any]] = {}

    for raw in rows:
        row = _row_dict(raw)
        dist = str(row.get("distance_bucket") or "unknown")
        hrs = hours_bucket(row.get("hours_to_expiry"))

        maker_side = str(row.get("maker_best_side") or "").lower().strip()
        if maker_side in {"yes", "no"}:
            key = (dist, maker_side, hrs)
            b = maker_agg.setdefault(key, {"trades": 0, "wins": 0, "losses": 0, "flat": 0, "pnl": 0.0})
            b["trades"] += 1
            corr = row.get("maker_side_correct")
            pnl = _safe_float(row.get("maker_realized_pnl"), 0.0)
            if corr in (1, True, "1"):
                b["wins"] += 1
            elif corr in (0, False, "0"):
                b["losses"] += 1
            else:
                b["flat"] += 1
            b["pnl"] += pnl

        taker_side = str(row.get("taker_best_side") or "").lower().strip()
        if taker_side in {"yes", "no"}:
            key = (dist, taker_side, hrs)
            b = taker_agg.setdefault(key, {"trades": 0, "wins": 0, "losses": 0, "flat": 0, "pnl": 0.0})
            b["trades"] += 1
            corr = row.get("taker_side_correct")
            pnl = _safe_float(row.get("taker_realized_pnl"), 0.0)
            if corr in (1, True, "1"):
                b["wins"] += 1
            elif corr in (0, False, "0"):
                b["losses"] += 1
            else:
                b["flat"] += 1
            b["pnl"] += pnl

    def finalize(agg: Dict[tuple, Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for (dist, side, hrs), v in sorted(agg.items(), key=lambda kv: (-kv[1]["trades"], kv[0][0], kv[0][1], kv[0][2])):
            trades = int(v["trades"])
            pnl = round(float(v["pnl"]), 4)
            out.append({
                "distance_bucket": dist,
                "best_side": side,
                "hours_bucket": hrs,
                "trades": trades,
                "wins": int(v["wins"]),
                "losses": int(v["losses"]),
                "flat": int(v["flat"]),
                "win_rate": round((int(v["wins"]) / trades), 4) if trades else 0.0,
                "realized_pnl": pnl,
                "avg_pnl_per_trade": round((pnl / trades), 4) if trades else 0.0,
            })
        return out

    return {
        "source_table": "execution_compare_cohorts",
        "row_count_used": len(rows),
        "maker_lookup": finalize(maker_agg)[:25],
        "taker_lookup": finalize(taker_agg)[:25],
    }


def build_surface() -> Dict[str, Any]:
    conn = get_conn()
    try:
        total_rows = conn.execute(
            "SELECT COUNT(*) FROM execution_compare_cohorts"
        ).fetchone()[0] if _table_exists(conn, "execution_compare_cohorts") else 0

        resolved_rows = conn.execute(
            """
            SELECT COUNT(*) FROM execution_compare_cohorts
            WHERE resolved_at IS NOT NULL AND resolved_at != ''
            """
        ).fetchone()[0] if _table_exists(conn, "execution_compare_cohorts") else 0
    finally:
        conn.close()

    lookup = build_lookup()

    return {
        "resolved_execution_compare": {
            "total_rows": int(total_rows),
            "resolved_rows": int(resolved_rows),
            "lookup_source": lookup.get("source_table"),
            "lookup_rows_used": int(lookup.get("row_count_used") or 0),
        },
        "historical_lookup": lookup,
        "architect_next_step": (
            "once_resolved_rows_are_nonzero_compare_maker_vs_taker_by_distance_side_hours_"
            "before_any_threshold_or_sizing_change"
        ),
    }


def plaintext(status: Dict[str, Any]) -> str:
    surf = _row_dict(status.get("architect_cohort_resolution") or {})
    res = _row_dict(surf.get("resolved_execution_compare") or {})
    lookup = _row_dict(surf.get("historical_lookup") or {})

    lines = []
    lines.append(f"mode: {status.get('mode')}")
    lines.append(f"live_armed: {status.get('live_armed')}")
    lines.append(f"loop_error: {(_row_dict(status.get('health') or {})).get('loop_error')}")
    lines.append("")
    lines.append("resolved_execution_compare:")
    lines.append(f"  total_rows: {res.get('total_rows')}")
    lines.append(f"  resolved_rows: {res.get('resolved_rows')}")
    lines.append(f"  lookup_source: {res.get('lookup_source')}")
    lines.append(f"  lookup_rows_used: {res.get('lookup_rows_used')}")
    lines.append("")
    lines.append("maker_lookup_top:")
    for row in (lookup.get("maker_lookup") or [])[:8]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('best_side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} pnl={row.get('realized_pnl')}"
        )
    lines.append("")
    lines.append("taker_lookup_top:")
    for row in (lookup.get("taker_lookup") or [])[:8]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('best_side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} pnl={row.get('realized_pnl')}"
        )
    return "\n".join(lines)


# _ARCHITECT_COHORT_BACKFILL_V1
def _ensure_column(conn: sqlite3.Connection, table_name: str, col: str, typ: str) -> None:
    cols = set(_table_columns(conn, table_name))
    if col not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {typ}")


def ensure_execution_compare_schema_and_backfill(conn: sqlite3.Connection) -> Dict[str, Any]:
    if not _table_exists(conn, "execution_compare_cohorts"):
        return {"ok": False, "reason": "execution_compare_cohorts_missing"}

    added = []
    for col, typ in [
        ("batch_ts", "TEXT"),
        ("ticker", "TEXT"),
        ("distance_bucket", "TEXT"),
        ("hours_to_expiry", "REAL"),
        ("close_time", "TEXT"),
        ("yes_bid", "REAL"),
        ("yes_ask", "REAL"),
        ("no_bid", "REAL"),
        ("no_ask", "REAL"),
        ("floor_strike", "REAL"),
        ("cap_strike", "REAL"),
        ("maker_best_side", "TEXT"),
        ("taker_best_side", "TEXT"),
        ("resolved_at", "TEXT"),
        ("settlement_spot_twap_60s", "REAL"),
        ("settlement_proxy_points", "INTEGER"),
        ("market_result", "TEXT"),
        ("maker_side_correct", "INTEGER"),
        ("taker_side_correct", "INTEGER"),
        ("maker_realized_pnl", "REAL"),
        ("taker_realized_pnl", "REAL"),
        ("outcome_source", "TEXT"),
    ]:
        before = set(_table_columns(conn, "execution_compare_cohorts"))
        _ensure_column(conn, "execution_compare_cohorts", col, typ)
        after = set(_table_columns(conn, "execution_compare_cohorts"))
        if col in after and col not in before:
            added.append(col)

    backfilled = 0
    opportunities_available = _table_exists(conn, "market_opportunities")
    if opportunities_available:
        cohort_cols = set(_table_columns(conn, "execution_compare_cohorts"))
        opp_cols = set(_table_columns(conn, "market_opportunities"))

        need_rows = conn.execute(
            """
            SELECT id, ticker, batch_ts
            FROM execution_compare_cohorts
            WHERE
                close_time IS NULL
                OR yes_ask IS NULL
                OR no_ask IS NULL
                OR maker_best_side IS NULL
                OR taker_best_side IS NULL
                OR distance_bucket IS NULL
                OR hours_to_expiry IS NULL
            ORDER BY id DESC
            LIMIT 5000
            """
        ).fetchall()

        for raw in need_rows:
            row = _row_dict(raw)
            ticker = str(row.get("ticker") or "").strip()
            batch_ts = str(row.get("batch_ts") or "").strip()

            if not ticker:
                continue

            # Prefer nearest batch_ts match when available, otherwise latest ticker row.
            opp = None
            if "batch_ts" in opp_cols and batch_ts:
                opp = conn.execute(
                    """
                    SELECT *
                    FROM market_opportunities
                    WHERE ticker = ?
                    ORDER BY ABS(julianday(COALESCE(batch_ts, ts)) - julianday(?)) ASC, id DESC
                    LIMIT 1
                    """,
                    (ticker, batch_ts),
                ).fetchone()

            if opp is None:
                opp = conn.execute(
                    """
                    SELECT *
                    FROM market_opportunities
                    WHERE ticker = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (ticker,),
                ).fetchone()

            if opp is None:
                continue

            d = _row_dict(opp)

            maker_best_side = d.get("best_side")
            taker_best_side = d.get("best_side")

            values = {
                "close_time": d.get("close_time"),
                "yes_bid": d.get("yes_bid"),
                "yes_ask": d.get("yes_ask"),
                "no_bid": d.get("no_bid"),
                "no_ask": d.get("no_ask"),
                "floor_strike": d.get("floor_strike"),
                "cap_strike": d.get("cap_strike"),
                "distance_bucket": d.get("distance_bucket"),
                "hours_to_expiry": d.get("hours_to_expiry"),
                "maker_best_side": maker_best_side,
                "taker_best_side": taker_best_side,
            }

            set_parts = []
            params = []
            for k, v in values.items():
                if k in cohort_cols:
                    set_parts.append(f"{k} = COALESCE({k}, ?)")
                    params.append(v)

            if not set_parts:
                continue

            params.append(int(row["id"]))
            conn.execute(
                f"""
                UPDATE execution_compare_cohorts
                SET {", ".join(set_parts)}
                WHERE id = ?
                """,
                params,
            )
            backfilled += 1

    conn.commit()
    return {
        "ok": True,
        "added_columns": added,
        "backfilled_rows": backfilled,
        "opportunities_available": opportunities_available,
    }


_prev_ensure_execution_compare_resolution_columns = ensure_execution_compare_resolution_columns
def ensure_execution_compare_resolution_columns(conn: sqlite3.Connection) -> None:
    _prev_ensure_execution_compare_resolution_columns(conn)
    ensure_execution_compare_schema_and_backfill(conn)


_prev_build_surface = build_surface
def build_surface() -> Dict[str, Any]:
    conn = get_conn()
    try:
        schema_memo = ensure_execution_compare_schema_and_backfill(conn)
    finally:
        conn.close()

    surf = _prev_build_surface()
    surf["schema_backfill_runtime"] = schema_memo
    return surf


# _ARCHITECT_MARKET_OPP_LOOKUP_FALLBACK_V1
def _hours_bucket_from_value(v) -> str:
    try:
        h = float(v)
    except Exception:
        return "unknown"
    if h < 0:
        return "expired"
    if h < 2:
        return "lt_2h"
    if h < 6:
        return "2_6h"
    if h < 12:
        return "6_12h"
    if h < 24:
        return "12_24h"
    return "24h_plus"


def _build_market_opportunities_taker_lookup(conn: sqlite3.Connection) -> Dict[str, Any]:
    if not _table_exists(conn, "market_opportunities"):
        return {
            "ok": False,
            "source": "market_opportunities",
            "reason": "market_opportunities_missing",
            "rows_used": 0,
            "lookup": [],
        }

    cols = set(_table_columns(conn, "market_opportunities"))
    required = {
        "distance_bucket",
        "best_side",
        "best_side_correct",
        "best_side_realized_pnl",
        "hours_to_expiry",
        "resolved_at",
    }
    missing = sorted(required - cols)
    if missing:
        return {
            "ok": False,
            "source": "market_opportunities",
            "reason": f"market_opportunities_missing_required_columns:{','.join(missing)}",
            "rows_used": 0,
            "lookup": [],
        }

    rows = conn.execute(
        """
        SELECT
            distance_bucket,
            best_side,
            best_side_correct,
            best_side_realized_pnl,
            hours_to_expiry,
            resolved_at
        FROM market_opportunities
        WHERE resolved_at IS NOT NULL
          AND distance_bucket IS NOT NULL
          AND best_side IS NOT NULL
        ORDER BY id DESC
        LIMIT 50000
        """
    ).fetchall()

    buckets = {}
    used = 0

    for raw in rows:
        row = _row_dict(raw)
        distance_bucket = str(row.get("distance_bucket") or "").strip()
        best_side = str(row.get("best_side") or "").strip().lower()
        hours_bucket = _hours_bucket_from_value(row.get("hours_to_expiry"))

        if not distance_bucket or best_side not in {"yes", "no"}:
            continue

        key = (distance_bucket, best_side, hours_bucket)
        bucket = buckets.setdefault(key, {
            "distance_bucket": distance_bucket,
            "best_side": best_side,
            "hours_bucket": hours_bucket,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "flat": 0,
            "realized_pnl": 0.0,
        })

        used += 1
        bucket["trades"] += 1

        try:
            pnl = float(row.get("best_side_realized_pnl") or 0.0)
        except Exception:
            pnl = 0.0
        bucket["realized_pnl"] += pnl

        correct = row.get("best_side_correct")
        try:
            correct_int = int(correct) if correct not in (None, "", "null") else None
        except Exception:
            correct_int = None

        if abs(pnl) < 1e-12:
            bucket["flat"] += 1
        elif correct_int == 1 or pnl > 0:
            bucket["wins"] += 1
        else:
            bucket["losses"] += 1

    lookup = []
    for _, row in buckets.items():
        trades = int(row["trades"])
        wins = int(row["wins"])
        losses = int(row["losses"])
        flat = int(row["flat"])
        realized_pnl = round(float(row["realized_pnl"]), 4)
        avg = round(realized_pnl / trades, 4) if trades else 0.0
        win_rate = round(wins / trades, 4) if trades else 0.0
        lookup.append({
            "distance_bucket": row["distance_bucket"],
            "best_side": row["best_side"],
            "hours_bucket": row["hours_bucket"],
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "flat": flat,
            "win_rate": win_rate,
            "realized_pnl": realized_pnl,
            "avg_pnl_per_trade": avg,
        })

    lookup.sort(
        key=lambda x: (
            -(x.get("trades") or 0),
            -(x.get("win_rate") or 0.0),
            -(x.get("realized_pnl") or 0.0),
        )
    )

    return {
        "ok": True,
        "source": "market_opportunities",
        "rows_used": used,
        "lookup": lookup[:50],
    }


_prev_build_surface_market_opp_lookup_fallback = build_surface
def build_surface() -> Dict[str, Any]:
    surf = _prev_build_surface_market_opp_lookup_fallback()

    try:
        resolved = surf.get("resolved_execution_compare", {}) or {}
        historical = surf.get("historical_lookup", {}) or {}

        resolved_rows = int(resolved.get("resolved_rows") or 0)
        maker_lookup = list(historical.get("maker_lookup") or [])
        taker_lookup = list(historical.get("taker_lookup") or [])

        need_fallback = resolved_rows <= 0 and len(taker_lookup) == 0

        if need_fallback:
            conn = get_conn()
            try:
                fallback = _build_market_opportunities_taker_lookup(conn)
            finally:
                conn.close()

            surf["historical_lookup"] = {
                "lookup_mode": "market_opportunities_taker_proxy_fallback",
                "maker_lookup": maker_lookup,
                "taker_lookup": list(fallback.get("lookup") or []),
                "empirical_lookup": list(fallback.get("lookup") or []),
                "source": fallback.get("source"),
                "rows_used": int(fallback.get("rows_used") or 0),
                "note": (
                    "Fallback uses resolved market_opportunities best_side outcomes "
                    "as a taker-style empirical replay proxy until execution_compare "
                    "cohorts have resolved rows."
                ),
            }

            if isinstance(surf.get("resolved_execution_compare"), dict):
                surf["resolved_execution_compare"]["lookup_source"] = "market_opportunities_fallback"
                surf["resolved_execution_compare"]["lookup_rows_used"] = int(fallback.get("rows_used") or 0)

            surf["architect_next_step"] = (
                "continue_collecting_resolved_execution_compare_rows_for_true_maker_vs_taker_"
                "while_using_market_opportunities_taker_proxy_for_replay_calibration"
            )
    except Exception as e:
        surf["historical_lookup_fallback_error"] = repr(e)

    return surf

