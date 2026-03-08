from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "strategy_lab.db"


def _row_dict(row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        try:
            return {k: row[k] for k in row.keys()}
        except Exception:
            pass
    try:
        return dict(row)
    except Exception:
        return {}


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


def _distance_bucket_from_pct(v: Any) -> str:
    try:
        if v in (None, "", "null"):
            return "unknown"
        x = abs(float(v))
    except Exception:
        return "unknown"

    if x < 0.005:
        return "atm_0_0.5pct"
    if x < 0.015:
        return "near_0.5_1.5pct"
    if x < 0.03:
        return "mid_1.5_3pct"
    return "far_3pct_plus"


def summarize_market_gates(status: Dict[str, Any]) -> Dict[str, Any]:
    markets = list(status.get("markets") or [])
    strategies = status.get("strategies") or {}
    cfg = strategies.get("crypto_lag") or {}

    min_edge = _safe_float(cfg.get("min_edge"), 0.0)
    max_spread = _safe_float(cfg.get("max_spread"), 999.0)
    min_hours = _safe_float(cfg.get("min_hours_to_expiry"), 0.0)
    max_hours = _safe_float(cfg.get("max_hours_to_expiry"), 9999.0)

    summary: Dict[str, Any] = {
        "strategy_name": "crypto_lag",
        "config": {
            "min_edge": min_edge,
            "max_spread": max_spread,
            "min_hours_to_expiry": min_hours,
            "max_hours_to_expiry": max_hours,
        },
        "visible_rows": len(markets),
        "positive_best_edge_count": 0,
        "above_threshold_count": 0,
        "pass_spread_count": 0,
        "pass_hours_count": 0,
        "actionable_count": 0,
        "count_by_best_side": {},
        "count_by_distance_bucket": {},
        "blocked": {
            "no_best_side": 0,
            "non_positive_edge": 0,
            "below_threshold": 0,
            "spread_too_wide": 0,
            "hours_out_of_range": 0,
        },
        "top_actionable": [],
        "top_blocked_near_threshold": [],
    }

    actionable = []
    near_misses = []

    for row in markets:
        if not isinstance(row, dict):
            continue

        side = str(row.get("best_side") or "").lower().strip() or "unknown"
        edge = row.get("best_exec_edge")
        if edge is None:
            edge = row.get("edge")
        edge = _safe_float(edge, 0.0)

        spread = _safe_float(row.get("spread"), 999.0)
        hours = _safe_float(row.get("hours_to_expiry"), -1.0)

        dist_bucket = row.get("distance_bucket") or _distance_bucket_from_pct(row.get("distance_from_spot_pct"))

        summary["count_by_best_side"][side] = summary["count_by_best_side"].get(side, 0) + 1
        bucket = summary["count_by_distance_bucket"].setdefault(
            dist_bucket,
            {"count": 0, "positive_best_count": 0, "actionable_count": 0},
        )
        bucket["count"] += 1

        if side not in {"yes", "no"}:
            summary["blocked"]["no_best_side"] += 1
            continue

        if edge > 0:
            summary["positive_best_edge_count"] += 1
            bucket["positive_best_count"] += 1
        else:
            summary["blocked"]["non_positive_edge"] += 1

        pass_edge = edge >= min_edge
        pass_spread = spread <= max_spread
        pass_hours = min_hours <= hours <= max_hours

        if pass_edge:
            summary["above_threshold_count"] += 1
        else:
            summary["blocked"]["below_threshold"] += 1

        if pass_spread:
            summary["pass_spread_count"] += 1
        else:
            summary["blocked"]["spread_too_wide"] += 1

        if pass_hours:
            summary["pass_hours_count"] += 1
        else:
            summary["blocked"]["hours_out_of_range"] += 1

        base = {
            "ticker": row.get("ticker"),
            "best_side": side,
            "best_exec_edge": round(edge, 4),
            "spread": round(spread, 4) if spread < 900 else None,
            "hours_to_expiry": round(hours, 4) if hours >= 0 else None,
            "distance_bucket": dist_bucket,
        }

        if pass_edge and pass_spread and pass_hours:
            summary["actionable_count"] += 1
            bucket["actionable_count"] += 1
            actionable.append(base)
        else:
            gap = round(min_edge - edge, 4)
            item = dict(base)
            item["edge_gap_to_threshold"] = gap
            item["blocked_reasons"] = [
                r for r, ok in [
                    ("below_threshold", pass_edge),
                    ("spread_too_wide", pass_spread),
                    ("hours_out_of_range", pass_hours),
                ] if not ok
            ]
            near_misses.append(item)

    actionable.sort(key=lambda x: x.get("best_exec_edge", -999), reverse=True)
    near_misses.sort(key=lambda x: abs(x.get("edge_gap_to_threshold", 999)))

    summary["top_actionable"] = actionable[:10]
    summary["top_blocked_near_threshold"] = near_misses[:10]

    if summary["actionable_count"] > 0:
        summary["gate_state"] = "active_candidates_present"
    elif summary["positive_best_edge_count"] <= 0:
        summary["gate_state"] = "idle_because_no_positive_edge"
    elif summary["above_threshold_count"] <= 0:
        summary["gate_state"] = "idle_because_threshold"
    else:
        summary["gate_state"] = "idle_because_secondary_gates"

    return summary


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def _aggregate_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    trades = len(rows)
    wins = sum(1 for r in rows if _safe_float(r.get("best_side_realized_pnl"), 0.0) > 0)
    losses = sum(1 for r in rows if _safe_float(r.get("best_side_realized_pnl"), 0.0) < 0)
    flat = trades - wins - losses
    pnl = round(sum(_safe_float(r.get("best_side_realized_pnl"), 0.0) for r in rows), 4)
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flat": flat,
        "win_rate": round((wins / trades), 4) if trades else 0.0,
        "realized_pnl": pnl,
        "avg_pnl_per_trade": round((pnl / trades), 4) if trades else 0.0,
    }


def replay_from_opportunities(
    strategy_name: str = "crypto_lag",
    min_edge: float = 0.018,
    max_spread: float = 0.08,
    min_hours: float = 2.0,
    max_hours: float = 24.0,
    limit: int = 20000,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "strategy_name": strategy_name,
        "db_path": str(DB_PATH),
        "config": {
            "min_edge": float(min_edge),
            "max_spread": float(max_spread),
            "min_hours_to_expiry": float(min_hours),
            "max_hours_to_expiry": float(max_hours),
            "limit": int(limit),
        },
    }

    if not DB_PATH.exists():
        result["error"] = "strategy_lab_db_missing"
        return result

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    try:
        if not _table_exists(conn, "market_opportunities"):
            result["error"] = "market_opportunities_missing"
            return result

        raw_rows = conn.execute(
            """
            SELECT *
            FROM market_opportunities
            WHERE best_side_realized_pnl IS NOT NULL
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        rows = [_row_dict(r) for r in raw_rows]
        result["resolved_count_total"] = len(rows)

        if not rows:
            result["note"] = "no_resolved_opportunities_yet"
            result["all_resolved"] = _aggregate_rows([])
            result["filtered_replay"] = _aggregate_rows([])
            result["by_best_side"] = {}
            result["by_distance_bucket"] = {}
            result["threshold_sweep"] = []
            return result

        normalized = []
        for r in rows:
            row = dict(r)
            row["best_side"] = str(row.get("best_side") or "").lower().strip()
            row["best_exec_edge"] = _safe_float(row.get("best_exec_edge"), 0.0)
            row["spread"] = _safe_float(row.get("spread"), 999.0)
            row["hours_to_expiry"] = _safe_float(row.get("hours_to_expiry"), -1.0)
            row["distance_bucket"] = row.get("distance_bucket") or _distance_bucket_from_pct(row.get("distance_from_spot_pct"))
            row["best_side_realized_pnl"] = _safe_float(row.get("best_side_realized_pnl"), 0.0)
            normalized.append(row)

        filtered = [
            r for r in normalized
            if r.get("best_side") in {"yes", "no"}
            and r["best_exec_edge"] >= float(min_edge)
            and r["spread"] <= float(max_spread)
            and float(min_hours) <= r["hours_to_expiry"] <= float(max_hours)
        ]

        by_side = {}
        for side in ("yes", "no"):
            by_side[side] = _aggregate_rows([r for r in filtered if r.get("best_side") == side])

        by_dist = {}
        for bucket in sorted(set(r["distance_bucket"] for r in filtered)):
            by_dist[bucket] = _aggregate_rows([r for r in filtered if r["distance_bucket"] == bucket])

        sweep = []
        for edge_cut in [0.0, 0.005, 0.01, 0.015, 0.018, 0.02, 0.025, 0.03]:
            cohort = [
                r for r in normalized
                if r.get("best_side") in {"yes", "no"}
                and r["best_exec_edge"] >= edge_cut
                and r["spread"] <= float(max_spread)
                and float(min_hours) <= r["hours_to_expiry"] <= float(max_hours)
            ]
            s = _aggregate_rows(cohort)
            s["min_edge"] = round(edge_cut, 4)
            sweep.append(s)

        result["all_resolved"] = _aggregate_rows(normalized)
        result["filtered_replay"] = _aggregate_rows(filtered)
        result["by_best_side"] = by_side
        result["by_distance_bucket"] = by_dist
        result["threshold_sweep"] = sweep
        result["sample_filtered"] = [
            {
                "ticker": r.get("ticker"),
                "best_side": r.get("best_side"),
                "best_exec_edge": round(_safe_float(r.get("best_exec_edge"), 0.0), 4),
                "distance_bucket": r.get("distance_bucket"),
                "spread": round(_safe_float(r.get("spread"), 0.0), 4),
                "hours_to_expiry": round(_safe_float(r.get("hours_to_expiry"), 0.0), 4),
                "best_side_realized_pnl": round(_safe_float(r.get("best_side_realized_pnl"), 0.0), 4),
            }
            for r in filtered[:20]
        ]
        return result
    finally:
        conn.close()
