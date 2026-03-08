from __future__ import annotations

import math
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

DB_PATH = Path("data/strategy_lab.db")


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


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _hours_bucket(h: Any) -> str:
    x = _safe_float(h, -1.0)
    if x < 0:
        return "unknown"
    if x < 2:
        return "lt_2h"
    if x < 6:
        return "2_6h"
    if x < 12:
        return "6_12h"
    if x < 24:
        return "12_24h"
    return "24h_plus"


def _score_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    out = []
    for row in rows:
        trades = _safe_int(row.get("trades"), 0)
        wins = _safe_int(row.get("wins"), 0)
        pnl = _safe_float(row.get("realized_pnl"), 0.0)
        avg = round((pnl / trades), 4) if trades else 0.0
        win_rate = round((wins / trades), 4) if trades else 0.0
        expectancy = avg
        out.append({
            **row,
            "trades": trades,
            "wins": wins,
            "win_rate": win_rate,
            "realized_pnl": round(pnl, 4),
            "avg_pnl_per_trade": avg,
            "expectancy": expectancy,
        })
    out.sort(
        key=lambda r: (
            -(r.get("expectancy") or 0.0),
            -(r.get("win_rate") or 0.0),
            -(r.get("trades") or 0),
        )
    )
    return {
        "rows": out,
        "top": out[:20],
    }


def build_replay_scoring_surface() -> Dict[str, Any]:
    surf: Dict[str, Any] = {
        "ok": True,
        "lookup_mode": "none",
        "rows_used": 0,
        "maker_lookup_top": [],
        "taker_lookup_top": [],
        "regime_lookup_top": [],
        "tail_overlay_replay": {
            "mode": "empirical_overlay_surface_only",
            "rows_used": 0,
            "top": [],
        },
        "architect_next_step": "wait_for_resolved_rows",
    }

    if not DB_PATH.exists():
        surf["ok"] = False
        surf["reason"] = "db_missing"
        return surf

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        source_rows: List[Dict[str, Any]] = []
        lookup_mode = "none"

        if _table_exists(conn, "execution_compare_cohorts"):
            cols = {r[1] for r in conn.execute("PRAGMA table_info(execution_compare_cohorts)").fetchall()}
            needed = {"distance_bucket", "hours_bucket", "maker_side", "taker_side", "maker_pnl", "taker_pnl", "resolved_at"}
            if needed.issubset(cols):
                rows = conn.execute(
                    """
                    SELECT *
                    FROM execution_compare_cohorts
                    WHERE resolved_at IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 50000
                    """
                ).fetchall()
                if rows:
                    lookup_mode = "execution_compare_resolved"
                    source_rows = [_row_dict(r) for r in rows]

        if not source_rows and _table_exists(conn, "market_opportunities"):
            cols = {r[1] for r in conn.execute("PRAGMA table_info(market_opportunities)").fetchall()}
            needed = {"distance_bucket", "best_side", "best_side_realized_pnl", "best_side_correct", "hours_to_expiry", "resolved_at"}
            if needed.issubset(cols):
                rows = conn.execute(
                    """
                    SELECT *
                    FROM market_opportunities
                    WHERE resolved_at IS NOT NULL
                    ORDER BY id DESC
                    LIMIT 50000
                    """
                ).fetchall()
                if rows:
                    lookup_mode = "market_opportunities_fallback"
                    source_rows = [_row_dict(r) for r in rows]

        surf["lookup_mode"] = lookup_mode
        surf["rows_used"] = len(source_rows)

        if not source_rows:
            surf["architect_next_step"] = "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            return surf

        maker_buckets: Dict[tuple, Dict[str, Any]] = {}
        taker_buckets: Dict[tuple, Dict[str, Any]] = {}
        regime_buckets: Dict[tuple, Dict[str, Any]] = {}
        tail_rows: List[Dict[str, Any]] = []

        for row in source_rows:
            distance_bucket = str(row.get("distance_bucket") or "unknown")
            regime = str(row.get("regime") or row.get("vol_regime") or "unknown")
            hours_bucket = str(row.get("hours_bucket") or _hours_bucket(row.get("hours_to_expiry")))

            if lookup_mode == "execution_compare_resolved":
                maker_side = str(row.get("maker_side") or "").lower()
                taker_side = str(row.get("taker_side") or "").lower()
                maker_pnl = _safe_float(row.get("maker_pnl"), 0.0)
                taker_pnl = _safe_float(row.get("taker_pnl"), 0.0)

                if maker_side in {"yes", "no"}:
                    key = (distance_bucket, maker_side, hours_bucket)
                    b = maker_buckets.setdefault(key, {
                        "distance_bucket": distance_bucket,
                        "side": maker_side,
                        "hours_bucket": hours_bucket,
                        "trades": 0,
                        "wins": 0,
                        "realized_pnl": 0.0,
                    })
                    b["trades"] += 1
                    b["wins"] += 1 if maker_pnl > 0 else 0
                    b["realized_pnl"] += maker_pnl

                if taker_side in {"yes", "no"}:
                    key = (distance_bucket, taker_side, hours_bucket)
                    b = taker_buckets.setdefault(key, {
                        "distance_bucket": distance_bucket,
                        "side": taker_side,
                        "hours_bucket": hours_bucket,
                        "trades": 0,
                        "wins": 0,
                        "realized_pnl": 0.0,
                    })
                    b["trades"] += 1
                    b["wins"] += 1 if taker_pnl > 0 else 0
                    b["realized_pnl"] += taker_pnl

                key = (distance_bucket, regime, hours_bucket)
                rb = regime_buckets.setdefault(key, {
                    "distance_bucket": distance_bucket,
                    "regime": regime,
                    "hours_bucket": hours_bucket,
                    "trades": 0,
                    "wins": 0,
                    "realized_pnl": 0.0,
                })
                best = maker_pnl if maker_pnl >= taker_pnl else taker_pnl
                rb["trades"] += 1
                rb["wins"] += 1 if best > 0 else 0
                rb["realized_pnl"] += best

            else:
                side = str(row.get("best_side") or "").lower()
                pnl = _safe_float(row.get("best_side_realized_pnl"), 0.0)
                correct = _safe_int(row.get("best_side_correct"), 1 if pnl > 0 else 0)

                if side in {"yes", "no"}:
                    key = (distance_bucket, side, hours_bucket)
                    b = taker_buckets.setdefault(key, {
                        "distance_bucket": distance_bucket,
                        "side": side,
                        "hours_bucket": hours_bucket,
                        "trades": 0,
                        "wins": 0,
                        "realized_pnl": 0.0,
                    })
                    b["trades"] += 1
                    b["wins"] += 1 if correct == 1 or pnl > 0 else 0
                    b["realized_pnl"] += pnl

                key = (distance_bucket, regime, hours_bucket)
                rb = regime_buckets.setdefault(key, {
                    "distance_bucket": distance_bucket,
                    "regime": regime,
                    "hours_bucket": hours_bucket,
                    "trades": 0,
                    "wins": 0,
                    "realized_pnl": 0.0,
                })
                rb["trades"] += 1
                rb["wins"] += 1 if correct == 1 or pnl > 0 else 0
                rb["realized_pnl"] += pnl

                tail_mult = _safe_float(row.get("tail_multiplier"), 1.0)
                fair_yes = _safe_float(row.get("fair_yes"), 0.0)
                tail_fair_yes = _safe_float(row.get("tail_adjusted_fair_yes"), fair_yes * tail_mult if fair_yes else 0.0)
                tail_rows.append({
                    "distance_bucket": distance_bucket,
                    "side": side,
                    "hours_bucket": hours_bucket,
                    "tail_multiplier": round(tail_mult, 4),
                    "fair_yes": round(fair_yes, 4),
                    "tail_adjusted_fair_yes": round(tail_fair_yes, 4),
                    "realized_pnl": round(pnl, 4),
                })

        maker_scored = _score_rows(list(maker_buckets.values()))
        taker_scored = _score_rows(list(taker_buckets.values()))
        regime_scored = _score_rows(list(regime_buckets.values()))

        tail_rows.sort(
            key=lambda r: (
                -(abs((r.get("tail_adjusted_fair_yes") or 0.0) - (r.get("fair_yes") or 0.0))),
                -(r.get("realized_pnl") or 0.0),
            )
        )

        surf["maker_lookup_top"] = maker_scored["top"]
        surf["taker_lookup_top"] = taker_scored["top"]
        surf["regime_lookup_top"] = regime_scored["top"]
        surf["tail_overlay_replay"] = {
            "mode": "empirical_overlay_surface_only",
            "rows_used": len(tail_rows),
            "top": tail_rows[:20],
        }

        if lookup_mode == "execution_compare_resolved":
            surf["architect_next_step"] = "compare_maker_vs_taker_expectancy_by_distance_side_hours_before_any_threshold_change"
        else:
            surf["architect_next_step"] = "use_fallback_taker_expectancy_for_replay_only_while_waiting_for_true_maker_resolution"

        return surf
    finally:
        conn.close()
