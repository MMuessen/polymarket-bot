from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Tuple

DB_PATH = Path("data/strategy_lab.db")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _distance_bucket_from_pct(v: Any) -> str:
    pct = _safe_float(v, -1.0)
    if pct < 0:
        return "unknown"
    if pct < 0.005:
        return "atm_0_0.5pct"
    if pct < 0.015:
        return "near_0.5_1.5pct"
    if pct < 0.03:
        return "mid_1.5_3pct"
    return "far_3pct_plus"


def _hours_bucket(v: Any) -> str:
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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


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


def ensure_tables() -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_compare_cohorts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_ts TEXT NOT NULL,
                ts TEXT NOT NULL,
                ticker TEXT NOT NULL,
                best_side TEXT,
                best_exec_edge REAL,
                taker_side TEXT,
                taker_edge REAL,
                maker_side TEXT,
                maker_edge REAL,
                preferred_execution_style TEXT,
                distance_bucket TEXT,
                hours_bucket TEXT,
                regime TEXT,
                basis_bps_vs_spot REAL,
                tail_model_mode TEXT,
                tail_multiplier REAL,
                size_multiplier_recommendation REAL,
                extra_json TEXT,
                UNIQUE(batch_ts, ticker)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def record_execution_compare_batch(status: Dict[str, Any]) -> Dict[str, Any]:
    ensure_tables()

    health = _row_dict(status.get("health") or {})
    rows = status.get("markets") or []
    v2 = _row_dict(status.get("architect_consultant_v2") or {})
    v3 = _row_dict(status.get("architect_consultant_v3") or {})
    v4 = _row_dict(status.get("architect_replay_calibration") or {})
    exec_decision = _row_dict(status.get("execution_decision") or v3.get("execution_decision") or {})

    batch_ts = str(
        health.get("last_market_refresh_finished_at")
        or health.get("last_market_refresh")
        or _now_iso()
    )
    regime = str(
        exec_decision.get("regime")
        or (_row_dict(v2.get("regime_filter") or {})).get("regime")
        or "unknown"
    )
    basis_bps = _safe_float(
        (_row_dict(v2.get("settlement_proxy") or {})).get("basis_bps_vs_spot"),
        0.0,
    )
    tail_mode = str((_row_dict(v2.get("tail_model") or {})).get("mode") or "unknown")
    size_mult = _safe_float(
        exec_decision.get("size_multiplier_recommendation")
        or (_row_dict(v2.get("regime_filter") or {})).get("size_multiplier_recommendation"),
        1.0,
    )

    inserted = 0
    conn = get_conn()
    try:
        for raw in rows:
            row = _row_dict(raw)
            ticker = str(row.get("ticker") or "").strip()
            if not ticker:
                continue

            best_side = str(row.get("best_side") or "").lower() or None
            best_exec_edge = _safe_float(row.get("best_exec_edge"), 0.0)

            taker_side = best_side
            taker_edge = best_exec_edge

            maker_side = str(row.get("maker_best_side") or best_side or "").lower() or None
            maker_edge = _safe_float(row.get("maker_best_edge"), 0.0)

            preferred_execution_style = str(row.get("preferred_execution_style") or "")
            distance_bucket = str(
                row.get("distance_bucket")
                or _distance_bucket_from_pct(row.get("distance_from_spot_pct"))
            )
            hours_bucket = _hours_bucket(row.get("hours_to_expiry"))
            tail_multiplier = _safe_float(row.get("tail_multiplier"), 1.0)

            extra = json.dumps(
                {
                    "spread": row.get("spread"),
                    "yes_ask": row.get("yes_ask"),
                    "no_ask": row.get("no_ask"),
                    "tail_adjusted_fair_yes": row.get("tail_adjusted_fair_yes"),
                    "rationale": row.get("rationale"),
                },
                sort_keys=True,
            )

            conn.execute(
                """
                INSERT OR REPLACE INTO execution_compare_cohorts (
                    batch_ts, ts, ticker, best_side, best_exec_edge,
                    taker_side, taker_edge, maker_side, maker_edge,
                    preferred_execution_style, distance_bucket, hours_bucket,
                    regime, basis_bps_vs_spot, tail_model_mode,
                    tail_multiplier, size_multiplier_recommendation, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_ts,
                    _now_iso(),
                    ticker,
                    best_side,
                    best_exec_edge,
                    taker_side,
                    taker_edge,
                    maker_side,
                    maker_edge,
                    preferred_execution_style,
                    distance_bucket,
                    hours_bucket,
                    regime,
                    basis_bps,
                    tail_mode,
                    tail_multiplier,
                    size_mult,
                    extra,
                ),
            )
            inserted += 1

        conn.commit()
        total_rows = conn.execute("SELECT COUNT(*) FROM execution_compare_cohorts").fetchone()[0]
    finally:
        conn.close()

    return {
        "batch_ts": batch_ts,
        "rows_seen": len(rows),
        "rows_inserted": inserted,
        "total_rows": int(total_rows),
        "regime": regime,
        "basis_bps_vs_spot": round(basis_bps, 4),
        "tail_model_mode": tail_mode,
        "size_multiplier_recommendation": size_mult,
    }


def _market_opportunity_lookup(limit_rows: int = 50000) -> Dict[str, Any]:
    if not DB_PATH.exists():
        return {
            "source_table": None,
            "row_count_used": 0,
            "lookup_by_distance_side_hours": [],
        }

    conn = get_conn()
    try:
        if not _table_exists(conn, "market_opportunities"):
            return {
                "source_table": None,
                "row_count_used": 0,
                "lookup_by_distance_side_hours": [],
            }

        cols = set(_table_columns(conn, "market_opportunities"))
        select_cols = ["best_side", "best_side_correct", "best_side_realized_pnl", "resolved_at"]

        if "distance_bucket" in cols:
            select_cols.append("distance_bucket")
        elif "distance_from_spot_pct" in cols:
            select_cols.append("distance_from_spot_pct")

        if "hours_to_expiry" in cols:
            select_cols.append("hours_to_expiry")

        sql = f"""
            SELECT {", ".join(select_cols)}
            FROM market_opportunities
            WHERE resolved_at IS NOT NULL
            ORDER BY id DESC
            LIMIT ?
        """
        rows = conn.execute(sql, (int(limit_rows),)).fetchall()
    finally:
        conn.close()

    agg: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = _row_dict(raw)
        best_side = str(row.get("best_side") or "").lower().strip()
        if best_side not in {"yes", "no"}:
            continue

        if "distance_bucket" in row:
            distance_bucket = str(row.get("distance_bucket") or "unknown")
        else:
            distance_bucket = _distance_bucket_from_pct(row.get("distance_from_spot_pct"))

        hours_bucket = _hours_bucket(row.get("hours_to_expiry"))
        agg[(distance_bucket, best_side, hours_bucket)].append(row)

    out = []
    for (distance_bucket, best_side, hours_bucket), items in sorted(agg.items()):
        trades = len(items)
        wins = 0
        losses = 0
        flats = 0
        pnls = []

        for row in items:
            correct = row.get("best_side_correct")
            pnl = row.get("best_side_realized_pnl")
            pnl_f = _safe_float(pnl, 0.0)
            pnls.append(pnl_f)

            if correct in (1, True, "1", "true", "True"):
                wins += 1
            elif correct in (0, False, "0", "false", "False"):
                losses += 1
            else:
                if abs(pnl_f) < 1e-9:
                    flats += 1
                elif pnl_f > 0:
                    wins += 1
                else:
                    losses += 1

        win_rate = round((wins / trades), 4) if trades else 0.0
        realized_pnl = round(sum(pnls), 4)
        avg_pnl = round((realized_pnl / trades), 4) if trades else 0.0

        out.append(
            {
                "distance_bucket": distance_bucket,
                "best_side": best_side,
                "hours_bucket": hours_bucket,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "flat": flats,
                "win_rate": win_rate,
                "realized_pnl": realized_pnl,
                "avg_pnl_per_trade": avg_pnl,
            }
        )

    out.sort(key=lambda x: (-x["trades"], x["distance_bucket"], x["best_side"], x["hours_bucket"]))
    return {
        "source_table": "market_opportunities",
        "row_count_used": len(rows),
        "lookup_by_distance_side_hours": out[:50],
    }


def _extract_price_points(bot: Any, symbol: str = "BTC") -> List[float]:
    candidates = [
        getattr(bot, "spot_price_history", None),
        getattr(bot, "spot_history", None),
        getattr(bot, "btc_spot_history", None),
        getattr(bot, "price_history", None),
        getattr(bot, "model_meta", None),
    ]

    out: List[float] = []

    for c in candidates:
        if not c:
            continue

        if isinstance(c, dict):
            # keyed history
            if symbol in c and isinstance(c[symbol], (list, tuple)):
                seq = c[symbol]
            elif "spot_history" in c and isinstance(c["spot_history"], (list, tuple)):
                seq = c["spot_history"]
            else:
                seq = [c]
        elif isinstance(c, (list, tuple)):
            seq = c
        else:
            continue

        for item in seq[-120:]:
            try:
                if isinstance(item, (int, float)):
                    out.append(float(item))
                elif isinstance(item, dict):
                    for key in ("price", "spot", "value", "close", symbol):
                        if key in item and item[key] not in (None, ""):
                            out.append(float(item[key]))
                            break
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append(float(item[-1]))
            except Exception:
                continue

        if out:
            break

    return [x for x in out if x > 0]


def build_twap_surface(bot: Any, status: Dict[str, Any]) -> Dict[str, Any]:
    points = _extract_price_points(bot, "BTC")
    spot = _safe_float((getattr(bot, "spot_prices", {}) or {}).get("BTC"), 0.0)

    if points:
        recent = points[-60:]
        twap = round(mean(recent), 4)
        proxy_points = len(recent)
    else:
        twap = round(spot, 4)
        proxy_points = 1 if spot > 0 else 0

    basis_bps = 0.0
    try:
        existing = _row_dict(
            (_row_dict(status.get("architect_consultant_v2") or {})).get("settlement_proxy") or {}
        )
        basis_bps = _safe_float(existing.get("basis_bps_vs_spot"), 0.0)
    except Exception:
        pass

    return {
        "mode": "rolling_60s_twap_proxy_surface_only",
        "symbol": "BTC",
        "spot": round(spot, 4),
        "twap_60s_proxy": twap,
        "proxy_points": proxy_points,
        "basis_bps_vs_spot": round(basis_bps, 4),
        "architect_truth_note": "surface_only_until_promoted_into_replay_settlement",
    }


def build_replay_calibration(bot: Any, status: Dict[str, Any]) -> Dict[str, Any]:
    cohort = record_execution_compare_batch(status)
    twap = build_twap_surface(bot, status)
    lookup = _market_opportunity_lookup()

    accepted = [
        "maker_vs_taker_cohort_recording",
        "rolling_60s_twap_proxy_surface_for_replay",
        "historical_lookup_by_distance_side_hours",
        "regime_scaling_replay_only_before_live_promotion",
    ]
    modified = [
        "direct_deribit_smile_live_pricing => first_measure_empirical_overlay_in_replay",
        "basis_proxy_live_authority => first_surface_and_compare_in_replay",
        "exit_rewrites => promote_only_after_post_policy_cohort_evidence",
    ]
    denied = [
        "threshold_or_sizing_changes_before_cohort_resolution",
        "optimistic_no_bid_entry_scoring",
        "production_promotion_of_tail_overlay_before_replay_evidence",
    ]

    return {
        "execution_compare_cohort_runtime": cohort,
        "twap_settlement_proxy": twap,
        "historical_lookup": lookup,
        "architect_choice_log": {
            "accepted": accepted,
            "modified": modified,
            "denied": denied,
        },
        "architect_next_step": "wire_lookup_and_twap_into_replay_before_threshold_or_sizing_changes",
    }


def build_plaintext_report(status: Dict[str, Any]) -> str:
    cal = _row_dict(status.get("architect_replay_calibration") or {})
    cohort = _row_dict(cal.get("execution_compare_cohort_runtime") or {})
    twap = _row_dict(cal.get("twap_settlement_proxy") or {})
    lookup = _row_dict(cal.get("historical_lookup") or {})
    choice_log = _row_dict(cal.get("architect_choice_log") or {})

    lines: List[str] = []
    lines.append(f"mode: {status.get('mode')}")
    lines.append(f"live_armed: {status.get('live_armed')}")
    lines.append(f"loop_error: {(_row_dict(status.get('health') or {})).get('loop_error')}")
    lines.append("")
    lines.append("execution_compare_cohort_runtime:")
    lines.append(f"  batch_ts: {cohort.get('batch_ts')}")
    lines.append(f"  rows_seen: {cohort.get('rows_seen')}")
    lines.append(f"  rows_inserted: {cohort.get('rows_inserted')}")
    lines.append(f"  total_rows: {cohort.get('total_rows')}")
    lines.append(f"  regime: {cohort.get('regime')}")
    lines.append("")
    lines.append("twap_settlement_proxy:")
    lines.append(f"  mode: {twap.get('mode')}")
    lines.append(f"  spot: {twap.get('spot')}")
    lines.append(f"  twap_60s_proxy: {twap.get('twap_60s_proxy')}")
    lines.append(f"  proxy_points: {twap.get('proxy_points')}")
    lines.append(f"  basis_bps_vs_spot: {twap.get('basis_bps_vs_spot')}")
    lines.append("")
    lines.append("historical_lookup_top:")
    for row in (lookup.get("lookup_by_distance_side_hours") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('best_side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} pnl={row.get('realized_pnl')}"
        )
    lines.append("")
    lines.append("architect_choice_log:")
    for key in ("accepted", "modified", "denied"):
        lines.append(f"  {key}:")
        for item in (choice_log.get(key) or []):
            lines.append(f"    - {item}")
    lines.append("")
    lines.append(f"architect_next_step: {cal.get('architect_next_step')}")
    return "\n".join(lines)
