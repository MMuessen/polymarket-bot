from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List


ARCHITECT_VERSION = "2026-03-08.architect.v1"


def _safe_float(v: Any, default: float | None = None) -> float | None:
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


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    try:
        return getattr(obj, key, default)
    except Exception:
        return default


def distance_bucket_from_pct(v: Any) -> str:
    x = _safe_float(v, None)
    if x is None:
        return "unknown"
    x = abs(x)
    if x <= 0.005:
        return "atm_0_0.5pct"
    if x <= 0.015:
        return "near_0.5_1.5pct"
    if x <= 0.03:
        return "mid_1.5_3pct"
    return "far_3pct_plus"


def market_view(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": row.get("ticker"),
        "best_side": row.get("best_side"),
        "best_exec_edge": _safe_float(row.get("best_exec_edge")),
        "execution_edge_yes": _safe_float(row.get("execution_edge_yes")),
        "execution_edge_no": _safe_float(row.get("execution_edge_no")),
        "fair_yes": _safe_float(row.get("fair_yes")),
        "yes_ask": _safe_float(row.get("yes_ask")),
        "no_ask": _safe_float(row.get("no_ask")),
        "entry_fee_yes": _safe_float(row.get("entry_fee_yes")),
        "entry_fee_no": _safe_float(row.get("entry_fee_no")),
        "spread": _safe_float(row.get("spread")),
        "distance_bucket": row.get("distance_bucket") or distance_bucket_from_pct(row.get("distance_from_spot_pct")),
        "distance_from_spot_pct": _safe_float(row.get("distance_from_spot_pct")),
        "hours_to_expiry": _safe_float(row.get("hours_to_expiry")),
        "rationale": row.get("rationale"),
    }


def build_architect_payload(status: Dict[str, Any]) -> Dict[str, Any]:
    markets = list(status.get("markets") or [])
    health = status.get("health") or {}
    strategies = status.get("strategies") or {}

    viewed = [market_view(m if isinstance(m, dict) else dict(m)) for m in markets]

    positive_rows = [m for m in viewed if (_safe_float(m.get("best_exec_edge"), None) or -999) > 0]
    by_side = defaultdict(int)
    by_bucket = defaultdict(lambda: {"count": 0, "positive_best_count": 0})

    for m in viewed:
        side = str(m.get("best_side") or "unknown")
        by_side[side] += 1
        bucket = m.get("distance_bucket") or "unknown"
        by_bucket[bucket]["count"] += 1
        if (_safe_float(m.get("best_exec_edge"), None) or -999) > 0:
            by_bucket[bucket]["positive_best_count"] += 1

    crypto_lag = strategies.get("crypto_lag") or {}

    return {
        "version": ARCHITECT_VERSION,
        "policy": {
            "role": "architect_final_say",
            "consultant_inputs_are_trusted": True,
            "consultant_inputs_are_not_auto_applied": True,
            "accepted_principles": [
                "side-aware execution",
                "fee-aware execution edges",
                "truthful expiry settlement",
                "opportunity-dataset-first research",
                "regime-aware calibration",
                "backtest before live expansion",
            ],
            "modified_or_rejected_principles": [
                {
                    "idea": "apply identical fee math to yes and no with no_bid for NO entry",
                    "decision": "rejected",
                    "reason": "NO entry must be evaluated off executable no_ask, not no_bid",
                },
                {
                    "idea": "remove edge_gone exits entirely",
                    "decision": "modified",
                    "reason": "replace with model-relative deterioration + hysteresis, not permanent disable",
                },
                {
                    "idea": "lower thresholds before measuring side/regime outcomes",
                    "decision": "rejected",
                    "reason": "thresholds come after calibrated replay, not before",
                },
            ],
        },
        "runtime": {
            "mode": status.get("mode"),
            "live_armed": status.get("live_armed"),
            "loop_error": health.get("loop_error"),
            "top_edge": status.get("top_edge"),
        },
        "execution_truth": {
            "side_aware": True,
            "fee_model": "schedule_shaped_estimate_for_scoring__actual_fill_fee_preferred",
            "yes_entry_reference": "yes_ask",
            "no_entry_reference": "no_ask",
            "settlement_truth_source": "CFB_RTI_60s_average__exchange_rules",
        },
        "market_snapshot": {
            "visible_market_count": len(viewed),
            "positive_best_edge_count": len(positive_rows),
            "top_positive": sorted(
                positive_rows,
                key=lambda x: _safe_float(x.get("best_exec_edge"), -999) or -999,
                reverse=True,
            )[:10],
            "count_by_best_side": dict(by_side),
            "count_by_distance_bucket": dict(by_bucket),
        },
        "strategy": {
            "crypto_lag": {
                "enabled": bool(crypto_lag.get("enabled")),
                "live_enabled": bool(crypto_lag.get("live_enabled")),
                "min_edge": _safe_float(crypto_lag.get("min_edge")),
                "max_spread": _safe_float(crypto_lag.get("max_spread")),
                "perf_trades": _safe_int(crypto_lag.get("perf_trades")),
                "perf_wins": _safe_int(crypto_lag.get("perf_wins")),
                "perf_pnl": _safe_float(crypto_lag.get("perf_pnl"), 0.0),
                "last_signal_at": crypto_lag.get("last_signal_at"),
                "last_signal_reason": crypto_lag.get("last_signal_reason"),
            }
        },
        "analytics": {
            "strategy_scorecard": status.get("strategy_scorecard"),
            "trade_analytics": status.get("trade_analytics"),
            "opportunity_analytics": status.get("opportunity_analytics"),
        },
        "data_quality": {
            "market_pipeline": health.get("market_pipeline"),
            "research_store": status.get("research_store"),
            "market_runtime": status.get("market_runtime"),
            "status_enrichment": status.get("status_enrichment"),
        },
    }


def render_consultant_report(status: Dict[str, Any]) -> str:
    a = build_architect_payload(status)
    lines: List[str] = []

    lines.append(f"architect_version: {a['version']}")
    lines.append(f"mode: {a['runtime']['mode']}")
    lines.append(f"live_armed: {a['runtime']['live_armed']}")
    lines.append(f"loop_error: {a['runtime']['loop_error']}")
    lines.append(f"top_edge: {a['runtime']['top_edge']}")
    lines.append("")
    lines.append("execution_truth:")
    et = a["execution_truth"]
    lines.append(f"  fee_model: {et['fee_model']}")
    lines.append(f"  yes_entry_reference: {et['yes_entry_reference']}")
    lines.append(f"  no_entry_reference: {et['no_entry_reference']}")
    lines.append(f"  settlement_truth_source: {et['settlement_truth_source']}")
    lines.append("")
    lines.append("market_snapshot:")
    ms = a["market_snapshot"]
    lines.append(f"  visible_market_count: {ms['visible_market_count']}")
    lines.append(f"  positive_best_edge_count: {ms['positive_best_edge_count']}")
    lines.append(f"  count_by_best_side: {ms['count_by_best_side']}")
    lines.append(f"  count_by_distance_bucket: {ms['count_by_distance_bucket']}")
    lines.append("")
    lines.append("top_positive:")
    for row in ms["top_positive"][:5]:
        lines.append(
            f"  - {row.get('ticker')} side={row.get('best_side')} "
            f"edge={row.get('best_exec_edge')} spread={row.get('spread')} "
            f"bucket={row.get('distance_bucket')}"
        )
    lines.append("")
    lines.append("crypto_lag:")
    cl = a["strategy"]["crypto_lag"]
    for k in [
        "enabled", "live_enabled", "min_edge", "max_spread",
        "perf_trades", "perf_wins", "perf_pnl", "last_signal_at", "last_signal_reason"
    ]:
        lines.append(f"  {k}: {cl.get(k)}")
    lines.append("")
    lines.append("policy_accept_reject:")
    for item in a["policy"]["modified_or_rejected_principles"]:
        lines.append(f"  - {item['idea']} => {item['decision']} ({item['reason']})")

    return "\n".join(lines)
