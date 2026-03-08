from __future__ import annotations

from typing import Any, Dict, List


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "null"):
            return default
        return float(v)
    except Exception:
        return default


def summarize_execution_compare(
    rows: List[Dict[str, Any]],
    min_edge: float,
    max_spread: float,
) -> Dict[str, Any]:
    seen = 0
    positive_taker = 0
    actionable_taker = 0
    positive_maker = 0
    actionable_maker = 0
    maker_better_than_taker = 0
    maker_adv_sum = 0.0
    maker_adv_n = 0

    by_bucket: Dict[str, Dict[str, Any]] = {}
    by_maker_side: Dict[str, Dict[str, Any]] = {
        "yes": {"count": 0, "positive": 0, "actionable": 0},
        "no": {"count": 0, "positive": 0, "actionable": 0},
    }

    top_rows: List[Dict[str, Any]] = []

    for row in rows or []:
        if not isinstance(row, dict):
            continue

        seen += 1
        ticker = row.get("ticker")
        bucket = str(row.get("distance_bucket") or "unknown")
        spread = _f(row.get("spread"))
        taker_edge = _f(row.get("best_exec_edge"))
        maker_edge = _f(row.get("maker_best_edge"))
        taker_side = str(row.get("best_side") or "").lower().strip()
        maker_side = str(row.get("maker_best_side") or "").lower().strip()
        preferred = str(row.get("preferred_execution_style") or "").strip()

        if taker_edge > 0:
            positive_taker += 1
        if maker_edge > 0:
            positive_maker += 1
        if spread <= max_spread and taker_edge >= min_edge:
            actionable_taker += 1
        if spread <= max_spread and maker_edge >= min_edge:
            actionable_maker += 1

        if maker_edge > taker_edge:
            maker_better_than_taker += 1
            maker_adv_sum += (maker_edge - taker_edge)
            maker_adv_n += 1

        bucket_stat = by_bucket.setdefault(bucket, {
            "count": 0,
            "positive_taker": 0,
            "actionable_taker": 0,
            "positive_maker": 0,
            "actionable_maker": 0,
        })
        bucket_stat["count"] += 1
        if taker_edge > 0:
            bucket_stat["positive_taker"] += 1
        if maker_edge > 0:
            bucket_stat["positive_maker"] += 1
        if spread <= max_spread and taker_edge >= min_edge:
            bucket_stat["actionable_taker"] += 1
        if spread <= max_spread and maker_edge >= min_edge:
            bucket_stat["actionable_maker"] += 1

        if maker_side in by_maker_side:
            by_maker_side[maker_side]["count"] += 1
            if maker_edge > 0:
                by_maker_side[maker_side]["positive"] += 1
            if spread <= max_spread and maker_edge >= min_edge:
                by_maker_side[maker_side]["actionable"] += 1

        top_rows.append({
            "ticker": ticker,
            "distance_bucket": bucket,
            "spread": round(spread, 4),
            "taker_side": taker_side,
            "taker_edge": round(taker_edge, 4),
            "maker_side": maker_side,
            "maker_edge": round(maker_edge, 4),
            "maker_advantage": round(maker_edge - taker_edge, 4),
            "preferred_execution_style": preferred,
        })

    top_rows = sorted(
        top_rows,
        key=lambda r: (r.get("maker_advantage") or 0.0, r.get("maker_edge") or 0.0),
        reverse=True,
    )[:10]

    return {
        "rows_seen": seen,
        "positive_taker_count": positive_taker,
        "actionable_taker_count": actionable_taker,
        "positive_maker_count": positive_maker,
        "actionable_maker_count": actionable_maker,
        "maker_better_than_taker_count": maker_better_than_taker,
        "avg_maker_advantage_over_taker": round(maker_adv_sum / maker_adv_n, 4) if maker_adv_n else 0.0,
        "by_distance_bucket": by_bucket,
        "by_maker_side": by_maker_side,
        "top_maker_advantage_rows": top_rows,
    }
