from __future__ import annotations

from typing import Any, Dict, List


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "null"):
            return default
        return float(v)
    except Exception:
        return default


def _clip_price(p: float) -> float:
    p = max(0.01, min(0.99, p))
    return round(p, 4)


def _tick_down(p: float, tick: float = 0.01) -> float:
    return _clip_price(p - tick)


def _tick_up(p: float, tick: float = 0.01) -> float:
    return _clip_price(p + tick)


def simulate_quote_row(
    row: Dict[str, Any],
    min_edge: float,
    max_spread: float,
    basis_bps_vs_spot: float = 0.0,
    regime_size_mult: float = 1.0,
    tail_multiplier: float = 1.0,
) -> Dict[str, Any]:
    yes_bid = _f(row.get("yes_bid"))
    yes_ask = _f(row.get("yes_ask"))
    no_bid = _f(row.get("no_bid"))
    no_ask = _f(row.get("no_ask"))
    spread = _f(row.get("spread"))
    fair_yes = _f(row.get("tail_adjusted_fair_yes"), _f(row.get("fair_yes")))
    best_exec_edge = _f(row.get("best_exec_edge"))
    maker_best_edge = _f(row.get("maker_best_edge"))
    best_side = str(row.get("best_side") or "").lower().strip()
    maker_best_side = str(row.get("maker_best_side") or "").lower().strip()
    hours = _f(row.get("hours_to_expiry"))
    ticker = row.get("ticker")
    bucket = row.get("distance_bucket") or "unknown"

    # We only simulate rows with valid market data.
    valid = yes_ask > 0 and no_ask > 0 and hours > 0

    yes_rest = _tick_down(yes_ask) if yes_ask > 0 else 0.0
    no_rest = _tick_down(no_ask) if no_ask > 0 else 0.0

    # Simplified maker fillability proxy:
    # closer-to-ask quotes are more likely to fill, wider spreads help makers.
    yes_fill_score = round(max(0.0, min(1.0, 0.35 + spread * 3.0)), 4)
    no_fill_score = round(max(0.0, min(1.0, 0.35 + spread * 3.0)), 4)

    yes_edge_if_rest = round(fair_yes - yes_rest, 4) if yes_rest > 0 else 0.0
    no_edge_if_rest = round((1.0 - fair_yes) - no_rest, 4) if no_rest > 0 else 0.0

    if yes_edge_if_rest >= no_edge_if_rest:
        quote_side = "yes"
        quote_price = yes_rest
        quote_edge = yes_edge_if_rest
        fill_score = yes_fill_score
    else:
        quote_side = "no"
        quote_price = no_rest
        quote_edge = no_edge_if_rest
        fill_score = no_fill_score

    # Small conservative adjustment so this remains a shadow comparison tool.
    adjusted_quote_edge = round(quote_edge * regime_size_mult, 4)

    action = "watch_only"
    if (
        valid
        and spread <= max_spread
        and adjusted_quote_edge >= min_edge
        and fill_score >= 0.35
    ):
        action = "maker_shadow_candidate"

    return {
        "ticker": ticker,
        "distance_bucket": bucket,
        "best_side": best_side,
        "best_exec_edge": best_exec_edge,
        "maker_best_side": maker_best_side,
        "maker_best_edge": maker_best_edge,
        "quote_side": quote_side,
        "quote_price": quote_price,
        "quote_edge": round(quote_edge, 4),
        "adjusted_quote_edge": adjusted_quote_edge,
        "fill_score": fill_score,
        "basis_bps_vs_spot": basis_bps_vs_spot,
        "tail_multiplier": tail_multiplier,
        "regime_size_mult": regime_size_mult,
        "action": action,
    }


def build_shadow_quotes(
    rows: List[Dict[str, Any]],
    min_edge: float,
    max_spread: float,
    basis_bps_vs_spot: float = 0.0,
    regime_size_mult: float = 1.0,
) -> Dict[str, Any]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out.append(
            simulate_quote_row(
                row=row,
                min_edge=min_edge,
                max_spread=max_spread,
                basis_bps_vs_spot=basis_bps_vs_spot,
                regime_size_mult=regime_size_mult,
                tail_multiplier=_f(row.get("tail_multiplier"), 1.0),
            )
        )

    maker_shadow = [r for r in out if r.get("action") == "maker_shadow_candidate"]
    watch_only = [r for r in out if r.get("action") != "maker_shadow_candidate"]

    by_side = {"yes": 0, "no": 0}
    for r in maker_shadow:
        s = str(r.get("quote_side") or "").lower()
        if s in by_side:
            by_side[s] += 1

    return {
        "rows_seen": len(out),
        "maker_shadow_candidate_count": len(maker_shadow),
        "watch_only_count": len(watch_only),
        "by_quote_side": by_side,
        "top_candidates": sorted(
            maker_shadow,
            key=lambda x: (x.get("adjusted_quote_edge") or 0.0, x.get("fill_score") or 0.0),
            reverse=True,
        )[:10],
        "all_rows": out,
    }
