"""
Fair value calculations — all pure functions, no I/O, no side effects.

These are extracted verbatim from the working logic in main.py and made
explicit, named, and unit-testable.

Key invariants:
- fair_yes is always clamped to [0.02, 0.98]
- execution_edge_yes = fair_yes - yes_ask - fee_est
- execution_edge_no  = (1 - fair_yes) - no_ask - fee_est
- best_side is the side with the higher positive execution edge
- if no side has positive execution edge, best_side/edge remain None
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


# --------------------------------------------------------------------------- #
# GBM log-normal model (from main.py _bucket_probability / _norm_cdf)         #
# --------------------------------------------------------------------------- #

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bucket_probability(
    spot: float,
    low: Optional[float],
    high: Optional[float],
    hours_to_expiry: float,
    annual_vol: float = 0.60,
) -> Optional[float]:
    """
    Log-normal GBM probability that BTC closes inside [low, high].

    Returns None if inputs are invalid.
    Clamps result to [0.02, 0.98].
    """
    if spot <= 0 or hours_to_expiry <= 0:
        return None

    t = hours_to_expiry / (24.0 * 365.0)
    sigma = annual_vol * math.sqrt(t)
    if sigma <= 0:
        return None

    def z(level: float) -> float:
        return math.log(level / spot) / sigma

    if low is None and high is not None:
        raw = _norm_cdf(z(high))
    elif high is None and low is not None:
        raw = 1.0 - _norm_cdf(z(low))
    elif low is not None and high is not None:
        raw = max(0.0, _norm_cdf(z(high)) - _norm_cdf(z(low)))
    else:
        return None

    return min(0.98, max(0.02, raw))


def threshold_probability(
    spot: float,
    threshold: float,
    direction: str,
    hours_to_expiry: float,
    asset: str = "BTC",
) -> Optional[float]:
    """
    Logistic-normal fair value for simple above/below threshold markets.

    direction: "above" → P(spot > threshold at expiry)
               "below" → 1 - P(spot > threshold at expiry)
    """
    if spot <= 0 or threshold <= 0 or hours_to_expiry <= 0:
        return None

    sigma_ann = {"BTC": 0.65, "ETH": 0.85, "SOL": 1.20}.get(asset.upper(), 0.90)
    sigma_horizon = max(sigma_ann * math.sqrt(hours_to_expiry / (365.0 * 24.0)), 0.04)

    z = math.log(max(spot, 1e-9) / max(threshold, 1e-9)) / sigma_horizon
    p_above = 1.0 / (1.0 + math.exp(-1.702 * z))
    fair = p_above if direction == "above" else 1.0 - p_above

    return min(0.98, max(0.02, fair))


# --------------------------------------------------------------------------- #
# Execution edge computation                                                    #
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class EnrichedSnapshot:
    """
    Enrichment output — all derived values from fair value + quotes.
    Never mutates the source snapshot.
    """
    fair_yes: float
    execution_edge_yes: Optional[float]
    execution_edge_no: Optional[float]
    best_side: Optional[str]           # "yes" | "no" | None
    best_exec_edge: Optional[float]
    entry_fee_yes: float
    entry_fee_no: float
    rationale: str


def enrich_snapshot(
    *,
    fair_yes: float,
    yes_ask: Optional[float],
    no_ask: Optional[float],
    fee_estimate: float = 0.015,
    spot: float = 0.0,
    low: Optional[float] = None,
    high: Optional[float] = None,
    hours: float = 0.0,
    ticker: str = "",
) -> EnrichedSnapshot:
    """
    Compute all execution-edge derived values from fair_yes + current quotes.

    YES execution edge: fair_yes - yes_ask - fee
    NO execution edge:  (1 - fair_yes) - no_ask - fee

    Best side = whichever has the higher positive edge (or None if both negative).
    """
    fair_yes = min(0.98, max(0.02, fair_yes))

    exec_yes: Optional[float] = None
    exec_no: Optional[float] = None

    if yes_ask is not None and yes_ask > 0:
        exec_yes = round(fair_yes - yes_ask - fee_estimate, 4)
    if no_ask is not None and no_ask > 0:
        exec_no = round((1.0 - fair_yes) - no_ask - fee_estimate, 4)

    # Fee symmetry: entry fee = fee_estimate for both sides (simplified)
    entry_fee_yes = fee_estimate if yes_ask is not None else 0.0
    entry_fee_no = fee_estimate if no_ask is not None else 0.0

    # Side selection: pick side with highest positive edge
    candidates = []
    if exec_yes is not None and exec_yes > 0:
        candidates.append(("yes", exec_yes))
    if exec_no is not None and exec_no > 0:
        candidates.append(("no", exec_no))

    if candidates:
        best_side, best_edge = max(candidates, key=lambda x: x[1])
    else:
        best_side, best_edge = None, None

    rationale = (
        f"{ticker} fair_yes={fair_yes:.4f} "
        f"exec_yes={exec_yes if exec_yes is not None else 'n/a'} "
        f"exec_no={exec_no if exec_no is not None else 'n/a'} "
        f"best={best_side}@{best_edge if best_edge is not None else 'n/a'}"
    )

    return EnrichedSnapshot(
        fair_yes=round(fair_yes, 4),
        execution_edge_yes=exec_yes,
        execution_edge_no=exec_no,
        best_side=best_side,
        best_exec_edge=round(best_edge, 4) if best_edge is not None else None,
        entry_fee_yes=entry_fee_yes,
        entry_fee_no=entry_fee_no,
        rationale=rationale,
    )


def distance_from_spot_pct(
    spot: float,
    floor_strike: Optional[float],
    cap_strike: Optional[float],
    threshold: Optional[float],
) -> Optional[float]:
    """
    Distance from spot to the nearest bucket boundary, as a fraction of spot.
    Returns 0.0 if spot is inside the bucket.
    """
    if spot <= 0:
        return None

    low = floor_strike
    high = cap_strike

    if low is None and high is None:
        if threshold is not None:
            return abs(float(threshold) - spot) / spot
        return None

    if low is None and high is not None:
        return 0.0 if spot <= high else abs(spot - high) / spot
    if high is None and low is not None:
        return 0.0 if spot >= low else abs(low - spot) / spot
    # Both defined
    low_f, high_f = float(low), float(high)  # type: ignore[arg-type]
    if low_f <= spot <= high_f:
        return 0.0
    if spot < low_f:
        return abs(low_f - spot) / spot
    return abs(spot - high_f) / spot


def distance_bucket_label(pct: Optional[float]) -> str:
    if pct is None:
        return "unknown"
    x = abs(pct)
    if x <= 0.005:
        return "atm_0_0.5pct"
    if x <= 0.015:
        return "near_0.5_1.5pct"
    if x <= 0.03:
        return "mid_1.5_3pct"
    return "far_3pct_plus"
