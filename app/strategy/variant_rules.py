"""
Variant eligibility rules — pure functions.

These replace the v46 _aqg_passes_variant_rules logic in main.py.
All inputs are explicit; no snapshot mutation; fully unit-testable.

Rule chain (all must pass):
  1. Only crypto_lag_aggressive variant
  2. Market must be a range_bucket
  3. Market must be active/open
  4. raw_close_time must exist and be a datetime
  5. Hours to expiry within [min_hours, max_hours]
  6. Spread within max_spread
  7. At least one side has positive execution edge ≥ threshold
  8. Near-ATM bucket blacklist (configurable)
  9. Ticker/cluster stacking limits (queried from DB — passed as counts here)
  10. Portfolio capacity (total + per-variant + per-ticker)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple


@dataclass(frozen=True)
class RulesConfig:
    """Tunable parameters for variant rules. Mirrors _AQG_V46 config."""
    only_variant: str = "crypto_lag_aggressive"
    yes_min_edge: float = 0.015
    no_min_edge: float = 0.010
    no_min_net_after_spread: float = 0.0025
    min_hours: float = 1.0
    max_hours: float = 24.0
    max_spread: float = 0.14
    # Distance from spot: buckets closer than this are blacklisted
    near_bucket_blacklist_pct: float = 0.015
    # Portfolio limits
    max_open_total: int = 8
    max_open_per_variant: int = 3
    max_open_per_ticker: int = 1
    max_open_per_cluster: int = 2
    max_open_per_cluster_all: int = 3
    max_daily_attempts: int = 3
    loss_ban_minutes: float = 180.0
    reentry_cooldown_minutes: float = 45.0


@dataclass(frozen=True)
class RulesInput:
    """
    All inputs needed to evaluate variant rules for a single snapshot.
    No live I/O — callers pre-fetch DB counts and pass them in.
    """
    variant_name: str
    ticker: str
    market_family: Optional[str]
    status: str
    raw_close_time: Optional[datetime]
    spread: Optional[float]
    yes_ask: Optional[float]
    no_ask: Optional[float]
    execution_edge_yes: Optional[float]
    execution_edge_no: Optional[float]
    distance_from_spot_pct: Optional[float]

    # Pre-fetched DB counts (caller async-loads these)
    open_total: int = 0
    open_per_variant: int = 0
    open_per_ticker: int = 0
    open_per_cluster: int = 0
    open_per_cluster_all: int = 0
    attempts_today: int = 0
    recent_loss_minutes_ago: Optional[float] = None
    recent_close_minutes_ago: Optional[float] = None


def passes_variant_rules(
    inp: RulesInput,
    cfg: RulesConfig = RulesConfig(),
    now: Optional[datetime] = None,
) -> Tuple[bool, str]:
    """
    Evaluate all variant rules. Returns (eligible, reason).

    reason is "eligible" on success or a short snake_case rejection code.
    """
    _now = now or datetime.now(timezone.utc)

    # 1. Only supported variant
    if inp.variant_name != cfg.only_variant:
        return False, "unsupported_variant"

    # 2. Market family
    if inp.market_family != "range_bucket":
        return False, "unsupported_market_family"

    # 3. Status
    if inp.status.lower() not in {"active", "open", "initialized"}:
        return False, "market_not_active"

    # 4. Close time exists and is a datetime
    if inp.raw_close_time is None:
        return False, "missing_close_time"
    if not isinstance(inp.raw_close_time, datetime):
        return False, "close_time_not_datetime"

    # 5. Hours to expiry
    try:
        hours = max((inp.raw_close_time - _now).total_seconds() / 3600.0, 0.0)
    except TypeError:
        # Timezone-naive vs aware mismatch
        return False, "close_time_tz_error"

    if hours <= 0:
        return False, "market_expired"
    if hours < cfg.min_hours:
        return False, "too_close_to_expiry"
    if hours > cfg.max_hours:
        return False, "too_far_to_expiry"

    # 6. Spread
    spread = inp.spread or 0.0
    if spread > cfg.max_spread:
        return False, "spread_too_wide"

    # 7. Near-ATM blacklist
    dist = inp.distance_from_spot_pct
    if dist is not None and abs(dist) < cfg.near_bucket_blacklist_pct:
        return False, "v46_blacklisted_near_bucket"

    # 8. Edge requirements — need at least one side with positive edge
    exec_yes = inp.execution_edge_yes
    exec_no = inp.execution_edge_no

    yes_ok = exec_yes is not None and exec_yes >= cfg.yes_min_edge
    no_ok = (
        exec_no is not None
        and exec_no >= cfg.no_min_edge
        and exec_no - spread / 2.0 >= cfg.no_min_net_after_spread
    )

    if not yes_ok and not no_ok:
        # Diagnose which side failed
        if exec_yes is None and exec_no is None:
            return False, "missing_both_edges"
        if exec_yes is not None and exec_yes > 0:
            return False, "v46_weak_yes_edge"
        if exec_no is not None and exec_no > 0:
            return False, "v46_weak_no_edge"
        return False, "v46_no_positive_edge"

    # 9. Cooldown / ban checks
    if inp.recent_loss_minutes_ago is not None and inp.recent_loss_minutes_ago < cfg.loss_ban_minutes:
        return False, "ticker_loss_ban"
    if inp.recent_close_minutes_ago is not None and inp.recent_close_minutes_ago < cfg.reentry_cooldown_minutes:
        return False, "ticker_reentry_cooldown"

    # 10. Daily attempt cap
    if inp.attempts_today >= cfg.max_daily_attempts:
        return False, "daily_attempt_cap"

    # 11. Portfolio stacking
    if inp.open_total >= cfg.max_open_total:
        return False, "portfolio_total_cap"
    if inp.open_per_variant >= cfg.max_open_per_variant:
        return False, "portfolio_variant_cap"
    if inp.open_per_ticker >= cfg.max_open_per_ticker:
        return False, "ticker_global_cap"
    if inp.open_per_cluster >= cfg.max_open_per_cluster:
        return False, "cluster_cap"
    if inp.open_per_cluster_all >= cfg.max_open_per_cluster_all:
        return False, "cluster_global_cap"

    return True, "eligible"


def choose_side(
    execution_edge_yes: Optional[float],
    execution_edge_no: Optional[float],
    cfg: RulesConfig = RulesConfig(),
) -> Optional[str]:
    """
    Given pre-computed execution edges, return the preferred side.
    Returns None if neither side meets the minimum threshold.
    """
    yes_ok = execution_edge_yes is not None and execution_edge_yes >= cfg.yes_min_edge
    no_ok = execution_edge_no is not None and execution_edge_no >= cfg.no_min_edge

    if yes_ok and no_ok:
        return "yes" if execution_edge_yes >= execution_edge_no else "no"  # type: ignore[operator]
    if yes_ok:
        return "yes"
    if no_ok:
        return "no"
    return None
