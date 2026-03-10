"""Unit tests for app.strategy.variant_rules — pure functions."""
from datetime import datetime, timezone, timedelta
import pytest

from app.strategy.variant_rules import (
    RulesConfig,
    RulesInput,
    passes_variant_rules,
    choose_side,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _future(hours: float = 12.0) -> datetime:
    return _now() + timedelta(hours=hours)

def _valid_input(**overrides) -> RulesInput:
    """Return a fully-valid RulesInput; override specific fields as needed."""
    defaults = dict(
        variant_name="crypto_lag_aggressive",
        ticker="KXBTCD-20260310-B84000T86000",
        market_family="range_bucket",
        status="active",
        raw_close_time=_future(12),
        spread=0.06,
        yes_ask=0.55,
        no_ask=0.55,
        execution_edge_yes=0.02,
        execution_edge_no=0.02,
        distance_from_spot_pct=0.05,   # well outside near-bucket blacklist
        open_total=0,
        open_per_variant=0,
        open_per_ticker=0,
        open_per_cluster=0,
        open_per_cluster_all=0,
        attempts_today=0,
        recent_loss_minutes_ago=None,
        recent_close_minutes_ago=None,
    )
    defaults.update(overrides)
    return RulesInput(**defaults)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_valid_input_passes():
    ok, reason = passes_variant_rules(_valid_input())
    assert ok is True
    assert reason == "eligible"


# ---------------------------------------------------------------------------
# Rule 1 – variant name
# ---------------------------------------------------------------------------

def test_wrong_variant_rejected():
    ok, reason = passes_variant_rules(_valid_input(variant_name="other_variant"))
    assert ok is False
    assert reason == "unsupported_variant"


# ---------------------------------------------------------------------------
# Rule 2 – market family
# ---------------------------------------------------------------------------

def test_non_range_bucket_rejected():
    ok, reason = passes_variant_rules(_valid_input(market_family="threshold"))
    assert ok is False
    assert reason == "unsupported_market_family"


# ---------------------------------------------------------------------------
# Rule 3 – status
# ---------------------------------------------------------------------------

def test_closed_market_rejected():
    ok, reason = passes_variant_rules(_valid_input(status="closed"))
    assert ok is False
    assert reason == "market_not_active"

def test_open_status_passes():
    ok, reason = passes_variant_rules(_valid_input(status="open"))
    assert ok is True


# ---------------------------------------------------------------------------
# Rule 4 – close time
# ---------------------------------------------------------------------------

def test_missing_close_time_rejected():
    ok, reason = passes_variant_rules(_valid_input(raw_close_time=None))
    assert ok is False
    assert reason == "missing_close_time"

def test_string_close_time_rejected():
    ok, reason = passes_variant_rules(_valid_input(raw_close_time="2026-03-10T12:00:00Z"))
    assert ok is False
    assert reason == "close_time_not_datetime"


# ---------------------------------------------------------------------------
# Rule 5 – hours to expiry
# ---------------------------------------------------------------------------

def test_expired_market_rejected():
    ok, reason = passes_variant_rules(_valid_input(raw_close_time=_future(-1)))
    assert ok is False
    assert reason == "market_expired"

def test_too_close_to_expiry_rejected():
    ok, reason = passes_variant_rules(_valid_input(raw_close_time=_future(0.5)))
    assert ok is False
    assert reason == "too_close_to_expiry"

def test_too_far_to_expiry_rejected():
    ok, reason = passes_variant_rules(_valid_input(raw_close_time=_future(48)))
    assert ok is False
    assert reason == "too_far_to_expiry"


# ---------------------------------------------------------------------------
# Rule 6 – spread
# ---------------------------------------------------------------------------

def test_wide_spread_rejected():
    ok, reason = passes_variant_rules(_valid_input(spread=0.20))
    assert ok is False
    assert reason == "spread_too_wide"


# ---------------------------------------------------------------------------
# Rule 7 – near-bucket blacklist
# ---------------------------------------------------------------------------

def test_near_atm_rejected():
    ok, reason = passes_variant_rules(_valid_input(distance_from_spot_pct=0.005))
    assert ok is False
    assert reason == "v46_blacklisted_near_bucket"

def test_none_distance_allowed():
    # When distance is unknown, skip blacklist check
    ok, reason = passes_variant_rules(_valid_input(distance_from_spot_pct=None))
    assert ok is True


# ---------------------------------------------------------------------------
# Rule 8 – edge requirements
# ---------------------------------------------------------------------------

def test_no_edge_on_either_side_rejected():
    ok, reason = passes_variant_rules(
        _valid_input(execution_edge_yes=-0.01, execution_edge_no=-0.01)
    )
    assert ok is False
    assert "edge" in reason or reason == "v46_no_positive_edge"

def test_only_yes_edge_sufficient():
    ok, _ = passes_variant_rules(
        _valid_input(execution_edge_yes=0.02, execution_edge_no=-0.01)
    )
    assert ok is True

def test_only_no_edge_sufficient():
    # spread=0.02 so net_after_spread = 0.02 - 0.01 = 0.01 >= 0.0025 ✓
    ok, _ = passes_variant_rules(
        _valid_input(execution_edge_yes=-0.01, execution_edge_no=0.02, spread=0.02)
    )
    assert ok is True


# ---------------------------------------------------------------------------
# Rule 9 – cooldowns
# ---------------------------------------------------------------------------

def test_ticker_loss_ban():
    ok, reason = passes_variant_rules(_valid_input(recent_loss_minutes_ago=30.0))
    assert ok is False
    assert reason == "ticker_loss_ban"

def test_ticker_reentry_cooldown():
    ok, reason = passes_variant_rules(_valid_input(recent_close_minutes_ago=10.0))
    assert ok is False
    assert reason == "ticker_reentry_cooldown"


# ---------------------------------------------------------------------------
# Rule 10 – daily attempt cap
# ---------------------------------------------------------------------------

def test_daily_attempt_cap():
    ok, reason = passes_variant_rules(_valid_input(attempts_today=3))
    assert ok is False
    assert reason == "daily_attempt_cap"


# ---------------------------------------------------------------------------
# Rule 11 – portfolio stacking
# ---------------------------------------------------------------------------

def test_portfolio_total_cap():
    ok, reason = passes_variant_rules(_valid_input(open_total=8))
    assert ok is False
    assert reason == "portfolio_total_cap"

def test_portfolio_variant_cap():
    ok, reason = passes_variant_rules(_valid_input(open_per_variant=3))
    assert ok is False
    assert reason == "portfolio_variant_cap"

def test_ticker_global_cap():
    ok, reason = passes_variant_rules(_valid_input(open_per_ticker=1))
    assert ok is False
    assert reason == "ticker_global_cap"

def test_cluster_cap():
    ok, reason = passes_variant_rules(_valid_input(open_per_cluster=2))
    assert ok is False
    assert reason == "cluster_cap"


# ---------------------------------------------------------------------------
# choose_side
# ---------------------------------------------------------------------------

class TestChooseSide:
    def test_yes_when_only_yes_qualifies(self):
        assert choose_side(0.02, 0.005) == "yes"

    def test_no_when_only_no_qualifies(self):
        assert choose_side(0.005, 0.015) == "no"

    def test_best_when_both_qualify(self):
        # yes_edge > no_edge → prefer yes
        assert choose_side(0.05, 0.02) == "yes"
        # no_edge > yes_edge → prefer no
        assert choose_side(0.02, 0.05) == "no"

    def test_none_when_neither_qualifies(self):
        assert choose_side(0.005, 0.005) is None

    def test_none_inputs(self):
        assert choose_side(None, None) is None
        assert choose_side(0.02, None) == "yes"
        assert choose_side(None, 0.02) == "no"
