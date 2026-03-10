"""Unit tests for app.strategy.fair_value — all pure functions, no I/O."""
import math
import pytest

from app.strategy.fair_value import (
    bucket_probability,
    threshold_probability,
    enrich_snapshot,
    distance_from_spot_pct,
    distance_bucket_label,
)


# ---------------------------------------------------------------------------
# bucket_probability
# ---------------------------------------------------------------------------

class TestBucketProbability:
    def test_returns_none_for_zero_spot(self):
        assert bucket_probability(0, 80000, 90000, 12) is None

    def test_returns_none_for_zero_hours(self):
        assert bucket_probability(85000, 80000, 90000, 0) is None

    def test_returns_none_for_neither_bound(self):
        assert bucket_probability(85000, None, None, 12) is None

    def test_clamped_below_0_98(self):
        # Very wide bucket almost certain: should be ≤ 0.98
        p = bucket_probability(85000, 1000, 200000, 1)
        assert p is not None
        assert p <= 0.98

    def test_clamped_above_0_02(self):
        # Very narrow far-OTM bucket: should be ≥ 0.02
        p = bucket_probability(85000, 500, 600, 1)
        assert p is not None
        assert p >= 0.02

    def test_above_only(self):
        # P(spot > 80000) when spot=85000 should be > 0.5
        p = bucket_probability(85000, 80000, None, 24)
        assert p is not None
        assert p > 0.5

    def test_below_only(self):
        # P(spot < 90000) when spot=85000 should be > 0.5
        p = bucket_probability(85000, None, 90000, 24)
        assert p is not None
        assert p > 0.5

    def test_spot_at_bucket_centre(self):
        # Spot exactly centred in a bucket should have meaningful probability
        p = bucket_probability(85000, 80000, 90000, 24)
        assert p is not None
        assert 0.02 <= p <= 0.98


# ---------------------------------------------------------------------------
# threshold_probability
# ---------------------------------------------------------------------------

class TestThresholdProbability:
    def test_returns_none_for_invalid_inputs(self):
        assert threshold_probability(0, 85000, "above", 12) is None
        assert threshold_probability(85000, 0, "above", 12) is None
        assert threshold_probability(85000, 85000, "above", 0) is None

    def test_above_when_spot_well_above_threshold(self):
        # spot=90000, threshold=80000 → P(above) should be high
        p = threshold_probability(90000, 80000, "above", 24)
        assert p is not None
        assert p > 0.5

    def test_below_when_spot_well_below_threshold(self):
        # spot=70000, threshold=80000 → P(below) should be high
        p = threshold_probability(70000, 80000, "below", 24)
        assert p is not None
        assert p > 0.5

    def test_above_plus_below_sum_to_1(self):
        spot, thresh = 85000, 87000
        p_above = threshold_probability(spot, thresh, "above", 12)
        p_below = threshold_probability(spot, thresh, "below", 12)
        assert p_above is not None and p_below is not None
        # They should sum to ~1 (minor clamping artefact may shift them slightly)
        assert abs(p_above + p_below - 1.0) < 0.02

    def test_clamped_output(self):
        # Very strong directional: must still be within [0.02, 0.98]
        p = threshold_probability(200000, 80000, "above", 1)
        assert p is not None
        assert 0.02 <= p <= 0.98


# ---------------------------------------------------------------------------
# enrich_snapshot
# ---------------------------------------------------------------------------

class TestEnrichSnapshot:
    def test_best_side_yes_when_yes_edge_higher(self):
        # fair_yes=0.70 → exec_yes = 0.70 - 0.60 - 0.015 = 0.085
        #               → exec_no  = 0.30 - 0.50 - 0.015 = -0.215 (negative)
        r = enrich_snapshot(fair_yes=0.70, yes_ask=0.60, no_ask=0.50, fee_estimate=0.015)
        assert r.best_side == "yes"
        assert r.best_exec_edge > 0

    def test_best_side_no_when_no_edge_higher(self):
        # fair_yes=0.30 → exec_yes = 0.30 - 0.60 - 0.015 = -0.315 (negative)
        #               → exec_no  = 0.70 - 0.45 - 0.015 = 0.235
        r = enrich_snapshot(fair_yes=0.30, yes_ask=0.60, no_ask=0.45, fee_estimate=0.015)
        assert r.best_side == "no"
        assert r.best_exec_edge > 0

    def test_no_best_side_when_both_negative(self):
        # Ask prices make both edges negative
        r = enrich_snapshot(fair_yes=0.50, yes_ask=0.55, no_ask=0.55, fee_estimate=0.015)
        assert r.best_side is None
        assert r.best_exec_edge is None

    def test_fair_yes_clamped_to_0_02_0_98(self):
        r_low = enrich_snapshot(fair_yes=0.001, yes_ask=0.01, no_ask=0.99, fee_estimate=0.015)
        assert r_low.fair_yes == 0.02
        r_high = enrich_snapshot(fair_yes=0.999, yes_ask=0.01, no_ask=0.99, fee_estimate=0.015)
        assert r_high.fair_yes == 0.98

    def test_none_ask_skips_edge_computation(self):
        r = enrich_snapshot(fair_yes=0.60, yes_ask=None, no_ask=0.50, fee_estimate=0.015)
        assert r.execution_edge_yes is None
        assert r.execution_edge_no is not None

    def test_zero_ask_skips_edge_computation(self):
        r = enrich_snapshot(fair_yes=0.60, yes_ask=0.0, no_ask=0.50, fee_estimate=0.015)
        assert r.execution_edge_yes is None


# ---------------------------------------------------------------------------
# distance_from_spot_pct
# ---------------------------------------------------------------------------

class TestDistanceFromSpotPct:
    def test_spot_inside_bucket_returns_zero(self):
        assert distance_from_spot_pct(85000, 80000, 90000, None) == 0.0

    def test_spot_below_bucket_returns_positive(self):
        d = distance_from_spot_pct(75000, 80000, 90000, None)
        assert d is not None
        assert d > 0

    def test_spot_above_bucket_returns_positive(self):
        d = distance_from_spot_pct(95000, 80000, 90000, None)
        assert d is not None
        assert d > 0

    def test_zero_spot_returns_none(self):
        assert distance_from_spot_pct(0, 80000, 90000, None) is None

    def test_threshold_only_market(self):
        d = distance_from_spot_pct(85000, None, None, 87000)
        assert d is not None
        expected = abs(87000 - 85000) / 85000
        assert abs(d - expected) < 1e-9

    def test_open_high_bound(self):
        # P(spot < 90000) when spot=85000: should be 0 (inside)
        assert distance_from_spot_pct(85000, None, 90000, None) == 0.0
        # spot above bound
        d = distance_from_spot_pct(95000, None, 90000, None)
        assert d is not None and d > 0


# ---------------------------------------------------------------------------
# distance_bucket_label
# ---------------------------------------------------------------------------

class TestDistanceBucketLabel:
    def test_atm(self):
        assert distance_bucket_label(0.001) == "atm_0_0.5pct"

    def test_near(self):
        assert distance_bucket_label(0.010) == "near_0.5_1.5pct"

    def test_mid(self):
        assert distance_bucket_label(0.020) == "mid_1.5_3pct"

    def test_far(self):
        assert distance_bucket_label(0.050) == "far_3pct_plus"

    def test_none_returns_unknown(self):
        assert distance_bucket_label(None) == "unknown"
