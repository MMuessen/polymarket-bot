"""Unit tests for app.strategy.gate — pure evaluation."""
import pytest
from app.strategy.gate import GateConfig, GateDecision, evaluate_gate


def _gate(**overrides):
    defaults = dict(
        side="yes",
        entry_price=0.55,
        fair_yes=0.70,
        hours_to_expiry=12.0,
        obi=None,
        vpin=None,
        cfg=GateConfig(),
    )
    defaults.update(overrides)
    return evaluate_gate(**defaults)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_allow_when_all_pass():
    d = _gate()
    assert d.allow is True
    assert d.reasons == []


# ---------------------------------------------------------------------------
# Reservation price check
# ---------------------------------------------------------------------------

def test_rejects_when_entry_above_reservation():
    # fair_yes=0.70, gamma=0.10 → reservation=0.60
    # entry_price=0.65 > 0.60 → should block
    d = _gate(entry_price=0.65, fair_yes=0.70)
    assert d.allow is False
    assert any("price_above_reservation" in r for r in d.reasons)

def test_reservation_price_returned():
    d = _gate(entry_price=0.55, fair_yes=0.70)  # reservation = 0.60
    assert d.reservation_price == pytest.approx(0.60, abs=1e-4)

def test_no_side_uses_1_minus_fair_yes():
    # side=no, fair_yes=0.30 → fair_no=0.70, reservation=0.60
    # entry=0.55 < 0.60 → should allow
    d = _gate(side="no", entry_price=0.55, fair_yes=0.30)
    assert d.allow is True

def test_no_side_blocks_when_above_reservation():
    # side=no, fair_yes=0.30 → fair_no=0.70, reservation=0.60
    # entry=0.65 > 0.60 → block
    d = _gate(side="no", entry_price=0.65, fair_yes=0.30)
    assert d.allow is False


# ---------------------------------------------------------------------------
# Settlement risk
# ---------------------------------------------------------------------------

def test_settlement_risk_blocks_high_entry_near_expiry():
    cfg = GateConfig(settlement_window_seconds=120.0, settlement_max_entry_price=0.80)
    # hours_to_expiry=0.02 → 72 seconds → inside 120s window; entry=0.85 > 0.80
    # But we also need to pass reservation: fair_yes=0.90, gamma=0.10 → reservation=0.80; entry=0.85>0.80
    # Both triggers will fire; test only checks settlement reason
    d = evaluate_gate(
        side="yes",
        entry_price=0.85,
        fair_yes=0.98,
        hours_to_expiry=0.02,
        cfg=cfg,
    )
    assert any("settlement_risk" in r for r in d.reasons)

def test_no_settlement_risk_when_far_from_expiry():
    d = _gate(entry_price=0.55, fair_yes=0.70, hours_to_expiry=12.0)
    assert not any("settlement_risk" in r for r in d.reasons)


# ---------------------------------------------------------------------------
# OBI check
# ---------------------------------------------------------------------------

def test_obi_unfavorable_yes():
    d = _gate(side="yes", obi=0.30)  # below obi_yes_min=0.50
    assert d.allow is False
    assert any("obi_unfavorable_yes" in r for r in d.reasons)

def test_obi_favorable_yes():
    d = _gate(side="yes", obi=0.60)  # above obi_yes_min=0.50
    assert "obi_unfavorable_yes" not in str(d.reasons)

def test_obi_unfavorable_no():
    d = _gate(side="no", entry_price=0.55, fair_yes=0.30, obi=0.10)  # above obi_no_max=-0.50
    assert any("obi_unfavorable_no" in r for r in d.reasons)

def test_obi_favorable_no():
    d = _gate(side="no", entry_price=0.55, fair_yes=0.30, obi=-0.70)  # below obi_no_max=-0.50
    assert not any("obi_unfavorable_no" in r for r in d.reasons)

def test_obi_none_skips_check():
    # With obi=None, no OBI reasons should appear
    d = _gate(obi=None)
    assert not any("obi" in r for r in d.reasons)


# ---------------------------------------------------------------------------
# VPIN / toxicity check
# ---------------------------------------------------------------------------

def test_toxicity_blocks_when_high():
    d = _gate(vpin=5.0)  # > toxicity_bps_1s_max=4.0
    assert d.allow is False
    assert any("toxicity_too_high" in r for r in d.reasons)

def test_toxicity_allows_when_low():
    d = _gate(vpin=2.0)
    assert not any("toxicity_too_high" in r for r in d.reasons)


# ---------------------------------------------------------------------------
# Multiple failures
# ---------------------------------------------------------------------------

def test_multiple_reasons_accumulated():
    cfg = GateConfig(
        gamma=0.10,
        obi_yes_min=0.50,
        toxicity_bps_1s_max=4.0,
    )
    # price_above_reservation + obi_unfavorable_yes + toxicity
    d = evaluate_gate(
        side="yes",
        entry_price=0.75,  # reservation=0.60 for fair_yes=0.70
        fair_yes=0.70,
        hours_to_expiry=12.0,
        obi=0.20,
        vpin=6.0,
        cfg=cfg,
    )
    assert d.allow is False
    assert len(d.reasons) >= 3


# ---------------------------------------------------------------------------
# GateDecision __bool__
# ---------------------------------------------------------------------------

def test_gate_decision_bool_true():
    gd = GateDecision(allow=True)
    assert bool(gd) is True

def test_gate_decision_bool_false():
    gd = GateDecision(allow=False, reasons=["test_reason"])
    assert bool(gd) is False
