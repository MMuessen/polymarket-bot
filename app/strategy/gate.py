"""
Institutional entry gate — pure evaluation, no I/O.

Extracted from _arch_iq_entry_gate / _aqg_decide logic in main.py.

The gate checks:
  1. Reservation price: entry_price must be below reservation (fair value - risk premium)
  2. OBI (Order Book Imbalance): must be on the correct side for the trade direction
  3. VPIN / toxicity: must be below threshold
  4. Settlement risk: reject if entry price is dangerously high near expiry

All checks produce named reasons; callers can inspect individual failures.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass(frozen=True)
class GateConfig:
    gamma: float = 0.10                      # risk premium: reservation = fair - gamma
    obi_yes_min: float = 0.50               # YES entry requires OBI ≥ this
    obi_no_max: float = -0.50               # NO entry requires OBI ≤ this
    toxicity_bps_1s_max: float = 4.0        # max toxic flow (bps/s)
    settlement_window_seconds: float = 120.0 # seconds before expiry to block entry
    settlement_max_entry_price: float = 0.80 # hard cap on entry near settlement
    min_sigma: float = 0.15
    max_sigma: float = 2.50


@dataclass
class GateDecision:
    allow: bool
    reasons: List[str] = field(default_factory=list)
    reservation_price: Optional[float] = None
    obi: Optional[float] = None

    def __bool__(self) -> bool:
        return self.allow


def evaluate_gate(
    *,
    side: str,                         # "yes" | "no"
    entry_price: float,
    fair_yes: float,
    hours_to_expiry: float,
    obi: Optional[float] = None,
    vpin: Optional[float] = None,
    cfg: GateConfig = GateConfig(),
) -> GateDecision:
    """
    Evaluate the institutional entry gate.

    Returns a GateDecision with allow=True/False and a list of reason codes.
    On allow=False, reasons explain which check blocked the entry.
    """
    reasons: List[str] = []

    # Derive fair value for the side being traded
    fair_side = fair_yes if side == "yes" else (1.0 - fair_yes)

    # 1. Reservation price check
    reservation = fair_side - cfg.gamma
    if entry_price > reservation:
        reasons.append(f"price_above_reservation:{entry_price:.4f}>{reservation:.4f}")

    # 2. Settlement risk: block entry when close to expiry at high price
    seconds_to_expiry = hours_to_expiry * 3600.0
    if seconds_to_expiry <= cfg.settlement_window_seconds and entry_price >= cfg.settlement_max_entry_price:
        reasons.append(
            f"settlement_risk:entry={entry_price:.4f} "
            f"expiry_in={seconds_to_expiry:.0f}s"
        )

    # 3. OBI check (only when OBI data is available)
    if obi is not None:
        if side == "yes" and obi < cfg.obi_yes_min:
            reasons.append(f"obi_unfavorable_yes:{obi:.3f}<{cfg.obi_yes_min}")
        elif side == "no" and obi > cfg.obi_no_max:
            reasons.append(f"obi_unfavorable_no:{obi:.3f}>{cfg.obi_no_max}")

    # 4. VPIN / toxicity check
    if vpin is not None and vpin > cfg.toxicity_bps_1s_max:
        reasons.append(f"toxicity_too_high:{vpin:.2f}>{cfg.toxicity_bps_1s_max}")

    allow = len(reasons) == 0
    return GateDecision(
        allow=allow,
        reasons=reasons,
        reservation_price=round(reservation, 4),
        obi=obi,
    )
