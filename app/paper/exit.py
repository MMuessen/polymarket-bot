"""
Exit evaluation — pure functions.

Determines whether an open position should be closed, and why.
No I/O, no side effects — fully testable.

Exit conditions (evaluated in priority order):
  1. Take-profit: mark - entry >= take_profit threshold
  2. Stop-loss: mark - entry <= -stop_loss threshold
  3. Profit giveback: was up 8c+, now back to +2c
  4. Model deterioration: best_exec_edge dropped significantly from entry
  5. Time exit: < 20 min to expiry
  6. Spread blowout: spread >= 0.12
  7. Market expired / delisted
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class ExitConfig:
    take_profit: float = 0.10          # per-contract P&L threshold
    stop_loss: float = 0.06            # per-contract loss threshold (positive value)
    profit_giveback_peak: float = 0.08 # giveback triggers after this peak gain
    profit_giveback_floor: float = 0.02
    min_hours_remaining: float = 0.33  # 20 minutes
    max_spread: float = 0.12
    model_deterioration_threshold: float = 0.0  # exit if current edge < this


def evaluate_exit(
    *,
    side: str,
    entry_price: float,
    qty: int,
    mark: float,                         # current bid on the position's side
    best_mark: float,                    # highest mark ever seen for this position
    hours_to_expiry: Optional[float],
    spread_now: Optional[float],
    best_exec_edge_now: Optional[float],
    cfg: ExitConfig = ExitConfig(),
) -> Tuple[bool, Optional[str]]:
    """
    Evaluate whether to exit an open position.

    Returns (should_exit, reason_code) where reason_code is None when not exiting.

    mark: the current exit price (yes_bid for YES positions, no_bid for NO positions).
    """
    pnl_per_contract = mark - entry_price

    # 1. Take profit
    if pnl_per_contract >= cfg.take_profit:
        return True, f"take_profit_{int(cfg.take_profit*100)}c"

    # 2. Stop loss
    if pnl_per_contract <= -cfg.stop_loss:
        return True, f"stop_loss_{int(cfg.stop_loss*100)}c"

    # 3. Profit giveback
    peak_gain = best_mark - entry_price
    if peak_gain >= cfg.profit_giveback_peak and pnl_per_contract <= cfg.profit_giveback_floor:
        return True, "profit_giveback"

    # 4. Model deterioration (edge fallen to zero or below)
    if best_exec_edge_now is not None and best_exec_edge_now <= cfg.model_deterioration_threshold:
        return True, "model_deterioration_v2"

    # 5. Time exit
    if hours_to_expiry is not None and hours_to_expiry <= cfg.min_hours_remaining:
        return True, "time_exit_20m"

    # 6. Spread blowout
    if spread_now is not None and spread_now >= cfg.max_spread:
        return True, "spread_blowout"

    return False, None
