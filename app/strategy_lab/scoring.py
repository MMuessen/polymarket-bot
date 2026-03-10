def score_summary(trade_count, win_rate, realized_pnl, expectancy, max_drawdown):
    sample_bonus = min((trade_count or 0) / 200.0, 1.0)
    dd_penalty = abs(max_drawdown or 0.0)
    return (
        0.40 * float(expectancy or 0.0)
        + 0.20 * float(realized_pnl or 0.0)
        + 0.10 * float(win_rate or 0.0)
        + 0.15 * sample_bonus
        - 0.15 * dd_penalty
    )
