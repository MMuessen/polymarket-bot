from datetime import datetime, timezone
from app.strategy_lab.storage import get_conn, json_dumps
from app.strategy_lab.config import DEFAULT_VARIANTS


def _utc_now():
    return datetime.now(timezone.utc)


class SummaryAndSuggestionEngine:
    def __init__(self, bot):
        self.bot = bot

    def rebuild_summaries(self):
        conn = get_conn()
        try:
            conn.execute("DELETE FROM strategy_summaries")
            now = _utc_now().isoformat()
            inserted = 0

            candidate_rows = conn.execute(
                """
                SELECT
                    variant_name,
                    COUNT(*) AS candidate_count,
                    SUM(CASE WHEN eligible = 1 THEN 1 ELSE 0 END) AS eligible_count,
                    AVG(CASE WHEN edge IS NOT NULL THEN edge END) AS avg_edge,
                    AVG(CASE WHEN spread IS NOT NULL THEN spread END) AS avg_spread,
                    AVG(CASE WHEN hours_to_expiry IS NOT NULL THEN hours_to_expiry END) AS avg_hours,
                    AVG(CASE WHEN distance_from_spot_pct IS NOT NULL THEN distance_from_spot_pct END) AS avg_dist
                FROM candidate_events
                GROUP BY variant_name
                """
            ).fetchall()

            trade_rows = conn.execute(
                """
                SELECT
                    variant_name,
                    COUNT(*) AS trade_count,
                    SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS win_count,
                    SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS loss_count,
                    COALESCE(SUM(realized_pnl), 0.0) AS realized_pnl,
                    AVG(CASE WHEN realized_pnl IS NOT NULL THEN realized_pnl END) AS expectancy
                FROM shadow_trades
                GROUP BY variant_name
                """
            ).fetchall()

            trade_map = {r["variant_name"]: r for r in trade_rows}

            for c in candidate_rows:
                variant = c["variant_name"]
                t = trade_map.get(variant)
                trade_count = int((t["trade_count"] if t else 0) or 0)
                win_count = int((t["win_count"] if t else 0) or 0)
                loss_count = int((t["loss_count"] if t else 0) or 0)
                realized_pnl = float((t["realized_pnl"] if t else 0.0) or 0.0)
                expectancy = float((t["expectancy"] if t else 0.0) or 0.0)
                win_rate = (win_count / trade_count) if trade_count else 0.0
                score = (
                    float(c["eligible_count"] or 0) * 0.1
                    + max(float(c["avg_edge"] or 0.0), 0.0) * 100
                    + realized_pnl
                )

                for window_name in ("24h", "7d", "30d", "all"):
                    conn.execute(
                        """
                        INSERT INTO strategy_summaries (
                            ts, strategy_name, variant_name, window_name,
                            trade_count, win_count, loss_count, win_rate,
                            realized_pnl, avg_edge, avg_spread, avg_hours_to_expiry,
                            avg_distance_from_spot_pct, expectancy, max_drawdown, score, meta_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            now,
                            "crypto_lag",
                            variant,
                            window_name,
                            trade_count,
                            win_count,
                            loss_count,
                            win_rate,
                            realized_pnl,
                            float(c["avg_edge"] or 0.0),
                            float(c["avg_spread"] or 0.0),
                            float(c["avg_hours"] or 0.0),
                            float(c["avg_dist"] or 0.0),
                            expectancy,
                            0.0,
                            score,
                            json_dumps(
                                {
                                    "candidate_count": int(c["candidate_count"] or 0),
                                    "eligible_count": int(c["eligible_count"] or 0),
                                }
                            ),
                        ),
                    )
                    inserted += 1

            conn.commit()
            self.bot.log(f"Summary engine: rebuilt {inserted} summary rows.")
        finally:
            conn.close()

    def rebuild_suggestions(self):
        conn = get_conn()
        try:
            conn.execute("DELETE FROM parameter_suggestions")
            rows = conn.execute(
                """
                SELECT * FROM strategy_summaries
                WHERE window_name = 'all'
                ORDER BY score DESC, trade_count DESC
                """
            ).fetchall()

            if not rows:
                conn.commit()
                return

            best = rows[0]
            best_variant = best["variant_name"]
            defaults = DEFAULT_VARIANTS.get(best_variant, {})
            sample_size = int(best["trade_count"] or 0)
            confidence = "high" if sample_size >= 100 else "medium" if sample_size >= 30 else "low"

            suggestion_map = {
                "min_edge": str(defaults.get("min_edge")),
                "max_spread": str(defaults.get("max_spread")),
                "min_hours": str(defaults.get("min_hours")),
                "max_hours": str(defaults.get("max_hours")),
                "size_multiplier": str(defaults.get("size_multiplier")),
                "cooldown_seconds": str(defaults.get("cooldown_seconds")),
                "variant_preference": best_variant,
                "shadow_trade_count": str(sample_size),
                "shadow_win_rate": str(float(best["win_rate"] or 0.0)),
            }

            inserted = 0
            for parameter_name, suggested_value in suggestion_map.items():
                conn.execute(
                    """
                    INSERT INTO parameter_suggestions (
                        ts, strategy_name, variant_name, parameter_name,
                        current_value, suggested_value, confidence, sample_size,
                        basis_window, metric_name, explanation, applied
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        _utc_now().isoformat(),
                        "crypto_lag",
                        best_variant,
                        parameter_name,
                        str(defaults.get(parameter_name, "")),
                        suggested_value,
                        confidence,
                        sample_size,
                        "all",
                        "score",
                        f"Suggested from best current variant {best_variant}.",
                    ),
                )
                inserted += 1

            conn.commit()
            self.bot.log(f"Summary engine: rebuilt {inserted} parameter suggestions.")
        finally:
            conn.close()
