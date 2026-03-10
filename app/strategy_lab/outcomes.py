from datetime import datetime, timezone
from app.strategy_lab.storage import get_conn


def _utc_now():
    return datetime.now(timezone.utc)


class OutcomeResolver:
    def __init__(self, bot):
        self.bot = bot

    def mark_positions(self):
        conn = get_conn()
        try:
            rows = conn.execute("SELECT * FROM shadow_positions").fetchall()
            for row in rows:
                snapshot = self.bot.market_snapshots.get(row["ticker"])
                if not snapshot or snapshot.mid is None:
                    continue

                entry = float(row["entry_price"] or 0.0)
                qty = int(row["qty"] or 0)
                mark = float(snapshot.mid)
                unrealized = (mark - entry) * qty

                best_mark = max(float(row["best_mark_price"] or mark), mark)
                worst_mark = min(float(row["worst_mark_price"] or mark), mark)

                conn.execute(
                    """
                    UPDATE shadow_positions
                    SET last_mark_price = ?, unrealized_pnl = ?, best_mark_price = ?, worst_mark_price = ?,
                        last_edge = ?, last_spread = ?
                    WHERE ticker = ? AND variant_name = ?
                    """,
                    (
                        mark,
                        unrealized,
                        best_mark,
                        worst_mark,
                        float(getattr(snapshot, "edge", 0.0) or 0.0),
                        float(getattr(snapshot, "spread", 0.0) or 0.0),
                        row["ticker"],
                        row["variant_name"],
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def resolve_finalized_positions(self):
        conn = get_conn()
        try:
            rows = conn.execute("SELECT * FROM shadow_positions").fetchall()
            now = _utc_now()

            for row in rows:
                ticker = row["ticker"]
                variant_name = row["variant_name"]
                snapshot = self.bot.market_snapshots.get(ticker)
                if not snapshot:
                    continue
                if not getattr(snapshot, "raw_close_time", None):
                    continue
                if snapshot.raw_close_time > now:
                    continue

                entry = float(row["entry_price"] or 0.0)
                qty = int(row["qty"] or 0)

                raw_spot = self.bot.spot_prices.get(getattr(snapshot, "spot_symbol", "BTC"))
                final_spot = float(raw_spot or 0.0)

                low = getattr(snapshot, "floor_strike", None)
                high = getattr(snapshot, "cap_strike", None)
                threshold = getattr(snapshot, "threshold", None)
                direction = str(getattr(snapshot, "direction", "") or "").lower()

                yes_wins = False

                if final_spot > 0:
                    if low is not None and high is not None:
                        yes_wins = float(low) <= final_spot <= float(high)
                    elif threshold is not None and direction == "above":
                        yes_wins = final_spot >= float(threshold)
                    elif threshold is not None and direction == "below":
                        yes_wins = final_spot <= float(threshold)
                    else:
                        # last-resort fallback if structure is incomplete
                        final_yes = float(getattr(snapshot, "fair_yes", 0.0) or 0.0)
                        yes_wins = final_yes >= 0.5
                else:
                    # no usable spot available, fallback to old model guess
                    final_yes = float(getattr(snapshot, "fair_yes", 0.0) or 0.0)
                    yes_wins = final_yes >= 0.5

                exit_price = 1.0 if yes_wins else 0.0
                realized = (exit_price - entry) * qty
                outcome = "win" if realized > 0 else "loss" if realized < 0 else "flat"

                trade_id_row = conn.execute(
                    """
                    SELECT id
                    FROM shadow_trades
                    WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (ticker, variant_name),
                ).fetchone()

                if trade_id_row:
                    trade_id = int(trade_id_row[0])
                    conn.execute(
                        """
                        UPDATE shadow_trades
                        SET closed_at = ?, exit_price = ?, realized_pnl = ?, outcome = ?, close_reason = ?,
                            edge_at_exit = ?, spread_at_exit = ?, distance_from_spot_pct_at_exit = ?
                        WHERE id = ?
                        """,
                        (
                            now.isoformat(),
                            exit_price,
                            realized,
                            outcome,
                            "expiry_resolution",
                            float(getattr(snapshot, "edge", 0.0) or 0.0),
                            float(getattr(snapshot, "spread", 0.0) or 0.0),
                            float(getattr(snapshot, "distance_from_spot_pct", 0.0) or 0.0),
                            trade_id,
                        ),
                    )

                conn.execute(
                    """
                    DELETE FROM shadow_positions
                    WHERE ticker = ? AND variant_name = ?
                    """,
                    (ticker, variant_name),
                )

            conn.commit()
        finally:
            conn.close()


# --- truthful expiry settlement helpers ---
import re
from datetime import datetime, timedelta, timezone
from app.strategy_lab.storage import get_conn

_truthful_expiry_settlement_helpers_installed = True

_BUCKET_CENTER_RE = re.compile(r"-B(\d+)$")
_ASSET_RE = re.compile(r"^KX([A-Z]+)-")

def _parse_iso_utc(ts: str | None):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _expiry_at_from_trade_row(row):
    opened_at = _parse_iso_utc(row["opened_at"])
    hours_to_expiry = row["hours_to_expiry_at_entry"]
    if opened_at is None or hours_to_expiry is None:
        return None
    try:
        return opened_at + timedelta(hours=float(hours_to_expiry))
    except Exception:
        return None

def _spot_symbol_from_ticker(ticker: str | None):
    if not ticker:
        return None
    m = _ASSET_RE.match(ticker)
    return m.group(1) if m else None

def _range_bucket_bounds_from_ticker(ticker: str | None):
    """
    Current crypto range-bucket tickers use the bucket CENTER in the -Bxxxxx suffix.
    Example: B66750 means $66,500 to $66,999.99.
    """
    if not ticker:
        return None
    m = _BUCKET_CENTER_RE.search(ticker)
    if not m:
        return None
    center = float(m.group(1))
    low = center - 250.0
    high = center + 249.99
    return (low, high)

def _side_wins(side: str | None, yes_wins: bool) -> bool:
    side = (side or "yes").lower()
    if side == "no":
        return not yes_wins
    return yes_wins

def count_expired_unsettled_shadow_trades(now=None) -> int:
    now = now or datetime.now(timezone.utc)
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT opened_at, hours_to_expiry_at_entry
            FROM shadow_trades
            WHERE closed_at IS NULL
            """
        ).fetchall()
        n = 0
        for row in rows:
            expiry_at = _expiry_at_from_trade_row(row)
            if expiry_at is not None and now >= expiry_at:
                n += 1
        return n
    finally:
        conn.close()

def settle_expired_shadow_positions(spot_prices: dict | None, now=None) -> int:
    """
    Truthful expiry settlement:
    - do NOT use fair_yes or any model proxy
    - settle YES/NO shadow trades from actual spot vs bucket bounds once expired
    - exit price is binary 1.0 / 0.0
    """
    spot_prices = spot_prices or {}
    now = now or datetime.now(timezone.utc)
    conn = get_conn()
    settled = 0

    try:
        rows = conn.execute(
            """
            SELECT ticker, variant_name, side, qty, entry_price, opened_at, hours_to_expiry_at_entry
            FROM shadow_trades
            WHERE closed_at IS NULL
            """
        ).fetchall()

        for row in rows:
            expiry_at = _expiry_at_from_trade_row(row)
            if expiry_at is None or now < expiry_at:
                continue

            ticker = row["ticker"]
            variant_name = row["variant_name"]
            side = row["side"] or "yes"
            qty = int(row["qty"] or 0)
            entry_price = float(row["entry_price"] or 0.0)

            asset = _spot_symbol_from_ticker(ticker)
            bounds = _range_bucket_bounds_from_ticker(ticker)
            spot = spot_prices.get(asset)

            if not asset or not bounds or spot is None:
                continue

            try:
                spot = float(spot)
            except Exception:
                continue

            low, high = bounds
            yes_wins = (spot >= low and spot <= high)
            trade_wins = _side_wins(side, yes_wins)

            exit_price = 1.0 if trade_wins else 0.0
            realized = (exit_price - entry_price) * qty
            outcome = "win" if trade_wins else "loss"
            close_reason = "expiry_settlement_actual_spot"

            conn.execute(
                """
                UPDATE shadow_trades
                SET closed_at = ?, exit_price = ?, realized_pnl = ?, outcome = ?, close_reason = ?
                WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                """,
                (
                    now.isoformat(),
                    exit_price,
                    realized,
                    outcome,
                    close_reason,
                    ticker,
                    variant_name,
                ),
            )
            conn.execute(
                "DELETE FROM shadow_positions WHERE ticker = ? AND variant_name = ?",
                (ticker, variant_name),
            )
            settled += 1

        conn.commit()
        return settled
    finally:
        conn.close()



# --- architect settlement reference proxy (rolling 60s if available) ---
try:
    from datetime import datetime, timezone

    _architect_settlement_reference_proxy_applied = True
    _orig_resolve_finalized_positions_architect_proxy = OutcomeResolver.resolve_finalized_positions

    def _arch_ts_to_dt(v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        try:
            s = str(v)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None

    def _arch_collect_spot_points(self, symbol="BTC"):
        out = []

        candidate_attrs = [
            "spot_history",
            "spot_price_history",
            "recent_spot_history",
            "recent_spots",
            "price_history",
            "prices_history",
        ]

        for attr in candidate_attrs:
            hist = getattr(self, attr, None)
            if hist is None:
                continue

            if isinstance(hist, dict):
                seq = hist.get(symbol) or hist.get(symbol.upper()) or []
            else:
                seq = hist

            if not seq:
                continue

            for item in list(seq)[-240:]:
                if isinstance(item, dict):
                    ts = _arch_ts_to_dt(item.get("ts") or item.get("time") or item.get("timestamp"))
                    px = item.get("price") or item.get("spot") or item.get("value")
                elif isinstance(item, (tuple, list)) and len(item) >= 2:
                    ts = _arch_ts_to_dt(item[0])
                    px = item[1]
                else:
                    ts = None
                    px = None
                try:
                    px = float(px)
                except Exception:
                    px = None
                if ts is not None and px is not None and px > 0:
                    out.append((ts, px))

        out.sort(key=lambda x: x[0])
        return out

    def _arch_rolling_60s_avg(self, symbol="BTC", fallback=None):
        try:
            now = datetime.now(timezone.utc)
            pts = _arch_collect_spot_points(self, symbol=symbol)
            recent = [px for ts, px in pts if (now - ts).total_seconds() <= 60]
            if recent:
                return sum(recent) / len(recent)
        except Exception:
            pass
        return fallback

    def _resolve_finalized_positions_architect_proxy(self, *args, **kwargs):
        source = "instantaneous_spot_fallback"
        replacements = []

        for attr in ["spot_prices", "latest_spot_prices", "last_spot_prices", "prices"]:
            mapping = getattr(self, attr, None)
            if not isinstance(mapping, dict):
                continue
            if "BTC" not in mapping:
                continue

            try:
                raw = float(mapping.get("BTC") or 0.0)
            except Exception:
                raw = 0.0
            if raw <= 0:
                continue

            proxy = _arch_rolling_60s_avg(self, symbol="BTC", fallback=raw)
            if proxy and proxy > 0:
                replacements.append((mapping, "BTC", raw))
                mapping["BTC"] = proxy
                source = "rolling_60s_coinbase_proxy_for_briti"

        try:
            result = _orig_resolve_finalized_positions_architect_proxy(self, *args, **kwargs)
            try:
                self.last_settlement_reference_source = source
            except Exception:
                pass
            return result
        finally:
            for mapping, key, raw in replacements:
                try:
                    mapping[key] = raw
                except Exception:
                    pass

    OutcomeResolver.resolve_finalized_positions = _resolve_finalized_positions_architect_proxy
except Exception:
    pass

