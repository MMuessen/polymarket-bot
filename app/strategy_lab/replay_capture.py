from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = Path("data/strategy_lab.db")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, "", "null"):
            return default
        return float(v)
    except Exception:
        return default


def _row_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if hasattr(v, "keys"):
        try:
            return {k: v[k] for k in v.keys()}
        except Exception:
            pass
    try:
        return dict(v)
    except Exception:
        return {}


def _parse_dt(v: Any) -> Optional[datetime]:
    try:
        if not v:
            return None
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
    except Exception:
        return []


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables() -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spot_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                symbol TEXT NOT NULL,
                spot REAL NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_spot_samples_symbol_ts ON spot_samples(symbol, ts)")
        conn.commit()
    finally:
        conn.close()


def capture_spot_sample(bot: Any) -> Dict[str, Any]:
    ensure_tables()
    spot_prices = getattr(bot, "spot_prices", {}) or {}
    spot = _safe_float(spot_prices.get("BTC"), 0.0)
    if spot <= 0:
        return {"ok": False, "reason": "no_btc_spot"}

    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO spot_samples (ts, symbol, spot) VALUES (?, ?, ?)",
            (_now_iso(), "BTC", spot),
        )
        conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM spot_samples WHERE symbol='BTC'").fetchone()[0]
        last_60s = conn.execute(
            """
            SELECT COUNT(*) FROM spot_samples
            WHERE symbol='BTC' AND ts >= ?
            """,
            ((_now_utc() - timedelta(seconds=60)).isoformat(),),
        ).fetchone()[0]
    finally:
        conn.close()

    return {
        "ok": True,
        "spot": round(spot, 4),
        "total_samples": int(total),
        "samples_last_60s": int(last_60s),
    }


def _current_twap(symbol: str = "BTC", seconds: int = 60) -> Tuple[Optional[float], int]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT spot FROM spot_samples
            WHERE symbol=? AND ts >= ?
            ORDER BY ts ASC
            """,
            (symbol, (_now_utc() - timedelta(seconds=seconds)).isoformat()),
        ).fetchall()
    finally:
        conn.close()

    vals = [_safe_float(r["spot"], 0.0) for r in rows]
    vals = [v for v in vals if v > 0]
    if not vals:
        return None, 0
    return float(round(mean(vals), 4)), len(vals)


def _twap_for_window(symbol: str, end_dt: datetime, seconds: int = 60) -> Tuple[Optional[float], int]:
    start_dt = end_dt - timedelta(seconds=seconds)
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT spot FROM spot_samples
            WHERE symbol=? AND ts >= ? AND ts <= ?
            ORDER BY ts ASC
            """,
            (symbol, start_dt.isoformat(), end_dt.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    vals = [_safe_float(r["spot"], 0.0) for r in rows]
    vals = [v for v in vals if v > 0]
    if not vals:
        return None, 0
    return float(round(mean(vals), 4)), len(vals)


def _distance_bucket_from_pct(v: Any) -> str:
    pct = _safe_float(v, -1.0)
    if pct < 0:
        return "unknown"
    if pct < 0.005:
        return "atm_0_0.5pct"
    if pct < 0.015:
        return "near_0.5_1.5pct"
    if pct < 0.03:
        return "mid_1.5_3pct"
    return "far_3pct_plus"


def _hours_bucket(v: Any) -> str:
    h = _safe_float(v, -1.0)
    if h < 0:
        return "unknown"
    if h < 2:
        return "lt_2h"
    if h < 6:
        return "2_6h"
    if h < 12:
        return "6_12h"
    if h < 24:
        return "12_24h"
    return "24h_plus"


def resolve_market_opportunities(limit: int = 500) -> Dict[str, Any]:
    ensure_tables()
    conn = get_conn()
    try:
        if not _table_exists(conn, "market_opportunities"):
            return {
                "ok": False,
                "reason": "market_opportunities_missing",
                "seen": 0,
                "resolved": 0,
                "skipped_no_twap": 0,
            }

        cols = set(_table_columns(conn, "market_opportunities"))
        required = {
            "id", "close_time", "best_side", "yes_ask", "no_ask",
            "result_yes", "market_result", "pnl_if_yes", "pnl_if_no",
            "best_side_correct", "best_side_realized_pnl", "resolved_at",
        }
        if not required.issubset(cols):
            return {
                "ok": False,
                "reason": "market_opportunities_missing_required_columns",
                "seen": 0,
                "resolved": 0,
                "skipped_no_twap": 0,
            }

        select_cols = [
            "id", "ticker", "market_family", "close_time", "best_side",
            "yes_ask", "no_ask", "floor_strike", "cap_strike",
            "threshold", "direction", "distance_bucket",
            "distance_from_spot_pct", "hours_to_expiry",
        ]
        select_cols = [c for c in select_cols if c in cols]

        rows = conn.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM market_opportunities
            WHERE close_time IS NOT NULL
              AND (resolved_at IS NULL OR resolved_at = '')
              AND close_time <= ?
            ORDER BY close_time ASC
            LIMIT ?
            """,
            (_now_iso(), int(limit)),
        ).fetchall()

        seen = 0
        resolved = 0
        skipped_no_twap = 0

        for raw in rows:
            row = _row_dict(raw)
            seen += 1

            close_dt = _parse_dt(row.get("close_time"))
            if close_dt is None:
                continue

            twap, sample_count = _twap_for_window("BTC", close_dt, 60)
            if twap is None or sample_count < 3:
                skipped_no_twap += 1
                continue

            market_family = str(row.get("market_family") or "").strip().lower()
            result_yes: Optional[int] = None

            floor_strike = row.get("floor_strike")
            cap_strike = row.get("cap_strike")
            threshold = row.get("threshold")
            direction = str(row.get("direction") or "").strip().lower()

            if market_family == "range_bucket" or (floor_strike not in (None, "") and cap_strike not in (None, "")):
                low = _safe_float(floor_strike, 0.0)
                high = _safe_float(cap_strike, 0.0)
                result_yes = 1 if low <= twap <= high else 0
            elif threshold not in (None, "") and direction in {"above", "below"}:
                thr = _safe_float(threshold, 0.0)
                if direction == "above":
                    result_yes = 1 if twap >= thr else 0
                else:
                    result_yes = 1 if twap <= thr else 0

            if result_yes is None:
                continue

            yes_ask = _safe_float(row.get("yes_ask"), 0.0)
            no_ask = _safe_float(row.get("no_ask"), 0.0)
            best_side = str(row.get("best_side") or "").strip().lower()

            pnl_if_yes = round((1.0 - yes_ask) if result_yes == 1 else (-yes_ask), 4) if yes_ask > 0 else None
            pnl_if_no = round((1.0 - no_ask) if result_yes == 0 else (-no_ask), 4) if no_ask > 0 else None
            market_result = "yes" if result_yes == 1 else "no"

            if best_side == "yes":
                best_side_correct = 1 if result_yes == 1 else 0
                best_side_realized_pnl = pnl_if_yes
            elif best_side == "no":
                best_side_correct = 1 if result_yes == 0 else 0
                best_side_realized_pnl = pnl_if_no
            else:
                best_side_correct = None
                best_side_realized_pnl = None

            distance_bucket = row.get("distance_bucket")
            if not distance_bucket and "distance_from_spot_pct" in row:
                distance_bucket = _distance_bucket_from_pct(row.get("distance_from_spot_pct"))

            updates = {
                "market_result": market_result,
                "result_yes": int(result_yes),
                "pnl_if_yes": pnl_if_yes,
                "pnl_if_no": pnl_if_no,
                "best_side_correct": best_side_correct,
                "best_side_realized_pnl": best_side_realized_pnl,
                "resolved_at": _now_iso(),
            }
            if "distance_bucket" in cols and distance_bucket:
                updates["distance_bucket"] = distance_bucket

            assignments = ", ".join([f"{k}=?" for k in updates.keys()])
            values = list(updates.values()) + [int(row["id"])]

            conn.execute(
                f"UPDATE market_opportunities SET {assignments} WHERE id=?",
                values,
            )
            resolved += 1

        conn.commit()

        total_resolved = conn.execute(
            """
            SELECT COUNT(*) FROM market_opportunities
            WHERE resolved_at IS NOT NULL AND resolved_at != ''
            """
        ).fetchone()[0]

        pending = conn.execute(
            """
            SELECT COUNT(*) FROM market_opportunities
            WHERE close_time IS NOT NULL
              AND close_time <= ?
              AND (resolved_at IS NULL OR resolved_at = '')
            """,
            (_now_iso(),),
        ).fetchone()[0]

    finally:
        conn.close()

    return {
        "ok": True,
        "seen": int(seen),
        "resolved": int(resolved),
        "skipped_no_twap": int(skipped_no_twap),
        "resolved_total": int(total_resolved),
        "pending_expired_unresolved": int(pending),
    }


def build_historical_lookup(limit_rows: int = 50000) -> Dict[str, Any]:
    conn = get_conn()
    try:
        if not _table_exists(conn, "market_opportunities"):
            return {
                "source_table": None,
                "row_count_used": 0,
                "lookup_by_distance_side_hours": [],
            }

        cols = set(_table_columns(conn, "market_opportunities"))
        needed = {
            "best_side", "best_side_correct",
            "best_side_realized_pnl", "resolved_at",
        }
        if not needed.issubset(cols):
            return {
                "source_table": "market_opportunities",
                "row_count_used": 0,
                "lookup_by_distance_side_hours": [],
            }

        select_cols = [
            "best_side", "best_side_correct", "best_side_realized_pnl",
            "resolved_at", "distance_bucket", "distance_from_spot_pct", "hours_to_expiry",
        ]
        select_cols = [c for c in select_cols if c in cols]

        rows = conn.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM market_opportunities
            WHERE resolved_at IS NOT NULL AND resolved_at != ''
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit_rows),),
        ).fetchall()
    finally:
        conn.close()

    agg: Dict[tuple, List[Dict[str, Any]]] = {}
    for raw in rows:
        row = _row_dict(raw)
        side = str(row.get("best_side") or "").strip().lower()
        if side not in {"yes", "no"}:
            continue

        dist = str(row.get("distance_bucket") or "")
        if not dist:
            dist = _distance_bucket_from_pct(row.get("distance_from_spot_pct"))

        hrs = _hours_bucket(row.get("hours_to_expiry"))
        key = (dist, side, hrs)
        agg.setdefault(key, []).append(row)

    out = []
    for (dist, side, hrs), items in sorted(agg.items(), key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1], kv[0][2])):
        trades = len(items)
        wins = 0
        losses = 0
        flat = 0
        pnls = []

        for row in items:
            corr = row.get("best_side_correct")
            pnl = _safe_float(row.get("best_side_realized_pnl"), 0.0)
            pnls.append(pnl)

            if corr in (1, True, "1", "true", "True"):
                wins += 1
            elif corr in (0, False, "0", "false", "False"):
                losses += 1
            else:
                if abs(pnl) < 1e-12:
                    flat += 1
                elif pnl > 0:
                    wins += 1
                else:
                    losses += 1

        realized_pnl = round(sum(pnls), 4)
        out.append(
            {
                "distance_bucket": dist,
                "best_side": side,
                "hours_bucket": hrs,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "flat": flat,
                "win_rate": round((wins / trades), 4) if trades else 0.0,
                "realized_pnl": realized_pnl,
                "avg_pnl_per_trade": round((realized_pnl / trades), 4) if trades else 0.0,
            }
        )

    return {
        "source_table": "market_opportunities",
        "row_count_used": len(rows),
        "lookup_by_distance_side_hours": out[:50],
    }


def build_status(runtime_memo: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ensure_tables()
    runtime_memo = runtime_memo or {}

    conn = get_conn()
    try:
        total_samples = conn.execute("SELECT COUNT(*) FROM spot_samples WHERE symbol='BTC'").fetchone()[0]
        samples_last_60s = conn.execute(
            """
            SELECT COUNT(*) FROM spot_samples
            WHERE symbol='BTC' AND ts >= ?
            """,
            ((_now_utc() - timedelta(seconds=60)).isoformat(),),
        ).fetchone()[0]

        if _table_exists(conn, "market_opportunities"):
            resolved_total = conn.execute(
                """
                SELECT COUNT(*) FROM market_opportunities
                WHERE resolved_at IS NOT NULL AND resolved_at != ''
                """
            ).fetchone()[0]
            pending_total = conn.execute(
                """
                SELECT COUNT(*) FROM market_opportunities
                WHERE close_time IS NOT NULL
                  AND close_time <= ?
                  AND (resolved_at IS NULL OR resolved_at = '')
                """,
                (_now_iso(),),
            ).fetchone()[0]
        else:
            resolved_total = 0
            pending_total = 0
    finally:
        conn.close()

    twap, points = _current_twap("BTC", 60)
    lookup = build_historical_lookup()

    return {
        "twap_settlement_proxy": {
            "mode": "rolling_60s_twap_proxy_runtime_capture",
            "symbol": "BTC",
            "twap_60s_proxy": twap,
            "proxy_points": int(points),
            "architect_truth_note": "captured_each_refresh__promotion_still_replay_only",
        },
        "historical_lookup": lookup,
        "capture_runtime": {
            "spot_sample_count_total": int(total_samples),
            "spot_samples_last_60s": int(samples_last_60s),
            "resolved_market_opportunities_total": int(resolved_total),
            "pending_market_opportunities": int(pending_total),
            "last_runtime_memo": runtime_memo,
        },
    }


def build_plaintext_report(status: Dict[str, Any]) -> str:
    surf = _row_dict(status.get("architect_replay_capture") or {})
    twap = _row_dict(surf.get("twap_settlement_proxy") or {})
    lookup = _row_dict(surf.get("historical_lookup") or {})
    cap = _row_dict(surf.get("capture_runtime") or {})

    lines: List[str] = []
    lines.append(f"mode: {status.get('mode')}")
    lines.append(f"live_armed: {status.get('live_armed')}")
    lines.append(f"loop_error: {(_row_dict(status.get('health') or {})).get('loop_error')}")
    lines.append("")
    lines.append("twap_settlement_proxy:")
    lines.append(f"  mode: {twap.get('mode')}")
    lines.append(f"  twap_60s_proxy: {twap.get('twap_60s_proxy')}")
    lines.append(f"  proxy_points: {twap.get('proxy_points')}")
    lines.append("")
    lines.append("capture_runtime:")
    lines.append(f"  spot_sample_count_total: {cap.get('spot_sample_count_total')}")
    lines.append(f"  spot_samples_last_60s: {cap.get('spot_samples_last_60s')}")
    lines.append(f"  resolved_market_opportunities_total: {cap.get('resolved_market_opportunities_total')}")
    lines.append(f"  pending_market_opportunities: {cap.get('pending_market_opportunities')}")
    lines.append("")
    lines.append("historical_lookup_top:")
    for row in (lookup.get("lookup_by_distance_side_hours") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('best_side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} pnl={row.get('realized_pnl')}"
        )
    return "\n".join(lines)
