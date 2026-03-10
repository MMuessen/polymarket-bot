from datetime import datetime, timezone
from app.strategy_lab.storage import get_conn

def _utc_now():
    return datetime.now(timezone.utc).isoformat()

def get_operator_settings():
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT strategy_name, variant_name, parameter_name, value_text, source, applied, ts
            FROM operator_settings
            ORDER BY id DESC
        """).fetchall()

        seen = set()
        out = []
        for r in rows:
            key = (r["strategy_name"], r["variant_name"], r["parameter_name"])
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(r))
        return out
    finally:
        conn.close()

def set_operator_setting(strategy_name, variant_name, parameter_name, value_text, source="manual", applied=0):
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO operator_settings (
                ts, strategy_name, variant_name, parameter_name, value_text, source, applied
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            _utc_now(),
            strategy_name,
            variant_name,
            parameter_name,
            str(value_text),
            source,
            int(applied),
        ))
        conn.commit()
    finally:
        conn.close()

def _coerce_operator_value(parameter_name, value_text):
    name = (parameter_name or "").strip().lower()
    raw = (value_text or "").strip()

    if name in {
        "min_edge", "max_spread", "max_ticket_dollars", "max_hours",
        "min_hours", "max_hours_to_expiry", "min_hours_to_expiry",
        "cooldown_seconds", "take_profit", "stop_loss", "max_hold_hours",
        "force_exit_near_expiry_hours", "size_multiplier", "exposure_multiplier"
    }:
        try:
            return float(raw)
        except Exception:
            return raw

    if name in {"enabled", "live_enabled"}:
        return raw.lower() in {"1", "true", "yes", "on"}

    return raw

def get_latest_strategy_operator_overrides():
    rows = get_operator_settings()
    rows = sorted(rows, key=lambda r: (r.get("ts") or ""), reverse=True)

    out = {}
    for r in rows:
        strategy_name = r.get("strategy_name")
        parameter_name = r.get("parameter_name")
        if not strategy_name or not parameter_name:
            continue

        bucket = out.setdefault(strategy_name, {})
        if parameter_name in bucket:
            continue

        bucket[parameter_name] = {
            "value": _coerce_operator_value(parameter_name, r.get("value_text")),
            "value_text": r.get("value_text"),
            "variant_name": r.get("variant_name"),
            "source": r.get("source"),
            "ts": r.get("ts"),
            "applied": int(r.get("applied") or 0),
        }
    return out

def mark_operator_setting_applied(strategy_name, parameter_name):
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE operator_settings
            SET applied = 1
            WHERE strategy_name = ? AND parameter_name = ?
            """,
            (strategy_name, parameter_name),
        )
        conn.commit()
    finally:
        conn.close()

def apply_operator_settings_to_bot(bot):
    supported_map = {
        "min_edge": "min_edge",
        "max_spread": "max_spread",
        "max_ticket_dollars": "max_ticket_dollars",
        "cooldown_seconds": "cooldown_seconds",
        "max_hours": "max_hours_to_expiry",
        "min_hours": "min_hours_to_expiry",
        "max_hours_to_expiry": "max_hours_to_expiry",
        "min_hours_to_expiry": "min_hours_to_expiry",
        "enabled": "enabled",
        "live_enabled": "live_enabled",
    }

    overrides = get_latest_strategy_operator_overrides()
    applied = []
    unsupported = []

    strategy_states = getattr(bot, "strategy_states", {}) or {}
    for strategy_name, params in overrides.items():
        state = strategy_states.get(strategy_name)
        if state is None:
            unsupported.append({
                "strategy_name": strategy_name,
                "reason": "unknown_strategy",
                "parameters": list(params.keys()),
            })
            continue

        for parameter_name, meta in params.items():
            attr = supported_map.get(parameter_name)
            if not attr or not hasattr(state, attr):
                unsupported.append({
                    "strategy_name": strategy_name,
                    "parameter_name": parameter_name,
                    "value": meta.get("value"),
                    "reason": "unsupported_runtime_parameter",
                    "variant_name": meta.get("variant_name"),
                })
                continue

            value = meta.get("value")
            setattr(state, attr, value)
            mark_operator_setting_applied(strategy_name, parameter_name)
            applied.append({
                "strategy_name": strategy_name,
                "parameter_name": parameter_name,
                "value": value,
                "variant_name": meta.get("variant_name"),
                "source": meta.get("source"),
                "ts": meta.get("ts"),
            })

    payload = {
        "applied": applied,
        "unsupported": unsupported,
        "raw": overrides,
    }

    try:
        bot.effective_operator_overrides = payload
    except Exception:
        pass

    return payload
