import json
import urllib.request
from datetime import datetime, timezone
from app.strategy_lab.storage import get_conn, rows_to_dicts

OLLAMA_URL = "http://ollama:11434/api/generate"
OLLAMA_MODEL = "llama3.2:3b"


def get_latest_summaries():
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM strategy_summaries
            ORDER BY ts DESC, score DESC
            LIMIT 200
            """
        ).fetchall()
        return rows_to_dicts(rows)
    finally:
        conn.close()


def get_latest_suggestions():
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM parameter_suggestions
            ORDER BY ts DESC
            LIMIT 100
            """
        ).fetchall()
        return rows_to_dicts(rows)
    finally:
        conn.close()


def get_shadow_positions():
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT * FROM shadow_positions
            ORDER BY opened_at DESC
            LIMIT 100
            """
        ).fetchall()
        return rows_to_dicts(rows)
    finally:
        conn.close()


def get_operator_settings():
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT strategy_name, variant_name, parameter_name, value_text, source, applied, ts
            FROM operator_settings
            ORDER BY ts DESC
            LIMIT 100
            """
        ).fetchall()
        return rows_to_dicts(rows)
    finally:
        conn.close()


def save_operator_setting(strategy_name: str, variant_name: str, parameter_name: str, value_text: str, source: str = "manual"):
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO operator_settings (
                ts, strategy_name, variant_name, parameter_name, value_text, source, applied
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                strategy_name,
                variant_name,
                parameter_name,
                value_text,
                source,
            ),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def ollama_advice(payload: dict):
    prompt = (
        "You are a bounded trading strategy advisor. "
        "You may only recommend one regime from {conservative, adaptive, aggressive}. "
        "Use the supplied performance and pipeline data. "
        "Return strict JSON with keys: regime, confidence, explanation, suggested_actions.\n\n"
        f"DATA:\n{json.dumps(payload, indent=2)}"
    )

    body = json.dumps(
        {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "format": "json",
        }
    ).encode()

    req = urllib.request.Request(
        OLLAMA_URL,
        data=body,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = json.loads(resp.read().decode())
            text = raw.get("response") or "{}"
            return json.loads(text)
    except Exception as exc:
        return {
            "regime": "conservative",
            "confidence": "low",
            "explanation": f"ollama unavailable: {exc}",
            "suggested_actions": [],
        }
