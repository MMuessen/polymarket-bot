from __future__ import annotations
import json
import os
from improver.config import load_config
from improver.http_utils import http_json
from improver.state import quota_inc, quota_used

OPENAI_URL = "https://api.openai.com/v1/responses"

def _extract_output_text(resp: dict) -> str:
    if resp.get("output_text"):
        return resp["output_text"]
    parts = []
    for item in resp.get("output", []):
        if item.get("type") == "message":
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"}:
                    txt = content.get("text")
                    if txt:
                        parts.append(txt)
    return "\n".join(parts).strip()

def fallback_task(reason: str, error: str | None = None) -> dict:
    return {
        "decision": "proceed",
        "priority": "high",
        "reason": reason,
        "task_title": "Fix expiry settlement truthfulness",
        "task_type": "code_patch",
        "safe_to_auto_apply": False,
        "requires_openai": True,
        "category": "strategy",
        "requested_outcome": "Replace any remaining proxy expiry resolution with truthful actual-outcome settlement so paper/shadow realized PnL reflects real bucket outcomes.",
        "planner_error": error,
        "planner_fallback": True,
    }

def plan_next_task(bundle_text: str, recent_results: dict) -> dict:
    cfg = load_config()

    # planner and coder now both use OpenAI, so this quota applies to both
    if quota_used("openai") >= int(cfg["max_openai_calls_per_day"]):
        return fallback_task("openai_daily_quota_reached")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return fallback_task("openai_api_key_missing")

    if not bundle_text.strip():
        return fallback_task("missing_debug_bundle")

    bundle_excerpt = bundle_text[:12000]
    planner_model = cfg.get("planner_model") or cfg.get("openai_model", "gpt-5")

    instructions = """
You are the planner for a safe trading-app improvement agent.

Choose exactly ONE safest, highest-value next task.

Rules:
- current phase is paper/shadow truthfulness and app hardening
- prefer tasks that improve truthful evaluation, observability, or stability
- do not suggest enabling live trading
- do not suggest changing secrets, environment, or DB files
- if the system already looks healthy, prefer the highest-value remaining truthfulness fix
- return strict JSON only

Return keys:
decision, priority, reason, task_title, task_type, safe_to_auto_apply, requires_openai, category, requested_outcome
""".strip()

    user_text = f"""
Recent results JSON:
{json.dumps(recent_results, indent=2)}

Bundle excerpt:
{bundle_excerpt}
""".strip()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    project_id = os.getenv("OPENAI_PROJECT_ID")
    org_id = os.getenv("OPENAI_ORG_ID")
    if project_id:
        headers["OpenAI-Project"] = project_id
    if org_id:
        headers["OpenAI-Organization"] = org_id

    try:
        resp = http_json(
            OPENAI_URL,
            method="POST",
            headers=headers,
            body={
                "model": planner_model,
                "instructions": instructions,
                "input": user_text,
                "max_output_tokens": 220,
            },
            timeout=90,
        )
        quota_inc("openai", 1)
    except Exception as e:
        return fallback_task("openai_planner_request_failed", str(e))

    raw = _extract_output_text(resp)
    try:
        parsed = json.loads(raw)
    except Exception:
        return fallback_task("openai_planner_invalid_json", raw[:1000])

    if not isinstance(parsed, dict):
        return fallback_task("openai_planner_non_dict_json", str(parsed)[:1000])

    return parsed
