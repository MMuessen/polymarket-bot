from __future__ import annotations
import os
from improver.config import load_config
from improver.http_utils import http_json
from improver.state import quota_inc, quota_used

OPENAI_URL = "https://api.openai.com/v1/responses"

def extract_output_text(resp: dict) -> str:
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

def build_request(bundle_text: str, recent_results: dict, planner: dict) -> dict:
    cfg = load_config()
    instructions = """
You are generating a safe terminal-only patch for a local Python/FastAPI trading research application.

Constraints:
- ultimate objective is profitable live trading, but current work must improve truthful paper/shadow validation
- DO NOT enable live trading
- DO NOT modify secrets, .env, docker host config, or external credentials
- DO NOT require manual file editing
- patch only files needed for the requested task
- keep changes minimal and coherent
- include verification commands
- include a final POST to /api/debug/handoff-note summarizing what changed

OUTPUT RULE:
- Return ONLY one fenced bash block.
- The first line must be ```bash
- The last line must be ```
- Do not include any prose before or after the bash block.
""".strip()

    user_text = f"""
Generate one terminal-only copy/paste patch for the requested task.

TASK:
planner={planner}

RECENT_RESULTS:
{recent_results}

BUNDLE:
{bundle_text[:120000]}
""".strip()

    return {
        "model": cfg["openai_model"],
        "instructions": instructions,
        "input": user_text
    }

def request_patch(bundle_text: str, recent_results: dict, planner: dict) -> dict:
    cfg = load_config()
    if quota_used("openai") >= int(cfg["max_openai_calls_per_day"]):
        return {"ok": False, "error": "openai_daily_quota_reached"}

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {"ok": False, "error": "OPENAI_API_KEY missing"}

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

    body = build_request(bundle_text, recent_results, planner)
    if bool(cfg.get("use_openai_background")):
        body["background"] = True

    resp = http_json(OPENAI_URL, method="POST", headers=headers, body=body, timeout=300)
    quota_inc("openai", 1)

    return {
        "ok": True,
        "response": resp,
        "output_text": extract_output_text(resp),
    }
