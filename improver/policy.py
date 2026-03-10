from __future__ import annotations
import re
from improver.config import load_config

def extract_bash_block(text: str) -> str | None:
    if not text:
        return None

    m = re.search(r"```bash\s*(.*?)```", text, re.S)
    if m:
        return m.group(1).strip() + "\n"

    m = re.search(r"```(?:sh)?\s*(.*?)```", text, re.S)
    if m:
        return m.group(1).strip() + "\n"

    # Fallback: accept plain shell-like output if the model forgot fences.
    shell_markers = [
        "cd ~/polymarket-bot",
        "cd /",
        "python3 - <<'PY'",
        "cat > ",
        "curl -s ",
        "docker-compose ",
        "pkill ",
        "echo \"===",
    ]
    if any(marker in text for marker in shell_markers):
        return text.strip() + "\n"

    return None

def patch_policy_check(script_text: str) -> dict:
    cfg = load_config()
    deny_hits = [p for p in cfg["deny_paths"] if p in script_text]
    kw_hits = [k for k in cfg["protected_keywords"] if k in script_text]
    allow_hits = [p for p in cfg["allow_paths"] if p in script_text]

    if deny_hits:
        return {"ok": False, "reason": "deny_paths_hit", "hits": deny_hits}
    if kw_hits:
        return {"ok": False, "reason": "protected_keywords_hit", "hits": kw_hits}
    if len(script_text) > int(cfg["max_patch_chars"]):
        return {"ok": False, "reason": "patch_too_large", "chars": len(script_text)}
    if not allow_hits:
        return {"ok": False, "reason": "no_allowlist_path_detected"}
    return {"ok": True}
