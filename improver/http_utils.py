from __future__ import annotations
import json
import urllib.request

def http_json(url: str, method: str = "GET", headers: dict | None = None, body: dict | None = None, timeout: int = 120):
    data = None
    hdrs = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)

def http_text(url: str, method: str = "GET", headers: dict | None = None, body: str | bytes | None = None, timeout: int = 120):
    data = None
    if isinstance(body, str):
        data = body.encode("utf-8")
    elif isinstance(body, bytes):
        data = body
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def safe_json(url: str):
    try:
        return {"ok": True, "value": http_json(url)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def safe_text(url: str):
    try:
        return {"ok": True, "value": http_text(url)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
