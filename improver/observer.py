from __future__ import annotations
from improver.http_utils import safe_json, safe_text
from improver.state import add_snapshot

def collect_runtime() -> dict:
    status = safe_json("http://localhost:8000/api/status")
    paper_controls = safe_json("http://localhost:8000/api/paper-controls")
    bundle = safe_text("http://localhost:8000/api/debug/full")
    memory = safe_text("http://localhost:8000/api/debug/handoff-memory")
    payload = {
        "status": status,
        "paper_controls": paper_controls,
        "bundle": bundle,
        "memory": memory,
    }
    add_snapshot("runtime", payload)
    return payload

def summarize_recent_results(runtime: dict) -> dict:
    status = ((runtime.get("status") or {}).get("value") or {}) if runtime.get("status", {}).get("ok") else {}
    strategy_lab = status.get("strategy_lab", {}) or {}
    paper = status.get("paper", {}) or {}
    health = status.get("health", {}) or {}
    pipeline = health.get("market_pipeline", {}) or {}

    return {
        "mode": status.get("mode"),
        "top_edge": status.get("top_edge"),
        "paper": paper,
        "eligible_markets": pipeline.get("eligible_markets"),
        "shadow_candidate_markets": pipeline.get("shadow_candidate_markets"),
        "loop_error": health.get("loop_error"),
        "strategy_lab": strategy_lab,
    }
