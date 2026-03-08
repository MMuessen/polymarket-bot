from __future__ import annotations
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "improver"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEFAULTS = {
    "enabled": False,
    "dry_run": True,
    "auto_promote": False,
    "observer_interval_minutes": 20,
    "max_openai_calls_per_day": 4,
    "max_ollama_calls_per_day": 96,
    "max_changed_files": 5,
    "max_patch_chars": 120000,
    "allow_paths": [
        "app/main.py",
        "app/debug_handoff.py",
        "app/static/app.js",
        "app/templates/index.html",
        "app/strategy_lab/shadow_engine.py",
        "app/strategy_lab/outcomes.py",
        "app/strategy_lab/suggestions.py",
        "app/strategy_lab/gui_api.py",
        "app/strategy_lab/config.py",
        "app/strategy_lab/operator_settings.py",
        "app/strategy_lab/storage.py",
        "app/strategy_lab/schema.sql",
        "improver/"
    ],
    "deny_paths": [
        ".env",
        "docker-compose.yml",
        "requirements.txt",
        "data/trades.db",
        "data/strategy_lab.db"
    ],
    "protected_keywords": [
        "live_armed = True",
        "setMode('live')",
        "kalshi_private_key",
        "OPENAI_API_KEY",
        "os.environ[\"OPENAI_API_KEY\"]"
    ],
    "ollama_model": os.getenv("IMPROVER_OLLAMA_MODEL", "llama3.2:3b"),
    "openai_model": os.getenv("IMPROVER_OPENAI_MODEL", "gpt-5"),
    "use_openai_background": False,
}

CONFIG_PATH = DATA_DIR / "config.json"

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(DEFAULTS, indent=2))
        return dict(DEFAULTS)
    try:
        raw = json.loads(CONFIG_PATH.read_text())
    except Exception:
        raw = {}
    merged = dict(DEFAULTS)
    merged.update(raw)
    return merged

def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, sort_keys=True))
