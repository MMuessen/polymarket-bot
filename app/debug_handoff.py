
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
APP_DIR = PROJECT_ROOT / "app"
DATA_DIR = PROJECT_ROOT / "data"
MEMORY_PATH = DATA_DIR / "ai_handoff_memory.md"
STRATEGY_DB = DATA_DIR / "strategy_lab.db"
ENV_PATH = PROJECT_ROOT / ".env"

IMPORTANT_FILES = [
    ("app/main.py", "Primary FastAPI app, runtime bot logic, routes, dashboard, paper/live flow."),
    ("app/templates/index.html", "Operator dashboard HTML."),
    ("app/static/app.js", "Frontend rendering and actions."),
    ("app/strategy_lab/shadow_engine.py", "Shadow trade entry/mark/exit logic."),
    ("app/strategy_lab/outcomes.py", "Expiry/final outcome resolution."),
    ("app/strategy_lab/suggestions.py", "Strategy summaries and parameter suggestions."),
    ("app/strategy_lab/gui_api.py", "Strategy lab API helpers."),
    ("app/strategy_lab/config.py", "Default strategy variants."),
    ("app/strategy_lab/operator_settings.py", "Persistent overrides/settings."),
    ("app/strategy_lab/storage.py", "Strategy lab DB helpers."),
    ("app/strategy_lab/schema.sql", "Strategy lab schema."),
    ("docker-compose.yml", "Container orchestration."),
    ("requirements.txt", "Dependencies."),
]

DB_TABLES = [
    "candidate_events",
    "shadow_trades",
    "shadow_positions",
    "strategy_summaries",
    "parameter_suggestions",
    "operator_settings",
    "operator_state",
]

DEFAULT_MEMORY = """# AI HANDOFF MEMORY

## Project Identity
- Project: Kalshi crypto range-bucket operator console
- Ultimate goal: build a profitable real-money trading system for Kalshi crypto range-bucket markets
- Current phase: paper/shadow validation before live deployment
- Current priority: truthful paper accounting, realistic shadow exits, correct expiry settlement, stable operator controls, portable AI handoff bundle
- Important framing: paper trading is not the end goal; it is a proving ground used to decide whether the system deserves live capital

## Operator Preferences
- Terminal-only copy/paste solutions
- No manual file editing
- Prefer larger complete patches over tiny fragmented ones
- Preserve working behavior unless intentionally replacing it
- Bundle should be enough to continue work in a brand new chat or another AI tool

## Confirmed Learnings
- Paper trading must be enable/disable/reset capable at any time
- Full debug bundle must also act as project memory and AI handoff context
- Duplicate routes and mixed old/new endpoints have caused instability before
- If paper equity dips below starting balance and then snaps back after closes, accounting is likely wrong
- Using mid-price for exits is too optimistic for realistic YES liquidation
- Using fair_yes >= 0.5 for expiry settlement is a model guess, not true market settlement
- The bundle must frame live profitability as the end objective and paper/shadow as validation layers

## Current Mission
- Keep the handoff bundle self-contained and always up to date
- Make paper/shadow evaluation truthful enough to decide if and when live trading should begin
- Preserve enough context that a new AI can continue immediately without extra explanation
- Improve the trading system toward real-money profitability without pretending it is ready before the evidence supports that

## How Future AI Should Work
- Read the full bundle first
- Distinguish confirmed facts from guesses
- Summarize architecture, confirmed issues, and highest-priority next fix
- Then provide terminal-only copy/paste patches
- Treat profitable live trading as the ultimate objective and paper/shadow accuracy as the current gate
"""

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_memory_file() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not MEMORY_PATH.exists():
        MEMORY_PATH.write_text(DEFAULT_MEMORY)

def append_memory_note(title: str, content: str, category: str = "general") -> None:
    ensure_memory_file()
    ts = _utc_now()
    block = f"""

## Note [{category}] {ts}
### {title}
{content}
"""
    existing = MEMORY_PATH.read_text(errors="replace")
    MEMORY_PATH.write_text(existing.rstrip() + block + "\n")

def read_memory_text() -> str:
    ensure_memory_file()
    return MEMORY_PATH.read_text(errors="replace")

def safe_read(relpath: str) -> str:
    p = PROJECT_ROOT / relpath
    if not p.exists():
        return f"[missing] {relpath}"
    try:
        return p.read_text(errors="replace")
    except Exception as e:
        return f"[read error] {relpath}: {e}"

def sql_rows(query: str, params=()) -> List[Dict[str, Any]]:
    if not STRATEGY_DB.exists():
        return [{"error": f"missing db: {STRATEGY_DB}"}]
    conn = sqlite3.connect(STRATEGY_DB)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(query, params).fetchall()]
    except Exception as e:
        return [{"error": str(e), "query": query}]
    finally:
        conn.close()

def sql_scalar(query: str, params=(), default=None):
    if not STRATEGY_DB.exists():
        return default
    conn = sqlite3.connect(STRATEGY_DB)
    try:
        row = conn.execute(query, params).fetchone()
        return default if not row else row[0]
    except Exception:
        return default
    finally:
        conn.close()

def table_info(table_name: str) -> List[Dict[str, Any]]:
    if not STRATEGY_DB.exists():
        return [{"error": f"missing db: {STRATEGY_DB}"}]
    conn = sqlite3.connect(STRATEGY_DB)
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [
            {
                "cid": r[0],
                "name": r[1],
                "type": r[2],
                "notnull": r[3],
                "default_value": r[4],
                "pk": r[5],
            }
            for r in rows
        ]
    except Exception as e:
        return [{"error": str(e), "table": table_name}]
    finally:
        conn.close()

def parse_routes_from_main() -> List[Dict[str, str]]:
    text = safe_read("app/main.py")
    routes = []
    for m in re.finditer(r'@app\.(get|post|put|delete|patch)\("([^"]+)"\)', text):
        routes.append({"method": m.group(1).upper(), "path": m.group(2)})
    return routes

def env_key_inventory() -> List[str]:
    keys = []
    if ENV_PATH.exists():
        try:
            for line in ENV_PATH.read_text(errors="replace").splitlines():
                raw = line.strip()
                if not raw or raw.startswith("#") or "=" not in raw:
                    continue
                key = raw.split("=", 1)[0].strip()
                if key:
                    keys.append(key)
        except Exception:
            pass
    return sorted(set(keys))

def detect_code_health() -> Dict[str, Any]:
    main_py = safe_read("app/main.py")
    shadow_py = safe_read("app/strategy_lab/shadow_engine.py")
    outcomes_py = safe_read("app/strategy_lab/outcomes.py")
    app_js = safe_read("app/static/app.js")

    return {
        "duplicate_paper_controls_get_routes": main_py.count('@app.get("/api/paper-controls")'),
        "duplicate_debug_full_routes": main_py.count('@app.get("/api/debug/full")'),
        "old_download_full_debug_route_present": '/api/download/full-debug' in main_py,
        "old_enabled_endpoint_still_in_main": '/api/paper-controls/enabled' in main_py,
        "old_reset_endpoint_still_in_main": '/api/paper-controls/reset' in main_py,
        "frontend_uses_old_enabled_endpoint": '/api/paper-controls/enabled' in app_js,
        "frontend_uses_old_reset_endpoint": '/api/paper-controls/reset' in app_js,
        "paper_equity_uses_open_only_formula": False,
        "shadow_exit_uses_mid_price": False,
        "expiry_resolution_uses_fair_yes_proxy": False,
        "crashing_run_monkey_patch_present": 'TradingBot.run = guarded_main_loop(TradingBot.run)' in main_py,
        "canonical_paper_post_route_present": '@app.post("/api/paper-controls")' in main_py,
        "canonical_paper_reset_route_present": '@app.post("/api/paper-reset")' in main_py,
        "canonical_debug_full_route_present": '@app.get("/api/debug/full")' in main_py,
        "canonical_handoff_note_route_present": '@app.post("/api/debug/handoff-note")' in main_py,
    }

def try_call(name: str, fn):
    try:
        return {"ok": True, "name": name, "value": fn()}
    except Exception as e:
        return {"ok": False, "name": name, "error": str(e)}

def add_section(parts: List[str], title: str, body) -> None:
    parts.append("")
    parts.append("=" * 110)
    parts.append(title)
    parts.append("=" * 110)
    if isinstance(body, str):
        parts.append(body)
    else:
        parts.append(json.dumps(body, indent=2, default=str))

def build_project_manifest() -> Dict[str, Any]:
    return {
        "project_name": "Kalshi crypto range-bucket operator console",
        "ultimate_goal": "Build a profitable real-money trading system for Kalshi crypto range-bucket markets.",
        "current_phase": "Paper/shadow validation and operator-console hardening before live deployment.",
        "current_priority": "Make paper/shadow evaluation truthful enough that it can be trusted as a gate for real-money trading decisions.",
        "success_definition": [
            "The system can identify and execute trades with positive real-world expectancy.",
            "Paper/shadow accounting is truthful enough to predict real-world behavior.",
            "Operator controls are stable and allow safe enable/disable/reset workflows.",
            "The codebase and handoff bundle are reliable enough for continued multi-model development."
        ],
        "secondary_goals": [
            "Maintain a strong operator dashboard and debugging surface.",
            "Allow paper trading to be enabled, disabled, and reset at any time.",
            "Collect enough evidence to know when the strategy is or is not ready for real money.",
            "Keep a portable AI handoff bundle that can continue the work in a new chat/tool.",
        ],
        "operator_preferences": {
            "terminal_only_copy_paste": True,
            "no_manual_file_editing": True,
            "prefer_complete_patches": True,
            "bundle_should_be_portable_project_memory": True,
        },
        "tech_stack": {
            "backend": "FastAPI + Python",
            "frontend": "HTML + Tailwind + vanilla JS",
            "db": "SQLite",
            "runtime": "Docker Compose",
        },
    }

def build_next_steps() -> List[str]:
    flags = detect_code_health()
    steps = []
    if flags["paper_equity_uses_open_only_formula"]:
        steps.append("Fix paper accounting to use realized closed-trade PnL plus unrealized open-position PnL.")
    if flags["shadow_exit_uses_mid_price"]:
        steps.append("Change shadow exits/marks to use yes_bid instead of mid for more realistic liquidation-side accounting.")
    if flags["expiry_resolution_uses_fair_yes_proxy"]:
        steps.append("Replace fair_yes-based expiry settlement with actual bucket/threshold settlement using final spot.")
    if flags["duplicate_paper_controls_get_routes"] > 1:
        steps.append("Remove duplicate paper-control route blocks and keep one canonical family.")
    if flags["frontend_uses_old_enabled_endpoint"] or flags["frontend_uses_old_reset_endpoint"]:
        steps.append("Normalize frontend JS to use only canonical paper endpoints.")
    steps.append("Keep adding confirmed learnings to ai_handoff_memory.md via /api/debug/handoff-note.")
    steps.append("Use the bundle alone in a new chat to continue development with minimal context loss.")
    return steps

def build_runtime_snapshot(bot) -> Dict[str, Any]:
    return {
        "dashboard_status": try_call("bot.dashboard_status", lambda: bot.dashboard_status()),
        "paper_controls_status": try_call("bot.paper_controls_status", lambda: bot.paper_controls_status()),
        "compute_paper_equity": try_call("bot.compute_paper_equity", lambda: bot.compute_paper_equity()),
    }

def generate_handoff_bundle(bot) -> str:
    ensure_memory_file()

    parts: List[str] = []
    parts.append("===== SELF-CONTAINED AI HANDOFF BUNDLE =====")
    parts.append("This file is intended to be pasted BY ITSELF into a new AI chat with no extra user instructions.")
    parts.append("Any AI reading this file should treat it as both project memory and the active task briefing.")
    parts.append(f"generated_at_utc: {_utc_now()}")
    parts.append(f"project_root: {PROJECT_ROOT}")

    add_section(parts, "READ THIS FIRST - INSTRUCTIONS FOR ANY AI MODEL", """READ THIS FIRST. THIS FILE IS A SELF-CONTAINED AI HANDOFF + PROJECT MEMORY BUNDLE.

DO NOT ASK THE USER WHAT THIS FILE IS.
DO NOT ASK THE USER FOR BACKGROUND CONTEXT BEFORE READING IT.
TREAT THIS ENTIRE DOCUMENT AS THE SOURCE OF TRUTH FOR THE PROJECT.

YOUR ROLE:
You are taking over development of an in-progress trading research / operator console application.

GOAL HIERARCHY:
- Ultimate goal: build a profitable real-money trading system.
- Current phase: paper/shadow validation and system hardening.
- Do not mistake paper trading for the final purpose of the project.
- Treat truthful paper/shadow evaluation as the gate that determines whether live deployment is deserved.

IMPORTANT WORKFLOW RULE:
Whenever you provide a solution patch, include the handoff-memory update in the same copy/paste solution so the project memory stays current automatically.

IMPORTANT GOAL HIERARCHY:
- The ultimate business objective is profitable live trading with real money.
- Paper trading and shadow trading are validation stages, not the final product.
- Current work should improve the system toward profitable live deployment while making sure paper/shadow evaluation is truthful enough to be trusted.
- Do not confuse the current validation phase with the final objective.

WHAT YOU MUST DO FIRST:
1. Read this full bundle.
2. Summarize current architecture.
3. Summarize confirmed issues vs suspected issues.
4. Identify the single highest-priority next fix.
5. Provide terminal-only copy/paste patches with no manual file editing.

RESPONSE RULES:
- Do not ask the user to restate project history.
- Do not ask the user to explain what the bundle contains.
- Prefer concrete patches and verification commands over vague advice.
- Preserve working functionality unless intentionally replacing it.
- Be explicit about what is confirmed vs inferred.
- Every time you provide a meaningful implementation patch or fix, ALSO update project memory.

MANDATORY MEMORY-UPDATE POLICY:
- Whenever you provide a meaningful solution, include a terminal-only copy/paste command that appends a concise project-memory note.
- Do NOT make the user manually invent the memory note text.
- The memory note should summarize:
  1) what was changed
  2) why it was changed
  3) what was confirmed afterward
  4) any remaining known issue
- Prefer appending the note as the final step of the same copy/paste block, not as a separate optional step.
- Treat the handoff memory as part of the implementation, not documentation fluff.

REQUIRED MEMORY-UPDATE COMMAND TEMPLATE:
curl -s -X POST http://localhost:8000/api/debug/handoff-note \
  -H "Content-Type: application/json" \
  -d '{"title":"<short title>","category":"<system|strategy|ui|bugfix|ops>","content":"<concise summary of what changed, why, what was verified, and what remains>"}' | python3 -m json.tool

OPERATOR PREFERENCES:
- Terminal-only copy/paste fixes
- No manual file editing
- Prefer complete patches
- Bundle should remain portable project memory
- Goal is truthful paper/shadow evaluation before real-money deployment
- Future solutions should automatically include updating handoff/project memory
""")

    add_section(parts, "PROJECT_MANIFEST", build_project_manifest())
    add_section(parts, "PERSISTENT_PROJECT_MEMORY", read_memory_text())
    add_section(parts, "IMPORTANT_FILE_MAP", [{"path": p, "purpose": d} for p, d in IMPORTANT_FILES])
    add_section(parts, "ROUTE_INVENTORY_PARSED_FROM_MAIN", parse_routes_from_main())
    add_section(parts, "ENV_KEY_INVENTORY_REDACTED", env_key_inventory())
    add_section(parts, "CODE_HEALTH_FLAGS", detect_code_health())
    add_section(parts, "NEXT_STEPS_PRIORITY", build_next_steps())
    add_section(parts, "RUNTIME_SNAPSHOT", build_runtime_snapshot(bot))

    add_section(parts, "DB_TABLE_COUNTS", [
        {"table": t, "count": sql_scalar(f"SELECT COUNT(*) FROM {t}", default=0)}
        for t in DB_TABLES
    ])

    add_section(parts, "DB_SCHEMAS", {t: table_info(t) for t in DB_TABLES})

    add_section(parts, "RECENT_SHADOW_POSITIONS", sql_rows("""
        SELECT * FROM shadow_positions
        ORDER BY opened_at DESC
        LIMIT 25
    """))

    add_section(parts, "RECENT_SHADOW_TRADES", sql_rows("""
        SELECT * FROM shadow_trades
        ORDER BY id DESC
        LIMIT 25
    """))

    add_section(parts, "REJECTION_SUMMARY", sql_rows("""
        SELECT variant_name, rejection_reason, COUNT(*) AS n
        FROM candidate_events
        WHERE eligible = 0
        GROUP BY variant_name, rejection_reason
        ORDER BY n DESC, variant_name, rejection_reason
        LIMIT 100
    """))

    add_section(parts, "CLOSE_REASON_SUMMARY", sql_rows("""
        SELECT COALESCE(close_reason, 'unknown') AS close_reason,
               COUNT(*) AS n,
               ROUND(SUM(COALESCE(realized_pnl, 0)), 4) AS pnl
        FROM shadow_trades
        WHERE closed_at IS NOT NULL
        GROUP BY COALESCE(close_reason, 'unknown')
        ORDER BY n DESC, pnl DESC
    """))

    add_section(parts, "OPERATOR_STATE", sql_rows("""
        SELECT * FROM operator_state
        ORDER BY key
    """))

    add_section(parts, "OPERATOR_SETTINGS", sql_rows("""
        SELECT * FROM operator_settings
        ORDER BY id DESC
        LIMIT 100
    """))

    for relpath, purpose in IMPORTANT_FILES:
        add_section(parts, f"FILE_SOURCE: {relpath} -- {purpose}", safe_read(relpath))

    return "\\n".join(parts)
