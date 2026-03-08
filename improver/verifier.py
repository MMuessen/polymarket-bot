from __future__ import annotations
import os
import py_compile
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STAGING = ROOT.parent / "polymarket-bot-staging"

IGNORE_NAMES = {
    "__pycache__", ".git", ".venv", "node_modules",
    "backup_before_fix", "backup_before_strategy_lab",
    "polymarket-bot-staging"
}

def _ignore(src, names):
    return [n for n in names if n in IGNORE_NAMES]

def reset_staging():
    if STAGING.exists():
        shutil.rmtree(STAGING)
    shutil.copytree(ROOT, STAGING, ignore=_ignore)

def run(cmd, cwd: Path):
    p = subprocess.run(cmd, cwd=str(cwd), shell=True, capture_output=True, text=True)
    return {
        "cmd": cmd,
        "returncode": p.returncode,
        "stdout": p.stdout[-12000:],
        "stderr": p.stderr[-12000:],
    }

def apply_patch_script(script_text: str):
    script_path = STAGING / "improver_apply_patch.sh"
    script_path.write_text("#!/usr/bin/env bash\nset -euo pipefail\n" + script_text)
    os.chmod(script_path, 0o755)
    return run(f"bash {script_path.name}", STAGING)

def verify_staging():
    files = [STAGING / "app" / "main.py"]
    optional = [
        STAGING / "app" / "debug_handoff.py",
    ]
    for p in optional:
        if p.exists():
            files.append(p)
    files.extend(sorted((STAGING / "app" / "strategy_lab").glob("*.py")))
    files.extend(sorted((STAGING / "improver").glob("*.py")))

    compiled = []
    errors = []
    for file in files:
        try:
            py_compile.compile(str(file), doraise=True)
            compiled.append(str(file.relative_to(STAGING)))
        except Exception as e:
            errors.append({"file": str(file.relative_to(STAGING)), "error": str(e)})

    grep_check = run("grep -Rni 'api/paper-controls\\|api/paper-reset\\|api/debug/full\\|api/debug/handoff-note' app improver || true", STAGING)
    return {
        "ok": len(errors) == 0,
        "checks": [
            {"compiled": compiled, "errors": errors},
            grep_check,
        ],
    }

def promote_staging():
    copied = []
    for rel in [
        "app/main.py",
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
    ]:
        src = STAGING / rel
        dst = ROOT / rel
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(rel)

    optional_rel = STAGING / "app" / "debug_handoff.py"
    if optional_rel.exists():
        dst = ROOT / "app" / "debug_handoff.py"
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(optional_rel, dst)
        copied.append("app/debug_handoff.py")

    src_dir = STAGING / "improver"
    if src_dir.exists():
        for src in src_dir.rglob("*.py"):
            rel = src.relative_to(STAGING)
            dst = ROOT / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            copied.append(str(rel))

    return {"ok": True, "copied": copied}
