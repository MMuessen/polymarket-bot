from __future__ import annotations
import py_compile
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def run(cmd: str):
    p = subprocess.run(cmd, cwd=str(ROOT), shell=True, capture_output=True, text=True)
    return {
        "cmd": cmd,
        "returncode": p.returncode,
        "stdout": p.stdout[-12000:],
        "stderr": p.stderr[-12000:],
    }

def compile_production_files():
    files = [ROOT / "app" / "main.py"]
    optional = [ROOT / "app" / "debug_handoff.py"]
    for p in optional:
        if p.exists():
            files.append(p)
    files.extend(sorted((ROOT / "app" / "strategy_lab").glob("*.py")))
    files.extend(sorted((ROOT / "improver").glob("*.py")))

    compiled = []
    errors = []
    for file in files:
        try:
            py_compile.compile(str(file), doraise=True)
            compiled.append(str(file.relative_to(ROOT)))
        except Exception as e:
            errors.append({"file": str(file.relative_to(ROOT)), "error": str(e)})
    return {"returncode": 0 if not errors else 1, "compiled": compiled, "errors": errors}

def rebuild_and_smoke():
    steps = []
    steps.append({"cmd": "py_compile", **compile_production_files()})
    steps.append(run("docker-compose up --build -d"))
    steps.append(run("sleep 12"))
    steps.append(run("docker-compose logs bot --tail=120 || true"))
    steps.append(run("curl -s http://localhost:8000/api/status > /tmp/improver_status.json && python3 - <<'PY'\nimport json\nfrom pathlib import Path\nraw = Path('/tmp/improver_status.json').read_text().strip()\nprint('status_len', len(raw))\nif not raw:\n    raise SystemExit(1)\ndata = json.loads(raw)\nprint('mode', data.get('mode'))\nprint('loop_error', (data.get('health') or {}).get('loop_error'))\nPY"))
    steps.append(run("curl -s http://localhost:8000/api/debug/full | head -n 40"))
    ok = all(s.get("returncode") == 0 for s in [steps[0], steps[3], steps[4]])
    return {"ok": ok, "steps": steps}
