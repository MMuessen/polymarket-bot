from __future__ import annotations
import json
from improver.config import load_config
from improver.observer import collect_runtime, summarize_recent_results
from improver.planner_ollama import plan_next_task
from improver.coder_openai import request_patch
from improver.policy import extract_bash_block, patch_policy_check
from improver.promoter import rebuild_and_smoke
from improver.state import add_cycle, init_db
from improver.verifier import reset_staging, apply_patch_script, verify_staging, promote_staging

def main():
    init_db()
    cfg = load_config()

    if not cfg.get("enabled"):
        print(json.dumps({"ok": False, "message": "improver disabled in config.json"}, indent=2))
        return

    runtime = collect_runtime()
    recent = summarize_recent_results(runtime)
    bundle_text = ((runtime.get("bundle") or {}).get("value") or "") if runtime.get("bundle", {}).get("ok") else ""

    try:
        planner = plan_next_task(bundle_text, recent)
    except Exception as e:
        planner = {"decision": "skip", "reason": "planner_exception", "error": str(e)}

    if planner.get("decision") in {"skip", "hold", None}:
        add_cycle("planner_skip", summary=planner.get("reason", "no_task"), planner=planner)
        print(json.dumps({"ok": True, "stage": "planner_skip", "planner": planner}, indent=2))
        return

    if not planner.get("requires_openai", True):
        add_cycle("planner_local_only", summary=planner.get("task_title", "local only"), planner=planner)
        print(json.dumps({"ok": True, "stage": "planner_local_only", "planner": planner}, indent=2))
        return

    try:
        coder = request_patch(bundle_text, recent, planner)
    except Exception as e:
        coder = {"ok": False, "error": f"coder_exception: {e}"}

    if not coder.get("ok"):
        add_cycle("openai_skip", summary=coder.get("error", "openai_failed"), planner=planner, coder=coder)
        print(json.dumps(coder, indent=2))
        return

    response = coder["response"]
    output_text = coder.get("output_text") or ""
    script = extract_bash_block(output_text)

    if not script:
        preview = output_text[:4000]
        add_cycle("coder_no_bash_block", summary="no bash block", planner=planner, coder={"raw": preview})
        print(json.dumps({"ok": False, "error": "no bash block found", "preview": preview[:1000]}, indent=2))
        return

    policy = patch_policy_check(script)
    if not policy.get("ok"):
        add_cycle("policy_reject", summary=policy.get("reason", "policy"), planner=planner, coder={"script_preview": script[:4000]})
        print(json.dumps({"ok": False, "policy": policy}, indent=2))
        return

    reset_staging()
    apply_result = apply_patch_script(script)
    verify = verify_staging()
    if apply_result["returncode"] != 0 or not verify["ok"]:
        add_cycle(
            "verify_reject",
            summary="staging verify failed",
            planner=planner,
            coder={"script_preview": script[:4000]},
            verify={"apply": apply_result, "verify": verify},
        )
        print(json.dumps({"ok": False, "apply": apply_result, "verify": verify}, indent=2))
        return

    if cfg.get("dry_run") or not cfg.get("auto_promote"):
        add_cycle(
            "verified_dry_run",
            summary="patch verified in staging only",
            planner=planner,
            coder={"script_preview": script[:4000]},
            verify={"apply": apply_result, "verify": verify},
        )
        print(json.dumps({"ok": True, "stage": "verified_dry_run", "verify": verify}, indent=2))
        return

    promote = promote_staging()
    smoke = rebuild_and_smoke()
    add_cycle(
        "promoted" if smoke["ok"] else "promote_failed",
        summary="promoted to production" if smoke["ok"] else "promotion smoke failed",
        planner=planner,
        coder={"script_preview": script[:4000]},
        verify={"apply": apply_result, "verify": verify},
        promote={"promote": promote, "smoke": smoke},
    )
    print(json.dumps({"ok": smoke["ok"], "promote": promote, "smoke": smoke}, indent=2))

if __name__ == "__main__":
    main()
