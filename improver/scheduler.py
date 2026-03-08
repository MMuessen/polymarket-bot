from __future__ import annotations
import time
from improver.config import load_config
from improver.runner import main as run_once

def main():
    while True:
        cfg = load_config()
        if cfg.get("enabled"):
            try:
                run_once()
            except Exception as e:
                print({"ok": False, "scheduler_error": str(e)})
        mins = int(cfg.get("observer_interval_minutes", 20))
        time.sleep(max(60, mins * 60))

if __name__ == "__main__":
    main()
