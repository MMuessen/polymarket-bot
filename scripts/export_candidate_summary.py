import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

SRC = Path("data/candidate_events.jsonl")
OUT = Path("data/candidate_summary.csv")

if not SRC.exists():
    print(f"missing {SRC}")
    raise SystemExit(1)

rows = []
with SRC.open() as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except Exception:
            pass

if not rows:
    print("no rows")
    raise SystemExit(0)

by_reason = Counter(r.get("reason", "unknown") for r in rows)
by_asset = Counter(r.get("asset", "unknown") for r in rows)

fieldnames = [
    "ts", "ticker", "asset", "status", "threshold", "direction",
    "spot", "distance_from_spot_pct", "hours_to_expiry",
    "yes_bid", "yes_ask", "no_bid", "no_ask", "spread",
    "eligible", "reason", "model_prob", "market_prob", "edge"
]

with OUT.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k) for k in fieldnames})

print(f"rows: {len(rows)}")
print("by_reason:", dict(by_reason))
print("by_asset:", dict(by_asset))
print(f"wrote {OUT}")
