# NEXT CHAT DEBUG BUNDLE
## Project
Kalshi BTC paper bot at: `~/polymarket-bot`

## Current objective
Finish the institutional funnel plumbing so the paper-only institutional gate evaluates **real live executable rows** and produces honest allow/block decisions without skipping steps or re-debugging already-solved issues.

## Ground truth as of latest session
- Mode: `paper`
- `live_armed`: `false`
- `paper_using_newest_infra`: `true`
- Institutional model bible file exists: `institutional_quant_strategy.md`
- Data-source foundations already landed earlier:
  - KXBTC event bootstrap live
  - queue positions live
  - Deribit skew live
  - public trade tape live

## What is already proven working
### 1) SQLite truth is good
`/api/debug/markouts-truth` proved:
- `data/strategy_lab.db` exists
- `shadow_trades` has history
- `candidate_events` is growing
- `shadow_positions` is currently `0`
- zero markout counts are expected until **new positions open**

### 2) fetch_rows argument normalization is now fixed
Patch: `v25`
Marker:
- `_architect_institutional_fetch_rows_arg_normalize_v25_applied`

This route now works:
- `/api/debug/institutional-fetch-rows-compat-v25`

Latest verified truth from v25:
- `ticker_only_calls > 0`
- `aggregate_hits > 0`
- rows are returned for ticker-only calls
- rows come from:
  - `market_opportunities`
  - `candidate_events`
  - `execution_compare_cohorts`

### 3) The DB actually contains live economics rows
Example truths from the latest v25 verification:
- `B66250`: best side mostly `no`, live edge still negative
- `B67250`: best side mostly `yes`, live edge still negative
- `B65750`: occasionally small positive live edge, but still thin / not prime-time quality
- `B67750`: negative
- `B65250`: negative

Meaning:
- the plumbing is closer now
- but most live rows are genuinely weak
- once the funnel is repaired, expect **few** relief allows unless quotes improve

## Current blocker
The institutional funnel still crashes after v25 because this helper is missing:

`NameError("name '_arch_icrp_v17_normalize' is not defined")`

Latest verified route state:
- `/api/debug/institutional-funnel-v17`
- `candidate_calls: 10`
- `base_allowed: 1`
- `db_hits: 0`
- `db_misses: 0`
- `relief_allowed: 0`
- `relief_blocked: 0`
- `last_error: NameError("_arch_icrp_v17_normalize is not defined")`

This means:
- old fetch/path issues are no longer the main problem
- the next chat should start by restoring `_arch_icrp_v17_normalize`
- do **not** re-open the already-solved fetch_rows argument bug

## Important history so next chat does NOT waste time
### Solved already
- missing institutional model bible file
- KXBTC bootstrap / future discovery
- queue positions foundation
- Deribit skew transport
- public trade tape foundation
- multiple broken monitoring routes
- markouts safe route / DB truth route
- fetch_rows ticker-only argument handling via v25

### Not solved yet
- the institutional funnel helper chain is incomplete
- newest missing symbol is `_arch_icrp_v17_normalize`
- after that, there may still be one or two more missing v17 helper symbols
- continue with **small compat shims only**, one missing helper at a time, then rebuild and verify

## Preferred repair style
- terminal-only
- no manual editing
- tiny patches
- always backup `app/main.py`
- always run:
  - marker check
  - `python3 -m py_compile app/main.py`
  - `docker-compose up -d --build bot`
  - health wait loop
  - verify route
  - compact status check
  - handoff note append

## Exact next move for the new chat
Start from this sentence:

> We are past the fetch_rows bug. The current blocker is `NameError("_arch_icrp_v17_normalize is not defined")` inside the institutional funnel. Ship a tiny compat patch that restores `_arch_icrp_v17_normalize` with a flexible `*args, **kwargs` signature, normalize rows from `market_opportunities`, `candidate_events`, and `execution_compare_cohorts` into one canonical dict, rebuild, verify `/api/debug/institutional-funnel-v17`, and then continue only if the next missing helper appears.

## What the normalize helper should probably do
The compat shim should be permissive and accept unknown call signatures.

Suggested canonical output fields:
- `ticker`
- `table` or `__source_table`
- `best_side`
- `entry_price`
- `yes_bid`
- `yes_ask`
- `no_bid`
- `no_ask`
- `spread`
- `order_book_imbalance`
- `maker_toxicity_bps_1s`
- `cluster_bias_same_side`
- `edge_after_rebate_and_inventory`
- `live_executable_row`

Suggested normalization logic:
- start from `dict(row)` when possible
- preserve `__source_table` if present
- `best_side`:
  - use existing `best_side`
  - else fall back from `maker_best_side`, `taker_best_side`
- `spread`:
  - prefer existing `spread`
  - else `yes_ask - yes_bid`
- `edge_after_rebate_and_inventory`:
  - prefer existing field
  - else `maker_edge_after_rebate_and_inventory`
  - else `maker_edge_after_rebate`
  - else `best_exec_edge`
  - else `edge`
- `entry_price`:
  - if side is `yes`, prefer `yes_ask`, then `requested_price`, then `entry_price`
  - if side is `no`, prefer `no_ask`, then `requested_price`, then `entry_price`
- `live_executable_row`:
  - true for `market_opportunities`
  - true for `candidate_events`
  - false for replay-only rows unless explicitly marked live
- default missing OBI / toxicity / cluster bias to `0.0`

## Expected behavior after normalize fix
Best-case:
- funnel starts producing real `picked_from_table`
- relief decisions become nonzero again
- then we can tell whether the remaining problem is honest weak live edge

Likely honest outcome:
- very few relief allows
- because most current live rows still have thin or negative economics

## Routes worth checking first in the next chat
- `/api/status`
- `/api/debug/institutional-fetch-rows-compat-v25`
- `/api/debug/institutional-funnel-v17`
- `/api/debug/markouts-truth`

## Reminder
Do not trust replay-rich rows to unlock live relief.
The gate should prefer:
1. `market_opportunities`
2. `candidate_events`
3. `execution_compare_cohorts` only as fallback / evidence, not as live executable truth

