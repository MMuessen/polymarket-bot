# ROADMAP AUDIT

## Current top-line state

- mode: `paper`
- live_armed: `False`
- loop_error: `None`
- eligible_markets: `None`
- paper_using_newest_infra: `True`

## Patch / roadmap marker presence

- ✅ Institutional model bible
- ✅ KXBTC events bootstrap
- ✅ Queue positions foundation
- ✅ Deribit skew transport fix
- ✅ Public trade tape foundation
- ✅ Institutional entry hotfix
- ✅ Institutional entry preview
- ✅ Institutional entry audit
- ✅ Institutional entry enforcement
- ✅ Institutional attach fix
- ✅ Institutional bridge
- ✅ Institutional candidate gate
- ✅ Institutional hydration bridge
- ✅ Institutional edge floor v9
- ✅ Institutional edge floor v10
- ✅ Institutional prime-time floor v11
- ✅ Institutional markouts v12
- ✅ Markouts safe route v13
- ❌ Markouts DB bridge v14
- ✅ Markouts truth route v16
- ❌ Throughput relief v12
- ❌ Throughput relief v13
- ✅ Canonical row picker v17
- ✅ Live row priority v18
- ✅ Live snapshot reprice v19
- ✅ Live snapshot bind fix v20
- ✅ ICRP helper compat v21
- ✅ ICRP helper family compat v22
- ✅ ICRP fetch_rows compat v23
- ✅ ICRP fetch_rows arg normalize v25

## Institutional helper chain

- ✅ `_arch_icrp_v17_normalize`
- ✅ `_arch_icrp_v17_fetch_rows`
- ✅ `_arch_icrp_v17_snapshot_ticker`
- ✅ `_arch_icrp_v17_snapshot_side`
- ✅ `_arch_icrp_v17_snapshot_field`
- ✅ `_arch_icrp_v17_float`

## What is working

- Patch markers are present for the major data-source and institutional-gate phases.
- SQLite strategy DB exists and can be read.
- Candidate generation is active; candidate_events is growing.
- Paper trade history exists in shadow_trades.
- Ticker-only DB row fetch normalization is working via v25.
- paper_using_newest_infra is true.

## What is not working or not yet proven

- No open shadow_positions right now, so live markout harvesting cannot accumulate new post-trade records.
- Markout runtime has zero completed records so far.
- The top-level status route is intermittently returning eligible_markets = null during later-stage checks.

## Database truth

- db_exists: `True`
- candidate_events_count: `158535`
- candidate_events_last_30m: `0`
- shadow_trades_count: `105`
- shadow_trades_last_120m: `14`
- shadow_positions_count: `0`

## Recent shadow trades

- id=338 ticker=KXBTC-26MAR1017-B70500 side=yes pnl=-3.9599999999999995 variant=crypto_lag_aggressive opened_at=2026-03-10T12:12:43.605581+00:00
- id=337 ticker=KXBTC-26MAR1017-B70500 side=yes pnl=-3.3999999999999995 variant=crypto_lag_aggressive opened_at=2026-03-10T12:07:09.044879+00:00
- id=336 ticker=KXBTC-26MAR1017-B70500 side=yes pnl=-3.4499999999999997 variant=crypto_lag_aggressive opened_at=2026-03-10T12:01:52.221662+00:00
- id=335 ticker=KXBTC-26MAR1017-B70500 side=yes pnl=-3.5 variant=crypto_lag_aggressive opened_at=2026-03-10T11:32:03.134708+00:00
- id=334 ticker=KXBTC-26MAR1017-B70500 side=yes pnl=-3.5999999999999996 variant=crypto_lag_aggressive opened_at=2026-03-10T11:24:11.295473+00:00

## Institutional funnel truth

- candidate_calls: `20`
- base_allowed: `1`
- db_hits: `19`
- db_misses: `0`
- relief_allowed: `19`
- relief_blocked: `0`
- picked_from_table: `{'market_opportunities': 19}`
- blocked_by_reason: `{}`
- last_error: `None`

## fetch_rows v25 truth

- calls: `61`
- ticker_only_calls: `61`
- aggregate_hits: `60`
- aggregate_misses: `0`

## Markouts truth

- open_captures: `0`
- markout_1s_done: `0`
- markout_5s_done: `0`
- markout_10s_done: `0`
- completed_records: `0`

## What still needs to be implemented / fixed

- Rebuild and re-check `/api/debug/institutional-funnel-v17` after the normalize shim.
- If another missing helper appears, patch only that helper next; do not reopen solved fetch/route issues.
- Once funnel errors are gone, confirm `picked_from_table`, `relief_allowed`, and `relief_blocked` are nonzero and honest.
- Only after live funnel decisions are stable, resume markout harvesting by confirming new shadow_positions actually open.
- PSV / settlement-sniper logic is still not proven in the logs and should be treated as not yet implemented/verified.
- BRTI / settlement-average truth is still missing from the verified working set.
- Post-trade toxicity learning exists as scaffolding, but not yet proven live because no new positions are opening.
- Inventory-aware reservation pricing is partially represented in gate language, but not yet proven end-to-end by a healthy live funnel.
- The real bottleneck now is logic completion, not raw data ingestion.

## Current best interpretation

- Data ingestion foundations are mostly in place.
- The paper bot is generating candidate events and has historical paper trades.
- The institutional funnel is still incomplete; the current failure point is helper-chain / normalization logic, not data availability.
- Because the funnel is not healthy, markout harvesting cannot progress meaningfully yet.
- Once `_arch_icrp_v17_normalize` is restored and the funnel produces real allow/block decisions again, you can measure whether the prime-time logic is honestly too strict or whether live quotes are simply bad.

