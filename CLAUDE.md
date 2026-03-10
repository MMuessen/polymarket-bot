# Polymarket Bot — Preservation Map

**Last updated:** 2026-03-10
**File:** `app/main.py` is ~35,224 lines of sequential monkey-patches applied at import time.
Every future AI session must read this file before making changes.

---

## Project Structure

```
/root/polymarket-bot/
  app/
    main.py                        # ALL logic — 35,224 lines, sequential patches
    kalshi_stream_v40.py           # Kalshi WebSocket market data feed
    debug_handoff.py               # Handoff bundle generator
    strategy_lab/
      shadow_engine.py             # Base ShadowEngine class (heavily overridden)
      architect_layer.py           # Architect payload builder + consultant report
      config.py                    # DEFAULT_VARIANTS config
      storage.py                   # SQLite helpers: get_conn(), init_db(), json_dumps()
      outcomes.py                  # OutcomeResolver: marks settled positions
      suggestions.py               # SummaryAndSuggestionEngine
      gui_api.py                   # REST helpers for strategy lab UI
      operator_settings.py         # Persistent operator settings (SQLite)
      replay_calibration.py        # Replay/calibration infrastructure
  data/
    strategy_lab.db                # Primary trading DB (shadow_trades, shadow_positions, candidate_events, ...)
    trades.db                      # Legacy live trade log (sqlalchemy ORM)
    crypto_market_cache.json       # BTC range bucket ticker cache
    btc_cache_meta.json            # Cache metadata
  docker-compose.yml               # Service: kalshi_bot on port 8000
  Dockerfile
  requirements.txt
  kalshi_private_key.pem           # Kalshi API signing key (never commit)
```

---

## Core Classes (base definitions, then overridden by patches)

### `TradingBot` (main.py ~line 340)
The central object. One instance runs in the FastAPI lifespan.

Key attributes:
- `self.paper_mode` — True (set by PAPER_MODE env)
- `self.live_armed` — False (set by ARM_LIVE_TRADING env)
- `self.market_snapshots: Dict[str, SnapshotRow]` — live market data (SnapshotRow from kalshi_stream_v40)
- `self.spot_prices: Dict[str, float]` — {"BTC": ..., "ETH": ..., "SOL": ...}
- `self.strategy_states: Dict[str, StrategyState]` — "crypto_lag" is the only active strategy
- `self.shadow_engine: ShadowEngine`
- `self.cooldowns: Dict[str, datetime]`
- `self.health: Dict[str, Any]`

Key methods (all heavily wrapped by patches):
- `_enrich_with_fair_value(snapshot)` — computes fair_yes, execution_edge_yes/no, best_side, best_exec_edge
- `refresh_markets()` — fetches market data; v40 wraps to seed from WebSocket stream
- `run_strategies()` — iterates snapshots, generates order intents
- `dashboard_status()` — returns JSON status blob consumed by `/api/status`
- `_bucket_probability(spot, low, high, hours)` — log-normal GBM fair value for range buckets (line 1423)

### `ShadowEngine` (strategy_lab/shadow_engine.py)
Paper trading engine. **Heavily overridden** — see patch chain below.

Base methods (all replaced by patches):
- `run_shadow_variants()` → replaced by v47 (via v50→v60→v28 wrappers)
- `open_shadow_trade()` → replaced by v29e→_arch_iq chain
- `_passes_variant_rules()` → replaced by v46
- `_should_exit()` → replaced by exit_v2
- `mark_positions()` → replaced by _arch_iq_mark_positions
- `close_shadow_trade()` → replaced by _close_shadow_trade_exit_v2
- `log_candidate()` — NOT replaced; still writes to candidate_events table

### `KalshiMarketStreamV40` (kalshi_stream_v40.py)
WebSocket feed producing `SnapshotRow` dicts (dict subclass with attribute access).
- `_row_from_market()` — sets `raw_close_time: ISO string`
- `_merge_ticker()` — updates existing SnapshotRow in-place
- `_apply_rows()` — replaces `bot.market_snapshots` with new SnapshotRow objects
- `_arch_rct_rewrite_market_snapshots()` (main.py ~line 11337) — converts `raw_close_time` string→datetime; wired into refresh_markets at line 11423. **Critical: `_hours_to_expiry()` only works after this runs.**

### `SnapshotRow`
Dict subclass. Supports both `row["key"]` and `row.key` access.
Key fields: `ticker, yes_bid, yes_ask, no_bid, no_ask, mid, spread, fair_yes, edge, best_side, best_exec_edge, execution_edge_yes, execution_edge_no, floor_strike, cap_strike, market_family, raw_close_time, distance_from_spot_pct, distance_bucket, hours_to_expiry, rationale, open_interest, obi`

---

## Main Loop (main.py ~line 585)

```
_main_loop():
  _ensure_spot_prices_ready()           # Coinbase REST fallback
  _run_btc_cache_rebuild() if due       # Rebuild ticker cache
  refresh_markets()                     # REST + WebSocket seed; rct rewrite
  shadow_engine.run_shadow_variants()   # Paper trading (v47 via v50→v60→v28)
  shadow_engine.mark_positions()        # Mark unrealized PnL
  shadow_engine.run_exit_pass()         # Check exit conditions
  outcome_resolver.mark_positions()     # Settlement check
  outcome_resolver.resolve_finalized_positions()
  summary_engine.rebuild_summaries()
  summary_engine.rebuild_suggestions()
  refresh_balance()
  run_strategies()                      # Live order intent (currently disabled)
  asyncio.sleep(poll_seconds)           # Default 15s
```

---

## ACTIVE Method Call Chains

These are the runtime chains after all patches are applied. Earlier patches in each chain are superseded.

### `run_shadow_variants` chain (outermost first)
```
_run_shadow_variants_sync (v28, line 32982)       — sync wrapper
  → _run_shadow_variants_v60 (v60, line 32028)    — async→sync bridge
    → _aqg_run_shadow_variants_v50 (v50, line 30127) — stats wrapper
      → _aqg_run_shadow_variants_v47 (v47, line 29847) — REAL WORK
          Phase 1: fill pending maker orders (line 29671)
          Phase 2: queue new maker orders (line 29751)
```

**v47 Phase 1 (fill):** For each pending order, checks `current_ask <= target_price`; if yes, calls `self.open_shadow_trade(..., side=order.get("side"))` [FIXED 2026-03-10].

**v47 Phase 2 (queue):** Calls `_passes_variant_rules`; if eligible, queues a maker order at `ask - price_improve`. Does NOT open trades directly.

### `open_shadow_trade` chain (outermost first)
```
_aqg_open_shadow_trade_wrapper (v29e, line 25353)  — quant gate check
  → _arch_iq_open_shadow_trade (line 18471)        — institutional gate + DB write
```

**v29e:** reads `snapshot.best_side`; calls `_aqg_decide(preferred_side=side)`; if approved, passes through `*args, **kwargs` (side kwarg propagates).

**_arch_iq_open_shadow_trade:** validates side; calls `_arch_iq_side_ask(snapshot, side)` — **if None/0.0, returns False** [FIXED 2026-03-10]; uses `no_ask` for NO trades; writes shadow_trades + shadow_positions.

### `_passes_variant_rules` chain
```
_aqg_passes_variant_rules_v46 (line 29523)  — FINAL ACTIVE
```
Config `_AQG_V46`:
- `only_variant`: "crypto_lag_aggressive"
- `yes_min_edge`: 0.015, `no_min_edge`: 0.010
- `no_min_net_after_spread`: 0.0025
- Blacklists near-ATM buckets
- Sets `snapshot.best_side` based on whichever side has positive edge

### `_should_exit` / `close_shadow_trade` chain
```
_should_exit_exit_v2 (line 5812)            — side-aware, uses _se_side_bid(snapshot, side)
_close_shadow_trade_exit_v2 (line ~5900)    — exits on yes_bid (YES) or no_bid (NO)
```

---

## Patch Layer Index (line numbers in main.py)

| Line | Patch Name | What it does |
|------|-----------|--------------|
| 1 | loop_traceback_v37 | Adds traceback to health dict |
| ~2565 | _mp_patch_shadow_engine | Paper controls wrapper (BYPASSED by later patches) |
| ~4280 | evaluate_market_eligibility_btc_range_window | BTC range window filter |
| ~4488 | generate_intent_side_aware | Side-aware order intent |
| ~5274 | architect_status_layer | Status enrichment |
| ~5574 | architect_exit_v2 | Replaces _should_exit, open/close/mark with side-aware versions |
| 5920 | Wire exit_v2 | `ShadowEngine.open_shadow_trade = _open_shadow_trade_exit_v2` |
| ~6030 | arch_cohort | Adds cohort metadata to open_shadow_trade |
| 6073 | Wire cohort | `_ArchShadowEngine.open_shadow_trade = _open_shadow_trade_arch_cohort` |
| ~8503 | microstructure_truth_foundation_v1 | OBI/VPIN/toxicity infrastructure |
| ~11230 | raw_close_time_snapshot_contract_v1 | _arch_rct_rewrite wired to refresh_markets |
| ~11466 | cluster_inventory_foundation_v1 | Cluster counting |
| ~12514 | paper_using_newest_infra_v1 | Paper infra bridge |
| ~17577 | institutional_quant_source_of_truth_v2 | Deribit DVOL, OBI, VPIN data |
| ~17837 | institutional_execution_gate_v1 | `_arch_iq_*` functions; wires _ArchShadowEngine |
| 18471 | Wire inst. gate | `_ArchShadowEngine.open_shadow_trade = _arch_iq_open_shadow_trade` |
| 18473 | Wire inst. gate | `_ArchShadowEngine.run_shadow_variants = _arch_iq_run_shadow_variants` |
| ~18533 | inst_entry_gate_hotfix_v1 | Gate hotfix |
| ~18879–21438 | v1–v11 institutional layers | Edge floors, prime-time, markouts, row pickers |
| ~23636 | fetch_rows_compat_v23 | DB fetch helpers |
| ~24225 | quant_gate_v29 | `_aqg_decide`, `_aqg_cfg`, `_aqg_stats` |
| ~25127 | quant_gate_v29d | Instance-level shadow attach |
| 25353 | quant_gate_v29e | `ShadowEngine.open_shadow_trade = _aqg_open_shadow_trade_wrapper` |
| ~25378 | quant_gate_v30 | Throughput relief |
| ~25853 | quant_gate_v33 | shadow variant bridge |
| ~25986 | quant_gate_v34 | run_shadow_variants bridge |
| ~26196 | quant_gate_v35 | variant source fix |
| ~27491 | architect_data_api_v1 | Data API routes |
| ~28720 | data_truth_v44 | `_aqg_passes_variant_rules_v44`; `setattr(_SE, "_passes_variant_rules", ...)` |
| 29064 | Wire v44 | `setattr(_SE, "run_shadow_variants", _aqg_run_shadow_variants_v44)` |
| ~29073 | data_truth_v45 | `_aqg_passes_variant_rules_v45`; supersedes v44 |
| 29289 | Wire v45 | `setattr(_SE, "_passes_variant_rules", _aqg_passes_variant_rules_v45)` |
| ~29298 | data_truth_v46 | **`_aqg_passes_variant_rules_v46` — FINAL ACTIVE** |
| 29523 | Wire v46 | `setattr(_SE, "_passes_variant_rules", _aqg_passes_variant_rules_v46)` |
| ~29532 | paper_maker_v47 | **`_aqg_run_shadow_variants_v47` — REAL WORK** |
| 29847 | Wire v47 | `setattr(_SE, "run_shadow_variants", _aqg_run_shadow_variants_v47)` |
| ~29856 | maker_enforce_v48 | `_aqg_open_shadow_trade_v48` |
| 29980 | Wire v48 | `setattr(_SE, "open_shadow_trade", _aqg_open_shadow_trade_v48)` |
| ~29989 | universe_breadth_v49 | `_aqg_is_shadow_candidate_v49` |
| ~30054 | universe_probe_v50 | **Stats wrapper; ACTIVE** |
| 30127 | Wire v50 | `setattr(_SE, "run_shadow_variants", _aqg_run_shadow_variants_v50)` |
| ~30136 | multi_asset_bootstrap_v51 | `refresh_markets` multi-asset |
| ~30326–30837 | v52–v54 | refresh_markets patches |
| ~30846 | loop_watchdog_v55 | Loop health watchdog |
| ~31066 | watchdog_bootstrap_v56 | dashboard_status patch |
| ~31532 | cache_stringify_v58 | cached_crypto_tickers stringify |
| ~31664 | refresh_to_entry_v59 | refresh_markets patch |
| ~31765 | live_instance_bridge_v60 | **Instance-level wiring of run_shadow_variants + refresh_markets** |
| 32028 | Wire v60 run | `setattr(eng_obj, "run_shadow_variants", _run_shadow_variants_v60)` |
| 31972 | Wire v60 refresh | `setattr(bot_obj, "refresh_markets", _refresh_markets_v60)` |
| ~32055 | normalize_compat_v26b | Normalize compat |
| ~32350 | status_perf_cache_v27 | Status perf cache |
| ~32746 | icrp_v19_float_v27c | ICRP float fix |
| ~32884 | async_bridge_v28 | **`_run_shadow_variants_sync` — outermost wrapper** |
| 32982 | Wire v28 | `setattr(eng, "run_shadow_variants", _run_shadow_variants_sync)` |
| ~33026 | async_future_guard_v29 | Async guard |
| ~33114 | snapshot_contract_compat_v30b | Snapshot dict/object compat |
| ~33386 | json_datetime_sanitize_v30c | JSON serialization safety |
| ~33453 | paper_freeze_legacy_crypto_lag_v31 | Blocks legacy variant names |
| ~33515 | newest_infra_dict_source_fix_v32b | Dict source fix |
| ~33741 | entry_truth_v33 | Entry price truth |
| ~34070 | snapshot_quote_truth_v34 | Quote truth route |
| ~34214 | datetime_type_guard_v36 | datetime guard |
| ~34334 | startup_prime_v38 | Startup priming |
| ~34548 | snapshot_bootstrap_v39 | Snapshot bootstrap |
| ~34877 | kalshi_stream_marketdata_v40 | **Attaches KalshiMarketStreamV40 WebSocket feed** |

---

## Critical Invariants (never violate these)

1. **Side-aware entry price:** YES trades use `yes_ask`; NO trades use `no_ask`. Never use `no_ask` as a YES entry price.
2. **`_arch_iq_safe_float(None, 0.0)` returns 0.0 not None.** This means `yes_ask=None → entry_from_book=0.0`. The safety guard added at line 18183 (`if not entry_from_book: return False`) prevents this from silently opening a wrong-price trade.
3. **`raw_close_time` must be a datetime object** (not ISO string) for `_hours_to_expiry()` to work. `_arch_rct_rewrite_market_snapshots()` does this conversion after each `refresh_markets()`.
4. **`side=` kwarg must propagate** through every `open_shadow_trade` wrapper. v47 fill loop now correctly passes `side=order.get("side")` (fixed 2026-03-10).
5. **v29e gate checks `snapshot.best_side`** for OBI/edge gate, then passes `**kwargs` through. If `side` is in kwargs, it reaches `_arch_iq_open_shadow_trade`. If not, it defaults to "yes".
6. **`_passes_variant_rules` (v46) sets `snapshot.best_side`** before returning True. The queued order stores `side` in the pending dict. The fill loop must pass that `side` back.
7. **Only "crypto_lag_aggressive" variant is active.** All others are blocked by v46 (`only_variant`) and v31 (legacy freeze).
8. **Settlement reference:** CFB (Coinbase)/RTI 60s average, NOT last price. Exit logic uses `_se_side_bid(snapshot, side)` as mark.

---

## Key Config Values (active as of 2026-03-10)

### `_AQG_V46` (line ~29303) — variant rules
```python
{
    "only_variant": "crypto_lag_aggressive",
    "yes_min_edge": 0.015,
    "no_min_edge": 0.010,
    "no_min_net_after_spread": 0.0025,
    "price_improve": (inherited from v47),
    # Blacklists buckets within ~1-2% of spot
}
```

### `_AQG_V47` (line ~29538) — maker orders
```python
{
    "only_variant": "crypto_lag_aggressive",
    "price_improve": 0.01,      # Queue at ask - 0.01
    "max_wait_seconds": 300,    # Cancel after 5 min
    "default_ticket_dollars": 25,
}
```

### `_aqg_cfg` (v29, line 24236) — quant gate
```python
{
    "gamma": 0.10,
    "obi_yes_min": 0.50,
    "obi_no_max": -0.50,
    "toxicity_bps_1s_max": 4.0,
    "settlement_max_entry_price": 0.80,
}
```

---

## Database Schema (strategy_lab.db)

### `shadow_trades`
`id, opened_at, closed_at, strategy_name, variant_name, regime_name, ticker, side, qty, entry_price, exit_price, outcome, realized_pnl, max_favorable_excursion, max_adverse_excursion, edge_at_entry, spread_at_entry, hours_to_expiry_at_entry, distance_from_spot_pct_at_entry, meta_json, close_reason, best_mark_price, worst_mark_price, entry_edge, edge_at_exit, spread_at_exit, distance_from_spot_pct_at_exit, minutes_open`

### `shadow_positions`
`ticker, variant_name, opened_at, strategy_name, regime_name, side, qty, entry_price, last_mark_price, unrealized_pnl, best_mark_price, worst_mark_price, entry_edge, last_edge, last_spread`

### `candidate_events`
`id, ts, strategy_name, variant_name, ticker, market_family, status, spot_symbol, spot_price, floor_strike, cap_strike, threshold, direction, yes_bid, yes_ask, no_bid, no_ask, mid, spread, fair_yes, edge, hours_to_expiry, distance_from_spot_pct, liquidity, volume_24h, open_interest, eligible, rejection_reason, regime_name, meta_json`

---

## Bugs Fixed

### 2026-03-10: Shadow trade side confusion (105 bad trades, ~$512 losses)
**Symptoms:** YES trades opened on out-of-range BTC buckets at 0.83–0.88; exit at 0.12–0.17; loss ~$3.50–3.80 each.

**Root cause chain:**
1. v46 correctly identifies bucket as NO-side (fair_yes≈0.02, no_edge≈0.10)
2. v47 queues NO maker order at `no_ask - 0.01 ≈ 0.86`
3. On fill, v47 called `open_shadow_trade(..., target_price=0.86)` **without `side="no"`**
4. `_arch_iq_open_shadow_trade` defaults `side="yes"`; `yes_ask=None` → `_arch_iq_safe_float(None, 0.0) = 0.0` → `entry_from_book=0.0 in (None, 0.0)` → fallback to `entry_price=0.86`
5. Trade recorded as YES at 0.86; immediate ~$3.70 loss

**Fix 1** (line 29721): `side=order.get("side")` added to fill call.
**Fix 2** (line 18183): `if not entry_from_book: return False` safety guard.
**Fix 3** (line 29789): Restored `self.log_candidate(...)` in v47 queue loop.

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| PAPER_MODE | true | Paper vs live |
| ARM_LIVE_TRADING | false | Live safety arm |
| KALSHI_API_KEY_ID | — | API auth |
| KALSHI_PRIVATE_KEY_PATH | /app/kalshi_private_key.pem | RSA key |
| KALSHI_BASE_URL | https://api.elections.kalshi.com/trade-api/v2 | API base |
| POLL_SECONDS | 15 | Main loop interval |
| FULL_MARKET_SCAN_SECONDS | 3600 | Full scan interval |
| MAX_MARKETS | 500 | Market fetch limit |
| KALSHI_TAKER_FEE_ESTIMATE | 0.015 | Fee used in edge calc |
| KALSHI_MIN_EXECUTION_EDGE | 0.02 | Live order min edge |

---

## How to Add a New Patch

Every patch follows this pattern at the bottom of main.py:

```python
# --- patch name vN ---
try:
    _patch_name_vN_applied = True

    # ... define functions ...

    # Wire it:
    setattr(ShadowEngine, "method_name", new_function)
    # or:
    setattr(_SE, "method_name", new_function)

except Exception as e:
    try:
        print(f"[patch name vN] {repr(e)}")
    except Exception:
        pass
# --- end patch name vN ---
```

**Rules:**
- Always wrap in try/except so a failed patch doesn't crash the bot
- Use `getattr(prev_method, "_already_wrapped_flag", False)` guard to prevent double-wrap
- Always preserve the `prev` reference for the chain to work
- For `open_shadow_trade`, always accept `*args, **kwargs` and pass them through so `side=` propagates
- Never set `side` default before checking kwargs

---

## API Endpoints (key ones)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/status` | GET | Full bot status JSON |
| `/api/paper-session` | GET | Open trades, session PnL |
| `/api/paper-controls` | GET | Paper mode controls |
| `/architect/data` | GET | Architect payload (market snapshot, strategy stats) |
| `/api/strategy-lab/settings` | GET | Strategy variant settings |
| `/api/debug/paper-freeze-legacy-crypto-lag-v31` | GET | v31 freeze stats |
