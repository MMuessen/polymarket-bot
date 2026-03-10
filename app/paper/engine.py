"""
PaperEngine: orchestrates one paper trading cycle.

Called once per main loop tick. Owns the MakerQueue (in-memory state) and
coordinates all repo writes through the repository layer.

Cycle order:
  1. Enrich all snapshots with latest fair values
  2. Mark open positions (update unrealized PnL)
  3. Run exit pass (close positions that hit exit conditions)
  4. Fill pending maker orders (side= always passed explicitly — never defaults)
  5. Queue new maker orders for eligible markets

No direct SQL here — all DB work via repos.
No monkey-patches. No globals.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..market.enrichment import MarketEnrichmentService
from ..market.snapshot import MarketSnapshot, SnapshotStore
from ..paper.maker import MakerQueue
from ..paper.exit import evaluate_exit, ExitConfig
from ..persistence.repos.trades import ShadowTradeRepository
from ..persistence.repos.positions import ShadowPositionRepository
from ..persistence.repos.candidates import CandidateEventRepository
from ..strategy.variant_rules import (
    passes_variant_rules,
    choose_side,
    RulesConfig,
    RulesInput,
)
from ..strategy.gate import evaluate_gate, GateConfig

logger = logging.getLogger(__name__)

# Only active variant
_ACTIVE_VARIANT = "crypto_lag_aggressive"
_STRATEGY_NAME = "crypto_lag"


class PaperEngine:
    """
    Clean, fully async paper trading engine.

    Dependencies are injected; no globals or singletons.
    """

    def __init__(
        self,
        *,
        store: SnapshotStore,
        enrichment: MarketEnrichmentService,
        trades_repo: ShadowTradeRepository,
        positions_repo: ShadowPositionRepository,
        candidates_repo: CandidateEventRepository,
        rules_cfg: Optional[RulesConfig] = None,
        gate_cfg: Optional[GateConfig] = None,
        exit_cfg: Optional[ExitConfig] = None,
        maker_price_improve: float = 0.01,
        maker_max_wait_seconds: float = 300.0,
        default_ticket_dollars: float = 25.0,
        enabled: bool = True,
    ) -> None:
        self._store = store
        self._enrichment = enrichment
        self._trades = trades_repo
        self._positions = positions_repo
        self._candidates = candidates_repo
        self._rules_cfg = rules_cfg or RulesConfig()
        self._gate_cfg = gate_cfg or GateConfig()
        self._exit_cfg = exit_cfg or ExitConfig()
        self._maker = MakerQueue(
            price_improve=maker_price_improve,
            max_wait_seconds=maker_max_wait_seconds,
        )
        self._ticket_dollars = default_ticket_dollars
        self.enabled = enabled
        self._cycle_stats: Dict[str, Any] = {}

    # ------------------------------------------------------------------ #
    # Public API                                                            #
    # ------------------------------------------------------------------ #

    async def run_cycle(self, spot_prices: Dict[str, float]) -> Dict[str, Any]:
        """
        Run one full paper trading cycle. Returns a stats dict for observability.
        """
        if not self.enabled:
            return {"skipped": "disabled"}

        now = datetime.now(timezone.utc)
        stats: Dict[str, Any] = {
            "cycle_at": now.isoformat(),
            "enriched": 0,
            "marks_updated": 0,
            "exits": 0,
            "fills": 0,
            "queued": 0,
            "expired": 0,
            "errors": [],
        }

        snapshots = self._store.snapshot()

        # 1. Enrich
        for snap in snapshots.values():
            try:
                self._enrichment.enrich(snap, spot_prices, now=now)
                stats["enriched"] += 1
            except Exception as exc:
                stats["errors"].append(f"enrich:{snap.ticker}:{exc}")

        # 2. Mark positions
        positions = await self._positions.list_all()
        for pos in positions:
            try:
                await self._mark_position(pos, snapshots)
                stats["marks_updated"] += 1
            except Exception as exc:
                stats["errors"].append(f"mark:{pos.get('ticker')}:{exc}")

        # 3. Exit pass
        for pos in positions:
            try:
                closed = await self._maybe_exit(pos, snapshots, now)
                if closed:
                    stats["exits"] += 1
            except Exception as exc:
                stats["errors"].append(f"exit:{pos.get('ticker')}:{exc}")

        # 4. Fill pending maker orders
        expired = self._maker.expire_old(now)
        stats["expired"] = len(expired)

        for order in self._maker.all_orders():
            try:
                filled = await self._maybe_fill(order, snapshots)
                if filled:
                    stats["fills"] += 1
            except Exception as exc:
                stats["errors"].append(f"fill:{order.ticker}:{exc}")

        # 5. Queue new orders
        open_total = await self._positions.count_all()
        for ticker, snap in snapshots.items():
            if snap.market_family != "range_bucket":
                continue
            if not snap.is_tradeable():
                continue
            try:
                queued = await self._maybe_queue(snap, open_total, now)
                if queued:
                    stats["queued"] += 1
            except Exception as exc:
                stats["errors"].append(f"queue:{ticker}:{exc}")

        self._cycle_stats = stats
        return stats

    def maker_stats(self) -> dict:
        return self._maker.stats()

    def last_cycle_stats(self) -> dict:
        return dict(self._cycle_stats)

    # ------------------------------------------------------------------ #
    # Internal: mark                                                        #
    # ------------------------------------------------------------------ #

    async def _mark_position(
        self,
        pos: Dict[str, Any],
        snapshots: Dict[str, MarketSnapshot],
    ) -> None:
        ticker = pos["ticker"]
        variant = pos["variant_name"]
        side = str(pos.get("side") or "yes").lower()
        snap = snapshots.get(ticker)
        if snap is None:
            return

        mark = snap.side_bid(side)
        if mark is None:
            mark = snap.mid
        if mark is None:
            return

        result = await self._positions.update_mark(
            ticker=ticker,
            variant_name=variant,
            mark=float(mark),
            edge=snap.best_exec_edge,
            spread=snap.spread,
        )
        if result:
            await self._trades.update_excursions(
                ticker=ticker,
                variant_name=variant,
                best_mark=result["best_mark"],
                worst_mark=result["worst_mark"],
                mfe=result["mfe"],
                mae=result["mae"],
            )

    # ------------------------------------------------------------------ #
    # Internal: exit                                                        #
    # ------------------------------------------------------------------ #

    async def _maybe_exit(
        self,
        pos: Dict[str, Any],
        snapshots: Dict[str, MarketSnapshot],
        now: datetime,
    ) -> bool:
        ticker = pos["ticker"]
        variant = pos["variant_name"]
        side = str(pos.get("side") or "yes").lower()
        snap = snapshots.get(ticker)
        if snap is None:
            return False

        entry = float(pos.get("entry_price") or 0.0)
        qty = int(pos.get("qty") or 0)
        best_mark = float(pos.get("best_mark_price") or entry)

        mark = snap.side_bid(side)
        if mark is None:
            mark = snap.mid
        if mark is None:
            return False

        hours = snap.hours_until_expiry(now)

        should_exit, reason = evaluate_exit(
            side=side,
            entry_price=entry,
            qty=qty,
            mark=float(mark),
            best_mark=best_mark,
            hours_to_expiry=hours,
            spread_now=snap.spread,
            best_exec_edge_now=snap.best_exec_edge,
            cfg=self._exit_cfg,
        )

        if not should_exit:
            return False

        exit_price = float(mark)
        realized_pnl = (exit_price - entry) * qty

        closed = await self._trades.close_trade(
            ticker=ticker,
            variant_name=variant,
            exit_price=exit_price,
            realized_pnl=realized_pnl,
            close_reason=reason or "unknown",
            edge_at_exit=snap.best_exec_edge,
            spread_at_exit=snap.spread,
        )
        if closed:
            await self._positions.close_position(ticker, variant)
            logger.info(
                "Closed %s %s side=%s reason=%s pnl=%.4f",
                variant, ticker, side, reason, realized_pnl,
            )
        return closed

    # ------------------------------------------------------------------ #
    # Internal: fill maker order                                            #
    # ------------------------------------------------------------------ #

    async def _maybe_fill(
        self,
        order: Any,  # PendingOrder
        snapshots: Dict[str, MarketSnapshot],
    ) -> bool:
        snap = snapshots.get(order.ticker)
        if snap is None:
            self._maker.remove(order.ticker, order.variant_name)
            return False

        # Already open?
        if await self._positions.is_open(order.ticker, order.variant_name):
            self._maker.remove(order.ticker, order.variant_name)
            return False

        # Get current ask for the queued side
        current_ask = snap.side_ask(order.side)
        if current_ask is None or current_ask <= 0:
            return False

        filled_order = self._maker.check_fill(order.ticker, order.variant_name, current_ask)
        if filled_order is None:
            return False

        # Institutional gate check
        if snap.fair_yes is not None and snap.hours_to_expiry is not None:
            gate = evaluate_gate(
                side=order.side,
                entry_price=filled_order.target_price,
                fair_yes=snap.fair_yes,
                hours_to_expiry=snap.hours_to_expiry,
                obi=snap.obi,
                vpin=snap.vpin,
                cfg=self._gate_cfg,
            )
            if not gate.allow:
                logger.debug(
                    "Gate blocked fill %s %s: %s",
                    order.variant_name, order.ticker, gate.reasons,
                )
                self._maker.remove(order.ticker, order.variant_name)
                return False

        # Safety: entry_price must be the real ask on the correct side, not a fallback
        entry_price = float(current_ask)  # use live ask at fill time, not target
        if entry_price <= 0.0 or entry_price >= 1.0:
            self._maker.remove(order.ticker, order.variant_name)
            return False

        qty = filled_order.qty()

        # Open trade in DB — side= always explicit, never defaults
        await self._trades.open_trade(
            strategy_name=_STRATEGY_NAME,
            variant_name=order.variant_name,
            regime_name=None,
            ticker=order.ticker,
            side=order.side,           # ← always explicit
            qty=qty,
            entry_price=entry_price,
            edge_at_entry=snap.best_exec_edge,
            spread_at_entry=snap.spread,
            hours_to_expiry_at_entry=snap.hours_to_expiry,
            distance_from_spot_pct_at_entry=snap.distance_from_spot_pct,
            meta={
                "rationale": snap.rationale,
                "fill_ask": current_ask,
                "target_price": filled_order.target_price,
                "side": order.side,
            },
        )
        await self._positions.open_position(
            ticker=order.ticker,
            variant_name=order.variant_name,
            strategy_name=_STRATEGY_NAME,
            regime_name=None,
            side=order.side,           # ← always explicit
            qty=qty,
            entry_price=entry_price,
            entry_edge=snap.best_exec_edge,
            initial_mark=entry_price,
        )

        self._maker.remove(order.ticker, order.variant_name)
        logger.info(
            "Filled %s %s side=%s qty=%d @ %.4f",
            order.variant_name, order.ticker, order.side, qty, entry_price,
        )
        return True

    # ------------------------------------------------------------------ #
    # Internal: queue new maker order                                       #
    # ------------------------------------------------------------------ #

    async def _maybe_queue(
        self,
        snap: MarketSnapshot,
        open_total: int,
        now: datetime,
    ) -> bool:
        ticker = snap.ticker
        variant = _ACTIVE_VARIANT

        # Already has pending order or open position?
        if self._maker.has(ticker, variant):
            return False
        if await self._positions.is_open(ticker, variant):
            return False

        # Pre-fetch counts for rules evaluation
        open_per_variant = await self._positions.count_by_variant(variant)
        open_per_ticker = await self._positions.count_by_ticker(ticker)
        attempts_today = await self._trades.count_today(ticker, variant)
        recent_closed = await self._trades.get_recent_closed(
            ticker, variant, within_minutes=self._rules_cfg.reentry_cooldown_minutes
        )
        recent_loss = await self._trades.get_recent_closed(
            ticker, variant, within_minutes=self._rules_cfg.loss_ban_minutes
        )

        # Cooldown timing
        recent_close_minutes: Optional[float] = None
        if recent_closed:
            try:
                closed_at = datetime.fromisoformat(recent_closed["closed_at"])
                recent_close_minutes = (now - closed_at).total_seconds() / 60.0
            except Exception:
                pass

        recent_loss_minutes: Optional[float] = None
        if recent_loss and str(recent_loss.get("outcome") or "").lower() == "loss":
            try:
                closed_at = datetime.fromisoformat(recent_loss["closed_at"])
                recent_loss_minutes = (now - closed_at).total_seconds() / 60.0
            except Exception:
                pass

        inp = RulesInput(
            variant_name=variant,
            ticker=ticker,
            market_family=snap.market_family,
            status=snap.status,
            raw_close_time=snap.raw_close_time,
            spread=snap.spread,
            yes_ask=snap.yes_ask,
            no_ask=snap.no_ask,
            execution_edge_yes=snap.execution_edge_yes,
            execution_edge_no=snap.execution_edge_no,
            distance_from_spot_pct=snap.distance_from_spot_pct,
            open_total=open_total,
            open_per_variant=open_per_variant,
            open_per_ticker=open_per_ticker,
            open_per_cluster=0,    # cluster counting omitted for simplicity; add if needed
            open_per_cluster_all=0,
            attempts_today=attempts_today,
            recent_loss_minutes_ago=recent_loss_minutes,
            recent_close_minutes_ago=recent_close_minutes,
        )

        eligible, reason = passes_variant_rules(inp, self._rules_cfg, now=now)

        # Log candidate event (always)
        await self._candidates.log(
            strategy_name=_STRATEGY_NAME,
            variant_name=variant,
            ticker=ticker,
            market_family=snap.market_family,
            status=snap.status,
            spot_symbol=snap.spot_symbol,
            spot_price=None,
            floor_strike=snap.floor_strike,
            cap_strike=snap.cap_strike,
            threshold=snap.threshold,
            direction=snap.direction,
            yes_bid=snap.yes_bid,
            yes_ask=snap.yes_ask,
            no_bid=snap.no_bid,
            no_ask=snap.no_ask,
            mid=snap.mid,
            spread=snap.spread,
            fair_yes=snap.fair_yes,
            edge=snap.best_exec_edge,
            hours_to_expiry=snap.hours_to_expiry,
            distance_from_spot_pct=snap.distance_from_spot_pct,
            liquidity=snap.liquidity,
            volume_24h=snap.volume_24h,
            open_interest=snap.open_interest,
            eligible=eligible,
            rejection_reason=None if eligible else reason,
            regime_name=None,
            meta={"rationale": snap.rationale},
        )

        if not eligible:
            return False

        # Choose side — the side with the best positive edge
        side = choose_side(
            snap.execution_edge_yes,
            snap.execution_edge_no,
            self._rules_cfg,
        )
        if side is None:
            return False

        # Get current ask on the chosen side
        ask = snap.side_ask(side)
        if ask is None or ask <= 0:
            return False

        self._maker.queue(
            ticker=ticker,
            variant_name=variant,
            side=side,               # ← stored explicitly in the order
            current_ask=ask,
            dollars=self._ticket_dollars,
            spread_at_queue=snap.spread,
            edge_at_queue=snap.best_exec_edge,
        )
        logger.debug("Queued maker order: %s %s side=%s ask=%.4f", variant, ticker, side, ask)
        return True
