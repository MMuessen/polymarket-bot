"""
MarketEnrichmentService: applies fair-value calculations to a MarketSnapshot.

This is the only place that mutates snapshot.fair_yes / edge / best_side etc.
It calls the pure functions in strategy/fair_value.py and writes results back
onto the snapshot in-place (since snapshots are owned by SnapshotStore and
already protected by a lock at the write boundary).

No I/O here — just CPU-bound math.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from ..market.snapshot import MarketSnapshot
from ..strategy.fair_value import (
    bucket_probability,
    threshold_probability,
    enrich_snapshot,
    distance_from_spot_pct,
    distance_bucket_label,
)

logger = logging.getLogger(__name__)


class MarketEnrichmentService:
    """
    Stateless enrichment service. Inject spot prices at call time.
    """

    def __init__(self, fee_estimate: float = 0.015) -> None:
        self.fee_estimate = fee_estimate

    def enrich(
        self,
        snapshot: MarketSnapshot,
        spot_prices: Dict[str, float],
        annual_vol_btc: float = 0.60,
        now: Optional[datetime] = None,
    ) -> None:
        """
        Compute and write fair-value fields onto the snapshot in-place.
        Clears all derived fields before recomputing so stale values never persist.
        """
        _now = now or datetime.now(timezone.utc)

        # Clear derived fields
        snapshot.fair_yes = None
        snapshot.execution_edge_yes = None
        snapshot.execution_edge_no = None
        snapshot.edge = None
        snapshot.best_side = None
        snapshot.best_exec_edge = None
        snapshot.hours_to_expiry = None
        snapshot.rationale = ""

        # Need spot price
        symbol = snapshot.spot_symbol or ""
        spot = spot_prices.get(symbol.upper(), 0.0)
        if spot <= 0:
            snapshot.rationale = f"no_spot_price:{symbol}"
            return

        # Need close time
        if snapshot.raw_close_time is None:
            snapshot.rationale = "missing_close_time"
            return

        if not isinstance(snapshot.raw_close_time, datetime):
            snapshot.rationale = "close_time_not_datetime"
            return

        hours = max((snapshot.raw_close_time - _now).total_seconds() / 3600.0, 0.0)
        if hours <= 0:
            snapshot.rationale = "market_expired"
            return

        snapshot.hours_to_expiry = round(hours, 4)

        # Compute distance from spot and bucket label
        dist = distance_from_spot_pct(
            spot,
            snapshot.floor_strike,
            snapshot.cap_strike,
            snapshot.threshold,
        )
        if dist is not None:
            snapshot.distance_from_spot_pct = round(dist, 6)
            snapshot.distance_bucket = distance_bucket_label(dist)

        # Compute fair_yes
        fair_yes: Optional[float] = None

        if snapshot.market_family == "range_bucket":
            fair_yes = bucket_probability(
                spot=spot,
                low=snapshot.floor_strike,
                high=snapshot.cap_strike,
                hours_to_expiry=hours,
                annual_vol=annual_vol_btc,
            )
            if fair_yes is None:
                snapshot.rationale = "bucket_probability_unavailable"
                return
        elif snapshot.threshold is not None and snapshot.direction is not None:
            fair_yes = threshold_probability(
                spot=spot,
                threshold=snapshot.threshold,
                direction=snapshot.direction,
                hours_to_expiry=hours,
                asset=symbol,
            )
            if fair_yes is None:
                snapshot.rationale = "threshold_probability_unavailable"
                return
        else:
            snapshot.rationale = "unrecognized_market_structure"
            return

        # Compute execution edges
        enriched = enrich_snapshot(
            fair_yes=fair_yes,
            yes_ask=snapshot.yes_ask,
            no_ask=snapshot.no_ask,
            fee_estimate=self.fee_estimate,
            spot=spot,
            low=snapshot.floor_strike,
            high=snapshot.cap_strike,
            hours=hours,
            ticker=snapshot.ticker,
        )

        snapshot.fair_yes = enriched.fair_yes
        snapshot.execution_edge_yes = enriched.execution_edge_yes
        snapshot.execution_edge_no = enriched.execution_edge_no
        snapshot.edge = enriched.best_exec_edge
        snapshot.best_side = enriched.best_side
        snapshot.best_exec_edge = enriched.best_exec_edge
        snapshot.entry_fee_yes = enriched.entry_fee_yes
        snapshot.entry_fee_no = enriched.entry_fee_no
        snapshot.rationale = (
            f"{symbol} bucket fair_yes={enriched.fair_yes:.4f}; "
            f"exec_yes={enriched.execution_edge_yes} "
            f"exec_no={enriched.execution_edge_no}; "
            f"spot=${spot:,.0f} "
            f"low={snapshot.floor_strike} high={snapshot.cap_strike} "
            f"hours={hours:.2f}"
        )
        snapshot.last_enriched_at = _now
