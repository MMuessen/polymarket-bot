"""
Composition root — wires all subsystems together.

No business logic here. Every dependency is constructed once and injected.
FastAPI lifespan handles startup/shutdown ordering.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import aiohttp
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config.settings import get_settings
from .observability.logging import configure_logging
from .observability.health import HealthRegistry

from .persistence.db import Database
from .persistence.repos.trades import ShadowTradeRepository
from .persistence.repos.positions import ShadowPositionRepository
from .persistence.repos.candidates import CandidateEventRepository
from .persistence.repos.settings import StrategySettingsRepository

from .market.snapshot import SnapshotStore
from .market.enrichment import MarketEnrichmentService
from .market.coinbase_ws import CoinbaseSpotFeed
from .market.kalshi_stream import KalshiMarketStream

from .paper.engine import PaperEngine
from .paper.accounting import PaperAccounting

from .jobs.loop import TradingLoop, StatusCache
from .jobs.outcome_resolver import OutcomeResolverJob
from .jobs.summarizer import SummarizerJob

from .api.routes.health import make_health_router
from .api.routes.status import make_status_router
from .api.routes.paper import make_paper_router
from .api.routes.debug import make_debug_router
from .api.routes.strategy_lab import make_strategy_lab_router

configure_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
cfg = get_settings()

# --------------------------------------------------------------------------- #
# Singleton construction                                                        #
# --------------------------------------------------------------------------- #

health = HealthRegistry()
db = Database(cfg.strategy_lab_db_path)
store = SnapshotStore()
status_cache = StatusCache()

trades_repo = ShadowTradeRepository(db)
positions_repo = ShadowPositionRepository(db)
candidates_repo = CandidateEventRepository(db)
settings_repo = StrategySettingsRepository(db)

enrichment = MarketEnrichmentService(fee_estimate=cfg.kalshi_taker_fee_estimate)
coinbase = CoinbaseSpotFeed()

kalshi_stream = KalshiMarketStream(
    store=store,
    api_key_id=cfg.kalshi_api_key_id,
    private_key_path=cfg.kalshi_private_key_path,
    poll_interval=float(cfg.poll_seconds),
)

paper_engine = PaperEngine(
    store=store,
    enrichment=enrichment,
    trades_repo=trades_repo,
    positions_repo=positions_repo,
    candidates_repo=candidates_repo,
    default_ticket_dollars=25.0,
    enabled=True,
)

accounting = PaperAccounting(
    trades_repo=trades_repo,
    positions_repo=positions_repo,
    starting_balance=cfg.paper_starting_balance,
)

trading_loop = TradingLoop(
    store=store,
    coinbase=coinbase,
    kalshi_stream=kalshi_stream,
    paper_engine=paper_engine,
    accounting=accounting,
    status_cache=status_cache,
    health=health,
    poll_seconds=cfg.poll_seconds,
)

outcome_resolver = OutcomeResolverJob(
    store=store,
    trades_repo=trades_repo,
    positions_repo=positions_repo,
    health=health,
)

summarizer = SummarizerJob(db=db, health=health)


# --------------------------------------------------------------------------- #
# Lifespan                                                                      #
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Starting kalshi-bot v2")

    # DB init + migrations
    await db.init()

    # Shared HTTP session
    connector = aiohttp.TCPConnector(limit=20)
    session = aiohttp.ClientSession(connector=connector)

    try:
        # Prime spot prices from REST before WS connects
        await coinbase.prime_from_rest(session)

        # Start background feeds
        await coinbase.start(session)
        await kalshi_stream.start(session)

        # Start background jobs
        await trading_loop.start()
        await outcome_resolver.start()
        await summarizer.start()

        logger.info("kalshi-bot v2 ready")
        yield

    finally:
        logger.info("Shutting down kalshi-bot v2")
        await trading_loop.stop()
        await outcome_resolver.stop()
        await summarizer.stop()
        await coinbase.stop()
        await kalshi_stream.stop()
        await session.close()
        logger.info("kalshi-bot v2 shutdown complete")


# --------------------------------------------------------------------------- #
# FastAPI app                                                                   #
# --------------------------------------------------------------------------- #

app = FastAPI(title="Kalshi Bot v2", version="2.0.0", lifespan=lifespan)

# Static files + templates
_STATIC = Path(__file__).parent / "static"
_STATIC.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

# Routers
app.include_router(make_health_router(health))
app.include_router(make_status_router(status_cache))
app.include_router(make_paper_router(accounting, paper_engine, db))
app.include_router(make_debug_router(store, paper_engine, candidates_repo, status_cache))
app.include_router(make_strategy_lab_router(db, settings_repo))


@app.get("/", response_class=HTMLResponse)
async def index():
    template_path = Path(__file__).parent / "templates" / "index_v2.html"
    if template_path.exists():
        return HTMLResponse(template_path.read_text())
    return HTMLResponse("<h1>kalshi-bot v2 starting…</h1>")
