from app.strategy_lab.storage import init_db as init_strategy_lab_db
from app.strategy_lab.shadow_engine import ShadowEngine
from app.strategy_lab.outcomes import OutcomeResolver
from app.strategy_lab.suggestions import SummaryAndSuggestionEngine
from app.strategy_lab.gui_api import get_latest_summaries, get_latest_suggestions, get_shadow_positions, get_operator_settings, save_operator_setting
from zoneinfo import ZoneInfo
import asyncio
import base64
import math
import os
import re
import sys
import platform
import socket
import json
import uuid
from collections import Counter, deque
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import aiohttp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from app.strategy_lab.operator_settings import get_operator_settings, set_operator_setting
from app.debug_handoff import generate_handoff_bundle, append_memory_note, read_memory_text

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
STATIC_DIR = BASE_DIR / "static"
TEMPLATE_DIR = BASE_DIR / "templates"

DATA_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "trades.db"
CRYPTO_CACHE_PATH = DATA_DIR / "crypto_market_cache.json"
BTC_CACHE_META_PATH = DATA_DIR / "btc_cache_meta.json"
CANDIDATE_LOG_PATH = DATA_DIR / "candidate_events.jsonl"

engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    shadow_group_id = Column(String, nullable=True)
    intent_id = Column(String, nullable=False, unique=True)
    mode = Column(String, nullable=False)
    broker = Column(String, nullable=False)
    strategy = Column(String, nullable=False)
    ticker = Column(String, nullable=False)
    title = Column(String, nullable=True)
    side = Column(String, nullable=False)
    action = Column(String, nullable=False)
    contracts = Column(Float, nullable=False)
    requested_price = Column(Float, nullable=False)
    fill_price = Column(Float, nullable=True)
    fair_value = Column(Float, nullable=True)
    edge = Column(Float, nullable=True)
    spot_symbol = Column(String, nullable=True)
    spot_price = Column(Float, nullable=True)
    status = Column(String, nullable=False, default="pending")
    external_order_id = Column(String, nullable=True)
    message = Column(Text, nullable=True)
    is_shadow = Column(Boolean, default=False, nullable=False)


Base.metadata.create_all(bind=engine)


@dataclass
class StrategyState:
    name: str
    enabled: bool
    live_enabled: bool
    min_edge: float
    max_spread: float
    max_ticket_dollars: float
    cooldown_seconds: int
    max_hours_to_expiry: int
    min_hours_to_expiry: float
    perf_trades: int = 0
    perf_wins: int = 0
    perf_pnl: float = 0.0
    last_signal_at: Optional[str] = None
    last_signal_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MarketSnapshot:
    ticker: str
    title: str
    subtitle: str
    category: str
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    last_price: Optional[float]
    mid: Optional[float]
    spread: Optional[float]
    close_time: Optional[str]
    raw_close_time: Optional[datetime]
    volume_24h: Optional[float]
    liquidity: Optional[float]
    status: str
    spot_symbol: Optional[str] = None
    threshold: Optional[float] = None
    direction: Optional[str] = None
    fair_yes: Optional[float] = None
    edge: Optional[float] = None
    rationale: str = ""

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["raw_close_time"] = self.close_time
        return payload


@dataclass
class OrderIntent:
    intent_id: str
    shadow_group_id: str
    strategy: str
    broker: str
    ticker: str
    title: str
    side: str
    action: str
    contracts: float
    requested_price: float
    fair_value: float
    edge: float
    spot_symbol: Optional[str]
    spot_price: Optional[float]
    rationale: str


class ModeUpdate(BaseModel):
    mode: str = Field(pattern="^(paper|live)$")


class ArmLiveUpdate(BaseModel):
    armed: bool


class StrategyUpdate(BaseModel):
    enabled: bool
    live_enabled: Optional[bool] = None


class PaperTradingToggle(BaseModel):
    enabled: bool

class StrategyLabSettingUpdate(BaseModel):
    strategy_name: str
    variant_name: Optional[str] = None
    parameter_name: str
    value_text: str
    source: str = "manual"

class KalshiClient:
    def __init__(self) -> None:
        self.base_url = os.getenv("KALSHI_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2").rstrip("/")
        self.api_key_id = os.getenv("KALSHI_API_KEY_ID", "").strip()
        self.private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip()
        self.private_key = self._load_private_key(self.private_key_path) if self.private_key_path else None

    @property
    def trading_enabled(self) -> bool:
        return bool(self.api_key_id and self.private_key)

    def _load_private_key(self, path: str):
        try:
            with open(path, "rb") as f:
                return serialization.load_pem_private_key(
                    f.read(), password=None, backend=default_backend()
                )
        except FileNotFoundError:
            return None

    
    def _sign_headers(self, method: str, path: str) -> Dict[str, str]:
        if not self.trading_enabled:
            raise RuntimeError("Kalshi credentials missing.")

        import time
        timestamp = str(int(time.time() * 1000))

        # Kalshi requires full API path in signature
        path_no_query = path.split("?", 1)[0]
        if not path_no_query.startswith("/trade-api/"):
            path_no_query = f"/trade-api/v2{path_no_query}"

        message = f"{timestamp}{method.upper()}{path_no_query}".encode("utf-8")

        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
            "Content-Type": "application/json",
        }


    async def get_json(self, session: aiohttp.ClientSession, path: str, auth: bool = False) -> Dict[str, Any]:
        headers: Dict[str, str] = {}
        if auth:
            headers.update(self._sign_headers("GET", path))
        async with session.get(f"{self.base_url}{path}", headers=headers, timeout=30) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Kalshi GET {path} failed ({resp.status}): {text[:300]}")
            return {} if not text else await resp.json()

    async def post_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        payload: Dict[str, Any],
        auth: bool = True,
    ) -> Dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if auth:
            headers.update(self._sign_headers("POST", path))
        async with session.post(f"{self.base_url}{path}", json=payload, headers=headers, timeout=30) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Kalshi POST {path} failed ({resp.status}): {text[:300]}")
            return {} if not text else await resp.json()


class PaperBroker:
    async def execute(self, intent: OrderIntent, snapshot: MarketSnapshot) -> Dict[str, Any]:
        return {
            "status": "filled",
            "fill_price": round(intent.requested_price, 4),
            "external_order_id": f"paper-{intent.intent_id}",
            "message": "Paper fill from current quote snapshot.",
        }


class LiveKalshiBroker:
    def __init__(self, client: KalshiClient) -> None:
        self.client = client

    async def execute(self, session: aiohttp.ClientSession, intent: OrderIntent) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "ticker": intent.ticker,
            "side": intent.side,
            "action": intent.action,
            "client_order_id": intent.intent_id,
            "count": max(1, int(round(intent.contracts))),
            "time_in_force": "fill_or_kill",
        }
        if intent.side == "yes":
            payload["yes_price_dollars"] = f"{intent.requested_price:.4f}"
        else:
            payload["no_price_dollars"] = f"{intent.requested_price:.4f}"

        response = await self.client.post_json(session, "/portfolio/orders", payload, auth=True)
        order = response.get("order", response)
        status = str(order.get("status", "submitted")).lower()
        fill_price = self._extract_fill_price(order, intent.requested_price)
        return {
            "status": status,
            "fill_price": fill_price,
            "external_order_id": order.get("order_id") or order.get("client_order_id"),
            "message": response.get("message") or "Kalshi order submitted.",
        }

    @staticmethod
    def _extract_fill_price(order: Dict[str, Any], fallback: float) -> Optional[float]:
        for key in ("yes_price_dollars", "no_price_dollars", "taker_fill_cost_dollars", "maker_fill_cost_dollars"):
            value = order.get(key)
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        return fallback


class TradingBot:
    NUMBER_RE = re.compile(r"\$?([0-9]{1,3}(?:,[0-9]{3})+(?:\.\d+)?|[0-9]+(?:\.\d+)?)")

    def __init__(self) -> None:
        self.paper_mode = os.getenv("PAPER_MODE", "true").lower() == "true"
        self.live_armed = os.getenv("ARM_LIVE_TRADING", "false").lower() == "true"
        self.poll_seconds = max(5, int(os.getenv("POLL_SECONDS", "15")))
        self.full_scan_seconds = max(300, int(os.getenv("FULL_MARKET_SCAN_SECONDS", "3600")))
        self.max_markets = max(100, int(os.getenv("MAX_MARKETS", "500")))
        self.watch_refresh_limit = min(10, max(1, int(os.getenv("WATCH_REFRESH_LIMIT", "10"))))
        self.watch_terms = [t.strip().upper() for t in os.getenv(
            "WATCH_TERMS", "BTC,BITCOIN,ETH,ETHEREUM,SOL,SOLANA,CRYPTO"
        ).split(",") if t.strip()]

        self.logs: Deque[str] = deque(maxlen=300)
        self.market_snapshots: Dict[str, MarketSnapshot] = {}
        self.shadow_market_snapshots: Dict[str, MarketSnapshot] = {}
        self.shadow_market_snapshots: Dict[str, MarketSnapshot] = {}
        self.raw_market_count: int = 0
        self.normalized_market_count: int = 0
        self.classified_market_count: int = 0
        self.eligible_market_count: int = 0
        self.watched_market_count: int = 0
        self.market_rejections: Counter = Counter()
        self.market_rejection_examples: Dict[str, List[str]] = {}
        self.raw_market_samples: List[Dict[str, Any]] = []
        self.watched_market_samples: List[Dict[str, Any]] = []
        self.cached_crypto_tickers: List[str] = []
        self.watch_cursor: int = 0
        self.last_full_scan_at: Optional[datetime] = None
        self.last_cache_write_at: Optional[datetime] = None
        self.spot_prices: Dict[str, float] = {"BTC": 0.0, "ETH": 0.0, "SOL": 0.0}
        self.health: Dict[str, Any] = {
            "last_market_refresh": None,
            "last_spot_refresh": None,
            "last_balance_refresh": None,
            "last_coinbase_ws_message": None,
            "coinbase_ws_status": "starting",
            "market_pipeline": {},
            "loop_error": None,
        }
        self.strategy_states: Dict[str, StrategyState] = {
            "crypto_lag": StrategyState(
                name="crypto_lag",
                enabled=True,
                live_enabled=False,
                min_edge=0.03,
                max_spread=0.08,
                max_ticket_dollars=25,
                cooldown_seconds=300,
                max_hours_to_expiry=24,
                min_hours_to_expiry=2,
            ),
            "sentiment": StrategyState(
                name="sentiment",
                enabled=False,
                live_enabled=False,
                min_edge=0.12,
                max_spread=0.08,
                max_ticket_dollars=25,
                cooldown_seconds=1800,
                max_hours_to_expiry=24,
                min_hours_to_expiry=2,
            ),
        }
        self.cooldowns: Dict[str, datetime] = {}
        self.kalshi_balance: float = 0.0
        self.portfolio_value: float = 0.0
        self.paper_starting_balance: float = float(os.getenv("PAPER_STARTING_BALANCE", "1000"))
        self.session: Optional[aiohttp.ClientSession] = None
        self.loop_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._load_crypto_cache()
        self.kalshi_client = KalshiClient()
        self.paper_broker = PaperBroker()
        self.live_broker = LiveKalshiBroker(self.kalshi_client)
        self.shadow_engine = ShadowEngine(self)
        self.outcome_resolver = OutcomeResolver(self)
        self.summary_engine = SummaryAndSuggestionEngine(self)
        self.paper_trading_enabled = True
        self.paper_last_reset_at = None
        self.log("Bot initialized. Live is disarmed.")


    def log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.logs.appendleft(f"[{stamp}] {message}")


    def set_paper_trading_enabled(self, enabled: bool) -> None:
        self.paper_trading_enabled = bool(enabled)
        self.log(f"Paper trading {'enabled' if self.paper_trading_enabled else 'paused'}.")

    def reset_paper_book(self) -> None:
        # hard reset the paper broker
        self.paper_broker = PaperBroker()
        self.paper_last_reset_at = datetime.now(timezone.utc).isoformat()

        # optional: clear paper trades from recent trade list if present
        if hasattr(self, "recent_trades") and isinstance(self.recent_trades, list):
            self.recent_trades = [
                t for t in self.recent_trades
                if str(t.get("mode", "")).lower() != "paper"
            ]

        self.log(f"Paper book reset at {self.paper_last_reset_at}.")


    def get_current_mode(self) -> str:
        # Try the common places mode may live.
        if getattr(self, "mode", None):
            return self.mode
        if hasattr(self, "settings") and isinstance(getattr(self, "settings"), dict):
            m = self.settings.get("mode")
            if m:
                return m
        if hasattr(self, "state") and isinstance(getattr(self, "state"), dict):
            m = self.state.get("mode")
            if m:
                return m
        # safest fallback based on current app behavior
        return "paper"

    def paper_controls_status(self) -> Dict[str, Any]:
        mode = self.get_current_mode()
        enabled = bool(getattr(self, "paper_trading_enabled", True))
        return {
            "enabled": enabled,
            "last_reset_at": getattr(self, "paper_last_reset_at", None),
            "mode": mode,
            "can_trade_now": bool(mode == "paper" and enabled),
        }


    def get_effective_variant_settings(self, variant_name: str, base: dict) -> dict:
        from app.strategy_lab.storage import get_conn

        cfg = dict(base)
        conn = get_conn()
        try:
            rows = conn.execute(
                """
                SELECT parameter_name, value_text
                FROM operator_settings
                WHERE strategy_name = 'crypto_lag' AND variant_name = ?
                ORDER BY ts DESC, id DESC
                """,
                (variant_name,),
            ).fetchall()

            seen = set()
            for row in rows:
                param = row["parameter_name"]
                if param in seen:
                    continue
                seen.add(param)

                raw = row["value_text"]
                if param in {"min_edge", "max_spread", "min_hours", "max_hours", "size_multiplier", "exposure_multiplier"}:
                    try:
                        cfg[param] = float(raw)
                    except Exception:
                        pass
                elif param in {"cooldown_seconds"}:
                    try:
                        cfg[param] = int(float(raw))
                    except Exception:
                        pass
                else:
                    cfg[param] = raw
        finally:
            conn.close()

        return cfg

    async def start(self) -> None:
        self._migrate_db()
        init_strategy_lab_db()
        if self.session is None:
            self.session = aiohttp.ClientSession()

        if self._should_rebuild_cache_on_startup():
            self.log("Startup cache rebuild triggered (older than 6 hours or missing).")
            await self._run_btc_cache_rebuild(reason="startup")
        if self.loop_task is None or self.loop_task.done():
            self.loop_task = asyncio.create_task(self._main_loop())
        if not hasattr(self, "coinbase_task") or self.coinbase_task is None or self.coinbase_task.done():
            self.coinbase_task = asyncio.create_task(self._coinbase_ws_loop())
        self.log("Background loop started.")

    async def stop(self) -> None:
        if self.loop_task:
            self.loop_task.cancel()
            try:
                await self.loop_task
            except asyncio.CancelledError:
                pass
            self.loop_task = None
        if hasattr(self, "coinbase_task") and self.coinbase_task:
            self.coinbase_task.cancel()
            try:
                await self.coinbase_task
            except asyncio.CancelledError:
                pass
            self.coinbase_task = None
        if self.session:
            await self.session.close()
            self.session = None
        self.log("Background loop stopped.")

    def _migrate_db(self) -> None:
        import sqlite3

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
        exists = cur.fetchone()
        if exists:
            cur.execute("PRAGMA table_info(trades)")
            cols = {row[1] for row in cur.fetchall()}
            required = {"created_at", "updated_at", "shadow_group_id", "intent_id", "strategy"}
            if not required.issubset(cols):
                cur.execute("DROP TABLE trades")
                conn.commit()
        # strategy_lab / shadow trade compatibility columns
        conn = sqlite3.connect("data/strategy_lab.db")
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(shadow_trades)")
        cols = {row[1] for row in cur.fetchall()}
        if "close_reason" not in cols:
            cur.execute("ALTER TABLE shadow_trades ADD COLUMN close_reason TEXT")
        if "exit_price" not in cols:
            cur.execute("ALTER TABLE shadow_trades ADD COLUMN exit_price REAL")
        if "closed_at" not in cols:
            cur.execute("ALTER TABLE shadow_trades ADD COLUMN closed_at TEXT")
        if "realized_pnl" not in cols:
            cur.execute("ALTER TABLE shadow_trades ADD COLUMN realized_pnl REAL")
        if "outcome" not in cols:
            cur.execute("ALTER TABLE shadow_trades ADD COLUMN outcome TEXT")
        conn.commit()
        conn.close()

        conn.close()
        Base.metadata.create_all(bind=engine)

    async def _main_loop(self) -> None:
        assert self.session is not None
        while True:
            try:
                self.log("Main loop tick: starting refresh cycle.")
                await self._ensure_spot_prices_ready()

                if self._is_rebuild_slot_due():
                    await self._run_btc_cache_rebuild(reason="scheduled")

                await self.refresh_markets()
                self.shadow_engine.run_shadow_variants()
                self.shadow_engine.mark_positions()
                self.shadow_engine.run_exit_pass()
                self.outcome_resolver.mark_positions()
                self.outcome_resolver.resolve_finalized_positions()
                self.summary_engine.rebuild_summaries()
                self.summary_engine.rebuild_suggestions()
                self.log("Main loop tick: market refresh complete.")
                await self.refresh_balance()
                self.log("Main loop tick: balance refresh complete.")
                await self.run_strategies()
                self.log("Main loop tick: strategy run complete.")
                self.health["loop_error"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health["loop_error"] = str(exc)
                self.log(f"Loop error: {exc}")
            await asyncio.sleep(self.poll_seconds)

    async def refresh_spots(self) -> None:
        return

    async def _ensure_spot_prices_ready(self) -> None:
        if self.session is None:
            return
        if self.spot_prices.get("BTC"):
            return
        try:
            async with self.session.get("https://api.exchange.coinbase.com/products/BTC-USD/ticker", timeout=10) as resp:
                data = await resp.json()
                px = float(data["price"])
                if px > 0:
                    self.spot_prices["BTC"] = px
                    now = datetime.now(timezone.utc).isoformat()
                    self.health["last_spot_refresh"] = now
        except Exception as exc:
            self.log(f"spot_fallback_error: {type(exc).__name__}: {exc!r}")

    async def _coinbase_ws_loop(self) -> None:
        assert self.session is not None
        url = "wss://ws-feed.exchange.coinbase.com"
        products = ["BTC-USD", "ETH-USD", "SOL-USD"]
        product_to_symbol = {
            "BTC-USD": "BTC",
            "ETH-USD": "ETH",
            "SOL-USD": "SOL",
        }

        while True:
            try:
                self.health["coinbase_ws_status"] = "connecting"
                async with self.session.ws_connect(url, heartbeat=20, autoping=True) as ws:
                    self.health["coinbase_ws_status"] = "connected"
                    await ws.send_json({
                        "type": "subscribe",
                        "product_ids": products,
                        "channels": ["ticker"]
                    })
                    self.log("Coinbase WS connected for BTC/ETH/SOL.")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = msg.json()
                            msg_type = data.get("type")
                            if msg_type == "ticker":
                                product_id = data.get("product_id")
                                price = data.get("price")
                                symbol = product_to_symbol.get(product_id)
                                if symbol and price not in (None, ""):
                                    try:
                                        px = float(price)
                                        if px > 0:
                                            self.spot_prices[symbol] = px
                                            now = datetime.now(timezone.utc).isoformat()
                                            self.health["last_spot_refresh"] = now
                                            self.health["last_coinbase_ws_message"] = now
                                    except (TypeError, ValueError):
                                        pass
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health["coinbase_ws_status"] = f"error: {exc}"
                self.log(f"Coinbase WS error: {exc}")

            self.health["coinbase_ws_status"] = "reconnecting"
            await asyncio.sleep(3)


    def _load_cache_meta(self) -> Dict[str, Any]:
        try:
            if BTC_CACHE_META_PATH.exists():
                return json.loads(BTC_CACHE_META_PATH.read_text())
        except Exception:
            pass
        return {}

    def _write_cache_meta(self, payload: Dict[str, Any]) -> None:
        try:
            BTC_CACHE_META_PATH.parent.mkdir(parents=True, exist_ok=True)
            BTC_CACHE_META_PATH.write_text(json.dumps(payload, indent=2))
        except Exception as exc:
            self.log(f"cache_meta_write_error: {type(exc).__name__}: {exc!r}")

    def _last_cache_rebuild_dt(self) -> Optional[datetime]:
        meta = self._load_cache_meta()
        raw = meta.get("last_rebuild_at")
        if not raw:
            return None
        try:
            if isinstance(raw, str) and raw.endswith("Z"):
                raw = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _should_rebuild_cache_on_startup(self) -> bool:
        last_dt = self._last_cache_rebuild_dt()
        if last_dt is None:
            return True
        age_hours = (datetime.now(timezone.utc) - last_dt).total_seconds() / 3600.0
        return age_hours >= 6.0

    def _is_rebuild_slot_due(self) -> bool:
        now_et = datetime.now(ZoneInfo("America/New_York"))
        slot_hours = {0, 6, 12, 18}
        if now_et.hour not in slot_hours:
            return False

        meta = self._load_cache_meta()
        last_slot = meta.get("last_scheduled_slot")
        current_slot = now_et.strftime("%Y-%m-%d-%H")
        return last_slot != current_slot

    async def _run_btc_cache_rebuild(self, reason: str = "manual") -> Dict[str, Any]:
        if self.session is None:
            return {"ok": False, "error": "session_not_ready"}

        try:
            spot = None
            if self.spot_prices.get("BTC"):
                spot = float(self.spot_prices["BTC"])
            else:
                async with self.session.get("https://api.exchange.coinbase.com/products/BTC-USD/ticker", timeout=10) as resp:
                    data = await resp.json()
                    spot = float(data["price"])

            path = "/markets?series_ticker=KXBTC&status=open&mve_filter=exclude&limit=200"
            data = await self.kalshi_client.get_json(self.session, path)
            markets = data.get("markets", []) or []

            kept = []
            sample = []
            now = datetime.now(timezone.utc)

            for m in markets:
                status = str(m.get("status") or "").lower()
                if status not in {"open", "active", "initialized"}:
                    continue

                close_raw = (
                    m.get("close_time")
                    or m.get("expiration_time")
                    or m.get("expiration_date")
                    or m.get("end_date")
                )
                if not close_raw:
                    continue

                raw = str(close_raw)
                if raw.endswith("Z"):
                    raw = raw.replace("Z", "+00:00")
                close_dt = datetime.fromisoformat(raw)
                if close_dt.tzinfo is None:
                    close_dt = close_dt.replace(tzinfo=timezone.utc)
                close_dt = close_dt.astimezone(timezone.utc)

                hours = (close_dt - now).total_seconds() / 3600.0
                if hours < 1 or hours > 24:
                    continue

                strike_type = str(m.get("strike_type") or "").lower()
                floor_strike = m.get("floor_strike")
                cap_strike = m.get("cap_strike")
                floor_strike = float(floor_strike) if floor_strike not in (None, "") else None
                cap_strike = float(cap_strike) if cap_strike not in (None, "") else None

                dist = None
                if strike_type in {"between", "range"} and floor_strike is not None and cap_strike is not None:
                    if floor_strike <= spot <= cap_strike:
                        dist = 0.0
                    elif spot < floor_strike:
                        dist = abs(floor_strike - spot) / spot
                    else:
                        dist = abs(spot - cap_strike) / spot
                elif strike_type == "less" and cap_strike is not None:
                    dist = 0.0 if spot <= cap_strike else abs(spot - cap_strike) / spot
                elif strike_type == "greater" and floor_strike is not None:
                    dist = 0.0 if spot >= floor_strike else abs(floor_strike - spot) / spot

                if dist is None or dist > 0.12:
                    continue

                ticker = str(m.get("ticker") or "").upper()
                if not ticker:
                    continue

                kept.append(ticker)
                if len(sample) < 25:
                    sample.append({
                        "ticker": ticker,
                        "subtitle": m.get("subtitle"),
                        "status": status,
                        "strike_type": strike_type,
                        "floor_strike": floor_strike,
                        "cap_strike": cap_strike,
                        "hours_to_expiry": round(hours, 3),
                        "distance_from_spot_pct": round(dist, 6),
                    })

            kept = sorted(set(kept))

            CRYPTO_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            CRYPTO_CACHE_PATH.write_text(json.dumps({
                "written_at": datetime.now(timezone.utc).isoformat(),
                "tickers": kept,
            }, indent=2))

            now_et = datetime.now(ZoneInfo("America/New_York"))
            slot_hours = {0, 6, 12, 18}
            last_scheduled_slot = None
            if now_et.hour in slot_hours:
                last_scheduled_slot = now_et.strftime("%Y-%m-%d-%H")

            self._write_cache_meta({
                "last_rebuild_at": datetime.now(timezone.utc).isoformat(),
                "last_rebuild_reason": reason,
                "last_scheduled_slot": last_scheduled_slot,
                "count": len(kept),
                "sample": sample,
            })

            self.cached_crypto_tickers = kept
            self.health["market_pipeline"] = {
                **(self.health.get("market_pipeline") or {}),
                "last_cache_write_at": datetime.now(timezone.utc).isoformat(),
                "cached_crypto_ticker_count": len(kept),
                "last_rebuild_reason": reason,
            }

            self.log(f"BTC cache rebuilt ({reason}): {len(kept)} tickers.")
            return {"ok": True, "count": len(kept), "reason": reason, "sample": sample}
        except Exception as exc:
            self.log(f"BTC cache rebuild failed ({reason}): {exc}")
            return {"ok": False, "error": str(exc), "reason": reason}

    async def refresh_markets(self) -> None:
        assert self.session is not None
        self.log("refresh_markets: entered")

        # Disable automatic full discovery scan (manual only)
        need_full_scan = not bool(getattr(self, "cached_crypto_tickers", []))
        self.log(f"refresh_markets: need_full_scan={need_full_scan} cached={len(self.cached_crypto_tickers)}")

        if need_full_scan:
            self.log("refresh_markets: starting full discovery scan")
            raw_markets = await asyncio.wait_for(self._fetch_all_open_markets(), timeout=180)
            self.log(f"refresh_markets: full discovery returned {len(raw_markets)} markets")
            self.last_full_scan_at = datetime.now(timezone.utc)
            self._refresh_crypto_cache_from_raw_markets(raw_markets)
            self.log(f"refresh_markets: cache refreshed with {len(self.cached_crypto_tickers)} tickers")

        self.log("refresh_markets: starting cached watch fetch")
        raw_markets = await asyncio.wait_for(self._fetch_cached_crypto_markets(), timeout=60)
        self.log(f"refresh_markets: cached watch fetch returned {len(raw_markets)} markets")

        self.raw_market_count = len(raw_markets)
        self.raw_market_samples = [
            {
                "ticker": str(m.get("ticker", "")),
                "title": str(m.get("title") or m.get("question") or ""),
                "subtitle": str(m.get("subtitle") or ""),
                "category": str(m.get("category") or m.get("series_ticker") or ""),
                "status": str(m.get("status") or ""),
            }
            for m in raw_markets[:20]
        ]

        snapshots: Dict[str, MarketSnapshot] = {}
        shadow_snapshots: Dict[str, MarketSnapshot] = {}
        rejections: Counter = Counter()
        examples: Dict[str, List[str]] = {}
        normalized_count = 0
        classified_count = 0
        eligible_count = 0
        watched_market_samples: List[Dict[str, Any]] = []

        for raw in raw_markets:
            normalized = self._normalize_market(raw)
            if not normalized:
                rejections["normalize_failed"] += 1
                self._add_rejection_example(examples, "normalize_failed", raw.get("ticker") or raw.get("title") or "unknown")
                continue
            normalized_count += 1

            classified = self._classify_market(normalized)

            if classified.get("asset"):
                classified_count += 1
                if len(watched_market_samples) < 20:
                    watched_market_samples.append({
                        "ticker": classified.get("ticker"),
                        "title": classified.get("title"),
                        "asset": classified.get("asset"),
                        "market_family": classified.get("market_family"),
                        "status": classified.get("status"),
                        "threshold": classified.get("threshold"),
                        "direction": classified.get("direction"),
                        "distance_from_spot_pct": self._threshold_distance_pct(
                            classified.get("asset"),
                            classified.get("threshold"),
                            classified.get("floor_strike"),
                            classified.get("cap_strike"),
                        ),
                    })

            model_prob = None
            market_prob = None
            edge = None

            if classified.get("asset") == "BTC" and classified.get("close_dt") is not None:
                spot = float(self.spot_prices.get("BTC", 0.0) or 0.0)
                hours_to_expiry = (classified["close_dt"] - datetime.now(timezone.utc)).total_seconds() / 3600.0

                if classified.get("market_family") == "range_bucket":
                    model_prob = self._bucket_probability(
                        spot,
                        classified.get("floor_strike"),
                        classified.get("cap_strike"),
                        hours_to_expiry,
                    )
                elif classified.get("market_family") == "price_threshold":
                    if classified.get("direction") == "above":
                        model_prob = self._bucket_probability(
                            spot,
                            classified.get("threshold"),
                            None,
                            hours_to_expiry,
                        )
                    elif classified.get("direction") == "below":
                        model_prob = self._bucket_probability(
                            spot,
                            None,
                            classified.get("threshold"),
                            hours_to_expiry,
                        )

                market_prob = classified.get("yes_ask")
                if model_prob is not None and market_prob is not None:
                    edge = model_prob - market_prob

            if self._is_shadow_candidate(classified):
                shadow_snapshot = self._classified_to_snapshot(classified)
                if shadow_snapshot is not None:
                    setattr(shadow_snapshot, "market_family", classified.get("market_family"))
                    setattr(shadow_snapshot, "floor_strike", classified.get("floor_strike"))
                    setattr(shadow_snapshot, "cap_strike", classified.get("cap_strike"))
                    setattr(shadow_snapshot, "distance_from_spot_pct", classified.get("distance_from_spot_pct"))
                    setattr(shadow_snapshot, "open_interest", classified.get("open_interest"))
                    shadow_snapshots[shadow_snapshot.ticker] = shadow_snapshot

            eligible, reason = self._evaluate_market_eligibility(classified)
            self._log_candidate_event(classified, eligible, reason, model_prob, market_prob, edge)

            if not eligible:
                rejections[reason] += 1
                self._add_rejection_example(examples, reason, classified.get("ticker") or classified.get("title") or "unknown")
                continue

            snapshot = self._classified_to_snapshot(classified)
            if snapshot:
                snapshots[snapshot.ticker] = snapshot
                eligible_count += 1
            else:
                rejections["snapshot_failed"] += 1
                self._add_rejection_example(examples, "snapshot_failed", classified.get("ticker") or classified.get("title") or "unknown")

        self.market_snapshots = snapshots
        self.shadow_market_snapshots = shadow_snapshots
        self.normalized_market_count = normalized_count
        self.classified_market_count = classified_count
        self.eligible_market_count = eligible_count
        self.watched_market_count = classified_count
        self.market_rejections = rejections
        self.market_rejection_examples = examples
        self.watched_market_samples = watched_market_samples
        self.health["last_market_refresh"] = datetime.now(timezone.utc).isoformat()
        self.health["market_pipeline"] = {
            "raw_open_markets": self.raw_market_count,
            "normalized_markets": self.normalized_market_count,
            "classified_crypto_markets": self.classified_market_count,
            "eligible_markets": self.eligible_market_count,
            "shadow_candidate_markets": len(self.shadow_market_snapshots),
            "cached_crypto_ticker_count": len(self.cached_crypto_tickers),
            "watch_cursor": self.watch_cursor,
            "watch_refresh_limit": self.watch_refresh_limit,
            "rejections": dict(self.market_rejections),
            "rejection_examples": self.market_rejection_examples,
            "raw_market_samples": self.raw_market_samples,
            "watched_market_samples": self.watched_market_samples,
            "last_full_scan_at": self.last_full_scan_at.isoformat() if self.last_full_scan_at else None,
            "last_cache_write_at": self.last_cache_write_at.isoformat() if self.last_cache_write_at else None,
            "full_scan_seconds": self.full_scan_seconds,
        }
        self.log(
            f"Market pipeline: cached={len(self.cached_crypto_tickers)} raw={self.raw_market_count} "
            f"normalized={self.normalized_market_count} classified={self.classified_market_count} "
            f"eligible={self.eligible_market_count}"
        )


    def _is_shadow_candidate(self, classified: Dict[str, Any]) -> bool:
        if classified.get("asset") != "BTC":
            return False
        if classified.get("market_family") != "range_bucket":
            return False

        status = str(classified.get("status") or "").lower()
        if status not in {"active", "open", "initialized"}:
            return False

        if classified.get("close_dt") is None:
            return False

        if classified.get("yes_bid") is None or classified.get("yes_ask") is None:
            return False
        if classified.get("no_bid") is None or classified.get("no_ask") is None:
            return False
        if classified.get("mid") is None or classified.get("spread") is None:
            return False

        return True

    async def _fetch_all_open_markets(self) -> List[Dict[str, Any]]:
        assert self.session is not None
        markets: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        pages = 0

        while True:
            path = f"/markets?status=open&limit={self.max_markets}"
            if cursor:
                path += f"&cursor={cursor}"

            data = await self.kalshi_client.get_json(self.session, path)
            batch = data.get("markets", []) or []
            markets.extend(batch)
            pages += 1

            cursor = data.get("cursor") or data.get("next_cursor")
            if not cursor or not batch:
                break

            await asyncio.sleep(0.35)

        self.log(f"Full discovery scan fetched {len(markets)} markets across {pages} pages.")
        return markets

    async def _fetch_cached_crypto_markets(self) -> List[Dict[str, Any]]:
        assert self.session is not None
        if not self.cached_crypto_tickers:
            self.log("_fetch_cached_crypto_markets: no cached tickers")
            return []

        import re

        def _bucket_center(ticker: str):
            m = re.search(r"-B(\d+)$", str(ticker or ""))
            return float(m.group(1)) if m else None

        total = len(self.cached_crypto_tickers)
        window = min(self.watch_refresh_limit, total)
        spot = float((getattr(self, "spot_prices", {}) or {}).get("BTC") or 0.0)

        ranked = []
        for t in self.cached_crypto_tickers:
            center = _bucket_center(t)
            if center is None:
                ranked.append((999999999.0, t))
            else:
                ranked.append((abs(center - spot), t))

        ranked.sort(key=lambda x: (x[0], x[1]))
        selected = [t for _, t in ranked[:window]]

        markets: List[Dict[str, Any]] = []

        self.log(
            f"_fetch_cached_crypto_markets: selected {len(selected)} nearest-to-spot tickers "
            f"(spot={spot}, total={total})"
        )

        for i in range(0, len(selected), 10):
            chunk = selected[i:i+10]
            self.log(f"_fetch_cached_crypto_markets: requesting chunk {i//10 + 1} size={len(chunk)} first={chunk[0]}")
            path = "/markets?tickers=" + ",".join(chunk)
            data = await asyncio.wait_for(self.kalshi_client.get_json(self.session, path), timeout=20)
            returned = len(data.get("markets", []) or [])
            self.log(f"_fetch_cached_crypto_markets: chunk {i//10 + 1} returned {returned} markets")
            markets.extend(data.get("markets", []) or [])
            await asyncio.sleep(0.05)

        self.watch_cursor = 0
        self.log(
            f"Refreshed spot-centered crypto watch window: {len(selected)} tickers "
            f"(spot={spot}, total={total})"
        )
        return markets

    async def refresh_balance(self) -> None:
        if not self.kalshi_client.trading_enabled or self.session is None:
            self.health["last_balance_error"] = "trading_not_enabled_or_no_session"
            return
        try:
            data = await self.kalshi_client.get_json(self.session, "/portfolio/balance", auth=True)

            balance = data.get("balance", 0)
            portfolio_value = data.get("portfolio_value", 0)

            self.kalshi_balance = round(float(balance) / 100.0, 2)
            self.portfolio_value = round(float(portfolio_value) / 100.0, 2)

            self.health["last_balance_refresh"] = datetime.now(timezone.utc).isoformat()
            self.health["last_balance_error"] = None
        except Exception as exc:
            self.health["last_balance_error"] = str(exc)
            self.log(f"Balance refresh failed: {type(exc).__name__}: {exc!r}")

    def _load_crypto_cache(self) -> None:
        if not CRYPTO_CACHE_PATH.exists():
            self.cached_crypto_tickers = []
            return
        try:
            payload = json.loads(CRYPTO_CACHE_PATH.read_text())
            tickers = payload.get("tickers", [])
            self.cached_crypto_tickers = [str(x).upper() for x in tickers if str(x).strip()]
            ts = payload.get("written_at")
            if ts:
                try:
                    self.last_cache_write_at = datetime.fromisoformat(ts)
                except Exception:
                    self.last_cache_write_at = None
            self.log(f"Loaded {len(self.cached_crypto_tickers)} cached crypto tickers.")
        except Exception as exc:
            self.cached_crypto_tickers = []
            self.log(f"Failed to load crypto cache: {exc}")

    def _write_crypto_cache(self) -> None:
        payload = {
            "written_at": datetime.now(timezone.utc).isoformat(),
            "tickers": self.cached_crypto_tickers,
        }
        CRYPTO_CACHE_PATH.write_text(json.dumps(payload, indent=2))
        self.last_cache_write_at = datetime.now(timezone.utc)

    def _is_discoverable_crypto_market(self, market: Dict[str, Any]) -> bool:
        ticker = str(market.get("ticker", "")).upper()
        title = str(market.get("title") or market.get("question") or "").upper()
        subtitle = str(market.get("subtitle") or "").upper()
        text = f"{ticker} {title} {subtitle}"

        valid_prefixes = ("KXBTC",)
        if not ticker.startswith(valid_prefixes):
            return False

        if "PRICE" not in text:
            return False

        if not any(word in text for word in ["ABOVE", "BELOW", "AT", "-T"]):
            return False

        return True

    def _refresh_crypto_cache_from_raw_markets(self, raw_markets: List[Dict[str, Any]]) -> None:
        tickers = []
        for m in raw_markets:
            if self._is_discoverable_crypto_market(m):
                t = str(m.get("ticker", "")).upper().strip()
                if t:
                    tickers.append(t)

        self.cached_crypto_tickers = sorted(set(tickers))
        self._write_crypto_cache()
        self.log(f"Discovered and cached {len(self.cached_crypto_tickers)} crypto tickers.")

    def _normalize_market(self, market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ticker = str(market.get("ticker", "")).upper().strip()
        if not ticker:
            return None

        title = str(market.get("title") or market.get("question") or ticker)
        subtitle = str(market.get("subtitle") or market.get("rules_primary") or "")
        category = str(market.get("category") or market.get("series_ticker") or "")

        yes_bid = self._price_to_float(market.get("yes_bid_dollars"), market.get("yes_bid"))
        yes_ask = self._price_to_float(market.get("yes_ask_dollars"), market.get("yes_ask"))
        no_bid = self._price_to_float(market.get("no_bid_dollars"), market.get("no_bid"))
        no_ask = self._price_to_float(market.get("no_ask_dollars"), market.get("no_ask"))
        last_price = self._price_to_float(market.get("last_price_dollars"), market.get("last_price"))

        mid = None
        if yes_bid is not None and yes_ask is not None:
            mid = round((yes_bid + yes_ask) / 2.0, 4)
        elif last_price is not None:
            mid = last_price

        spread = None
        if yes_bid is not None and yes_ask is not None:
            spread = round(yes_ask - yes_bid, 4)

        close_time = (
            market.get("close_time")
            or market.get("expiration_time")
            or market.get("expiration_date")
            or market.get("end_date")
        )
        close_dt = None
        if close_time:
            raw = str(close_time)
            if raw.endswith("Z"):
                raw = raw.replace("Z", "+00:00")
            close_dt = datetime.fromisoformat(raw)
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=timezone.utc)
            close_dt = close_dt.astimezone(timezone.utc)

        floor_strike = market.get("floor_strike")
        cap_strike = market.get("cap_strike")
        strike_type = str(market.get("strike_type") or "").lower().strip()

        floor_strike = float(floor_strike) if floor_strike not in (None, "") else None
        cap_strike = float(cap_strike) if cap_strike not in (None, "") else None

        return {
            "ticker": ticker,
            "title": title,
            "subtitle": subtitle,
            "category": category,
            "status": str(market.get("status") or "").lower(),
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "last_price": last_price,
            "mid": mid,
            "spread": spread,
            "close_dt": close_dt,
            "floor_strike": floor_strike,
            "cap_strike": cap_strike,
            "strike_type": strike_type,
            "volume_24h": float(market.get("volume_24h") or 0.0),
            "open_interest": float(market.get("open_interest") or 0.0),
            "raw": market,
        }

    def _classify_market(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        ticker = str(normalized.get("ticker", "")).upper()
        title = str(normalized.get("title", "")).upper()
        subtitle = str(normalized.get("subtitle", "")).upper()
        text = f"{ticker} {title} {subtitle}"

        asset = None
        if ticker.startswith("KXBTC"):
            asset = "BTC"

        floor_strike = normalized.get("floor_strike")
        cap_strike = normalized.get("cap_strike")
        strike_type = str(normalized.get("strike_type", "")).lower()

        threshold, direction = self._parse_threshold(f"{ticker} {title} {subtitle}")

        market_family = "unsupported"
        parse_confidence = "low"
        blacklisted = False

        if asset:
            if strike_type in {"between", "range"} and floor_strike is not None and cap_strike is not None:
                market_family = "range_bucket"
                parse_confidence = "high"
            elif strike_type == "less" and cap_strike is not None:
                market_family = "price_threshold"
                threshold = cap_strike
                direction = "below"
                parse_confidence = "high"
            elif strike_type == "greater" and floor_strike is not None:
                market_family = "price_threshold"
                threshold = floor_strike
                direction = "above"
                parse_confidence = "high"
            elif "PRICE" in text and threshold is not None and direction is not None:
                market_family = "price_threshold"
                parse_confidence = "high"

        classified = dict(normalized)
        classified.update({
            "asset": asset,
            "threshold": threshold,
            "direction": direction,
            "market_family": market_family,
            "parse_confidence": parse_confidence,
            "blacklisted": blacklisted,
        })
        return classified

    def _log_candidate_event(self, classified: Dict[str, Any], eligible: bool, reason: str, model_prob=None, market_prob=None, edge=None) -> None:
        try:
            if classified.get("asset") != "BTC":
                return

            close_dt = classified.get("close_dt")
            hours_to_expiry = None
            if close_dt is not None:
                hours_to_expiry = (close_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0

            spot = self.spot_prices.get(classified.get("asset"))
            dist = self._threshold_distance_pct(
                classified.get("asset"),
                classified.get("threshold"),
                classified.get("floor_strike"),
                classified.get("cap_strike"),
            )

            rec = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "ticker": classified.get("ticker"),
                "asset": classified.get("asset"),
                "status": classified.get("status"),
                "market_family": classified.get("market_family"),
                "threshold": classified.get("threshold"),
                "direction": classified.get("direction"),
                "floor_strike": classified.get("floor_strike"),
                "cap_strike": classified.get("cap_strike"),
                "spot": spot,
                "distance_from_spot_pct": dist,
                "hours_to_expiry": hours_to_expiry,
                "yes_bid": classified.get("yes_bid"),
                "yes_ask": classified.get("yes_ask"),
                "no_bid": classified.get("no_bid"),
                "no_ask": classified.get("no_ask"),
                "spread": classified.get("spread"),
                "eligible": eligible,
                "reason": reason,
                "model_prob": model_prob,
                "market_prob": market_prob,
                "edge": edge,
            }

            with CANDIDATE_LOG_PATH.open("a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception as exc:
            self.log(f"candidate_log_error: {type(exc).__name__}: {exc!r}")

    def _evaluate_market_eligibility(self, classified: Dict[str, Any]) -> tuple[bool, str]:
        status = str(classified.get("status", "")).lower()
        if status and status not in {"open", "active", "initialized"}:
            return False, f"status_{status}"

        if classified.get("blacklisted"):
            return False, "blacklisted_market"

        if classified.get("asset") not in {"BTC"}:
            return False, "unsupported_spot_asset"

        if classified.get("market_family") not in {"range_bucket"}:
            return False, "unsupported_market_structure"

        if classified.get("yes_ask") is None or classified.get("yes_bid") is None:
            return False, "missing_yes_quotes"
        if classified.get("no_ask") is None or classified.get("no_bid") is None:
            return False, "missing_no_quotes"
        if classified.get("mid") is None:
            return False, "missing_mid"
        if classified.get("spread") is None:
            return False, "missing_spread"
        if classified.get("close_dt") is None:
            return False, "missing_expiry"

        spread = float(classified.get("spread") or 0.0)
        if spread > 0.10:
            return False, "spread_too_wide"

        if classified.get("yes_ask") in (0.0, 1.0) and classified.get("no_ask") in (0.0, 1.0):
            return False, "degenerate_quotes"

        hours_to_expiry = (classified["close_dt"] - datetime.now(timezone.utc)).total_seconds() / 3600.0
        if hours_to_expiry <= 0:
            return False, "expired"
        if hours_to_expiry < 2:
            return False, "too_close_to_expiry"
        if hours_to_expiry > 12:
            return False, "too_far_to_expiry"

        dist = self._threshold_distance_pct(
            classified.get("asset"),
            classified.get("threshold"),
            classified.get("floor_strike"),
            classified.get("cap_strike"),
        )
        if dist is None:
            return False, "spot_not_ready"
        if dist > 0.05:
            return False, "too_far_from_spot"

        return True, "eligible"

    def _norm_cdf(self, x: float) -> float:
        import math
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def _bucket_probability(self, spot: float, low: Optional[float], high: Optional[float], hours_to_expiry: float) -> Optional[float]:
        import math

        if spot <= 0 or hours_to_expiry <= 0:
            return None

        annual_vol = self._dynamic_annual_vol("BTC", fallback=0.60)
        t = hours_to_expiry / (24.0 * 365.0)
        sigma = annual_vol * math.sqrt(t)

        if sigma <= 0:
            return None

        def z(level: float) -> float:
            return (math.log(level / spot)) / sigma

        if low is None and high is not None:
            return self._norm_cdf(z(high))

        if high is None and low is not None:
            return 1.0 - self._norm_cdf(z(low))

        if low is not None and high is not None:
            return max(0.0, self._norm_cdf(z(high)) - self._norm_cdf(z(low)))

        return None

    def _threshold_distance_pct(self, asset: Optional[str], threshold: Optional[float], floor_strike: Optional[float] = None, cap_strike: Optional[float] = None) -> Optional[float]:
        if not asset:
            return None

        raw_spot = self.spot_prices.get(asset)
        if raw_spot is None:
            return None

        spot = float(raw_spot or 0.0)
        if spot <= 0:
            return None

        if floor_strike is not None or cap_strike is not None:
            low = float(floor_strike) if floor_strike is not None else None
            high = float(cap_strike) if cap_strike is not None else None

            if low is None and high is not None:
                if spot <= high:
                    return 0.0
                return abs(spot - high) / spot

            if high is None and low is not None:
                if spot >= low:
                    return 0.0
                return abs(low - spot) / spot

            if low is not None and high is not None:
                if low <= spot <= high:
                    return 0.0
                if spot < low:
                    return abs(low - spot) / spot
                return abs(spot - high) / spot

            return None

        if threshold is None:
            return None

        return abs(float(threshold) - spot) / spot

    def _classified_to_snapshot(self, classified: Dict[str, Any]) -> Optional[MarketSnapshot]:
        snapshot = MarketSnapshot(
            ticker=classified["ticker"],
            title=classified["title"],
            subtitle=classified["subtitle"],
            category=classified["category"],
            yes_bid=classified["yes_bid"],
            yes_ask=classified["yes_ask"],
            no_bid=classified["no_bid"],
            no_ask=classified["no_ask"],
            last_price=classified["last_price"],
            mid=classified["mid"],
            spread=classified["spread"],
            close_time=classified["close_dt"].isoformat() if classified.get("close_dt") else None,
            raw_close_time=classified["close_dt"],
            volume_24h=float(classified.get("volume_24h") or 0.0),
            liquidity=float(classified.get("liquidity") or 0.0),
            status=classified["status"],
            spot_symbol=classified["asset"],
            threshold=classified["threshold"],
            direction=classified["direction"],
        )
        snapshot.market_family = classified.get("market_family")
        snapshot.floor_strike = classified.get("floor_strike")
        snapshot.cap_strike = classified.get("cap_strike")
        snapshot.distance_from_spot_pct = classified.get("distance_from_spot_pct")
        snapshot.open_interest = classified.get("open_interest")
        return snapshot


    @staticmethod
    def _add_rejection_example(examples: Dict[str, List[str]], reason: str, label: str) -> None:
        bucket = examples.setdefault(reason, [])
        if len(bucket) < 5 and label not in bucket:
            bucket.append(label)

    async def run_strategies(self) -> None:
        for snapshot in list(self.market_snapshots.values()):
            self._enrich_with_fair_value(snapshot)
            for strategy in self.strategy_states.values():
                if not strategy.enabled:
                    continue
                intent = self._generate_intent(strategy, snapshot)
                if intent is None:
                    continue
                await self._submit_intent(intent, snapshot)

    def _enrich_with_fair_value(self, snapshot: MarketSnapshot) -> None:
        spot = self.spot_prices.get(snapshot.spot_symbol, 0.0)
        if spot <= 0:
            snapshot.rationale = f"Skipped: no spot for {snapshot.spot_symbol}."
            return
        if not snapshot.raw_close_time:
            snapshot.rationale = "Skipped: close time missing."
            return

        hours = max((snapshot.raw_close_time - datetime.now(timezone.utc)).total_seconds() / 3600.0, 0.0)
        if hours <= 0:
            snapshot.rationale = "Skipped: market expired."
            return

        market_family = getattr(snapshot, "market_family", None)

        if market_family == "range_bucket":
            low = getattr(snapshot, "floor_strike", None)
            high = getattr(snapshot, "cap_strike", None)
            fair_yes = self._bucket_probability(spot, low, high, hours)
            if fair_yes is None:
                snapshot.rationale = "Skipped: bucket probability unavailable."
                return
            fair_yes = min(0.98, max(0.02, fair_yes))
            snapshot.fair_yes = round(fair_yes, 4)

            fee_est = float(__import__("os").getenv("KALSHI_TAKER_FEE_ESTIMATE", "0.015"))
            snapshot.mid_edge = round(snapshot.fair_yes - snapshot.mid, 4) if snapshot.mid is not None else None
            snapshot.execution_edge_yes = (
                round(snapshot.fair_yes - snapshot.yes_ask - fee_est, 4)
                if getattr(snapshot, "yes_ask", None) is not None else None
            )
            snapshot.execution_edge_no = (
                round((1.0 - snapshot.fair_yes) - snapshot.no_ask - fee_est, 4)
                if getattr(snapshot, "no_ask", None) is not None else None
            )

            exec_edges = [e for e in [snapshot.execution_edge_yes, snapshot.execution_edge_no] if e is not None]
            if exec_edges:
                snapshot.edge = round(max(exec_edges), 4)
            else:
                snapshot.edge = snapshot.mid_edge

            snapshot.rationale = (
                f"{snapshot.spot_symbol} bucket fair YES {snapshot.fair_yes:.3f}; "
                f"exec_yes={snapshot.execution_edge_yes if snapshot.execution_edge_yes is not None else 'n/a'} "
                f"exec_no={snapshot.execution_edge_no if snapshot.execution_edge_no is not None else 'n/a'}; "
                f"spot=${spot:,.0f} low={low} high={high} hours={hours:.2f}"
            )
            return

        if not snapshot.threshold or not snapshot.direction:
            snapshot.rationale = "Skipped: couldn't parse market into simple above/below threshold."
            return

        sigma_ann = {"BTC": 0.65, "ETH": 0.85, "SOL": 1.20}.get(snapshot.spot_symbol, 0.90)
        sigma_horizon = max(sigma_ann * math.sqrt(hours / (365.0 * 24.0)), 0.04)
        z = math.log(max(spot, 1e-9) / max(snapshot.threshold, 1e-9)) / sigma_horizon
        p_above = 1.0 / (1.0 + math.exp(-1.702 * z))
        fair_yes = p_above if snapshot.direction == "above" else 1.0 - p_above
        fair_yes = min(0.98, max(0.02, fair_yes))
        snapshot.fair_yes = round(fair_yes, 4)

        fee_est = float(__import__("os").getenv("KALSHI_TAKER_FEE_ESTIMATE", "0.015"))
        snapshot.mid_edge = round(snapshot.fair_yes - snapshot.mid, 4) if snapshot.mid is not None else None
        snapshot.execution_edge_yes = (
            round(snapshot.fair_yes - snapshot.yes_ask - fee_est, 4)
            if getattr(snapshot, "yes_ask", None) is not None else None
        )
        snapshot.execution_edge_no = (
            round((1.0 - snapshot.fair_yes) - snapshot.no_ask - fee_est, 4)
            if getattr(snapshot, "no_ask", None) is not None else None
        )

        exec_edges = [e for e in [snapshot.execution_edge_yes, snapshot.execution_edge_no] if e is not None]
        if exec_edges:
            snapshot.edge = round(max(exec_edges), 4)
        else:
            snapshot.edge = snapshot.mid_edge

        snapshot.rationale = (
            f"{snapshot.spot_symbol} ${spot:,.0f} vs threshold ${snapshot.threshold:,.0f}; "
            f"fair YES {snapshot.fair_yes:.3f}; "
            f"exec_yes={snapshot.execution_edge_yes if snapshot.execution_edge_yes is not None else 'n/a'} "
            f"exec_no={snapshot.execution_edge_no if snapshot.execution_edge_no is not None else 'n/a'}"
        )

    def _generate_intent(self, strategy: StrategyState, snapshot: MarketSnapshot) -> Optional[OrderIntent]:
        now = datetime.now(timezone.utc)
        strategy.last_signal_at = now.isoformat()
        strategy.last_signal_reason = snapshot.rationale

        if strategy.name != "crypto_lag":
            return None
        if snapshot.fair_yes is None:
            return None
        if snapshot.spread is None or snapshot.spread > strategy.max_spread:
            return None
        if not snapshot.raw_close_time:
            return None

        hours_to_expiry = (snapshot.raw_close_time - now).total_seconds() / 3600.0
        if hours_to_expiry < strategy.min_hours_to_expiry or hours_to_expiry > strategy.max_hours_to_expiry:
            return None

        min_exec_edge = max(float(strategy.min_edge), float(__import__("os").getenv("KALSHI_MIN_EXECUTION_EDGE", "0.02")))

        execution_edge_yes = getattr(snapshot, "execution_edge_yes", None)
        execution_edge_no = getattr(snapshot, "execution_edge_no", None)

        cooldown_key = f"{strategy.name}:{snapshot.ticker}"
        last = self.cooldowns.get(cooldown_key)
        if last and (now - last).total_seconds() < strategy.cooldown_seconds:
            return None

        candidates = []
        if execution_edge_yes is not None and execution_edge_yes >= min_exec_edge:
            candidates.append(("yes", snapshot.yes_ask, execution_edge_yes))
        if execution_edge_no is not None and execution_edge_no >= min_exec_edge:
            candidates.append(("no", snapshot.no_ask, execution_edge_no))

        if not candidates:
            return None

        side, requested_price, chosen_edge = max(candidates, key=lambda x: x[2])

        if requested_price is None or requested_price <= 0 or requested_price >= 1:
            return None

        max_contracts = max(1, int(strategy.max_ticket_dollars / requested_price))
        contracts = float(max(1, min(max_contracts, 10)))

        return OrderIntent(
            intent_id=str(uuid.uuid4()),
            shadow_group_id=str(uuid.uuid4()),
            strategy=strategy.name,
            broker="kalshi",
            ticker=snapshot.ticker,
            title=snapshot.title,
            side=side,
            action="buy",
            contracts=contracts,
            requested_price=round(requested_price, 4),
            fair_value=float(snapshot.fair_yes),
            edge=float(chosen_edge),
            spot_symbol=snapshot.spot_symbol,
            spot_price=self.spot_prices.get(snapshot.spot_symbol or "", 0.0),
            rationale=(
                f"execution_edge={chosen_edge:+.3f}, spread={snapshot.spread:.3f}, "
                f"fair_yes={snapshot.fair_yes:.3f}, "
                f"yes_ask={getattr(snapshot, 'yes_ask', None)}, no_ask={getattr(snapshot, 'no_ask', None)}, "
                f"mid={getattr(snapshot, 'mid', None)}. {snapshot.rationale}"
            ),
        )

    async def _submit_intent(self, intent: OrderIntent, snapshot: MarketSnapshot) -> None:
        async with self._lock:
            strategy = self.strategy_states[intent.strategy]
            cooldown_key = f"{strategy.name}:{snapshot.ticker}"
            self.cooldowns[cooldown_key] = datetime.now(timezone.utc)

            
            bankroll = self.effective_bankroll()
            total_exposure = 0
            with SessionLocal() as db:
                trades = db.query(Trade).filter(Trade.status.in_(["filled","submitted","pending"])).all()
                for t in trades:
                    if t.fill_price and _field(t, "contracts"):
                        total_exposure += float(t.fill_price) * float(_field(t, "contracts"))

            if total_exposure > bankroll * 0.05:
                self.log("Skipped: total exposure cap reached (5%).")
                return

            shadow_result = await self.paper_broker.execute(intent, snapshot)
            self._store_trade(intent, "paper" if self.paper_mode else "shadow", shadow_result, not self.paper_mode)

            if self.paper_mode:
                self.log(f"PAPER {intent.ticker} {intent.side.upper()} x{int(intent.contracts)} @ {intent.requested_price:.4f}")
                return

            if not self.live_armed:
                self.log(f"Skipped LIVE {intent.ticker}: live not armed.")
                return

            if not strategy.live_enabled:
                self.log(f"Skipped LIVE {intent.ticker}: strategy not live-enabled.")
                return

            if self.session is None:
                self.log("Skipped LIVE order: session missing.")
                return

            try:
                live_result = await self.live_broker.execute(self.session, intent)
                self._store_trade(intent, "live", live_result, False)
                self.log(f"LIVE {intent.ticker} {intent.side.upper()} x{int(intent.contracts)} submitted.")
            except Exception as exc:
                self._store_trade(
                    intent,
                    "live",
                    {"status": "error", "fill_price": None, "external_order_id": None, "message": str(exc)},
                    False,
                )
                self.log(f"LIVE order failed for {intent.ticker}: {exc}")

    def _store_trade(self, intent: OrderIntent, mode: str, result: Dict[str, Any], is_shadow: bool) -> None:
        with SessionLocal() as db:
            row = Trade(
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
                shadow_group_id=intent.shadow_group_id,
                intent_id=f"{intent.intent_id}:{mode}",
                mode=mode,
                broker=intent.broker if mode == "live" else "paper",
                strategy=intent.strategy,
                ticker=intent.ticker,
                title=intent.title,
                side=intent.side,
                action=intent.action,
                contracts=intent.contracts,
                requested_price=intent.requested_price,
                fill_price=result.get("fill_price"),
                fair_value=intent.fair_value,
                edge=intent.edge,
                spot_symbol=intent.spot_symbol,
                spot_price=intent.spot_price,
                status=str(result.get("status", "unknown")),
                external_order_id=result.get("external_order_id"),
                message=result.get("message"),
                is_shadow=is_shadow,
            )
            db.add(row)
            db.commit()

            state = self.strategy_states[intent.strategy]
            state.perf_trades += 1
            if result.get("status") == "filled":
                state.perf_wins += 1
            state.perf_pnl += 0.0

    def recent_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        with SessionLocal() as db:
            rows = db.query(Trade).order_by(Trade.id.desc()).limit(limit).all()
        return [
            {
                "id": row.id,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "mode": row.mode,
                "broker": row.broker,
                "strategy": _field(row, "strategy"),
                "ticker": _field(row, "ticker"),
                "title": row.title,
                "side": _field(row, "side"),
                "action": row.action,
                "contracts": _field(row, "contracts"),
                "requested_price": row.requested_price,
                "fill_price": row.fill_price,
                "fair_value": row.fair_value,
                "edge": row.edge,
                "status": _field(row, "status"),
                "message": _field(row, "message"),
                "spot_symbol": row.spot_symbol,
                "spot_price": row.spot_price,
                "is_shadow": row.is_shadow,
            }
            for row in rows
        ]

    def _mark_price_for_side(self, row: Trade) -> float:
        snapshot = self.market_snapshots.get(_field(row, "ticker"))
        if snapshot:
            if _field(row, "side") == "yes":
                candidates = [snapshot.yes_bid, snapshot.mid, snapshot.yes_ask]
            else:
                candidates = [snapshot.no_bid, None, snapshot.no_ask]
                if snapshot.mid is not None:
                    candidates[1] = round(max(0.0, min(1.0, 1.0 - snapshot.mid)), 4)
            for val in candidates:
                if val is not None:
                    return float(val)
        return float(row.fill_price if row.fill_price is not None else row.requested_price)

    def paper_metrics(self) -> Dict[str, Any]:
        with SessionLocal() as db:
            rows = (
                db.query(Trade)
                .filter(Trade.mode.in_(["paper", "shadow"]))
                .filter(Trade.status.in_(["filled", "submitted", "pending"]))
                .all()
            )

        positions = []
        total_cost = 0.0
        total_mark_value = 0.0

        for row in rows:
            fill = float(row.fill_price if row.fill_price is not None else row.requested_price)
            qty = float(_field(row, "contracts"))
            cost = qty * fill
            mark = self._mark_price_for_side(row)
            mark_value = qty * mark
            pnl = mark_value - cost

            total_cost += cost
            total_mark_value += mark_value

            positions.append({
                "ticker": _field(row, "ticker"),
                "title": row.title,
                "mode": row.mode,
                "side": _field(row, "side"),
                "contracts": qty,
                "avg_cost": round(fill, 4),
                "mark_price": round(mark, 4),
                "cost_basis": round(cost, 2),
                "mark_value": round(mark_value, 2),
                "unrealized_pnl": round(pnl, 2),
                "status": _field(row, "status"),
            })

        paper_cash = round(self.paper_starting_balance - total_cost, 2)
        paper_market_value = round(total_mark_value, 2)
        paper_equity = round(paper_cash + paper_market_value, 2)
        paper_unrealized = round(paper_equity - self.paper_starting_balance, 2)

        positions.sort(key=lambda x: abs(x["unrealized_pnl"]), reverse=True)

        return {
            "starting_balance": round(self.paper_starting_balance, 2),
            "cash": paper_cash,
            "market_value": paper_market_value,
            "equity": paper_equity,
            "unrealized_pnl": paper_unrealized,
            "net_status": "green" if paper_unrealized > 0 else ("red" if paper_unrealized < 0 else "flat"),
            "position_count": len(positions),
            "positions": positions[:30],
        }


    def _strategy_lab_count(self, table_name: str) -> int:
        import sqlite3
        conn = sqlite3.connect(DATA_DIR / "strategy_lab.db")
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        finally:
            conn.close()

    def _best_strategy_lab_variant(self) -> Optional[Dict[str, Any]]:
        import sqlite3
        conn = sqlite3.connect(DATA_DIR / "strategy_lab.db")
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute("""
                SELECT variant_name, window_name, trade_count, win_rate, realized_pnl, score
                FROM strategy_summaries
                ORDER BY score DESC, trade_count DESC
                LIMIT 1
            """).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def effective_bankroll(self) -> float:
        """
        Safe bankroll accessor used by sizing / dashboard code.
        In paper mode, use current paper equity.
        In live mode, use current Kalshi cash balance.
        """
        mode = str(getattr(self, "mode", "paper") or "paper").lower()

        try:
            paper = self.paper_summary() if hasattr(self, "paper_summary") else {}
        except Exception:
            paper = {}

        try:
            paper_equity = float((paper or {}).get("equity", getattr(self, "paper_starting_balance", 1000.0)) or getattr(self, "paper_starting_balance", 1000.0))
        except Exception:
            paper_equity = float(getattr(self, "paper_starting_balance", 1000.0) or 1000.0)

        try:
            live_cash = float(getattr(self, "kalshi_balance", 0.0) or 0.0)
        except Exception:
            live_cash = 0.0

        if mode == "live":
            return max(live_cash, 0.0)

        return max(paper_equity, 0.0)


    def dashboard_status(self) -> Dict[str, Any]:
        markets = sorted(
            [m.to_dict() for m in self.market_snapshots.values()],
            key=lambda x: abs((x.get("edge") or 0.0)),
            reverse=True,
        )[:30]

        top_edge = max([abs(m.get("edge") or 0.0) for m in markets], default=0.0)

        try:
            from datetime import datetime, timezone
            from app.strategy_lab.research_store import record_market_observations

            ts = datetime.now(timezone.utc).isoformat()
            rows = []
            market_rows_built = 0
            snapshot_rows_built = 0

            raw_snapshots = getattr(self, "market_snapshots", None) or {}
            self.last_market_count_for_capture = len(markets or [])
            self.last_snapshot_count_for_capture = len(raw_snapshots or {})

            # First attempt: use dashboard markets list
            for m in (markets or []):
                if not isinstance(m, dict):
                    continue
                ticker = m.get("ticker")
                if not ticker:
                    continue

                asset = m.get("spot_symbol") or m.get("asset")
                try:
                    spot = float((self.spot_prices or {}).get(asset) or 0.0) if asset else None
                except Exception:
                    spot = None

                rows.append({
                    "ts": ts,
                    "ticker": ticker,
                    "asset": asset,
                    "market_family": m.get("market_family"),
                    "status": m.get("status") or "active",
                    "floor_strike": m.get("floor_strike"),
                    "cap_strike": m.get("cap_strike"),
                    "threshold": m.get("threshold"),
                    "direction": m.get("direction"),
                    "yes_bid": m.get("yes_bid"),
                    "yes_ask": m.get("yes_ask"),
                    "no_bid": m.get("no_bid"),
                    "no_ask": m.get("no_ask"),
                    "mid": m.get("mid"),
                    "spread": m.get("spread"),
                    "open_interest": m.get("open_interest"),
                    "volume": m.get("volume"),
                    "close_time": m.get("close_time"),
                    "spot": spot,
                    "fair_yes": m.get("fair_yes"),
                    "edge": m.get("edge"),
                    "distance_from_spot_pct": m.get("distance_from_spot_pct"),
                })
                market_rows_built += 1

            # Fallback: if dashboard markets were empty/useless, use raw snapshots directly
            if not rows:
                for ticker, snapshot in (raw_snapshots or {}).items():
                    if not ticker:
                        continue

                    asset = getattr(snapshot, "spot_symbol", None)
                    try:
                        spot = float((self.spot_prices or {}).get(asset) or 0.0) if asset else None
                    except Exception:
                        spot = None

                    close_time = getattr(snapshot, "raw_close_time", None)
                    if close_time is not None:
                        try:
                            close_time = close_time.isoformat()
                        except Exception:
                            close_time = str(close_time)

                    rows.append({
                        "ts": ts,
                        "ticker": ticker,
                        "asset": asset,
                        "market_family": getattr(snapshot, "market_family", None),
                        "status": "active",
                        "floor_strike": getattr(snapshot, "floor_strike", None),
                        "cap_strike": getattr(snapshot, "cap_strike", None),
                        "threshold": getattr(snapshot, "threshold", None),
                        "direction": getattr(snapshot, "direction", None),
                        "yes_bid": getattr(snapshot, "yes_bid", None),
                        "yes_ask": getattr(snapshot, "yes_ask", None),
                        "no_bid": getattr(snapshot, "no_bid", None),
                        "no_ask": getattr(snapshot, "no_ask", None),
                        "mid": getattr(snapshot, "mid", None),
                        "spread": getattr(snapshot, "spread", None),
                        "open_interest": getattr(snapshot, "open_interest", None),
                        "volume": getattr(snapshot, "volume", None),
                        "close_time": close_time,
                        "spot": spot,
                        "fair_yes": getattr(snapshot, "fair_yes", None),
                        "edge": getattr(snapshot, "edge", None),
                        "distance_from_spot_pct": getattr(snapshot, "distance_from_spot_pct", None),
                    })
                    snapshot_rows_built += 1

            self.last_market_rows_built_for_capture = market_rows_built
            self.last_snapshot_rows_built_for_capture = snapshot_rows_built

            if rows:
                self.last_research_rows_inserted = record_market_observations(rows)
            else:
                self.last_research_rows_inserted = 0
        except Exception as e:
            try:
                self.last_research_rows_inserted = 0
                self.log(f"Inline research capture failed: {e}")
            except Exception:
                pass

        return {
            "mode": "paper" if self.paper_mode else "live",
            "live_armed": self.live_armed,
            "top_edge": top_edge,
            "credentials": {
                "kalshi_key_loaded": bool(self.kalshi_client.api_key_id),
                "kalshi_private_key_loaded": bool(self.kalshi_client.private_key),
                "kalshi_trading_enabled": self.kalshi_client.trading_enabled,
                "kalshi_base_url": self.kalshi_client.base_url,
                "spot_source": "coinbase_websocket",
            },
            "health": self.health,
            "pipeline": {
                "raw_open_markets": self.raw_market_count,
                "normalized_markets": self.normalized_market_count,
                "classified_crypto_markets": self.classified_market_count,
                "eligible_markets": self.eligible_market_count,
                "cached_crypto_ticker_count": len(self.cached_crypto_tickers),
                "watch_cursor": self.watch_cursor,
                "watch_refresh_limit": self.watch_refresh_limit,
                "rejections": dict(self.market_rejections),
                "rejection_examples": self.market_rejection_examples,
                "raw_market_samples": self.raw_market_samples,
                "watched_market_samples": self.watched_market_samples,
                "last_full_scan_at": self.last_full_scan_at.isoformat() if self.last_full_scan_at else None,
                "full_scan_seconds": self.full_scan_seconds,
            },
            "spots": self.spot_prices,
            "balances": {
                "kalshi_balance": self.kalshi_balance,
                "portfolio_value": self.portfolio_value,
            },
            "paper": self.paper_metrics(),
            "regime": {
            "current": getattr(bot, "current_regime", "conservative"),
            "config": getattr(bot, "current_regime_config", {}),
        },
        "strategy_lab": {
                "totals": {
                    "candidate_events": self._strategy_lab_count("candidate_events"),
                    "shadow_trades": self._strategy_lab_count("shadow_trades"),
                    "shadow_positions": self._strategy_lab_count("shadow_positions"),
                    "strategy_summaries": self._strategy_lab_count("strategy_summaries"),
                    "parameter_suggestions": self._strategy_lab_count("parameter_suggestions"),
                },
                "best_variant": self._best_strategy_lab_variant(),
                "summaries": get_latest_summaries()[:12],
                "suggestions": get_latest_suggestions()[:12],
                "shadow_positions": get_shadow_positions()[:12],
                "operator_settings": get_operator_settings()[:24],
            },
            "strategies": {name: state.to_dict() for name, state in self.strategy_states.items()},
            "markets": markets,
            "recent_trades": self.recent_trades(40),
            "logs": list(self.logs),
        }

    def debug_report_text(self) -> str:
        status = self.dashboard_status()
        masked_env = {}
        for key, value in os.environ.items():
            upper = key.upper()
            if any(token in upper for token in ["KEY", "SECRET", "TOKEN", "PASSWORD", "PRIVATE"]):
                if value:
                    masked_env[key] = f"{value[:4]}...{value[-4:]}" if len(value) > 8 else "***"
                else:
                    masked_env[key] = ""
            elif upper.startswith(("KALSHI_", "PAPER_", "ARM_", "MAX_", "WATCH_", "MODE", "OLLAMA_")):
                masked_env[key] = value

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "host": {
                "hostname": socket.gethostname(),
                "python": sys.version,
                "platform": platform.platform(),
            },
            "app": {
                "db_path": str(DB_PATH),
                "paper_mode": self.paper_mode,
                "live_armed": self.live_armed,
                "poll_seconds": self.poll_seconds,
                "watch_terms": self.watch_terms,
                "market_count": len(self.market_snapshots),
                "log_count": len(self.logs),
            },
            "credentials": status["credentials"],
            "health": status["health"],
            "pipeline": status["pipeline"],
            "balances": status["balances"],
            "paper": status["paper"],
            "spots": status["spots"],
            "strategies": status["strategies"],
            "top_markets": status["markets"][:20],
            "recent_trades": status["recent_trades"][:40],
            "recent_logs": status["logs"][:150],
            "env": masked_env,
        }

        sections = [
            "=== KALSHI QUANT DESK DEBUG REPORT ===",
            json.dumps(report, indent=2, default=str),
        ]
        return "\n\n".join(sections)

    def set_mode(self, mode: str) -> None:
        if mode == "live" and not self.live_armed:
            raise HTTPException(status_code=400, detail="Live trading is not armed.")
        if mode == "live" and not self.kalshi_client.trading_enabled:
            raise HTTPException(status_code=400, detail="Kalshi credentials are not configured.")
        self.paper_mode = mode == "paper"
        self.log(f"Mode switched to {mode.upper()}.")

    def set_live_arm(self, armed: bool) -> None:
        self.live_armed = armed
        self.log(f"Live trading {'ARMED' if armed else 'DISARMED'}.")

    def update_strategy(self, name: str, payload: StrategyUpdate) -> None:
        if name not in self.strategy_states:
            raise HTTPException(status_code=404, detail=f"Unknown strategy: {name}")
        state = self.strategy_states[name]
        state.enabled = payload.enabled
        if payload.live_enabled is not None:
            state.live_enabled = payload.live_enabled
        self.log(f"Strategy {name}: enabled={state.enabled}, live_enabled={state.live_enabled}")

    @staticmethod
    def _float_or_none(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _price_to_float(cls, dollars_value: Any, cents_value: Any) -> Optional[float]:
        if dollars_value not in (None, ""):
            try:
                return float(dollars_value)
            except (TypeError, ValueError):
                pass
        if cents_value not in (None, ""):
            try:
                return float(cents_value) / 100.0
            except (TypeError, ValueError):
                pass
        return None

    @staticmethod
    def _parse_dt(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    @staticmethod
    def _detect_symbol(text: str) -> Optional[str]:
        upper = text.upper()
        if "BITCOIN" in upper or re.search(r"\bBTC\b", upper):
            return "BTC"
        if "ETHEREUM" in upper or re.search(r"\bETH\b", upper):
            return "ETH"
        if "SOLANA" in upper or re.search(r"\bSOL\b", upper):
            return "SOL"
        return None

    @classmethod
    def _parse_threshold(cls, text: str) -> tuple[Optional[float], Optional[str]]:
        upper = f" {text.upper()} "
        direction = None
        if any(token in upper for token in [" ABOVE ", " OVER ", " GREATER THAN ", " AT LEAST "]):
            direction = "above"
        elif any(token in upper for token in [" BELOW ", " UNDER ", " LESS THAN ", " AT MOST "]):
            direction = "below"
        if direction is None:
            return None, None

        numbers = cls.NUMBER_RE.findall(upper)
        parsed: List[float] = []
        for raw in numbers:
            try:
                parsed.append(float(raw.replace(",", "")))
            except ValueError:
                pass
        parsed = [x for x in parsed if x >= 10]
        if not parsed:
            return None, direction
        return max(parsed), direction


bot = TradingBot()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await bot.start()
    try:
        yield
    finally:
        await bot.stop()


app = FastAPI(title="Kalshi Operator", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/strategy-lab/settings")
async def api_strategy_lab_settings():
    return {"ok": True, "settings": get_operator_settings()}


@app.post("/api/strategy-lab/settings")
async def api_strategy_lab_settings_update(payload: StrategyLabSettingUpdate):
    set_operator_setting(
        strategy_name=payload.strategy_name,
        variant_name=payload.variant_name,
        parameter_name=payload.parameter_name,
        value_text=payload.value_text,
        source=payload.source,
        applied=0,
    )
    return {"ok": True}


@app.get("/api/status")
async def api_status():
    result = bot.dashboard_status()
    try:
        import inspect
        if inspect.isawaitable(result):
            result = await result
    except Exception:
        pass
    return JSONResponse(result)


@app.post("/api/mode")
async def api_mode(payload: ModeUpdate):
    bot.set_mode(payload.mode)
    return {"ok": True, "mode": payload.mode}


@app.post("/api/arm-live")
async def api_arm_live(payload: ArmLiveUpdate):
    bot.set_live_arm(payload.armed)
    return {"ok": True, "live_armed": payload.armed}


@app.post("/api/strategy/{name}")
async def api_strategy(name: str, payload: StrategyUpdate):
    bot.update_strategy(name, payload)
    return {"ok": True, "strategy": name}


@app.get("/api/debug-report", response_class=PlainTextResponse)
async def api_debug_report():
    return PlainTextResponse(
        bot.debug_report_text(),
        headers={"Content-Disposition": f'attachment; filename="kalshi-debug-report-{datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")}.txt"'},
    )


@app.get("/api/trades")
async def api_trades(limit: int = 50):
    return {"trades": bot.recent_trades(limit=max(1, min(limit, 200)))}


@app.post("/api/rebuild-cache")
async def rebuild_cache():
    result = await bot._run_btc_cache_rebuild(reason="manual")
    return result


@app.get("/api/status")
def api_status():
    try:
        return bot.safe_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------------------------------------------
# 4️⃣ DB AUTO PRUNE (prevents table explosion)
# -------------------------------------------------------------------

def prune_candidate_events():
    """
    Keeps candidate_events under control.
    """
    try:
        conn = sqlite3.connect("data/strategy_lab.db")
        conn.execute("""
            DELETE FROM candidate_events
            WHERE id NOT IN (
                SELECT id FROM candidate_events
                ORDER BY id DESC
                LIMIT 2000
            )
        """)
        conn.commit()
        conn.close()
    except Exception:
        pass


# -------------------------------------------------------------------
# 5️⃣ LOOP GUARD (prevents event loop starvation)
# -------------------------------------------------------------------

def guarded_main_loop(original_loop):
    """
    Wraps the main loop so it never blocks FastAPI.
    """
    def wrapper(*args, **kwargs):
        while True:
            try:
                original_loop(*args, **kwargs)
            except Exception as e:
                bot.loop_error = str(e)

            prune_candidate_events()

            time.sleep(0.5)

    return wrapper


if not hasattr(TradingBot, "_loop_wrapped"):
    TradingBot._loop_wrapped = True
# old crashing loop monkey-patch removed

# =====================================================================================
# END PATCH
# =====================================================================================


# === MEGA_PATCH_RUNTIME_CONTROLS_V1 ===
import os
import io
import json
import sqlite3
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

class PaperControlsUpdate(BaseModel):
    enabled: bool
    mode: Optional[str] = "paper"

def _mp_utc_now():
    return datetime.now(timezone.utc).isoformat()

def _mp_get_repo_root():
    return Path("/app")

def _mp_db_path():
    return "data/strategy_lab.db"

def _mp_safe_json(obj):
    try:
        return json.dumps(obj, indent=2, default=str)
    except Exception as e:
        return json.dumps({"json_error": str(e)}, indent=2)

def _mp_ensure_runtime_tables():
    conn = sqlite3.connect(_mp_db_path())
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS operator_state (
            key TEXT PRIMARY KEY,
            value_text TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS operator_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_name TEXT NOT NULL,
            variant_name TEXT NOT NULL,
            parameter_name TEXT NOT NULL,
            value_text TEXT NOT NULL,
            source TEXT,
            applied INTEGER DEFAULT 0,
            ts TEXT NOT NULL
        )
    """)

    cur.execute("PRAGMA table_info(shadow_trades)")
    cols = {row[1] for row in cur.fetchall()}
    if "close_reason" not in cols:
        cur.execute("ALTER TABLE shadow_trades ADD COLUMN close_reason TEXT")
    if "exit_price" not in cols:
        cur.execute("ALTER TABLE shadow_trades ADD COLUMN exit_price REAL")
    if "closed_at" not in cols:
        cur.execute("ALTER TABLE shadow_trades ADD COLUMN closed_at TEXT")
    if "realized_pnl" not in cols:
        cur.execute("ALTER TABLE shadow_trades ADD COLUMN realized_pnl REAL")
    if "outcome" not in cols:
        cur.execute("ALTER TABLE shadow_trades ADD COLUMN outcome TEXT")

    conn.commit()
    conn.close()

def _mp_state_get(key, default=None):
    conn = sqlite3.connect(_mp_db_path())
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT value_text FROM operator_state WHERE key = ?",
        (key,)
    ).fetchone()
    conn.close()
    return row["value_text"] if row else default

def _mp_state_set(key, value):
    conn = sqlite3.connect(_mp_db_path())
    conn.execute(
        """
        INSERT INTO operator_state (key, value_text, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value_text = excluded.value_text,
            updated_at = excluded.updated_at
        """,
        (key, str(value), _mp_utc_now()),
    )
    conn.commit()
    conn.close()

def _mp_bool(raw, default=False):
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}

def _mp_get_paper_controls():
    enabled = _mp_bool(_mp_state_get("paper_enabled", "true"), True)
    mode = _mp_state_get("paper_mode", "paper") or "paper"
    last_reset_at = _mp_state_get("paper_last_reset_at", None)
    return {
        "enabled": enabled,
        "last_reset_at": last_reset_at,
        "mode": mode,
        "can_trade_now": bool(enabled and mode == "paper"),
    }

def _mp_set_paper_controls(enabled: bool, mode: str = "paper"):
    _mp_state_set("paper_enabled", "true" if enabled else "false")
    _mp_state_set("paper_mode", mode or "paper")

def _mp_reset_paper_state():
    conn = sqlite3.connect(_mp_db_path())
    cur = conn.cursor()

    cur.execute("DELETE FROM shadow_positions")
    cur.execute("DELETE FROM shadow_trades WHERE closed_at IS NULL")
    cur.execute("DELETE FROM candidate_events")
    cur.execute("DELETE FROM strategy_summaries")
    cur.execute("DELETE FROM parameter_suggestions")

    conn.commit()
    conn.close()

    _mp_state_set("paper_last_reset_at", _mp_utc_now())

def _mp_effective_bankroll(self):
    try:
        mode = getattr(self, "mode", "paper")
        if mode == "live":
            return float(getattr(self, "kalshi_balance", 0.0) or 0.0)

        paper = self.get_paper_snapshot() if hasattr(self, "get_paper_snapshot") else {}
        if isinstance(paper, dict):
            return float(paper.get("equity", getattr(self, "paper_starting_balance", 1000.0)) or 0.0)

        return float(getattr(self, "paper_starting_balance", 1000.0) or 1000.0)
    except Exception:
        return float(getattr(self, "paper_starting_balance", 1000.0) or 1000.0)

def _mp_patch_tradingbot():
    if "TradingBot" not in globals():
        return

    if hasattr(TradingBot, "__init__"):
        original_init = TradingBot.__init__

        def wrapped_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            try:
                _mp_ensure_runtime_tables()
            except Exception:
                pass
            try:
                self.paper_controls = _mp_get_paper_controls()
            except Exception:
                self.paper_controls = {
                    "enabled": True,
                    "last_reset_at": None,
                    "mode": "paper",
                    "can_trade_now": True,
                }

        TradingBot.__init__ = wrapped_init

def _mp_patch_shadow_engine():
    try:
        from app.strategy_lab.shadow_engine import ShadowEngine
    except Exception:
        return

    if getattr(ShadowEngine, "_mega_patch_wrapped", False):
        return

    original_run_shadow_variants = ShadowEngine.run_shadow_variants

    def wrapped_run_shadow_variants(self, *args, **kwargs):
        controls = _mp_get_paper_controls()

        # always allow marking/exits to continue even if paper opens are disabled
        if not controls.get("enabled", True):
            try:
                if hasattr(self, "mark_positions"):
                    self.mark_positions()
                if hasattr(self, "run_exit_logic"):
                    self.run_exit_pass()
                elif hasattr(self, "run_exit_pass"):
                    self.run_exit_pass()
            except Exception as e:
                try:
                    self.bot.loop_error = str(e)
                except Exception:
                    pass
            return

        return original_run_shadow_variants(self, *args, **kwargs)

    ShadowEngine.run_shadow_variants = wrapped_run_shadow_variants
    ShadowEngine._mega_patch_wrapped = True

def _mp_debug_full_text():
    parts = []
    parts.append("===== FULL DEBUG BUNDLE =====")
    parts.append(f"generated_at_utc: {_mp_utc_now()}")

    try:
        parts.append("\n===== PAPER CONTROLS =====")
        parts.append(_mp_safe_json(_mp_get_paper_controls()))
    except Exception as e:
        parts.append(f"paper_controls_error: {e}")

    try:
        parts.append("\n===== LIVE STATUS SNAPSHOT =====")
        if "bot" in globals() and hasattr(bot, "get_status"):
            parts.append(_mp_safe_json(bot.get_status()))
        elif "bot" in globals() and hasattr(bot, "status"):
            parts.append(_mp_safe_json(bot.status()))
        else:
            parts.append("status method not found on bot")
    except Exception as e:
        parts.append(f"status_error: {e}")

    try:
        conn = sqlite3.connect(_mp_db_path())
        conn.row_factory = sqlite3.Row

        parts.append("\n===== DB TABLE COUNTS =====")
        for table in [
            "candidate_events",
            "shadow_trades",
            "shadow_positions",
            "strategy_summaries",
            "parameter_suggestions",
            "operator_settings",
            "operator_state",
        ]:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                parts.append(f"{table}: {count}")
            except Exception as e:
                parts.append(f"{table}: ERROR -> {e}")

        parts.append("\n===== OPEN SHADOW POSITIONS =====")
        try:
            rows = conn.execute("""
                SELECT ticker, variant_name, opened_at, entry_price, qty, last_mark_price, unrealized_pnl
                FROM shadow_positions
                ORDER BY opened_at DESC
                LIMIT 100
            """).fetchall()
            for row in rows:
                parts.append(_mp_safe_json(dict(row)))
        except Exception as e:
            parts.append(f"open_positions_error: {e}")

        parts.append("\n===== RECENT SHADOW TRADES =====")
        try:
            rows = conn.execute("""
                SELECT ticker, variant_name, opened_at, closed_at, entry_price, exit_price,
                       realized_pnl, outcome, close_reason
                FROM shadow_trades
                ORDER BY id DESC
                LIMIT 200
            """).fetchall()
            for row in rows:
                parts.append(_mp_safe_json(dict(row)))
        except Exception as e:
            parts.append(f"shadow_trades_error: {e}")

        parts.append("\n===== REJECTION SUMMARY =====")
        try:
            rows = conn.execute("""
                SELECT variant_name, rejection_reason, COUNT(*) n
                FROM candidate_events
                WHERE eligible = 0
                GROUP BY variant_name, rejection_reason
                ORDER BY n DESC
                LIMIT 200
            """).fetchall()
            for row in rows:
                parts.append(_mp_safe_json(dict(row)))
        except Exception as e:
            parts.append(f"rejections_error: {e}")

        parts.append("\n===== CLOSE REASON SUMMARY =====")
        try:
            rows = conn.execute("""
                SELECT close_reason, COUNT(*) n, ROUND(SUM(COALESCE(realized_pnl,0)),4) pnl
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                GROUP BY close_reason
                ORDER BY n DESC
            """).fetchall()
            for row in rows:
                parts.append(_mp_safe_json(dict(row)))
        except Exception as e:
            parts.append(f"close_reasons_error: {e}")

        conn.close()
    except Exception as e:
        parts.append(f"db_error: {e}")

    repo_root = _mp_get_repo_root()
    for rel in [
        "app/main.py",
        "app/strategy_lab/shadow_engine.py",
        "app/strategy_lab/outcomes.py",
        "app/strategy_lab/suggestions.py",
        "app/strategy_lab/gui_api.py",
        "app/strategy_lab/config.py",
        "app/templates/index.html",
        "app/static/app.js",
        "docker-compose.yml",
        "Dockerfile",
    ]:
        try:
            fp = repo_root / rel
            if fp.exists():
                parts.append(f"\n===== FILE: {rel} =====")
                parts.append(fp.read_text())
        except Exception as e:
            parts.append(f"\n===== FILE: {rel} ERROR =====")
            parts.append(str(e))

    return "\n".join(parts)

_mp_ensure_runtime_tables()
_mp_patch_tradingbot()
_mp_patch_shadow_engine()

@app.get("/api/paper-controls")
async def api_get_paper_controls():
    return {"ok": True, "paper_controls": _mp_get_paper_controls()}

@app.post("/api/paper-controls")
async def api_set_paper_controls(payload: PaperControlsUpdate):
    _mp_set_paper_controls(payload.enabled, payload.mode or "paper")
    if "bot" in globals():
        try:
            bot.paper_controls = _mp_get_paper_controls()
        except Exception:
            pass
    return {"ok": True, "paper_controls": _mp_get_paper_controls()}

@app.post("/api/paper-reset")
async def api_paper_reset():
    _mp_reset_paper_state()
    if "bot" in globals():
        try:
            bot.paper_controls = _mp_get_paper_controls()
        except Exception:
            pass
    return {"ok": True, "paper_controls": _mp_get_paper_controls()}


@app.get("/api/paper-session")
async def api_paper_session():
    import sqlite3
    from pathlib import Path

    db_path = Path(__file__).resolve().parent.parent / "data" / "strategy_lab.db"
    controls = bot.paper_controls_status() if hasattr(bot, "paper_controls_status") else {}
    reset_at = controls.get("last_reset_at")

    def _rows(conn, query, params=()):
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(query, params).fetchall()]

    if not db_path.exists():
        return {
            "ok": False,
            "error": f"missing db: {db_path}",
            "session": {
                "controls": controls,
                "summary": {},
                "close_reasons": [],
                "variants": [],
                "recent_closed": [],
                "recent_open": [],
                "readiness": {
                    "status": "NO_DB",
                    "ready_for_live": False,
                    "notes": ["strategy_lab.db not found"],
                },
            },
        }

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        trade_where = ""
        trade_params = []
        if reset_at:
            trade_where = "WHERE opened_at >= ?"
            trade_params = [reset_at]

        trade_summary = dict(
            conn.execute(
                f"""
                SELECT
                    COUNT(*) AS total_trades,
                    SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS open_trades,
                    SUM(CASE WHEN closed_at IS NOT NULL THEN 1 ELSE 0 END) AS closed_trades,
                    SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN outcome = 'flat' THEN 1 ELSE 0 END) AS flats,
                    ROUND(SUM(CASE WHEN closed_at IS NOT NULL THEN COALESCE(realized_pnl, 0) ELSE 0 END), 4) AS realized_pnl,
                    ROUND(AVG(CASE WHEN closed_at IS NOT NULL THEN realized_pnl END), 4) AS avg_realized_pnl
                FROM shadow_trades
                {trade_where}
                """,
                trade_params,
            ).fetchone()
        )

        total_trades = int(trade_summary.get("total_trades") or 0)
        open_trades = int(trade_summary.get("open_trades") or 0)
        closed_trades = int(trade_summary.get("closed_trades") or 0)
        wins = int(trade_summary.get("wins") or 0)
        losses = int(trade_summary.get("losses") or 0)
        flats = int(trade_summary.get("flats") or 0)
        realized_pnl = float(trade_summary.get("realized_pnl") or 0.0)
        avg_realized_pnl = float(trade_summary.get("avg_realized_pnl") or 0.0)
        win_rate = round((wins / closed_trades), 4) if closed_trades else 0.0

        close_reasons = _rows(
            conn,
            f"""
            SELECT
                COALESCE(close_reason, 'unknown') AS close_reason,
                COUNT(*) AS n,
                ROUND(SUM(COALESCE(realized_pnl, 0)), 4) AS pnl
            FROM shadow_trades
            {trade_where.replace("opened_at", "opened_at")}
            AND closed_at IS NOT NULL
            GROUP BY COALESCE(close_reason, 'unknown')
            ORDER BY n DESC, pnl DESC
            """
            if trade_where
            else """
            SELECT
                COALESCE(close_reason, 'unknown') AS close_reason,
                COUNT(*) AS n,
                ROUND(SUM(COALESCE(realized_pnl, 0)), 4) AS pnl
            FROM shadow_trades
            WHERE closed_at IS NOT NULL
            GROUP BY COALESCE(close_reason, 'unknown')
            ORDER BY n DESC, pnl DESC
            """,
            trade_params if trade_where else (),
        )

        variants = _rows(
            conn,
            f"""
            SELECT
                variant_name,
                COUNT(*) AS total_trades,
                SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) AS open_trades,
                SUM(CASE WHEN closed_at IS NOT NULL THEN 1 ELSE 0 END) AS closed_trades,
                SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN outcome = 'loss' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN outcome = 'flat' THEN 1 ELSE 0 END) AS flats,
                ROUND(SUM(CASE WHEN closed_at IS NOT NULL THEN COALESCE(realized_pnl, 0) ELSE 0 END), 4) AS realized_pnl
            FROM shadow_trades
            {trade_where}
            GROUP BY variant_name
            ORDER BY realized_pnl DESC, total_trades DESC, variant_name
            """,
            trade_params,
        )

        recent_closed = _rows(
            conn,
            f"""
            SELECT
                ticker, variant_name, opened_at, closed_at,
                entry_price, exit_price, realized_pnl, outcome, close_reason
            FROM shadow_trades
            {trade_where.replace("opened_at", "opened_at")}
            AND closed_at IS NOT NULL
            ORDER BY id DESC
            LIMIT 15
            """
            if trade_where
            else """
            SELECT
                ticker, variant_name, opened_at, closed_at,
                entry_price, exit_price, realized_pnl, outcome, close_reason
            FROM shadow_trades
            WHERE closed_at IS NOT NULL
            ORDER BY id DESC
            LIMIT 15
            """,
            trade_params if trade_where else (),
        )

        recent_open = _rows(
            conn,
            """
            SELECT
                ticker, variant_name, opened_at, entry_price, qty,
                last_mark_price, unrealized_pnl
            FROM shadow_positions
            ORDER BY opened_at DESC
            LIMIT 15
            """,
        )

        notes = []
        ready = True

        if closed_trades < 20:
            ready = False
            notes.append(f"Need more closed trades since reset. Current sample: {closed_trades}/20.")

        if realized_pnl <= 0:
            ready = False
            notes.append(f"Realized PnL since reset is not positive: {realized_pnl:.4f}.")

        if closed_trades >= 10 and win_rate < 0.55:
            ready = False
            notes.append(f"Win rate since reset is below threshold: {win_rate:.2%} < 55.00%.")

        if open_trades > 6:
            ready = False
            notes.append(f"Too many concurrent open trades for a clean paper evaluation: {open_trades}.")

        if not controls.get("enabled", False):
            ready = False
            notes.append("Paper trading is currently disabled.")

        if controls.get("mode") != "paper":
            ready = False
            notes.append(f"Paper control mode is not 'paper': {controls.get('mode')}.")

        if not notes:
            notes.append("Paper session meets current basic thresholds for a small live pilot.")
            status = "READY_FOR_SMALL_LIVE_PILOT"
        elif closed_trades < 20:
            status = "BUILDING_SAMPLE"
        else:
            status = "NOT_READY"

        return {
            "ok": True,
            "session": {
                "controls": controls,
                "summary": {
                    "since_reset_at": reset_at,
                    "total_trades": total_trades,
                    "open_trades": open_trades,
                    "closed_trades": closed_trades,
                    "wins": wins,
                    "losses": losses,
                    "flats": flats,
                    "win_rate": win_rate,
                    "realized_pnl": realized_pnl,
                    "avg_realized_pnl": avg_realized_pnl,
                },
                "close_reasons": close_reasons,
                "variants": variants,
                "recent_closed": recent_closed,
                "recent_open": recent_open,
                "readiness": {
                    "status": status,
                    "ready_for_live": ready,
                    "notes": notes,
                },
            },
        }
    finally:
        conn.close()

def compute_paper_equity(self):
    """
    Truthier paper accounting for the current paper session.

    Equity =
        starting_balance
        + realized PnL from closed shadow trades
        + unrealized PnL from currently open shadow positions

    Cash =
        starting_balance
        + realized PnL
        - current cost basis of open positions

    Market value =
        liquidation-style mark value of open positions
    """
    import sqlite3

    db_path = "data/strategy_lab.db"
    starting = float(getattr(self, "paper_starting_balance", 1000.0) or 1000.0)

    reset_at = getattr(self, "paper_last_reset_at", None)

    # fallback to persisted operator_state if present
    if not reset_at:
        try:
            conn0 = sqlite3.connect(db_path)
            row0 = conn0.execute(
                "SELECT value_text FROM operator_state WHERE key = 'paper_last_reset_at'"
            ).fetchone()
            conn0.close()
            if row0 and row0[0]:
                reset_at = row0[0]
        except Exception:
            reset_at = None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # closed-trade realized pnl
        if reset_at:
            realized_row = conn.execute(
                """
                SELECT ROUND(SUM(COALESCE(realized_pnl, 0)), 4)
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                  AND (opened_at >= ? OR closed_at >= ?)
                """,
                (reset_at, reset_at),
            ).fetchone()
        else:
            realized_row = conn.execute(
                """
                SELECT ROUND(SUM(COALESCE(realized_pnl, 0)), 4)
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                """
            ).fetchone()

        realized_pnl = float((realized_row[0] if realized_row and realized_row[0] is not None else 0.0) or 0.0)

        # open positions
        if reset_at:
            pos_rows = conn.execute(
                """
                SELECT ticker, variant_name, opened_at, qty, entry_price, last_mark_price, unrealized_pnl
                FROM shadow_positions
                WHERE opened_at >= ?
                ORDER BY opened_at DESC
                """,
                (reset_at,),
            ).fetchall()
        else:
            pos_rows = conn.execute(
                """
                SELECT ticker, variant_name, opened_at, qty, entry_price, last_mark_price, unrealized_pnl
                FROM shadow_positions
                ORDER BY opened_at DESC
                """
            ).fetchall()

        total_cost = 0.0
        total_mark_value = 0.0
        positions = []

        for row in pos_rows:
            qty = int(row["qty"] or 0)
            entry = float(row["entry_price"] or 0.0)
            mark = float((row["last_mark_price"] if row["last_mark_price"] is not None else entry) or entry)

            cost_basis = qty * entry
            market_value = qty * mark
            total_cost += cost_basis
            total_mark_value += market_value

            positions.append({
                "ticker": row["ticker"],
                "variant_name": row["variant_name"],
                "opened_at": row["opened_at"],
                "qty": qty,
                "entry_price": round(entry, 4),
                "mark_price": round(mark, 4),
                "cost_basis": round(cost_basis, 4),
                "market_value": round(market_value, 4),
                "unrealized_pnl": round(market_value - cost_basis, 4),
            })

        unrealized_pnl = total_mark_value - total_cost
        cash = starting + realized_pnl - total_cost
        equity = cash + total_mark_value
        net_pnl = realized_pnl + unrealized_pnl

        return {
            "starting_balance": round(starting, 2),
            "cash": round(cash, 2),
            "market_value": round(total_mark_value, 2),
            "equity": round(equity, 2),
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": round(unrealized_pnl, 2),
            "net_pnl": round(net_pnl, 2),
            "net_status": "green" if net_pnl > 0 else ("red" if net_pnl < 0 else "flat"),
            "position_count": len(positions),
            "positions": positions[:30],
            "session_reset_at": reset_at,
        }

    finally:
        conn.close()

TradingBot.compute_paper_equity = compute_paper_equity

@app.get("/api/debug/full")
async def api_debug_full():
    from datetime import datetime, timezone
    from fastapi.responses import PlainTextResponse

    text = generate_handoff_bundle(bot)
    filename = f'kalshi-self-contained-ai-handoff-{datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")}.txt'
    return PlainTextResponse(
        text,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

@app.get("/api/debug/handoff-memory")
async def api_debug_handoff_memory():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(read_memory_text())

@app.post("/api/debug/handoff-note")
async def api_debug_handoff_note(payload: dict):
    title = str((payload or {}).get("title") or "").strip()
    content = str((payload or {}).get("content") or "").strip()
    category = str((payload or {}).get("category") or "general").strip() or "general"

    if not title:
        raise HTTPException(status_code=400, detail="title is required")
    if not content:
        raise HTTPException(status_code=400, detail="content is required")

    append_memory_note(title=title, content=content, category=category)
    return {
        "ok": True,
        "message": "handoff note appended",
        "title": title,
        "category": category,
    }


# --- truthful paper dashboard wrapper ---
try:
    _orig_dashboard_status_for_truthful_paper = TradingBot.dashboard_status

    def _dashboard_status_truthful_wrapper_applied(self, *args, **kwargs):
        status = _orig_dashboard_status_for_truthful_paper(self, *args, **kwargs)
        try:
            if isinstance(status, dict):
                status["paper"] = self.compute_paper_equity()
        except Exception:
            pass
        return status

    TradingBot.dashboard_status = _dashboard_status_truthful_wrapper_applied
except Exception:
    pass


# --- truthful strategy perf dashboard wrapper ---
try:
    _prev_dashboard_status_for_strategy_perf = TradingBot.dashboard_status

    def _dashboard_status_strategy_perf_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_for_strategy_perf(self, *args, **kwargs)
        try:
            strategy_lab = status.get("strategy_lab") or {}
            summaries = strategy_lab.get("summaries") or []

            perf_by_strategy = {}
            for row in summaries:
                if row is None:
                    continue
                if not isinstance(row, dict):
                    try:
                        row = dict(row)
                    except Exception:
                        try:
                            row = {k: row[k] for k in row.keys()}
                        except Exception:
                            continue

                if (row.get("window_name") or "") != "all":
                    continue
                strategy_name = row.get("strategy_name")
                if not strategy_name:
                    continue
                bucket = perf_by_strategy.setdefault(strategy_name, {
                    "perf_trades": 0,
                    "perf_wins": 0,
                    "perf_pnl": 0.0,
                })
                bucket["perf_trades"] += int(row.get("trade_count") or 0)
                bucket["perf_wins"] += int(row.get("win_count") or 0)
                bucket["perf_pnl"] += float(row.get("realized_pnl") or 0.0)

            strategies = status.get("strategies") or {}
            for strategy_name, perf in perf_by_strategy.items():
                if strategy_name in strategies and isinstance(strategies[strategy_name], dict):
                    strategies[strategy_name]["perf_trades"] = perf["perf_trades"]
                    strategies[strategy_name]["perf_wins"] = perf["perf_wins"]
                    strategies[strategy_name]["perf_pnl"] = round(perf["perf_pnl"], 6)
        except Exception:
            pass
        return status

    TradingBot.dashboard_status = _dashboard_status_strategy_perf_wrapper_applied
except Exception:
    pass


# --- truthful expiry settlement loop wrapper ---
try:
    from app.strategy_lab.outcomes import settle_expired_shadow_positions
    _prev_run_strategies_for_expiry = TradingBot.run_strategies

    async def _run_strategies_expiry_settlement_wrapper_applied(self, *args, **kwargs):
        try:
            settle_expired_shadow_positions(getattr(self, "spot_prices", {}) or {})
        except Exception:
            pass
        return await _prev_run_strategies_for_expiry(self, *args, **kwargs)

    TradingBot.run_strategies = _run_strategies_expiry_settlement_wrapper_applied
except Exception:
    pass


# --- dedicated per-strategy live flag endpoint ---
from fastapi import Request

@app.post("/api/strategy-live/{name}")
async def update_strategy_live(name: str, request: Request):
    body = await request.json()
    strategy_map = getattr(bot, "strategy_states", None) or getattr(bot, "strategies", None)

    if not strategy_map or name not in strategy_map:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {name}")

    if "live_enabled" not in body:
        raise HTTPException(status_code=422, detail="Missing live_enabled")

    s = strategy_map[name]
    s.live_enabled = bool(body["live_enabled"])

    if hasattr(bot, "log"):
        bot.log(f"Strategy {name}: enabled={getattr(s, 'enabled', None)}, live_enabled={getattr(s, 'live_enabled', None)}")

    return {
        "ok": True,
        "name": name,
        "enabled": bool(getattr(s, "enabled", False)),
        "live_enabled": bool(getattr(s, "live_enabled", False)),
    }


# --- simple live gate wrappers: live_enabled always mirrors enabled ---
try:
    _simple_live_gate_wrappers_installed = True

    _orig_tradingbot_init_simple_live_gate = TradingBot.__init__

    def _tradingbot_init_simple_live_gate(self, *args, **kwargs):
        _orig_tradingbot_init_simple_live_gate(self, *args, **kwargs)
        try:
            for state in (getattr(self, "strategy_states", {}) or {}).values():
                state.live_enabled = bool(getattr(state, "enabled", False))
        except Exception:
            pass

    TradingBot.__init__ = _tradingbot_init_simple_live_gate

    _orig_update_strategy_simple_live_gate = TradingBot.update_strategy

    def _update_strategy_simple_live_gate(self, name, payload):
        result = _orig_update_strategy_simple_live_gate(self, name, payload)
        try:
            state = self.strategy_states[name]
            state.live_enabled = bool(getattr(state, "enabled", False))
            self.log(f"Strategy {name}: enabled={state.enabled}, live_enabled={state.live_enabled} (mirrored)")
        except Exception:
            pass
        return result

    TradingBot.update_strategy = _update_strategy_simple_live_gate
except Exception:
    pass


# --- hard live guardrails ---
try:
    import os
    import sqlite3
    from datetime import datetime, timedelta, timezone

    _live_guardrail_wrappers_installed = True

    def _guard_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _guard_float(name: str, default: float) -> float:
        raw = os.getenv(name)
        try:
            return float(raw) if raw is not None else float(default)
        except Exception:
            return float(default)

    def _guard_int(name: str, default: int) -> int:
        raw = os.getenv(name)
        try:
            return int(raw) if raw is not None else int(default)
        except Exception:
            return int(default)

    def _paper_guardrail_snapshot(bot):
        db_path = "data/strategy_lab.db"
        start = float(getattr(bot, "paper_starting_balance", 1000.0) or 1000.0)
        reset_at = getattr(bot, "paper_last_reset_at", None)

        min_closed = _guard_int("KALSHI_LIVE_REQUIRE_CLOSED_TRADES", 30)
        min_win_rate = _guard_float("KALSHI_LIVE_REQUIRE_WIN_RATE", 0.55)
        require_positive_realized = _guard_bool("KALSHI_LIVE_REQUIRE_POSITIVE_REALIZED", True)
        daily_dd_limit_pct = _guard_float("KALSHI_LIVE_DAILY_DD_LIMIT_PCT", 0.03)
        weekly_dd_limit_pct = _guard_float("KALSHI_LIVE_WEEKLY_DD_LIMIT_PCT", 0.06)
        max_consec_losses = _guard_int("KALSHI_LIVE_MAX_CONSEC_LOSSES", 4)
        max_open_exposure_pct = _guard_float("KALSHI_LIVE_MAX_OPEN_EXPOSURE_PCT", 0.08)
        max_open_trades = _guard_int("KALSHI_LIVE_MAX_OPEN_TRADES", 6)

        now = datetime.now(timezone.utc)
        day_cutoff = (now - timedelta(days=1)).isoformat()
        week_cutoff = (now - timedelta(days=7)).isoformat()

        trade_where = "WHERE closed_at IS NOT NULL"
        trade_params = []
        open_where = ""
        open_params = []

        if reset_at:
            trade_where += " AND closed_at >= ?"
            trade_params.append(reset_at)
            open_where = "WHERE opened_at >= ?"
            open_params.append(reset_at)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            summary_row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS closed_trades,
                    COALESCE(SUM(realized_pnl), 0.0) AS realized_pnl,
                    COALESCE(SUM(CASE WHEN COALESCE(realized_pnl, 0) > 0 THEN 1 ELSE 0 END), 0) AS wins,
                    COALESCE(SUM(CASE WHEN COALESCE(realized_pnl, 0) < 0 THEN 1 ELSE 0 END), 0) AS losses
                FROM shadow_trades
                {trade_where}
                """,
                trade_params,
            ).fetchone()

            open_row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS open_trades,
                    COALESCE(SUM(entry_price * qty), 0.0) AS open_cost_basis
                FROM shadow_positions
                {open_where}
                """,
                open_params,
            ).fetchone()

            daily_row = conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0.0) AS daily_realized
                FROM shadow_trades
                WHERE closed_at IS NOT NULL AND closed_at >= ?
                """,
                (day_cutoff,),
            ).fetchone()

            weekly_row = conn.execute(
                """
                SELECT COALESCE(SUM(realized_pnl), 0.0) AS weekly_realized
                FROM shadow_trades
                WHERE closed_at IS NOT NULL AND closed_at >= ?
                """,
                (week_cutoff,),
            ).fetchone()

            recent_rows = conn.execute(
                f"""
                SELECT realized_pnl
                FROM shadow_trades
                {trade_where}
                ORDER BY closed_at DESC
                LIMIT 20
                """,
                trade_params,
            ).fetchall()
        finally:
            conn.close()

        closed_trades = int((summary_row["closed_trades"] or 0) if summary_row else 0)
        realized_pnl = float((summary_row["realized_pnl"] or 0.0) if summary_row else 0.0)
        wins = int((summary_row["wins"] or 0) if summary_row else 0)
        losses = int((summary_row["losses"] or 0) if summary_row else 0)
        win_rate = (wins / closed_trades) if closed_trades else 0.0

        open_trades = int((open_row["open_trades"] or 0) if open_row else 0)
        open_cost_basis = float((open_row["open_cost_basis"] or 0.0) if open_row else 0.0)
        daily_realized = float((daily_row["daily_realized"] or 0.0) if daily_row else 0.0)
        weekly_realized = float((weekly_row["weekly_realized"] or 0.0) if weekly_row else 0.0)

        consec_losses = 0
        for row in recent_rows:
            pnl = float(row["realized_pnl"] or 0.0)
            if pnl < 0:
                consec_losses += 1
            elif pnl > 0:
                break

        try:
            paper = bot.compute_paper_equity()
            current_equity = float((paper or {}).get("equity") or start)
        except Exception:
            current_equity = start

        open_exposure_pct = (open_cost_basis / current_equity) if current_equity > 0 else 0.0

        notes = []
        ready = True

        if closed_trades < min_closed:
            ready = False
            notes.append(f"Need more closed trades since reset: {closed_trades}/{min_closed}.")
        if require_positive_realized and realized_pnl <= 0:
            ready = False
            notes.append(f"Realized PnL since reset is not positive: {realized_pnl:.4f}.")
        if closed_trades >= 10 and win_rate < min_win_rate:
            ready = False
            notes.append(f"Win rate {win_rate:.2%} is below threshold {min_win_rate:.2%}.")
        if open_trades > max_open_trades:
            ready = False
            notes.append(f"Open trades {open_trades} exceeds cap {max_open_trades}.")
        if daily_realized <= -(daily_dd_limit_pct * start):
            ready = False
            notes.append(f"Daily realized drawdown {daily_realized:.4f} breached limit {-daily_dd_limit_pct*start:.4f}.")
        if weekly_realized <= -(weekly_dd_limit_pct * start):
            ready = False
            notes.append(f"Weekly realized drawdown {weekly_realized:.4f} breached limit {-weekly_dd_limit_pct*start:.4f}.")
        if consec_losses >= max_consec_losses:
            ready = False
            notes.append(f"Consecutive losing trades {consec_losses} reached cap {max_consec_losses}.")
        if open_exposure_pct > max_open_exposure_pct:
            ready = False
            notes.append(f"Open exposure {open_exposure_pct:.2%} exceeds cap {max_open_exposure_pct:.2%}.")

        return {
            "ready_for_live": ready,
            "notes": notes or ["Live guardrails currently pass."],
            "summary": {
                "closed_trades": closed_trades,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "realized_pnl": realized_pnl,
                "open_trades": open_trades,
                "open_cost_basis": open_cost_basis,
                "open_exposure_pct": open_exposure_pct,
                "daily_realized": daily_realized,
                "weekly_realized": weekly_realized,
                "consecutive_losses": consec_losses,
                "current_equity": current_equity,
            },
            "thresholds": {
                "min_closed_trades": min_closed,
                "min_win_rate": min_win_rate,
                "require_positive_realized": require_positive_realized,
                "daily_dd_limit_pct": daily_dd_limit_pct,
                "weekly_dd_limit_pct": weekly_dd_limit_pct,
                "max_consecutive_losses": max_consec_losses,
                "max_open_exposure_pct": max_open_exposure_pct,
                "max_open_trades": max_open_trades,
            },
        }

    _orig_dashboard_status_live_guardrails = TradingBot.dashboard_status

    def _dashboard_status_live_guardrails(self, *args, **kwargs):
        status = _orig_dashboard_status_live_guardrails(self, *args, **kwargs)
        try:
            if isinstance(status, dict):
                status["live_guardrails"] = _paper_guardrail_snapshot(self)
        except Exception as e:
            if isinstance(status, dict):
                status["live_guardrails"] = {"ready_for_live": False, "notes": [f"guardrail_status_error: {e}"]}
        return status

    TradingBot.dashboard_status = _dashboard_status_live_guardrails

    _prev_run_strategies_live_guardrails = TradingBot.run_strategies

    async def _run_strategies_live_guardrails(self, *args, **kwargs):
        try:
            guard = _paper_guardrail_snapshot(self)
            auto_halt = _guard_bool("KALSHI_LIVE_AUTO_HALT", True)
            is_live = str(getattr(self, "mode", "paper")).lower() == "live"
            is_armed = bool(getattr(self, "live_armed", False))

            if auto_halt and is_live and is_armed and not guard.get("ready_for_live", False):
                self.live_armed = False
                self.mode = "paper"
                if hasattr(self, "log"):
                    self.log("Live guardrails triggered: switching to paper and disarming live.")
                    for note in guard.get("notes", []):
                        self.log(f"Live guardrail: {note}")
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Live guardrail evaluation failed: {e}")

        return await _prev_run_strategies_live_guardrails(self, *args, **kwargs)

    TradingBot.run_strategies = _run_strategies_live_guardrails
except Exception:
    pass


# --- request-level live guardrail enforcement middleware ---
try:
    import json
    from fastapi import Request
    from starlette.responses import JSONResponse

    _live_guardrail_request_middleware_installed = True

    @app.middleware("http")
    async def live_guardrail_request_middleware(request: Request, call_next):
        body_bytes = await request.body()

        async def receive():
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        request = Request(request.scope, receive)

        if request.method == "POST" and request.url.path in {"/api/mode", "/api/arm-live"}:
            try:
                payload = json.loads(body_bytes.decode("utf-8")) if body_bytes else {}
            except Exception:
                payload = {}

            wants_live = request.url.path == "/api/mode" and str(payload.get("mode", "")).lower() == "live"
            wants_arm = request.url.path == "/api/arm-live" and bool(payload.get("armed", False)) is True

            if wants_live or wants_arm:
                try:
                    guard = _paper_guardrail_snapshot(bot)
                except Exception as e:
                    return JSONResponse(
                        status_code=409,
                        content={
                            "ok": False,
                            "error": "live_guardrail_evaluation_failed",
                            "detail": str(e),
                        },
                    )

                if not guard.get("ready_for_live", False):
                    return JSONResponse(
                        status_code=409,
                        content={
                            "ok": False,
                            "error": "live_guardrails_blocked",
                            "notes": guard.get("notes", []),
                            "summary": guard.get("summary", {}),
                            "thresholds": guard.get("thresholds", {}),
                        },
                    )

        return await call_next(request)
except Exception:
    pass


# --- research foundation: dynamic vol, operator settings application, market observation recording ---
try:
    import math
    import os
    from collections import deque
    from datetime import datetime, timezone
    from app.strategy_lab.operator_settings import apply_operator_settings_to_bot
    from app.strategy_lab.research_store import init_research_store, record_market_observations, get_research_store_stats

    _research_foundation_wrappers_installed = True

    def _dynamic_annual_vol(self, symbol: str, fallback: float = 0.60) -> float:
        try:
            hist = (getattr(self, "spot_history", {}) or {}).get(symbol) or deque()
            points = [(ts, float(px)) for ts, px in hist if px is not None and float(px) > 0]
            if len(points) < 20:
                return float(fallback)

            returns = []
            prev = None
            for _, px in points:
                if prev is not None and prev > 0 and px > 0 and px != prev:
                    returns.append(math.log(px / prev))
                prev = px

            if len(returns) < 10:
                return float(fallback)

            mean = sum(returns) / len(returns)
            var = sum((r - mean) ** 2 for r in returns) / max(1, (len(returns) - 1))
            if var <= 0:
                return float(fallback)

            loop_seconds = float(getattr(self, "research_loop_seconds_estimate", 15.0) or 15.0)
            intervals_per_year = (365.0 * 24.0 * 3600.0) / max(1.0, loop_seconds)
            vol = math.sqrt(var) * math.sqrt(intervals_per_year)

            min_vol = float(os.getenv("KALSHI_DYNAMIC_VOL_MIN", "0.20"))
            max_vol = float(os.getenv("KALSHI_DYNAMIC_VOL_MAX", "2.50"))
            vol = max(min_vol, min(max_vol, vol))
            return round(vol, 6)
        except Exception:
            return float(fallback)

    TradingBot._dynamic_annual_vol = _dynamic_annual_vol

    def _record_spot_history(self):
        now = datetime.now(timezone.utc).isoformat()
        spot_history = getattr(self, "spot_history", None)
        if not isinstance(spot_history, dict):
            return
        for symbol, px in (getattr(self, "spot_prices", {}) or {}).items():
            try:
                px = float(px or 0.0)
            except Exception:
                continue
            if px <= 0:
                continue
            bucket = spot_history.setdefault(symbol, deque(maxlen=self.spot_history_maxlen))
            bucket.append((now, px))

    TradingBot._record_spot_history = _record_spot_history

    _orig_tradingbot_init_research_foundation = TradingBot.__init__

    def _tradingbot_init_research_foundation(self, *args, **kwargs):
        _orig_tradingbot_init_research_foundation(self, *args, **kwargs)

        self.spot_history_maxlen = int(os.getenv("KALSHI_SPOT_HISTORY_MAXLEN", "720"))
        self.research_loop_seconds_estimate = float(os.getenv("KALSHI_LOOP_SECONDS_ESTIMATE", "15"))
        self.spot_history = {
            "BTC": deque(maxlen=self.spot_history_maxlen),
            "ETH": deque(maxlen=self.spot_history_maxlen),
            "SOL": deque(maxlen=self.spot_history_maxlen),
        }
        self.effective_operator_overrides = {"applied": [], "unsupported": [], "raw": {}}

        try:
            init_research_store()
        except Exception as e:
            self.log(f"Research store init failed: {e}")

        try:
            self._record_spot_history()
        except Exception:
            pass

        try:
            apply_operator_settings_to_bot(self)
            self.log("Applied operator settings at startup.")
        except Exception as e:
            self.log(f"Operator settings startup apply failed: {e}")

    TradingBot.__init__ = _tradingbot_init_research_foundation

    _prev_run_strategies_research_foundation = TradingBot.run_strategies

    async def _run_strategies_research_foundation(self, *args, **kwargs):
        try:
            self._record_spot_history()
        except Exception as e:
            self.log(f"Spot history record failed: {e}")

        try:
            apply_operator_settings_to_bot(self)
        except Exception as e:
            self.log(f"Operator settings apply failed: {e}")

        return await _prev_run_strategies_research_foundation(self, *args, **kwargs)

    TradingBot.run_strategies = _run_strategies_research_foundation

    _prev_refresh_markets_research_foundation = TradingBot.refresh_markets

    async def _refresh_markets_research_foundation(self, *args, **kwargs):
        result = await _prev_refresh_markets_research_foundation(self, *args, **kwargs)

        try:
            rows = []
            ts = datetime.now(timezone.utc).isoformat()
            for ticker, snapshot in (getattr(self, "market_snapshots", {}) or {}).items():
                asset = getattr(snapshot, "spot_symbol", None)
                spot = None
                if asset:
                    try:
                        spot = float((getattr(self, "spot_prices", {}) or {}).get(asset) or 0.0)
                    except Exception:
                        spot = None

                rows.append({
                    "ts": ts,
                    "ticker": ticker,
                    "asset": asset,
                    "market_family": getattr(snapshot, "market_family", None),
                    "status": "active",
                    "floor_strike": getattr(snapshot, "floor_strike", None),
                    "cap_strike": getattr(snapshot, "cap_strike", None),
                    "threshold": getattr(snapshot, "threshold", None),
                    "direction": getattr(snapshot, "direction", None),
                    "yes_bid": getattr(snapshot, "yes_bid", None),
                    "yes_ask": getattr(snapshot, "yes_ask", None),
                    "no_bid": getattr(snapshot, "no_bid", None),
                    "no_ask": getattr(snapshot, "no_ask", None),
                    "mid": getattr(snapshot, "mid", None),
                    "spread": getattr(snapshot, "spread", None),
                    "open_interest": getattr(snapshot, "open_interest", None),
                    "volume": getattr(snapshot, "volume", None),
                    "close_time": (
                        getattr(snapshot, "raw_close_time", None).isoformat()
                        if getattr(snapshot, "raw_close_time", None) is not None
                        else None
                    ),
                    "spot": spot,
                    "fair_yes": getattr(snapshot, "fair_yes", None),
                    "edge": getattr(snapshot, "edge", None),
                    "distance_from_spot_pct": getattr(snapshot, "distance_from_spot_pct", None),
                })

            if rows:
                inserted = record_market_observations(rows)
                setattr(self, "last_research_rows_inserted", inserted)
        except Exception as e:
            self.log(f"Research observation capture failed: {e}")

        return result

    TradingBot.refresh_markets = _refresh_markets_research_foundation

    _prev_dashboard_status_research_foundation = TradingBot.dashboard_status

    def _dashboard_status_research_foundation(self, *args, **kwargs):
        status = _prev_dashboard_status_research_foundation(self, *args, **kwargs)
        try:
            if isinstance(status, dict):
                status["model_meta"] = {
                    "dynamic_annual_vol_btc": self._dynamic_annual_vol("BTC", fallback=0.60),
                    "dynamic_vol_history_points_btc": len((getattr(self, "spot_history", {}) or {}).get("BTC", [])),
                    "execution_fee_estimate": float(os.getenv("KALSHI_TAKER_FEE_ESTIMATE", "0.015")),
                    "min_execution_edge": float(os.getenv("KALSHI_MIN_EXECUTION_EDGE", "0.02")),
                }
                status["operator_overrides"] = getattr(self, "effective_operator_overrides", {})
                stats = get_research_store_stats()
                stats["last_rows_inserted"] = int(getattr(self, "last_research_rows_inserted", 0) or 0)
                stats["last_market_count_for_capture"] = int(getattr(self, "last_market_count_for_capture", 0) or 0)
                stats["last_snapshot_count_for_capture"] = int(getattr(self, "last_snapshot_count_for_capture", 0) or 0)
                stats["last_market_rows_built_for_capture"] = int(getattr(self, "last_market_rows_built_for_capture", 0) or 0)
                stats["last_snapshot_rows_built_for_capture"] = int(getattr(self, "last_snapshot_rows_built_for_capture", 0) or 0)
                status["research_store"] = stats
        except Exception as e:
            if isinstance(status, dict):
                status["model_meta"] = {"error": str(e)}
        return status

    TradingBot.dashboard_status = _dashboard_status_research_foundation
except Exception:
    pass


# --- lazy research runtime bootstrap for existing bot instances ---
try:
    from collections import deque
    from app.strategy_lab.research_store import init_research_store

    _research_runtime_bootstrap_installed = True

    def _ensure_research_runtime_bootstrap(self):
        if not hasattr(self, "spot_history") or not isinstance(getattr(self, "spot_history", None), dict):
            maxlen = int(getattr(self, "spot_history_maxlen", 720) or 720)
            self.spot_history_maxlen = maxlen
            self.spot_history = {
                "BTC": deque(maxlen=maxlen),
                "ETH": deque(maxlen=maxlen),
                "SOL": deque(maxlen=maxlen),
            }

        if not hasattr(self, "effective_operator_overrides") or not isinstance(getattr(self, "effective_operator_overrides", None), dict):
            self.effective_operator_overrides = {"applied": [], "unsupported": [], "raw": {}}

        try:
            init_research_store()
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Lazy research store init failed: {e}")

    TradingBot._ensure_research_runtime_bootstrap = _ensure_research_runtime_bootstrap

    _prev_run_strategies_runtime_bootstrap = TradingBot.run_strategies
    async def _run_strategies_runtime_bootstrap(self, *args, **kwargs):
        try:
            self._ensure_research_runtime_bootstrap()
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Runtime bootstrap failed in run_strategies: {e}")
        return await _prev_run_strategies_runtime_bootstrap(self, *args, **kwargs)
    TradingBot.run_strategies = _run_strategies_runtime_bootstrap

    _prev_refresh_markets_runtime_bootstrap = TradingBot.refresh_markets
    async def _refresh_markets_runtime_bootstrap(self, *args, **kwargs):
        try:
            self._ensure_research_runtime_bootstrap()
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Runtime bootstrap failed in refresh_markets: {e}")
        return await _prev_refresh_markets_runtime_bootstrap(self, *args, **kwargs)
    TradingBot.refresh_markets = _refresh_markets_runtime_bootstrap

    _prev_dashboard_status_runtime_bootstrap = TradingBot.dashboard_status
    def _dashboard_status_runtime_bootstrap(self, *args, **kwargs):
        try:
            self._ensure_research_runtime_bootstrap()
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Runtime bootstrap failed in dashboard_status: {e}")
        return _prev_dashboard_status_runtime_bootstrap(self, *args, **kwargs)
    TradingBot.dashboard_status = _dashboard_status_runtime_bootstrap
except Exception:
    pass


# --- explicit market observation capture helpers ---
try:
    from datetime import datetime, timezone
    from app.strategy_lab.research_store import record_market_observations, get_research_store_stats

    _explicit_market_capture_installed = True

    def _capture_market_observations(self):
        rows = []
        ts = datetime.now(timezone.utc).isoformat()

        snapshots = getattr(self, "market_snapshots", None) or {}
        for ticker, snapshot in snapshots.items():
            asset = getattr(snapshot, "spot_symbol", None)
            try:
                spot = float((getattr(self, "spot_prices", {}) or {}).get(asset) or 0.0) if asset else None
            except Exception:
                spot = None

            rows.append({
                "ts": ts,
                "ticker": ticker,
                "asset": asset,
                "market_family": getattr(snapshot, "market_family", None),
                "status": "active",
                "floor_strike": getattr(snapshot, "floor_strike", None),
                "cap_strike": getattr(snapshot, "cap_strike", None),
                "threshold": getattr(snapshot, "threshold", None),
                "direction": getattr(snapshot, "direction", None),
                "yes_bid": getattr(snapshot, "yes_bid", None),
                "yes_ask": getattr(snapshot, "yes_ask", None),
                "no_bid": getattr(snapshot, "no_bid", None),
                "no_ask": getattr(snapshot, "no_ask", None),
                "mid": getattr(snapshot, "mid", None),
                "spread": getattr(snapshot, "spread", None),
                "open_interest": getattr(snapshot, "open_interest", None),
                "volume": getattr(snapshot, "volume", None),
                "close_time": (
                    getattr(snapshot, "raw_close_time", None).isoformat()
                    if getattr(snapshot, "raw_close_time", None) is not None else None
                ),
                "spot": spot,
                "fair_yes": getattr(snapshot, "fair_yes", None),
                "edge": getattr(snapshot, "edge", None),
                "distance_from_spot_pct": getattr(snapshot, "distance_from_spot_pct", None),
            })

        if not rows:
            return 0

        inserted = record_market_observations(rows)
        setattr(self, "last_research_rows_inserted", inserted)
        return inserted

    TradingBot._capture_market_observations = _capture_market_observations

    _prev_refresh_markets_explicit_capture = TradingBot.refresh_markets
    async def _refresh_markets_explicit_capture(self, *args, **kwargs):
        result = await _prev_refresh_markets_explicit_capture(self, *args, **kwargs)
        try:
            inserted = self._capture_market_observations()
            if hasattr(self, "log") and inserted:
                self.log(f"Research capture: inserted {inserted} market observations.")
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Research capture failed in refresh_markets: {e}")
        return result
    TradingBot.refresh_markets = _refresh_markets_explicit_capture

    _prev_dashboard_status_explicit_capture = TradingBot.dashboard_status
    def _dashboard_status_explicit_capture(self, *args, **kwargs):
        try:
            stats = get_research_store_stats()
            if int(stats.get("observation_count") or 0) == 0:
                self._capture_market_observations()
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Research capture failed in dashboard_status: {e}")
        return _prev_dashboard_status_explicit_capture(self, *args, **kwargs)
    TradingBot.dashboard_status = _dashboard_status_explicit_capture
except Exception:
    pass

# --- market runtime observability + warm bootstrap ---
try:
    from datetime import datetime, timezone

    _market_runtime_observability_installed = True

    def _mark_market_refresh_started(self):
        self.last_market_refresh_started_at = datetime.now(timezone.utc).isoformat()

    def _mark_market_refresh_finished(self):
        self.last_market_refresh_finished_at = datetime.now(timezone.utc).isoformat()
        try:
            self.last_market_snapshot_count = len(getattr(self, "market_snapshots", {}) or {})
        except Exception:
            self.last_market_snapshot_count = 0

    TradingBot._mark_market_refresh_started = _mark_market_refresh_started
    TradingBot._mark_market_refresh_finished = _mark_market_refresh_finished

    _prev_refresh_markets_runtime_obs = TradingBot.refresh_markets
    async def _refresh_markets_runtime_obs(self, *args, **kwargs):
        try:
            self._mark_market_refresh_started()
        except Exception:
            pass

        result = await _prev_refresh_markets_runtime_obs(self, *args, **kwargs)

        try:
            self._mark_market_refresh_finished()
        except Exception:
            pass

        return result
    TradingBot.refresh_markets = _refresh_markets_runtime_obs

    async def _warm_bootstrap_market_snapshots(self):
        try:
            snapshots = getattr(self, "market_snapshots", None) or {}
            if snapshots:
                return False
            await self.refresh_markets()
            return True
        except Exception as e:
            try:
                self.log(f"Warm bootstrap refresh failed: {e}")
            except Exception:
                pass
            return False

    TradingBot._warm_bootstrap_market_snapshots = _warm_bootstrap_market_snapshots

    _prev_dashboard_status_market_obs = TradingBot.dashboard_status
    def _dashboard_status_market_obs_async(self, *args, **kwargs):
        status = _prev_dashboard_status_market_obs(self, *args, **kwargs)

        try:
            if isinstance(status, dict):
                status["market_runtime"] = {
                    "snapshot_count": len(getattr(self, "market_snapshots", {}) or {}),
                    "last_market_snapshot_count": int(getattr(self, "last_market_snapshot_count", 0) or 0),
                    "last_market_refresh_started_at": getattr(self, "last_market_refresh_started_at", None),
                    "last_market_refresh_finished_at": getattr(self, "last_market_refresh_finished_at", None),
                    "last_research_rows_inserted": int(getattr(self, "last_research_rows_inserted", 0) or 0),
                    "last_market_count_for_capture": int(getattr(self, "last_market_count_for_capture", 0) or 0),
                    "last_snapshot_count_for_capture": int(getattr(self, "last_snapshot_count_for_capture", 0) or 0),
                    "last_market_rows_built_for_capture": int(getattr(self, "last_market_rows_built_for_capture", 0) or 0),
                    "last_snapshot_rows_built_for_capture": int(getattr(self, "last_snapshot_rows_built_for_capture", 0) or 0),
                    "spot_price_keys": sorted(list((getattr(self, "spot_prices", {}) or {}).keys()))[:10],
                    "watchlist_count": len(getattr(self, "cached_crypto_tickers", []) or []),
                }
        except Exception:
            pass

        return status

    TradingBot.dashboard_status = _dashboard_status_market_obs_async
except Exception:
    pass


# --- crypto watchlist self-heal + runtime diagnostics ---
try:
    _crypto_watchlist_self_heal_installed = True

    def _current_crypto_watchlist_count(self):
        candidates = [
            getattr(self, "crypto_watchlist", None),
            getattr(self, "cached_crypto_watchlist", None),
            getattr(self, "market_watchlist", None),
            getattr(self, "watchlist", None),
        ]
        for c in candidates:
            if isinstance(c, (list, tuple, set)):
                return len(c)
        return 0

    def _set_crypto_watchlist_if_possible(self, tickers):
        tickers = [t for t in (tickers or []) if t]
        if not tickers:
            return 0

        assigned = False
        for attr in ["crypto_watchlist", "cached_crypto_watchlist", "market_watchlist", "watchlist"]:
            cur = getattr(self, attr, None)
            if cur is None or isinstance(cur, (list, tuple, set)):
                try:
                    setattr(self, attr, list(tickers))
                    assigned = True
                except Exception:
                    pass

        if assigned:
            try:
                self.last_watchlist_rebuild_count = len(tickers)
            except Exception:
                pass
            return len(tickers)
        return 0

    def _rebuild_crypto_watchlist_from_any_cache(self):
        candidates = []

        for attr in [
            "cached_crypto_market_tickers",
            "crypto_market_tickers",
            "market_tickers",
            "all_market_tickers",
        ]:
            val = getattr(self, attr, None)
            if isinstance(val, (list, tuple, set)):
                candidates.extend([x for x in val if isinstance(x, str)])

        for attr in [
            "cached_crypto_markets",
            "market_cache",
            "markets_cache",
            "cached_markets",
        ]:
            val = getattr(self, attr, None)
            if isinstance(val, dict):
                candidates.extend([k for k in val.keys() if isinstance(k, str)])
                for v in val.values():
                    if isinstance(v, dict):
                        t = v.get("ticker")
                        if isinstance(t, str):
                            candidates.append(t)

        # Keep only crypto Kalshi-style tickers
        filtered = []
        seen = set()
        for t in candidates:
            if not isinstance(t, str):
                continue
            if not (t.startswith("KXBTC-") or t.startswith("KXETH-") or t.startswith("KXSOL-")):
                continue
            if t in seen:
                continue
            seen.add(t)
            filtered.append(t)

        return self._set_crypto_watchlist_if_possible(filtered)

    TradingBot._current_crypto_watchlist_count = _current_crypto_watchlist_count
    TradingBot._set_crypto_watchlist_if_possible = _set_crypto_watchlist_if_possible
    TradingBot._rebuild_crypto_watchlist_from_any_cache = _rebuild_crypto_watchlist_from_any_cache

    _prev_refresh_markets_watchlist_heal = TradingBot.refresh_markets
    async def _refresh_markets_watchlist_heal(self, *args, **kwargs):
        try:
            before_ct = self._current_crypto_watchlist_count()
            self.last_watchlist_count_before_refresh = before_ct
            if before_ct == 0:
                rebuilt = self._rebuild_crypto_watchlist_from_any_cache()
                self.last_watchlist_rebuild_count = int(rebuilt or 0)
                if hasattr(self, "log"):
                    self.log(f"Watchlist self-heal attempted: rebuilt={rebuilt}")
        except Exception as e:
            if hasattr(self, "log"):
                self.log(f"Watchlist self-heal failed: {e}")

        result = await _prev_refresh_markets_watchlist_heal(self, *args, **kwargs)

        try:
            self.last_watchlist_count_after_refresh = self._current_crypto_watchlist_count()
        except Exception:
            pass

        return result
    TradingBot.refresh_markets = _refresh_markets_watchlist_heal

    _prev_dashboard_status_watchlist_heal = TradingBot.dashboard_status
    def _dashboard_status_watchlist_heal(self, *args, **kwargs):
        status = _prev_dashboard_status_watchlist_heal(self, *args, **kwargs)
        try:
            if isinstance(status, dict):
                mr = status.get("market_runtime") or {}
                mr["watchlist_count_before_refresh"] = int(getattr(self, "last_watchlist_count_before_refresh", 0) or 0)
                mr["watchlist_rebuild_count"] = int(getattr(self, "last_watchlist_rebuild_count", 0) or 0)
                mr["watchlist_count_after_refresh"] = int(getattr(self, "last_watchlist_count_after_refresh", 0) or 0)
                status["market_runtime"] = mr
        except Exception:
            pass
        return status
    TradingBot.dashboard_status = _dashboard_status_watchlist_heal
except Exception:
    pass

@app.get("/api/debug/cached-tickers")
async def api_debug_cached_tickers():
    tickers = list(getattr(bot, "cached_crypto_tickers", []) or [])
    return {
        "ok": True,
        "count": len(tickers),
        "sample": tickers[:25],
        "watch_cursor": int(getattr(bot, "watch_cursor", 0) or 0),
    }

@app.post("/api/debug/force-market-refresh")
async def api_debug_force_market_refresh():
    before_tickers = len(getattr(bot, "cached_crypto_tickers", []) or [])
    before_snapshots = len(getattr(bot, "market_snapshots", {}) or {})
    await bot.refresh_markets()
    after_tickers = len(getattr(bot, "cached_crypto_tickers", []) or [])
    after_snapshots = len(getattr(bot, "market_snapshots", {}) or {})
    return {
        "ok": True,
        "before_tickers": before_tickers,
        "after_tickers": after_tickers,
        "before_snapshots": before_snapshots,
        "after_snapshots": after_snapshots,
        "watch_cursor": int(getattr(bot, "watch_cursor", 0) or 0),
    }


@app.get("/api/debug/refresh-pipeline")
async def api_debug_refresh_pipeline():
    result = {
        "ok": True,
        "cached_ticker_count_before": len(getattr(bot, "cached_crypto_tickers", []) or []),
        "watch_cursor_before": int(getattr(bot, "watch_cursor", 0) or 0),
        "market_snapshot_count_before": len(getattr(bot, "market_snapshots", {}) or {}),
    }

    cached = list(getattr(bot, "cached_crypto_tickers", []) or [])
    cursor = int(getattr(bot, "watch_cursor", 0) or 0)
    result["selected_window_preview"] = cached[cursor:cursor+10]

    try:
        raw = await bot._fetch_cached_crypto_markets()
        result["raw_market_count"] = len(raw or [])
        result["raw_market_sample"] = [
            {
                "ticker": (m.get("ticker") if isinstance(m, dict) else None),
                "status": (m.get("status") if isinstance(m, dict) else None),
                "yes_bid": (m.get("yes_bid") if isinstance(m, dict) else None),
                "yes_ask": (m.get("yes_ask") if isinstance(m, dict) else None),
                "no_bid": (m.get("no_bid") if isinstance(m, dict) else None),
                "no_ask": (m.get("no_ask") if isinstance(m, dict) else None),
                "close_time": (m.get("close_time") if isinstance(m, dict) else None),
                "can_close_early": (m.get("can_close_early") if isinstance(m, dict) else None),
            }
            for m in (raw or [])[:5]
            if isinstance(m, dict)
        ]
    except Exception as e:
        result["raw_market_error"] = repr(e)

    result["watch_cursor_after_fetch"] = int(getattr(bot, "watch_cursor", 0) or 0)

    try:
        await bot.refresh_markets()
        result["refresh_ok"] = True
    except Exception as e:
        result["refresh_ok"] = False
        result["refresh_error"] = repr(e)

    snapshots = getattr(bot, "market_snapshots", {}) or {}
    result["market_snapshot_count_after"] = len(snapshots)
    result["market_snapshot_sample"] = list(snapshots.keys())[:10]

    return result

@app.get("/api/debug/market-decision/{ticker}")
async def api_debug_market_decision(ticker: str):
    assert bot.session is not None

    result = {
        "ok": True,
        "ticker": ticker,
        "spot_prices": dict(getattr(bot, "spot_prices", {}) or {}),
        "cached_ticker_count": len(getattr(bot, "cached_crypto_tickers", []) or []),
        "watch_cursor": int(getattr(bot, "watch_cursor", 0) or 0),
    }

    try:
        data = await bot.kalshi_client.get_json(bot.session, f"/markets?tickers={ticker}")
        raw_markets = data.get("markets", []) or []
        result["raw_market_count"] = len(raw_markets)
        raw = raw_markets[0] if raw_markets else None
        result["raw_market"] = raw
    except Exception as e:
        result["raw_fetch_error"] = repr(e)
        return result

    if not raw:
        result["stop_reason"] = "no_raw_market"
        return result

    try:
        normalized = bot._normalize_market(raw)
        result["normalized"] = normalized
    except Exception as e:
        result["normalize_error"] = repr(e)
        return result

    if not normalized:
        result["stop_reason"] = "normalize_returned_none"
        return result

    try:
        classified = bot._classify_market(normalized)
        result["classified"] = classified
    except Exception as e:
        result["classify_error"] = repr(e)
        return result

    try:
        eligible, reason = bot._evaluate_market_eligibility(classified)
        result["eligible"] = eligible
        result["eligibility_reason"] = reason
    except Exception as e:
        result["eligibility_error"] = repr(e)
        return result

    try:
        snapshot = bot._classified_to_snapshot(classified)
        result["snapshot_ok"] = snapshot is not None
        result["snapshot_dict"] = snapshot.to_dict() if snapshot is not None else None
    except Exception as e:
        result["snapshot_error"] = repr(e)

    return result


# --- BTC range-bucket eligibility window wrapper ---
try:
    import os
    from datetime import datetime, timezone

    _orig_evaluate_market_eligibility_btc_range_window = TradingBot._evaluate_market_eligibility
    _evaluate_market_eligibility_btc_range_window_wrapper_applied = True

    def _evaluate_market_eligibility_btc_range_window_wrapper_applied(self, classified):
        eligible, reason = _orig_evaluate_market_eligibility_btc_range_window(self, classified)

        try:
            if eligible:
                return eligible, reason

            if reason != "too_far_to_expiry":
                return eligible, reason

            if (classified or {}).get("asset") != "BTC":
                return eligible, reason

            if (classified or {}).get("market_family") != "range_bucket":
                return eligible, reason

            close_dt = (classified or {}).get("close_dt")
            if close_dt is None:
                return eligible, reason

            hours_to_expiry = (close_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0

            min_hours = float(os.getenv("BTC_RANGE_BUCKET_MIN_HOURS_TO_EXPIRY", "0.05"))
            max_hours = float(os.getenv("BTC_RANGE_BUCKET_MAX_HOURS_TO_EXPIRY", "24.0"))

            try:
                classified["hours_to_expiry"] = round(hours_to_expiry, 4)
                classified["eligibility_min_hours"] = min_hours
                classified["eligibility_max_hours"] = max_hours
            except Exception:
                pass

            if min_hours <= hours_to_expiry <= max_hours:
                return True, "eligible"
        except Exception:
            pass

        return eligible, reason

    TradingBot._evaluate_market_eligibility = _evaluate_market_eligibility_btc_range_window_wrapper_applied
except Exception:
    pass


# --- side-aware market dashboard wrapper ---
try:
    _prev_dashboard_status_side_aware_market = TradingBot.dashboard_status
    _dashboard_status_side_aware_market_wrapper_applied = True

    def _dashboard_status_side_aware_market_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_side_aware_market(self, *args, **kwargs)

        try:
            snapshots = getattr(self, "market_snapshots", {}) or {}
            markets = status.get("markets") or []

            for row in markets:
                ticker = row.get("ticker")
                snapshot = snapshots.get(ticker)
                if snapshot is None:
                    continue

                yes_edge = getattr(snapshot, "execution_edge_yes", None)
                no_edge = getattr(snapshot, "execution_edge_no", None)

                yes_spread = None
                no_spread = None
                try:
                    if getattr(snapshot, "yes_bid", None) is not None and getattr(snapshot, "yes_ask", None) is not None:
                        yes_spread = round(float(snapshot.yes_ask) - float(snapshot.yes_bid), 4)
                    if getattr(snapshot, "no_bid", None) is not None and getattr(snapshot, "no_ask", None) is not None:
                        no_spread = round(float(snapshot.no_ask) - float(snapshot.no_bid), 4)
                except Exception:
                    pass

                candidates = []
                if yes_edge is not None:
                    candidates.append(("yes", float(yes_edge), yes_spread))
                if no_edge is not None:
                    candidates.append(("no", float(no_edge), no_spread))

                best_side = None
                best_exec_edge = None
                best_side_spread = None
                if candidates:
                    best_side, best_exec_edge, best_side_spread = max(candidates, key=lambda x: x[1])

                row["execution_edge_yes"] = round(float(yes_edge), 4) if yes_edge is not None else None
                row["execution_edge_no"] = round(float(no_edge), 4) if no_edge is not None else None
                row["yes_spread"] = yes_spread
                row["no_spread"] = no_spread
                row["best_side"] = best_side
                row["best_exec_edge"] = round(float(best_exec_edge), 4) if best_exec_edge is not None else None
                row["best_side_spread"] = best_side_spread

            status["markets"] = sorted(
                markets,
                key=lambda x: float(x.get("best_exec_edge") or -999.0),
                reverse=True,
            )

            positive = [m for m in status["markets"] if (m.get("best_exec_edge") or 0.0) > 0]
            status["market_side_summary"] = {
                "positive_best_edge_count": len(positive),
                "top_positive_tickers": [
                    {
                        "ticker": m.get("ticker"),
                        "best_side": m.get("best_side"),
                        "best_exec_edge": m.get("best_exec_edge"),
                        "yes_spread": m.get("yes_spread"),
                        "no_spread": m.get("no_spread"),
                    }
                    for m in positive[:10]
                ],
            }
        except Exception:
            pass

        return status

    TradingBot.dashboard_status = _dashboard_status_side_aware_market_wrapper_applied
except Exception:
    pass


# --- force enrichment of visible market rows at status time ---
try:
    _prev_dashboard_status_force_enrichment = TradingBot.dashboard_status
    _dashboard_status_force_enrichment_wrapper_applied = True

    def _dashboard_status_force_enrichment_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_force_enrichment(self, *args, **kwargs)

        try:
            snapshots = getattr(self, "market_snapshots", {}) or {}
            markets = status.get("markets") or []

            enriched_count = 0
            missing_after = 0

            for row in markets:
                ticker = row.get("ticker")
                snapshot = snapshots.get(ticker)
                if snapshot is None:
                    continue

                try:
                    if getattr(snapshot, "fair_yes", None) is None:
                        self._enrich_with_fair_value(snapshot)
                        if getattr(snapshot, "fair_yes", None) is not None:
                            enriched_count += 1
                except Exception:
                    pass

                fair_yes = getattr(snapshot, "fair_yes", None)
                edge = getattr(snapshot, "edge", None)
                exec_yes = getattr(snapshot, "execution_edge_yes", None)
                exec_no = getattr(snapshot, "execution_edge_no", None)

                if fair_yes is None:
                    missing_after += 1

                row["fair_yes"] = round(float(fair_yes), 4) if fair_yes is not None else None
                row["edge"] = round(float(edge), 4) if edge is not None else None
                row["execution_edge_yes"] = round(float(exec_yes), 4) if exec_yes is not None else None
                row["execution_edge_no"] = round(float(exec_no), 4) if exec_no is not None else None
                row["rationale"] = getattr(snapshot, "rationale", "") or row.get("rationale") or ""

                candidates = []
                if exec_yes is not None:
                    candidates.append(("yes", float(exec_yes)))
                if exec_no is not None:
                    candidates.append(("no", float(exec_no)))

                if candidates:
                    best_side, best_edge = max(candidates, key=lambda x: x[1])
                    row["best_side"] = best_side
                    row["best_exec_edge"] = round(best_edge, 4)
                else:
                    row["best_side"] = None
                    row["best_exec_edge"] = None

            status["markets"] = sorted(
                markets,
                key=lambda x: float(x.get("best_exec_edge") or -999.0),
                reverse=True,
            )

            status["status_enrichment"] = {
                "rows_seen": len(markets),
                "rows_enriched_now": enriched_count,
                "rows_missing_fair_yes_after": missing_after,
            }
        except Exception:
            pass

        return status

    TradingBot.dashboard_status = _dashboard_status_force_enrichment_wrapper_applied
except Exception:
    pass


# --- side-aware intent wrapper for crypto_lag family ---
try:
    _orig_generate_intent_side_aware = TradingBot._generate_intent
    _generate_intent_side_aware_wrapper_applied = True

    def _generate_intent_side_aware_wrapper_applied(self, strategy, snapshot):
        intent = _orig_generate_intent_side_aware(self, strategy, snapshot)

        try:
            strategy_name = getattr(strategy, "name", "") or ""
            if strategy_name not in {"crypto_lag", "crypto_lag_safe", "crypto_lag_aggressive"}:
                return intent

            fair_yes = getattr(snapshot, "fair_yes", None)
            yes_ask = getattr(snapshot, "yes_ask", None)
            no_ask = getattr(snapshot, "no_ask", None)
            exec_yes = getattr(snapshot, "execution_edge_yes", None)
            exec_no = getattr(snapshot, "execution_edge_no", None)
            spread = getattr(snapshot, "spread", None)

            min_edge = float(getattr(strategy, "min_edge", 0.0) or 0.0)
            max_spread = float(getattr(strategy, "max_spread", 1.0) or 1.0)

            if spread is not None and float(spread) > max_spread:
                return intent

            candidates = []
            if exec_yes is not None and yes_ask is not None:
                candidates.append(("yes", float(exec_yes), float(yes_ask)))
            if exec_no is not None and no_ask is not None:
                candidates.append(("no", float(exec_no), float(no_ask)))

            if not candidates:
                return intent

            best_side, best_edge, best_price = max(candidates, key=lambda x: x[1])

            try:
                setattr(snapshot, "best_side", best_side)
                setattr(snapshot, "best_exec_edge", best_edge)
            except Exception:
                pass

            if best_edge < min_edge:
                return None

            size_multiplier = 1.0
            if min_edge > 0:
                size_multiplier = max(0.25, min(2.0, best_edge / min_edge))

            ticket = float(getattr(strategy, "max_ticket_dollars", 25.0) or 25.0)
            contract_budget = max(1.0, ticket * size_multiplier)
            qty = max(1, int(contract_budget / max(best_price, 0.01)))

            rationale = (
                f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware "
                f"{best_side.upper()} edge {best_edge:.4f}; "
                f"fair_yes={float(fair_yes):.4f} "
                f"exec_yes={float(exec_yes):.4f} " if exec_yes is not None else
                f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware {best_side.upper()} edge {best_edge:.4f}; "
            )

            if best_side == "yes":
                return {
                    "action": "buy",
                    "side": "yes",
                    "ticker": snapshot.ticker,
                    "price": round(best_price, 4),
                    "quantity": qty,
                    "edge": round(best_edge, 4),
                    "rationale": (
                        f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware YES edge {best_edge:.4f}; "
                        f"fair_yes={float(fair_yes):.4f} "
                        f"exec_yes={float(exec_yes):.4f} "
                        f"exec_no={float(exec_no):.4f}" if exec_no is not None else
                        f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware YES edge {best_edge:.4f}; "
                        f"fair_yes={float(fair_yes):.4f} "
                        f"exec_yes={float(exec_yes):.4f}"
                    ),
                }

            return {
                "action": "buy",
                "side": "no",
                "ticker": snapshot.ticker,
                "price": round(best_price, 4),
                "quantity": qty,
                "edge": round(best_edge, 4),
                "rationale": (
                    f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware NO edge {best_edge:.4f}; "
                    f"fair_yes={float(fair_yes):.4f} "
                    f"exec_yes={float(exec_yes):.4f} "
                    f"exec_no={float(exec_no):.4f}" if exec_yes is not None else
                    f"{getattr(snapshot, 'spot_symbol', 'UNK')} side-aware NO edge {best_edge:.4f}; "
                    f"fair_yes={float(fair_yes):.4f} "
                    f"exec_no={float(exec_no):.4f}"
                ),
            }
        except Exception:
            return intent

        return intent

    TradingBot._generate_intent = _generate_intent_side_aware_wrapper_applied
except Exception:
    pass


# --- strategy scorecard wrapper from recent trades ---
try:
    import sqlite3
    from collections import defaultdict

    _prev_dashboard_status_strategy_scorecard = TradingBot.dashboard_status
    _dashboard_status_strategy_scorecard_wrapper_applied = True

    def _dashboard_status_strategy_scorecard_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_strategy_scorecard(self, *args, **kwargs)

        try:
            db_path = "data/trades.db"
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row

            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            candidate_table = None
            for name in ["trades", "paper_trades", "fills", "orders"]:
                if name in tables:
                    candidate_table = name
                    break

            scorecard = {
                "table_used": candidate_table,
                "totals": {},
                "by_side": {},
                "note": None,
            }

            if not candidate_table:
                scorecard["note"] = "No trade-like table found in data/trades.db"
                status["strategy_scorecard"] = scorecard
                conn.close()
                return status

            rows = conn.execute(f"SELECT * FROM {candidate_table} ORDER BY rowid DESC LIMIT 500").fetchall()
            conn.close()

            normalized = []
            for r in rows:
                d = dict(r)

                strategy = d.get("strategy") or d.get("strategy_name") or ""
                mode = d.get("mode") or ""
                side = (d.get("side") or "").lower()
                ticker = d.get("ticker") or ""
                status_text = (d.get("status") or "").lower()
                msg = d.get("message") or ""

                pnl = d.get("realized_pnl")
                if pnl is None:
                    pnl = d.get("pnl")
                if pnl is None:
                    pnl = 0.0
                try:
                    pnl = float(pnl or 0.0)
                except Exception:
                    pnl = 0.0

                if strategy != "crypto_lag":
                    continue
                if mode not in {"paper", ""}:
                    continue

                normalized.append({
                    "strategy": strategy,
                    "mode": mode or "paper",
                    "side": side or "unknown",
                    "ticker": ticker,
                    "status": status_text,
                    "message": msg,
                    "pnl": pnl,
                })

            def summarize(items):
                total = len(items)
                wins = sum(1 for x in items if x["pnl"] > 0)
                losses = sum(1 for x in items if x["pnl"] < 0)
                flat = total - wins - losses
                pnl = round(sum(x["pnl"] for x in items), 4)
                avg = round((pnl / total), 4) if total else 0.0
                win_rate = round((wins / total), 4) if total else 0.0
                return {
                    "trades": total,
                    "wins": wins,
                    "losses": losses,
                    "flat": flat,
                    "win_rate": win_rate,
                    "realized_pnl": pnl,
                    "avg_pnl_per_trade": avg,
                }

            scorecard["totals"] = summarize(normalized)

            by_side = defaultdict(list)
            for item in normalized:
                by_side[item["side"]].append(item)

            scorecard["by_side"] = {side: summarize(items) for side, items in by_side.items()}
            status["strategy_scorecard"] = scorecard
        except Exception as e:
            status["strategy_scorecard"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_strategy_scorecard_wrapper_applied
except Exception:
    pass


# --- robust multi-source strategy scorecard wrapper ---
try:
    import sqlite3
    from collections import defaultdict

    _prev_dashboard_status_strategy_scorecard_v2 = TradingBot.dashboard_status
    _dashboard_status_strategy_scorecard_v2_wrapper_applied = True

    def _safe_float(v, default=0.0):
        try:
            if v in (None, "", "null"):
                return default
            return float(v)
        except Exception:
            return default

    def _safe_int(v, default=0):
        try:
            if v in (None, "", "null"):
                return default
            return int(v)
        except Exception:
            return default

    def _table_columns(conn, table_name):
        try:
            return [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
        except Exception:
            return []

    def _find_realized_side_rows():
        candidate_sources = []
        db_candidates = [
            "data/trades.db",
            "data/strategy_lab.db",
            "data/paper.db",
        ]

        for db_path in db_candidates:
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

                for table in tables:
                    cols = set(_table_columns(conn, table))
                    if not cols:
                        continue

                    has_strategy = any(c in cols for c in ["strategy", "strategy_name"])
                    has_side = "side" in cols
                    has_pnl = any(c in cols for c in ["realized_pnl", "pnl", "profit_loss"])
                    if not (has_strategy and has_side and has_pnl):
                        continue

                    # Pull recent rows only; we only need scorecard context.
                    rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT 1000").fetchall()

                    normalized = []
                    for r in rows:
                        d = dict(r)
                        strategy = d.get("strategy") or d.get("strategy_name") or ""
                        if strategy != "crypto_lag":
                            continue

                        side = str(d.get("side") or "").lower().strip()
                        if side not in {"yes", "no"}:
                            continue

                        pnl = d.get("realized_pnl")
                        if pnl is None:
                            pnl = d.get("pnl")
                        if pnl is None:
                            pnl = d.get("profit_loss")
                        pnl = _safe_float(pnl, 0.0)

                        status = str(d.get("status") or "").lower()
                        message = str(d.get("message") or "").lower()

                        closed_markers = [
                            d.get("closed_at"),
                            d.get("settled_at"),
                            d.get("resolved_at"),
                            d.get("exited_at"),
                        ]
                        has_closed_ts = any(x not in (None, "", "null") for x in closed_markers)

                        looks_closed = (
                            has_closed_ts
                            or status in {"closed", "settled", "resolved", "expired", "exited", "complete", "completed"}
                            or "settled" in message
                            or "expired" in message
                            or "closed" in message
                            or "exit" in message
                            or abs(pnl) > 1e-12
                        )

                        if not looks_closed:
                            continue

                        normalized.append({
                            "side": side,
                            "pnl": pnl,
                            "ticker": d.get("ticker"),
                            "status": status,
                        })

                    if normalized:
                        candidate_sources.append({
                            "db_path": db_path,
                            "table": table,
                            "rows": normalized,
                            "count": len(normalized),
                            "nonzero_pnl_count": sum(1 for x in normalized if abs(x["pnl"]) > 1e-12),
                        })

                conn.close()
            except Exception:
                continue

        if not candidate_sources:
            return None

        # Prefer source with most nonzero realized pnl rows, then most rows.
        candidate_sources.sort(
            key=lambda s: (s["nonzero_pnl_count"], s["count"]),
            reverse=True,
        )
        return candidate_sources[0]

    def _summarize_rows(rows):
        total = len(rows)
        wins = sum(1 for x in rows if x["pnl"] > 0)
        losses = sum(1 for x in rows if x["pnl"] < 0)
        flat = total - wins - losses
        realized_pnl = round(sum(x["pnl"] for x in rows), 4)
        avg = round((realized_pnl / total), 4) if total else 0.0
        win_rate = round((wins / total), 4) if total else 0.0
        return {
            "trades": total,
            "wins": wins,
            "losses": losses,
            "flat": flat,
            "win_rate": win_rate,
            "realized_pnl": realized_pnl,
            "avg_pnl_per_trade": avg,
        }

    def _dashboard_status_strategy_scorecard_v2_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_strategy_scorecard_v2(self, *args, **kwargs)

        try:
            strategies = status.get("strategies") or {}
            s = strategies.get("crypto_lag") or {}

            perf_trades = _safe_int(s.get("perf_trades"), 0)
            perf_wins = _safe_int(s.get("perf_wins"), 0)
            perf_pnl = round(_safe_float(s.get("perf_pnl"), 0.0), 4)
            perf_losses = max(perf_trades - perf_wins, 0)
            perf_win_rate = round((perf_wins / perf_trades), 4) if perf_trades else 0.0
            perf_avg = round((perf_pnl / perf_trades), 4) if perf_trades else 0.0

            scorecard = {
                "totals": {
                    "trades": perf_trades,
                    "wins": perf_wins,
                    "losses": perf_losses,
                    "flat": 0 if perf_trades else 0,
                    "win_rate": perf_win_rate,
                    "realized_pnl": perf_pnl,
                    "avg_pnl_per_trade": perf_avg,
                },
                "by_side": {},
                "sources": {
                    "totals_source": "status.strategies.crypto_lag.perf_*",
                    "by_side_source": None,
                },
                "note": None,
            }

            realized_source = _find_realized_side_rows()
            if realized_source is None:
                scorecard["note"] = "No realized side-level source found; totals still reflect the strategy perf counters."
                status["strategy_scorecard"] = scorecard
                return status

            by_side = defaultdict(list)
            for row in realized_source["rows"]:
                by_side[row["side"]].append(row)

            scorecard["by_side"] = {
                side: _summarize_rows(rows)
                for side, rows in by_side.items()
            }
            scorecard["sources"]["by_side_source"] = {
                "db_path": realized_source["db_path"],
                "table": realized_source["table"],
                "row_count_used": realized_source["count"],
                "nonzero_pnl_count": realized_source["nonzero_pnl_count"],
            }

            # Extra sanity signals
            scorecard["side_trade_balance"] = {
                "yes_trades": len(by_side.get("yes", [])),
                "no_trades": len(by_side.get("no", [])),
            }

            status["strategy_scorecard"] = scorecard
        except Exception as e:
            status["strategy_scorecard"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_strategy_scorecard_v2_wrapper_applied
except Exception:
    pass


# --- final row-safe perf + analytics wrapper ---
try:
    import sqlite3
    from app.strategy_lab.analytics_store import (
        ensure_trade_analytics_table,
        backfill_from_shadow_trades,
        get_trade_analytics_summary,
    )

    _prev_dashboard_status_trade_analytics = TradingBot.dashboard_status
    _dashboard_status_trade_analytics_wrapper_applied = True

    def _row_to_plain_dict(v):
        if isinstance(v, dict):
            return v
        if hasattr(v, "keys"):
            try:
                return {k: v[k] for k in v.keys()}
            except Exception:
                try:
                    return dict(v)
                except Exception:
                    return {}
        try:
            return dict(v)
        except Exception:
            return {}

    def _dashboard_status_trade_analytics_wrapper_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_trade_analytics(self, *args, **kwargs)

        # 1) Canonical realized trade analytics from closed shadow trades
        try:
            ensure_trade_analytics_table()
            backfill = backfill_from_shadow_trades("crypto_lag")
            analytics = get_trade_analytics_summary("crypto_lag")
            analytics["backfill"] = backfill
            status["trade_analytics"] = analytics
        except Exception as e:
            status["trade_analytics"] = {"error": repr(e)}

        # 2) Final safe strategy perf overwrite from summaries (all window)
        try:
            conn = sqlite3.connect("data/strategy_lab.db")
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT strategy_name, trade_count, win_count, realized_pnl
                FROM summaries
                WHERE window_name = 'all'
                """
            ).fetchall()
            conn.close()

            aggregate = {}
            for raw in rows:
                row = _row_to_plain_dict(raw)
                name = str(row.get("strategy_name") or "").strip()
                if not name:
                    continue
                if name not in aggregate:
                    aggregate[name] = {"trades": 0, "wins": 0, "pnl": 0.0}
                aggregate[name]["trades"] += int(row.get("trade_count") or 0)
                aggregate[name]["wins"] += int(row.get("win_count") or 0)
                aggregate[name]["pnl"] += float(row.get("realized_pnl") or 0.0)

            strategies = status.get("strategies") or {}
            for name, vals in aggregate.items():
                if name in strategies and isinstance(strategies[name], dict):
                    strategies[name]["perf_trades"] = int(vals["trades"])
                    strategies[name]["perf_wins"] = int(vals["wins"])
                    strategies[name]["perf_pnl"] = round(float(vals["pnl"]), 4)
            status["strategies"] = strategies
        except Exception as e:
            errs = status.get("status_wrapper_errors") or []
            errs.append(f"row_safe_perf_wrapper: {e!r}")
            status["status_wrapper_errors"] = errs

        return status

    TradingBot.dashboard_status = _dashboard_status_trade_analytics_wrapper_applied
except Exception:
    pass


# --- dict/object field helper ---
def _field(obj, name, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    try:
        return getattr(obj, name)
    except Exception:
        pass
    try:
        return obj[name]
    except Exception:
        return default


# --- canonical opportunity dataset layer ---
try:
    import json as _json
    from datetime import datetime, timezone
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab.opportunity_store import (
        ensure_opportunity_schema as _ensure_opportunity_schema,
        record_market_opportunities as _record_market_opportunities,
        list_unresolved_expired_opportunities as _list_unresolved_expired_opportunities,
        resolve_market_opportunity_outcomes as _resolve_market_opportunity_outcomes,
        summarize_market_opportunities as _summarize_market_opportunities,
        export_market_opportunities_csv as _export_market_opportunities_csv,
    )

    _ensure_opportunity_schema()
    _prev_refresh_markets_opportunity_dataset = TradingBot.refresh_markets
    _prev_dashboard_status_opportunity_dataset = TradingBot.dashboard_status
    _opportunity_dataset_layer_applied = True

    def _snapshot_to_opportunity_row(self, snapshot, batch_ts):
        asset = getattr(snapshot, "spot_symbol", None)
        close_dt = getattr(snapshot, "raw_close_time", None)

        spot = None
        try:
            if asset:
                spot = float((getattr(self, "spot_prices", {}) or {}).get(asset) or 0.0)
        except Exception:
            spot = None

        hours_to_expiry = None
        if close_dt is not None:
            try:
                hours_to_expiry = round((close_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0, 4)
            except Exception:
                hours_to_expiry = None

        exec_yes = getattr(snapshot, "execution_edge_yes", None)
        exec_no = getattr(snapshot, "execution_edge_no", None)

        best_side = None
        best_exec_edge = None
        if exec_yes is not None or exec_no is not None:
            if exec_no is None or (exec_yes is not None and float(exec_yes) >= float(exec_no)):
                best_side, best_exec_edge = "yes", exec_yes
            else:
                best_side, best_exec_edge = "no", exec_no

        extra = {
            "title": getattr(snapshot, "title", None),
            "category": getattr(snapshot, "category", None),
            "status": getattr(snapshot, "status", None),
        }

        return {
            "batch_ts": batch_ts,
            "ts": batch_ts,
            "ticker": getattr(snapshot, "ticker", None),
            "asset": asset,
            "market_family": getattr(snapshot, "market_family", None),
            "yes_bid": getattr(snapshot, "yes_bid", None),
            "yes_ask": getattr(snapshot, "yes_ask", None),
            "no_bid": getattr(snapshot, "no_bid", None),
            "no_ask": getattr(snapshot, "no_ask", None),
            "mid": getattr(snapshot, "mid", None),
            "spread": getattr(snapshot, "spread", None),
            "spot": spot,
            "fair_yes": getattr(snapshot, "fair_yes", None),
            "execution_edge_yes": exec_yes,
            "execution_edge_no": exec_no,
            "best_side": best_side,
            "best_exec_edge": best_exec_edge,
            "distance_from_spot_pct": getattr(snapshot, "distance_from_spot_pct", None),
            "hours_to_expiry": hours_to_expiry,
            "floor_strike": getattr(snapshot, "floor_strike", None),
            "cap_strike": getattr(snapshot, "cap_strike", None),
            "threshold": getattr(snapshot, "threshold", None),
            "close_time": close_dt.isoformat() if close_dt is not None else None,
            "rationale": getattr(snapshot, "rationale", None),
            "extra_json": _json.dumps(extra, ensure_ascii=False),
        }

    async def _resolve_recent_market_opportunity_outcomes(self, limit=200):
        pending = _list_unresolved_expired_opportunities(limit=limit)
        if not pending or self.session is None:
            return {"pending": len(pending), "resolved": 0}

        tickers = sorted({str(r.get("ticker") or "").strip() for r in pending if r.get("ticker")})
        if not tickers:
            return {"pending": len(pending), "resolved": 0}

        fetched = {}
        for i in range(0, len(tickers), 10):
            chunk = tickers[i:i+10]
            try:
                data = await self.kalshi_client.get_json(self.session, "/markets?tickers=" + ",".join(chunk))
            except Exception:
                continue
            for m in (data.get("markets") or []):
                t = str(m.get("ticker") or "").strip()
                if t:
                    fetched[t] = m

        def _result_yes(raw):
            if raw is None:
                return None
            s = str(raw).strip().lower()
            if s in {"yes", "true", "1", "y"}:
                return 1
            if s in {"no", "false", "0", "n"}:
                return 0
            return None

        updates = []
        now_iso = datetime.now(timezone.utc).isoformat()
        resolved = 0

        for row in pending:
            market = fetched.get(str(row.get("ticker") or ""))
            if not market:
                continue

            raw_result = (
                market.get("result")
                or market.get("market_result")
                or market.get("settlement_result")
                or market.get("final_result")
            )
            result_yes = _result_yes(raw_result)
            if result_yes is None:
                continue

            yes_ask = float(row.get("yes_ask") or 0.0)
            no_ask = float(row.get("no_ask") or 0.0)
            pnl_if_yes = round(float(result_yes) - yes_ask, 4)
            pnl_if_no = round(float(1 - result_yes) - no_ask, 4)

            best_side = str(row.get("best_side") or "").lower().strip()
            best_side_correct = None
            best_side_realized_pnl = None
            if best_side == "yes":
                best_side_correct = 1 if result_yes == 1 else 0
                best_side_realized_pnl = pnl_if_yes
            elif best_side == "no":
                best_side_correct = 1 if result_yes == 0 else 0
                best_side_realized_pnl = pnl_if_no

            updates.append({
                "id": int(row["id"]),
                "market_result": str(raw_result),
                "result_yes": int(result_yes),
                "pnl_if_yes": pnl_if_yes,
                "pnl_if_no": pnl_if_no,
                "best_side_correct": best_side_correct,
                "best_side_realized_pnl": best_side_realized_pnl,
                "resolved_at": now_iso,
            })
            resolved += 1

        if updates:
            _resolve_market_opportunity_outcomes(updates)

        return {"pending": len(pending), "resolved": resolved}

    async def _refresh_markets_opportunity_dataset(self, *args, **kwargs):
        result = await _prev_refresh_markets_opportunity_dataset(self, *args, **kwargs)

        try:
            _ensure_opportunity_schema()
            batch_ts = datetime.now(timezone.utc).isoformat()
            snapshots = list((getattr(self, "market_snapshots", {}) or {}).values())
            rows = []

            for snapshot in snapshots:
                try:
                    self._enrich_with_fair_value(snapshot)
                except Exception:
                    pass
                row = self._snapshot_to_opportunity_row(snapshot, batch_ts)
                if row.get("ticker"):
                    rows.append(row)

            self.last_opportunity_batch_ts = batch_ts
            self.last_opportunity_rows_inserted = int(_record_market_opportunities(rows) or 0)
        except Exception as e:
            self.last_opportunity_rows_inserted = 0
            if hasattr(self, "log"):
                self.log(f"Opportunity dataset capture failed: {e!r}")

        try:
            outcome_result = await self._resolve_recent_market_opportunity_outcomes(limit=200)
            self.last_opportunity_rows_resolved = int(outcome_result.get("resolved", 0) or 0)
            self.last_opportunity_pending_to_resolve = int(outcome_result.get("pending", 0) or 0)
        except Exception as e:
            self.last_opportunity_rows_resolved = 0
            if hasattr(self, "log"):
                self.log(f"Opportunity outcome resolver failed: {e!r}")

        return result

    def _dashboard_status_opportunity_dataset(self, *args, **kwargs):
        status = _prev_dashboard_status_opportunity_dataset(self, *args, **kwargs)
        try:
            analytics = _summarize_market_opportunities(limit_recent=20)
            analytics["runtime"] = {
                "last_batch_ts": getattr(self, "last_opportunity_batch_ts", None),
                "rows_inserted_last_batch": int(getattr(self, "last_opportunity_rows_inserted", 0) or 0),
                "rows_resolved_last_batch": int(getattr(self, "last_opportunity_rows_resolved", 0) or 0),
                "pending_to_resolve": int(getattr(self, "last_opportunity_pending_to_resolve", 0) or 0),
            }
            status["opportunity_analytics"] = analytics
        except Exception as e:
            status["opportunity_analytics"] = {"error": repr(e)}
        return status

    TradingBot._snapshot_to_opportunity_row = _snapshot_to_opportunity_row
    TradingBot._resolve_recent_market_opportunity_outcomes = _resolve_recent_market_opportunity_outcomes
    TradingBot.refresh_markets = _refresh_markets_opportunity_dataset
    TradingBot.dashboard_status = _dashboard_status_opportunity_dataset

    @app.get("/api/debug/opportunity-analytics")
    async def api_debug_opportunity_analytics():
        return {"ok": True, "analytics": _summarize_market_opportunities(limit_recent=50)}

    @app.get("/api/debug/opportunity-csv", response_class=PlainTextResponse)
    async def api_debug_opportunity_csv(limit: int = 250):
        limit = max(1, min(limit, 5000))
        csv_text = _export_market_opportunities_csv(limit=limit)
        return PlainTextResponse(
            csv_text,
            headers={
                "Content-Disposition": f'attachment; filename="market-opportunities-{datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")}.csv"'
            },
        )

except Exception as _opportunity_dataset_exc:
    try:
        print(f"Opportunity dataset layer patch skipped: {_opportunity_dataset_exc!r}")
    except Exception:
        pass


# --- architect layer: authoritative consultant/debug surface + fee-correct enrichment ---
try:
    from fastapi.responses import JSONResponse, PlainTextResponse
    from app.strategy_lab.architect_layer import (
        build_architect_payload,
        render_consultant_report,
        distance_bucket_from_pct,
    )

    _architect_status_layer_applied = True

    def _architect_estimated_fee(price, contracts=1.0):
        try:
            p = float(price)
            c = float(contracts)
            if p <= 0 or c <= 0:
                return 0.0
            p = max(0.01, min(0.99, p))
            # General Kalshi fee schedule shape for scoring.
            # Actual fills should supersede this with returned fee_cost when available.
            return round(0.07 * c * p * (1.0 - p), 4)
        except Exception:
            return 0.0

    _prev_enrich_with_fair_value_architect = TradingBot._enrich_with_fair_value

    def _enrich_with_fair_value_architect(self, snapshot):
        _prev_enrich_with_fair_value_architect(self, snapshot)

        try:
            fair_yes = getattr(snapshot, "fair_yes", None)
            if fair_yes is None:
                return

            yes_ask = getattr(snapshot, "yes_ask", None)
            no_ask = getattr(snapshot, "no_ask", None)

            fee_yes = _architect_estimated_fee(yes_ask, 1.0) if yes_ask is not None else None
            fee_no = _architect_estimated_fee(no_ask, 1.0) if no_ask is not None else None

            setattr(snapshot, "entry_fee_yes", fee_yes)
            setattr(snapshot, "entry_fee_no", fee_no)

            exec_yes = None
            exec_no = None
            if yes_ask is not None:
                exec_yes = round(float(fair_yes) - float(yes_ask) - float(fee_yes or 0.0), 4)
            if no_ask is not None:
                exec_no = round((1.0 - float(fair_yes)) - float(no_ask) - float(fee_no or 0.0), 4)

            setattr(snapshot, "execution_edge_yes", exec_yes)
            setattr(snapshot, "execution_edge_no", exec_no)

            candidates = []
            if exec_yes is not None:
                candidates.append(("yes", exec_yes))
            if exec_no is not None:
                candidates.append(("no", exec_no))

            if candidates:
                best_side, best_edge = max(candidates, key=lambda x: x[1])
                setattr(snapshot, "best_side", best_side)
                setattr(snapshot, "best_exec_edge", round(float(best_edge), 4))
                setattr(snapshot, "edge", round(float(best_edge), 4))

            try:
                spot = float((getattr(self, "spot_prices", {}) or {}).get(getattr(snapshot, "spot_symbol", None)) or 0.0)
            except Exception:
                spot = 0.0

            try:
                low = getattr(snapshot, "floor_strike", None)
                high = getattr(snapshot, "cap_strike", None)
                if spot > 0 and low is not None and high is not None:
                    mid = (float(low) + float(high)) / 2.0
                    distance_pct = abs(mid - spot) / spot
                    setattr(snapshot, "distance_from_spot_pct", round(distance_pct, 6))
                    setattr(snapshot, "distance_bucket", distance_bucket_from_pct(distance_pct))
            except Exception:
                pass

            try:
                raw_close_time = getattr(snapshot, "raw_close_time", None)
                if raw_close_time is not None:
                    from datetime import datetime, timezone
                    hours = max((raw_close_time - datetime.now(timezone.utc)).total_seconds() / 3600.0, 0.0)
                    setattr(snapshot, "hours_to_expiry", round(hours, 4))
            except Exception:
                pass

        except Exception as e:
            try:
                self.health["architect_enrich_error"] = repr(e)
            except Exception:
                pass

    TradingBot._enrich_with_fair_value = _enrich_with_fair_value_architect

    _prev_dashboard_status_architect = TradingBot.dashboard_status

    def _dashboard_status_architect(self, *args, **kwargs):
        status = _prev_dashboard_status_architect(self, *args, **kwargs)

        try:
            rows = status.get("markets") or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if not row.get("distance_bucket"):
                    row["distance_bucket"] = distance_bucket_from_pct(row.get("distance_from_spot_pct"))
                if row.get("best_exec_edge") is None:
                    ey = row.get("execution_edge_yes")
                    en = row.get("execution_edge_no")
                    candidates = []
                    if ey is not None:
                        candidates.append(("yes", float(ey)))
                    if en is not None:
                        candidates.append(("no", float(en)))
                    if candidates:
                        best_side, best_edge = max(candidates, key=lambda x: x[1])
                        row["best_side"] = row.get("best_side") or best_side
                        row["best_exec_edge"] = round(float(best_edge), 4)

            status["architect"] = build_architect_payload(status)
        except Exception as e:
            status["architect"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_architect

    @app.get("/api/debug/consultant-bundle")
    async def api_debug_consultant_bundle():
        status = bot.dashboard_status()
        return JSONResponse({
            "ok": True,
            "architect": status.get("architect"),
            "status": status,
        })

    @app.get("/api/debug/consultant-report", response_class=PlainTextResponse)
    async def api_debug_consultant_report():
        status = bot.dashboard_status()
        return PlainTextResponse(render_consultant_report(status))

except Exception as _architect_patch_exc:
    try:
        TradingBot._architect_patch_error = repr(_architect_patch_exc)
    except Exception:
        pass


# --- row enrichment repair: keep visible rows aligned with snapshot/opportunity fields ---
try:
    from datetime import datetime, timezone

    _prev_dashboard_status_row_enrichment_repair = TradingBot.dashboard_status
    _dashboard_status_row_enrichment_repair_applied = True

    def _repair_distance_bucket(distance_pct):
        try:
            if distance_pct in (None, "", "null"):
                return "unknown"
            x = abs(float(distance_pct))
            if x <= 0.005:
                return "atm_0_0.5pct"
            if x <= 0.015:
                return "near_0.5_1.5pct"
            if x <= 0.03:
                return "mid_1.5_3pct"
            return "far_3pct_plus"
        except Exception:
            return "unknown"

    def _dashboard_status_row_enrichment_repair_applied(self, *args, **kwargs):
        status = _prev_dashboard_status_row_enrichment_repair(self, *args, **kwargs)

        repaired = 0
        rows = status.get("markets") or []
        snapshots = getattr(self, "market_snapshots", {}) or {}
        spot_prices = getattr(self, "spot_prices", {}) or {}

        for row in rows:
            if not isinstance(row, dict):
                continue

            ticker = row.get("ticker")
            snapshot = snapshots.get(ticker) if ticker else None
            if snapshot is None:
                continue

            touched = False

            def push_if_missing(key, value):
                nonlocal touched
                if row.get(key) in (None, "", "null", "unknown"):
                    if value not in (None, "", "null"):
                        row[key] = value
                        touched = True

            # direct snapshot fields
            for key in [
                "fair_yes",
                "execution_edge_yes",
                "execution_edge_no",
                "best_side",
                "best_exec_edge",
                "entry_fee_yes",
                "entry_fee_no",
                "rationale",
                "distance_from_spot_pct",
                "distance_bucket",
                "hours_to_expiry",
                "floor_strike",
                "cap_strike",
                "threshold",
                "direction",
                "market_family",
            ]:
                push_if_missing(key, getattr(snapshot, key, None))

            # compute missing distance_from_spot_pct from strikes + spot
            if row.get("distance_from_spot_pct") in (None, "", "null"):
                try:
                    asset = getattr(snapshot, "spot_symbol", None)
                    spot = float((spot_prices or {}).get(asset) or 0.0) if asset else 0.0
                    low = getattr(snapshot, "floor_strike", None)
                    high = getattr(snapshot, "cap_strike", None)
                    if spot > 0 and low is not None and high is not None:
                        mid = (float(low) + float(high)) / 2.0
                        row["distance_from_spot_pct"] = round(abs(mid - spot) / spot, 6)
                        touched = True
                except Exception:
                    pass

            # compute missing distance bucket
            if row.get("distance_bucket") in (None, "", "null", "unknown"):
                row["distance_bucket"] = _repair_distance_bucket(row.get("distance_from_spot_pct"))
                if row["distance_bucket"] != "unknown":
                    touched = True

            # compute missing hours_to_expiry
            if row.get("hours_to_expiry") in (None, "", "null"):
                try:
                    raw_close_time = getattr(snapshot, "raw_close_time", None)
                    if raw_close_time is not None:
                        hours = max((raw_close_time - datetime.now(timezone.utc)).total_seconds() / 3600.0, 0.0)
                        row["hours_to_expiry"] = round(hours, 4)
                        touched = True
                except Exception:
                    pass

            # if best side missing but exec edges exist, repair it
            if row.get("best_side") in (None, "", "null") or row.get("best_exec_edge") in (None, "", "null"):
                try:
                    ey = row.get("execution_edge_yes")
                    en = row.get("execution_edge_no")
                    candidates = []
                    if ey is not None:
                        candidates.append(("yes", float(ey)))
                    if en is not None:
                        candidates.append(("no", float(en)))
                    if candidates:
                        side, edge = max(candidates, key=lambda x: x[1])
                        row["best_side"] = side
                        row["best_exec_edge"] = round(float(edge), 4)
                        touched = True
                except Exception:
                    pass

            if touched:
                repaired += 1

        status["row_enrichment_repair"] = {
            "rows_seen": len(rows),
            "rows_repaired": repaired,
            "rows_with_entry_fee_yes": sum(1 for r in rows if isinstance(r, dict) and r.get("entry_fee_yes") not in (None, "", "null")),
            "rows_with_entry_fee_no": sum(1 for r in rows if isinstance(r, dict) and r.get("entry_fee_no") not in (None, "", "null")),
            "rows_with_distance_bucket": sum(1 for r in rows if isinstance(r, dict) and (r.get("distance_bucket") not in (None, "", "null", "unknown"))),
            "rows_with_hours_to_expiry": sum(1 for r in rows if isinstance(r, dict) and r.get("hours_to_expiry") not in (None, "", "null")),
        }

        # rebuild architect payload if present so consultant bundle reflects repaired rows
        try:
            if "architect" in status:
                from app.strategy_lab.architect_layer import build_architect_payload
                status["architect"] = build_architect_payload(status)
        except Exception as e:
            status["row_enrichment_repair"]["architect_rebuild_error"] = repr(e)

        return status

    TradingBot.dashboard_status = _dashboard_status_row_enrichment_repair_applied

except Exception as _row_enrichment_repair_exc:
    try:
        print(f"Row enrichment repair patch skipped: {_row_enrichment_repair_exc!r}")
    except Exception:
        pass


# --- architect exit-v2 + side-correct shadow engine wrapper ---
try:
    import os
    import sqlite3
    from datetime import datetime, timezone
    from app.strategy_lab import shadow_engine as _se

    ShadowEngine = _se.ShadowEngine
    _architect_exit_v2_shadow_wrapper_applied = True

    def _se_row_dict(v):
        if hasattr(_se, "_row_dict"):
            try:
                return _se._row_dict(v)
            except Exception:
                pass
        if isinstance(v, dict):
            return v
        if hasattr(v, "keys"):
            try:
                return {k: v[k] for k in v.keys()}
            except Exception:
                pass
        try:
            return dict(v)
        except Exception:
            return {}

    def _se_side(snapshot, fallback="yes"):
        side = str(getattr(snapshot, "best_side", None) or fallback or "").lower().strip()
        if side not in {"yes", "no"}:
            ey = float(getattr(snapshot, "execution_edge_yes", 0.0) or 0.0)
            en = float(getattr(snapshot, "execution_edge_no", 0.0) or 0.0)
            side = "yes" if ey >= en else "no"
        return side

    def _se_side_bid(snapshot, side):
        attr = "yes_bid" if side == "yes" else "no_bid"
        v = getattr(snapshot, attr, None)
        return None if v is None else float(v)

    def _se_side_ask(snapshot, side):
        attr = "yes_ask" if side == "yes" else "no_ask"
        v = getattr(snapshot, attr, None)
        return None if v is None else float(v)

    def _se_side_edge(snapshot, side):
        attr = "execution_edge_yes" if side == "yes" else "execution_edge_no"
        v = getattr(snapshot, attr, None)
        if v is None:
            v = getattr(snapshot, "best_exec_edge", None)
        if v is None:
            v = getattr(snapshot, "edge", None)
        return float(v or 0.0)

    def _se_side_spread(snapshot, side):
        ask = _se_side_ask(snapshot, side)
        bid = _se_side_bid(snapshot, side)
        if ask is not None and bid is not None:
            return round(float(ask) - float(bid), 4)
        return float(getattr(snapshot, "spread", 0.0) or 0.0)

    def _safe_iso_to_dt(v):
        if not v:
            return None
        try:
            raw = str(v)
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _open_shadow_trade_exit_v2(self, variant_name, snapshot, qty, entry_price, regime_name, side=None):
        if self._already_open(snapshot.ticker, variant_name):
            return False

        side = str(side or _se_side(snapshot, "yes")).lower().strip()
        if side not in {"yes", "no"}:
            side = "yes"

        entry_from_book = _se_side_ask(snapshot, side)
        entry_price = float(entry_from_book if entry_from_book not in (None, 0.0) else (entry_price or 0.0))
        if entry_price <= 0 or entry_price >= 1:
            return False

        entry_edge = _se_side_edge(snapshot, side)
        spread_at_entry = _se_side_spread(snapshot, side)
        mark0 = _se_side_bid(snapshot, side)
        if mark0 is None:
            mid = getattr(snapshot, "mid", None)
            mark0 = float(mid if mid is not None else entry_price)

        conn = _se.get_conn()
        now = _se._utc_now().isoformat()
        try:
            conn.execute(
                """
                INSERT INTO shadow_trades (
                    opened_at, strategy_name, variant_name, regime_name, ticker, side, qty,
                    entry_price, edge_at_entry, spread_at_entry, hours_to_expiry_at_entry,
                    distance_from_spot_pct_at_entry, max_favorable_excursion, max_adverse_excursion,
                    best_mark_price, worst_mark_price, entry_edge, edge_at_exit, spread_at_exit,
                    distance_from_spot_pct_at_exit, minutes_open, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    "crypto_lag",
                    variant_name,
                    regime_name,
                    snapshot.ticker,
                    side,
                    int(qty),
                    float(entry_price),
                    float(entry_edge),
                    float(spread_at_entry),
                    self._hours_to_expiry(snapshot),
                    float(getattr(snapshot, "distance_from_spot_pct", 0.0) or 0.0),
                    0.0,
                    0.0,
                    float(mark0),
                    float(mark0),
                    float(entry_edge),
                    None,
                    None,
                    None,
                    None,
                    _se.json_dumps({"rationale": getattr(snapshot, "rationale", ""), "entry_side": side}),
                ),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO shadow_positions (
                    ticker, variant_name, opened_at, strategy_name, regime_name, side, qty,
                    entry_price, last_mark_price, unrealized_pnl, best_mark_price, worst_mark_price,
                    entry_edge, last_edge, last_spread
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.ticker,
                    variant_name,
                    now,
                    "crypto_lag",
                    regime_name,
                    side,
                    int(qty),
                    float(entry_price),
                    float(mark0),
                    0.0,
                    float(mark0),
                    float(mark0),
                    float(entry_edge),
                    float(entry_edge),
                    float(spread_at_entry),
                ),
            )
            conn.commit()
            return True
        finally:
            conn.close()

    def _mark_positions_exit_v2(self):
        conn = _se.get_conn()
        try:
            rows = conn.execute("SELECT * FROM shadow_positions").fetchall()
            for row in rows:
                rowd = _se_row_dict(row)
                snapshot = self._market_store().get(rowd["ticker"])
                if not snapshot:
                    continue

                side = str(rowd.get("side") or "yes").lower().strip()
                if side not in {"yes", "no"}:
                    side = "yes"

                entry = float(rowd.get("entry_price") or 0.0)
                qty = int(rowd.get("qty") or 0)

                bid_mark = _se_side_bid(snapshot, side)
                if bid_mark is None:
                    mid = getattr(snapshot, "mid", None)
                    bid_mark = float(mid if mid is not None else entry)

                mark = float(bid_mark or entry)
                unrealized = (mark - entry) * qty

                best_mark = max(float(rowd.get("best_mark_price") or mark), mark)
                worst_mark = min(float(rowd.get("worst_mark_price") or mark), mark)

                current_edge = _se_side_edge(snapshot, side)
                spread_now = _se_side_spread(snapshot, side)

                conn.execute(
                    """
                    UPDATE shadow_positions
                    SET last_mark_price = ?, unrealized_pnl = ?, best_mark_price = ?, worst_mark_price = ?,
                        last_edge = ?, last_spread = ?
                    WHERE ticker = ? AND variant_name = ?
                    """,
                    (
                        mark,
                        unrealized,
                        best_mark,
                        worst_mark,
                        float(current_edge),
                        float(spread_now),
                        rowd["ticker"],
                        rowd["variant_name"],
                    ),
                )

                opened_dt = _safe_iso_to_dt(rowd.get("opened_at"))
                minutes_open = None
                if opened_dt is not None:
                    try:
                        minutes_open = max((_se._utc_now() - opened_dt).total_seconds() / 60.0, 0.0)
                    except Exception:
                        minutes_open = None

                conn.execute(
                    """
                    UPDATE shadow_trades
                    SET best_mark_price = ?, worst_mark_price = ?, edge_at_exit = ?, spread_at_exit = ?,
                        distance_from_spot_pct_at_exit = ?, minutes_open = ?
                    WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                    """,
                    (
                        float(best_mark),
                        float(worst_mark),
                        float(current_edge),
                        float(spread_now),
                        float(getattr(snapshot, "distance_from_spot_pct", 0.0) or 0.0),
                        None if minutes_open is None else round(float(minutes_open), 4),
                        rowd["ticker"],
                        rowd["variant_name"],
                    ),
                )

            conn.commit()
        finally:
            conn.close()

    def _should_exit_exit_v2(self, row, snapshot):
        rowd = _se_row_dict(row)
        side = str(rowd.get("side") or "yes").lower().strip()
        if side not in {"yes", "no"}:
            side = "yes"

        entry = float(rowd.get("entry_price") or 0.0)
        mark = _se_side_bid(snapshot, side)
        if mark is None:
            mid = getattr(snapshot, "mid", None)
            mark = float(mid if mid is not None else entry)
        else:
            mark = float(mark)

        current_edge = _se_side_edge(snapshot, side)
        spread_now = _se_side_spread(snapshot, side)
        hours = self._hours_to_expiry(snapshot) or 0.0

        entry_edge = rowd.get("entry_edge")
        if entry_edge in (None, "", "null"):
            entry_edge = rowd.get("edge_at_entry")
        entry_edge = float(entry_edge or 0.0)

        pnl_per_contract = mark - entry
        best_mark = float(rowd.get("best_mark_price") or mark)
        mfe = best_mark - entry

        rel_drop = float(os.getenv("SHADOW_EXIT_REL_EDGE_DROP", "0.65"))
        hysteresis = float(os.getenv("SHADOW_EXIT_EDGE_HYSTERESIS", "0.015"))
        profit_arm = float(os.getenv("SHADOW_EXIT_PROFIT_ARM", "0.05"))
        profit_keep_frac = float(os.getenv("SHADOW_EXIT_PROFIT_KEEP_FRAC", "0.35"))
        neg_edge_floor = float(os.getenv("SHADOW_EXIT_NEG_EDGE_FLOOR", "-0.025"))
        time_exit_hours = float(os.getenv("SHADOW_TIME_EXIT_HOURS", "0.33"))
        spread_blowout = float(os.getenv("SHADOW_SPREAD_BLOWOUT", "0.12"))

        if mfe >= profit_arm:
            keep_floor = max(0.01, mfe * profit_keep_frac)
            if pnl_per_contract <= keep_floor:
                return True, "profit_giveback_v2"

        if entry_edge > 0:
            deterioration_trigger = max(hysteresis, abs(entry_edge) * rel_drop)
            if (entry_edge - current_edge) >= deterioration_trigger:
                return True, "model_deterioration_v2"

        if current_edge <= neg_edge_floor:
            return True, "model_invalidated_v2"

        if hours <= time_exit_hours:
            return True, "time_exit_v2"

        if spread_now >= spread_blowout:
            return True, "spread_blowout_v2"

        return False, None

    def _close_shadow_trade_exit_v2(self, row, snapshot, reason):
        rowd = _se_row_dict(row)
        side = str(rowd.get("side") or "yes").lower().strip()
        if side not in {"yes", "no"}:
            side = "yes"

        conn = _se.get_conn()
        try:
            qty = int(rowd.get("qty") or 0)
            entry = float(rowd.get("entry_price") or 0.0)

            bid_exit = _se_side_bid(snapshot, side)
            if bid_exit is None or float(bid_exit) <= 0:
                return False

            exit_price = float(bid_exit)
            realized = (exit_price - entry) * qty
            now = _se._utc_now()

            conn.execute(
                """
                UPDATE shadow_trades
                SET closed_at = ?, exit_price = ?, realized_pnl = ?, close_reason = ?, outcome = ?,
                    edge_at_exit = ?, spread_at_exit = ?, distance_from_spot_pct_at_exit = ?, minutes_open = ?
                WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                """,
                (
                    now.isoformat(),
                    exit_price,
                    realized,
                    reason,
                    "win" if realized > 0 else ("loss" if realized < 0 else "flat"),
                    float(_se_side_edge(snapshot, side)),
                    float(_se_side_spread(snapshot, side)),
                    float(getattr(snapshot, "distance_from_spot_pct", 0.0) or 0.0),
                    (
                        max((now - _safe_iso_to_dt(rowd.get("opened_at"))).total_seconds() / 60.0, 0.0)
                        if _safe_iso_to_dt(rowd.get("opened_at")) is not None else None
                    ),
                    rowd["ticker"],
                    rowd["variant_name"],
                ),
            )
            conn.execute(
                "DELETE FROM shadow_positions WHERE ticker = ? AND variant_name = ?",
                (rowd["ticker"], rowd["variant_name"]),
            )
            conn.commit()
            return True
        finally:
            conn.close()

    ShadowEngine.open_shadow_trade = _open_shadow_trade_exit_v2
    ShadowEngine.mark_positions = _mark_positions_exit_v2
    ShadowEngine._should_exit = _should_exit_exit_v2
    ShadowEngine.close_shadow_trade = _close_shadow_trade_exit_v2

    _prev_dashboard_status_exit_v2_architect = TradingBot.dashboard_status

    def _dashboard_status_exit_v2_architect(self, *args, **kwargs):
        status = _prev_dashboard_status_exit_v2_architect(self, *args, **kwargs)

        exit_policy = {
            "version": "architect_exit_v2",
            "entry_side_truth": "best executable side persisted into shadow_trades/shadow_positions",
            "mark_truth": "yes_bid for YES / no_bid for NO",
            "early_exit_rules": {
                "profit_giveback_v2": {
                    "profit_arm": float(os.getenv("SHADOW_EXIT_PROFIT_ARM", "0.05")),
                    "profit_keep_frac": float(os.getenv("SHADOW_EXIT_PROFIT_KEEP_FRAC", "0.35")),
                },
                "model_deterioration_v2": {
                    "rel_edge_drop": float(os.getenv("SHADOW_EXIT_REL_EDGE_DROP", "0.65")),
                    "hysteresis": float(os.getenv("SHADOW_EXIT_EDGE_HYSTERESIS", "0.015")),
                },
                "model_invalidated_v2": {
                    "neg_edge_floor": float(os.getenv("SHADOW_EXIT_NEG_EDGE_FLOOR", "-0.025")),
                },
                "time_exit_v2": {
                    "hours": float(os.getenv("SHADOW_TIME_EXIT_HOURS", "0.33")),
                },
                "spread_blowout_v2": {
                    "spread": float(os.getenv("SHADOW_SPREAD_BLOWOUT", "0.12")),
                },
            },
            "removed_legacy_reasons": ["take_profit_10c", "stop_loss_6c", "edge_gone"],
        }

        close_reason_summary = []
        try:
            conn = sqlite3.connect("data/strategy_lab.db")
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT COALESCE(close_reason, 'unknown') AS close_reason,
                       COUNT(*) AS n,
                       ROUND(COALESCE(SUM(realized_pnl), 0), 4) AS pnl
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                GROUP BY 1
                ORDER BY n DESC, pnl ASC
                LIMIT 20
                """
            ).fetchall()
            conn.close()
            close_reason_summary = [dict(r) for r in rows]
        except Exception as e:
            close_reason_summary = [{"error": repr(e)}]

        status["exit_policy"] = exit_policy
        status["close_reason_summary"] = close_reason_summary

        try:
            architect = status.get("architect") or {}
            if isinstance(architect, dict):
                architect["exit_policy"] = exit_policy
                architect["close_reason_summary"] = close_reason_summary
                status["architect"] = architect
        except Exception:
            pass

        return status

    TradingBot.dashboard_status = _dashboard_status_exit_v2_architect

except Exception as _architect_exit_v2_exc:
    try:
        print(f"Architect exit-v2 patch skipped: {_architect_exit_v2_exc!r}")
    except Exception:
        pass


# --- architect post-exit-v2 cohort analytics ---
try:
    import json
    import sqlite3
    from datetime import datetime, timezone

    _architect_post_exit_v2_cohort_wrapper_applied = True

    def _arch_now_iso():
        return datetime.now(timezone.utc).isoformat()

    def _arch_safe_json_loads(v):
        try:
            if not v:
                return {}
            if isinstance(v, dict):
                return v
            return json.loads(v)
        except Exception:
            return {}

    def _arch_trade_meta_with_policy(meta_json, opened_at=None):
        d = _arch_safe_json_loads(meta_json)
        d.setdefault("architect_policy_version", "architect_exit_v2")
        d.setdefault("architect_policy_family", "shadow_exit_policy")
        if opened_at:
            d.setdefault("architect_policy_applied_at_open", str(opened_at))
        d.setdefault("architect_policy_recorded_at", _arch_now_iso())
        return json.dumps(d, sort_keys=True)

    try:
        from app.strategy_lab import shadow_engine as _arch_se
        _ArchShadowEngine = _arch_se.ShadowEngine

        _orig_open_shadow_trade_arch_cohort = _ArchShadowEngine.open_shadow_trade

        def _open_shadow_trade_arch_cohort(self, variant_name, snapshot, qty, entry_price, regime_name, side=None):
            ok = _orig_open_shadow_trade_arch_cohort(self, variant_name, snapshot, qty, entry_price, regime_name, side=side)
            if not ok:
                return ok
            try:
                conn = _arch_se.get_conn()
                opened_at = None
                try:
                    row = conn.execute(
                        """
                        SELECT opened_at, meta_json
                        FROM shadow_trades
                        WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                        ORDER BY rowid DESC
                        LIMIT 1
                        """,
                        (snapshot.ticker, variant_name),
                    ).fetchone()
                    if row:
                        rd = dict(row) if not isinstance(row, dict) else row
                        opened_at = rd.get("opened_at")
                        meta_json = _arch_trade_meta_with_policy(rd.get("meta_json"), opened_at=opened_at)
                        conn.execute(
                            """
                            UPDATE shadow_trades
                            SET meta_json = ?
                            WHERE ticker = ? AND variant_name = ? AND closed_at IS NULL
                            """,
                            (meta_json, snapshot.ticker, variant_name),
                        )
                        conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass
            return ok

        _ArchShadowEngine.open_shadow_trade = _open_shadow_trade_arch_cohort
    except Exception as _arch_shadow_exc:
        print(f"Architect cohort open-trade hook skipped: {_arch_shadow_exc!r}")

    _prev_dashboard_status_post_exit_v2 = TradingBot.dashboard_status

    def _dashboard_status_post_exit_v2(self, *args, **kwargs):
        status = _prev_dashboard_status_post_exit_v2(self, *args, **kwargs)

        cohort = {
            "policy_version": "architect_exit_v2",
            "note": "pre/post cohorts split so consultant reviews do not mix legacy exits with new exit-v2 behavior",
            "all_closed": {},
            "post_exit_v2_closed": {},
            "post_exit_v2_by_side": {},
            "post_exit_v2_by_reason": [],
        }

        try:
            conn = sqlite3.connect("data/strategy_lab.db")
            conn.row_factory = sqlite3.Row

            def one(sql, params=()):
                row = conn.execute(sql, params).fetchone()
                return dict(row) if row else {}

            def many(sql, params=()):
                return [dict(r) for r in conn.execute(sql, params).fetchall()]

            all_closed = one(
                """
                SELECT
                    COUNT(*) AS trades,
                    COALESCE(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END), 0) AS wins,
                    COALESCE(SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END), 0) AS losses,
                    COALESCE(SUM(CASE WHEN realized_pnl = 0 THEN 1 ELSE 0 END), 0) AS flat,
                    ROUND(COALESCE(SUM(realized_pnl), 0), 4) AS realized_pnl
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                """
            )
            trades = int(all_closed.get("trades") or 0)
            wins = int(all_closed.get("wins") or 0)
            all_closed["win_rate"] = round(wins / trades, 4) if trades else 0.0
            cohort["all_closed"] = all_closed

            post_rows = many(
                """
                SELECT side, close_reason, realized_pnl, meta_json
                FROM shadow_trades
                WHERE closed_at IS NOT NULL
                ORDER BY rowid DESC
                """
            )

            post = []
            for r in post_rows:
                meta = _arch_safe_json_loads(r.get("meta_json"))
                if (meta.get("architect_policy_version") or "") == "architect_exit_v2":
                    post.append({
                        "side": str(r.get("side") or "").lower().strip(),
                        "close_reason": str(r.get("close_reason") or "unknown"),
                        "realized_pnl": float(r.get("realized_pnl") or 0.0),
                    })

            p_trades = len(post)
            p_wins = sum(1 for r in post if r["realized_pnl"] > 0)
            p_losses = sum(1 for r in post if r["realized_pnl"] < 0)
            p_flat = sum(1 for r in post if r["realized_pnl"] == 0)
            p_pnl = round(sum(r["realized_pnl"] for r in post), 4)

            cohort["post_exit_v2_closed"] = {
                "trades": p_trades,
                "wins": p_wins,
                "losses": p_losses,
                "flat": p_flat,
                "win_rate": round(p_wins / p_trades, 4) if p_trades else 0.0,
                "realized_pnl": p_pnl,
                "sample_ready": p_trades >= 10,
            }

            by_side = {}
            for side in ("yes", "no"):
                rows = [r for r in post if r["side"] == side]
                n = len(rows)
                w = sum(1 for r in rows if r["realized_pnl"] > 0)
                l = sum(1 for r in rows if r["realized_pnl"] < 0)
                f = sum(1 for r in rows if r["realized_pnl"] == 0)
                pnl = round(sum(r["realized_pnl"] for r in rows), 4)
                by_side[side] = {
                    "trades": n,
                    "wins": w,
                    "losses": l,
                    "flat": f,
                    "win_rate": round(w / n, 4) if n else 0.0,
                    "realized_pnl": pnl,
                    "avg_pnl_per_trade": round(pnl / n, 4) if n else 0.0,
                }
            cohort["post_exit_v2_by_side"] = by_side

            reason_map = {}
            for r in post:
                key = r["close_reason"]
                bucket = reason_map.setdefault(key, {"close_reason": key, "trades": 0, "wins": 0, "losses": 0, "flat": 0, "realized_pnl": 0.0})
                bucket["trades"] += 1
                bucket["realized_pnl"] += r["realized_pnl"]
                if r["realized_pnl"] > 0:
                    bucket["wins"] += 1
                elif r["realized_pnl"] < 0:
                    bucket["losses"] += 1
                else:
                    bucket["flat"] += 1

            out = []
            for k, v in reason_map.items():
                n = int(v["trades"] or 0)
                v["realized_pnl"] = round(float(v["realized_pnl"] or 0.0), 4)
                v["win_rate"] = round((v["wins"] / n), 4) if n else 0.0
                out.append(v)
            out.sort(key=lambda x: (-x["trades"], x["realized_pnl"]))
            cohort["post_exit_v2_by_reason"] = out[:20]

            conn.close()
        except Exception as e:
            cohort["error"] = repr(e)

        status["post_exit_v2_cohorts"] = cohort

        try:
            architect = status.get("architect") or {}
            if isinstance(architect, dict):
                architect["post_exit_v2_cohorts"] = cohort
                status["architect"] = architect
        except Exception:
            pass

        return status

    TradingBot.dashboard_status = _dashboard_status_post_exit_v2

except Exception as _architect_post_exit_v2_exc:
    try:
        print(f"Architect post-exit-v2 cohort patch skipped: {_architect_post_exit_v2_exc!r}")
    except Exception:
        pass



# --- architect gates + replay surface ---
try:
    from app.strategy_lab.architect_backtest import summarize_market_gates, replay_from_opportunities

    _prev_dashboard_status_architect_gates = TradingBot.dashboard_status
    _architect_gates_and_replay_wrapper_applied = True

    def _dashboard_status_architect_gates_and_replay(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_gates(self, *args, **kwargs)
        try:
            gates = summarize_market_gates(status if isinstance(status, dict) else {})
            status["architect_gates"] = gates

            cfg = ((status.get("strategies") or {}).get("crypto_lag") or {}) if isinstance(status, dict) else {}
            replay = replay_from_opportunities(
                strategy_name="crypto_lag",
                min_edge=float(cfg.get("min_edge") or 0.018),
                max_spread=float(cfg.get("max_spread") or 0.08),
                min_hours=float(cfg.get("min_hours_to_expiry") or 2.0),
                max_hours=float(cfg.get("max_hours_to_expiry") or 24.0),
                limit=20000,
            )
            status["architect_backtest"] = replay

            if gates.get("gate_state") == "active_candidates_present":
                status["architect_next_step"] = "verify_entry_pipeline_emits_post_exit_v2_trades"
            elif gates.get("gate_state") == "idle_because_threshold":
                status["architect_next_step"] = "use_replay_threshold_sweep_before_any_threshold_change"
            elif gates.get("gate_state") == "idle_because_no_positive_edge":
                status["architect_next_step"] = "improve_fair_value_model_before_threshold_or_sizing_changes"
            else:
                status["architect_next_step"] = "inspect_secondary_gates_spread_hours_and_side_routing"
        except Exception as e:
            status["architect_gates"] = {"error": repr(e)}
            status["architect_backtest"] = {"error": repr(e)}
            status["architect_next_step"] = "wrapper_error"
        return status

    TradingBot.dashboard_status = _dashboard_status_architect_gates_and_replay

    @app.get("/api/debug/architect-gates")
    async def api_debug_architect_gates():
        return JSONResponse((bot.dashboard_status() or {}).get("architect_gates") or {})

    @app.get("/api/debug/architect-backtest")
    async def api_debug_architect_backtest():
        return JSONResponse((bot.dashboard_status() or {}).get("architect_backtest") or {})

    @app.get("/api/debug/consultant-bundle-v2")
    async def api_debug_consultant_bundle_v2():
        status = bot.dashboard_status() or {}
        gates = status.get("architect_gates") or {}
        replay = status.get("architect_backtest") or {}
        strategies = status.get("strategies") or {}
        crypto_lag = strategies.get("crypto_lag") or {}

        lines = []
        lines.append(f"architect_version: {((status.get('architect_snapshot') or {}).get('architect_version')) or 'unknown'}")
        lines.append(f"mode: {status.get('mode')}")
        lines.append(f"live_armed: {status.get('live_armed')}")
        lines.append(f"loop_error: {(status.get('health') or {}).get('loop_error')}")
        lines.append(f"top_edge: {status.get('top_edge')}")
        lines.append("")
        lines.append("gate_diagnostics:")
        lines.append(f"  gate_state: {gates.get('gate_state')}")
        lines.append(f"  visible_rows: {gates.get('visible_rows')}")
        lines.append(f"  positive_best_edge_count: {gates.get('positive_best_edge_count')}")
        lines.append(f"  above_threshold_count: {gates.get('above_threshold_count')}")
        lines.append(f"  actionable_count: {gates.get('actionable_count')}")
        lines.append(f"  blocked: {gates.get('blocked')}")
        lines.append("")
        lines.append("replay_summary:")
        lines.append(f"  resolved_count_total: {replay.get('resolved_count_total')}")
        lines.append(f"  filtered_replay: {replay.get('filtered_replay')}")
        lines.append(f"  by_best_side: {replay.get('by_best_side')}")
        lines.append("")
        lines.append("threshold_sweep:")
        for row in (replay.get("threshold_sweep") or [])[:8]:
            lines.append(
                f"  - min_edge={row.get('min_edge')} trades={row.get('trades')} "
                f"wins={row.get('wins')} pnl={row.get('realized_pnl')} win_rate={row.get('win_rate')}"
            )
        lines.append("")
        lines.append("crypto_lag:")
        for k in ["enabled", "live_enabled", "min_edge", "max_spread", "perf_trades", "perf_wins", "perf_pnl", "last_signal_at", "last_signal_reason"]:
            lines.append(f"  {k}: {crypto_lag.get(k)}")
        lines.append("")
        lines.append(f"architect_next_step: {status.get('architect_next_step')}")
        return PlainTextResponse("\\n".join(lines))
except Exception:
    pass



# --- architect OrderIntent regression guard + runtime surfacing ---
try:
    import uuid
    from dataclasses import fields as _dc_fields

    _prev_generate_intent_architect_guard = TradingBot._generate_intent
    _architect_order_intent_regression_guard_applied = True

    def _architect_coerce_order_intent(intent, strategy=None, snapshot=None):
        if intent is None:
            return None
        if isinstance(intent, OrderIntent):
            return intent
        if not isinstance(intent, dict):
            return intent

        try:
            field_names = [f.name for f in _dc_fields(OrderIntent)]
        except Exception:
            field_names = list(getattr(OrderIntent, "__annotations__", {}).keys())

        strategy_name = getattr(strategy, "name", None) or intent.get("strategy") or "crypto_lag"

        payload = {}
        defaults = {
            "intent_id": str(uuid.uuid4()),
            "shadow_group_id": str(uuid.uuid4()),
            "strategy": strategy_name,
            "broker": "kalshi",
            "ticker": getattr(snapshot, "ticker", None),
            "title": getattr(snapshot, "title", None),
            "side": intent.get("side"),
            "action": intent.get("action", "buy"),
            "contracts": float(intent.get("contracts") or 0.0),
            "requested_price": intent.get("requested_price"),
            "fair_value": intent.get("fair_value", getattr(snapshot, "fair_yes", None)),
            "edge": intent.get("edge", getattr(snapshot, "best_exec_edge", getattr(snapshot, "edge", None))),
            "spot_symbol": intent.get("spot_symbol", getattr(snapshot, "spot_symbol", None)),
            "spot_price": intent.get("spot_price"),
            "rationale": intent.get("rationale", getattr(snapshot, "rationale", "")),
        }

        for name in field_names:
            if name in intent:
                payload[name] = intent.get(name)
            elif name in defaults:
                payload[name] = defaults[name]

        if "ticker" in field_names and not payload.get("ticker") and snapshot is not None:
            payload["ticker"] = getattr(snapshot, "ticker", None)
        if "title" in field_names and not payload.get("title") and snapshot is not None:
            payload["title"] = getattr(snapshot, "title", None)
        if "spot_price" in field_names and payload.get("spot_price") in (None, "") and snapshot is not None:
            try:
                bot_ref = globals().get("bot")
                sym = getattr(snapshot, "spot_symbol", None)
                if bot_ref is not None and sym:
                    payload["spot_price"] = float((getattr(bot_ref, "spot_prices", {}) or {}).get(sym) or 0.0)
            except Exception:
                pass

        return OrderIntent(**payload)

    def _generate_intent_architect_guard(self, strategy, snapshot):
        intent = _prev_generate_intent_architect_guard(self, strategy, snapshot)
        return _architect_coerce_order_intent(intent, strategy=strategy, snapshot=snapshot)

    TradingBot._generate_intent = _generate_intent_architect_guard

    _prev_dashboard_status_architect_runtime_flags = TradingBot.dashboard_status
    def _dashboard_status_architect_runtime_flags(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_runtime_flags(self, *args, **kwargs)
        try:
            resolver = getattr(self, "outcome_resolver", None)
            status["architect_runtime_guards"] = {
                "intent_return_contract": "OrderIntent_dataclass_only",
                "intent_dataclass_guard": True,
                "shadow_side_marking_policy": "row_side_uses_yes_bid_or_no_bid",
                "settlement_reference_policy": getattr(
                    resolver,
                    "last_settlement_reference_source",
                    "rolling_60s_proxy_if_available_else_instantaneous"
                ),
            }
        except Exception as e:
            status["architect_runtime_guards"] = {"error": repr(e)}
        return status

    TradingBot.dashboard_status = _dashboard_status_architect_runtime_flags
except Exception:
    pass


# --- architect consultant v2 surfaces: basis proxy + regime + tail overlay + maker readiness ---
try:
    import math
    from datetime import datetime, timezone, timedelta

    _architect_consultant_v2_surfaces_applied = True
    _prev_dashboard_status_architect_consultant_v2 = TradingBot.dashboard_status

    def _arch_parse_dt(v):
        try:
            if v is None:
                return None
            if isinstance(v, datetime):
                dt = v
            else:
                raw = str(v)
                if raw.endswith("Z"):
                    raw = raw[:-1] + "+00:00"
                dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _arch_safe_float(v, default=None):
        try:
            if v in (None, "", "null"):
                return default
            return float(v)
        except Exception:
            return default

    def _arch_row_to_price_points(source):
        pts = []
        if source is None:
            return pts

        for item in source:
            try:
                if isinstance(item, dict):
                    ts = (
                        item.get("ts")
                        or item.get("time")
                        or item.get("timestamp")
                        or item.get("at")
                    )
                    px = (
                        item.get("price")
                        if item.get("price") is not None else
                        item.get("spot")
                        if item.get("spot") is not None else
                        item.get("value")
                    )
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    ts, px = item[0], item[1]
                else:
                    continue

                dt = _arch_parse_dt(ts)
                px = _arch_safe_float(px, None)
                if dt is None or px is None or px <= 0:
                    continue
                pts.append((dt, px))
            except Exception:
                continue

        pts.sort(key=lambda x: x[0])
        return pts

    def _arch_recent_spot_points(self, symbol="BTC", lookback_seconds=3600):
        symbol = str(symbol or "").upper()
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=max(1, int(lookback_seconds)))

        gathered = []
        candidate_attrs = [
            "spot_history",
            "recent_spot_history",
            "spot_price_history",
            "price_history",
            "market_spot_history",
        ]

        for attr in candidate_attrs:
            container = getattr(self, attr, None)
            if container is None:
                continue

            try:
                if isinstance(container, dict):
                    source = (
                        container.get(symbol)
                        or container.get(symbol.lower())
                        or container.get(symbol.title())
                    )
                else:
                    source = container

                pts = _arch_row_to_price_points(source or [])
                gathered.extend(pts)
            except Exception:
                continue

        dedup = {}
        for dt, px in gathered:
            if dt >= cutoff:
                dedup[dt.isoformat()] = (dt, px)

        pts = list(dedup.values())
        pts.sort(key=lambda x: x[0])

        if pts:
            return pts

        spot_now = _arch_safe_float((getattr(self, "spot_prices", {}) or {}).get(symbol), None)
        if spot_now is not None and spot_now > 0:
            return [(now, spot_now)]

        return []

    def _arch_rolling_avg(points, seconds=60):
        if not points:
            return None, 0
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=max(1, int(seconds)))
        recent = [px for dt, px in points if dt >= cutoff and px > 0]
        if not recent:
            recent = [points[-1][1]]
        if not recent:
            return None, 0
        return round(sum(recent) / len(recent), 6), len(recent)

    def _arch_move_pct(points, seconds):
        if not points:
            return None
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=max(1, int(seconds)))
        recent = [(dt, px) for dt, px in points if dt >= cutoff and px > 0]
        if len(recent) < 2:
            return 0.0
        start = recent[0][1]
        end = recent[-1][1]
        if not start:
            return 0.0
        return round((end - start) / start, 6)

    def _arch_distance_bucket(dist):
        d = _arch_safe_float(dist, None)
        if d is None:
            return "unknown"
        d = abs(d)
        if d < 0.005:
            return "atm_0_0.5pct"
        if d < 0.015:
            return "near_0.5_1.5pct"
        if d < 0.03:
            return "mid_1.5_3pct"
        return "far_3pct_plus"

    def _arch_tail_multiplier(bucket, regime):
        base = {
            "atm_0_0.5pct": 1.00,
            "near_0.5_1.5pct": 1.04,
            "mid_1.5_3pct": 1.10,
            "far_3pct_plus": 1.18,
            "unknown": 1.00,
        }.get(bucket, 1.00)

        if regime == "breakout_high_vol":
            base += 0.06
        elif regime == "mean_revert_low_vol":
            base -= 0.02

        return round(max(0.98, min(1.35, base)), 4)

    def _arch_tail_adjusted_yes(fair_yes, tail_mult):
        fy = _arch_safe_float(fair_yes, None)
        tm = _arch_safe_float(tail_mult, 1.0)
        if fy is None:
            return None
        fy = max(0.0001, min(0.9999, fy))
        if fy <= 0.5:
            out = fy * tm
        else:
            out = 1.0 - ((1.0 - fy) * tm)
        return round(max(0.02, min(0.98, out)), 4)

    def _arch_regime(dynamic_vol, move_1h, move_6h):
        dv = _arch_safe_float(dynamic_vol, 0.0) or 0.0
        m1 = abs(_arch_safe_float(move_1h, 0.0) or 0.0)
        m6 = abs(_arch_safe_float(move_6h, 0.0) or 0.0)

        if dv >= 0.70 or m1 >= 0.03 or m6 >= 0.06:
            return "breakout_high_vol"
        if dv <= 0.40 and m1 <= 0.015 and m6 <= 0.03:
            return "mean_revert_low_vol"
        return "neutral"

    def _arch_size_multiplier(regime):
        return {
            "mean_revert_low_vol": 1.25,
            "neutral": 1.0,
            "breakout_high_vol": 0.5,
        }.get(regime, 1.0)

    def _arch_maker_quote(bid, ask, tick=0.01):
        bid = _arch_safe_float(bid, None)
        ask = _arch_safe_float(ask, None)
        if bid is None or ask is None:
            return None
        if ask <= bid:
            return round(max(0.01, min(0.99, bid)), 4)
        spread = ask - bid
        if spread >= (2 * tick):
            q = bid + tick
        else:
            q = bid
        q = max(0.01, min(0.99, q))
        return round(q, 4)

    def _dashboard_status_architect_consultant_v2(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_consultant_v2(self, *args, **kwargs)
        if not isinstance(status, dict):
            return status

        try:
            model_meta = status.get("model_meta", {}) or {}
            dyn_vol = _arch_safe_float(
                model_meta.get("dynamic_annual_vol_btc"),
                _arch_safe_float(model_meta.get("dynamic_annual_vol"), 0.0),
            ) or 0.0

            pts = _arch_recent_spot_points(self, "BTC", 6 * 3600)
            spot_now = _arch_safe_float((getattr(self, "spot_prices", {}) or {}).get("BTC"), None)
            brti_proxy_60s, brti_proxy_points = _arch_rolling_avg(pts, 60)
            move_1h = _arch_move_pct(pts, 3600)
            move_6h = _arch_move_pct(pts, 6 * 3600)
            regime = _arch_regime(dyn_vol, move_1h, move_6h)
            size_mult = _arch_size_multiplier(regime)
            queue_haircut = 0.003

            positive_maker = []
            rows = status.get("markets") or []
            rows_with_tail = 0

            for row in rows:
                if not isinstance(row, dict):
                    continue

                row["distance_bucket"] = row.get("distance_bucket") or _arch_distance_bucket(row.get("distance_from_spot_pct"))

                fy = _arch_safe_float(row.get("fair_yes"), None)
                if fy is not None:
                    tm = _arch_tail_multiplier(row["distance_bucket"], regime)
                    row["tail_multiplier"] = tm
                    row["tail_adjusted_fair_yes"] = _arch_tail_adjusted_yes(fy, tm)
                    rows_with_tail += 1
                else:
                    row["tail_multiplier"] = None
                    row["tail_adjusted_fair_yes"] = None

                yes_bid = _arch_safe_float(row.get("yes_bid"), None)
                yes_ask = _arch_safe_float(row.get("yes_ask"), None)
                no_bid = _arch_safe_float(row.get("no_bid"), None)
                no_ask = _arch_safe_float(row.get("no_ask"), None)

                maker_yes_quote = _arch_maker_quote(yes_bid, yes_ask)
                maker_no_quote = _arch_maker_quote(no_bid, no_ask)

                tail_yes = _arch_safe_float(row.get("tail_adjusted_fair_yes"), fy)
                if tail_yes is None:
                    maker_yes_edge = None
                    maker_no_edge = None
                else:
                    maker_yes_edge = (
                        round(tail_yes - maker_yes_quote - queue_haircut, 4)
                        if maker_yes_quote is not None else None
                    )
                    maker_no_edge = (
                        round((1.0 - tail_yes) - maker_no_quote - queue_haircut, 4)
                        if maker_no_quote is not None else None
                    )

                row["maker_quote_yes"] = maker_yes_quote
                row["maker_quote_no"] = maker_no_quote
                row["maker_edge_yes"] = maker_yes_edge
                row["maker_edge_no"] = maker_no_edge

                maker_candidates = []
                if maker_yes_edge is not None:
                    maker_candidates.append(("yes", maker_yes_edge))
                if maker_no_edge is not None:
                    maker_candidates.append(("no", maker_no_edge))

                if maker_candidates:
                    maker_side, maker_best = max(maker_candidates, key=lambda x: x[1])
                else:
                    maker_side, maker_best = None, None

                row["maker_best_side"] = maker_side
                row["maker_best_edge"] = maker_best

                taker_best = _arch_safe_float(row.get("best_exec_edge"), None)
                if maker_best is not None and taker_best is not None and maker_best > max(taker_best, 0.0):
                    pref = "maker_candidate"
                elif taker_best is not None and taker_best > 0:
                    pref = "taker_candidate"
                else:
                    pref = "no_trade"
                row["preferred_execution_style"] = pref

                if maker_best is not None and maker_best > 0:
                    positive_maker.append({
                        "ticker": row.get("ticker"),
                        "maker_best_side": maker_side,
                        "maker_best_edge": maker_best,
                        "distance_bucket": row.get("distance_bucket"),
                        "preferred_execution_style": pref,
                    })

            positive_maker = sorted(
                positive_maker,
                key=lambda x: float(x.get("maker_best_edge") or 0.0),
                reverse=True,
            )

            basis_bps = None
            if brti_proxy_60s and spot_now:
                basis_bps = round(((brti_proxy_60s - spot_now) / spot_now) * 10000.0, 2)

            status["architect_consultant_v2"] = {
                "settlement_proxy": {
                    "symbol": "BTC",
                    "proxy_type": "rolling_60s_coinbase_average",
                    "spot_now": spot_now,
                    "proxy_60s": brti_proxy_60s,
                    "proxy_points": brti_proxy_points,
                    "basis_bps_vs_spot": basis_bps,
                    "policy": "diagnostic_now__wire_into_settlement_and_final_2h_edge_logic_next",
                },
                "regime_filter": {
                    "dynamic_annual_vol_btc": dyn_vol,
                    "move_1h_pct": move_1h,
                    "move_6h_pct": move_6h,
                    "regime": regime,
                    "size_multiplier_recommendation": size_mult,
                    "policy": "diagnostic_now__do_not_change_live_sizing_until_replay_confirms",
                },
                "tail_model": {
                    "mode": "tail_adjusted_overlay_v1",
                    "rows_with_tail_adjustment": rows_with_tail,
                    "policy": "diagnostic_now__replace_with_surface_calibration_after_replay",
                },
                "maker_readiness": {
                    "queue_haircut_assumption": queue_haircut,
                    "positive_maker_candidate_count": len(positive_maker),
                    "top_positive_maker_candidates": positive_maker[:5],
                    "policy": "diagnostic_now__no_live_resting_quotes_until_fill_model_exists",
                },
                "architect_decisions": [
                    "accept synthetic_BRTI_proxy and surface it everywhere",
                    "accept regime filter but keep it advisory until replay proves edge by regime",
                    "accept fat-tail adjustment as overlay diagnostics first, not live pricing replacement yet",
                    "accept maker path as maker-readiness and replay path first, not live quoting yet",
                ],
            }

            status["architect_next_step"] = "wire_basis_proxy_and_tail_overlay_into_replay_before_threshold_or_sizing_changes"
        except Exception as e:
            status["architect_consultant_v2"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_architect_consultant_v2
except Exception:
    pass


# --- architect consultant v3: execution-decision surfaces ---
try:
    from fastapi.responses import PlainTextResponse

    _prev_dashboard_status_architect_consultant_v3 = TradingBot.dashboard_status
    _architect_consultant_v3_execution_surfaces_applied = True

    def _arch_v3_num(v, default=0.0):
        try:
            if v in (None, "", "null"):
                return default
            return float(v)
        except Exception:
            return default

    def _arch_v3_int(v, default=0):
        try:
            if v in (None, "", "null"):
                return default
            return int(v)
        except Exception:
            return default

    def _dashboard_status_architect_consultant_v3(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_consultant_v3(self, *args, **kwargs)

        try:
            rows = list(status.get("markets") or [])
            strategies = status.get("strategies") or {}
            crypto_lag = (strategies.get("crypto_lag") or {}) if isinstance(strategies, dict) else {}
            min_edge = _arch_v3_num(crypto_lag.get("min_edge"), 0.018)
            max_spread = _arch_v3_num(crypto_lag.get("max_spread"), 0.08)

            v2 = status.get("architect_consultant_v2") or {}
            regime_filter = v2.get("regime_filter") or {}
            settlement_proxy = v2.get("settlement_proxy") or {}
            tail_model = v2.get("tail_model") or {}

            regime = str(regime_filter.get("regime") or "unknown")
            size_mult = _arch_v3_num(regime_filter.get("size_multiplier_recommendation"), 1.0)

            positive_taker = 0
            actionable_taker = 0
            positive_maker = 0
            actionable_maker = 0
            maker_better_than_taker = 0

            maker_by_side = {"yes": 0, "no": 0}
            taker_by_side = {"yes": 0, "no": 0}
            actionable_maker_rows = []
            actionable_taker_rows = []

            total_delta = 0.0
            delta_n = 0

            distance_view = {}
            style_counts = {}

            for row in rows:
                if not isinstance(row, dict):
                    continue

                best_side = str(row.get("best_side") or "").lower().strip()
                maker_side = str(row.get("maker_best_side") or "").lower().strip()
                best_exec_edge = _arch_v3_num(row.get("best_exec_edge"), 0.0)
                maker_best_edge = _arch_v3_num(row.get("maker_best_edge"), 0.0)
                spread = _arch_v3_num(row.get("spread"), 0.0)
                hours = _arch_v3_num(row.get("hours_to_expiry"), 0.0)
                bucket = str(row.get("distance_bucket") or "unknown")
                style = str(row.get("preferred_execution_style") or "unknown")

                style_counts[style] = style_counts.get(style, 0) + 1

                dv = distance_view.setdefault(bucket, {
                    "count": 0,
                    "positive_taker_count": 0,
                    "positive_maker_count": 0,
                    "actionable_taker_count": 0,
                    "actionable_maker_count": 0,
                })
                dv["count"] += 1

                if best_exec_edge > 0:
                    positive_taker += 1
                    dv["positive_taker_count"] += 1
                    if best_side in {"yes", "no"}:
                        taker_by_side[best_side] = taker_by_side.get(best_side, 0) + 1

                if maker_best_edge > 0:
                    positive_maker += 1
                    dv["positive_maker_count"] += 1
                    if maker_side in {"yes", "no"}:
                        maker_by_side[maker_side] = maker_by_side.get(maker_side, 0) + 1

                if best_exec_edge >= min_edge and spread <= max_spread and hours > 0:
                    actionable_taker += 1
                    dv["actionable_taker_count"] += 1
                    actionable_taker_rows.append({
                        "ticker": row.get("ticker"),
                        "side": best_side,
                        "edge": round(best_exec_edge, 4),
                        "spread": round(spread, 4),
                        "distance_bucket": bucket,
                    })

                if maker_best_edge >= min_edge and hours > 0:
                    actionable_maker += 1
                    dv["actionable_maker_count"] += 1
                    actionable_maker_rows.append({
                        "ticker": row.get("ticker"),
                        "side": maker_side,
                        "edge": round(maker_best_edge, 4),
                        "spread": round(spread, 4),
                        "distance_bucket": bucket,
                    })

                if maker_best_edge > best_exec_edge:
                    maker_better_than_taker += 1

                total_delta += (maker_best_edge - best_exec_edge)
                delta_n += 1

            avg_maker_advantage = round((total_delta / delta_n), 4) if delta_n else 0.0

            if actionable_maker >= 2 and actionable_taker == 0:
                recommendation = "build_maker_shadow_quote_simulator_next"
                execution_mode = "maker_research_priority"
            elif actionable_maker > actionable_taker:
                recommendation = "compare_maker_vs_taker_in_shadow_before_threshold_changes"
                execution_mode = "maker_advantage_detected"
            elif actionable_taker >= 2:
                recommendation = "keep_taker_shadow_and_improve_fair_value_model"
                execution_mode = "taker_still_viable"
            else:
                recommendation = "improve_fair_value_model_before_execution_changes"
                execution_mode = "edge_insufficient"

            priority_queue = [
                "implement_maker_shadow_quote_simulator",
                "promote_tail_overlay_into_replay_scoring",
                "promote_basis_proxy_into_replay_settlement",
                "promote_regime_scaling_into_replay_only",
            ]

            status["architect_execution_decision"] = {
                "execution_mode": execution_mode,
                "recommendation": recommendation,
                "min_edge_reference": round(min_edge, 6),
                "max_spread_reference": round(max_spread, 6),
                "regime": regime,
                "size_multiplier_recommendation": size_mult,
                "basis_bps_vs_spot": _arch_v3_num(settlement_proxy.get("basis_bps_vs_spot"), 0.0),
                "proxy_points": _arch_v3_int(settlement_proxy.get("proxy_points"), 0),
                "tail_model_mode": str(tail_model.get("mode") or "unknown"),
                "counts": {
                    "visible_rows": len(rows),
                    "positive_taker_count": positive_taker,
                    "actionable_taker_count": actionable_taker,
                    "positive_maker_count": positive_maker,
                    "actionable_maker_count": actionable_maker,
                    "maker_better_than_taker_count": maker_better_than_taker,
                    "preferred_execution_style_counts": style_counts,
                },
                "by_side": {
                    "taker_positive_by_side": taker_by_side,
                    "maker_positive_by_side": maker_by_side,
                },
                "distance_bucket_view": distance_view,
                "avg_maker_advantage_over_taker": avg_maker_advantage,
                "top_actionable_taker": sorted(actionable_taker_rows, key=lambda x: x["edge"], reverse=True)[:10],
                "top_actionable_maker": sorted(actionable_maker_rows, key=lambda x: x["edge"], reverse=True)[:10],
                "priority_queue": priority_queue,
            }

            status["architect_next_step"] = recommendation
        except Exception as e:
            status["architect_execution_decision"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_architect_consultant_v3

    @app.get("/api/debug/consultant-report-v3", response_class=PlainTextResponse)
    async def api_debug_consultant_report_v3():
        d = bot.dashboard_status()
        lines = []

        lines.append(f"architect_version: {d.get('architect_version') or 'unknown'}")
        lines.append(f"mode: {d.get('mode')}")
        lines.append(f"live_armed: {d.get('live_armed')}")
        lines.append(f"loop_error: {(d.get('health') or {}).get('loop_error')}")
        lines.append(f"top_edge: {d.get('top_edge')}")
        lines.append("")

        dec = d.get("architect_execution_decision") or {}
        counts = dec.get("counts") or {}
        lines.append("execution_decision:")
        lines.append(f"  execution_mode: {dec.get('execution_mode')}")
        lines.append(f"  recommendation: {dec.get('recommendation')}")
        lines.append(f"  regime: {dec.get('regime')}")
        lines.append(f"  size_multiplier_recommendation: {dec.get('size_multiplier_recommendation')}")
        lines.append(f"  basis_bps_vs_spot: {dec.get('basis_bps_vs_spot')}")
        lines.append(f"  proxy_points: {dec.get('proxy_points')}")
        lines.append(f"  tail_model_mode: {dec.get('tail_model_mode')}")
        lines.append(f"  positive_taker_count: {counts.get('positive_taker_count')}")
        lines.append(f"  actionable_taker_count: {counts.get('actionable_taker_count')}")
        lines.append(f"  positive_maker_count: {counts.get('positive_maker_count')}")
        lines.append(f"  actionable_maker_count: {counts.get('actionable_maker_count')}")
        lines.append(f"  maker_better_than_taker_count: {counts.get('maker_better_than_taker_count')}")
        lines.append(f"  avg_maker_advantage_over_taker: {dec.get('avg_maker_advantage_over_taker')}")
        lines.append("")

        lines.append("top_actionable_maker:")
        for row in (dec.get("top_actionable_maker") or [])[:5]:
            lines.append(
                f"  - {row.get('ticker')} side={row.get('side')} edge={row.get('edge')} "
                f"spread={row.get('spread')} bucket={row.get('distance_bucket')}"
            )
        lines.append("")

        lines.append("top_actionable_taker:")
        for row in (dec.get("top_actionable_taker") or [])[:5]:
            lines.append(
                f"  - {row.get('ticker')} side={row.get('side')} edge={row.get('edge')} "
                f"spread={row.get('spread')} bucket={row.get('distance_bucket')}"
            )
        lines.append("")

        lines.append("architect_priority_queue:")
        for item in (dec.get("priority_queue") or []):
            lines.append(f"  - {item}")

        return PlainTextResponse("\n".join(lines))
except Exception:
    pass


# --- architect maker shadow simulator ---
try:
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab.maker_shadow import build_shadow_quotes

    _prev_dashboard_status_architect_maker_shadow = TradingBot.dashboard_status
    _architect_maker_shadow_simulator_applied = True

    def _arch_ms_float(v, default=0.0):
        try:
            if v in (None, "", "null"):
                return default
            return float(v)
        except Exception:
            return default

    def _dashboard_status_architect_maker_shadow(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_maker_shadow(self, *args, **kwargs)

        try:
            rows = list(status.get("markets") or [])
            strategies = status.get("strategies") or {}
            crypto_lag = (strategies.get("crypto_lag") or {}) if isinstance(strategies, dict) else {}
            min_edge = _arch_ms_float(crypto_lag.get("min_edge"), 0.018)
            max_spread = _arch_ms_float(crypto_lag.get("max_spread"), 0.08)

            v2 = status.get("architect_consultant_v2") or {}
            reg = v2.get("regime_filter") or {}
            sett = v2.get("settlement_proxy") or {}

            regime_mult = _arch_ms_float(reg.get("size_multiplier_recommendation"), 1.0)
            basis_bps = _arch_ms_float(sett.get("basis_bps_vs_spot"), 0.0)

            sim = build_shadow_quotes(
                rows=rows,
                min_edge=min_edge,
                max_spread=max_spread,
                basis_bps_vs_spot=basis_bps,
                regime_size_mult=regime_mult,
            )

            status["maker_shadow_simulator"] = {
                "min_edge_reference": round(min_edge, 6),
                "max_spread_reference": round(max_spread, 6),
                "basis_bps_vs_spot": basis_bps,
                "regime_size_mult": regime_mult,
                "rows_seen": sim.get("rows_seen"),
                "maker_shadow_candidate_count": sim.get("maker_shadow_candidate_count"),
                "watch_only_count": sim.get("watch_only_count"),
                "by_quote_side": sim.get("by_quote_side"),
                "top_candidates": sim.get("top_candidates"),
            }

            # Architect decision override only if maker shadow evidence is clearly strong.
            dec = status.get("architect_execution_decision") or {}
            maker_ct = int(sim.get("maker_shadow_candidate_count") or 0)
            if maker_ct >= 3:
                dec["recommendation"] = "implement_maker_shadow_quote_comparison_before_any_threshold_change"
                dec["execution_mode"] = "maker_shadow_ready_for_comparison"
                status["architect_execution_decision"] = dec
                status["architect_next_step"] = dec["recommendation"]
        except Exception as e:
            status["maker_shadow_simulator"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_architect_maker_shadow

    @app.get("/api/debug/maker-shadow", response_class=PlainTextResponse)
    async def api_debug_maker_shadow():
        d = bot.dashboard_status()
        sim = d.get("maker_shadow_simulator") or {}
        lines = []
        lines.append(f"rows_seen: {sim.get('rows_seen')}")
        lines.append(f"maker_shadow_candidate_count: {sim.get('maker_shadow_candidate_count')}")
        lines.append(f"watch_only_count: {sim.get('watch_only_count')}")
        lines.append(f"by_quote_side: {sim.get('by_quote_side')}")
        lines.append("")
        lines.append("top_candidates:")
        for row in (sim.get("top_candidates") or [])[:10]:
            lines.append(
                f"  - {row.get('ticker')} quote_side={row.get('quote_side')} "
                f"quote_price={row.get('quote_price')} "
                f"adjusted_quote_edge={row.get('adjusted_quote_edge')} "
                f"fill_score={row.get('fill_score')} "
                f"bucket={row.get('distance_bucket')}"
            )
        return PlainTextResponse("\n".join(lines))
except Exception:
    pass


# --- architect execution compare surface ---
try:
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab.execution_compare import summarize_execution_compare

    _prev_dashboard_status_architect_execution_compare = TradingBot.dashboard_status
    _architect_execution_compare_surface_applied = True

    def _arch_ec_float(v, default=0.0):
        try:
            if v in (None, "", "null"):
                return default
            return float(v)
        except Exception:
            return default

    def _dashboard_status_architect_execution_compare(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_execution_compare(self, *args, **kwargs)

        try:
            rows = list(status.get("markets") or [])
            strategies = status.get("strategies") or {}
            crypto_lag = (strategies.get("crypto_lag") or {}) if isinstance(strategies, dict) else {}
            min_edge = _arch_ec_float(crypto_lag.get("min_edge"), 0.018)
            max_spread = _arch_ec_float(crypto_lag.get("max_spread"), 0.08)

            comp = summarize_execution_compare(
                rows=rows,
                min_edge=min_edge,
                max_spread=max_spread,
            )
            status["execution_compare"] = comp

            dec = status.get("architect_execution_decision") or {}
            if int(comp.get("actionable_maker_count") or 0) > int(comp.get("actionable_taker_count") or 0):
                dec["execution_mode"] = "maker_advantage_confirmed_by_compare_surface"
                dec["recommendation"] = "record_maker_vs_taker_comparison_cohort_before_any_threshold_change"
                status["architect_execution_decision"] = dec
                status["architect_next_step"] = dec["recommendation"]

            queue = list(status.get("architect_priority_queue") or [])
            wanted = "record_maker_vs_taker_comparison_cohort"
            if wanted not in queue:
                queue = [wanted] + queue
            status["architect_priority_queue"] = queue[:10]
        except Exception as e:
            status["execution_compare"] = {"error": repr(e)}

        return status

    TradingBot.dashboard_status = _dashboard_status_architect_execution_compare

    @app.get("/api/debug/execution-compare", response_class=PlainTextResponse)
    async def api_debug_execution_compare():
        d = bot.dashboard_status()
        comp = d.get("execution_compare") or {}
        lines = []
        lines.append(f"rows_seen: {comp.get('rows_seen')}")
        lines.append(f"positive_taker_count: {comp.get('positive_taker_count')}")
        lines.append(f"actionable_taker_count: {comp.get('actionable_taker_count')}")
        lines.append(f"positive_maker_count: {comp.get('positive_maker_count')}")
        lines.append(f"actionable_maker_count: {comp.get('actionable_maker_count')}")
        lines.append(f"maker_better_than_taker_count: {comp.get('maker_better_than_taker_count')}")
        lines.append(f"avg_maker_advantage_over_taker: {comp.get('avg_maker_advantage_over_taker')}")
        lines.append(f"by_maker_side: {comp.get('by_maker_side')}")
        lines.append("")
        lines.append("top_maker_advantage_rows:")
        for row in (comp.get("top_maker_advantage_rows") or [])[:10]:
            lines.append(
                f"  - {row.get('ticker')} "
                f"maker_side={row.get('maker_side')} maker_edge={row.get('maker_edge')} "
                f"taker_side={row.get('taker_side')} taker_edge={row.get('taker_edge')} "
                f"adv={row.get('maker_advantage')} bucket={row.get('distance_bucket')}"
            )
        return PlainTextResponse("\\n".join(lines))
except Exception:
    pass


# --- architect replay calibration + consultant evidence layer ---
try:
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab import replay_calibration as _arc_replay

    _prev_dashboard_status_architect_replay_calibration = TradingBot.dashboard_status
    _architect_replay_calibration_surface_applied = True

    def _dashboard_status_architect_replay_calibration(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_replay_calibration(self, *args, **kwargs)
        try:
            cal = _arc_replay.build_replay_calibration(self, status)
            status["architect_replay_calibration"] = cal

            cohort = (cal.get("execution_compare_cohort_runtime") or {})
            twap = (cal.get("twap_settlement_proxy") or {})
            lookup = (cal.get("historical_lookup") or {})
            choice_log = (cal.get("architect_choice_log") or {})

            status["architect_consultant_v4"] = {
                "execution_compare_cohort_runtime": cohort,
                "twap_settlement_proxy": twap,
                "historical_lookup": lookup,
                "architect_choice_log": choice_log,
                "architect_next_step": cal.get("architect_next_step"),
            }
        except Exception as e:
            status["architect_replay_calibration"] = {"error": repr(e)}
            status["architect_consultant_v4"] = {"error": repr(e)}
        return status

    TradingBot.dashboard_status = _dashboard_status_architect_replay_calibration

    @app.get("/api/debug/architect-replay-calibration", response_class=PlainTextResponse)
    async def api_debug_architect_replay_calibration():
        status = bot.dashboard_status()
        return PlainTextResponse(_arc_replay.build_plaintext_report(status))

except Exception:
    pass


# --- architect replay capture runtime layer ---
try:
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab import replay_capture as _arcap

    _architect_replay_capture_runtime_applied = True

    _prev_refresh_markets_architect_replay_capture = TradingBot.refresh_markets
    async def _refresh_markets_architect_replay_capture(self, *args, **kwargs):
        result = await _prev_refresh_markets_architect_replay_capture(self, *args, **kwargs)
        try:
            sample = _arcap.capture_spot_sample(self)
            resolved = _arcap.resolve_market_opportunities()
            self.architect_replay_capture_last = {
                "spot_sample": sample,
                "resolve_pass": resolved,
            }
        except Exception as e:
            self.architect_replay_capture_last = {"error": repr(e)}
        return result
    TradingBot.refresh_markets = _refresh_markets_architect_replay_capture

    _prev_dashboard_status_architect_replay_capture = TradingBot.dashboard_status
    def _dashboard_status_architect_replay_capture(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_replay_capture(self, *args, **kwargs)
        try:
            surf = _arcap.build_status(getattr(self, "architect_replay_capture_last", None))
            status["architect_replay_capture"] = surf

            cal = status.get("architect_replay_calibration")
            if isinstance(cal, dict):
                cal["twap_settlement_proxy"] = surf.get("twap_settlement_proxy") or cal.get("twap_settlement_proxy")
                if (surf.get("historical_lookup") or {}).get("row_count_used", 0):
                    cal["historical_lookup"] = surf.get("historical_lookup")
                cal["capture_runtime"] = surf.get("capture_runtime")
                status["architect_replay_calibration"] = cal

            v4 = status.get("architect_consultant_v4")
            if isinstance(v4, dict):
                v4["twap_settlement_proxy"] = (status.get("architect_replay_calibration") or {}).get("twap_settlement_proxy")
                v4["historical_lookup"] = (status.get("architect_replay_calibration") or {}).get("historical_lookup")
                v4["capture_runtime"] = surf.get("capture_runtime")
                status["architect_consultant_v4"] = v4
        except Exception as e:
            status["architect_replay_capture"] = {"error": repr(e)}
        return status
    TradingBot.dashboard_status = _dashboard_status_architect_replay_capture

    @app.get("/api/debug/architect-replay-capture", response_class=PlainTextResponse)
    async def api_debug_architect_replay_capture():
        status = bot.dashboard_status()
        return PlainTextResponse(_arcap.build_plaintext_report(status))

except Exception:
    pass


# --- architect cohort resolution runtime layer ---
try:
    from fastapi.responses import PlainTextResponse
    from app.strategy_lab import cohort_resolution as _acres

    _architect_cohort_resolution_runtime_applied = True

    _prev_refresh_markets_architect_cohort_resolution = TradingBot.refresh_markets
    async def _refresh_markets_architect_cohort_resolution(self, *args, **kwargs):
        result = await _prev_refresh_markets_architect_cohort_resolution(self, *args, **kwargs)
        try:
            self.architect_cohort_resolution_last = _acres.resolve_execution_compare_cohorts()
        except Exception as e:
            self.architect_cohort_resolution_last = {"error": repr(e)}
        return result
    TradingBot.refresh_markets = _refresh_markets_architect_cohort_resolution

    _prev_dashboard_status_architect_cohort_resolution = TradingBot.dashboard_status
    def _dashboard_status_architect_cohort_resolution(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_cohort_resolution(self, *args, **kwargs)
        try:
            surf = _acres.build_surface()
            surf["last_runtime_memo"] = getattr(self, "architect_cohort_resolution_last", None)
            status["architect_cohort_resolution"] = surf

            cal = status.get("architect_replay_calibration")
            if isinstance(cal, dict):
                cal["historical_lookup"] = surf.get("historical_lookup")
                cal["architect_next_step"] = surf.get("architect_next_step") or cal.get("architect_next_step")
                status["architect_replay_calibration"] = cal
        except Exception as e:
            status["architect_cohort_resolution"] = {"error": repr(e)}
        return status
    TradingBot.dashboard_status = _dashboard_status_architect_cohort_resolution

    @app.get("/api/debug/architect-cohort-resolution", response_class=PlainTextResponse)
    async def api_debug_architect_cohort_resolution():
        return PlainTextResponse(_acres.plaintext(bot.dashboard_status()))

except Exception:
    pass


# --- architect replay scoring bridge ---
try:
    from app.strategy_lab.replay_scoring import build_replay_scoring_surface as _arch_build_replay_scoring_surface

    _prev_dashboard_status_architect_replay_scoring = TradingBot.dashboard_status
    _architect_replay_scoring_bridge_applied = True

    def _dashboard_status_architect_replay_scoring(self, *args, **kwargs):
        status = _prev_dashboard_status_architect_replay_scoring(self, *args, **kwargs)
        try:
            status["architect_replay_scoring"] = _arch_build_replay_scoring_surface()
        except Exception as e:
            status["architect_replay_scoring"] = {"ok": False, "error": repr(e)}
        return status

    TradingBot.dashboard_status = _dashboard_status_architect_replay_scoring
except Exception:
    pass


@app.get("/api/debug/replay-scoring")
async def api_debug_replay_scoring():
    try:
        return _arch_build_replay_scoring_surface()
    except Exception as e:
        return {"ok": False, "error": repr(e)}


@app.get("/api/debug/consultant-report-v5")
async def api_debug_consultant_report_v5():
    status = bot.dashboard_status()
    replay = status.get("architect_replay_scoring", {}) or {}
    lines = []
    lines.append(f"mode: {status.get('mode')}")
    lines.append(f"live_armed: {status.get('live_armed')}")
    lines.append(f"loop_error: {(status.get('health') or {}).get('loop_error')}")
    lines.append("")
    lines.append("replay_scoring:")
    lines.append(f"  lookup_mode: {replay.get('lookup_mode')}")
    lines.append(f"  rows_used: {replay.get('rows_used')}")
    lines.append(f"  architect_next_step: {replay.get('architect_next_step')}")
    lines.append("")
    lines.append("maker_lookup_top:")
    for row in (replay.get("maker_lookup_top") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} expectancy={row.get('expectancy')}"
        )
    lines.append("")
    lines.append("taker_lookup_top:")
    for row in (replay.get("taker_lookup_top") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('side')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} expectancy={row.get('expectancy')}"
        )
    lines.append("")
    lines.append("regime_lookup_top:")
    for row in (replay.get("regime_lookup_top") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} regime={row.get('regime')} "
            f"hours={row.get('hours_bucket')} trades={row.get('trades')} "
            f"win_rate={row.get('win_rate')} expectancy={row.get('expectancy')}"
        )
    lines.append("")
    lines.append("tail_overlay_replay_top:")
    for row in ((replay.get("tail_overlay_replay") or {}).get("top") or [])[:10]:
        lines.append(
            f"  - {row.get('distance_bucket')} side={row.get('side')} "
            f"hours={row.get('hours_bucket')} tail_mult={row.get('tail_multiplier')} "
            f"fair={row.get('fair_yes')} tail_fair={row.get('tail_adjusted_fair_yes')} "
            f"pnl={row.get('realized_pnl')}"
        )
    return PlainTextResponse("\\n".join(lines))



# --- architect replay/capture/cohort debug routes v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_replay_capture_and_cohort_debug_routes_v1_applied = True

    async def _arch_debug_safe_status():
        status = bot.dashboard_status() if "bot" in globals() else {}
        if _inspect.isawaitable(status):
            status = await status
        return status or {}

    def _arch_debug_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_find_strategy_lab_db():
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_resolution_db_snapshot():
        out = {"db_path": None}
        db = _arch_find_strategy_lab_db()
        if not db:
            out["error"] = "strategy_lab.db_not_found"
            return out

        out["db_path"] = str(db)
        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()
            out["now_utc"] = cur.execute("SELECT datetime('now')").fetchone()[0]

            queries = {
                "cohorts_total": """
                    SELECT COUNT(*) FROM execution_compare_cohorts
                """,
                "cohorts_resolved": """
                    SELECT COUNT(*) FROM execution_compare_cohorts
                    WHERE resolved_at IS NOT NULL
                """,
                "cohorts_expired": """
                    SELECT COUNT(*) FROM execution_compare_cohorts
                    WHERE close_time IS NOT NULL
                      AND datetime(close_time) <= datetime('now')
                """,
                "cohorts_expired_unresolved": """
                    SELECT COUNT(*) FROM execution_compare_cohorts
                    WHERE close_time IS NOT NULL
                      AND datetime(close_time) <= datetime('now')
                      AND resolved_at IS NULL
                """,
                "opps_total": """
                    SELECT COUNT(*) FROM market_opportunities
                """,
                "opps_resolved": """
                    SELECT COUNT(*) FROM market_opportunities
                    WHERE resolved_at IS NOT NULL
                """,
                "opps_expired": """
                    SELECT COUNT(*) FROM market_opportunities
                    WHERE close_time IS NOT NULL
                      AND datetime(close_time) <= datetime('now')
                """,
                "opps_expired_unresolved": """
                    SELECT COUNT(*) FROM market_opportunities
                    WHERE close_time IS NOT NULL
                      AND datetime(close_time) <= datetime('now')
                      AND resolved_at IS NULL
                """,
            }

            for key, sql in queries.items():
                try:
                    out[key] = cur.execute(sql).fetchone()[0]
                except Exception as e:
                    out[key] = f"ERROR: {e!r}"

            try:
                rows = cur.execute("""
                    SELECT ticker, ts, close_time, resolved_at
                    FROM execution_compare_cohorts
                    ORDER BY close_time ASC
                    LIMIT 5
                """).fetchall()
                out["next_cohort_expiries"] = [
                    {
                        "ticker": r[0],
                        "ts": r[1],
                        "close_time": r[2],
                        "resolved_at": r[3],
                    }
                    for r in rows
                ]
            except Exception as e:
                out["next_cohort_expiries"] = {"error": repr(e)}

            con.close()
        except Exception as e:
            out["error"] = repr(e)

        return out

    if not _arch_debug_route_exists("/api/debug/replay-capture-report"):
        @app.get("/api/debug/replay-capture-report")
        async def api_debug_replay_capture_report():
            status = await _arch_debug_safe_status()
            capture = status.get("architect_replay_capture") or {}
            replay = status.get("architect_replay_calibration") or {}

            if not isinstance(capture, dict):
                capture = {"value": capture}

            payload = dict(capture)
            payload.setdefault("ok", True)
            payload.setdefault("capture_runtime", payload.get("capture_runtime") or {})
            payload.setdefault("historical_lookup", replay.get("historical_lookup") or {})
            payload.setdefault("twap_settlement_proxy", replay.get("twap_settlement_proxy") or {})
            payload.setdefault(
                "architect_next_step",
                replay.get("architect_next_step") or status.get("architect_next_step"),
            )
            payload["resolution_db"] = _arch_resolution_db_snapshot()
            return JSONResponse(payload)

    if not _arch_debug_route_exists("/api/debug/cohort-resolution-report"):
        @app.get("/api/debug/cohort-resolution-report")
        async def api_debug_cohort_resolution_report():
            status = await _arch_debug_safe_status()
            replay = status.get("architect_replay_calibration") or {}
            consultant = status.get("architect_consultant_v4") or {}
            cohort = (
                replay.get("execution_compare_cohort_runtime")
                or consultant.get("execution_compare_cohort_runtime")
                or {}
            )

            if not isinstance(cohort, dict):
                cohort = {"value": cohort}

            payload = {
                "ok": True,
                "execution_compare_cohort_runtime": cohort,
                "historical_lookup": replay.get("historical_lookup") or consultant.get("historical_lookup") or {},
                "twap_settlement_proxy": replay.get("twap_settlement_proxy") or consultant.get("twap_settlement_proxy") or {},
                "architect_next_step": replay.get("architect_next_step") or consultant.get("architect_next_step") or status.get("architect_next_step"),
                "resolution_db": _arch_resolution_db_snapshot(),
            }
            return JSONResponse(payload)

except Exception:
    pass



# --- architect regime-sliced replay views v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_regime_sliced_replay_views_v1_applied = True

    async def _arch_regime_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_regime_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_regime_find_db():
        find_fn = globals().get("_arch_find_strategy_lab_db")
        if callable(find_fn):
            try:
                found = find_fn()
                if found:
                    return found
            except Exception:
                pass
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_regime_hours_bucket_sql(col):
        return f"""
            CASE
                WHEN {col} IS NULL THEN 'unknown'
                WHEN {col} < 1 THEN 'lt_1h'
                WHEN {col} < 2 THEN '1_2h'
                WHEN {col} < 6 THEN '2_6h'
                WHEN {col} < 12 THEN '6_12h'
                ELSE '12h_plus'
            END
        """

    def _arch_regime_spread_bucket_sql(col):
        return f"""
            CASE
                WHEN {col} IS NULL THEN 'unknown'
                WHEN {col} < 0.03 THEN 'tight_lt_3c'
                WHEN {col} < 0.07 THEN 'mid_3_7c'
                ELSE 'wide_ge_7c'
            END
        """

    def _arch_query_rows(cur, sql, params=(), limit=None):
        rows = cur.execute(sql, params).fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        out = [dict(zip(cols, r)) for r in rows]
        return out[:limit] if limit else out

    def _arch_build_regime_replay_view_from_db(status):
        out = {
            "ok": True,
            "db_path": None,
            "runtime_regime_state": {},
            "spread_state_runtime": {},
            "maker_regime_lookup_top": [],
            "taker_regime_lookup_top": [],
            "market_opportunities_spread_replay_top": [],
            "row_counts": {},
            "architect_next_step": None,
        }

        consultant_v2 = (status.get("architect_consultant_v2") or {}) if isinstance(status, dict) else {}
        regime_filter = (consultant_v2.get("regime_filter") or {}) if isinstance(consultant_v2, dict) else {}
        settlement_proxy = (consultant_v2.get("settlement_proxy") or {}) if isinstance(consultant_v2, dict) else {}

        out["runtime_regime_state"] = {
            "dynamic_annual_vol_btc": regime_filter.get("dynamic_annual_vol_btc"),
            "move_1h_pct": regime_filter.get("move_1h_pct"),
            "move_6h_pct": regime_filter.get("move_6h_pct"),
            "regime": regime_filter.get("regime"),
            "size_multiplier_recommendation": regime_filter.get("size_multiplier_recommendation"),
            "basis_bps_vs_spot": settlement_proxy.get("basis_bps_vs_spot"),
            "policy": "replay_only__diagnostic_view__no_threshold_or_sizing_changes",
        }

        out["architect_next_step"] = (
            ((status.get("architect_replay_scoring") or {}).get("architect_next_step"))
            or status.get("architect_next_step")
            or "continue_collecting_resolved_rows_before_threshold_or_sizing_changes"
        )

        db = _arch_regime_find_db()
        if not db:
            out["ok"] = False
            out["error"] = "strategy_lab.db_not_found"
            return out

        out["db_path"] = str(db)

        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()

            try:
                out["row_counts"]["cohorts_total"] = cur.execute(
                    "SELECT COUNT(*) FROM execution_compare_cohorts"
                ).fetchone()[0]
                out["row_counts"]["cohorts_resolved"] = cur.execute(
                    "SELECT COUNT(*) FROM execution_compare_cohorts WHERE resolved_at IS NOT NULL"
                ).fetchone()[0]
            except Exception as e:
                out["row_counts"]["cohorts_error"] = repr(e)

            try:
                out["row_counts"]["opps_total"] = cur.execute(
                    "SELECT COUNT(*) FROM market_opportunities"
                ).fetchone()[0]
                out["row_counts"]["opps_resolved"] = cur.execute(
                    "SELECT COUNT(*) FROM market_opportunities WHERE resolved_at IS NOT NULL"
                ).fetchone()[0]
            except Exception as e:
                out["row_counts"]["opps_error"] = repr(e)

            try:
                spread_sql = f"""
                    SELECT
                        COUNT(*) AS recent_rows,
                        ROUND(AVG(spread), 6) AS avg_spread,
                        ROUND(MIN(spread), 6) AS min_spread,
                        ROUND(MAX(spread), 6) AS max_spread,
                        SUM(CASE WHEN {_arch_regime_spread_bucket_sql("spread")} = 'tight_lt_3c' THEN 1 ELSE 0 END) AS tight_count,
                        SUM(CASE WHEN {_arch_regime_spread_bucket_sql("spread")} = 'mid_3_7c' THEN 1 ELSE 0 END) AS mid_count,
                        SUM(CASE WHEN {_arch_regime_spread_bucket_sql("spread")} = 'wide_ge_7c' THEN 1 ELSE 0 END) AS wide_count
                    FROM (
                        SELECT spread
                        FROM market_opportunities
                        WHERE spread IS NOT NULL
                        ORDER BY ts DESC
                        LIMIT 500
                    )
                """
                rows = _arch_query_rows(cur, spread_sql, limit=1)
                out["spread_state_runtime"] = rows[0] if rows else {}
            except Exception as e:
                out["spread_state_runtime"] = {"error": repr(e)}

            try:
                maker_sql = """
                    SELECT
                        regime,
                        distance_bucket,
                        hours_bucket,
                        maker_side AS side,
                        COUNT(*) AS trades,
                        ROUND(AVG(CASE WHEN maker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS win_rate,
                        ROUND(AVG(maker_realized_pnl), 6) AS expectancy
                    FROM execution_compare_cohorts
                    WHERE resolved_at IS NOT NULL
                      AND maker_side IS NOT NULL
                      AND regime IS NOT NULL
                      AND distance_bucket IS NOT NULL
                      AND hours_bucket IS NOT NULL
                    GROUP BY regime, distance_bucket, hours_bucket, maker_side
                    HAVING COUNT(*) > 0
                    ORDER BY trades DESC, expectancy DESC
                    LIMIT 15
                """
                out["maker_regime_lookup_top"] = _arch_query_rows(cur, maker_sql)
            except Exception as e:
                out["maker_regime_lookup_top"] = [{"error": repr(e)}]

            try:
                taker_sql = """
                    SELECT
                        regime,
                        distance_bucket,
                        hours_bucket,
                        taker_side AS side,
                        COUNT(*) AS trades,
                        ROUND(AVG(CASE WHEN taker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS win_rate,
                        ROUND(AVG(taker_realized_pnl), 6) AS expectancy
                    FROM execution_compare_cohorts
                    WHERE resolved_at IS NOT NULL
                      AND taker_side IS NOT NULL
                      AND regime IS NOT NULL
                      AND distance_bucket IS NOT NULL
                      AND hours_bucket IS NOT NULL
                    GROUP BY regime, distance_bucket, hours_bucket, taker_side
                    HAVING COUNT(*) > 0
                    ORDER BY trades DESC, expectancy DESC
                    LIMIT 15
                """
                out["taker_regime_lookup_top"] = _arch_query_rows(cur, taker_sql)
            except Exception as e:
                out["taker_regime_lookup_top"] = [{"error": repr(e)}]

            try:
                opps_sql = f"""
                    SELECT
                        {_arch_regime_spread_bucket_sql("spread")} AS spread_bucket,
                        best_side AS side,
                        {_arch_regime_hours_bucket_sql("hours_to_expiry")} AS hours_bucket,
                        COUNT(*) AS trades,
                        ROUND(AVG(CASE WHEN best_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS win_rate,
                        ROUND(AVG(best_side_realized_pnl), 6) AS expectancy
                    FROM market_opportunities
                    WHERE resolved_at IS NOT NULL
                      AND best_side IS NOT NULL
                    GROUP BY spread_bucket, best_side, hours_bucket
                    HAVING COUNT(*) > 0
                    ORDER BY trades DESC, expectancy DESC
                    LIMIT 15
                """
                out["market_opportunities_spread_replay_top"] = _arch_query_rows(cur, opps_sql)
            except Exception as e:
                out["market_opportunities_spread_replay_top"] = [{"error": repr(e)}]

            con.close()
        except Exception as e:
            out["ok"] = False
            out["error"] = repr(e)

        return out

    if not _arch_regime_route_exists("/api/debug/replay-regime-report"):
        @app.get("/api/debug/replay-regime-report")
        async def api_debug_replay_regime_report():
            status = await _arch_regime_safe_status()
            return JSONResponse(_arch_build_regime_replay_view_from_db(status))

except Exception:
    pass



# --- architect maker shadow comparison report v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_maker_shadow_comparison_report_v1_applied = True

    async def _arch_maker_shadow_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_maker_shadow_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_maker_shadow_find_db():
        for helper_name in ("_arch_regime_find_db", "_arch_find_strategy_lab_db"):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_rows(cur, sql, params=()):
        rows = cur.execute(sql, params).fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in rows]

    def _arch_build_maker_shadow_report(status):
        out = {
            "ok": True,
            "policy": "replay_only__diagnostic_view__no_threshold_or_sizing_changes",
            "db_path": None,
            "maker_shadow_simulator": {},
            "architect_execution_decision": {},
            "execution_compare_cohort_runtime": {},
            "persistence_summary": {},
            "latest_maker_candidates": [],
            "resolved_maker_vs_taker_buckets": [],
            "architect_next_step": None,
        }

        if isinstance(status, dict):
            out["maker_shadow_simulator"] = status.get("maker_shadow_simulator") or {}
            out["architect_execution_decision"] = status.get("architect_execution_decision") or {}
            out["execution_compare_cohort_runtime"] = (
                (status.get("architect_replay_calibration") or {}).get("execution_compare_cohort_runtime")
                or {}
            )
            out["architect_next_step"] = (
                status.get("architect_next_step")
                or ((status.get("architect_replay_calibration") or {}).get("architect_next_step"))
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )

        db = _arch_maker_shadow_find_db()
        if not db:
            out["ok"] = False
            out["error"] = "strategy_lab.db_not_found"
            return out

        out["db_path"] = str(db)

        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()

            try:
                summary_sql = """
                    SELECT
                        COUNT(*) AS total_rows,
                        SUM(CASE WHEN preferred_execution_style = 'maker_candidate' THEN 1 ELSE 0 END) AS maker_candidate_rows,
                        SUM(CASE WHEN preferred_execution_style = 'taker_candidate' THEN 1 ELSE 0 END) AS taker_candidate_rows,
                        SUM(CASE WHEN preferred_execution_style = 'no_trade' THEN 1 ELSE 0 END) AS no_trade_rows,
                        SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
                        SUM(CASE WHEN maker_edge IS NOT NULL THEN 1 ELSE 0 END) AS maker_edge_rows,
                        SUM(CASE WHEN taker_edge IS NOT NULL THEN 1 ELSE 0 END) AS taker_edge_rows,
                        SUM(CASE WHEN maker_edge IS NOT NULL AND taker_edge IS NOT NULL AND maker_edge > taker_edge THEN 1 ELSE 0 END) AS maker_beats_taker_rows
                    FROM execution_compare_cohorts
                """
                rows = _arch_rows(cur, summary_sql)
                out["persistence_summary"] = rows[0] if rows else {}
            except Exception as e:
                out["persistence_summary"] = {"error": repr(e)}

            try:
                latest_sql = """
                    SELECT
                        ticker,
                        ts,
                        regime,
                        distance_bucket,
                        hours_bucket,
                        maker_best_side,
                        maker_side,
                        maker_edge,
                        taker_side,
                        taker_edge,
                        preferred_execution_style,
                        ROUND(COALESCE(maker_edge, 0) - COALESCE(taker_edge, 0), 6) AS maker_advantage
                    FROM execution_compare_cohorts
                    WHERE maker_edge IS NOT NULL
                    ORDER BY ts DESC
                    LIMIT 12
                """
                out["latest_maker_candidates"] = _arch_rows(cur, latest_sql)
            except Exception as e:
                out["latest_maker_candidates"] = [{"error": repr(e)}]

            try:
                resolved_sql = """
                    SELECT
                        regime,
                        distance_bucket,
                        hours_bucket,
                        preferred_execution_style,
                        COUNT(*) AS rows,
                        ROUND(AVG(maker_realized_pnl), 6) AS maker_expectancy,
                        ROUND(AVG(taker_realized_pnl), 6) AS taker_expectancy,
                        ROUND(AVG(CASE WHEN maker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS maker_win_rate,
                        ROUND(AVG(CASE WHEN taker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS taker_win_rate
                    FROM execution_compare_cohorts
                    WHERE resolved_at IS NOT NULL
                    GROUP BY regime, distance_bucket, hours_bucket, preferred_execution_style
                    HAVING COUNT(*) > 0
                    ORDER BY rows DESC, maker_expectancy DESC
                    LIMIT 20
                """
                out["resolved_maker_vs_taker_buckets"] = _arch_rows(cur, resolved_sql)
            except Exception as e:
                out["resolved_maker_vs_taker_buckets"] = [{"error": repr(e)}]

            con.close()
        except Exception as e:
            out["ok"] = False
            out["error"] = repr(e)

        return out

    if not _arch_maker_shadow_route_exists("/api/debug/maker-shadow-comparison-report"):
        @app.get("/api/debug/maker-shadow-comparison-report")
        async def api_debug_maker_shadow_comparison_report():
            status = await _arch_maker_shadow_safe_status()
            return JSONResponse(_arch_build_maker_shadow_report(status))

except Exception:
    pass



# --- architect replay settlement comparison v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_replay_settlement_comparison_v1_applied = True

    async def _arch_settlement_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_settlement_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_settlement_find_db():
        for helper_name in (
            "_arch_maker_shadow_find_db",
            "_arch_regime_find_db",
            "_arch_find_strategy_lab_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_rows(cur, sql, params=()):
        rows = cur.execute(sql, params).fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in rows]

    def _arch_build_replay_settlement_report(status):
        out = {
            "ok": True,
            "policy": "replay_only__diagnostic_view__no_threshold_or_sizing_changes",
            "db_path": None,
            "twap_settlement_proxy_runtime": {},
            "comparison_summary": {},
            "by_regime": [],
            "by_distance_hours": [],
            "agreement_examples": [],
            "disagreement_examples": [],
            "architect_next_step": None,
        }

        if isinstance(status, dict):
            replay_capture = status.get("architect_replay_capture") or {}
            replay_cal = status.get("architect_replay_calibration") or {}
            consultant_v2 = status.get("architect_consultant_v2") or {}

            out["twap_settlement_proxy_runtime"] = (
                replay_cal.get("twap_settlement_proxy")
                or consultant_v2.get("settlement_proxy")
                or {}
            )
            out["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or replay_cal.get("architect_next_step")
                or status.get("architect_next_step")
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )

        db = _arch_settlement_find_db()
        if not db:
            out["ok"] = False
            out["error"] = "strategy_lab.db_not_found"
            return out

        out["db_path"] = str(db)

        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()

            base_where = """
                FROM execution_compare_cohorts
                WHERE resolved_at IS NOT NULL
                  AND settlement_spot_twap_60s IS NOT NULL
                  AND market_result IS NOT NULL
                  AND floor_strike IS NOT NULL
                  AND cap_strike IS NOT NULL
            """

            summary_sql = f"""
                SELECT
                    COUNT(*) AS resolved_rows_with_proxy,
                    SUM(CASE WHEN settlement_proxy_points IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_proxy_points,
                    ROUND(AVG(settlement_proxy_points), 4) AS avg_proxy_points,
                    SUM(
                        CASE
                            WHEN (
                                CASE
                                    WHEN settlement_spot_twap_60s >= floor_strike
                                     AND settlement_spot_twap_60s <= cap_strike
                                    THEN 'yes' ELSE 'no'
                                END
                            ) = LOWER(COALESCE(market_result, ''))
                            THEN 1 ELSE 0
                        END
                    ) AS proxy_agree_count,
                    SUM(
                        CASE
                            WHEN (
                                CASE
                                    WHEN settlement_spot_twap_60s >= floor_strike
                                     AND settlement_spot_twap_60s <= cap_strike
                                    THEN 'yes' ELSE 'no'
                                END
                            ) <> LOWER(COALESCE(market_result, ''))
                            THEN 1 ELSE 0
                        END
                    ) AS proxy_disagree_count
                {base_where}
            """
            rows = _arch_rows(cur, summary_sql)
            out["comparison_summary"] = rows[0] if rows else {}

            if out["comparison_summary"].get("resolved_rows_with_proxy"):
                total = float(out["comparison_summary"]["resolved_rows_with_proxy"] or 0)
                agree = float(out["comparison_summary"]["proxy_agree_count"] or 0)
                out["comparison_summary"]["proxy_agreement_rate"] = round(agree / total, 4) if total else None

            by_regime_sql = f"""
                SELECT
                    COALESCE(regime, 'unknown') AS regime,
                    COUNT(*) AS rows,
                    ROUND(AVG(settlement_proxy_points), 4) AS avg_proxy_points,
                    SUM(
                        CASE
                            WHEN (
                                CASE
                                    WHEN settlement_spot_twap_60s >= floor_strike
                                     AND settlement_spot_twap_60s <= cap_strike
                                    THEN 'yes' ELSE 'no'
                                END
                            ) = LOWER(COALESCE(market_result, ''))
                            THEN 1 ELSE 0
                        END
                    ) AS agree_count,
                    ROUND(
                        AVG(
                            CASE
                                WHEN (
                                    CASE
                                        WHEN settlement_spot_twap_60s >= floor_strike
                                         AND settlement_spot_twap_60s <= cap_strike
                                        THEN 'yes' ELSE 'no'
                                    END
                                ) = LOWER(COALESCE(market_result, ''))
                                THEN 1.0 ELSE 0.0
                            END
                        ),
                        4
                    ) AS agreement_rate
                {base_where}
                GROUP BY COALESCE(regime, 'unknown')
                HAVING COUNT(*) > 0
                ORDER BY rows DESC, agreement_rate DESC
                LIMIT 12
            """
            out["by_regime"] = _arch_rows(cur, by_regime_sql)

            by_bucket_sql = f"""
                SELECT
                    COALESCE(distance_bucket, 'unknown') AS distance_bucket,
                    COALESCE(hours_bucket, 'unknown') AS hours_bucket,
                    COUNT(*) AS rows,
                    ROUND(AVG(settlement_proxy_points), 4) AS avg_proxy_points,
                    ROUND(
                        AVG(
                            CASE
                                WHEN (
                                    CASE
                                        WHEN settlement_spot_twap_60s >= floor_strike
                                         AND settlement_spot_twap_60s <= cap_strike
                                        THEN 'yes' ELSE 'no'
                                    END
                                ) = LOWER(COALESCE(market_result, ''))
                                THEN 1.0 ELSE 0.0
                            END
                        ),
                        4
                    ) AS agreement_rate
                {base_where}
                GROUP BY COALESCE(distance_bucket, 'unknown'), COALESCE(hours_bucket, 'unknown')
                HAVING COUNT(*) > 0
                ORDER BY rows DESC, agreement_rate DESC
                LIMIT 15
            """
            out["by_distance_hours"] = _arch_rows(cur, by_bucket_sql)

            agreement_sql = f"""
                SELECT
                    ticker,
                    regime,
                    distance_bucket,
                    hours_bucket,
                    close_time,
                    resolved_at,
                    settlement_spot_twap_60s,
                    settlement_proxy_points,
                    floor_strike,
                    cap_strike,
                    market_result,
                    CASE
                        WHEN settlement_spot_twap_60s >= floor_strike
                         AND settlement_spot_twap_60s <= cap_strike
                        THEN 'yes' ELSE 'no'
                    END AS proxy_market_result
                {base_where}
                  AND (
                    CASE
                        WHEN settlement_spot_twap_60s >= floor_strike
                         AND settlement_spot_twap_60s <= cap_strike
                        THEN 'yes' ELSE 'no'
                    END
                  ) = LOWER(COALESCE(market_result, ''))
                ORDER BY resolved_at DESC
                LIMIT 8
            """
            out["agreement_examples"] = _arch_rows(cur, agreement_sql)

            disagreement_sql = f"""
                SELECT
                    ticker,
                    regime,
                    distance_bucket,
                    hours_bucket,
                    close_time,
                    resolved_at,
                    settlement_spot_twap_60s,
                    settlement_proxy_points,
                    floor_strike,
                    cap_strike,
                    market_result,
                    CASE
                        WHEN settlement_spot_twap_60s >= floor_strike
                         AND settlement_spot_twap_60s <= cap_strike
                        THEN 'yes' ELSE 'no'
                    END AS proxy_market_result
                {base_where}
                  AND (
                    CASE
                        WHEN settlement_spot_twap_60s >= floor_strike
                         AND settlement_spot_twap_60s <= cap_strike
                        THEN 'yes' ELSE 'no'
                    END
                  ) <> LOWER(COALESCE(market_result, ''))
                ORDER BY resolved_at DESC
                LIMIT 8
            """
            out["disagreement_examples"] = _arch_rows(cur, disagreement_sql)

            con.close()
        except Exception as e:
            out["ok"] = False
            out["error"] = repr(e)

        return out

    if not _arch_settlement_route_exists("/api/debug/replay-settlement-comparison"):
        @app.get("/api/debug/replay-settlement-comparison")
        async def api_debug_replay_settlement_comparison():
            status = await _arch_settlement_safe_status()
            return JSONResponse(_arch_build_replay_settlement_report(status))

except Exception:
    pass



# --- architect tail overlay replay report v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_tail_overlay_replay_report_v1_applied = True

    async def _arch_tail_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_tail_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_tail_find_db():
        for helper_name in (
            "_arch_settlement_find_db",
            "_arch_maker_shadow_find_db",
            "_arch_regime_find_db",
            "_arch_find_strategy_lab_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_tail_rows(cur, sql, params=()):
        rows = cur.execute(sql, params).fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, r)) for r in rows]

    def _arch_tail_multiplier_bucket_sql(col):
        return f"""
            CASE
                WHEN {col} IS NULL THEN 'unknown'
                WHEN {col} < 0.98 THEN 'lt_0.98'
                WHEN {col} < 1.02 THEN '0.98_1.02'
                WHEN {col} < 1.08 THEN '1.02_1.08'
                ELSE 'ge_1.08'
            END
        """

    def _arch_build_tail_overlay_report(status):
        out = {
            "ok": True,
            "policy": "replay_only__diagnostic_view__no_threshold_or_sizing_changes",
            "db_path": None,
            "tail_runtime": {},
            "replay_scoring_surface": {},
            "resolved_tail_buckets": [],
            "resolved_tail_by_regime": [],
            "resolved_tail_examples": [],
            "architect_next_step": None,
        }

        if isinstance(status, dict):
            consultant_v2 = status.get("architect_consultant_v2") or {}
            replay_scoring = status.get("architect_replay_scoring") or {}
            replay_cal = status.get("architect_replay_calibration") or {}
            exec_decision = status.get("architect_execution_decision") or {}

            out["tail_runtime"] = {
                "tail_model_mode": (
                    exec_decision.get("tail_model_mode")
                    or (replay_cal.get("execution_compare_cohort_runtime") or {}).get("tail_model_mode")
                    or consultant_v2.get("tail_overlay", {}).get("tail_model_mode")
                ),
                "tail_overlay": consultant_v2.get("tail_overlay") or {},
                "basis_proxy": consultant_v2.get("settlement_proxy") or {},
            }
            out["replay_scoring_surface"] = replay_scoring.get("tail_overlay_replay") or {}
            out["architect_next_step"] = (
                replay_scoring.get("architect_next_step")
                or replay_cal.get("architect_next_step")
                or status.get("architect_next_step")
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )

        db = _arch_tail_find_db()
        if not db:
            out["ok"] = False
            out["error"] = "strategy_lab.db_not_found"
            return out

        out["db_path"] = str(db)

        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()

            tail_bucket_sql = _arch_tail_multiplier_bucket_sql("tail_multiplier")

            bucket_sql = f"""
                SELECT
                    {tail_bucket_sql} AS tail_bucket,
                    COALESCE(distance_bucket, 'unknown') AS distance_bucket,
                    COALESCE(hours_bucket, 'unknown') AS hours_bucket,
                    COUNT(*) AS rows,
                    ROUND(AVG(tail_multiplier), 4) AS avg_tail_multiplier,
                    ROUND(AVG(maker_realized_pnl), 6) AS maker_expectancy,
                    ROUND(AVG(taker_realized_pnl), 6) AS taker_expectancy,
                    ROUND(AVG(CASE WHEN maker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS maker_win_rate,
                    ROUND(AVG(CASE WHEN taker_side_correct = 1 THEN 1.0 ELSE 0.0 END), 4) AS taker_win_rate
                FROM execution_compare_cohorts
                WHERE resolved_at IS NOT NULL
                  AND tail_multiplier IS NOT NULL
                GROUP BY {tail_bucket_sql}, COALESCE(distance_bucket, 'unknown'), COALESCE(hours_bucket, 'unknown')
                HAVING COUNT(*) > 0
                ORDER BY rows DESC, maker_expectancy DESC
                LIMIT 20
            """
            out["resolved_tail_buckets"] = _arch_tail_rows(cur, bucket_sql)

            regime_sql = f"""
                SELECT
                    COALESCE(regime, 'unknown') AS regime,
                    {tail_bucket_sql} AS tail_bucket,
                    COUNT(*) AS rows,
                    ROUND(AVG(tail_multiplier), 4) AS avg_tail_multiplier,
                    ROUND(AVG(maker_realized_pnl), 6) AS maker_expectancy,
                    ROUND(AVG(taker_realized_pnl), 6) AS taker_expectancy
                FROM execution_compare_cohorts
                WHERE resolved_at IS NOT NULL
                  AND tail_multiplier IS NOT NULL
                GROUP BY COALESCE(regime, 'unknown'), {tail_bucket_sql}
                HAVING COUNT(*) > 0
                ORDER BY rows DESC, maker_expectancy DESC
                LIMIT 20
            """
            out["resolved_tail_by_regime"] = _arch_tail_rows(cur, regime_sql)

            examples_sql = """
                SELECT
                    ticker,
                    ts,
                    regime,
                    distance_bucket,
                    hours_bucket,
                    tail_model_mode,
                    tail_multiplier,
                    maker_side,
                    maker_edge,
                    taker_side,
                    taker_edge,
                    maker_realized_pnl,
                    taker_realized_pnl,
                    resolved_at
                FROM execution_compare_cohorts
                WHERE resolved_at IS NOT NULL
                  AND tail_multiplier IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT 12
            """
            out["resolved_tail_examples"] = _arch_tail_rows(cur, examples_sql)

            con.close()
        except Exception as e:
            out["ok"] = False
            out["error"] = repr(e)

        return out

    if not _arch_tail_route_exists("/api/debug/tail-overlay-replay-report"):
        @app.get("/api/debug/tail-overlay-replay-report")
        async def api_debug_tail_overlay_replay_report():
            status = await _arch_tail_safe_status()
            return JSONResponse(_arch_build_tail_overlay_report(status))

except Exception:
    pass



# --- architect microstructure truth foundation v1 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from datetime import datetime as _arch_dt, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_microstructure_truth_foundation_v1_applied = True

    async def _arch_mt_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_mt_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_mt_find_db():
        for helper_name in (
            "_arch_settlement_find_db",
            "_arch_maker_shadow_find_db",
            "_arch_regime_find_db",
            "_arch_find_strategy_lab_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass

        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_mt_safe_float(value, default=None):
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _arch_mt_parse_iso(ts):
        if not ts:
            return None
        try:
            return _arch_dt.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None

    def _arch_mt_weekend_flag(close_time):
        dt = _arch_mt_parse_iso(close_time)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        dt = dt.astimezone(_arch_tz.utc)

        # Replay-only weekend/gap stress flag:
        # Friday 21:00 UTC through Sunday inclusive, plus early Monday UTC.
        wd = dt.weekday()  # Mon=0 ... Sun=6
        if wd == 4 and dt.hour >= 21:
            return 1
        if wd in (5, 6):
            return 1
        if wd == 0 and dt.hour < 6:
            return 1
        return 0

    def _arch_mt_slippage_ticks(spread):
        spread = _arch_mt_safe_float(spread, None)
        if spread is None:
            return None
        if spread >= 0.08:
            return 4
        if spread >= 0.05:
            return 3
        if spread >= 0.03:
            return 2
        if spread >= 0.01:
            return 1
        return 0

    def _arch_mt_partial_fill_pct(spread):
        spread = _arch_mt_safe_float(spread, None)
        if spread is None:
            return None
        if spread >= 0.08:
            return 0.35
        if spread >= 0.05:
            return 0.50
        if spread >= 0.03:
            return 0.75
        return 0.90

    def _arch_mt_tick_size():
        return 0.01

    def _arch_mt_table_cols(cur, table):
        try:
            return [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]
        except Exception:
            return []

    def _arch_mt_ensure_columns():
        out = {
            "db_path": None,
            "added": [],
            "existing": [],
            "warnings": [],
        }

        db = _arch_mt_find_db()
        if not db:
            out["warnings"].append("strategy_lab.db_not_found")
            return out

        out["db_path"] = str(db)

        spec = {
            "execution_compare_cohorts": [
                ("requested_price", "REAL"),
                ("simulated_fill_price", "REAL"),
                ("partial_fill_pct", "REAL"),
                ("queue_depth_proxy", "REAL"),
                ("maker_toxicity_bps_5s", "REAL"),
                ("maker_toxicity_bps_10s", "REAL"),
                ("taker_exit_slippage_ticks", "REAL"),
                ("taker_exit_slippage_cost", "REAL"),
                ("weekend_flag", "INTEGER"),
                ("regime_vol_percentile", "REAL"),
                ("microstructure_extra_json", "TEXT"),
            ],
            "market_opportunities": [
                ("slippage_penalty_ticks", "REAL"),
                ("slippage_penalty_cost", "REAL"),
                ("weekend_flag", "INTEGER"),
                ("regime_vol_percentile", "REAL"),
                ("microstructure_extra_json", "TEXT"),
            ],
        }

        con = None
        try:
            con = _sqlite3.connect(str(db))
            cur = con.cursor()

            for table, cols in spec.items():
                existing = set(_arch_mt_table_cols(cur, table))
                if not existing:
                    out["warnings"].append(f"{table}_missing")
                    continue

                for col_name, col_type in cols:
                    if col_name in existing:
                        out["existing"].append(f"{table}.{col_name}")
                        continue
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                    out["added"].append(f"{table}.{col_name}")

            con.commit()
        except Exception as e:
            out["warnings"].append(repr(e))
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

        return out

    def _arch_mt_spot_move_bps(cur, seconds):
        cols = set(_arch_mt_table_cols(cur, "spot_samples"))
        if not cols:
            return None

        ts_col = None
        px_col = None
        for candidate in ("ts", "timestamp", "created_at"):
            if candidate in cols:
                ts_col = candidate
                break
        for candidate in ("spot", "price", "mid", "value"):
            if candidate in cols:
                px_col = candidate
                break

        if not ts_col or not px_col:
            return None

        try:
            rows = cur.execute(
                f"SELECT {ts_col}, {px_col} FROM spot_samples "
                f"WHERE {ts_col} IS NOT NULL AND {px_col} IS NOT NULL "
                f"ORDER BY {ts_col} DESC LIMIT 300"
            ).fetchall()
        except Exception:
            return None

        parsed = []
        for ts, px in rows:
            dt = _arch_mt_parse_iso(ts)
            px = _arch_mt_safe_float(px, None)
            if dt and px:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_arch_tz.utc)
                parsed.append((dt.astimezone(_arch_tz.utc), px))

        if len(parsed) < 2:
            return None

        latest_dt, latest_px = parsed[0]
        threshold = latest_dt.timestamp() - float(seconds)

        older = None
        for dt, px in parsed[1:]:
            if dt.timestamp() <= threshold:
                older = (dt, px)
                break

        if not older:
            older = parsed[-1]

        old_px = older[1]
        if not old_px:
            return None

        try:
            return round(abs((latest_px - old_px) / old_px) * 10000.0, 2)
        except Exception:
            return None

    def _arch_mt_count_not_null(cur, table, col):
        try:
            return cur.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL"
            ).fetchone()[0]
        except Exception:
            return None

    def _arch_mt_visible_market_adjustments(status, spot_move_bps_5s, spot_move_bps_10s):
        markets = []
        raw = []
        if isinstance(status, dict):
            raw = status.get("markets") or []

        tick_size = _arch_mt_tick_size()
        tox_penalty_5s = None
        tox_penalty_10s = None
        if spot_move_bps_5s is not None:
            tox_penalty_5s = round(min(max(spot_move_bps_5s / 10000.0, 0.0), 0.05), 4)
        if spot_move_bps_10s is not None:
            tox_penalty_10s = round(min(max(spot_move_bps_10s / 10000.0, 0.0), 0.08), 4)

        for row in raw:
            if not isinstance(row, dict):
                continue

            spread = _arch_mt_safe_float(
                row.get("spread", row.get("yes_spread", row.get("no_spread"))), None
            )
            best_edge = _arch_mt_safe_float(row.get("best_exec_edge"), None)
            maker_edge = _arch_mt_safe_float(
                row.get("maker_best_edge", row.get("maker_edge")), None
            )
            if best_edge is None and maker_edge is None:
                continue

            sl_ticks = _arch_mt_slippage_ticks(spread)
            sl_cost = round((sl_ticks or 0) * tick_size, 4) if sl_ticks is not None else None
            partial_fill = _arch_mt_partial_fill_pct(spread)
            weekend_flag = _arch_mt_weekend_flag(row.get("close_time"))

            item = {
                "ticker": row.get("ticker"),
                "best_side": row.get("best_side"),
                "distance_bucket": row.get("distance_bucket"),
                "hours_bucket": row.get("hours_bucket"),
                "spread": spread,
                "best_exec_edge": best_edge,
                "taker_exit_slippage_ticks_est": sl_ticks,
                "taker_exit_slippage_cost_est": sl_cost,
                "slippage_adjusted_taker_edge_est": (
                    round(best_edge - sl_cost, 4)
                    if best_edge is not None and sl_cost is not None else None
                ),
                "maker_best_edge": maker_edge,
                "maker_toxicity_penalty_est_5s": tox_penalty_5s,
                "maker_toxicity_penalty_est_10s": tox_penalty_10s,
                "maker_edge_after_toxicity_est_5s": (
                    round(maker_edge - tox_penalty_5s, 4)
                    if maker_edge is not None and tox_penalty_5s is not None else None
                ),
                "maker_edge_after_toxicity_est_10s": (
                    round(maker_edge - tox_penalty_10s, 4)
                    if maker_edge is not None and tox_penalty_10s is not None else None
                ),
                "partial_fill_pct_est": partial_fill,
                "queue_depth_proxy": None,
                "weekend_flag_est": weekend_flag,
            }
            item["_rank"] = max(
                item.get("slippage_adjusted_taker_edge_est") or -999,
                item.get("maker_edge_after_toxicity_est_5s") or -999,
            )
            markets.append(item)

        markets = sorted(markets, key=lambda x: x.get("_rank", -999), reverse=True)
        for item in markets:
            item.pop("_rank", None)
        return markets[:12]

    def _arch_mt_build_report(status):
        schema = _arch_mt_ensure_columns()

        out = {
            "ok": True,
            "policy": "replay_only__diagnostic_view__no_threshold_or_sizing_changes",
            "schema_update": schema,
            "db_path": schema.get("db_path"),
            "current_doctrine": {
                "mode": status.get("mode") if isinstance(status, dict) else None,
                "live_armed": status.get("live_armed") if isinstance(status, dict) else None,
                "threshold_change_allowed_now": False,
                "sizing_change_allowed_now": False,
                "live_promotion_allowed_now": False,
            },
            "penalty_assumptions": {
                "tick_size": 0.01,
                "taker_exit_slippage_rule": {
                    "spread_ge_0.08": 4,
                    "spread_ge_0.05": 3,
                    "spread_ge_0.03": 2,
                    "spread_ge_0.01": 1,
                },
                "partial_fill_rule": {
                    "spread_ge_0.08": 0.35,
                    "spread_ge_0.05": 0.50,
                    "spread_ge_0.03": 0.75,
                    "spread_lt_0.03": 0.90,
                },
                "maker_toxicity_windows_seconds": [5, 10],
                "weekend_flag_rule": "friday_21utc_to_monday_06utc",
                "status": "foundation_only__columns_and_reports_now__populate_and_score_next",
            },
            "spot_move_bps": {},
            "dataset_readiness": {},
            "visible_market_adjustments_top": [],
            "architect_next_step": None,
        }

        if isinstance(status, dict):
            out["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                or status.get("architect_next_step")
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )

        db = _arch_mt_find_db()
        if not db:
            out["ok"] = False
            out["error"] = "strategy_lab.db_not_found"
            return out

        con = None
        try:
            con = _sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            cur = con.cursor()

            spot_move_bps_5s = _arch_mt_spot_move_bps(cur, 5)
            spot_move_bps_10s = _arch_mt_spot_move_bps(cur, 10)
            out["spot_move_bps"] = {
                "move_5s_bps_abs": spot_move_bps_5s,
                "move_10s_bps_abs": spot_move_bps_10s,
            }

            # Dataset readiness counts
            readiness = {}

            for name, sql in {
                "cohorts_total": "SELECT COUNT(*) FROM execution_compare_cohorts",
                "cohorts_resolved": "SELECT COUNT(*) FROM execution_compare_cohorts WHERE resolved_at IS NOT NULL",
                "opps_total": "SELECT COUNT(*) FROM market_opportunities",
                "opps_resolved": "SELECT COUNT(*) FROM market_opportunities WHERE resolved_at IS NOT NULL",
            }.items():
                try:
                    readiness[name] = cur.execute(sql).fetchone()[0]
                except Exception as e:
                    readiness[name] = f"ERROR: {e!r}"

            for table, cols in {
                "execution_compare_cohorts": [
                    "requested_price",
                    "simulated_fill_price",
                    "partial_fill_pct",
                    "queue_depth_proxy",
                    "maker_toxicity_bps_5s",
                    "maker_toxicity_bps_10s",
                    "taker_exit_slippage_ticks",
                    "taker_exit_slippage_cost",
                    "weekend_flag",
                    "regime_vol_percentile",
                ],
                "market_opportunities": [
                    "slippage_penalty_ticks",
                    "slippage_penalty_cost",
                    "weekend_flag",
                    "regime_vol_percentile",
                ],
            }.items():
                for col in cols:
                    readiness[f"{table}.{col}.not_null"] = _arch_mt_count_not_null(cur, table, col)

            out["dataset_readiness"] = readiness
            out["visible_market_adjustments_top"] = _arch_mt_visible_market_adjustments(
                status,
                spot_move_bps_5s,
                spot_move_bps_10s,
            )

        except Exception as e:
            out["ok"] = False
            out["error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

        return out

    if not _arch_mt_route_exists("/api/debug/microstructure-truth-report"):
        @app.get("/api/debug/microstructure-truth-report")
        async def api_debug_microstructure_truth_report():
            status = await _arch_mt_safe_status()
            return JSONResponse(_arch_mt_build_report(status))

except Exception:
    pass



# --- architect btc near-expiry same-day override v1 ---
try:
    import os as _arch_os
    import inspect as _inspect
    from datetime import datetime as _arch_dt, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_btc_near_expiry_same_day_override_v1_applied = True

    def _arch_ne_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_ne_parse_dt(value):
        if not value:
            return None
        try:
            return _arch_dt.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None

    def _arch_ne_hours_to_expiry(market):
        hrs = _arch_ne_field(market, "hours_to_expiry", None)
        try:
            if hrs is not None:
                return float(hrs)
        except Exception:
            pass

        close_time = _arch_ne_field(market, "close_time", None)
        dt = _arch_ne_parse_dt(close_time)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        now = _arch_dt.now(_arch_tz.utc)
        return (dt.astimezone(_arch_tz.utc) - now).total_seconds() / 3600.0

    def _arch_ne_same_day_close(market):
        close_time = _arch_ne_field(market, "close_time", None)
        dt = _arch_ne_parse_dt(close_time)
        if not dt:
            return False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        now = _arch_dt.now(_arch_tz.utc)
        return dt.astimezone(_arch_tz.utc).date() == now.date()

    def _arch_ne_is_btc_range_bucket(market):
        ticker = str(_arch_ne_field(market, "ticker", "") or "").upper()
        asset = str(_arch_ne_field(market, "asset", "") or "").upper()
        family = str(_arch_ne_field(market, "market_family", "") or "").lower()
        title = str(_arch_ne_field(market, "title", "") or "").lower()
        subtitle = str(_arch_ne_field(market, "subtitle", "") or "").lower()

        if asset == "BTC" and family == "range_bucket":
            return True
        if ticker.startswith("KXBTC-") and ("price range" in title or "$" in subtitle):
            return True
        if "bitcoin price range" in title:
            return True
        return False

    def _arch_ne_best_spread(market):
        vals = []
        for key in ("spread", "yes_spread", "no_spread"):
            v = _arch_ne_field(market, key, None)
            try:
                if v is not None:
                    vals.append(float(v))
            except Exception:
                pass
        if vals:
            return min(vals)
        try:
            yes_bid = float(_arch_ne_field(market, "yes_bid", None))
            yes_ask = float(_arch_ne_field(market, "yes_ask", None))
            vals.append(max(0.0, yes_ask - yes_bid))
        except Exception:
            pass
        try:
            no_bid = float(_arch_ne_field(market, "no_bid", None))
            no_ask = float(_arch_ne_field(market, "no_ask", None))
            vals.append(max(0.0, no_ask - no_bid))
        except Exception:
            pass
        return min(vals) if vals else None

    def _arch_ne_reason(result):
        if not isinstance(result, dict):
            return None
        return (
            result.get("rejection_reason")
            or result.get("reason")
            or result.get("reject_reason")
            or result.get("status_reason")
        )

    def _arch_ne_make_allowed(result, market, hours_to_expiry, spread):
        if not isinstance(result, dict):
            return result
        out = dict(result)
        out["eligible"] = True
        for k in ("rejection_reason", "reason", "reject_reason", "status_reason"):
            if k in out:
                out[k] = None
        out["override_reason"] = "btc_range_bucket_same_day_near_expiry_allowed"
        out["override_meta"] = {
            "hours_to_expiry": hours_to_expiry,
            "spread": spread,
            "policy": {
                "same_day_only": True,
                "reopen_only_when_reason_is_too_close_to_expiry": True,
                "preserve_thresholds": True,
                "preserve_sizing": True,
                "preserve_live_guardrails": True,
            },
        }
        return out

    _arch_near_expiry_override_stats = {
        "seen": 0,
        "overridden": 0,
        "last_examples": [],
        "config": {
            "min_hours": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MIN_HOURS", "0.10")),
            "max_hours": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MAX_HOURS", "6.0")),
            "max_spread": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MAX_SPREAD", "0.08")),
            "same_day_only": True,
            "reason_required": "too_close_to_expiry",
        },
    }

    if hasattr(TradingBot, "_evaluate_market_eligibility"):
        _arch_prev_eval_market_eligibility_near = TradingBot._evaluate_market_eligibility

        def _architect_btc_near_expiry_same_day_override_v1(self, market, *args, **kwargs):
            result = _arch_prev_eval_market_eligibility_near(self, market, *args, **kwargs)
            try:
                _arch_near_expiry_override_stats["seen"] += 1

                if not isinstance(result, dict):
                    return result
                if result.get("eligible") is True:
                    return result

                reason = _arch_ne_reason(result)
                if reason != "too_close_to_expiry":
                    return result
                if not _arch_ne_is_btc_range_bucket(market):
                    return result
                if not _arch_ne_same_day_close(market):
                    return result

                hours_to_expiry = _arch_ne_hours_to_expiry(market)
                spread = _arch_ne_best_spread(market)
                cfg = _arch_near_expiry_override_stats["config"]

                if hours_to_expiry is None:
                    return result
                if not (cfg["min_hours"] <= float(hours_to_expiry) <= cfg["max_hours"]):
                    return result
                if spread is None or float(spread) > cfg["max_spread"]:
                    return result

                patched = _arch_ne_make_allowed(result, market, round(float(hours_to_expiry), 4), spread)
                _arch_near_expiry_override_stats["overridden"] += 1
                _arch_near_expiry_override_stats["last_examples"] = (
                    [{
                        "ticker": _arch_ne_field(market, "ticker"),
                        "hours_to_expiry": round(float(hours_to_expiry), 4),
                        "spread": spread,
                        "close_time": _arch_ne_field(market, "close_time"),
                    }]
                    + _arch_near_expiry_override_stats["last_examples"]
                )[:8]
                return patched
            except Exception:
                return result

        TradingBot._evaluate_market_eligibility = _architect_btc_near_expiry_same_day_override_v1

    def _arch_ne_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    async def _arch_ne_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_ne_route_exists("/api/debug/eligibility-policy-report"):
        @app.get("/api/debug/eligibility-policy-report")
        async def api_debug_eligibility_policy_report():
            status = await _arch_ne_safe_status()
            payload = {
                "ok": True,
                "policy": "same_day_near_expiry_btc_range_bucket_override__replay_safe__no_threshold_or_sizing_changes",
                "override_stats": _arch_near_expiry_override_stats,
                "market_pipeline": ((status.get("health") or {}).get("market_pipeline") or {}),
                "architect_next_step": (
                    (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                    or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                    or status.get("architect_next_step")
                    or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
                ),
            }
            return JSONResponse(payload)

except Exception:
    pass



# --- architect execution realism + eligibility v2 ---
try:
    import os as _arch_os
    import json as _arch_json
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from datetime import datetime as _arch_dt, timezone as _arch_tz, timedelta as _arch_td
    from fastapi.responses import JSONResponse

    _architect_execution_realism_and_eligibility_v2_applied = True

    _arch_exec_realism_override_stats = {
        "seen": 0,
        "overridden": 0,
        "last_examples": [],
        "veto_examples": [],
        "last_error": None,
        "config": {
            "min_hours": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MIN_HOURS_V2", "0.05")),
            "max_hours": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MAX_HOURS_V2", "6.0")),
            "max_spread": float(_arch_os.getenv("BTC_RANGE_BUCKET_NEAR_EXPIRY_MAX_SPREAD_V2", "0.08")),
            "same_day_only": True,
            "reason_mode": "soft_match_non_hard_veto",
        },
    }

    _arch_exec_realism_backfill_stats = {
        "runs": 0,
        "db_path": None,
        "opp_rows_updated": 0,
        "cohort_rows_updated": 0,
        "move_5s_bps": None,
        "move_10s_bps": None,
        "last_run_utc": None,
        "last_error": None,
    }

    def _arch_er_field(obj, *names, default=None):
        for name in names:
            try:
                if isinstance(obj, dict) and name in obj and obj.get(name) is not None:
                    return obj.get(name)
                if hasattr(obj, name):
                    value = getattr(obj, name)
                    if value is not None:
                        return value
            except Exception:
                pass
        return default

    def _arch_er_safe_float(value, default=None):
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _arch_er_parse_dt(value):
        if value is None:
            return None
        if isinstance(value, _arch_dt):
            dt = value
        else:
            try:
                dt = _arch_dt.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        return dt.astimezone(_arch_tz.utc)

    def _arch_er_close_dt(market):
        raw = _arch_er_field(market, "raw", default=None)
        candidates = [
            _arch_er_field(market, "close_dt", "close_time", "expected_expiration_time"),
            _arch_er_field(raw, "close_dt", "close_time", "expected_expiration_time") if isinstance(raw, dict) else None,
        ]
        for value in candidates:
            dt = _arch_er_parse_dt(value)
            if dt:
                return dt
        return None

    def _arch_er_hours_to_expiry(market):
        hrs = _arch_er_field(market, "hours_to_expiry", default=None)
        hrs = _arch_er_safe_float(hrs, None)
        if hrs is not None:
            return hrs
        dt = _arch_er_close_dt(market)
        if not dt:
            return None
        return (dt - _arch_dt.now(_arch_tz.utc)).total_seconds() / 3600.0

    def _arch_er_is_btc_range_bucket(market):
        asset = str(_arch_er_field(market, "asset", default="") or "").upper()
        family = str(_arch_er_field(market, "market_family", default="") or "").lower()
        ticker = str(_arch_er_field(market, "ticker", default="") or "").upper()
        title = str(_arch_er_field(market, "title", default="") or "").lower()
        subtitle = str(_arch_er_field(market, "subtitle", default="") or "").lower()

        if asset == "BTC" and family == "range_bucket":
            return True
        if ticker.startswith("KXBTC-") and ("price range" in title or "$" in subtitle):
            return True
        if "bitcoin price range" in title:
            return True
        return False

    def _arch_er_best_spread(market):
        vals = []

        for key in ("spread", "yes_spread", "no_spread"):
            v = _arch_er_safe_float(_arch_er_field(market, key, default=None), None)
            if v is not None:
                vals.append(v)

        yes_bid = _arch_er_safe_float(_arch_er_field(market, "yes_bid", default=None), None)
        yes_ask = _arch_er_safe_float(_arch_er_field(market, "yes_ask", default=None), None)
        no_bid = _arch_er_safe_float(_arch_er_field(market, "no_bid", default=None), None)
        no_ask = _arch_er_safe_float(_arch_er_field(market, "no_ask", default=None), None)

        if yes_bid is not None and yes_ask is not None:
            vals.append(max(0.0, yes_ask - yes_bid))
        if no_bid is not None and no_ask is not None:
            vals.append(max(0.0, no_ask - no_bid))

        return min(vals) if vals else None

    def _arch_er_reason_blob(result):
        if result is None:
            return ""
        if isinstance(result, dict):
            parts = []
            for k, v in result.items():
                if v is None:
                    continue
                if isinstance(v, (str, int, float, bool)):
                    parts.append(f"{k}={v}")
            return " | ".join(parts).lower()
        return str(result).lower()

    def _arch_er_hard_veto(reason_blob):
        tokens = [
            "spread_too_wide",
            "invalid",
            "missing_quote",
            "bad_price",
            "not_crypto",
            "not_range_bucket",
            "closed",
            "halted",
            "suspended",
            "not_tradeable",
        ]
        return any(tok in (reason_blob or "") for tok in tokens)

    def _arch_er_find_db():
        for helper_name in (
            "_arch_mt_find_db",
            "_arch_settlement_find_db",
            "_arch_maker_shadow_find_db",
            "_arch_regime_find_db",
            "_arch_find_strategy_lab_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (
            _Path("data/strategy_lab.db"),
            _Path("/app/data/strategy_lab.db"),
        ):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_er_slippage_ticks(spread):
        helper = globals().get("_arch_mt_slippage_ticks")
        if callable(helper):
            try:
                return helper(spread)
            except Exception:
                pass
        spread = _arch_er_safe_float(spread, None)
        if spread is None:
            return None
        if spread >= 0.08:
            return 4
        if spread >= 0.05:
            return 3
        if spread >= 0.03:
            return 2
        if spread >= 0.01:
            return 1
        return 0

    def _arch_er_partial_fill_pct(spread):
        helper = globals().get("_arch_mt_partial_fill_pct")
        if callable(helper):
            try:
                return helper(spread)
            except Exception:
                pass
        spread = _arch_er_safe_float(spread, None)
        if spread is None:
            return None
        if spread >= 0.08:
            return 0.35
        if spread >= 0.05:
            return 0.50
        if spread >= 0.03:
            return 0.75
        return 0.90

    def _arch_er_weekend_flag(value):
        helper = globals().get("_arch_mt_weekend_flag")
        if callable(helper):
            try:
                return helper(value)
            except Exception:
                pass

        dt = _arch_er_parse_dt(value)
        if not dt:
            return None
        wd = dt.weekday()  # Mon=0
        if wd == 4 and dt.hour >= 21:
            return 1
        if wd in (5, 6):
            return 1
        if wd == 0 and dt.hour < 6:
            return 1
        return 0

    def _arch_er_spot_move_bps(cur, seconds):
        helper = globals().get("_arch_mt_spot_move_bps")
        if callable(helper):
            try:
                return helper(cur, seconds)
            except Exception:
                pass
        return None

    if hasattr(TradingBot, "_evaluate_market_eligibility"):
        _arch_prev_eval_market_eligibility_v2 = TradingBot._evaluate_market_eligibility

        def _architect_execution_realism_and_eligibility_v2(self, market, *args, **kwargs):
            result = _arch_prev_eval_market_eligibility_v2(self, market, *args, **kwargs)
            try:
                _arch_exec_realism_override_stats["seen"] += 1

                if isinstance(result, dict) and result.get("eligible") is True:
                    return result
                if not _arch_er_is_btc_range_bucket(market):
                    return result

                cfg = _arch_exec_realism_override_stats["config"]
                dt = _arch_er_close_dt(market)
                hours = _arch_er_hours_to_expiry(market)
                spread = _arch_er_best_spread(market)
                reason_blob = _arch_er_reason_blob(result)
                now = _arch_dt.now(_arch_tz.utc)

                veto_reason = None
                if dt is None:
                    veto_reason = "missing_close_dt"
                elif cfg["same_day_only"] and dt.date() != now.date():
                    veto_reason = "not_same_day_utc"
                elif hours is None:
                    veto_reason = "missing_hours_to_expiry"
                elif not (cfg["min_hours"] <= float(hours) <= cfg["max_hours"]):
                    veto_reason = "outside_near_expiry_window"
                elif spread is None:
                    veto_reason = "missing_spread"
                elif float(spread) > cfg["max_spread"]:
                    veto_reason = "spread_guardrail"
                elif _arch_er_hard_veto(reason_blob):
                    veto_reason = "hard_veto_reason"

                if veto_reason:
                    _arch_exec_realism_override_stats["veto_examples"] = (
                        [{
                            "ticker": _arch_er_field(market, "ticker"),
                            "hours_to_expiry": round(float(hours), 4) if hours is not None else None,
                            "spread": spread,
                            "close_dt": dt.isoformat() if dt else None,
                            "veto_reason": veto_reason,
                            "reason_blob": reason_blob[:180],
                        }]
                        + _arch_exec_realism_override_stats["veto_examples"]
                    )[:8]
                    return result

                out = dict(result) if isinstance(result, dict) else {"original_result": result}
                out["eligible"] = True
                for k in ("rejection_reason", "reason", "reject_reason", "status_reason"):
                    if k in out:
                        out[k] = None
                out["override_reason"] = "btc_range_bucket_same_day_near_expiry_allowed_v2"
                out["override_meta"] = {
                    "hours_to_expiry": round(float(hours), 4) if hours is not None else None,
                    "spread": spread,
                    "reason_blob": reason_blob[:180],
                    "policy": {
                        "same_day_only": True,
                        "max_spread": cfg["max_spread"],
                        "min_hours": cfg["min_hours"],
                        "max_hours": cfg["max_hours"],
                        "preserve_thresholds": True,
                        "preserve_sizing": True,
                        "preserve_live_guardrails": True,
                    },
                }

                _arch_exec_realism_override_stats["overridden"] += 1
                _arch_exec_realism_override_stats["last_examples"] = (
                    [{
                        "ticker": _arch_er_field(market, "ticker"),
                        "hours_to_expiry": round(float(hours), 4) if hours is not None else None,
                        "spread": spread,
                        "close_dt": dt.isoformat() if dt else None,
                    }]
                    + _arch_exec_realism_override_stats["last_examples"]
                )[:8]
                return out
            except Exception as e:
                _arch_exec_realism_override_stats["last_error"] = repr(e)
                return result

        TradingBot._evaluate_market_eligibility = _architect_execution_realism_and_eligibility_v2

    def _arch_er_backfill_microstructure_fields():
        db = _arch_er_find_db()
        out = {
            "db_path": str(db) if db else None,
            "opp_rows_updated": 0,
            "cohort_rows_updated": 0,
            "move_5s_bps": None,
            "move_10s_bps": None,
            "last_run_utc": _arch_dt.now(_arch_tz.utc).isoformat(),
        }
        if not db:
            out["error"] = "strategy_lab.db_not_found"
            _arch_exec_realism_backfill_stats.update(out)
            _arch_exec_realism_backfill_stats["runs"] += 1
            return out

        con = None
        try:
            con = _sqlite3.connect(str(db))
            con.row_factory = _sqlite3.Row
            cur = con.cursor()

            move_5s = _arch_er_spot_move_bps(cur, 5)
            move_10s = _arch_er_spot_move_bps(cur, 10)
            out["move_5s_bps"] = move_5s
            out["move_10s_bps"] = move_10s

            opp_rows = cur.execute("""
                SELECT id, spread, close_time
                FROM market_opportunities
                WHERE slippage_penalty_ticks IS NULL
                   OR slippage_penalty_cost IS NULL
                   OR weekend_flag IS NULL
                ORDER BY id DESC
                LIMIT 1000
            """).fetchall()

            for row in opp_rows:
                spread = _arch_er_safe_float(row["spread"], None)
                ticks = _arch_er_slippage_ticks(spread)
                cost = round((ticks or 0) * 0.01, 4) if ticks is not None else None
                weekend = _arch_er_weekend_flag(row["close_time"])
                extra = _arch_json.dumps({
                    "source": "execution_realism_v2_backfill",
                    "spread": spread,
                    "slippage_ticks": ticks,
                })
                cur.execute("""
                    UPDATE market_opportunities
                    SET slippage_penalty_ticks = COALESCE(slippage_penalty_ticks, ?),
                        slippage_penalty_cost  = COALESCE(slippage_penalty_cost, ?),
                        weekend_flag           = COALESCE(weekend_flag, ?),
                        microstructure_extra_json = COALESCE(microstructure_extra_json, ?)
                    WHERE id = ?
                """, (ticks, cost, weekend, extra, row["id"]))
                out["opp_rows_updated"] += 1

            cohort_rows = cur.execute("""
                SELECT
                    id, ts, close_time, best_side, taker_side, maker_side,
                    yes_bid, yes_ask, no_bid, no_ask,
                    requested_price, simulated_fill_price, partial_fill_pct,
                    taker_exit_slippage_ticks, taker_exit_slippage_cost,
                    weekend_flag, maker_toxicity_bps_5s, maker_toxicity_bps_10s
                FROM execution_compare_cohorts
                WHERE requested_price IS NULL
                   OR simulated_fill_price IS NULL
                   OR partial_fill_pct IS NULL
                   OR taker_exit_slippage_ticks IS NULL
                   OR taker_exit_slippage_cost IS NULL
                   OR weekend_flag IS NULL
                   OR maker_toxicity_bps_5s IS NULL
                   OR maker_toxicity_bps_10s IS NULL
                ORDER BY id DESC
                LIMIT 1000
            """).fetchall()

            now = _arch_dt.now(_arch_tz.utc)

            for row in cohort_rows:
                side = (row["taker_side"] or row["best_side"] or row["maker_side"] or "yes").lower()

                yes_bid = _arch_er_safe_float(row["yes_bid"], None)
                yes_ask = _arch_er_safe_float(row["yes_ask"], None)
                no_bid  = _arch_er_safe_float(row["no_bid"], None)
                no_ask  = _arch_er_safe_float(row["no_ask"], None)

                if side == "no":
                    req_price = no_ask
                    spread = (no_ask - no_bid) if (no_ask is not None and no_bid is not None) else None
                else:
                    req_price = yes_ask
                    spread = (yes_ask - yes_bid) if (yes_ask is not None and yes_bid is not None) else None

                sim_fill = req_price
                partial_fill = _arch_er_partial_fill_pct(spread)
                ticks = _arch_er_slippage_ticks(spread)
                cost = round((ticks or 0) * 0.01, 4) if ticks is not None else None
                weekend = _arch_er_weekend_flag(row["close_time"])

                tox5 = None
                tox10 = None
                ts_dt = _arch_er_parse_dt(row["ts"])
                if ts_dt and (now - ts_dt) <= _arch_td(seconds=180):
                    tox5 = move_5s
                    tox10 = move_10s

                extra = _arch_json.dumps({
                    "source": "execution_realism_v2_backfill",
                    "side": side,
                    "spread": spread,
                    "slippage_ticks": ticks,
                    "partial_fill_pct": partial_fill,
                })

                cur.execute("""
                    UPDATE execution_compare_cohorts
                    SET requested_price          = COALESCE(requested_price, ?),
                        simulated_fill_price     = COALESCE(simulated_fill_price, ?),
                        partial_fill_pct         = COALESCE(partial_fill_pct, ?),
                        taker_exit_slippage_ticks= COALESCE(taker_exit_slippage_ticks, ?),
                        taker_exit_slippage_cost = COALESCE(taker_exit_slippage_cost, ?),
                        weekend_flag             = COALESCE(weekend_flag, ?),
                        maker_toxicity_bps_5s    = COALESCE(maker_toxicity_bps_5s, ?),
                        maker_toxicity_bps_10s   = COALESCE(maker_toxicity_bps_10s, ?),
                        microstructure_extra_json= COALESCE(microstructure_extra_json, ?)
                    WHERE id = ?
                """, (req_price, sim_fill, partial_fill, ticks, cost, weekend, tox5, tox10, extra, row["id"]))
                out["cohort_rows_updated"] += 1

            con.commit()
            _arch_exec_realism_backfill_stats.update(out)
            _arch_exec_realism_backfill_stats["runs"] += 1
            _arch_exec_realism_backfill_stats["last_error"] = None
            return out
        except Exception as e:
            out["error"] = repr(e)
            _arch_exec_realism_backfill_stats.update(out)
            _arch_exec_realism_backfill_stats["runs"] += 1
            _arch_exec_realism_backfill_stats["last_error"] = repr(e)
            return out
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_exec_realism_v2 = TradingBot.dashboard_status

        def _arch_exec_realism_dashboard_status_v2(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_exec_realism_v2(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    try:
                        bf = _arch_er_backfill_microstructure_fields()
                        if isinstance(resolved, dict):
                            arch = resolved.setdefault("architect", {})
                            arch["execution_realism_backfill"] = bf
                    except Exception:
                        pass
                    return resolved
                return _await_and_attach()

            try:
                bf = _arch_er_backfill_microstructure_fields()
                if isinstance(status, dict):
                    arch = status.setdefault("architect", {})
                    arch["execution_realism_backfill"] = bf
            except Exception:
                pass
            return status

        TradingBot.dashboard_status = _arch_exec_realism_dashboard_status_v2

    async def _arch_er_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    def _arch_er_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    if not _arch_er_route_exists("/api/debug/execution-realism-report"):
        @app.get("/api/debug/execution-realism-report")
        async def api_debug_execution_realism_report():
            status = await _arch_er_safe_status()
            backfill = _arch_er_backfill_microstructure_fields()

            if callable(globals().get("_arch_mt_build_report")):
                try:
                    payload = _arch_mt_build_report(status)
                except Exception as e:
                    payload = {"ok": False, "error": repr(e)}
            else:
                payload = {"ok": True}

            if not isinstance(payload, dict):
                payload = {"ok": True}

            payload["eligibility_override_v2"] = _arch_exec_realism_override_stats
            payload["execution_realism_backfill"] = backfill
            payload["market_pipeline"] = ((status.get("health") or {}).get("market_pipeline") or {}) if isinstance(status, dict) else {}
            payload["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                or status.get("architect_next_step")
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )
            return JSONResponse(payload)

except Exception:
    pass



# --- architect effective eligibility status v1 ---
try:
    import inspect as _inspect
    from fastapi.responses import JSONResponse

    _architect_effective_eligibility_status_v1_applied = True

    def _arch_eff_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_eff_reason_blob(result):
        if result is None:
            return ""
        if isinstance(result, dict):
            parts = []
            for k, v in result.items():
                if v is None:
                    continue
                if isinstance(v, (str, int, float, bool)):
                    parts.append(f"{k}={v}")
            return " | ".join(parts)
        return str(result)

    def _arch_eff_market_to_dict(m):
        if isinstance(m, dict):
            return dict(m)
        out = {}
        for name in (
            "ticker","title","subtitle","asset","market_family","status",
            "yes_bid","yes_ask","no_bid","no_ask","mid","spread",
            "close_dt","close_time","hours_to_expiry","distance_bucket","hours_bucket"
        ):
            try:
                val = getattr(m, name, None)
                if val is not None:
                    out[name] = val
            except Exception:
                pass
        return out

    def _arch_eff_collect_markets(self_ref, status):
        candidates = []

        if isinstance(status, dict):
            for key in ("markets", "market_snapshots", "visible_markets", "watched_markets"):
                v = status.get(key)
                if isinstance(v, list) and v:
                    candidates = v
                    break

        if not candidates:
            for attr in ("markets", "market_snapshots", "visible_markets", "watched_markets"):
                try:
                    v = getattr(self_ref, attr, None)
                    if isinstance(v, list) and v:
                        candidates = v
                        break
                except Exception:
                    pass

        return candidates or []

    def _arch_eff_compute_pipeline(self_ref, status):
        markets = _arch_eff_collect_markets(self_ref, status)

        out = {
            "raw_candidate_markets": len(markets),
            "eligible_markets": 0,
            "rejections": {},
            "rejection_examples": {},
            "eligible_examples": [],
            "overridden_examples": [],
        }

        eval_fn = getattr(self_ref, "_evaluate_market_eligibility", None)
        if not callable(eval_fn):
            out["error"] = "missing__evaluate_market_eligibility"
            return out

        for m in markets:
            md = _arch_eff_market_to_dict(m)
            try:
                result = eval_fn(m)
            except Exception as e:
                reason = f"eval_error:{e!r}"
                out["rejections"][reason] = out["rejections"].get(reason, 0) + 1
                out["rejection_examples"].setdefault(reason, []).append(md.get("ticker"))
                continue

            eligible = bool(isinstance(result, dict) and result.get("eligible") is True)
            reason_blob = _arch_eff_reason_blob(result)
            override_reason = result.get("override_reason") if isinstance(result, dict) else None

            if eligible:
                out["eligible_markets"] += 1
                sample = {
                    "ticker": md.get("ticker"),
                    "spread": md.get("spread"),
                    "close_dt": md.get("close_dt") or md.get("close_time"),
                }
                if len(out["eligible_examples"]) < 8:
                    out["eligible_examples"].append(sample)
                if override_reason and len(out["overridden_examples"]) < 8:
                    sample2 = dict(sample)
                    sample2["override_reason"] = override_reason
                    out["overridden_examples"].append(sample2)
                continue

            reason_key = "unknown_rejection"
            if isinstance(result, dict):
                reason_key = (
                    result.get("rejection_reason")
                    or result.get("reason")
                    or result.get("reject_reason")
                    or result.get("status_reason")
                    or reason_blob
                    or "unknown_rejection"
                )
            elif reason_blob:
                reason_key = reason_blob

            out["rejections"][reason_key] = out["rejections"].get(reason_key, 0) + 1
            ex = out["rejection_examples"].setdefault(reason_key, [])
            if len(ex) < 8:
                ex.append(md.get("ticker"))

        return out

    async def _arch_eff_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_eff_v1 = TradingBot.dashboard_status

        def _arch_effective_eligibility_dashboard_status_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_eff_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    try:
                        if isinstance(resolved, dict):
                            health = resolved.setdefault("health", {})
                            raw_mp = health.get("market_pipeline") or {}
                            eff = _arch_eff_compute_pipeline(self, resolved)
                            health["market_pipeline_effective"] = eff

                            merged = dict(raw_mp) if isinstance(raw_mp, dict) else {}
                            if eff.get("raw_candidate_markets") is not None:
                                merged.setdefault("raw_open_markets", eff.get("raw_candidate_markets"))
                            if eff.get("eligible_markets") is not None:
                                merged["eligible_markets"] = eff.get("eligible_markets")
                            if eff.get("rejections") is not None:
                                merged["rejections"] = eff.get("rejections")
                            if eff.get("rejection_examples") is not None:
                                merged["rejection_examples"] = eff.get("rejection_examples")
                            health["market_pipeline"] = merged
                    except Exception:
                        pass
                    return resolved
                return _await_and_attach()

            try:
                if isinstance(status, dict):
                    health = status.setdefault("health", {})
                    raw_mp = health.get("market_pipeline") or {}
                    eff = _arch_eff_compute_pipeline(self, status)
                    health["market_pipeline_effective"] = eff

                    merged = dict(raw_mp) if isinstance(raw_mp, dict) else {}
                    if eff.get("raw_candidate_markets") is not None:
                        merged.setdefault("raw_open_markets", eff.get("raw_candidate_markets"))
                    if eff.get("eligible_markets") is not None:
                        merged["eligible_markets"] = eff.get("eligible_markets")
                    if eff.get("rejections") is not None:
                        merged["rejections"] = eff.get("rejections")
                    if eff.get("rejection_examples") is not None:
                        merged["rejection_examples"] = eff.get("rejection_examples")
                    health["market_pipeline"] = merged
            except Exception:
                pass

            return status

        TradingBot.dashboard_status = _arch_effective_eligibility_dashboard_status_v1

    if not _arch_eff_route_exists("/api/debug/effective-eligibility-report"):
        @app.get("/api/debug/effective-eligibility-report")
        async def api_debug_effective_eligibility_report():
            status = await _arch_eff_safe_status()
            payload = {"ok": True}
            try:
                if "bot" in globals():
                    payload["effective_market_pipeline"] = _arch_eff_compute_pipeline(bot, status)
                else:
                    payload["effective_market_pipeline"] = {"error": "bot_missing"}
            except Exception as e:
                payload["effective_market_pipeline"] = {"error": repr(e)}

            if isinstance(status, dict):
                payload["status_market_pipeline"] = ((status.get("health") or {}).get("market_pipeline") or {})
                payload["architect_next_step"] = (
                    (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                    or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                    or status.get("architect_next_step")
                    or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
                )
            return JSONResponse(payload)

except Exception:
    pass



# --- architect market bootstrap truth v1 ---
try:
    import inspect as _inspect
    from fastapi.responses import JSONResponse

    _architect_market_bootstrap_truth_v1_applied = True

    def _arch_mb_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_mb_len(value):
        try:
            return len(value) if value is not None else 0
        except Exception:
            return None

    def _arch_mb_sample_tickers(value, limit=5):
        out = []
        try:
            if isinstance(value, dict):
                items = list(value.values())
            else:
                items = list(value or [])
            for item in items[:limit]:
                if isinstance(item, dict):
                    out.append(item.get("ticker") or item.get("event_ticker") or str(item)[:120])
                else:
                    out.append(getattr(item, "ticker", None) or str(item)[:120])
        except Exception:
            pass
        return out

    def _arch_mb_get_attr(obj, name, default=None):
        try:
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_mb_inventory(self_ref, status=None):
        status = status or {}
        health = (status.get("health") or {}) if isinstance(status, dict) else {}
        mp = (health.get("market_pipeline") or {}) if isinstance(health, dict) else {}

        inv = {
            "status_market_pipeline": {
                "raw_open_markets": mp.get("raw_open_markets"),
                "normalized_markets": mp.get("normalized_markets"),
                "classified_crypto_markets": mp.get("classified_crypto_markets"),
                "eligible_markets": mp.get("eligible_markets"),
                "shadow_candidate_markets": mp.get("shadow_candidate_markets"),
                "cached_crypto_ticker_count": mp.get("cached_crypto_ticker_count"),
                "raw_market_samples_count": _arch_mb_len(mp.get("raw_market_samples")),
                "watched_market_samples_count": _arch_mb_len(mp.get("watched_market_samples")),
            },
            "bot_sources": {},
            "refresh_methods": {},
        }

        source_names = [
            "cached_crypto_tickers",
            "market_snapshots",
            "markets",
            "market_cache",
            "watched_markets",
            "visible_markets",
            "raw_markets",
            "crypto_markets",
        ]
        for name in source_names:
            val = _arch_mb_get_attr(self_ref, name, None)
            inv["bot_sources"][name] = {
                "present": val is not None,
                "len": _arch_mb_len(val),
                "sample_tickers": _arch_mb_sample_tickers(val),
            }

        for name in (
            "refresh_markets",
            "_refresh_markets",
            "_fetch_cached_crypto_markets",
            "fetch_markets",
        ):
            fn = _arch_mb_get_attr(self_ref, name, None)
            inv["refresh_methods"][name] = callable(fn)

        return inv

    async def _arch_mb_try_refresh(self_ref):
        result = {"attempted": False, "method": None, "ok": False, "error": None}

        for name in ("refresh_markets", "_refresh_markets", "fetch_markets"):
            fn = _arch_mb_get_attr(self_ref, name, None)
            if not callable(fn):
                continue

            result["attempted"] = True
            result["method"] = name
            try:
                value = fn()
                if _inspect.isawaitable(value):
                    await value
                result["ok"] = True
                return result
            except Exception as e:
                result["error"] = repr(e)
                return result

        result["error"] = "no_refresh_method_found"
        return result

    def _arch_mb_collect_candidates(self_ref, status):
        # Prefer true in-memory snapshots/lists first
        for attr in ("market_snapshots", "markets", "watched_markets", "visible_markets"):
            val = _arch_mb_get_attr(self_ref, attr, None)
            if isinstance(val, list) and len(val) > 0:
                return val, f"bot.{attr}"

        # Fallback to status lists if present
        if isinstance(status, dict):
            for key in ("markets", "market_snapshots", "visible_markets", "watched_markets"):
                val = status.get(key)
                if isinstance(val, list) and len(val) > 0:
                    return val, f"status.{key}"

            mp = ((status.get("health") or {}).get("market_pipeline") or {})
            for key in ("watched_market_samples", "raw_market_samples"):
                val = mp.get(key)
                if isinstance(val, list) and len(val) > 0:
                    return val, f"status.health.market_pipeline.{key}"

        return [], None

    def _arch_mb_reason(result):
        if isinstance(result, dict):
            return (
                result.get("rejection_reason")
                or result.get("reason")
                or result.get("reject_reason")
                or result.get("status_reason")
                or "unknown_rejection"
            )
        return str(result) if result is not None else "unknown_rejection"

    def _arch_mb_effective_pipeline(self_ref, status):
        eval_fn = _arch_mb_get_attr(self_ref, "_evaluate_market_eligibility", None)
        markets, source_used = _arch_mb_collect_candidates(self_ref, status)

        out = {
            "source_used": source_used,
            "raw_candidate_markets": len(markets),
            "eligible_markets": 0,
            "rejections": {},
            "rejection_examples": {},
            "eligible_examples": [],
            "overridden_examples": [],
        }

        if not callable(eval_fn):
            out["error"] = "missing__evaluate_market_eligibility"
            return out

        for market in markets:
            try:
                result = eval_fn(market)
            except Exception as e:
                reason = f"eval_error:{e!r}"
                out["rejections"][reason] = out["rejections"].get(reason, 0) + 1
                continue

            if isinstance(result, dict) and result.get("eligible") is True:
                out["eligible_markets"] += 1
                ticker = None
                spread = None
                close_dt = None
                try:
                    if isinstance(market, dict):
                        ticker = market.get("ticker")
                        spread = market.get("spread")
                        close_dt = market.get("close_dt") or market.get("close_time")
                    else:
                        ticker = getattr(market, "ticker", None)
                        spread = getattr(market, "spread", None)
                        close_dt = getattr(market, "close_dt", None) or getattr(market, "close_time", None)
                except Exception:
                    pass

                sample = {"ticker": ticker, "spread": spread, "close_dt": close_dt}
                if len(out["eligible_examples"]) < 8:
                    out["eligible_examples"].append(sample)

                if result.get("override_reason") and len(out["overridden_examples"]) < 8:
                    sample2 = dict(sample)
                    sample2["override_reason"] = result.get("override_reason")
                    out["overridden_examples"].append(sample2)
                continue

            reason = _arch_mb_reason(result)
            out["rejections"][reason] = out["rejections"].get(reason, 0) + 1
            ex = out["rejection_examples"].setdefault(reason, [])
            if len(ex) < 8:
                try:
                    ex.append(market.get("ticker") if isinstance(market, dict) else getattr(market, "ticker", None))
                except Exception:
                    ex.append(None)

        return out

    async def _arch_mb_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_mb_v1 = TradingBot.dashboard_status

        def _arch_market_bootstrap_truth_dashboard_status_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_mb_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    try:
                        if isinstance(resolved, dict):
                            health = resolved.setdefault("health", {})
                            eff = _arch_mb_effective_pipeline(self, resolved)
                            health["market_pipeline_effective"] = eff

                            # Non-destructive: only overwrite top-level counters if we have real candidates
                            if eff.get("raw_candidate_markets", 0) > 0:
                                mp = dict(health.get("market_pipeline") or {})
                                mp["eligible_markets"] = eff.get("eligible_markets")
                                mp["rejections"] = eff.get("rejections")
                                mp["rejection_examples"] = eff.get("rejection_examples")
                                health["market_pipeline"] = mp
                    except Exception:
                        pass
                    return resolved
                return _await_and_attach()

            try:
                if isinstance(status, dict):
                    health = status.setdefault("health", {})
                    eff = _arch_mb_effective_pipeline(self, status)
                    health["market_pipeline_effective"] = eff

                    if eff.get("raw_candidate_markets", 0) > 0:
                        mp = dict(health.get("market_pipeline") or {})
                        mp["eligible_markets"] = eff.get("eligible_markets")
                        mp["rejections"] = eff.get("rejections")
                        mp["rejection_examples"] = eff.get("rejection_examples")
                        health["market_pipeline"] = mp
            except Exception:
                pass

            return status

        TradingBot.dashboard_status = _arch_market_bootstrap_truth_dashboard_status_v1

    if not _arch_mb_route_exists("/api/debug/market-bootstrap-truth"):
        @app.get("/api/debug/market-bootstrap-truth")
        async def api_debug_market_bootstrap_truth():
            status = await _arch_mb_safe_status()
            payload = {"ok": True}

            if "bot" not in globals():
                payload["error"] = "bot_missing"
                return JSONResponse(payload)

            payload["before_inventory"] = _arch_mb_inventory(bot, status)
            payload["before_effective_pipeline"] = _arch_mb_effective_pipeline(bot, status)

            should_refresh = (
                (payload["before_effective_pipeline"].get("raw_candidate_markets", 0) == 0) and
                (
                    ((payload["before_inventory"].get("bot_sources") or {}).get("cached_crypto_tickers") or {}).get("len", 0)
                    or ((payload["before_inventory"].get("status_market_pipeline") or {}).get("cached_crypto_ticker_count"))
                )
            )

            payload["refresh_probe"] = {"attempted": False, "ok": False, "reason": "not_needed"}
            if should_refresh:
                payload["refresh_probe"] = await _arch_mb_try_refresh(bot)

                try:
                    status = await _arch_mb_safe_status()
                except Exception:
                    pass

            payload["after_inventory"] = _arch_mb_inventory(bot, status)
            payload["after_effective_pipeline"] = _arch_mb_effective_pipeline(bot, status)
            payload["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                or status.get("architect_next_step")
                or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
            )

            return JSONResponse(payload)

except Exception:
    pass



# --- architect cached ticker shape fix v1 ---
try:
    import inspect as _inspect
    from fastapi.responses import JSONResponse

    _architect_cached_ticker_shape_fix_v1_applied = True

    _arch_ct_shape_fix_stats = {
        "refresh_calls": 0,
        "refresh_retries": 0,
        "fetch_calls": 0,
        "coercions": 0,
        "last_original_type": None,
        "last_original_len": None,
        "last_coerced_len": None,
        "last_error": None,
        "last_method": None,
        "preview_pairs": [],
    }

    def _arch_ct_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_ct_safe_len(value):
        try:
            return len(value) if value is not None else 0
        except Exception:
            return None

    def _arch_ct_to_pair_iter(value):
        _arch_ct_shape_fix_stats["last_original_type"] = type(value).__name__ if value is not None else None
        _arch_ct_shape_fix_stats["last_original_len"] = _arch_ct_safe_len(value)

        out = []

        if isinstance(value, dict):
            for idx, (ticker, payload) in enumerate(value.items()):
                metric = None
                if isinstance(payload, dict):
                    for key in ("distance_from_spot_pct", "distance_pct", "distance", "rank", "score"):
                        try:
                            if payload.get(key) is not None:
                                metric = float(payload.get(key))
                                break
                        except Exception:
                            pass
                if metric is None:
                    metric = float(idx)
                out.append((ticker, metric))

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, tuple) and len(item) == 2:
                    out.append(item)
                elif isinstance(item, dict):
                    ticker = item.get("ticker") or item.get("event_ticker") or item.get("symbol")
                    if ticker:
                        metric = None
                        for key in ("distance_from_spot_pct", "distance_pct", "distance", "rank", "score"):
                            try:
                                if item.get(key) is not None:
                                    metric = float(item.get(key))
                                    break
                            except Exception:
                                pass
                        if metric is None:
                            metric = float(idx)
                        out.append((ticker, metric))
                elif isinstance(item, str):
                    out.append((item, float(idx)))

        _arch_ct_shape_fix_stats["last_coerced_len"] = len(out)
        _arch_ct_shape_fix_stats["preview_pairs"] = out[:8]
        if out:
            _arch_ct_shape_fix_stats["coercions"] += 1
        return out if out else value

    def _arch_ct_with_pairs(self_ref, fn, method_name, *args, **kwargs):
        original = getattr(self_ref, "cached_crypto_tickers", None)
        coerced = _arch_ct_to_pair_iter(original)

        # Only swap in if coercion actually changed shape meaningfully
        should_swap = (
            original is not None and
            coerced is not original and
            isinstance(coerced, list)
        )

        if should_swap:
            setattr(self_ref, "cached_crypto_tickers", coerced)

        try:
            _arch_ct_shape_fix_stats["last_method"] = method_name
            return fn(*args, **kwargs)
        finally:
            if should_swap:
                try:
                    setattr(self_ref, "cached_crypto_tickers", original)
                except Exception:
                    pass

    if hasattr(TradingBot, "_fetch_cached_crypto_markets"):
        _arch_prev_fetch_cached_crypto_markets_v1 = TradingBot._fetch_cached_crypto_markets

        def _arch_cached_ticker_shape_fix_fetch_v1(self, *args, **kwargs):
            _arch_ct_shape_fix_stats["fetch_calls"] += 1
            try:
                return _arch_ct_with_pairs(
                    self,
                    lambda *a, **k: _arch_prev_fetch_cached_crypto_markets_v1(self, *a, **k),
                    "_fetch_cached_crypto_markets",
                    *args, **kwargs
                )
            except Exception as e:
                _arch_ct_shape_fix_stats["last_error"] = repr(e)
                raise

        TradingBot._fetch_cached_crypto_markets = _arch_cached_ticker_shape_fix_fetch_v1

    if hasattr(TradingBot, "refresh_markets"):
        _arch_prev_refresh_markets_v1 = TradingBot.refresh_markets

        def _arch_cached_ticker_shape_fix_refresh_v1(self, *args, **kwargs):
            _arch_ct_shape_fix_stats["refresh_calls"] += 1

            try:
                result = _arch_prev_refresh_markets_v1(self, *args, **kwargs)
                if _inspect.isawaitable(result):
                    async def _await_result():
                        try:
                            return await result
                        except ValueError as e:
                            if "too many values to unpack" not in str(e):
                                _arch_ct_shape_fix_stats["last_error"] = repr(e)
                                raise
                            _arch_ct_shape_fix_stats["refresh_retries"] += 1
                            retry = _arch_ct_with_pairs(
                                self,
                                lambda *a, **k: _arch_prev_refresh_markets_v1(self, *a, **k),
                                "refresh_markets_retry",
                                *args, **kwargs
                            )
                            if _inspect.isawaitable(retry):
                                return await retry
                            return retry
                    return _await_result()
                return result

            except ValueError as e:
                if "too many values to unpack" not in str(e):
                    _arch_ct_shape_fix_stats["last_error"] = repr(e)
                    raise
                _arch_ct_shape_fix_stats["refresh_retries"] += 1
                return _arch_ct_with_pairs(
                    self,
                    lambda *a, **k: _arch_prev_refresh_markets_v1(self, *a, **k),
                    "refresh_markets_retry",
                    *args, **kwargs
                )
            except Exception as e:
                _arch_ct_shape_fix_stats["last_error"] = repr(e)
                raise

        TradingBot.refresh_markets = _arch_cached_ticker_shape_fix_refresh_v1

    async def _arch_ct_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_ct_route_exists("/api/debug/cached-ticker-shape-fix"):
        @app.get("/api/debug/cached-ticker-shape-fix")
        async def api_debug_cached_ticker_shape_fix():
            status = await _arch_ct_safe_status()
            payload = {
                "ok": True,
                "shape_fix_stats": _arch_ct_shape_fix_stats,
                "market_pipeline": ((status.get("health") or {}).get("market_pipeline") or {}) if isinstance(status, dict) else {},
                "market_pipeline_effective": ((status.get("health") or {}).get("market_pipeline_effective") or {}) if isinstance(status, dict) else {},
                "architect_next_step": (
                    (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                    or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                    or status.get("architect_next_step")
                    or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
                ),
            }
            try:
                if "bot" in globals():
                    payload["cached_crypto_tickers_type"] = type(getattr(bot, "cached_crypto_tickers", None)).__name__
                    payload["cached_crypto_tickers_len"] = _arch_ct_safe_len(getattr(bot, "cached_crypto_tickers", None))
                    payload["market_snapshots_len"] = _arch_ct_safe_len(getattr(bot, "market_snapshots", None))
            except Exception:
                pass
            return JSONResponse(payload)

except Exception:
    pass



# --- architect manual refresh fallback v1 ---
try:
    import inspect as _inspect
    from datetime import datetime as _arch_dt, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_manual_refresh_fallback_v1_applied = True

    _arch_manual_refresh_stats = {
        "refresh_calls": 0,
        "fallback_calls": 0,
        "fallback_success": 0,
        "fallback_errors": 0,
        "last_error": None,
        "last_run_utc": None,
        "raw_count": 0,
        "normalized_count": 0,
        "classified_count": 0,
        "eligible_count": 0,
        "snapshot_count": 0,
        "eligible_examples": [],
        "rejection_examples": [],
    }

    def _arch_mrf_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_mrf_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_mrf_reason(result):
        if isinstance(result, dict):
            return (
                result.get("rejection_reason")
                or result.get("reason")
                or result.get("reject_reason")
                or result.get("status_reason")
                or "unknown_rejection"
            )
        return str(result) if result is not None else "unknown_rejection"

    def _arch_mrf_safe_call(fn, *args, **kwargs):
        value = fn(*args, **kwargs)
        if _inspect.isawaitable(value):
            return value
        return value

    async def _arch_mrf_manual_refresh(self):
        _arch_manual_refresh_stats["fallback_calls"] += 1
        _arch_manual_refresh_stats["last_run_utc"] = _arch_dt.now(_arch_tz.utc).isoformat()
        _arch_manual_refresh_stats["last_error"] = None
        _arch_manual_refresh_stats["eligible_examples"] = []
        _arch_manual_refresh_stats["rejection_examples"] = []

        raw_markets = []
        try:
            fetch_fn = getattr(self, "_fetch_cached_crypto_markets", None)
            if not callable(fetch_fn):
                raise RuntimeError("missing _fetch_cached_crypto_markets")
            raw_markets = fetch_fn()
            if _inspect.isawaitable(raw_markets):
                raw_markets = await raw_markets
            raw_markets = list(raw_markets or [])
            _arch_manual_refresh_stats["raw_count"] = len(raw_markets)

            normalized = []
            norm_fn = getattr(self, "_normalize_market", None)
            for raw in raw_markets:
                if callable(norm_fn):
                    n = norm_fn(raw)
                    if _inspect.isawaitable(n):
                        n = await n
                else:
                    n = raw
                if n:
                    normalized.append(n)
            _arch_manual_refresh_stats["normalized_count"] = len(normalized)

            classified = []
            classify_fn = getattr(self, "_classify_market", None)
            for item in normalized:
                if callable(classify_fn):
                    c = classify_fn(item)
                    if _inspect.isawaitable(c):
                        c = await c
                else:
                    c = item
                if c:
                    classified.append(c)
            _arch_manual_refresh_stats["classified_count"] = len(classified)

            enrich_fn = getattr(self, "_enrich_with_fair_value", None)
            if callable(enrich_fn):
                try:
                    maybe = enrich_fn(classified)
                    if _inspect.isawaitable(maybe):
                        maybe = await maybe
                    if isinstance(maybe, list):
                        classified = maybe
                except Exception:
                    pass

            eval_fn = getattr(self, "_evaluate_market_eligibility", None)
            snapshots = []
            rejected = {}
            if not callable(eval_fn):
                raise RuntimeError("missing _evaluate_market_eligibility")

            for item in classified:
                result = eval_fn(item)
                if _inspect.isawaitable(result):
                    result = await result

                if isinstance(result, dict) and result.get("eligible") is True:
                    snapshots.append(item)
                    if len(_arch_manual_refresh_stats["eligible_examples"]) < 8:
                        _arch_manual_refresh_stats["eligible_examples"].append({
                            "ticker": _arch_mrf_field(item, "ticker"),
                            "spread": _arch_mrf_field(item, "spread"),
                            "close_dt": _arch_mrf_field(item, "close_dt") or _arch_mrf_field(item, "close_time"),
                            "override_reason": result.get("override_reason") if isinstance(result, dict) else None,
                        })
                else:
                    reason = _arch_mrf_reason(result)
                    rejected[reason] = rejected.get(reason, 0) + 1
                    if len(_arch_manual_refresh_stats["rejection_examples"]) < 8:
                        _arch_manual_refresh_stats["rejection_examples"].append({
                            "ticker": _arch_mrf_field(item, "ticker"),
                            "reason": reason,
                            "spread": _arch_mrf_field(item, "spread"),
                        })

            self.market_snapshots = snapshots
            _arch_manual_refresh_stats["eligible_count"] = len(snapshots)
            _arch_manual_refresh_stats["snapshot_count"] = len(snapshots)

            try:
                self.last_market_refresh = _arch_dt.now(_arch_tz.utc).isoformat()
            except Exception:
                pass

            try:
                self._market_pipeline = {
                    "raw_open_markets": len(raw_markets),
                    "normalized_markets": len(normalized),
                    "classified_crypto_markets": len(classified),
                    "eligible_markets": len(snapshots),
                    "cached_crypto_ticker_count": len(getattr(self, "cached_crypto_tickers", []) or []),
                    "rejections": rejected,
                    "rejection_examples": {},
                    "raw_market_samples": [
                        {
                            "ticker": _arch_mrf_field(x, "ticker"),
                            "title": _arch_mrf_field(x, "title"),
                            "subtitle": _arch_mrf_field(x, "subtitle"),
                            "status": _arch_mrf_field(x, "status"),
                        }
                        for x in raw_markets[:10]
                    ],
                    "watched_market_samples": [
                        {
                            "ticker": _arch_mrf_field(x, "ticker"),
                            "title": _arch_mrf_field(x, "title"),
                            "asset": _arch_mrf_field(x, "asset"),
                            "market_family": _arch_mrf_field(x, "market_family"),
                            "status": _arch_mrf_field(x, "status"),
                            "distance_from_spot_pct": _arch_mrf_field(x, "distance_from_spot_pct"),
                            "spread": _arch_mrf_field(x, "spread"),
                        }
                        for x in snapshots[:10]
                    ],
                }
            except Exception:
                pass

            _arch_manual_refresh_stats["fallback_success"] += 1
            return snapshots

        except Exception as e:
            _arch_manual_refresh_stats["fallback_errors"] += 1
            _arch_manual_refresh_stats["last_error"] = repr(e)
            raise

    if hasattr(TradingBot, "refresh_markets"):
        _arch_prev_refresh_markets_manual_v1 = TradingBot.refresh_markets

        def _arch_manual_refresh_wrapper_v1(self, *args, **kwargs):
            _arch_manual_refresh_stats["refresh_calls"] += 1
            try:
                result = _arch_prev_refresh_markets_manual_v1(self, *args, **kwargs)
                if _inspect.isawaitable(result):
                    async def _await_result():
                        try:
                            return await result
                        except ValueError as e:
                            if "too many values to unpack" not in str(e):
                                _arch_manual_refresh_stats["last_error"] = repr(e)
                                raise
                            return await _arch_mrf_manual_refresh(self)
                    return _await_result()
                return result
            except ValueError as e:
                if "too many values to unpack" not in str(e):
                    _arch_manual_refresh_stats["last_error"] = repr(e)
                    raise
                coro = _arch_mrf_manual_refresh(self)
                return coro
            except Exception as e:
                _arch_manual_refresh_stats["last_error"] = repr(e)
                raise

        TradingBot.refresh_markets = _arch_manual_refresh_wrapper_v1

    async def _arch_mrf_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_mrf_route_exists("/api/debug/manual-refresh-fallback"):
        @app.get("/api/debug/manual-refresh-fallback")
        async def api_debug_manual_refresh_fallback():
            status = await _arch_mrf_safe_status()
            payload = {
                "ok": True,
                "manual_refresh_stats": _arch_manual_refresh_stats,
                "market_pipeline": ((status.get("health") or {}).get("market_pipeline") or {}) if isinstance(status, dict) else {},
                "market_pipeline_effective": ((status.get("health") or {}).get("market_pipeline_effective") or {}) if isinstance(status, dict) else {},
                "architect_next_step": (
                    (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                    or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                    or status.get("architect_next_step")
                    or "keep_collecting_resolved_rows_before_threshold_or_sizing_changes"
                ),
            }
            try:
                if "bot" in globals():
                    payload["market_snapshots_len"] = len(getattr(bot, "market_snapshots", []) or [])
                    payload["cached_crypto_tickers_len"] = len(getattr(bot, "cached_crypto_tickers", []) or [])
            except Exception:
                pass
            return JSONResponse(payload)

except Exception:
    pass



# --- architect snapshot shape recovery v1 ---
try:
    import inspect as _inspect
    from fastapi.responses import JSONResponse

    _architect_snapshot_shape_recovery_v1_applied = True

    class _ArchSnapshotAdapter(dict):
        def to_dict(self):
            return dict(self)

    _arch_snapshot_shape_recovery_stats = {
        "dashboard_repairs": 0,
        "refresh_repairs": 0,
        "last_before_type": None,
        "last_after_len": None,
        "last_error": None,
        "sample_keys": [],
    }

    def _arch_ssr_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_ssr_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_ssr_as_snapshot_obj(item):
        if item is None:
            return None
        if hasattr(item, "to_dict") and callable(getattr(item, "to_dict", None)):
            return item
        if isinstance(item, dict):
            return _ArchSnapshotAdapter(item)
        # Last-resort shallow adapter from object attrs
        data = {}
        for key in (
            "ticker","title","subtitle","asset","market_family","status",
            "yes_bid","yes_ask","no_bid","no_ask","mid","spread","edge",
            "distance_from_spot_pct","hours_to_expiry","close_dt","close_time"
        ):
            val = _arch_ssr_field(item, key, None)
            if val is not None:
                data[key] = val
        return _ArchSnapshotAdapter(data)

    def _arch_ssr_normalize_market_snapshots(value):
        _arch_snapshot_shape_recovery_stats["last_before_type"] = type(value).__name__ if value is not None else None

        out = {}

        if isinstance(value, dict):
            for key, item in value.items():
                obj = _arch_ssr_as_snapshot_obj(item)
                if obj is None:
                    continue
                ticker = _arch_ssr_field(obj, "ticker", None) or key
                out[str(ticker)] = obj

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                obj = _arch_ssr_as_snapshot_obj(item)
                if obj is None:
                    continue
                ticker = _arch_ssr_field(obj, "ticker", None) or f"snapshot_{idx}"
                out[str(ticker)] = obj

        elif value is None:
            out = {}

        else:
            obj = _arch_ssr_as_snapshot_obj(value)
            if obj is not None:
                ticker = _arch_ssr_field(obj, "ticker", None) or "snapshot_0"
                out[str(ticker)] = obj

        _arch_snapshot_shape_recovery_stats["last_after_len"] = len(out)
        _arch_snapshot_shape_recovery_stats["sample_keys"] = list(out.keys())[:8]
        return out

    def _arch_ssr_repair(self, source):
        try:
            current = getattr(self, "market_snapshots", None)
            if isinstance(current, dict):
                # still normalize dict values in case they are raw dicts
                needs_repair = any(not hasattr(v, "to_dict") for v in current.values())
                if not needs_repair:
                    return False
            repaired = _arch_ssr_normalize_market_snapshots(current)
            setattr(self, "market_snapshots", repaired)
            if source == "dashboard":
                _arch_snapshot_shape_recovery_stats["dashboard_repairs"] += 1
            else:
                _arch_snapshot_shape_recovery_stats["refresh_repairs"] += 1
            return True
        except Exception as e:
            _arch_snapshot_shape_recovery_stats["last_error"] = repr(e)
            return False

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_snapshot_recovery_v1 = TradingBot.dashboard_status

        def _arch_snapshot_shape_recovery_dashboard_status_v1(self, *args, **kwargs):
            _arch_ssr_repair(self, "dashboard")
            status = _arch_prev_dashboard_status_snapshot_recovery_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_return():
                    return await status
                return _await_and_return()

            return status

        TradingBot.dashboard_status = _arch_snapshot_shape_recovery_dashboard_status_v1

    if hasattr(TradingBot, "refresh_markets"):
        _arch_prev_refresh_markets_snapshot_recovery_v1 = TradingBot.refresh_markets

        def _arch_snapshot_shape_recovery_refresh_v1(self, *args, **kwargs):
            result = _arch_prev_refresh_markets_snapshot_recovery_v1(self, *args, **kwargs)

            if _inspect.isawaitable(result):
                async def _await_and_repair():
                    out = await result
                    _arch_ssr_repair(self, "refresh")
                    return out
                return _await_and_repair()

            _arch_ssr_repair(self, "refresh")
            return result

        TradingBot.refresh_markets = _arch_snapshot_shape_recovery_refresh_v1

    if not _arch_ssr_route_exists("/api/debug/snapshot-shape-recovery"):
        @app.get("/api/debug/snapshot-shape-recovery")
        async def api_debug_snapshot_shape_recovery():
            payload = {
                "ok": True,
                "stats": _arch_snapshot_shape_recovery_stats,
            }
            try:
                if "bot" in globals():
                    ms = getattr(bot, "market_snapshots", None)
                    payload["market_snapshots_type"] = type(ms).__name__ if ms is not None else None
                    payload["market_snapshots_len"] = len(ms) if ms is not None else 0
                    payload["market_snapshots_sample_keys"] = list(ms.keys())[:8] if isinstance(ms, dict) else []
            except Exception as e:
                payload["error"] = repr(e)
            return JSONResponse(payload)

except Exception:
    pass



# --- architect runtime roadmap + p0 health v1 ---
try:
    import inspect as _inspect
    from datetime import datetime as _arch_dt, date as _arch_date, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_runtime_roadmap_and_p0_health_v1_applied = True

    _ARCH_RUNTIME_ROADMAP_V1 = {
        "source_of_truth_policy": {
            "bundle_is_source_of_truth": True,
            "memory_update_required_with_every_meaningful_patch": True,
            "check_off_completed_items_in_memory": True,
            "current_doctrine": {
                "paper_only": True,
                "live_disarmed": True,
                "no_threshold_changes_until_resolved_evidence": True,
                "no_sizing_changes_until_resolved_evidence": True
            }
        },
        "phases": [
            {
                "id": "P0",
                "name": "Runtime health and snapshot invariants",
                "status": "active",
                "goal": "Restore stable /api/status, stable dashboard loop, snapshot shape invariants, and JSON-safe responses.",
                "done_definition": [
                    "market_snapshots is always a dict keyed by ticker",
                    "snapshot objects expose downstream-required fields like spot_symbol",
                    "dashboard_status returns JSON-safe payloads with no raw datetime leaks",
                    "loop_error is null across repeated checks"
                ],
                "current_known_failures": [
                    "snapshot adapter / snapshot shape regressions",
                    "datetime leaking into /api/status JSON serialization"
                ]
            },
            {
                "id": "P1",
                "name": "Expiry-cluster inventory skew control",
                "status": "planned",
                "goal": "Add Hummingbot/Avellaneda-style inventory controller across correlated BTC buckets for the same expiry.",
                "done_definition": [
                    "cluster inventory state visible in debug surfaces",
                    "candidate rows show inventory penalty / side suppression / quote-size multiplier",
                    "replay can compare pre- and post-inventory-adjusted maker/taker expectancy"
                ]
            },
            {
                "id": "P2",
                "name": "Maker quoting + LIP rebate replay simulation",
                "status": "planned",
                "goal": "Simulate maker economics as spread capture + queue realism + toxicity + Kalshi LIP rebate.",
                "done_definition": [
                    "replay compares taker expectancy vs maker ex-rebate vs maker inc-rebate",
                    "quote distance / queue / partial-fill / rebate estimates stored in cohort rows",
                    "maker rebate debug surface exists"
                ]
            },
            {
                "id": "P3",
                "name": "Queue, imbalance, and toxic-fill telemetry",
                "status": "planned",
                "goal": "Add markouts, queue-ahead proxy, imbalance, and maker shield telemetry."
            },
            {
                "id": "P4",
                "name": "Ensemble fair-value layer",
                "status": "planned",
                "goal": "Upgrade from single-model fair value to ensemble replay calibration."
            },
            {
                "id": "P5",
                "name": "Projected-settlement / final-minute replay logic",
                "status": "planned",
                "goal": "Model final-minute settlement/index behavior in replay."
            }
        ],
        "next_patch_after_p0": [
            "Implement expiry-cluster inventory state and scenario payout surface",
            "Add maker rebate estimator fields and debug route",
            "Keep all of it replay-only until evidence matures"
        ]
    }

    _arch_rr_p0_stats = {
        "dashboard_repairs": 0,
        "refresh_repairs": 0,
        "json_safe_conversions": 0,
        "last_market_snapshots_type_before": None,
        "last_market_snapshots_len_after": None,
        "last_error": None,
    }

    def _arch_rr_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_rr_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_rr_guess_spot_symbol(item):
        for key in ("spot_symbol", "asset", "symbol"):
            val = _arch_rr_field(item, key, None)
            if val:
                return str(val).upper()
        ticker = str(_arch_rr_field(item, "ticker", "") or "").upper()
        if ticker.startswith("KXBTC-"):
            return "BTC"
        if ticker.startswith("KXETH-"):
            return "ETH"
        if ticker.startswith("KXSOL-"):
            return "SOL"
        return None

    def _arch_rr_json_safe(value):
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, (_arch_dt, _arch_date)):
            _arch_rr_p0_stats["json_safe_conversions"] += 1
            return value.isoformat()
        if isinstance(value, dict):
            return {str(k): _arch_rr_json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [_arch_rr_json_safe(v) for v in value]
        if hasattr(value, "to_dict") and callable(getattr(value, "to_dict", None)):
            try:
                return _arch_rr_json_safe(value.to_dict())
            except Exception:
                pass
        if hasattr(value, "__dict__"):
            try:
                return _arch_rr_json_safe(vars(value))
            except Exception:
                pass
        return repr(value)

    class _ArchSnapshotAdapterV2(dict):
        def __getattr__(self, name):
            if name in self:
                return self[name]
            if name == "spot_symbol":
                return self.get("spot_symbol")
            raise AttributeError(name)

        def to_dict(self):
            return _arch_rr_json_safe(dict(self))

    def _arch_rr_as_snapshot_obj(item):
        if item is None:
            return None

        if hasattr(item, "to_dict") and callable(getattr(item, "to_dict", None)) and hasattr(item, "spot_symbol"):
            return item

        data = {}
        if isinstance(item, dict):
            data.update(item)
        else:
            for key in (
                "ticker","title","subtitle","asset","market_family","status",
                "yes_bid","yes_ask","no_bid","no_ask","mid","spread","edge",
                "distance_from_spot_pct","distance_bucket","hours_bucket",
                "hours_to_expiry","close_dt","close_time","fair_yes",
                "execution_edge_yes","execution_edge_no","best_side","best_exec_edge",
                "spot_symbol","entry_fee_yes","entry_fee_no"
            ):
                val = _arch_rr_field(item, key, None)
                if val is not None:
                    data[key] = val

        if "spot_symbol" not in data or not data.get("spot_symbol"):
            guessed = _arch_rr_guess_spot_symbol(item)
            if guessed:
                data["spot_symbol"] = guessed

        if "close_dt" in data and isinstance(data["close_dt"], _arch_dt):
            data["close_dt"] = data["close_dt"].isoformat()
        if "close_time" in data and isinstance(data["close_time"], _arch_dt):
            data["close_time"] = data["close_time"].isoformat()

        return _ArchSnapshotAdapterV2(data)

    def _arch_rr_normalize_market_snapshots(value):
        _arch_rr_p0_stats["last_market_snapshots_type_before"] = type(value).__name__ if value is not None else None
        out = {}

        if isinstance(value, dict):
            for key, item in value.items():
                obj = _arch_rr_as_snapshot_obj(item)
                if obj is None:
                    continue
                ticker = _arch_rr_field(obj, "ticker", None) or key
                out[str(ticker)] = obj

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                obj = _arch_rr_as_snapshot_obj(item)
                if obj is None:
                    continue
                ticker = _arch_rr_field(obj, "ticker", None) or f"snapshot_{idx}"
                out[str(ticker)] = obj

        elif value is None:
            out = {}

        else:
            obj = _arch_rr_as_snapshot_obj(value)
            if obj is not None:
                ticker = _arch_rr_field(obj, "ticker", None) or "snapshot_0"
                out[str(ticker)] = obj

        _arch_rr_p0_stats["last_market_snapshots_len_after"] = len(out)
        return out

    def _arch_rr_repair_market_snapshots(self, source):
        try:
            current = getattr(self, "market_snapshots", None)
            repaired = _arch_rr_normalize_market_snapshots(current)
            setattr(self, "market_snapshots", repaired)
            if source == "dashboard":
                _arch_rr_p0_stats["dashboard_repairs"] += 1
            else:
                _arch_rr_p0_stats["refresh_repairs"] += 1
            return True
        except Exception as e:
            _arch_rr_p0_stats["last_error"] = repr(e)
            return False

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_rr_v1 = TradingBot.dashboard_status

        def _arch_runtime_roadmap_and_p0_health_dashboard_v1(self, *args, **kwargs):
            _arch_rr_repair_market_snapshots(self, "dashboard")
            status = _arch_prev_dashboard_status_rr_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_clean():
                    resolved = await status
                    if isinstance(resolved, dict):
                        arch = resolved.setdefault("architect", {})
                        arch["runtime_roadmap"] = _ARCH_RUNTIME_ROADMAP_V1
                        arch["p0_health"] = _arch_rr_p0_stats
                    return _arch_rr_json_safe(resolved)
                return _await_and_clean()

            if isinstance(status, dict):
                arch = status.setdefault("architect", {})
                arch["runtime_roadmap"] = _ARCH_RUNTIME_ROADMAP_V1
                arch["p0_health"] = _arch_rr_p0_stats
            return _arch_rr_json_safe(status)

        TradingBot.dashboard_status = _arch_runtime_roadmap_and_p0_health_dashboard_v1

    if hasattr(TradingBot, "refresh_markets"):
        _arch_prev_refresh_markets_rr_v1 = TradingBot.refresh_markets

        def _arch_runtime_roadmap_and_p0_health_refresh_v1(self, *args, **kwargs):
            result = _arch_prev_refresh_markets_rr_v1(self, *args, **kwargs)

            if _inspect.isawaitable(result):
                async def _await_and_repair():
                    out = await result
                    _arch_rr_repair_market_snapshots(self, "refresh")
                    return out
                return _await_and_repair()

            _arch_rr_repair_market_snapshots(self, "refresh")
            return result

        TradingBot.refresh_markets = _arch_runtime_roadmap_and_p0_health_refresh_v1

    async def _arch_rr_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_rr_route_exists("/api/debug/runtime-roadmap"):
        @app.get("/api/debug/runtime-roadmap")
        async def api_debug_runtime_roadmap():
            status = await _arch_rr_safe_status()
            payload = {
                "ok": True,
                "roadmap": _ARCH_RUNTIME_ROADMAP_V1,
                "p0_health": _arch_rr_p0_stats,
                "market_snapshots_type": None,
                "market_snapshots_len": 0,
                "architect_next_step": (
                    (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                    or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                    or status.get("architect_next_step")
                    or "restore_runtime_health_then_begin_P1_inventory_skew"
                ) if isinstance(status, dict) else "restore_runtime_health_then_begin_P1_inventory_skew"
            }
            try:
                if "bot" in globals():
                    ms = getattr(bot, "market_snapshots", None)
                    payload["market_snapshots_type"] = type(ms).__name__ if ms is not None else None
                    payload["market_snapshots_len"] = len(ms) if ms is not None else 0
            except Exception as e:
                payload["inspect_error"] = repr(e)

            return JSONResponse(_arch_rr_json_safe(payload))

except Exception:
    pass



# --- architect raw_close_time snapshot contract v1 ---
try:
    import inspect as _inspect
    from datetime import datetime as _arch_dt, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_raw_close_time_snapshot_contract_v1_applied = True

    _arch_rct_stats = {
        "adapter_rewrites": 0,
        "raw_close_time_inferred": 0,
        "last_error": None,
        "sample_tickers": [],
    }

    def _arch_rct_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_rct_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_rct_parse_dt(value):
        if value is None:
            return None
        if isinstance(value, _arch_dt):
            dt = value
        else:
            try:
                dt = _arch_dt.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        return dt

    def _arch_rct_to_iso(value):
        if value is None:
            return None
        if isinstance(value, _arch_dt):
            return value.isoformat()
        return str(value)

    class _ArchSnapshotContractAdapter(dict):
        def __getattr__(self, name):
            if name in self:
                return self[name]
            raise AttributeError(name)

        def to_dict(self):
            payload = dict(self)
            # Preserve original MarketSnapshot contract:
            # serialized raw_close_time should mirror close_time string.
            payload["raw_close_time"] = payload.get("close_time")
            return payload

    def _arch_rct_guess_spot_symbol(item):
        for key in ("spot_symbol", "asset", "symbol"):
            val = _arch_rct_field(item, key, None)
            if val:
                return str(val).upper()
        ticker = str(_arch_rct_field(item, "ticker", "") or "").upper()
        if ticker.startswith("KXBTC-"):
            return "BTC"
        if ticker.startswith("KXETH-"):
            return "ETH"
        if ticker.startswith("KXSOL-"):
            return "SOL"
        return None

    def _arch_rct_adapt_one(item):
        if item is None:
            return None

        if isinstance(item, _ArchSnapshotContractAdapter):
            return item

        data = {}
        if isinstance(item, dict):
            data.update(item)
        else:
            for key in (
                "ticker","title","subtitle","category","asset","market_family","status",
                "yes_bid","yes_ask","no_bid","no_ask","last_price","mid","spread",
                "close_time","close_dt","raw_close_time","volume_24h","liquidity",
                "threshold","direction","fair_yes","edge","rationale","spot_symbol",
                "floor_strike","cap_strike","distance_from_spot_pct","hours_to_expiry",
                "best_side","best_exec_edge","execution_edge_yes","execution_edge_no",
                "yes_spread","no_spread","open_interest","volume"
            ):
                val = _arch_rct_field(item, key, None)
                if val is not None:
                    data[key] = val

        if "spot_symbol" not in data or not data.get("spot_symbol"):
            guessed = _arch_rct_guess_spot_symbol(item)
            if guessed:
                data["spot_symbol"] = guessed

        # Restore the original MarketSnapshot field contract:
        # - raw_close_time attribute should exist and be datetime-like if possible
        # - close_time should remain a serialized string
        raw_dt = None
        if data.get("raw_close_time") is not None:
            raw_dt = _arch_rct_parse_dt(data.get("raw_close_time"))
        if raw_dt is None and data.get("close_dt") is not None:
            raw_dt = _arch_rct_parse_dt(data.get("close_dt"))
        if raw_dt is None and data.get("close_time") is not None:
            raw_dt = _arch_rct_parse_dt(data.get("close_time"))

        if raw_dt is not None:
            data["raw_close_time"] = raw_dt
            data["close_time"] = _arch_rct_to_iso(raw_dt)
            if "close_dt" not in data or not data.get("close_dt"):
                data["close_dt"] = _arch_rct_to_iso(raw_dt)
            _arch_rct_stats["raw_close_time_inferred"] += 1
        else:
            # still preserve fields even if unparsable
            if "close_dt" in data and data.get("close_dt") is not None:
                data["close_time"] = _arch_rct_to_iso(data.get("close_dt"))
            elif "close_time" in data and data.get("close_time") is not None:
                data["close_time"] = _arch_rct_to_iso(data.get("close_time"))
            else:
                data.setdefault("close_time", None)
            data.setdefault("raw_close_time", None)

        return _ArchSnapshotContractAdapter(data)

    def _arch_rct_rewrite_market_snapshots(self):
        try:
            current = getattr(self, "market_snapshots", None)
            out = {}

            if isinstance(current, dict):
                items = current.items()
            elif isinstance(current, list):
                items = []
                for idx, item in enumerate(current):
                    ticker = _arch_rct_field(item, "ticker", f"snapshot_{idx}")
                    items.append((ticker, item))
            elif current is None:
                items = []
            else:
                ticker = _arch_rct_field(current, "ticker", "snapshot_0")
                items = [(ticker, current)]

            for key, item in items:
                adapted = _arch_rct_adapt_one(item)
                if adapted is None:
                    continue
                ticker = _arch_rct_field(adapted, "ticker", None) or key
                out[str(ticker)] = adapted

            setattr(self, "market_snapshots", out)
            _arch_rct_stats["adapter_rewrites"] += 1
            _arch_rct_stats["sample_tickers"] = list(out.keys())[:8]
            return True
        except Exception as e:
            _arch_rct_stats["last_error"] = repr(e)
            return False

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_rct_v1 = TradingBot.dashboard_status

        def _arch_raw_close_time_snapshot_contract_dashboard_v1(self, *args, **kwargs):
            _arch_rct_rewrite_market_snapshots(self)
            return _arch_prev_dashboard_status_rct_v1(self, *args, **kwargs)

        TradingBot.dashboard_status = _arch_raw_close_time_snapshot_contract_dashboard_v1

    if hasattr(TradingBot, "refresh_markets"):
        _arch_prev_refresh_markets_rct_v1 = TradingBot.refresh_markets

        def _arch_raw_close_time_snapshot_contract_refresh_v1(self, *args, **kwargs):
            result = _arch_prev_refresh_markets_rct_v1(self, *args, **kwargs)

            if _inspect.isawaitable(result):
                async def _await_and_rewrite():
                    out = await result
                    _arch_rct_rewrite_market_snapshots(self)
                    return out
                return _await_and_rewrite()

            _arch_rct_rewrite_market_snapshots(self)
            return result

        TradingBot.refresh_markets = _arch_raw_close_time_snapshot_contract_refresh_v1

    async def _arch_rct_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_rct_route_exists("/api/debug/raw-close-time-contract"):
        @app.get("/api/debug/raw-close-time-contract")
        async def api_debug_raw_close_time_contract():
            status = await _arch_rct_safe_status()
            payload = {
                "ok": True,
                "stats": _arch_rct_stats,
                "loop_error": ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None,
            }
            try:
                if "bot" in globals():
                    ms = getattr(bot, "market_snapshots", {}) or {}
                    payload["market_snapshots_type"] = type(ms).__name__
                    payload["market_snapshots_len"] = len(ms)
                    sample = []
                    for _, snap in list(ms.items())[:5]:
                        sample.append({
                            "ticker": _arch_rct_field(snap, "ticker", None),
                            "spot_symbol": _arch_rct_field(snap, "spot_symbol", None),
                            "close_time": _arch_rct_field(snap, "close_time", None),
                            "raw_close_time_type": type(_arch_rct_field(snap, "raw_close_time", None)).__name__ if _arch_rct_field(snap, "raw_close_time", None) is not None else None,
                        })
                    payload["sample"] = sample
            except Exception as e:
                payload["inspect_error"] = repr(e)
            return JSONResponse(payload)

except Exception:
    pass



# --- architect cluster inventory foundation v1 ---
try:
    import copy as _arch_copy
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_cluster_inventory_foundation_v1_applied = True

    _arch_ci_stats = {
        "schema_added": [],
        "schema_existing": [],
        "last_db_path": None,
        "last_error": None,
        "dashboard_attaches": 0,
        "eligible_counter_repairs": 0,
    }

    def _arch_ci_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_ci_safe_float(v, default=0.0):
        try:
            if v is None:
                return default
            return float(v)
        except Exception:
            return default

    def _arch_ci_find_db():
        for helper_name in (
            "_arch_find_strategy_lab_db",
            "_arch_mt_find_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (_Path("data/strategy_lab.db"), _Path("/app/data/strategy_lab.db")):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_ci_ensure_schema():
        db = _arch_ci_find_db()
        _arch_ci_stats["last_db_path"] = str(db) if db else None
        if not db:
            _arch_ci_stats["last_error"] = "strategy_lab.db_not_found"
            return

        spec = {
            "execution_compare_cohorts": [
                ("cluster_key", "TEXT"),
                ("cluster_inventory_bias", "REAL"),
                ("cluster_worst_case_pnl_before", "REAL"),
                ("cluster_worst_case_pnl_after", "REAL"),
                ("inventory_side_penalty", "REAL"),
                ("inventory_quote_size_mult", "REAL"),
                ("inventory_reservation_shift_cents", "REAL"),
            ],
            "market_opportunities": [
                ("cluster_key", "TEXT"),
                ("inventory_side_penalty", "REAL"),
                ("inventory_quote_size_mult", "REAL"),
                ("inventory_reservation_shift_cents", "REAL"),
            ],
        }

        con = None
        try:
            con = _sqlite3.connect(str(db))
            cur = con.cursor()
            for table, cols in spec.items():
                existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
                for col_name, col_type in cols:
                    key = f"{table}.{col_name}"
                    if col_name in existing:
                        if key not in _arch_ci_stats["schema_existing"]:
                            _arch_ci_stats["schema_existing"].append(key)
                        continue
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                    if key not in _arch_ci_stats["schema_added"]:
                        _arch_ci_stats["schema_added"].append(key)
            con.commit()
            _arch_ci_stats["last_error"] = None
        except Exception as e:
            _arch_ci_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    def _arch_ci_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_ci_cluster_key(ticker):
        ticker = str(ticker or "")
        if "-B" in ticker:
            return ticker.split("-B", 1)[0]
        return ticker

    def _arch_ci_collect_visible_markets(status):
        markets = []
        if isinstance(status, dict):
            m = status.get("markets")
            if isinstance(m, list) and m:
                markets = m
        if not markets and "bot" in globals():
            try:
                ms = getattr(bot, "market_snapshots", {}) or {}
                if isinstance(ms, dict):
                    markets = list(ms.values())
            except Exception:
                pass
        return markets or []

    def _arch_ci_collect_shadow_positions(status):
        rows = []
        if isinstance(status, dict):
            sl = status.get("strategy_lab") or {}
            sp = sl.get("shadow_positions")
            if isinstance(sp, list):
                rows = sp
        return rows or []

    def _arch_ci_position_side(row):
        for key in ("side", "best_side", "direction"):
            val = str(_arch_ci_field(row, key, "") or "").lower()
            if val in ("yes", "no"):
                return val
        return None

    def _arch_ci_position_size(row):
        for key in ("qty", "quantity", "contracts", "size", "shares"):
            val = _arch_ci_field(row, key, None)
            try:
                if val is not None:
                    return abs(float(val))
            except Exception:
                pass
        return 1.0

    def _arch_ci_build_report(status):
        _arch_ci_ensure_schema()

        visible_markets = _arch_ci_collect_visible_markets(status)
        shadow_positions = _arch_ci_collect_shadow_positions(status)

        cluster_state = {}
        for row in shadow_positions:
            ticker = _arch_ci_field(row, "ticker", None)
            side = _arch_ci_position_side(row)
            size = _arch_ci_position_size(row)
            if not ticker or not side:
                continue
            ck = _arch_ci_cluster_key(ticker)
            state = cluster_state.setdefault(ck, {
                "cluster_key": ck,
                "position_yes": 0.0,
                "position_no": 0.0,
                "position_total": 0.0,
                "net_bias": 0.0,
                "quote_size_mult": 1.0,
                "reservation_shift_cents_yes": 0.0,
                "reservation_shift_cents_no": 0.0,
                "inventory_penalty_yes": 0.0,
                "inventory_penalty_no": 0.0,
            })
            if side == "yes":
                state["position_yes"] += size
            elif side == "no":
                state["position_no"] += size
            state["position_total"] += size

        for state in cluster_state.values():
            total = max(state["position_total"], 1.0)
            bias = (state["position_yes"] - state["position_no"]) / total
            state["net_bias"] = round(bias, 4)
            mag = min(abs(bias), 1.0)
            state["quote_size_mult"] = round(max(0.25, 1.0 - 0.75 * mag), 4)
            state["inventory_penalty_yes"] = round(max(0.0, bias) * 0.03, 4)
            state["inventory_penalty_no"] = round(max(0.0, -bias) * 0.03, 4)
            state["reservation_shift_cents_yes"] = round(max(0.0, bias) * 5.0, 4)
            state["reservation_shift_cents_no"] = round(max(0.0, -bias) * 5.0, 4)

        visible_adjustments = []
        for m in visible_markets:
            ticker = _arch_ci_field(m, "ticker", None)
            if not ticker:
                continue
            ck = _arch_ci_cluster_key(ticker)
            state = cluster_state.get(ck, {
                "cluster_key": ck,
                "position_yes": 0.0,
                "position_no": 0.0,
                "position_total": 0.0,
                "net_bias": 0.0,
                "quote_size_mult": 1.0,
                "inventory_penalty_yes": 0.0,
                "inventory_penalty_no": 0.0,
                "reservation_shift_cents_yes": 0.0,
                "reservation_shift_cents_no": 0.0,
            })

            best_side = str(_arch_ci_field(m, "best_side", "") or "").lower()
            if best_side not in ("yes", "no"):
                best_side = "yes"

            inventory_penalty = state["inventory_penalty_yes"] if best_side == "yes" else state["inventory_penalty_no"]
            reservation_shift = state["reservation_shift_cents_yes"] if best_side == "yes" else state["reservation_shift_cents_no"]
            best_edge = _arch_ci_safe_float(_arch_ci_field(m, "best_exec_edge", None), 0.0)
            edge_after_inventory = round(best_edge - inventory_penalty, 4)

            visible_adjustments.append({
                "ticker": ticker,
                "cluster_key": ck,
                "best_side": best_side,
                "best_exec_edge": best_edge,
                "edge_after_inventory_penalty": edge_after_inventory,
                "inventory_side_penalty": inventory_penalty,
                "inventory_quote_size_mult": state["quote_size_mult"],
                "inventory_reservation_shift_cents": reservation_shift,
                "cluster_net_bias": state["net_bias"],
                "spread": _arch_ci_field(m, "spread", None),
                "distance_from_spot_pct": _arch_ci_field(m, "distance_from_spot_pct", None),
            })

        visible_adjustments.sort(key=lambda x: x["edge_after_inventory_penalty"], reverse=True)

        summary = {
            "ok": True,
            "policy": "replay_only__inventory_skew_foundation__no_threshold_or_sizing_changes",
            "phase": "P1_foundation_started",
            "cluster_count": len(cluster_state),
            "clusters": list(cluster_state.values())[:12],
            "visible_market_inventory_adjustments": visible_adjustments[:20],
            "shadow_position_count_sampled": len(shadow_positions),
            "visible_market_count": len(visible_markets),
            "schema": {
                "added": _arch_ci_stats["schema_added"],
                "existing": _arch_ci_stats["schema_existing"],
                "db_path": _arch_ci_stats["last_db_path"],
                "last_error": _arch_ci_stats["last_error"],
            }
        }
        return summary

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_ci_v1 = TradingBot.dashboard_status

        def _arch_cluster_inventory_foundation_dashboard_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_ci_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    if isinstance(resolved, dict):
                        health = resolved.setdefault("health", {})
                        mp = health.setdefault("market_pipeline", {})
                        try:
                            ms = getattr(self, "market_snapshots", {}) or {}
                            if isinstance(ms, dict) and len(ms) > 0 and (mp.get("eligible_markets") in (None, 0)):
                                mp["eligible_markets"] = len(ms)
                                _arch_ci_stats["eligible_counter_repairs"] += 1
                        except Exception:
                            pass

                        arch = resolved.setdefault("architect", {})
                        arch["cluster_inventory_foundation"] = _arch_ci_build_report(resolved)

                        rr = arch.get("runtime_roadmap")
                        if isinstance(rr, dict):
                            rr2 = _arch_copy.deepcopy(rr)
                            for phase in rr2.get("phases", []):
                                if phase.get("id") == "P0" and ((resolved.get("health") or {}).get("loop_error") is None):
                                    phase["status"] = "materially_green"
                                if phase.get("id") == "P1":
                                    phase["status"] = "active_foundation"
                            arch["runtime_roadmap"] = rr2

                        _arch_ci_stats["dashboard_attaches"] += 1
                    return resolved
                return _await_and_attach()

            if isinstance(status, dict):
                health = status.setdefault("health", {})
                mp = health.setdefault("market_pipeline", {})
                try:
                    ms = getattr(self, "market_snapshots", {}) or {}
                    if isinstance(ms, dict) and len(ms) > 0 and (mp.get("eligible_markets") in (None, 0)):
                        mp["eligible_markets"] = len(ms)
                        _arch_ci_stats["eligible_counter_repairs"] += 1
                except Exception:
                    pass

                arch = status.setdefault("architect", {})
                arch["cluster_inventory_foundation"] = _arch_ci_build_report(status)

                rr = arch.get("runtime_roadmap")
                if isinstance(rr, dict):
                    rr2 = _arch_copy.deepcopy(rr)
                    for phase in rr2.get("phases", []):
                        if phase.get("id") == "P0" and ((status.get("health") or {}).get("loop_error") is None):
                            phase["status"] = "materially_green"
                        if phase.get("id") == "P1":
                            phase["status"] = "active_foundation"
                    arch["runtime_roadmap"] = rr2

                _arch_ci_stats["dashboard_attaches"] += 1

            return status

        TradingBot.dashboard_status = _arch_cluster_inventory_foundation_dashboard_v1

    async def _arch_ci_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_ci_route_exists("/api/debug/cluster-inventory-report"):
        @app.get("/api/debug/cluster-inventory-report")
        async def api_debug_cluster_inventory_report():
            status = await _arch_ci_safe_status()
            payload = _arch_ci_build_report(status)
            payload["stats"] = _arch_ci_stats
            payload["loop_error"] = ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None
            payload["eligible_markets"] = (((status.get("health") or {}).get("market_pipeline") or {}).get("eligible_markets")) if isinstance(status, dict) else None
            payload["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or (status.get("architect_replay_calibration") or {}).get("architect_next_step")
                or "begin_P1_inventory_surface_then_P2_rebate_surface"
            ) if isinstance(status, dict) else "begin_P1_inventory_surface_then_P2_rebate_surface"
            return JSONResponse(payload)

except Exception:
    pass



# --- architect maker rebate foundation v1 ---
try:
    import copy as _arch_copy
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_maker_rebate_foundation_v1_applied = True

    _arch_mr_stats = {
        "schema_added": [],
        "schema_existing": [],
        "last_db_path": None,
        "last_error": None,
        "dashboard_attaches": 0,
        "opp_rows_backfilled": 0,
        "cohort_rows_backfilled": 0,
    }

    def _arch_mr_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_mr_safe_float(v, default=0.0):
        try:
            if v is None:
                return default
            return float(v)
        except Exception:
            return default

    def _arch_mr_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_mr_find_db():
        for helper_name in (
            "_arch_find_strategy_lab_db",
            "_arch_mt_find_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (_Path("data/strategy_lab.db"), _Path("/app/data/strategy_lab.db")):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_mr_cluster_key(ticker):
        ticker = str(ticker or "")
        if "-B" in ticker:
            return ticker.split("-B", 1)[0]
        return ticker

    def _arch_mr_distance_multiplier(spread):
        spread = _arch_mr_safe_float(spread, None)
        if spread is None:
            return 0.35
        if spread <= 0.02:
            return 1.00
        if spread <= 0.05:
            return 0.85
        if spread <= 0.08:
            return 0.65
        return 0.35

    def _arch_mr_eligible_size(side, row):
        side = str(side or "yes").lower()
        candidates = []
        if side == "yes":
            candidates = [
                _arch_mr_field(row, "yes_bid_size_fp", None),
                _arch_mr_field(row, "yes_ask_size_fp", None),
            ]
        else:
            candidates = [
                _arch_mr_field(row, "no_bid_size_fp", None),
                _arch_mr_field(row, "no_ask_size_fp", None),
            ]

        for c in candidates:
            try:
                v = float(c)
                if v > 0:
                    return min(v, 25.0)
            except Exception:
                pass

        oi = _arch_mr_safe_float(_arch_mr_field(row, "open_interest", None), 0.0)
        vol = _arch_mr_safe_float(_arch_mr_field(row, "volume_24h", None), 0.0)
        fallback = max(2.0, min(25.0, max(oi * 0.002, vol * 0.0005, 5.0)))
        return round(fallback, 4)

    def _arch_mr_rebate_triplet(score):
        # Replay-only conservative scenario assumptions in $/contract-price points.
        low_rate = 0.00002
        base_rate = 0.00005
        high_rate = 0.00010
        return (
            round(score * low_rate, 6),
            round(score * base_rate, 6),
            round(score * high_rate, 6),
        )

    def _arch_mr_ensure_schema():
        db = _arch_mr_find_db()
        _arch_mr_stats["last_db_path"] = str(db) if db else None
        if not db:
            _arch_mr_stats["last_error"] = "strategy_lab.db_not_found"
            return

        spec = {
            "execution_compare_cohorts": [
                ("lip_distance_from_best", "REAL"),
                ("lip_distance_multiplier", "REAL"),
                ("lip_eligible_size", "REAL"),
                ("lip_score_gross", "REAL"),
                ("lip_rebate_estimate_low", "REAL"),
                ("lip_rebate_estimate_base", "REAL"),
                ("lip_rebate_estimate_high", "REAL"),
                ("maker_edge_after_rebate", "REAL"),
                ("maker_edge_after_rebate_and_inventory", "REAL"),
            ],
            "market_opportunities": [
                ("lip_distance_from_best", "REAL"),
                ("lip_distance_multiplier", "REAL"),
                ("lip_eligible_size", "REAL"),
                ("lip_score_gross", "REAL"),
                ("lip_rebate_estimate_low", "REAL"),
                ("lip_rebate_estimate_base", "REAL"),
                ("lip_rebate_estimate_high", "REAL"),
                ("maker_edge_after_rebate", "REAL"),
            ],
        }

        con = None
        try:
            con = _sqlite3.connect(str(db))
            cur = con.cursor()
            for table, cols in spec.items():
                existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
                for col_name, col_type in cols:
                    key = f"{table}.{col_name}"
                    if col_name in existing:
                        if key not in _arch_mr_stats["schema_existing"]:
                            _arch_mr_stats["schema_existing"].append(key)
                        continue
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                    if key not in _arch_mr_stats["schema_added"]:
                        _arch_mr_stats["schema_added"].append(key)
            con.commit()
            _arch_mr_stats["last_error"] = None
        except Exception as e:
            _arch_mr_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    def _arch_mr_backfill_recent_rows():
        db = _arch_mr_find_db()
        _arch_mr_stats["last_db_path"] = str(db) if db else None
        if not db:
            return

        con = None
        try:
            con = _sqlite3.connect(str(db))
            con.row_factory = _sqlite3.Row
            cur = con.cursor()

            opp_rows = cur.execute("""
                SELECT id, ticker, best_side, best_exec_edge, spread, open_interest, volume_24h,
                       yes_bid_size_fp, yes_ask_size_fp, no_bid_size_fp, no_ask_size_fp
                FROM market_opportunities
                WHERE lip_score_gross IS NULL
                ORDER BY id DESC
                LIMIT 500
            """).fetchall()

            for row in opp_rows:
                side = str(row["best_side"] or "yes").lower()
                if side not in ("yes", "no"):
                    side = "yes"
                spread = _arch_mr_safe_float(row["spread"], 0.0)
                dist = 0.0
                mult = _arch_mr_distance_multiplier(spread)
                size = _arch_mr_eligible_size(side, row)
                score = round(size * mult, 6)
                low, base, high = _arch_mr_rebate_triplet(score)
                edge = _arch_mr_safe_float(row["best_exec_edge"], 0.0)
                edge_after = round(edge + base, 6)
                ck = _arch_mr_cluster_key(row["ticker"])

                cur.execute("""
                    UPDATE market_opportunities
                    SET cluster_key = COALESCE(cluster_key, ?),
                        lip_distance_from_best = COALESCE(lip_distance_from_best, ?),
                        lip_distance_multiplier = COALESCE(lip_distance_multiplier, ?),
                        lip_eligible_size = COALESCE(lip_eligible_size, ?),
                        lip_score_gross = COALESCE(lip_score_gross, ?),
                        lip_rebate_estimate_low = COALESCE(lip_rebate_estimate_low, ?),
                        lip_rebate_estimate_base = COALESCE(lip_rebate_estimate_base, ?),
                        lip_rebate_estimate_high = COALESCE(lip_rebate_estimate_high, ?),
                        maker_edge_after_rebate = COALESCE(maker_edge_after_rebate, ?)
                    WHERE id = ?
                """, (ck, dist, mult, size, score, low, base, high, edge_after, row["id"]))
                _arch_mr_stats["opp_rows_backfilled"] += 1

            cohort_rows = cur.execute("""
                SELECT id, ticker, maker_side, maker_best_side, maker_edge, best_exec_edge, spread,
                       inventory_side_penalty, open_interest, yes_bid_size_fp, yes_ask_size_fp, no_bid_size_fp, no_ask_size_fp
                FROM execution_compare_cohorts
                WHERE lip_score_gross IS NULL
                ORDER BY id DESC
                LIMIT 500
            """).fetchall()

            for row in cohort_rows:
                side = str(row["maker_best_side"] or row["maker_side"] or "yes").lower()
                if side not in ("yes", "no"):
                    side = "yes"
                spread = _arch_mr_safe_float(row["spread"], 0.0)
                dist = 0.0
                mult = _arch_mr_distance_multiplier(spread)
                size = _arch_mr_eligible_size(side, row)
                score = round(size * mult, 6)
                low, base, high = _arch_mr_rebate_triplet(score)
                maker_edge = _arch_mr_safe_float(row["maker_edge"], None)
                if maker_edge is None:
                    maker_edge = _arch_mr_safe_float(row["best_exec_edge"], 0.0)
                inv_penalty = _arch_mr_safe_float(row["inventory_side_penalty"], 0.0)
                edge_after = round(maker_edge + base, 6)
                edge_after_inv = round(edge_after - inv_penalty, 6)
                ck = _arch_mr_cluster_key(row["ticker"])

                cur.execute("""
                    UPDATE execution_compare_cohorts
                    SET cluster_key = COALESCE(cluster_key, ?),
                        lip_distance_from_best = COALESCE(lip_distance_from_best, ?),
                        lip_distance_multiplier = COALESCE(lip_distance_multiplier, ?),
                        lip_eligible_size = COALESCE(lip_eligible_size, ?),
                        lip_score_gross = COALESCE(lip_score_gross, ?),
                        lip_rebate_estimate_low = COALESCE(lip_rebate_estimate_low, ?),
                        lip_rebate_estimate_base = COALESCE(lip_rebate_estimate_base, ?),
                        lip_rebate_estimate_high = COALESCE(lip_rebate_estimate_high, ?),
                        maker_edge_after_rebate = COALESCE(maker_edge_after_rebate, ?),
                        maker_edge_after_rebate_and_inventory = COALESCE(maker_edge_after_rebate_and_inventory, ?)
                    WHERE id = ?
                """, (ck, dist, mult, size, score, low, base, high, edge_after, edge_after_inv, row["id"]))
                _arch_mr_stats["cohort_rows_backfilled"] += 1

            con.commit()
            _arch_mr_stats["last_error"] = None
        except Exception as e:
            _arch_mr_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    def _arch_mr_collect_visible_markets(status):
        markets = []
        if isinstance(status, dict):
            m = status.get("markets")
            if isinstance(m, list) and m:
                markets = m
        if not markets and "bot" in globals():
            try:
                ms = getattr(bot, "market_snapshots", {}) or {}
                if isinstance(ms, dict):
                    markets = list(ms.values())
            except Exception:
                pass
        return markets or []

    def _arch_mr_cluster_penalty_map(status):
        out = {}
        if isinstance(status, dict):
            arch = status.get("architect") or {}
            ci = arch.get("cluster_inventory_foundation") or {}
            for row in ci.get("visible_market_inventory_adjustments") or []:
                ticker = row.get("ticker")
                if ticker:
                    out[ticker] = row
        return out

    def _arch_mr_build_report(status):
        _arch_mr_ensure_schema()
        _arch_mr_backfill_recent_rows()

        markets = _arch_mr_collect_visible_markets(status)
        penalty_map = _arch_mr_cluster_penalty_map(status)

        rows = []
        for m in markets:
            ticker = _arch_mr_field(m, "ticker", None)
            if not ticker:
                continue

            maker_side = str(
                _arch_mr_field(m, "maker_best_side",
                _arch_mr_field(m, "maker_side",
                _arch_mr_field(m, "best_side", "yes"))) or "yes"
            ).lower()
            if maker_side not in ("yes", "no"):
                maker_side = "yes"

            maker_edge = _arch_mr_field(m, "maker_best_edge", _arch_mr_field(m, "maker_edge", None))
            if maker_edge is None:
                maker_edge = _arch_mr_field(m, "best_exec_edge", 0.0)
            maker_edge = _arch_mr_safe_float(maker_edge, 0.0)

            spread = _arch_mr_safe_float(_arch_mr_field(m, "spread", None), 0.0)
            dist = 0.0
            mult = _arch_mr_distance_multiplier(spread)
            size = _arch_mr_eligible_size(maker_side, m)
            score = round(size * mult, 6)
            low, base, high = _arch_mr_rebate_triplet(score)

            inv = penalty_map.get(ticker, {})
            inv_penalty = _arch_mr_safe_float(inv.get("inventory_side_penalty"), 0.0)
            qmult = _arch_mr_safe_float(inv.get("inventory_quote_size_mult"), 1.0)

            rows.append({
                "ticker": ticker,
                "cluster_key": _arch_mr_cluster_key(ticker),
                "maker_side": maker_side,
                "maker_edge_raw": round(maker_edge, 6),
                "spread": spread,
                "lip_distance_from_best": dist,
                "lip_distance_multiplier": mult,
                "lip_eligible_size": size,
                "lip_score_gross": score,
                "lip_rebate_estimate_low": low,
                "lip_rebate_estimate_base": base,
                "lip_rebate_estimate_high": high,
                "maker_edge_after_rebate": round(maker_edge + base, 6),
                "inventory_side_penalty": inv_penalty,
                "inventory_quote_size_mult": qmult,
                "maker_edge_after_rebate_and_inventory": round(maker_edge + base - inv_penalty, 6),
            })

        rows.sort(key=lambda x: x["maker_edge_after_rebate_and_inventory"], reverse=True)

        return {
            "ok": True,
            "policy": "replay_only__maker_rebate_foundation__no_threshold_or_sizing_changes",
            "phase": "P2_foundation_started",
            "assumptions": {
                "quote_distance_from_best_assumed": 0.0,
                "distance_multiplier_rule": {
                    "spread_le_0.02": 1.0,
                    "spread_le_0.05": 0.85,
                    "spread_le_0.08": 0.65,
                    "spread_gt_0.08": 0.35
                },
                "rebate_rate_per_score_unit": {
                    "low": 0.00002,
                    "base": 0.00005,
                    "high": 0.00010
                },
                "note": "foundation_only__relative_rebate_scenarios_until_true_LIP_params_are_ingested"
            },
            "visible_market_maker_rebate_adjustments": rows[:20],
            "visible_market_count": len(markets),
            "schema": {
                "added": _arch_mr_stats["schema_added"],
                "existing": _arch_mr_stats["schema_existing"],
                "db_path": _arch_mr_stats["last_db_path"],
                "last_error": _arch_mr_stats["last_error"],
                "opp_rows_backfilled": _arch_mr_stats["opp_rows_backfilled"],
                "cohort_rows_backfilled": _arch_mr_stats["cohort_rows_backfilled"],
            }
        }

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_mr_v1 = TradingBot.dashboard_status

        def _arch_maker_rebate_foundation_dashboard_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_mr_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    if isinstance(resolved, dict):
                        arch = resolved.setdefault("architect", {})
                        arch["maker_rebate_foundation"] = _arch_mr_build_report(resolved)

                        rr = arch.get("runtime_roadmap")
                        if isinstance(rr, dict):
                            rr2 = _arch_copy.deepcopy(rr)
                            for phase in rr2.get("phases", []):
                                if phase.get("id") == "P2":
                                    phase["status"] = "active_foundation"
                            arch["runtime_roadmap"] = rr2

                        _arch_mr_stats["dashboard_attaches"] += 1
                    return resolved
                return _await_and_attach()

            if isinstance(status, dict):
                arch = status.setdefault("architect", {})
                arch["maker_rebate_foundation"] = _arch_mr_build_report(status)

                rr = arch.get("runtime_roadmap")
                if isinstance(rr, dict):
                    rr2 = _arch_copy.deepcopy(rr)
                    for phase in rr2.get("phases", []):
                        if phase.get("id") == "P2":
                            phase["status"] = "active_foundation"
                    arch["runtime_roadmap"] = rr2

                _arch_mr_stats["dashboard_attaches"] += 1

            return status

        TradingBot.dashboard_status = _arch_maker_rebate_foundation_dashboard_v1

    async def _arch_mr_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_mr_route_exists("/api/debug/maker-rebate-report"):
        @app.get("/api/debug/maker-rebate-report")
        async def api_debug_maker_rebate_report():
            status = await _arch_mr_safe_status()
            payload = _arch_mr_build_report(status)
            payload["stats"] = _arch_mr_stats
            payload["loop_error"] = ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None
            payload["eligible_markets"] = (((status.get("health") or {}).get("market_pipeline") or {}).get("eligible_markets")) if isinstance(status, dict) else None
            payload["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or "begin_P3_queue_imbalance_and_toxic_fill_foundation"
            ) if isinstance(status, dict) else "begin_P3_queue_imbalance_and_toxic_fill_foundation"
            return JSONResponse(payload)

except Exception:
    pass



# --- architect maker rebate backfill fix v2 ---
try:
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_maker_rebate_backfill_fix_v2_applied = True

    _arch_mr_v2_stats = {
        "last_db_path": None,
        "last_error": None,
        "opp_rows_backfilled": 0,
        "cohort_rows_backfilled": 0,
        "opp_columns_used": [],
        "cohort_columns_used": [],
    }

    def _arch_mr_v2_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_mr_v2_find_db():
        for helper_name in ("_arch_mr_find_db", "_arch_find_strategy_lab_db", "_arch_mt_find_db"):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (_Path("data/strategy_lab.db"), _Path("/app/data/strategy_lab.db")):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_mr_v2_table_cols(cur, table):
        return [r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()]

    def _arch_mr_v2_select_existing(cur, table, desired):
        existing = set(_arch_mr_v2_table_cols(cur, table))
        cols = [c for c in desired if c in existing]
        return cols, existing

    def _arch_mr_v2_fetch_rows(cur, table, desired_cols, where_sql, limit_sql):
        cols, existing = _arch_mr_v2_select_existing(cur, table, desired_cols)
        if "id" not in cols and "id" in existing:
            cols = ["id"] + cols
        if not cols:
            return [], cols
        sql = f"SELECT {', '.join(cols)} FROM {table} {where_sql} {limit_sql}"
        rows = cur.execute(sql).fetchall()
        out = []
        for row in rows:
            if isinstance(row, _sqlite3.Row):
                out.append(dict(row))
            else:
                out.append({cols[i]: row[i] for i in range(len(cols))})
        return out, cols

    def _arch_mr_v2_backfill():
        db = _arch_mr_v2_find_db()
        _arch_mr_v2_stats["last_db_path"] = str(db) if db else None
        _arch_mr_v2_stats["last_error"] = None
        _arch_mr_v2_stats["opp_rows_backfilled"] = 0
        _arch_mr_v2_stats["cohort_rows_backfilled"] = 0
        _arch_mr_v2_stats["opp_columns_used"] = []
        _arch_mr_v2_stats["cohort_columns_used"] = []

        if not db:
            _arch_mr_v2_stats["last_error"] = "strategy_lab.db_not_found"
            return

        con = None
        try:
            con = _sqlite3.connect(str(db))
            con.row_factory = _sqlite3.Row
            cur = con.cursor()

            opp_desired = [
                "id", "ticker", "best_side", "best_exec_edge", "spread",
                "open_interest", "volume_24h",
                "yes_bid_size_fp", "yes_ask_size_fp", "no_bid_size_fp", "no_ask_size_fp",
                "lip_score_gross"
            ]
            opp_rows, opp_cols = _arch_mr_v2_fetch_rows(
                cur,
                "market_opportunities",
                opp_desired,
                "WHERE lip_score_gross IS NULL",
                "ORDER BY id DESC LIMIT 500"
            )
            _arch_mr_v2_stats["opp_columns_used"] = opp_cols

            for row in opp_rows:
                side = str(row.get("best_side") or "yes").lower()
                if side not in ("yes", "no"):
                    side = "yes"
                spread = _arch_mr_safe_float(row.get("spread"), 0.0)
                dist = 0.0
                mult = _arch_mr_distance_multiplier(spread)
                size = _arch_mr_eligible_size(side, row)
                score = round(size * mult, 6)
                low, base, high = _arch_mr_rebate_triplet(score)
                edge = _arch_mr_safe_float(row.get("best_exec_edge"), 0.0)
                edge_after = round(edge + base, 6)
                ck = _arch_mr_cluster_key(row.get("ticker"))

                cur.execute("""
                    UPDATE market_opportunities
                    SET cluster_key = COALESCE(cluster_key, ?),
                        lip_distance_from_best = COALESCE(lip_distance_from_best, ?),
                        lip_distance_multiplier = COALESCE(lip_distance_multiplier, ?),
                        lip_eligible_size = COALESCE(lip_eligible_size, ?),
                        lip_score_gross = COALESCE(lip_score_gross, ?),
                        lip_rebate_estimate_low = COALESCE(lip_rebate_estimate_low, ?),
                        lip_rebate_estimate_base = COALESCE(lip_rebate_estimate_base, ?),
                        lip_rebate_estimate_high = COALESCE(lip_rebate_estimate_high, ?),
                        maker_edge_after_rebate = COALESCE(maker_edge_after_rebate, ?)
                    WHERE id = ?
                """, (ck, dist, mult, size, score, low, base, high, edge_after, row["id"]))
                _arch_mr_v2_stats["opp_rows_backfilled"] += 1

            cohort_desired = [
                "id", "ticker", "maker_side", "maker_best_side", "maker_edge",
                "best_exec_edge", "spread", "inventory_side_penalty",
                "open_interest",
                "yes_bid_size_fp", "yes_ask_size_fp", "no_bid_size_fp", "no_ask_size_fp",
                "lip_score_gross"
            ]
            cohort_rows, cohort_cols = _arch_mr_v2_fetch_rows(
                cur,
                "execution_compare_cohorts",
                cohort_desired,
                "WHERE lip_score_gross IS NULL",
                "ORDER BY id DESC LIMIT 500"
            )
            _arch_mr_v2_stats["cohort_columns_used"] = cohort_cols

            for row in cohort_rows:
                side = str(row.get("maker_best_side") or row.get("maker_side") or "yes").lower()
                if side not in ("yes", "no"):
                    side = "yes"
                spread = _arch_mr_safe_float(row.get("spread"), 0.0)
                dist = 0.0
                mult = _arch_mr_distance_multiplier(spread)
                size = _arch_mr_eligible_size(side, row)
                score = round(size * mult, 6)
                low, base, high = _arch_mr_rebate_triplet(score)
                maker_edge = row.get("maker_edge")
                if maker_edge is None:
                    maker_edge = row.get("best_exec_edge")
                maker_edge = _arch_mr_safe_float(maker_edge, 0.0)
                inv_penalty = _arch_mr_safe_float(row.get("inventory_side_penalty"), 0.0)
                edge_after = round(maker_edge + base, 6)
                edge_after_inv = round(edge_after - inv_penalty, 6)
                ck = _arch_mr_cluster_key(row.get("ticker"))

                cur.execute("""
                    UPDATE execution_compare_cohorts
                    SET cluster_key = COALESCE(cluster_key, ?),
                        lip_distance_from_best = COALESCE(lip_distance_from_best, ?),
                        lip_distance_multiplier = COALESCE(lip_distance_multiplier, ?),
                        lip_eligible_size = COALESCE(lip_eligible_size, ?),
                        lip_score_gross = COALESCE(lip_score_gross, ?),
                        lip_rebate_estimate_low = COALESCE(lip_rebate_estimate_low, ?),
                        lip_rebate_estimate_base = COALESCE(lip_rebate_estimate_base, ?),
                        lip_rebate_estimate_high = COALESCE(lip_rebate_estimate_high, ?),
                        maker_edge_after_rebate = COALESCE(maker_edge_after_rebate, ?),
                        maker_edge_after_rebate_and_inventory = COALESCE(maker_edge_after_rebate_and_inventory, ?)
                    WHERE id = ?
                """, (ck, dist, mult, size, score, low, base, high, edge_after, edge_after_inv, row["id"]))
                _arch_mr_v2_stats["cohort_rows_backfilled"] += 1

            con.commit()
        except Exception as e:
            _arch_mr_v2_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    # Override the original brittle backfill with schema-tolerant version.
    if "_arch_mr_backfill_recent_rows" in globals():
        _arch_mr_backfill_recent_rows = _arch_mr_v2_backfill

    async def _arch_mr_v2_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_mr_v2_route_exists("/api/debug/maker-rebate-backfill-fix"):
        @app.get("/api/debug/maker-rebate-backfill-fix")
        async def api_debug_maker_rebate_backfill_fix():
            _arch_mr_v2_backfill()
            status = await _arch_mr_v2_safe_status()
            payload = {
                "ok": True,
                "fix_stats": _arch_mr_v2_stats,
                "loop_error": ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None,
                "eligible_markets": (((status.get("health") or {}).get("market_pipeline") or {}).get("eligible_markets")) if isinstance(status, dict) else None,
            }
            return JSONResponse(payload)

except Exception:
    pass



# --- architect paper_using_newest_infra v1 ---
try:
    import inspect as _inspect
    from datetime import datetime as _arch_dt, timezone as _arch_tz
    from fastapi.responses import JSONResponse

    _architect_paper_using_newest_infra_v1_applied = True

    _arch_puni_stats = {
        "eligibility_time_repairs": 0,
        "eligible_counter_repairs": 0,
        "dashboard_attaches": 0,
        "last_error": None,
    }

    def _arch_puni_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_puni_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_puni_parse_dt(value):
        if value is None:
            return None
        if isinstance(value, _arch_dt):
            dt = value
        else:
            try:
                dt = _arch_dt.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_arch_tz.utc)
        return dt

    def _arch_puni_iso(value):
        if value is None:
            return None
        if isinstance(value, _arch_dt):
            return value.isoformat()
        return str(value)

    def _arch_puni_normalize_market_time_contract(market):
        try:
            raw_dt = _arch_puni_parse_dt(_arch_puni_field(market, "raw_close_time", None))
            if raw_dt is None:
                raw_dt = _arch_puni_parse_dt(_arch_puni_field(market, "close_dt", None))
            if raw_dt is None:
                raw_dt = _arch_puni_parse_dt(_arch_puni_field(market, "close_time", None))

            if raw_dt is None:
                return market

            if isinstance(market, dict):
                out = dict(market)
                out["raw_close_time"] = raw_dt
                out["close_time"] = _arch_puni_iso(raw_dt)
                out.setdefault("close_dt", _arch_puni_iso(raw_dt))
                _arch_puni_stats["eligibility_time_repairs"] += 1
                return out

            for attr, value in (
                ("raw_close_time", raw_dt),
                ("close_time", _arch_puni_iso(raw_dt)),
                ("close_dt", _arch_puni_iso(raw_dt)),
            ):
                try:
                    setattr(market, attr, value)
                except Exception:
                    pass
            _arch_puni_stats["eligibility_time_repairs"] += 1
            return market
        except Exception as e:
            _arch_puni_stats["last_error"] = repr(e)
            return market

    if hasattr(TradingBot, "_evaluate_market_eligibility"):
        _arch_prev_eval_market_eligibility_puni_v1 = TradingBot._evaluate_market_eligibility

        def _arch_paper_using_newest_infra_eval_v1(self, market, *args, **kwargs):
            repaired = _arch_puni_normalize_market_time_contract(market)
            return _arch_prev_eval_market_eligibility_puni_v1(self, repaired, *args, **kwargs)

        TradingBot._evaluate_market_eligibility = _arch_paper_using_newest_infra_eval_v1

    def _arch_puni_snapshot_contract_ok(self_ref):
        try:
            ms = getattr(self_ref, "market_snapshots", {}) or {}
            if not isinstance(ms, dict) or len(ms) == 0:
                return False
            for _, snap in list(ms.items())[:10]:
                spot_symbol = _arch_puni_field(snap, "spot_symbol", None)
                raw_close_time = _arch_puni_field(snap, "raw_close_time", None)
                if not spot_symbol:
                    return False
                if raw_close_time is not None and not isinstance(raw_close_time, _arch_dt):
                    return False
            return True
        except Exception:
            return False

    def _arch_puni_attach_status(self_ref, status):
        if not isinstance(status, dict):
            return status

        try:
            health = status.setdefault("health", {})
            mp = health.setdefault("market_pipeline", {})
            mpe = health.get("market_pipeline_effective") or {}

            ms = getattr(self_ref, "market_snapshots", {}) or {}
            ms_len = len(ms) if isinstance(ms, dict) else 0

            effective_eligible = mpe.get("eligible_markets")
            pipeline_eligible = mp.get("eligible_markets")

            repaired_eligible = pipeline_eligible
            if (pipeline_eligible in (None, 0)) and isinstance(effective_eligible, int) and effective_eligible > 0:
                repaired_eligible = effective_eligible
                mp["eligible_markets"] = effective_eligible
                _arch_puni_stats["eligible_counter_repairs"] += 1
            elif (pipeline_eligible in (None, 0)) and ms_len > 0:
                repaired_eligible = ms_len
                mp["eligible_markets"] = ms_len
                _arch_puni_stats["eligible_counter_repairs"] += 1

            arch = status.setdefault("architect", {})
            checks = {
                "mode_is_paper": status.get("mode") == "paper",
                "live_disarmed": status.get("live_armed") is False,
                "loop_error_null": health.get("loop_error") is None,
                "market_snapshots_dict": isinstance(ms, dict),
                "market_snapshots_nonempty": ms_len > 0,
                "snapshot_contract_ok": _arch_puni_snapshot_contract_ok(self_ref),
                "eligible_markets_positive": isinstance(repaired_eligible, int) and repaired_eligible > 0,
                "p1_foundation_present": "cluster_inventory_foundation" in arch,
                "p2_foundation_present": "maker_rebate_foundation" in arch,
                "roadmap_present": "runtime_roadmap" in arch,
            }

            flag = all(checks.values())
            status["paper_using_newest_infra"] = flag
            status["paper_using_newest_infra_checks"] = checks
            status["paper_using_newest_infra_reason"] = (
                "all_checks_green" if flag else
                ",".join([k for k, v in checks.items() if not v])[:500]
            )

            _arch_puni_stats["dashboard_attaches"] += 1
        except Exception as e:
            _arch_puni_stats["last_error"] = repr(e)

        return status

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_puni_v1 = TradingBot.dashboard_status

        def _arch_paper_using_newest_infra_dashboard_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_puni_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    return _arch_puni_attach_status(self, resolved)
                return _await_and_attach()

            return _arch_puni_attach_status(self, status)

        TradingBot.dashboard_status = _arch_paper_using_newest_infra_dashboard_v1

    async def _arch_puni_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_puni_route_exists("/api/debug/paper-using-newest-infra"):
        @app.get("/api/debug/paper-using-newest-infra")
        async def api_debug_paper_using_newest_infra():
            status = await _arch_puni_safe_status()
            payload = {
                "ok": True,
                "paper_using_newest_infra": status.get("paper_using_newest_infra") if isinstance(status, dict) else None,
                "paper_using_newest_infra_checks": status.get("paper_using_newest_infra_checks") if isinstance(status, dict) else None,
                "paper_using_newest_infra_reason": status.get("paper_using_newest_infra_reason") if isinstance(status, dict) else None,
                "stats": _arch_puni_stats,
                "loop_error": ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None,
                "eligible_markets": (((status.get("health") or {}).get("market_pipeline") or {}).get("eligible_markets")) if isinstance(status, dict) else None,
            }
            return JSONResponse(payload)

except Exception:
    pass



# --- architect p3 queue/imbalance/toxic-fill foundation v1 ---
try:
    import copy as _arch_copy
    import inspect as _inspect
    import sqlite3 as _sqlite3
    from pathlib import Path as _Path
    from fastapi.responses import JSONResponse

    _architect_p3_queue_imbalance_toxic_fill_foundation_v1_applied = True

    _arch_p3_stats = {
        "schema_added": [],
        "schema_existing": [],
        "last_db_path": None,
        "last_error": None,
        "dashboard_attaches": 0,
        "opp_rows_backfilled": 0,
        "cohort_rows_backfilled": 0,
    }

    def _arch_p3_route_exists(path):
        try:
            return any(getattr(r, "path", None) == path for r in app.router.routes)
        except Exception:
            return False

    def _arch_p3_safe_float(v, default=0.0):
        try:
            if v is None:
                return default
            return float(v)
        except Exception:
            return default

    def _arch_p3_field(obj, name, default=None):
        try:
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)
        except Exception:
            return default

    def _arch_p3_find_db():
        for helper_name in (
            "_arch_find_strategy_lab_db",
            "_arch_mt_find_db",
            "_arch_mr_find_db",
        ):
            helper = globals().get(helper_name)
            if callable(helper):
                try:
                    found = helper()
                    if found:
                        return found
                except Exception:
                    pass
        for candidate in (_Path("data/strategy_lab.db"), _Path("/app/data/strategy_lab.db")):
            try:
                if candidate.exists():
                    return candidate
            except Exception:
                pass
        return None

    def _arch_p3_ensure_schema():
        db = _arch_p3_find_db()
        _arch_p3_stats["last_db_path"] = str(db) if db else None
        if not db:
            _arch_p3_stats["last_error"] = "strategy_lab.db_not_found"
            return

        spec = {
            "execution_compare_cohorts": [
                ("order_book_imbalance", "REAL"),
                ("maker_toxicity_bps_1s", "REAL"),
                ("maker_shield_suggested", "INTEGER"),
                ("maker_shield_reason", "TEXT"),
            ],
            "market_opportunities": [
                ("order_book_imbalance", "REAL"),
                ("maker_toxicity_bps_1s", "REAL"),
                ("maker_shield_suggested", "INTEGER"),
                ("maker_shield_reason", "TEXT"),
            ],
        }

        con = None
        try:
            con = _sqlite3.connect(str(db))
            cur = con.cursor()
            for table, cols in spec.items():
                existing = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
                for col_name, col_type in cols:
                    key = f"{table}.{col_name}"
                    if col_name in existing:
                        if key not in _arch_p3_stats["schema_existing"]:
                            _arch_p3_stats["schema_existing"].append(key)
                        continue
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                    if key not in _arch_p3_stats["schema_added"]:
                        _arch_p3_stats["schema_added"].append(key)
            con.commit()
            _arch_p3_stats["last_error"] = None
        except Exception as e:
            _arch_p3_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    def _arch_p3_collect_markets(status):
        markets = []
        if isinstance(status, dict):
            m = status.get("markets")
            if isinstance(m, list) and m:
                markets = m
        if not markets and "bot" in globals():
            try:
                ms = getattr(bot, "market_snapshots", {}) or {}
                if isinstance(ms, dict):
                    markets = list(ms.values())
            except Exception:
                pass
        return markets or []

    def _arch_p3_maker_map(status):
        out = {}
        if isinstance(status, dict):
            arch = status.get("architect") or {}
            mr = arch.get("maker_rebate_foundation") or {}
            for row in mr.get("visible_market_maker_rebate_adjustments") or []:
                ticker = row.get("ticker")
                if ticker:
                    out[ticker] = row
        return out

    def _arch_p3_size_for_side(row, side):
        side = str(side or "yes").lower()
        size_keys = ["yes_bid_size_fp", "yes_ask_size_fp"] if side == "yes" else ["no_bid_size_fp", "no_ask_size_fp"]
        for key in size_keys:
            v = _arch_p3_safe_float(_arch_p3_field(row, key, None), None)
            if v is not None and v > 0:
                return v
        oi = _arch_p3_safe_float(_arch_p3_field(row, "open_interest", None), 0.0)
        if oi > 0:
            return max(5.0, min(25.0, oi * 0.002))
        return 5.0

    def _arch_p3_imbalance(row):
        yes_sz = _arch_p3_size_for_side(row, "yes")
        no_sz = _arch_p3_size_for_side(row, "no")
        denom = yes_sz + no_sz
        if denom <= 0:
            return 0.5
        return round(yes_sz / denom, 6)

    def _arch_p3_current_move_1s_bps(status):
        try:
            arch = (status.get("architect") or {}) if isinstance(status, dict) else {}
            mt = arch.get("microstructure_truth_foundation") or {}
            spot = mt.get("spot_move_bps") or {}
            move5 = _arch_p3_safe_float(spot.get("move_5s_bps_abs"), 0.0)
            if move5 > 0:
                return round(move5 / 5.0, 4)
        except Exception:
            pass
        return 0.0

    def _arch_p3_shield(market_row, maker_row, move_1s_bps):
        spread = _arch_p3_safe_float(_arch_p3_field(market_row, "spread", None), 0.0)
        obi = _arch_p3_imbalance(market_row)
        maker_side = str(
            (maker_row or {}).get("maker_side")
            or _arch_p3_field(market_row, "maker_best_side", None)
            or _arch_p3_field(market_row, "maker_side", None)
            or _arch_p3_field(market_row, "best_side", "yes")
        ).lower()
        if maker_side not in ("yes", "no"):
            maker_side = "yes"

        reasons = []
        pred_toxicity_1s = round(max(move_1s_bps, 0.0), 4)

        if spread >= 0.08:
            reasons.append("wide_spread")
            pred_toxicity_1s = max(pred_toxicity_1s, 8.0)
        elif spread >= 0.05:
            pred_toxicity_1s = max(pred_toxicity_1s, 4.0)

        # If maker is trying to lean against strong pressure, suggest shield.
        if maker_side == "yes" and obi <= 0.20:
            reasons.append("heavy_sell_pressure_vs_yes")
            pred_toxicity_1s = max(pred_toxicity_1s, 10.0)
        if maker_side == "no" and obi >= 0.80:
            reasons.append("heavy_buy_pressure_vs_no")
            pred_toxicity_1s = max(pred_toxicity_1s, 10.0)

        if move_1s_bps >= 8.0:
            reasons.append("fast_spot_move")
            pred_toxicity_1s = max(pred_toxicity_1s, move_1s_bps)

        maker_edge_after = _arch_p3_safe_float((maker_row or {}).get("maker_edge_after_rebate_and_inventory"), None)
        if maker_edge_after is not None and maker_edge_after <= 0:
            reasons.append("non_positive_net_maker_edge")

        shield = 1 if len(reasons) > 0 else 0
        reason = ",".join(reasons) if reasons else "none"

        queue_depth_proxy = round(max(_arch_p3_size_for_side(market_row, maker_side), 1.0), 4)

        return {
            "maker_side": maker_side,
            "order_book_imbalance": obi,
            "queue_depth_proxy": queue_depth_proxy,
            "maker_toxicity_bps_1s": round(pred_toxicity_1s, 4),
            "maker_shield_suggested": shield,
            "maker_shield_reason": reason,
        }

    def _arch_p3_backfill(status):
        db = _arch_p3_find_db()
        _arch_p3_stats["last_db_path"] = str(db) if db else None
        if not db:
            return

        market_map = {}
        maker_map = _arch_p3_maker_map(status)
        move_1s_bps = _arch_p3_current_move_1s_bps(status)

        for m in _arch_p3_collect_markets(status):
            ticker = _arch_p3_field(m, "ticker", None)
            if ticker:
                market_map[ticker] = _arch_p3_shield(m, maker_map.get(ticker), move_1s_bps)

        if not market_map:
            return

        con = None
        try:
            con = _sqlite3.connect(str(db))
            cur = con.cursor()

            for ticker, row in market_map.items():
                ids = [r[0] for r in cur.execute(
                    "SELECT id FROM market_opportunities WHERE ticker = ? ORDER BY id DESC LIMIT 50",
                    (ticker,)
                ).fetchall()]
                for row_id in ids:
                    cur.execute("""
                        UPDATE market_opportunities
                        SET order_book_imbalance = COALESCE(order_book_imbalance, ?),
                            maker_toxicity_bps_1s = COALESCE(maker_toxicity_bps_1s, ?),
                            maker_shield_suggested = COALESCE(maker_shield_suggested, ?),
                            maker_shield_reason = COALESCE(maker_shield_reason, ?)
                        WHERE id = ?
                    """, (
                        row["order_book_imbalance"],
                        row["maker_toxicity_bps_1s"],
                        row["maker_shield_suggested"],
                        row["maker_shield_reason"],
                        row_id
                    ))
                    _arch_p3_stats["opp_rows_backfilled"] += 1

                ids = [r[0] for r in cur.execute(
                    "SELECT id FROM execution_compare_cohorts WHERE ticker = ? ORDER BY id DESC LIMIT 50",
                    (ticker,)
                ).fetchall()]
                for row_id in ids:
                    cur.execute("""
                        UPDATE execution_compare_cohorts
                        SET queue_depth_proxy = COALESCE(queue_depth_proxy, ?),
                            order_book_imbalance = COALESCE(order_book_imbalance, ?),
                            maker_toxicity_bps_1s = COALESCE(maker_toxicity_bps_1s, ?),
                            maker_shield_suggested = COALESCE(maker_shield_suggested, ?),
                            maker_shield_reason = COALESCE(maker_shield_reason, ?)
                        WHERE id = ?
                    """, (
                        row["queue_depth_proxy"],
                        row["order_book_imbalance"],
                        row["maker_toxicity_bps_1s"],
                        row["maker_shield_suggested"],
                        row["maker_shield_reason"],
                        row_id
                    ))
                    _arch_p3_stats["cohort_rows_backfilled"] += 1

            con.commit()
            _arch_p3_stats["last_error"] = None
        except Exception as e:
            _arch_p3_stats["last_error"] = repr(e)
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception:
                pass

    def _arch_p3_build_report(status):
        _arch_p3_ensure_schema()
        _arch_p3_backfill(status)

        markets = _arch_p3_collect_markets(status)
        maker_map = _arch_p3_maker_map(status)
        move_1s_bps = _arch_p3_current_move_1s_bps(status)

        rows = []
        for m in markets:
            ticker = _arch_p3_field(m, "ticker", None)
            if not ticker:
                continue
            shield = _arch_p3_shield(m, maker_map.get(ticker), move_1s_bps)
            row = {
                "ticker": ticker,
                "spread": _arch_p3_field(m, "spread", None),
                "best_side": _arch_p3_field(m, "best_side", None),
                "maker_side": shield["maker_side"],
                "order_book_imbalance": shield["order_book_imbalance"],
                "queue_depth_proxy": shield["queue_depth_proxy"],
                "maker_toxicity_bps_1s": shield["maker_toxicity_bps_1s"],
                "maker_toxicity_bps_5s": _arch_p3_field(m, "maker_toxicity_bps_5s", None),
                "maker_toxicity_bps_10s": _arch_p3_field(m, "maker_toxicity_bps_10s", None),
                "maker_shield_suggested": shield["maker_shield_suggested"],
                "maker_shield_reason": shield["maker_shield_reason"],
                "maker_edge_after_rebate_and_inventory": (maker_map.get(ticker) or {}).get("maker_edge_after_rebate_and_inventory"),
            }
            rows.append(row)

        rows.sort(key=lambda x: (x["maker_shield_suggested"], -(x["maker_toxicity_bps_1s"] or 0)), reverse=True)

        return {
            "ok": True,
            "policy": "replay_only__queue_imbalance_toxic_fill_foundation__no_threshold_or_sizing_changes",
            "phase": "P3_foundation_started",
            "assumptions": {
                "imbalance_definition": "yes_depth / (yes_depth + no_depth)",
                "queue_depth_proxy_rule": "side_size_fp_when_available_else_open_interest_fallback",
                "toxic_fill_1s_rule": "spread/imbalance/spot_move heuristic until true markout history is wired",
                "maker_shield_logic": [
                    "wide_spread",
                    "heavy_buy_pressure_vs_no",
                    "heavy_sell_pressure_vs_yes",
                    "fast_spot_move",
                    "non_positive_net_maker_edge"
                ]
            },
            "current_move_1s_bps_proxy": move_1s_bps,
            "visible_market_queue_toxicity_adjustments": rows[:20],
            "visible_market_count": len(markets),
            "schema": {
                "added": _arch_p3_stats["schema_added"],
                "existing": _arch_p3_stats["schema_existing"],
                "db_path": _arch_p3_stats["last_db_path"],
                "last_error": _arch_p3_stats["last_error"],
                "opp_rows_backfilled": _arch_p3_stats["opp_rows_backfilled"],
                "cohort_rows_backfilled": _arch_p3_stats["cohort_rows_backfilled"],
            }
        }

    if hasattr(TradingBot, "dashboard_status"):
        _arch_prev_dashboard_status_p3_v1 = TradingBot.dashboard_status

        def _arch_p3_queue_imbalance_toxic_fill_dashboard_v1(self, *args, **kwargs):
            status = _arch_prev_dashboard_status_p3_v1(self, *args, **kwargs)

            if _inspect.isawaitable(status):
                async def _await_and_attach():
                    resolved = await status
                    if isinstance(resolved, dict):
                        arch = resolved.setdefault("architect", {})
                        arch["queue_imbalance_toxic_fill_foundation"] = _arch_p3_build_report(resolved)

                        rr = arch.get("runtime_roadmap")
                        if isinstance(rr, dict):
                            rr2 = _arch_copy.deepcopy(rr)
                            for phase in rr2.get("phases", []):
                                if phase.get("id") == "P3":
                                    phase["status"] = "active_foundation"
                            arch["runtime_roadmap"] = rr2

                        _arch_p3_stats["dashboard_attaches"] += 1
                    return resolved
                return _await_and_attach()

            if isinstance(status, dict):
                arch = status.setdefault("architect", {})
                arch["queue_imbalance_toxic_fill_foundation"] = _arch_p3_build_report(status)

                rr = arch.get("runtime_roadmap")
                if isinstance(rr, dict):
                    rr2 = _arch_copy.deepcopy(rr)
                    for phase in rr2.get("phases", []):
                        if phase.get("id") == "P3":
                            phase["status"] = "active_foundation"
                    arch["runtime_roadmap"] = rr2

                _arch_p3_stats["dashboard_attaches"] += 1

            return status

        TradingBot.dashboard_status = _arch_p3_queue_imbalance_toxic_fill_dashboard_v1

    async def _arch_p3_safe_status():
        try:
            status = bot.dashboard_status() if "bot" in globals() else {}
            if _inspect.isawaitable(status):
                status = await status
            return status or {}
        except Exception as e:
            return {"ok": False, "status_error": repr(e)}

    if not _arch_p3_route_exists("/api/debug/queue-imbalance-toxic-fill-report"):
        @app.get("/api/debug/queue-imbalance-toxic-fill-report")
        async def api_debug_queue_imbalance_toxic_fill_report():
            status = await _arch_p3_safe_status()
            payload = _arch_p3_build_report(status)
            payload["stats"] = _arch_p3_stats
            payload["loop_error"] = ((status.get("health") or {}).get("loop_error")) if isinstance(status, dict) else None
            payload["eligible_markets"] = (((status.get("health") or {}).get("market_pipeline") or {}).get("eligible_markets")) if isinstance(status, dict) else None
            payload["paper_using_newest_infra"] = status.get("paper_using_newest_infra") if isinstance(status, dict) else None
            payload["architect_next_step"] = (
                (status.get("architect_replay_scoring") or {}).get("architect_next_step")
                or "begin_true_markout_capture_and_queue_history_after_foundation"
            ) if isinstance(status, dict) else "begin_true_markout_capture_and_queue_history_after_foundation"
            return JSONResponse(payload)

except Exception:
    pass

