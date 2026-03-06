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
        self.full_scan_seconds = max(30, int(os.getenv("FULL_MARKET_SCAN_SECONDS", "300")))
        self.max_markets = max(1000, int(os.getenv("MAX_MARKETS", "1000")))
        self.watch_terms = [t.strip().upper() for t in os.getenv(
            "WATCH_TERMS", "BTC,BITCOIN,ETH,ETHEREUM,SOL,SOLANA,CRYPTO"
        ).split(",") if t.strip()]

        self.logs: Deque[str] = deque(maxlen=300)
        self.market_snapshots: Dict[str, MarketSnapshot] = {}
        self.raw_market_count: int = 0
        self.normalized_market_count: int = 0
        self.classified_market_count: int = 0
        self.eligible_market_count: int = 0
        self.watched_market_count: int = 0
        self.market_rejections: Counter = Counter()
        self.market_rejection_examples: Dict[str, List[str]] = {}
        self.raw_market_samples: List[Dict[str, Any]] = []
        self.watched_market_samples: List[Dict[str, Any]] = []
        self.last_full_scan_at: Optional[datetime] = None
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
                min_edge=0.09,
                max_spread=0.08,
                max_ticket_dollars=float(os.getenv("MAX_TICKET_DOLLARS", "25")),
                cooldown_seconds=900,
                max_hours_to_expiry=168,
                min_hours_to_expiry=1,
            ),
            "sentiment": StrategyState(
                name="sentiment",
                enabled=False,
                live_enabled=False,
                min_edge=0.12,
                max_spread=0.10,
                max_ticket_dollars=float(os.getenv("MAX_TICKET_DOLLARS", "25")),
                cooldown_seconds=1800,
                max_hours_to_expiry=48,
                min_hours_to_expiry=1,
            ),
        }
        self.cooldowns: Dict[str, datetime] = {}
        self.kalshi_balance: float = 0.0
        self.portfolio_value: float = 0.0
        self.paper_starting_balance: float = float(os.getenv("PAPER_STARTING_BALANCE", "1000"))
        self.session: Optional[aiohttp.ClientSession] = None
        self.loop_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.kalshi_client = KalshiClient()
        self.paper_broker = PaperBroker()
        self.live_broker = LiveKalshiBroker(self.kalshi_client)
        self.log("Bot initialized. Live is disarmed.")

    def log(self, message: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.logs.appendleft(f"[{stamp}] {message}")

    async def start(self) -> None:
        self._migrate_db()
        if self.session is None:
            self.session = aiohttp.ClientSession()
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
        conn.close()
        Base.metadata.create_all(bind=engine)

    async def _main_loop(self) -> None:
        assert self.session is not None
        while True:
            try:
                await self.refresh_markets()
                await self.refresh_balance()
                await self.run_strategies()
                self.health["loop_error"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health["loop_error"] = str(exc)
                self.log(f"Loop error: {exc}")
            await asyncio.sleep(self.poll_seconds)

    async def refresh_spots(self) -> None:
        return

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

    async def refresh_markets(self) -> None:
        assert self.session is not None

        need_full_scan = (
            self.last_full_scan_at is None
            or (datetime.now(timezone.utc) - self.last_full_scan_at).total_seconds() >= self.full_scan_seconds
            or not self.market_snapshots
        )

        if not need_full_scan:
            self.health["last_market_refresh"] = datetime.now(timezone.utc).isoformat()
            return

        raw_markets = await self._fetch_all_open_markets()
        self.last_full_scan_at = datetime.now(timezone.utc)
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
                    })

            eligible, reason = self._evaluate_market_eligibility(classified)
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
            "rejections": dict(self.market_rejections),
            "rejection_examples": self.market_rejection_examples,
            "raw_market_samples": self.raw_market_samples,
            "watched_market_samples": self.watched_market_samples,
            "last_full_scan_at": self.last_full_scan_at.isoformat() if self.last_full_scan_at else None,
            "full_scan_seconds": self.full_scan_seconds,
        }
        self.log(
            f"Market pipeline: raw={self.raw_market_count} normalized={self.normalized_market_count} "
            f"classified={self.classified_market_count} eligible={self.eligible_market_count}"
        )

    async def _fetch_all_open_markets(self) -> List[Dict[str, Any]]:
        assert self.session is not None
        markets: List[Dict[str, Any]] = []
        cursor: Optional[str] = None

        while True:
            path = f"/markets?status=open&limit={self.max_markets}"
            if cursor:
                path += f"&cursor={cursor}"
            data = await self.kalshi_client.get_json(self.session, path)

            batch = data.get("markets", []) or []
            markets.extend(batch)

            cursor = data.get("cursor") or data.get("next_cursor")
            if not cursor or not batch:
                break

            if len(batch) < self.max_markets:
                break

        return markets

    async def refresh_balance(self) -> None:
        if not self.kalshi_client.trading_enabled or self.session is None:
            return
        try:
            data = await self.kalshi_client.get_json(self.session, "/portfolio/balance", auth=True)
            self.kalshi_balance = round(float(data.get("balance", 0)) / 100.0, 2)
            self.portfolio_value = round(float(data.get("portfolio_value", 0)) / 100.0, 2)
            self.health["last_balance_refresh"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            self.log(f"Balance refresh failed: {exc}")

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
            spread = round(max(0.0, yes_ask - yes_bid), 4)

        close_dt = self._parse_dt(
            market.get("close_time")
            or market.get("expiration_time")
            or market.get("settlement_time")
        )

        return {
            "ticker": ticker,
            "title": title,
            "subtitle": subtitle,
            "category": category,
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "last_price": last_price,
            "mid": mid,
            "spread": spread,
            "close_dt": close_dt,
            "close_time": close_dt.isoformat() if close_dt else None,
            "volume_24h": self._float_or_none(market.get("volume_24h_fp") or market.get("volume_24h")),
            "liquidity": self._price_to_float(market.get("liquidity_dollars"), market.get("liquidity")),
            "status": str(market.get("status") or "unknown"),
            "raw": market,
        }

    def _classify_market(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        text = " ".join([
            normalized.get("ticker", ""),
            normalized.get("title", ""),
            normalized.get("subtitle", ""),
            normalized.get("category", ""),
        ])
        asset = self._detect_symbol(text)

        if not asset:
            upper = text.upper()
            for term in self.watch_terms:
                if term in upper:
                    if "BTC" in term or "BITCOIN" in term:
                        asset = "BTC"
                        break
                    if "ETH" in term or "ETHEREUM" in term:
                        asset = "ETH"
                        break
                    if "SOL" in term or "SOLANA" in term:
                        asset = "SOL"
                        break
        threshold, direction = self._parse_threshold(
            f"{normalized.get('title', '')} {normalized.get('subtitle', '')}"
        )

        market_family = "unsupported"
        parse_confidence = "low"
        if asset and threshold and direction:
            market_family = "price_threshold"
            parse_confidence = "high"
        elif asset:
            market_family = "crypto_other"
            parse_confidence = "medium"

        classified = dict(normalized)
        classified.update({
            "asset": asset,
            "threshold": threshold,
            "direction": direction,
            "market_family": market_family,
            "parse_confidence": parse_confidence,
        })
        return classified

    def _evaluate_market_eligibility(self, classified: Dict[str, Any]) -> tuple[bool, str]:
        status = str(classified.get("status", "")).lower()
        if status and status not in {"open", "active", "initialized"}:
            return False, f"status_{status}"
        if not classified.get("asset"):
            return False, "not_crypto_asset"
        if classified.get("market_family") != "price_threshold":
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
        if classified.get("threshold") is None or classified.get("direction") is None:
            return False, "threshold_parse_failed"
        return True, "eligible"

    def _classified_to_snapshot(self, classified: Dict[str, Any]) -> Optional[MarketSnapshot]:
        return MarketSnapshot(
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
            close_time=classified["close_time"],
            raw_close_time=classified["close_dt"],
            volume_24h=classified["volume_24h"],
            liquidity=classified["liquidity"],
            status=classified["status"],
            spot_symbol=classified["asset"],
            threshold=classified["threshold"],
            direction=classified["direction"],
        )

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
        if not snapshot.spot_symbol or not snapshot.threshold or not snapshot.direction:
            snapshot.rationale = "Skipped: couldn't parse market into simple above/below threshold."
            return
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

        sigma_ann = {"BTC": 0.65, "ETH": 0.85, "SOL": 1.20}.get(snapshot.spot_symbol, 0.90)
        sigma_horizon = max(sigma_ann * math.sqrt(hours / (365.0 * 24.0)), 0.04)
        z = math.log(max(spot, 1e-9) / max(snapshot.threshold, 1e-9)) / sigma_horizon
        p_above = 1.0 / (1.0 + math.exp(-1.702 * z))
        fair_yes = p_above if snapshot.direction == "above" else 1.0 - p_above
        fair_yes = min(0.98, max(0.02, fair_yes))
        snapshot.fair_yes = round(fair_yes, 4)
        if snapshot.mid is not None:
            snapshot.edge = round(snapshot.fair_yes - snapshot.mid, 4)
        snapshot.rationale = (
            f"{snapshot.spot_symbol} ${spot:,.0f} vs threshold ${snapshot.threshold:,.0f}; "
            f"fair YES {snapshot.fair_yes:.3f}"
        )

    def _generate_intent(self, strategy: StrategyState, snapshot: MarketSnapshot) -> Optional[OrderIntent]:
        now = datetime.now(timezone.utc)
        strategy.last_signal_at = now.isoformat()
        strategy.last_signal_reason = snapshot.rationale

        if strategy.name != "crypto_lag":
            return None
        if snapshot.mid is None or snapshot.fair_yes is None or snapshot.edge is None:
            return None
        if snapshot.spread is None or snapshot.spread > strategy.max_spread:
            return None
        if not snapshot.raw_close_time:
            return None

        hours_to_expiry = (snapshot.raw_close_time - now).total_seconds() / 3600.0
        if hours_to_expiry < strategy.min_hours_to_expiry or hours_to_expiry > strategy.max_hours_to_expiry:
            return None
        if abs(snapshot.edge) < strategy.min_edge:
            return None

        cooldown_key = f"{strategy.name}:{snapshot.ticker}"
        last = self.cooldowns.get(cooldown_key)
        if last and (now - last).total_seconds() < strategy.cooldown_seconds:
            return None

        if snapshot.edge > 0:
            side = "yes"
            requested_price = snapshot.yes_ask
        else:
            side = "no"
            requested_price = snapshot.no_ask

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
            edge=float(snapshot.edge),
            spot_symbol=snapshot.spot_symbol,
            spot_price=self.spot_prices.get(snapshot.spot_symbol or "", 0.0),
            rationale=(
                f"edge={snapshot.edge:+.3f}, spread={snapshot.spread:.3f}, "
                f"fair_yes={snapshot.fair_yes:.3f}, mid={snapshot.mid:.3f}. {snapshot.rationale}"
            ),
        )

    async def _submit_intent(self, intent: OrderIntent, snapshot: MarketSnapshot) -> None:
        async with self._lock:
            strategy = self.strategy_states[intent.strategy]
            cooldown_key = f"{strategy.name}:{snapshot.ticker}"
            self.cooldowns[cooldown_key] = datetime.now(timezone.utc)

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
                "strategy": row.strategy,
                "ticker": row.ticker,
                "title": row.title,
                "side": row.side,
                "action": row.action,
                "contracts": row.contracts,
                "requested_price": row.requested_price,
                "fill_price": row.fill_price,
                "fair_value": row.fair_value,
                "edge": row.edge,
                "status": row.status,
                "message": row.message,
                "spot_symbol": row.spot_symbol,
                "spot_price": row.spot_price,
                "is_shadow": row.is_shadow,
            }
            for row in rows
        ]

    def _mark_price_for_side(self, row: Trade) -> float:
        snapshot = self.market_snapshots.get(row.ticker)
        if snapshot:
            if row.side == "yes":
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
            qty = float(row.contracts)
            cost = qty * fill
            mark = self._mark_price_for_side(row)
            mark_value = qty * mark
            pnl = mark_value - cost

            total_cost += cost
            total_mark_value += mark_value

            positions.append({
                "ticker": row.ticker,
                "title": row.title,
                "mode": row.mode,
                "side": row.side,
                "contracts": qty,
                "avg_cost": round(fill, 4),
                "mark_price": round(mark, 4),
                "cost_basis": round(cost, 2),
                "mark_value": round(mark_value, 2),
                "unrealized_pnl": round(pnl, 2),
                "status": row.status,
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

    def dashboard_status(self) -> Dict[str, Any]:
        markets = sorted(
            [m.to_dict() for m in self.market_snapshots.values()],
            key=lambda x: abs((x.get("edge") or 0.0)),
            reverse=True,
        )[:30]

        top_edge = max([abs(m.get("edge") or 0.0) for m in markets], default=0.0)

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


@app.get("/api/status")
async def api_status():
    return JSONResponse(bot.dashboard_status())


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
