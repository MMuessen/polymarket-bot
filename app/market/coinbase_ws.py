"""
CoinbaseSpotFeed: WebSocket-based BTC/ETH/SOL spot price feed.

Connects to Coinbase's public WS feed and maintains a live dict of spot prices.
Reconnects automatically on disconnect. Exposes a simple async-safe API.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
_SYMBOL_MAP = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL"}
_WS_URL = "wss://ws-feed.exchange.coinbase.com"
_REST_URL = "https://api.exchange.coinbase.com/products/{}/ticker"

# Reconnect backoff: 2s, 4s, 8s, 16s, capped at 60s
_BACKOFF_BASE = 2.0
_BACKOFF_MAX = 60.0


class CoinbaseSpotFeed:
    """
    Maintains live spot prices from Coinbase WebSocket.

    Usage:
        feed = CoinbaseSpotFeed()
        await feed.start(session)      # call once; runs forever as a background task
        price = feed.get("BTC")        # non-blocking read
        await feed.stop()
    """

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {"BTC": 0.0, "ETH": 0.0, "SOL": 0.0}
        self._last_update: Dict[str, Optional[datetime]] = {k: None for k in self._prices}
        self._status: str = "stopped"
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._on_update: Optional[Callable[[str, float], None]] = None

    def get(self, symbol: str) -> float:
        return self._prices.get(symbol.upper(), 0.0)

    def all_prices(self) -> Dict[str, float]:
        return dict(self._prices)

    @property
    def status(self) -> str:
        return self._status

    def on_update(self, callback: Callable[[str, float], None]) -> None:
        """Register a callback called whenever a price updates."""
        self._on_update = callback

    async def prime_from_rest(self, session: aiohttp.ClientSession) -> None:
        """Bootstrap prices from REST before WS connects."""
        for product in _PRODUCTS:
            symbol = _SYMBOL_MAP[product]
            try:
                async with session.get(
                    _REST_URL.format(product), timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        px = float(data.get("price", 0))
                        if px > 0:
                            self._prices[symbol] = px
                            self._last_update[symbol] = datetime.now(timezone.utc)
                            logger.info("Coinbase REST prime: %s = %.2f", symbol, px)
            except Exception as exc:
                logger.warning("Coinbase REST prime failed for %s: %s", product, exc)

    async def start(self, session: aiohttp.ClientSession) -> None:
        """Start the background feed task."""
        if self._task and not self._task.done():
            return
        self._stop_evt.clear()
        self._task = asyncio.create_task(
            self._run(session), name="coinbase-spot-feed"
        )
        logger.info("CoinbaseSpotFeed started")

    async def stop(self) -> None:
        self._stop_evt.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._status = "stopped"
        logger.info("CoinbaseSpotFeed stopped")

    async def _run(self, session: aiohttp.ClientSession) -> None:
        attempt = 0
        while not self._stop_evt.is_set():
            try:
                await self._connect(session)
                attempt = 0  # reset on successful connection
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._status = "reconnecting"
                backoff = min(_BACKOFF_BASE * (2 ** attempt), _BACKOFF_MAX)
                logger.warning(
                    "CoinbaseSpotFeed disconnected (%s). Reconnecting in %.0fs.", exc, backoff
                )
                attempt += 1
                try:
                    await asyncio.wait_for(
                        self._stop_evt.wait(), timeout=backoff
                    )
                except asyncio.TimeoutError:
                    pass

    async def _connect(self, session: aiohttp.ClientSession) -> None:
        self._status = "connecting"
        async with session.ws_connect(
            _WS_URL, heartbeat=20, autoping=True
        ) as ws:
            self._status = "connected"
            logger.info("CoinbaseSpotFeed connected")

            await ws.send_json({
                "type": "subscribe",
                "product_ids": _PRODUCTS,
                "channels": ["ticker"],
            })

            async for msg in ws:
                if self._stop_evt.is_set():
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(msg.data)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break

        self._status = "disconnected"

    def _handle_message(self, raw: str) -> None:
        try:
            import json
            data = json.loads(raw)
            if data.get("type") != "ticker":
                return
            product = data.get("product_id", "")
            symbol = _SYMBOL_MAP.get(product)
            if not symbol:
                return
            price_str = data.get("price")
            if price_str is None:
                return
            px = float(price_str)
            if px <= 0:
                return
            self._prices[symbol] = px
            self._last_update[symbol] = datetime.now(timezone.utc)
            if self._on_update:
                self._on_update(symbol, px)
        except Exception as exc:
            logger.debug("CoinbaseSpotFeed parse error: %s", exc)

    def health_dict(self) -> dict:
        return {
            "status": self._status,
            "prices": dict(self._prices),
            "last_update": {
                k: v.isoformat() if v else None
                for k, v in self._last_update.items()
            },
        }
