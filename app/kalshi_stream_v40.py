import asyncio
import base64
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def _utc_now():
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def _parse_dt(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        s = str(v).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _f(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("$", "").replace(",", "")
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _prob(v, dollars_v=None):
    d = _f(dollars_v, None)
    if d is not None:
        if d < 0:
            return 0.0
        if d > 1:
            return 1.0
        return round(d, 4)
    n = _f(v, None)
    if n is None:
        return None
    if n > 1:
        n = n / 100.0
    if n < 0:
        n = 0.0
    if n > 1:
        n = 1.0
    return round(n, 4)


class SnapshotRow(dict):
    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError as e:
            raise AttributeError(k) from e

    def to_dict(self):
        return dict(self)


class KalshiMarketStreamV40:
    def __init__(self, bot_ref):
        self.bot = bot_ref
        self.task = None
        self.session = None
        self.ws = None
        self.stop_evt = asyncio.Event()
        self.stats = {
            "start_calls": 0,
            "seed_calls": 0,
            "seed_successes": 0,
            "ws_connects": 0,
            "ws_messages": 0,
            "ticker_updates": 0,
            "lifecycle_updates": 0,
            "repair_applies": 0,
            "cache_clears": 0,
            "auth_header_uses": 0,
            "auth_header_failures": 0,
            "last_seed_source": None,
            "last_error": None,
            "last_ws_error": None,
            "last_seed_at": None,
            "last_msg_at": None,
            "last_snapshot_count": 0,
            "last_active_count": 0,
            "connected": False,
        }

    def _log(self, msg: str):
        try:
            self.bot.log(f"[v40-stream] {msg}")
        except Exception:
            pass

    def _clear_status_cache(self):
        try:
            if hasattr(self.bot, "_arch_status_cache_v27"):
                delattr(self.bot, "_arch_status_cache_v27")
                self.stats["cache_clears"] += 1
        except Exception:
            pass

    def _private_key_path(self):
        for cand in (
            os.getenv("KALSHI_PRIVATE_KEY_PATH"),
            getattr(getattr(self.bot, "kalshi_client", None), "private_key_path", None),
            "/app/kalshi_private_key.pem",
            "./kalshi_private_key.pem",
        ):
            if cand and os.path.exists(cand):
                return cand
        return None

    def _api_key_id(self):
        for cand in (
            os.getenv("KALSHI_ACCESS_KEY"),
            os.getenv("KALSHI_API_KEY_ID"),
            getattr(getattr(self.bot, "kalshi_client", None), "api_key_id", None),
            getattr(getattr(self.bot, "kalshi_client", None), "key_id", None),
        ):
            if cand:
                return str(cand)
        return None

    def _sign_headers(self):
        key_id = self._api_key_id()
        key_path = self._private_key_path()
        if not key_id or not key_path:
            return None
        try:
            with open(key_path, "rb") as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            ts = str(int(time.time() * 1000))
            msg = f"{ts}GET/trade-api/ws/v2".encode("utf-8")
            sig = private_key.sign(
                msg,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            self.stats["auth_header_uses"] += 1
            return {
                "KALSHI-ACCESS-KEY": key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            }
        except Exception as e:
            self.stats["auth_header_failures"] += 1
            self.stats["last_error"] = repr(e)
            return None

    def _series_list(self):
        raw = os.getenv("KALSHI_STREAM_SERIES", "KXBTC")
        out = [x.strip().upper() for x in raw.split(",") if x.strip()]
        return out or ["KXBTC"]

    def _stream_url(self):
        return os.getenv("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")

    def _public_base(self):
        return os.getenv("KALSHI_PUBLIC_BASE", "https://api.elections.kalshi.com/trade-api/v2")

    def _is_range_bucket(self, row: Dict[str, Any]) -> bool:
        title = str(row.get("title") or "").lower()
        subtitle = str(row.get("subtitle") or "").lower()
        ticker = str(row.get("ticker") or "").upper()
        family = str(row.get("market_family") or "").lower()
        if family == "range_bucket":
            return True
        if "price range" in title:
            return True
        if ticker.startswith(("KXBTC-", "KXETH-", "KXSOL-")) and "$" in subtitle:
            return True
        return False

    def _row_from_market(self, market: Dict[str, Any], event: Optional[Dict[str, Any]] = None):
        if not isinstance(market, dict):
            return None
        ticker = str(market.get("ticker") or "").strip()
        if not ticker:
            return None

        title = market.get("title") or (event or {}).get("title") or ""
        subtitle = market.get("subtitle") or market.get("yes_sub_title") or ""
        close_dt = (
            _parse_dt(market.get("close_dt"))
            or _parse_dt(market.get("raw_close_time"))
            or _parse_dt(market.get("close_time"))
            or _parse_dt(market.get("expiration_time"))
        )
        if close_dt is None or close_dt <= _utc_now():
            return None

        yes_bid = _prob(market.get("yes_bid"), market.get("yes_bid_dollars"))
        yes_ask = _prob(market.get("yes_ask"), market.get("yes_ask_dollars"))
        no_bid = _prob(market.get("no_bid"), market.get("no_bid_dollars"))
        no_ask = _prob(market.get("no_ask"), market.get("no_ask_dollars"))

        spread = None
        if yes_bid is not None and yes_ask is not None:
            spread = round(max(yes_ask - yes_bid, 0.0), 4)
        elif no_bid is not None and no_ask is not None:
            spread = round(max(no_ask - no_bid, 0.0), 4)

        mid = None
        if yes_bid is not None and yes_ask is not None:
            mid = round((yes_bid + yes_ask) / 2.0, 4)

        floor_strike = _f(market.get("floor_strike"), None)
        cap_strike = _f(market.get("cap_strike"), None)
        asset = (
            str(market.get("asset") or market.get("spot_symbol") or "").upper()
            or ("BTC" if ticker.startswith("KXBTC-") else "")
            or ("ETH" if ticker.startswith("KXETH-") else "")
            or ("SOL" if ticker.startswith("KXSOL-") else "")
        )
        market_family = market.get("market_family") or ("range_bucket" if self._is_range_bucket({
            "ticker": ticker, "title": title, "subtitle": subtitle
        }) else None)

        spot = _f((getattr(self.bot, "spot_prices", {}) or {}).get(asset), None)
        dist = None
        if spot and floor_strike is not None and cap_strike is not None:
            center = (floor_strike + cap_strike) / 2.0
            if spot > 0:
                dist = round(abs(center - spot) / spot, 6)

        row = SnapshotRow({
            "ticker": ticker,
            "title": title,
            "subtitle": subtitle,
            "status": str(market.get("status") or "active").lower(),
            "asset": asset,
            "spot_symbol": asset,
            "market_family": market_family,
            "series_ticker": market.get("series_ticker") or (event or {}).get("series_ticker"),
            "event_ticker": market.get("event_ticker") or (event or {}).get("event_ticker"),
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "best_yes_bid": yes_bid,
            "best_yes_ask": yes_ask,
            "best_no_bid": no_bid,
            "best_no_ask": no_ask,
            "bid": yes_bid,
            "ask": yes_ask,
            "mid": mid,
            "spread": spread,
            "yes_bid_size_fp": market.get("yes_bid_size_fp"),
            "yes_ask_size_fp": market.get("yes_ask_size_fp"),
            "floor_strike": floor_strike,
            "cap_strike": cap_strike,
            "threshold": _f(market.get("threshold"), None),
            "direction": market.get("direction"),
            "distance_from_spot_pct": dist,
            "close_dt": _iso(close_dt),
            "close_time": close_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "raw_close_time": _iso(close_dt),
            "updated_time": market.get("updated_time"),
            "fractional_trading_enabled": market.get("fractional_trading_enabled"),
        })
        return row

    def _apply_rows(self, rows: List[Dict[str, Any]], source: str):
        repaired = {}
        now = _utc_now()
        for row in rows:
            if not isinstance(row, dict):
                continue
            ticker = row.get("ticker")
            if not ticker:
                continue
            dt = _parse_dt(row.get("raw_close_time") or row.get("close_dt") or row.get("close_time"))
            if dt is None or dt <= now:
                continue
            repaired[ticker] = SnapshotRow(dict(row))

        active = list(repaired.values())
        latest_close = None
        if active:
            close_times = [
                _parse_dt(r.get("raw_close_time") or r.get("close_dt") or r.get("close_time"))
                for r in active
            ]
            close_times = [x for x in close_times if x is not None]
            latest_close = max(close_times) if close_times else None

        self.bot.market_snapshots = repaired
        self.bot.shadow_market_snapshots = dict(repaired)
        self.bot.raw_market_count = max(len(active), int(getattr(self.bot, "raw_market_count", 0) or 0))
        self.bot.normalized_market_count = len(active)
        self.bot.classified_market_count = len(active)
        self.bot.eligible_market_count = len(active)
        self.bot.watched_market_count = len(active)
        self.bot.market_rejections = {}
        self.bot.market_rejection_examples = {}
        self.bot.raw_market_samples = [
            {
                "ticker": r.get("ticker"),
                "title": r.get("title"),
                "subtitle": r.get("subtitle"),
                "category": "",
                "status": r.get("status"),
            }
            for r in active[:10]
        ]
        self.bot.watched_market_samples = [
            {
                "ticker": r.get("ticker"),
                "title": r.get("title"),
                "asset": r.get("asset"),
                "market_family": r.get("market_family"),
                "status": r.get("status"),
                "threshold": r.get("threshold"),
                "direction": r.get("direction"),
                "distance_from_spot_pct": r.get("distance_from_spot_pct"),
            }
            for r in active[:10]
        ]

        health = getattr(self.bot, "health", None)
        if isinstance(health, dict):
            health["last_market_refresh"] = _iso(_utc_now())
            health["market_pipeline"] = {
                "raw_open_markets": len(active),
                "normalized_markets": len(active),
                "classified_crypto_markets": len(active),
                "eligible_markets": len(active),
                "shadow_candidate_markets": len(active),
                "cached_crypto_ticker_count": len(getattr(self.bot, "cached_crypto_tickers", []) or []),
                "watch_cursor": getattr(self.bot, "watch_cursor", 0),
                "watch_refresh_limit": getattr(self.bot, "watch_refresh_limit", 0),
                "rejections": {},
                "rejection_examples": {},
                "raw_market_samples": self.bot.raw_market_samples,
                "watched_market_samples": self.bot.watched_market_samples,
                "last_full_scan_at": getattr(getattr(self.bot, "last_full_scan_at", None), "isoformat", lambda: None)(),
                "full_scan_seconds": getattr(self.bot, "full_scan_seconds", None),
            }

        self.stats["repair_applies"] += 1
        self.stats["last_seed_source"] = source
        self.stats["last_seed_at"] = _iso(_utc_now())
        self.stats["last_snapshot_count"] = len(active)
        self.stats["last_active_count"] = len(active)
        self._clear_status_cache()

    async def seed_from_public(self, force=False):
        self.stats["seed_calls"] += 1
        if not force:
            current = getattr(self.bot, "market_snapshots", {}) or {}
            if current:
                self.stats["last_snapshot_count"] = len(current)
                return len(current)

        base = self._public_base().rstrip("/")
        rows: List[Dict[str, Any]] = []
        now = _utc_now()

        for series in self._series_list():
            params = {
                "status": "open",
                "series_ticker": series,
                "with_nested_markets": "true",
                "limit": "200",
            }
            url = f"{base}/events?{urllib.parse.urlencode(params)}"
            try:
                with urllib.request.urlopen(
                    urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "kalshi-stream-v40"}),
                    timeout=20,
                ) as r:
                    data = json.load(r)
                events = data.get("events") or []
                for ev in events:
                    for m in (ev.get("markets") or []):
                        row = self._row_from_market(m, ev)
                        if row is not None:
                            dt = _parse_dt(row.get("raw_close_time"))
                            if dt and dt > now:
                                rows.append(row)
            except Exception as e:
                self.stats["last_error"] = repr(e)

        if rows:
            by_ticker = {}
            for r in rows:
                by_ticker[r["ticker"]] = r
            self._apply_rows(list(by_ticker.values()), source="public_events_nested")
            self.stats["seed_successes"] += 1
            return len(by_ticker)

        return 0

    def _current_tickers(self):
        snaps = getattr(self.bot, "market_snapshots", {}) or {}
        return sorted([str(k) for k in snaps.keys() if k])[:200]

    async def _connect_ws(self, headers):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=20, sock_read=None)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return await self.session.ws_connect(
            self._stream_url(),
            heartbeat=20,
            headers=headers or {},
            autoping=True,
        )

    async def _subscribe(self, ws, tickers):
        msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker"],
                "market_tickers": tickers,
                "skip_ticker_ack": True,
            },
        }
        await ws.send_json(msg)

    def _merge_ticker(self, msg: Dict[str, Any]):
        ticker = msg.get("market_ticker") or msg.get("ticker")
        if not ticker:
            return False
        snaps = getattr(self.bot, "market_snapshots", {}) or {}
        row = snaps.get(ticker)
        if row is None:
            row = SnapshotRow({"ticker": ticker})
            snaps[ticker] = row

        changed = False
        for field in ("yes_bid_size_fp", "yes_ask_size_fp", "last_trade_size_fp", "open_interest"):
            if field in msg and row.get(field) != msg.get(field):
                row[field] = msg.get(field)
                changed = True

        yb = _prob(msg.get("yes_bid"), msg.get("yes_bid_dollars"))
        ya = _prob(msg.get("yes_ask"), msg.get("yes_ask_dollars"))
        nb = _prob(msg.get("no_bid"), msg.get("no_bid_dollars"))
        na = _prob(msg.get("no_ask"), msg.get("no_ask_dollars"))

        pairs = {
            "yes_bid": yb,
            "yes_ask": ya,
            "no_bid": nb,
            "no_ask": na,
            "best_yes_bid": yb,
            "best_yes_ask": ya,
            "best_no_bid": nb,
            "best_no_ask": na,
        }
        for k, v in pairs.items():
            if v is not None and row.get(k) != v:
                row[k] = v
                changed = True

        if row.get("yes_bid") is not None and row.get("yes_ask") is not None:
            spread = round(max(row["yes_ask"] - row["yes_bid"], 0.0), 4)
            mid = round((row["yes_ask"] + row["yes_bid"]) / 2.0, 4)
            if row.get("spread") != spread:
                row["spread"] = spread
                changed = True
            if row.get("mid") != mid:
                row["mid"] = mid
                changed = True

        if "time" in msg:
            row["updated_time"] = msg.get("time")

        try:
            self.bot.market_snapshots = snaps
            self.bot.shadow_market_snapshots = dict(snaps)
        except Exception:
            pass

        if changed:
            self.stats["ticker_updates"] += 1
            self.stats["last_msg_at"] = _iso(_utc_now())
            self._clear_status_cache()
        return changed

    async def run_forever(self):
        backoff = 2
        while not self.stop_evt.is_set():
            try:
                await self.seed_from_public(force=False)
                tickers = self._current_tickers()
                if not tickers:
                    await asyncio.sleep(5)
                    await self.seed_from_public(force=True)
                    tickers = self._current_tickers()
                    if not tickers:
                        await asyncio.sleep(10)
                        continue

                headers = self._sign_headers()
                try:
                    self.ws = await self._connect_ws(headers=headers)
                except Exception:
                    self.ws = await self._connect_ws(headers=None)

                self.stats["ws_connects"] += 1
                self.stats["connected"] = True
                await self._subscribe(self.ws, tickers)
                self._log(f"ws subscribed ticker_count={len(tickers)}")

                subscribed = set(tickers)
                while not self.stop_evt.is_set():
                    msg = await self.ws.receive(timeout=30)
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f"ws_closed:{msg.type}")
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue

                    data = json.loads(msg.data)
                    self.stats["ws_messages"] += 1

                    mtype = data.get("type")
                    body = data.get("msg") or {}
                    if mtype == "ticker":
                        self._merge_ticker(body)
                    elif mtype == "market_lifecycle_v2":
                        self.stats["lifecycle_updates"] += 1

                    current = set(self._current_tickers())
                    if current and current != subscribed:
                        break

                backoff = 2
            except Exception as e:
                self.stats["last_ws_error"] = repr(e)
                self.stats["connected"] = False
                try:
                    await self.seed_from_public(force=True)
                except Exception as e2:
                    self.stats["last_error"] = repr(e2)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            finally:
                try:
                    if self.ws is not None:
                        await self.ws.close()
                except Exception:
                    pass
                self.ws = None
                self.stats["connected"] = False

    async def start(self):
        self.stats["start_calls"] += 1
        if self.task and not self.task.done():
            return
        self.task = asyncio.create_task(self.run_forever())

    async def stop(self):
        self.stop_evt.set()
        try:
            if self.task:
                await self.task
        except Exception:
            pass
        try:
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception:
            pass

    def debug_payload(self):
        snaps = getattr(self.bot, "market_snapshots", {}) or {}
        health = getattr(self.bot, "health", {}) or {}
        return {
            "ok": True,
            "stats": self.stats,
            "snapshot_count_now": len(snaps),
            "tickers_now": list(sorted(snaps.keys()))[:20],
            "last_market_refresh": health.get("last_market_refresh"),
            "loop_error": health.get("loop_error"),
            "status_cache_present": hasattr(self.bot, "_arch_status_cache_v27"),
        }


async def attach_stream_v40(bot_ref):
    stream = getattr(bot_ref, "_kalshi_stream_v40", None)
    if stream is None:
        stream = KalshiMarketStreamV40(bot_ref)
        setattr(bot_ref, "_kalshi_stream_v40", stream)
    await stream.start()
    return stream
