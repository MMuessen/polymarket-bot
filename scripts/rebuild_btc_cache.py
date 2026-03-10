import asyncio
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import aiohttp

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
OUT = Path("/app/data/crypto_market_cache.json")
SNAP = Path("/app/data/btc_candidate_snapshot.json")

BTC_SERIES = "KXBTC"
ACTIVE = {"open", "active", "initialized"}

MIN_HOURS = 4
MAX_HOURS = 12
MAX_DIST_PCT = 0.05

def parse_close_dt(market: dict):
    raw = (
        market.get("close_time")
        or market.get("expiration_time")
        or market.get("expiration_date")
        or market.get("end_date")
    )
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def market_bucket(m: dict):
    floor_val = m.get("floor_strike")
    cap_val = m.get("cap_strike")
    strike_type = str(m.get("strike_type", "")).lower()

    floor_val = float(floor_val) if floor_val not in (None, "") else None
    cap_val = float(cap_val) if cap_val not in (None, "") else None

    if strike_type == "less":
        return {"kind": "below", "low": None, "high": cap_val}
    if strike_type == "greater":
        return {"kind": "above", "low": floor_val, "high": None}
    if floor_val is not None and cap_val is not None:
        return {"kind": "range", "low": floor_val, "high": cap_val}

    return None

def distance_from_spot_pct(bucket, spot):
    if not bucket:
        return None

    low = bucket["low"]
    high = bucket["high"]

    if low is None and high is not None:
        # below bucket
        if spot <= high:
            return 0.0
        return abs(spot - high) / spot

    if high is None and low is not None:
        # above bucket
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

async def get_json(session: aiohttp.ClientSession, path: str):
    async with session.get(BASE_URL + path, timeout=30) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise RuntimeError(f"GET {path} failed ({resp.status}): {text[:300]}")
        return json.loads(text)

async def get_btc_spot(session: aiohttp.ClientSession):
    url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    async with session.get(url, timeout=15) as resp:
        data = await resp.json()
        return float(data["price"])

async def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    min_close_ts = int((now + timedelta(hours=MIN_HOURS)).timestamp())
    max_close_ts = int((now + timedelta(hours=MAX_HOURS)).timestamp())

    async with aiohttp.ClientSession() as session:
        spot = await get_btc_spot(session)
        print(f"BTC spot: {spot}", flush=True)

        cursor = None
        kept = []
        sample = []
        pages = 0

        while True:
            path = (
                f"/markets?"
                f"series_ticker={BTC_SERIES}"
                f"&status=open"
                f"&mve_filter=exclude"
                f"&min_close_ts={min_close_ts}"
                f"&max_close_ts={max_close_ts}"
                f"&limit=200"
            )
            if cursor:
                path += f"&cursor={cursor}"

            data = await get_json(session, path)
            markets = data.get("markets", []) or []
            pages += 1
            print(f"page {pages}: {len(markets)} markets", flush=True)

            for m in markets:
                ticker = str(m.get("ticker", "")).upper()
                status = str(m.get("status", "")).lower()
                if status not in ACTIVE:
                    continue

                close_dt = parse_close_dt(m)
                if close_dt is None:
                    continue

                hours = (close_dt - datetime.now(timezone.utc)).total_seconds() / 3600.0
                if hours < MIN_HOURS or hours > MAX_HOURS:
                    continue

                bucket = market_bucket(m)
                if not bucket:
                    continue

                dist = distance_from_spot_pct(bucket, spot)
                if dist is None or dist > MAX_DIST_PCT:
                    continue

                kept.append(ticker)

                if len(sample) < 50:
                    sample.append({
                        "ticker": ticker,
                        "title": m.get("title"),
                        "subtitle": m.get("subtitle"),
                        "status": status,
                        "strike_type": m.get("strike_type"),
                        "floor_strike": m.get("floor_strike"),
                        "cap_strike": m.get("cap_strike"),
                        "hours_to_expiry": round(hours, 3),
                        "distance_from_spot_pct": round(dist, 6),
                    })

            cursor = data.get("cursor") or data.get("next_cursor")
            if not cursor or not markets:
                break

            await asyncio.sleep(0.5)

        kept = sorted(set(kept))

        OUT.write_text(json.dumps({
            "written_at": datetime.now(timezone.utc).isoformat(),
            "tickers": kept,
        }, indent=2))

        SNAP.write_text(json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "btc_spot": spot,
            "count": len(kept),
            "series_ticker": BTC_SERIES,
            "pages": pages,
            "sample": sample,
        }, indent=2))

        print(f"saved {len(kept)} BTC tickers to {OUT}", flush=True)
        print(f"snapshot written to {SNAP}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
