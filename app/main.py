import os
import time
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List
import aiohttp
import feedparser
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# DB
Base = declarative_base()
engine = create_engine('sqlite:///data/trades.db')
Session = sessionmaker(bind=engine)

class Trade(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True)
    platform = Column(String, default='kalshi')
    strat = Column(String)
    market_id = Column(String)
    side = Column(String)
    size = Column(Float)
    entry_price = Column(Float)
    pnl = Column(Float)
    paper = Column(Boolean)
    timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String, default='open')
    exit_price = Column(Float)
    hold_time = Column(Float)

Base.metadata.create_all(engine)

class RichBot:
    def __init__(self):
        self.paper_mode = os.getenv("PAPER_MODE", "true").lower() == "true"
        self.ollama_url = "http://192.168.1.249:11434"
        self.ollama_model = os.getenv("OLLAMA_MODEL", "llama3.2")
        self.mode = os.getenv("MODE", "kalshi").lower()
        self.strats = {
            'crypto_lag': {'paper': True, 'live': False, 'perf': {'trades':0, 'wins':0, 'pnl':0, 'ema_hold_win':60.0, 'ema_hold_loss':60.0}, 'risk':0.004, 'dynamic_interval':60, 'dynamic_profit':0.02, 'dynamic_stop':-0.015},
            'sentiment': {'paper': True, 'live': False, 'perf': {'trades':0, 'wins':0, 'pnl':0, 'ema_hold_win':60.0, 'ema_hold_loss':60.0}, 'risk':0.004, 'dynamic_interval':60, 'dynamic_profit':0.02, 'dynamic_stop':-0.015},
        }
        self.balance_poly = 0.0
        self.balance_kalshi = 0.0
        self.deposits_lifetime = 0.0
        self.withdraws_lifetime = 0.0
        self.exposure = 0.0
        self.loss_hour = 0.0
        self.last_hour = datetime.now()
        self.binance_prices = {'BTC': 0, 'ETH': 0, 'SOL': 0}
        self.kalshi_prices = {}
        self.rss_urls = ["https://cointelegraph.com/rss", "https://cryptopotato.com/feed/"]
        self.positions = []
        self.load_positions()
        asyncio.create_task(self.init_ollama())
        asyncio.create_task(self.poll_binance_prices())
        asyncio.create_task(self.poll_kalshi_markets())
        asyncio.create_task(self.monitor_closes())
        asyncio.create_task(self.update_balances())
        asyncio.create_task(self.test_paper_trade())  # Added for immediate paper testing
        print("🚀 Kalshi-Only Bot Started")

    def load_positions(self):
        with Session() as session:
            self.positions = session.query(Trade).filter(Trade.status == 'open').all()

    async def init_ollama(self):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{self.ollama_url}") as resp:
                    print("Ollama health check:", await resp.text())
            except Exception as e:
                print("Ollama connection failed:", str(e))

    async def ollama_sentiment(self, text):
        messages = [{"role": "user", "content": f"Analyze sentiment: positive, negative, or neutral? Respond only with the word: {text[:500]}"}]
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(f"{self.ollama_url}/api/chat", json={"model": self.ollama_model, "messages": messages, "stream": False}) as resp:
                    data = await resp.json()
                    return data.get('message', {}).get('content', 'neutral').strip().lower()
            except Exception as e:
                print("Ollama sentiment error:", str(e))
                return "neutral"

    async def update_balances(self):
        while True:
            try:
                # Real Kalshi balance fetch - replace with SDK if available
                url = "https://api.elections.kalshi.com/trade-api/v2/balance"  # Placeholder - check docs
                headers = {"Authorization": f"Bearer {os.getenv('KALSHI_API_KEY_ID')}"}
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.balance_kalshi = data.get('balance', 0.0)
                            print(f"Updated Kalshi balance: ${self.balance_kalshi:.2f}")
                        else:
                            print("Balance fetch failed:", resp.status)
            except Exception as e:
                print("Balance update failed:", str(e))
            await asyncio.sleep(60)

    async def poll_binance_prices(self):
        coins = ['bitcoin', 'ethereum', 'solana']
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(coins)}&vs_currencies=usd"
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for coin in coins:
                                price = data.get(coin, {}).get('usd', 0)
                                sym = coin.upper()[:3]  # BTC, ETH, SOL
                                self.binance_prices[sym] = price
                                print(f"CoinGecko update: {sym} = ${price:,.0f}")
                                await self.check_opps(sym)
                        else:
                            print(f"CoinGecko fetch failed: {resp.status}")
            except Exception as e:
                print("Price poll error:", str(e))
            await asyncio.sleep(10)

    async def poll_kalshi_markets(self):
        url = "https://api.elections.kalshi.com/trade-api/v2/markets?limit=200"
        headers = {"Authorization": f"Bearer {os.getenv('KALSHI_API_KEY_ID')}"}
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            found = 0
                            sample_tickers = []
                            sample_questions = []
                            sample_categories = []
                            for m in data.get('markets', []):
                                ticker = m.get('ticker', '').upper()
                                question = m.get('question', '').upper()
                                category = m.get('category', '').upper()
                                sample_tickers.append(ticker[:30])
                                sample_questions.append(question[:50])
                                sample_categories.append(category)
                                if any(x in ticker or x in question or x in category for x in ['BTC', 'BITCOIN', 'ETH', 'ETHEREUM', 'SOL', 'SOLANA', 'CRYPTO']):
                                    found += 1
                                    yes_bid = m.get('yes_bid', 0) or 0
                                    yes_ask = m.get('yes_ask', 0) or 0
                                    mid = (yes_bid + yes_ask) / 200
                                    self.kalshi_prices[ticker] = mid
                                    print(f"Kalshi crypto market: {ticker} ({question[:50]}) mid-price {mid:.4f} (category: {category})")
                            print(f"Kalshi poll OK - found {found} crypto markets (total {len(data.get('markets', []))} markets)")
                            if found == 0:
                                print(f"Sample tickers (first 5): {sample_tickers[:5]}")
                                print(f"Sample questions (first 5): {sample_questions[:5]}")
                                print(f"Sample categories (first 5): {sample_categories[:5]}")
                        else:
                            text = await resp.text()
                            print(f"Kalshi poll failed - status {resp.status}: {text}")
                except Exception as e:
                    print("Kalshi poll exception:", str(e))
                await asyncio.sleep(10)

    async def scan_sentiment_opps(self):
        opps = []
        print("Scanning RSS for sentiment...")
        for url in self.rss_urls:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                age = datetime.now() - datetime(*entry.published_parsed[:6])
                if age < timedelta(minutes=15):
                    print(f"Recent news: {entry.title[:50]}...")
                    sentiment = await self.ollama_sentiment(entry.title + " " + (entry.summary or ""))
                    print(f"  → Sentiment: {sentiment}")
                    if sentiment == "positive":
                        opps.append(('sentiment_up', 'EXAMPLE_KALSHI_MARKET_ID', 'BUY', 5.0))
                    elif sentiment == "negative":
                        opps.append(('sentiment_down', 'EXAMPLE_KALSHI_MARKET_ID', 'SELL', 5.0))
        print(f"Found {len(opps)} sentiment opportunities")
        return opps

    async def test_paper_trade(self):
        while True:
            await asyncio.sleep(60)
            print("Test trigger: Simulating a positive sentiment opp")
            await self.execute('sentiment_up', 'kalshi', 'TEST_MARKET_ID', 'BUY', 5.0)

    async def check_opps(self, sym):
        sent_opps = await self.scan_sentiment_opps()
        for opp in sent_opps:
            strat = opp[0]
            market_id = opp[1]
            side = opp[2]
            size = opp[3]
            await self.execute(strat, 'kalshi', market_id, side, size)

    async def execute(self, strat, platform, market_id, side, size):
        entry_p = await self.get_current_price(platform, market_id)
        trade = Trade(platform=platform, strat=strat, market_id=market_id, side=side, size=size, entry_price=entry_p, paper=self.paper_mode)
        with Session() as session:
            session.add(trade)
            session.commit()
        self.positions.append(trade)
        if self.paper_mode:
            await asyncio.sleep(2)
            exit_p = entry_p * 1.02 if side == 'BUY' else entry_p * 0.98
            pnl = size * (exit_p - entry_p) if side == 'BUY' else size * (entry_p - exit_p)
            print(f"📝 PAPER {strat} | Entry: {entry_p:.4f} | Exit: {exit_p:.4f} | PNL: ${pnl:.2f}")
        else:
            print(f"✅ REAL KALSHI {strat} executed (placeholder)")
        return trade

    async def monitor_closes(self):
        while True:
            for strat, s in self.strats.items():
                for pos in [p for p in self.positions if p.strat == strat and p.status == 'open'][:]:
                    current_p = await self.get_current_price(pos.platform, pos.market_id)
                    pnl_pct = (current_p - pos.entry_price) / pos.entry_price if pos.side == 'BUY' else (pos.entry_price - current_p) / pos.entry_price
                    pred_hold = await self.ollama_predict_hold(pos.market_id)
                    hold_elapsed = (datetime.now() - pos.timestamp).total_seconds()
                    if hold_elapsed > pred_hold * 1.5:
                        await self.close_position(pos, current_p)
                        self.positions.remove(pos)
                        continue
                    sent = await self.ollama_sentiment(f"Current sentiment for {pos.market_id}")
                    flip = (sent == 'negative' and pos.side == 'BUY') or (sent == 'positive' and pos.side == 'SELL')
                    expiry_min = await self.get_expiry_min(pos.platform, pos.market_id)
                    if pnl_pct > s['dynamic_profit'] or pnl_pct < s['dynamic_stop'] or flip or expiry_min < 2:
                        await self.close_position(pos, current_p)
                        self.positions.remove(pos)
            await asyncio.sleep(30)

    async def get_current_price(self, platform, market_id):
        return self.kalshi_prices.get(market_id, 0.5)

    async def get_expiry_min(self, platform, market_id):
        return 5

    async def close_position(self, pos, exit_p):
        pnl = pos.size * (exit_p - pos.entry_price) if pos.side == 'BUY' else pos.size * (pos.entry_price - exit_p)
        pos.pnl = pnl
        pos.exit_price = exit_p
        pos.hold_time = (datetime.now() - pos.timestamp).total_seconds()
        pos.status = 'closed'
        with Session() as session:
            session.merge(pos)
            session.commit()
        self.update_perf(pos.strat, pnl)

    def update_perf(self, strat, pnl):
        s = self.strats[strat]
        s['perf']['trades'] += 1
        if pnl > 0:
            s['perf']['wins'] += 1
        s['perf']['pnl'] += pnl
        alpha = 0.1
        ema_key = 'ema_hold_win' if pnl > 0 else 'ema_hold_loss'
        s['perf'][ema_key] = alpha * pos.hold_time + (1 - alpha) * s['perf'][ema_key]
        if s['perf']['trades'] % 20 == 0 and s['perf']['trades'] > 0:
            ema_win = s['perf']['ema_hold_win']
            ema_loss = s['perf']['ema_hold_loss']
            s['dynamic_interval'] = max(10, min(120, ema_win * 1.2 if ema_win < ema_loss else ema_loss * 0.8))
            s['dynamic_profit'] = 0.015 if ema_win < 30 else 0.025
            s['dynamic_stop'] = -0.02 if ema_loss > 90 else -0.012
            print(f"🔥 ADAPTED {strat}: Interval={s['dynamic_interval']}s, Profit={s['dynamic_profit']*100}%, Stop={s['dynamic_stop']*100}%")

bot = RichBot()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "bot": bot})

@app.post("/toggle")
async def toggle(strat: str = Form(...), live: bool = Form(...)):
    bot.strats[strat]['live'] = live
    return {"status": "ok"}

@app.post("/set_mode")
async def set_mode(mode: str = Form(...)):
    bot.mode = mode.lower()
    return {"status": "ok"}

@app.post("/toggle_paper")
async def toggle_paper(paper: bool = Form(...)):
    bot.paper_mode = paper
    return {"status": "ok"}

@app.get("/api/status")
async def status():
    return {
        "strats": bot.strats,
        "balance_kalshi": bot.balance_kalshi,
        "balance_poly": bot.balance_poly,
        "mode": bot.mode,
        "paper_mode": bot.paper_mode
    }
EOF
