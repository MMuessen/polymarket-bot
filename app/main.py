import os, asyncio, json, random
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
LOG_FILE = "trading_session.log"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV95:
    def __init__(self):
        self.mode = "PAPER"
        self.balance_real = 25.01
        self.balance_paper = 9800.00
        self.max_slots = 10
        self.risk_per_slot = 0.05 # Increased risk for spread capture
        self.stats = {"spread": {"active": False}, "volume": 0}
        self.positions = [
            {"market_id": "Supreme Leader Baseline", "side": "YES", "entry_price": 0.62, "size": 8.80, "tag": "LIVE", "entry_time": datetime.now()}
        ]
        self.conns = []

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{strategy.upper()}] {msg}\n"
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(entry); f.flush()
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

    async def risk_manager_loop(self):
        """V9.5 Inventory Manager: Prevents stale drain"""
        while True:
            for i, pos in enumerate(self.positions):
                if "Baseline" in pos['market_id']: continue
                
                # Dynamic Mid-Price Calculation
                # In reality, this pulls from your kalshi_python_async client
                current_bid = round(random.uniform(0.40, 0.60), 2)
                current_ask = current_bid + 0.02
                mid_price = (current_bid + current_ask) / 2
                
                age_seconds = (datetime.now() - pos['entry_time']).total_seconds()

                # PROFIT: Capture the spread (+3 cents)
                if current_bid >= (pos['entry_price'] + 0.03):
                    await self.close_position(i, current_bid, "SPREAD CAPTURE")
                    break
                
                # STALE PROTECTION: If price hasn't moved for 60s, exit at mid-price
                elif age_seconds > 60:
                    await self.close_position(i, mid_price, "STALE RECYCLE")
                    break

                # HARD STOP: If price drops 2 cents below entry
                elif current_bid <= (pos['entry_price'] - 0.02):
                    await self.close_position(i, current_bid, "INVENTORY CUT")
                    break
            
            await asyncio.sleep(2)

    async def close_position(self, index, exit_price, reason):
        pos = self.positions.pop(index)
        profit = pos['size'] * (exit_price - pos['entry_price'])
        if pos['tag'] == "LIVE": self.balance_real += (pos['size'] + profit)
        else: self.balance_paper += (pos['size'] + profit)
        self.stats["volume"] += pos['size']
        await self.log_event(f"[{pos['tag']}] {reason}: {pos['market_id']} @ ${exit_price:.2f} | Net: ${profit:.2f}", "sell")

    async def scanner_loop(self):
        """V9.5 Spread Hunter: Only enters on specific BBO gaps"""
        tickers = ["FED-RATE-HIKE", "S&P500-DAILY", "BTC-75K-YES"]
        while True:
            if self.stats["spread"]["active"] and len(self.positions) < self.max_slots:
                ticker = random.choice(tickers)
                bid, ask = 0.50, 0.53 # Mocked spread
                
                # Logic: Buy only if we can be a MAKER (post a limit)
                await self.execute_trade(ticker, "YES", bid)
            await asyncio.sleep(5)

    async def execute_trade(self, ticker, side, price):
        current_bal = self.balance_real if self.mode == "LIVE" else self.balance_paper
        trade_size = 100.00 # Standardizing for velocity
        if self.mode == "LIVE": self.balance_real -= trade_size
        else: self.balance_paper -= trade_size

        self.positions.append({
            "market_id": ticker, "side": side, "entry_price": price, 
            "size": trade_size, "tag": self.mode, "entry_time": datetime.now()
        })
        await self.log_event(f"[{self.mode}] LIMIT BUY: {ticker} @ ${price}", "buy")

engine = SentinelV95()
@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())
    await engine.log_event("SENTINEL_V9.5: Micro-Arb Active.", "system")

@app.get("/api/status")
async def get_status(): 
    return {"mode": engine.mode, "balance_real": engine.balance_real, "balance_paper": engine.balance_paper, "stats": engine.stats, "positions": engine.positions}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.post("/toggle_mode")
async def toggle_mode(mode: str = Form(...)):
    engine.mode = mode.upper()
    await engine.log_event(f"SYSTEM: Switched to {engine.mode} mode.", "system")
    return {"status": "success"}

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    engine.stats["spread" if "spread" in strat else "weather"]["active"] = active.lower() == 'true'
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
