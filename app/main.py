import os, asyncio, json, random
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
LOG_FILE = "trading_session.log"
SETTINGS_FILE = "bot_settings.json"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV96:
    def __init__(self):
        # Initial defaults: Spread Capture is ON by default
        self.defaults = {
            "mode": "PAPER",
            "spread_active": True, 
            "balance_paper": 9800.00
        }
        self.load_settings()
        
        self.balance_real = 25.01 #
        self.max_slots = 10
        self.risk_per_slot = 0.05
        self.stats = {"spread": {"active": self.spread_active}, "volume": 0}
        self.positions = [
            {"market_id": "Supreme Leader Baseline", "side": "YES", "entry_price": 0.62, "size": 8.80, "tag": "LIVE", "entry_time": datetime.now()}
        ]
        self.conns = []

    def load_settings(self):
        """Loads state from disk to survive hot-reloads"""
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    saved = json.load(f)
                    self.mode = saved.get("mode", self.defaults["mode"])
                    self.spread_active = saved.get("spread_active", self.defaults["spread_active"])
                    self.balance_paper = saved.get("balance_paper", self.defaults["balance_paper"])
            except: self.apply_defaults()
        else:
            self.apply_defaults()

    def apply_defaults(self):
        self.mode = self.defaults["mode"]
        self.spread_active = self.defaults["spread_active"]
        self.balance_paper = self.defaults["balance_paper"]
        self.save_settings()

    def save_settings(self):
        """Persists checkbox and balance state"""
        with open(SETTINGS_FILE, "w") as f:
            json.dump({
                "mode": self.mode,
                "spread_active": self.spread_active,
                "balance_paper": self.balance_paper
            }, f)

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{strategy.upper()}] {msg}\n"
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(entry); f.flush()
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

    async def risk_manager_loop(self):
        """V9.6 Exit Engine: Captures spreads & recycles stale trades"""
        while True:
            for i, pos in enumerate(self.positions):
                if "Baseline" in pos['market_id']: continue
                current_bid = round(random.uniform(0.40, 0.60), 2)
                age = (datetime.now() - pos['entry_time']).total_seconds()

                # Take Profit: Spread Capture (+3 cents)
                if current_bid >= (pos['entry_price'] + 0.03):
                    await self.close_position(i, current_bid, "SPREAD CAPTURE")
                    break
                # Stale Protection: Recycle after 60s
                elif age > 60:
                    await self.close_position(i, current_bid, "STALE RECYCLE")
                    break
            await asyncio.sleep(2)

    async def close_position(self, index, exit_price, reason):
        pos = self.positions.pop(index)
        profit = pos['size'] * (exit_price - pos['entry_price'])
        if pos['tag'] == "LIVE": self.balance_real += (pos['size'] + profit)
        else: self.balance_paper += (pos['size'] + profit)
        self.save_settings() # Save balance update
        await engine.log_event(f"[{pos['tag']}] {reason}: Net ${profit:.2f}", "sell")

    async def scanner_loop(self):
        tickers = ["FED-RATE", "NASDAQ-100", "BTC-USD"]
        while True:
            if self.spread_active and len(self.positions) < self.max_slots:
                ticker = random.choice(tickers)
                await self.execute_trade(ticker, "YES", 0.50)
            await asyncio.sleep(8)

    async def execute_trade(self, ticker, side, price):
        trade_size = 100.00
        if self.mode == "LIVE": self.balance_real -= trade_size
        else: self.balance_paper -= trade_size
        self.positions.append({
            "market_id": ticker, "side": side, "entry_price": price, 
            "size": trade_size, "tag": self.mode, "entry_time": datetime.now()
        })
        self.save_settings()
        await self.log_event(f"[{self.mode}] LIMIT BUY: {ticker} @ ${price}", "buy")

engine = SentinelV96()

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())
    await engine.log_event("SENTINEL_V9.6: Persistence Active.", "system")

@app.get("/api/status")
async def get_status(): 
    return {"mode": engine.mode, "balance_real": engine.balance_real, "balance_paper": engine.balance_paper, "stats": engine.stats, "positions": engine.positions}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.post("/toggle_mode")
async def toggle_mode(mode: str = Form(...)):
    engine.mode = mode.upper()
    engine.save_settings()
    await engine.log_event(f"SYSTEM: Switched to {engine.mode} mode.", "system")
    return {"status": "success"}

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    is_active = active.lower() == 'true'
    if "spread" in strat:
        engine.spread_active = is_active
        engine.stats["spread"]["active"] = is_active
        engine.save_settings() # Physically save the checkbox state
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
