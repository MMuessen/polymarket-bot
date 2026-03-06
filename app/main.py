import os, asyncio, json, random
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Core logic: We use the real Kalshi order book reciprocal relationship
# YES_ASK = 100 - BEST_NO_BID
# YES_BID = BEST_YES_BID

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
SETTINGS_FILE = "bot_settings.json"
if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV97:
    def __init__(self):
        self.load_settings()
        self.balance_real = 25.01
        self.max_slots = 10
        self.risk_per_slot = 100.00 # For Paper
        self.positions = [{"market_id": "Iran Supreme Leader", "side": "YES", "entry_price": 0.62, "size": 8.80, "tag": "LIVE", "entry_time": datetime.now()}]
        self.conns = []

    def load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as f:
                saved = json.load(f)
                self.mode = saved.get("mode", "PAPER")
                self.spread_active = saved.get("spread_active", True)
                self.balance_paper = saved.get("balance_paper", 9800.00)
        else: self.mode, self.spread_active, self.balance_paper = "PAPER", True, 9800.00

    def save_settings(self):
        with open(SETTINGS_FILE, "w") as f:
            json.dump({"mode": self.mode, "spread_active": self.spread_active, "balance_paper": self.balance_paper}, f)

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

    async def risk_manager_loop(self):
        """V9.7 Exit Hunter: Uses synthetic spreads to close"""
        while True:
            for i, pos in enumerate(self.positions):
                if "Supreme" in pos['market_id']: continue
                
                # Mocking real-time depth pull
                # In V9.8 we will replace this with: await kalshi.get_orderbook(pos['market_id'])
                current_bid = round(random.uniform(0.40, 0.60), 2)
                
                # SPREAD CAPTURE: Exit if bid is 3 cents above our entry
                if current_bid >= (pos['entry_price'] + 0.03):
                    profit = pos['size'] * (current_bid - pos['entry_price'])
                    self.balance_paper += (pos['size'] + profit)
                    self.positions.pop(i)
                    self.save_settings()
                    await self.log_event(f"SUCCESS: Captured $0.03 spread on {pos['market_id']}", "sell")
                    break
            await asyncio.sleep(2)

    async def scanner_loop(self):
        """V9.7 Real-Market Scanner"""
        real_tickers = ["FED-26MAR-B5.25", "NASDAQ-26MAR-18500", "BTC-26MAR-75000"]
        while True:
            if self.spread_active and len(self.positions) < self.max_slots:
                ticker = random.choice(real_tickers)
                # Buy at the simulated 'Best Bid' to act as a MAKER
                await self.execute_trade(ticker, "YES", 0.50)
            await asyncio.sleep(10)

    async def execute_trade(self, ticker, side, price):
        self.balance_paper -= 100.00
        self.positions.append({"market_id": ticker, "side": side, "entry_price": price, "size": 100.00, "tag": self.mode, "entry_time": datetime.now()})
        self.save_settings()
        await self.log_event(f"[{self.mode}] ENTRY: {ticker} @ ${price}", "buy")

engine = SentinelV97()

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())

@app.get("/api/status")
async def get_status(): return {"mode": engine.mode, "balance_real": engine.balance_real, "balance_paper": engine.balance_paper, "stats": {"spread": {"active": engine.spread_active}}, "positions": engine.positions}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    engine.spread_active = (active.lower() == 'true')
    engine.save_settings()
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
