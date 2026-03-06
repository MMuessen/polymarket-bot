import os, asyncio, json, httpx
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
SETTINGS_FILE = "bot_settings.json"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV98:
    def __init__(self):
        self.load_settings()
        self.balance_real = 25.01 #
        self.max_slots = 10
        self.positions = []
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

    async def get_real_market_data(self, ticker):
        """Fetches 100% real order book from Kalshi production"""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(f"{KALSHI_API}/markets/{ticker}/orderbook")
                data = res.json().get('orderbook', {})
                # Best YES Bid is the highest price in 'yes' array
                best_yes_bid = data.get('yes', [[0,0]])[-1] # [price, quantity]
                # Best NO Bid determines our 'YES Ask' (what we pay to buy YES)
                best_no_bid = data.get('no', [[0,0]])[-1]
                
                return {
                    "bid": best_yes_bid[0],
                    "bid_qty": best_yes_bid[1],
                    "ask": 100 - best_no_bid[0],
                    "ask_qty": best_no_bid[1]
                }
            except Exception as e:
                return None

    async def risk_manager_loop(self):
        """Monitor exits based on REAL market movement"""
        while True:
            for i, pos in enumerate(self.positions):
                market_data = await self.get_real_market_data(pos['market_id'])
                if not market_data: continue

                # Realistic Exit: You can only 'sell' YES if there is a real 'bid'
                if market_data['bid'] >= (pos['entry_price'] + 3):
                    profit = pos['size'] * (market_data['bid'] - pos['entry_price'])
                    self.balance_paper += (pos['size'] + profit)
                    self.positions.pop(i)
                    self.save_settings()
                    await self.log_event(f"REAL PROFIT: Closed {pos['market_id']} at ${market_data['bid']}", "sell")
                    break
            await asyncio.sleep(5)

    async def scanner_loop(self):
        """Bridge to Real Market Opportunities"""
        tickers = ["KXNASDAQ100-26MAR26-B18500", "KXFED-26MAR26-B5.25"] # Real standard tickers
        while True:
            if self.spread_active and len(self.positions) < self.max_slots:
                ticker = random.choice(tickers)
                market_data = await self.get_real_market_data(ticker)
                
                if market_data and market_data['ask_qty'] > 50: # Only fill if > 50 contracts available
                    await self.execute_trade(ticker, "YES", market_data['ask'])
            await asyncio.sleep(15)

    async def execute_trade(self, ticker, side, price):
        self.balance_paper -= 100.00
        self.positions.append({"market_id": ticker, "side": side, "entry_price": price, "size": 100.00, "tag": self.mode, "entry_time": datetime.now()})
        self.save_settings()
        await self.log_event(f"REAL-DATA ENTRY: {ticker} @ ${price}", "buy")

engine = SentinelV98()

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())
    await engine.log_event("SENTINEL_V9.8: Real-Market Bridge Active.", "system")

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
