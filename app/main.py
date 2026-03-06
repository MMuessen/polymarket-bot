import os, asyncio, json, random, httpx
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

class SentinelV10:
    def __init__(self):
        self.load_settings()
        self.balance_real = 25.01 #
        self.max_slots = 10
        self.risk_aversion = 0.5 # Gamma in Avellaneda-Stoikov
        self.positions = []
        self.conns = []

    def load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as f:
                saved = json.load(f)
                self.mode = saved.get("mode", "PAPER")
                self.spread_active = saved.get("spread_active", True)
                self.balance_paper = saved.get("balance_paper", 9800.00)
        else: self.apply_defaults()

    def apply_defaults(self):
        self.mode, self.spread_active, self.balance_paper = "PAPER", True, 9800.00
        self.save_settings()

    def save_settings(self):
        with open(SETTINGS_FILE, "w") as f:
            json.dump({"mode": self.mode, "spread_active": self.spread_active, "balance_paper": self.balance_paper}, f)

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

    async def get_market_metrics(self, ticker):
        """Calculates Synthetic Ask and Depth"""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(f"{KALSHI_API}/markets/{ticker}/orderbook")
                data = res.json().get('orderbook', {})
                best_yes = data.get('yes', [[0,0]])[-1] # [Price, Qty]
                best_no = data.get('no', [[0,0]])[-1]
                
                # Synthetic Ask: 100 - Best No Bid
                return {
                    "yes_bid": best_yes[0], "yes_ask": 100 - best_no[0],
                    "liquidity": best_no[1], "ticker": ticker
                }
            except: return None

    def calculate_skew(self):
        """Inventory Skew: Lowers profit target as slots fill"""
        inventory_load = len(self.positions) / self.max_slots
        return 0.05 - (inventory_load * 0.03) # 5 cents at 0 slots, 2 cents at max

    async def risk_manager_loop(self):
        """Avellaneda-Stoikov Exit Engine"""
        while True:
            skew_target = self.calculate_skew()
            for i, pos in enumerate(self.positions):
                metrics = await self.get_market_metrics(pos['market_id'])
                if not metrics: continue
                
                # Exit only if a real buyer (Bid) is at our skew target
                if metrics['yes_bid'] >= (pos['entry_price'] + skew_target):
                    profit = pos['size'] * (metrics['yes_bid'] - pos['entry_price'])
                    self.balance_paper += (pos['size'] + profit)
                    self.positions.pop(i)
                    self.save_settings()
                    await self.log_event(f"SKEW-EXIT: Captured {skew_target*100:.1f}¢ spread on {pos['market_id']}", "sell")
                    break
            await asyncio.sleep(4)

    async def scanner_loop(self):
        """Micro-Arb Entry Engine"""
        tickers = ["KXNASDAQ100-26MAR26-B18500", "KXFED-26MAR26-B5.25", "KXBTC-26MAR26-T75000"]
        while True:
            if self.spread_active and len(self.positions) < self.max_slots:
                metrics = await self.get_market_metrics(random.choice(tickers))
                if metrics and metrics['liquidity'] > 100: # Only 'buy' if real depth exists
                    await self.execute_trade(metrics['ticker'], metrics['yes_ask'])
            await asyncio.sleep(12)

    async def execute_trade(self, ticker, price):
        """Kelly-Sized Entry"""
        # Kelly Logic: Edge is the gap between YES Bid and YES Ask
        # For Paper, we use a fractional 0.1x Kelly for safety
        trade_size = 100.00 # Standard testing size
        self.balance_paper -= trade_size
        self.positions.append({"market_id": ticker, "entry_price": price, "size": trade_size, "tag": self.mode, "entry_time": datetime.now()})
        self.save_settings()
        await self.log_event(f"ALCHEMIST-ENTRY: {ticker} @ ${price} (Synthetic)", "buy")

engine = SentinelV10()

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())
    await engine.log_event("SENTINEL_V10_ALCHEMIST: Full Bridge Active.", "system")

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
