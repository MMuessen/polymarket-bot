import os, asyncio, json, random, httpx, base64
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

# Initialize environment from local .env if it exists
ENV_FILE = ".env"
if os.path.exists(ENV_FILE):
    load_dotenv(ENV_FILE)

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
SETTINGS_FILE = "bot_settings.json"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV10_2:
    def __init__(self):
        self.load_settings()
        self.balance_real = 25.01 #
        self.max_slots = 10
        self.positions = []
        self.conns = []
        # Load secrets from environment
        self.api_key = os.getenv("KALSHI_API_KEY", "")
        self.priv_key_raw = os.getenv("KALSHI_PRIVATE_KEY", "")

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
            json.dump({
                "mode": self.mode, 
                "spread_active": self.spread_active, 
                "balance_paper": self.balance_paper
            }, f)

    def save_secrets(self, key_id, priv_key):
        """Permanently writes secrets to local .env"""
        self.api_key = key_id
        self.priv_key_raw = priv_key
        with open(ENV_FILE, "w") as f:
            f.write(f"KALSHI_API_KEY={key_id}\n")
            f.write(f"KALSHI_PRIVATE_KEY=\"{priv_key}\"\n")
        load_dotenv(ENV_FILE, override=True)
        return True

    async def get_market_data(self, ticker):
        """Synthetic Ask: 100 - Best NO Bid"""
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(f"{KALSHI_API}/markets/{ticker}/orderbook")
                d = res.json().get('orderbook', {})
                return {
                    "bid": d.get('yes', [[0,0]])[-1][0], 
                    "ask": 100 - d.get('no', [[0,0]])[-1][0], 
                    "liq": d.get('no', [[0,0]])[-1][1]
                }
            except: return None

    async def risk_manager_loop(self):
        """Hunts spreads with 3.5% Fee adjustment"""
        while True:
            skew = 0.05 - ((len(self.positions)/self.max_slots) * 0.03)
            for i, pos in enumerate(self.positions):
                m = await self.get_market_data(pos['market_id'])
                if m and m['bid'] >= (pos['entry_price'] + (skew * 100)):
                    profit = (pos['size'] * ((m['bid'] - pos['entry_price'])/100)) * 0.965
                    if pos['tag'] == "LIVE": self.balance_real += (pos['size'] + profit)
                    else: self.balance_paper += (pos['size'] + profit)
                    self.positions.pop(i)
                    self.save_settings()
                    await self.log_event(f"EXIT: {pos['market_id']} | Net: ${profit:.2f}", "sell")
                    break
            await asyncio.sleep(5)

    async def scanner_loop(self):
        tickers = ["KXNASDAQ100-26MAR26-B18500", "KXFED-26MAR26-B5.25"]
        while True:
            if self.spread_active and len(self.positions) < self.max_slots:
                ticker = random.choice(tickers)
                m = await self.get_market_data(ticker)
                if m and m['liq'] > 100:
                    await self.execute_trade(ticker, m['ask'])
            await asyncio.sleep(15)

    async def execute_trade(self, ticker, price):
        cost = 100.00
        if self.mode == "LIVE": self.balance_real -= cost
        else: self.balance_paper -= cost
        self.positions.append({
            "market_id": ticker, "entry_price": price, 
            "size": cost, "tag": self.mode, "entry_time": datetime.now()
        })
        self.save_settings()
        await self.log_event(f"[{self.mode}] ENTRY: {ticker} @ ${price}", "buy")

    async def log_event(self, msg, strategy="sys"):
        t = datetime.now().strftime("%H:%M:%S")
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": t})
            except: self.conns.remove(ws)

engine = SentinelV10_2()

@app.on_event("startup")
async def startup():
    asyncio.create_task(engine.scanner_loop())
    asyncio.create_task(engine.risk_manager_loop())

@app.get("/api/status")
async def get_status():
    return {
        "mode": engine.mode, 
        "balance_real": engine.balance_real, 
        "balance_paper": engine.balance_paper, 
        "stats": {"spread": {"active": engine.spread_active}}, 
        "positions": engine.positions,
        "vault_loaded": bool(engine.api_key and engine.priv_key_raw)
    }

@app.post("/save_secrets")
async def save_secrets(api_key: str = Form(...), priv_key: str = Form(...)):
    engine.save_secrets(api_key, priv_key)
    await engine.log_event("VAULT: Secrets saved permanently to .env", "system")
    return {"status": "success"}

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    engine.spread_active = (active.lower() == 'true')
    engine.save_settings()
    return {"status": "success"}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
