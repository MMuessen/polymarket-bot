import os, asyncio, json, httpx, time, base64, uuid
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV10_Master:
    def __init__(self):
        # 1. Credentials & Config
        self.api_key = os.getenv("KALSHI_API_KEY", "")
        self.priv_key_raw = os.getenv("KALSHI_PRIVATE_KEY", "").replace("\\n", "\n")
        self.openai_key = os.getenv("OPENAI_API_KEY", "")
        
        # 2. State Management
        self.mode = "PAPER" # "PAPER" or "LIVE"
        self.balance_paper = 10000.00
        self.balance_real = 0.00
        self.live_positions = []
        self.paper_positions = []
        self.active_signals = [] # For GenAI Decision tracking
        self.conns = []
        
        # 3. Strategy Hyper-parameters (Avellaneda-Stoikov)
        self.gamma = 0.1    # Risk aversion
        self.sigma = 0.02   # Volatility (estimated)
        self.k = 1.5        # Liquidity parameter
        self.max_inventory = 100

    # --- AUTHENTICATION LAYER (RSA PSS) ---
    def sign_request(self, method, path):
        if not self.priv_key_raw: return None, None
        try:
            timestamp = str(int(time.time() * 1000))
            msg = f"{timestamp}{method}{path.split('?')[0]}"
            pkey = serialization.load_pem_private_key(self.priv_key_raw.encode(), password=None)
            signature = pkey.sign(
                msg.encode(),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                hashes.SHA256()
            )
            return base64.b64encode(signature).decode(), timestamp
        except Exception as e:
            print(f"Signing Error: {e}")
            return None, None

    # --- DATA SYNC LAYER (The Harmony) ---
    async def sync_all_data(self):
        """Always-on sync for Live Wallet and Arbitrage opportunities."""
        if not self.api_key: return
        async with httpx.AsyncClient() as client:
            try:
                # Sync Real Balance
                path = "/trade-api/v2/portfolio/balance"
                sig, ts = self.sign_request("GET", path)
                if sig:
                    res = await client.get(f"https://api.elections.kalshi.com{path}", 
                        headers={"KALSHI-ACCESS-KEY": self.api_key, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": ts})
                    if res.status_code == 200: self.balance_real = res.json().get('balance', 0) / 100

                # Sync Real Positions
                path_pos = "/trade-api/v2/portfolio/positions"
                sig_p, ts_p = self.sign_request("GET", path_pos)
                if sig_p:
                    res_p = await client.get(f"https://api.elections.kalshi.com{path_pos}", 
                        headers={"KALSHI-ACCESS-KEY": self.api_key, "KALSHI-ACCESS-SIGNATURE": sig_p, "KALSHI-ACCESS-TIMESTAMP": ts_p})
                    if res_p.status_code == 200: self.live_positions = res_p.json().get('market_positions', [])
            except: pass

    # --- STRATEGY ENGINE (Inventory Skew Logic) ---
    def calculate_skew(self, mid_price, current_inventory):
        """Avellaneda-Stoikov Reservation Price calculation."""
        # Reservation Price = mid - (inventory * gamma * sigma^2)
        reservation_price = mid_price - (current_inventory * self.gamma * (self.sigma ** 2))
        # Spread = (2/gamma) * ln(1 + gamma/k)
        spread = (2 / self.gamma) * (0.5 * self.gamma * (self.sigma**2)) + ( (2/self.gamma) * (1 + (self.gamma/self.k)) )
        return reservation_price, spread

    # --- GEN-AI LAYER (LLM CONVICTION) ---
    async def get_ai_conviction(self, market_data):
        """Consults LLM for trade confirmation."""
        if not self.openai_key: return 0.5
        # This acts as the 'ajwann' style decision gateway
        return 0.85 # Placeholder for successful LLM handshake

    async def log_event(self, msg, category="sys"):
        t = datetime.now().strftime("%H:%M:%S")
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": category, "time": t})
            except: self.conns.remove(ws)

engine = SentinelV10_Master()

@app.on_event("startup")
async def startup_event():
    async def loop():
        while True:
            await engine.sync_all_data()
            await asyncio.sleep(15)
    asyncio.create_task(loop())

@app.get("/api/status")
async def get_status():
    return {
        "mode": engine.mode,
        "balance_paper": engine.balance_paper,
        "balance_real": engine.balance_real,
        "vault_ready": bool(engine.api_key),
        "live_positions": engine.live_positions,
        "paper_positions": engine.paper_positions
    }

@app.post("/api/set_mode")
async def set_mode(mode: str = Form(...)):
    engine.mode = mode
    await engine.log_event(f"System: Execution switched to {mode}", "system")
    return {"status": "success"}

@app.post("/save_secrets")
async def save_secrets(api_key: str = Form(...), priv_key: str = Form(...)):
    # Write to local persistent env for Docker restarts
    with open(".env", "a") as f:
        f.write(f"\nKALSHI_API_KEY={api_key}\nKALSHI_PRIVATE_KEY=\"{priv_key}\"")
    engine.api_key = api_key
    engine.priv_key_raw = priv_key
    await engine.sync_all_data()
    return {"status": "success"}

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
