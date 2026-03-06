import os, asyncio, json, random
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
LOG_FILE = "trading_session.log"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV9:
    def __init__(self):
        self.mode = "PAPER"  # User Toggle: "PAPER" or "LIVE"
        self.balance_real = 25.01 #
        self.balance_paper = 10000.00
        self.max_slots = 10
        self.risk_per_slot = 0.01 # 1%
        
        self.stats = {
            "spread": {"trades_today": 0, "active": False},
            "weather": {"trades_today": 0, "active": False}
        }
        self.ai = {"analysis": "Risk Chief: Monitoring 1% caps.", "target_spread": 0.04}
        # Permanent Iran position
        self.positions = [{"market_id": "Next Supreme Leader of Iran", "side": "YES", "entry_price": 0.6244, "size": 8.80, "tag": "LIVE"}]
        self.conns = []

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{strategy.upper()}] {msg}\n"
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(entry); f.flush(); os.fsync(f.fileno())
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

    async def execute_trade(self, market, side, price):
        if len(self.positions) >= self.max_slots:
            await self.log_event("VETO: Max 10 slots full.", "ai")
            return
        
        # Determine which balance to use based on mode
        current_bal = self.balance_real if self.mode == "LIVE" else self.balance_paper
        trade_size = current_bal * self.risk_per_slot
        
        # Deduct from the correct balance
        if self.mode == "LIVE": self.balance_real -= trade_size
        else: self.balance_paper -= trade_size

        self.positions.append({
            "market_id": market, 
            "side": side, 
            "entry_price": price, 
            "size": trade_size,
            "tag": self.mode # Adds the "PAPER" or "LIVE" tag
        })
        self.stats["spread"]["trades_today"] += 1
        await engine.log_event(f"[{self.mode}] {side} {market} | Size: ${trade_size:.2f}", "buy")

engine = SentinelV9()

async def scanner_loop():
    while True:
        if engine.stats["spread"]["active"]:
            await engine.log_event(f"SCANNING ({engine.mode}): Checking Kalshi depth...", "spread")
            # Simulation: 20% chance to find a unique trade
            if random.random() > 0.8:
                m_id = f"GAP-{random.randint(100,999)}"
                await engine.execute_trade(m_id, "YES", 0.55)
        await asyncio.sleep(10) # Slower 10s poll for sanity

@app.on_event("startup")
async def startup():
    asyncio.create_task(scanner_loop())
    await engine.log_event("SENTINEL_V9.2: Mode-Aware Logic Active.", "system")

@app.get("/api/status")
async def get_status():
    return {"mode": engine.mode, "balance_real": engine.balance_real, "balance_paper": engine.balance_paper, "stats": engine.stats, "positions": engine.positions, "ai": engine.ai}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.post("/toggle_mode")
async def toggle_mode(mode: str = Form(...)):
    engine.mode = mode.upper()
    await engine.log_event(f"SYSTEM: Switched to {engine.mode} mode.", "system")
    return {"status": "success"}

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    key = "spread" if "spread" in strat else "weather"
    engine.stats[key]["active"] = active.lower() == 'true'
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
