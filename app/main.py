import os, asyncio, json, random
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
LOG_FILE = "trading_session.log"

class SentinelV9:
    def __init__(self):
        self.paper_mode = True
        self.balance_kalshi = 25.01 # Baseline
        self.paper_balance = 10000.00
        self.max_slots = 10
        self.risk_per_slot = 0.01 # 1% Guardrail
        self.stats = {
            "spread": {"trades_today": 0, "win_rate": 0.0, "pnl": 0.0, "active": False},
            "weather": {"trades_today": 0, "win_rate": 0.0, "pnl": 0.0, "active": False}
        }
        self.ai = {"analysis": "Risk Chief: Monitoring 1% caps on 10 slots.", "target_spread": 0.04}
        # Permanent Iran position
        self.positions = [{"market_id": "Next Supreme Leader of Iran", "side": "YES", "entry_price": 0.6244, "size": 8.80}]
        self.conns = []

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{strategy.upper()}] {msg}\n"
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(entry); f.flush()
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

engine = SentinelV9()

async def spread_scanner():
    """Wired to the Global Spread Capture UI checkbox"""
    while True:
        if engine.stats["spread"]["active"]:
            await engine.log_event("SCANNING: Analyzing bid/ask depth for 1% spread capture...", "spread")
            # Simulation: Find a gap, place a paper trade
            if random.random() > 0.8:
                trade_size = engine.balance_kalshi * engine.risk_per_slot
                engine.positions.append({"market_id": "SCAN-FOUND-MKTR", "side": "YES", "entry_price": 0.72, "size": trade_size})
                engine.stats["spread"]["trades_today"] += 1
                await engine.log_event(f"PAPER TRADE: YES SCAN-FOUND-MKTR | Risk: ${trade_size:.2f}", "buy")
        await asyncio.sleep(8)

@app.on_event("startup")
async def startup():
    asyncio.create_task(spread_scanner())
    await engine.log_event("SENTINEL_V9.1_REDEPLOYED: Auto-Deploy Logic Active.", "system")

@app.get("/api/status")
async def get_status():
    return {"paper_mode": engine.paper_mode, "balance_kalshi": engine.balance_kalshi, "paper_balance": engine.paper_balance, "stats": engine.stats, "positions": engine.positions, "ai": engine.ai}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/telemetry/download")
async def dl(): return FileResponse(LOG_FILE, filename="sentinel_v9_1.log")

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    key = "spread" if "spread" in strat else "weather"
    engine.stats[key]["active"] = active.lower() == 'true'
    await engine.log_event(f"ENGINE: {key.upper()} {'Enabled' if active.lower() == 'true' else 'Disabled'}", "system")
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
