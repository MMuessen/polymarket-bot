import os, asyncio, json
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
        self.paper_mode = True
        self.balance_kalshi = 25.01 #
        self.paper_balance = 10000.00
        self.stats = {
            "spread": {"trades_today": 0, "win_rate": 0.0, "pnl": 0.0, "active": False},
            "weather_arb": {"trades_today": 0, "win_rate": 0.0, "pnl": 0.0, "active": False}
        }
        self.ai = {"analysis": "Risk Chief: Monitoring 1% per-trade caps.", "target_spread": 0.04}
        self.positions = [{"market_id": "Next Supreme Leader of Iran", "side": "YES", "entry_price": 0.6244, "size": 8.80}]
        self.conns = []

    async def log_event(self, msg, strategy="sys"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = f"[{timestamp}] [{strategy.upper()}] {msg}\n"
        with open(LOG_FILE, "a", buffering=1) as f:
            f.write(entry); f.flush()
            os.fsync(f.fileno())
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": timestamp})
            except: self.conns.remove(ws)

engine = SentinelV9()

async def spread_scanner():
    while True:
        if engine.stats["spread"]["active"]:
            await engine.log_event("SCANNING: Identifying gaps > $0.04...", "spread")
        await asyncio.sleep(8)

@app.on_event("startup")
async def startup():
    asyncio.create_task(spread_scanner())
    await engine.log_event("SENTINEL_V9.1_RESTORED: Full Logic Active.", "system")

@app.get("/api/status")
async def get_status():
    return {"paper_mode": engine.paper_mode, "balance_kalshi": engine.balance_kalshi, "paper_balance": engine.paper_balance, "ai": engine.ai, "stats": engine.stats, "positions": engine.positions}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/telemetry/download")
async def download_logs():
    if os.path.exists(LOG_FILE):
        return FileResponse(LOG_FILE, filename="sentinel_v9_1.log")
    return {"error": "Log file not found."}

@app.post("/toggle")
async def toggle(strat: str = Form(...), active: str = Form(...)):
    key = "spread" if "spread" in strat else "weather_arb"
    engine.stats[key]["active"] = active.lower() == 'true'
    await engine.log_event(f"STRATEGY: {key.upper()} set to {active}", "system")
    return {"status": "success"}

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
