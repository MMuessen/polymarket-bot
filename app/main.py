import os, asyncio, json, random, httpx, base64
from datetime import datetime
from fastapi import FastAPI, Request, WebSocket, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

ENV_FILE = ".env"
if os.path.exists(ENV_FILE): load_dotenv(ENV_FILE)

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
SETTINGS_FILE = "bot_settings.json"
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"

if not os.path.exists("app/static"): os.makedirs("app/static")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

class SentinelV10_2:
    def __init__(self):
        self.load_settings()
        self.api_key = os.getenv("KALSHI_API_KEY", "")
        self.priv_key_raw = os.getenv("KALSHI_PRIVATE_KEY", "")
        self.positions = []
        self.conns = []

    def load_settings(self):
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, "r") as f:
                saved = json.load(f)
                self.balance_paper = saved.get("balance_paper", 10286.00) #
        else: self.balance_paper = 10286.00

    def save_secrets(self, key_id, priv_key):
        with open(ENV_FILE, "w") as f:
            f.write(f"KALSHI_API_KEY={key_id}\nKALSHI_PRIVATE_KEY=\"{priv_key}\"\n")
        load_dotenv(ENV_FILE, override=True)
        self.api_key, self.priv_key_raw = key_id, priv_key
        return True

    async def log_event(self, msg, strategy="sys"):
        t = datetime.now().strftime("%H:%M:%S")
        for ws in self.conns:
            try: await ws.send_json({"msg": msg, "type": strategy, "time": t})
            except: self.conns.remove(ws)

engine = SentinelV10_2()

@app.get("/api/status")
async def get_status():
    return {"balance_paper": engine.balance_paper, "vault_loaded": bool(engine.api_key), "positions": engine.positions}

@app.post("/save_secrets")
async def save_secrets(api_key: str = Form(...), priv_key: str = Form(...)):
    engine.save_secrets(api_key, priv_key)
    await engine.log_event("VAULT: Secrets Locked.", "system")
    return {"status": "success"}

@app.get("/")
async def index(request: Request): return templates.TemplateResponse("index.html", {"request": request})

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); engine.conns.append(websocket)
    try:
        while True: await websocket.receive_text()
    except: engine.conns.remove(websocket)
