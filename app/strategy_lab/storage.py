import json
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "strategy_lab.db"
SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

def get_conn():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    try:
        conn.executescript(SCHEMA_PATH.read_text())
        conn.commit()
    finally:
        conn.close()

def rows_to_dicts(rows):
    return [dict(r) for r in rows]

def json_dumps(obj):
    return json.dumps(obj, separators=(",", ":"), default=str)
