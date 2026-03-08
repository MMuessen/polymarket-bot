from __future__ import annotations
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "improver" / "state.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS cycles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            status TEXT NOT NULL,
            summary TEXT,
            task_json TEXT,
            planner_json TEXT,
            coder_json TEXT,
            verify_json TEXT,
            promote_json TEXT
        );
        CREATE TABLE IF NOT EXISTS quotas (
            day_key TEXT NOT NULL,
            provider TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(day_key, provider)
        );
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            kind TEXT NOT NULL,
            body TEXT NOT NULL
        );
        """)
        conn.commit()
    finally:
        conn.close()

def day_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def quota_used(provider: str) -> int:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT count FROM quotas WHERE day_key = ? AND provider = ?",
            (day_key(), provider)
        ).fetchone()
        return int(row["count"]) if row else 0
    finally:
        conn.close()

def quota_inc(provider: str, n: int = 1):
    conn = get_conn()
    try:
        conn.execute("""
        INSERT INTO quotas(day_key, provider, count)
        VALUES (?, ?, ?)
        ON CONFLICT(day_key, provider) DO UPDATE SET count = count + excluded.count
        """, (day_key(), provider, n))
        conn.commit()
    finally:
        conn.close()

def add_snapshot(kind: str, body):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO snapshots(ts, kind, body) VALUES (?, ?, ?)",
            (utc_now(), kind, json.dumps(body, indent=2, default=str))
        )
        conn.commit()
    finally:
        conn.close()

def add_cycle(status: str, summary: str = "", **payloads):
    conn = get_conn()
    try:
        conn.execute("""
        INSERT INTO cycles(ts, status, summary, task_json, planner_json, coder_json, verify_json, promote_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            utc_now(),
            status,
            summary,
            json.dumps(payloads.get("task"), indent=2, default=str) if payloads.get("task") is not None else None,
            json.dumps(payloads.get("planner"), indent=2, default=str) if payloads.get("planner") is not None else None,
            json.dumps(payloads.get("coder"), indent=2, default=str) if payloads.get("coder") is not None else None,
            json.dumps(payloads.get("verify"), indent=2, default=str) if payloads.get("verify") is not None else None,
            json.dumps(payloads.get("promote"), indent=2, default=str) if payloads.get("promote") is not None else None,
        ))
        conn.commit()
    finally:
        conn.close()
