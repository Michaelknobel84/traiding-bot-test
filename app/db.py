from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from .config import DB_PATH


class Database:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init()

    def init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bots (
              id TEXT PRIMARY KEY,
              name TEXT NOT NULL,
              template TEXT NOT NULL,
              symbol TEXT NOT NULL,
              params_json TEXT NOT NULL,
              budget_usdt REAL NOT NULL,
              version TEXT NOT NULL,
              status TEXT NOT NULL,
              created_ts REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
              id TEXT PRIMARY KEY,
              mode TEXT NOT NULL,
              status TEXT NOT NULL,
              start_ts REAL NOT NULL,
              end_ts REAL,
              deadline_ts REAL,
              impaired INTEGER NOT NULL,
              notes TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trades (
              id TEXT PRIMARY KEY,
              run_id TEXT NOT NULL,
              bot_id TEXT NOT NULL,
              event_id TEXT NOT NULL,
              symbol TEXT NOT NULL,
              qty REAL NOT NULL,
              entry_price REAL NOT NULL,
              exit_price REAL,
              entry_ts REAL NOT NULL,
              exit_ts REAL,
              fees REAL NOT NULL,
              pnl_net REAL,
              status TEXT NOT NULL,
              reason_open TEXT NOT NULL,
              reason_close TEXT
            );
            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              run_id TEXT,
              bot_id TEXT,
              event_id TEXT,
              ts REAL NOT NULL,
              level TEXT NOT NULL,
              message TEXT NOT NULL,
              data_json TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def mark_running_runs_interrupted(self) -> int:
        cur = self.conn.execute("UPDATE runs SET status='interrupted', impaired=1, notes='Server neu gestartet, Lauf unterbrochen' WHERE status='running'")
        self.conn.commit()
        return cur.rowcount

    def save_bot(self, bot: dict) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO bots(id,name,template,symbol,params_json,budget_usdt,version,status,created_ts) VALUES(?,?,?,?,?,?,?,?,?)",
            (bot["id"], bot["name"], bot["template"], bot["symbol"], json.dumps(bot["params"]), bot["budget_usdt"], bot["version"], bot["status"], bot["created_ts"]),
        )
        self.conn.commit()

    def list_bots(self) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM bots ORDER BY created_ts DESC").fetchall()
        return [
            {
                **dict(r),
                "params": json.loads(r["params_json"]),
            }
            for r in rows
        ]

    def update_bot_status(self, bot_id: str, status: str) -> None:
        self.conn.execute("UPDATE bots SET status=? WHERE id=?", (status, bot_id))
        self.conn.commit()

    def add_event(self, run_id: str | None, bot_id: str | None, event_id: str | None, level: str, message: str, data: dict | None = None) -> None:
        self.conn.execute(
            "INSERT INTO events(run_id,bot_id,event_id,ts,level,message,data_json) VALUES(?,?,?,?,?,?,?)",
            (run_id, bot_id, event_id, time.time(), level, message, json.dumps(data or {})),
        )
        self.conn.commit()

    def latest_events(self, limit: int = 25) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def create_run(self, run: dict) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO runs(id,mode,status,start_ts,end_ts,deadline_ts,impaired,notes) VALUES(?,?,?,?,?,?,?,?)",
            (run["id"], run["mode"], run["status"], run["start_ts"], run["end_ts"], run["deadline_ts"], int(run["impaired"]), run["notes"]),
        )
        self.conn.commit()

    def update_run(self, run_id: str, **updates) -> None:
        keys = list(updates.keys())
        if not keys:
            return
        sql = "UPDATE runs SET " + ",".join(f"{k}=?" for k in keys) + " WHERE id=?"
        self.conn.execute(sql, tuple(updates[k] for k in keys) + (run_id,))
        self.conn.commit()

    def add_trade(self, trade: dict) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO trades(id,run_id,bot_id,event_id,symbol,qty,entry_price,exit_price,entry_ts,exit_ts,fees,pnl_net,status,reason_open,reason_close) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                trade["id"],
                trade["run_id"],
                trade["bot_id"],
                trade["event_id"],
                trade["symbol"],
                trade["qty"],
                trade["entry_price"],
                trade.get("exit_price"),
                trade["entry_ts"],
                trade.get("exit_ts"),
                trade["fees"],
                trade.get("pnl_net"),
                trade["status"],
                trade["reason_open"],
                trade.get("reason_close"),
            ),
        )
        self.conn.commit()

    def list_trades_for_run(self, run_id: str) -> list[dict]:
        rows = self.conn.execute("SELECT * FROM trades WHERE run_id=? ORDER BY entry_ts", (run_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_run(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None
