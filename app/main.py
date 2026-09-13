from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from .config import ALLOWED_SYMBOLS, DEFAULT_SYMBOLS, LIVE_SESSION_SECONDS
from .db import Database
from .domain import Candle, Quote
from .market import MarketDataFeed
from .worker import TradingWorker

ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT / "Index.html"

app = FastAPI(title="Paper Trading Lab", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "PUT", "PATCH"], allow_headers=["*"])

db = Database()
worker = TradingWorker(db)


class BotCreateRequest(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    template: str
    symbol: str
    budget_usdt: float = Field(gt=0)
    version: str = "v1"
    params: dict = Field(default_factory=dict)

    @field_validator("template")
    @classmethod
    def validate_template(cls, v: str) -> str:
        if v not in {"trend", "range", "breakout"}:
            raise ValueError("invalid template")
        return v

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if v not in ALLOWED_SYMBOLS:
            raise ValueError("invalid symbol")
        return ALLOWED_SYMBOLS[v]


class BotActionRequest(BaseModel):
    action: str


class BacktestRequest(BaseModel):
    template: str
    symbol: str
    candles: list[dict]
    params: dict = Field(default_factory=dict)
    budget_usdt: float = 100


class LiveStartRequest(BaseModel):
    mode: str = "shared"
    duration_seconds: int = LIVE_SESSION_SECONDS


feed: MarketDataFeed | None = None


@app.on_event("startup")
async def startup() -> None:
    worker.start()

    async def on_quote(q: Quote):
        worker.on_quote(q)
        await worker.publish({"type": "quote", "symbol": q.symbol, "bid": q.bid, "ask": q.ask, "quote_ts": q.quote_ts, "recv_ts": q.recv_ts})

    async def on_candle(c: Candle):
        worker.on_candle(c)
        await worker.publish({"type": "candle", "symbol": c.symbol, "close": c.close, "close_ts": c.close_ts})

    async def on_info(level: str, message: str, data: dict):
        db.add_event(worker.current_run["id"] if worker.current_run else None, None, None, level, message, data)
        await worker.publish({"type": "feed_info", "level": level, "message": message, "data": data})

    global feed
    feed = MarketDataFeed(list(DEFAULT_SYMBOLS), on_quote, on_candle, on_info)
    asyncio.create_task(feed.start())
    asyncio.create_task(_ticker())


async def _ticker() -> None:
    while True:
        await asyncio.sleep(1)
        worker.tick()
        await worker.publish({"type": "heartbeat", "ts": time.time(), "dashboard": worker.dashboard()})


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(INDEX_PATH)


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True, "backend": "online", "feed": bool(feed and feed.state.connected)}


@app.get("/api/templates")
async def templates() -> dict:
    return {
        "trend": {"version": "v1", "defaults": {"fast": 9, "slow": 21, "risk_per_trade": 5.0}},
        "range": {"version": "v1", "defaults": {"period": 20, "std_mult": 2.0, "risk_per_trade": 5.0}},
        "breakout": {"version": "v1", "defaults": {"lookback": 20, "volume_mult": 1.2, "risk_per_trade": 5.0}},
    }


@app.post("/api/bots")
async def create_bot(payload: BotCreateRequest) -> dict:
    bot = worker.create_bot(payload.model_dump())
    db.add_event(worker.current_run["id"] if worker.current_run else None, bot["bot_id"], None, "info", "Bot erstellt", bot)
    return bot


@app.get("/api/bots")
async def list_bots() -> list[dict]:
    return db.list_bots()


@app.post("/api/bots/{bot_id}/action")
async def bot_action(bot_id: str, payload: BotActionRequest) -> dict:
    if bot_id not in worker.bots:
        raise HTTPException(404, "Bot nicht gefunden")
    action = payload.action
    if action == "start":
        bot = worker.set_bot_status(bot_id, "running")
    elif action == "pause":
        bot = worker.set_bot_status(bot_id, "paused")
    elif action == "stop":
        bot = worker.set_bot_status(bot_id, "stopping")
    else:
        raise HTTPException(400, "Ungültige Aktion")
    return {"bot_id": bot.bot_id, "status": bot.status}


@app.get("/api/bots/{bot_id}")
async def bot_details(bot_id: str) -> dict:
    b = worker.bots.get(bot_id)
    if not b:
        raise HTTPException(404, "Bot nicht gefunden")
    quote = worker.last_quote_by_symbol.get(b.symbol)
    return {
        "bot_id": b.bot_id,
        "name": b.name,
        "template": b.template,
        "version": b.version,
        "status": b.status,
        "warmup": {"complete": b.warmup_complete, "candles": len(b.candles)},
        "signal_reason": b.signal_reason,
        "pending_close_reason": b.pending_close_reason,
        "position": {
            "qty": b.position.qty,
            "entry_price": b.position.entry_price,
            "stop": b.position.stop_price,
            "target": b.position.target_price,
        }
        if b.position
        else None,
        "quote_age_seconds": (time.time() - quote.quote_ts) if quote else None,
    }


@app.get("/api/dashboard")
async def dashboard() -> dict:
    return worker.dashboard()


@app.post("/api/lab/backtest")
async def backtest(payload: BacktestRequest) -> dict:
    if payload.symbol not in ALLOWED_SYMBOLS:
        raise HTTPException(400, "Ungültiges Symbol")
    return worker.backtest({**payload.model_dump(), "symbol": ALLOWED_SYMBOLS[payload.symbol]})


@app.post("/api/lab/compare")
async def compare(payload: dict) -> dict:
    # Begrenzter Parametervergleich
    variants = payload.get("variants", [])[:5]
    candles = payload.get("candles", [])
    symbol = ALLOWED_SYMBOLS.get(payload.get("symbol", "BTC/USDT"), "BTCUSDT")
    results = []
    for v in variants:
        results.append({"name": v.get("name", "var"), "result": worker.backtest({"template": payload.get("template", "trend"), "symbol": symbol, "candles": candles, "params": v.get("params", {})})})
    return {"results": results}


@app.post("/api/lab/live-session")
async def live_session(payload: LiveStartRequest) -> dict:
    if payload.mode not in {"shared", "compare"}:
        raise HTTPException(400, "Ungültiger Modus")
    run = worker.start_live_session(payload.mode, payload.duration_seconds)
    return run


@app.get("/api/export/run/{run_id}.json")
async def export_json(run_id: str) -> JSONResponse:
    return JSONResponse(worker.export_run_json(run_id))


@app.get("/api/export/run/{run_id}.csv")
async def export_csv(run_id: str) -> PlainTextResponse:
    return PlainTextResponse(worker.export_run_csv(run_id), media_type="text/csv")


@app.get("/api/stream")
async def stream() -> StreamingResponse:
    q = worker.subscribe()

    async def gen():
        try:
            yield f"data: {json.dumps({'type': 'hello', 'ts': time.time()})}\n\n"
            while True:
                payload = await q.get()
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            worker.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream")
