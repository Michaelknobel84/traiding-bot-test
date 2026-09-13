from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
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
allowed_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "http://127.0.0.1:8000,http://localhost:8000").split(",") if o.strip()]
app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_methods=["GET", "POST", "PUT", "PATCH", "OPTIONS"], allow_headers=["*"])

db = Database()
worker = TradingWorker(db)
API_TOKEN = os.getenv("API_TOKEN")


def require_api_key(request: Request) -> None:
    if not API_TOKEN:
        host = (request.client.host if request.client else "") or ""
        if host in {"127.0.0.1", "::1", "localhost", "testclient"}:
            return
        raise HTTPException(401, "Unauthorized")
    else:
        provided = request.headers.get("x-api-key")
        if not provided or not secrets.compare_digest(provided, API_TOKEN):
            raise HTTPException(401, "Unauthorized")


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


class CompareVariant(BaseModel):
    name: str
    params: dict = Field(default_factory=dict)


class CompareRequest(BaseModel):
    template: str
    symbol: str
    candles: list[dict]
    variants: list[CompareVariant] = Field(default_factory=list)


class LiveStartRequest(BaseModel):
    mode: str = "shared"
    duration_seconds: int = Field(default=LIVE_SESSION_SECONDS, gt=0)


feed: MarketDataFeed | None = None
ticker_task: asyncio.Task | None = None
feed_task: asyncio.Task | None = None


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
    global ticker_task
    global feed_task
    feed = MarketDataFeed(list(DEFAULT_SYMBOLS), on_quote, on_candle, on_info)
    feed_task = asyncio.create_task(feed.start())
    ticker_task = asyncio.create_task(_ticker())


@app.on_event("shutdown")
async def shutdown() -> None:
    global ticker_task
    global feed_task
    if feed:
        await feed.stop()
    if ticker_task:
        ticker_task.cancel()
    if feed_task:
        feed_task.cancel()


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
async def create_bot(payload: BotCreateRequest, request: Request) -> dict:
    require_api_key(request)
    bot = worker.create_bot(payload.model_dump())
    db.add_event(worker.current_run["id"] if worker.current_run else None, bot["bot_id"], None, "info", "Bot erstellt", bot)
    return bot


@app.get("/api/bots")
async def list_bots() -> list[dict]:
    return db.list_bots()


@app.post("/api/bots/{bot_id}/action")
async def bot_action(bot_id: str, payload: BotActionRequest, request: Request) -> dict:
    require_api_key(request)
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
async def backtest(payload: BacktestRequest, request: Request) -> dict:
    require_api_key(request)
    if payload.symbol not in ALLOWED_SYMBOLS:
        raise HTTPException(400, "Ungültiges Symbol")
    return worker.backtest({**payload.model_dump(), "symbol": ALLOWED_SYMBOLS[payload.symbol]})


@app.post("/api/lab/compare")
async def compare(payload: CompareRequest, request: Request) -> dict:
    require_api_key(request)
    # Begrenzter Parametervergleich
    if payload.template not in {"trend", "range", "breakout"}:
        raise HTTPException(400, "Ungültiges Template")
    variants = payload.variants[:5]
    candles = payload.candles
    symbol_key = payload.symbol
    if symbol_key not in ALLOWED_SYMBOLS:
        raise HTTPException(400, "Ungültiges Symbol")
    symbol = ALLOWED_SYMBOLS[symbol_key]
    results = []
    for v in variants:
        results.append({"name": v.name, "result": worker.backtest({"template": payload.template, "symbol": symbol, "candles": candles, "params": v.params})})
    return {"results": results}


@app.post("/api/lab/live-session")
async def live_session(payload: LiveStartRequest, request: Request) -> dict:
    require_api_key(request)
    if payload.mode != "shared":
        raise HTTPException(400, "Nur shared-Modus in Sprint 1 unterstützt")
    try:
        run = worker.start_live_session(payload.mode, payload.duration_seconds)
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
    return run


@app.get("/api/export/run/{run_id}.json")
async def export_json(run_id: str, request: Request) -> JSONResponse:
    require_api_key(request)
    payload = worker.export_run_json(run_id)
    if payload.get("error") == "run_not_found":
        raise HTTPException(404, "Run nicht gefunden")
    return JSONResponse(payload)


@app.get("/api/export/run/{run_id}.csv")
async def export_csv(run_id: str, request: Request) -> PlainTextResponse:
    require_api_key(request)
    data = worker.export_run_csv(run_id)
    if data is None:
        raise HTTPException(404, "Run nicht gefunden")
    return PlainTextResponse(data, media_type="text/csv")


@app.get("/api/stream")
async def stream(request: Request) -> StreamingResponse:
    q = worker.subscribe()

    async def gen():
        try:
            yield f"data: {json.dumps({'type': 'hello', 'ts': time.time()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=1)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            worker.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream")
