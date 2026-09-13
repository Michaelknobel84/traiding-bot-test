from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass

import websockets

from .config import MAX_BUFFER, QUOTE_STALE_SECONDS
from .domain import Candle, Quote


@dataclass
class FeedState:
    connected: bool = False
    reconnect_attempt: int = 0
    generation: int = 0
    last_quote: Quote | None = None
    last_candle: Candle | None = None
    last_trade_recv_ts: float | None = None
    last_msg_recv_ts: float | None = None


class MarketDataFeed:
    def __init__(self, symbols: list[str], on_quote, on_candle, on_info):
        self.symbols = symbols
        self.on_quote = on_quote
        self.on_candle = on_candle
        self.on_info = on_info
        self.state = FeedState()
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self.quote_seen: set[tuple[str, float, float, float]] = set()
        self.quote_order: deque[tuple[str, float, float, float]] = deque()
        self.candle_buffer: list[Candle] = []

    @property
    def quote_age_seconds(self) -> float | None:
        if not self.state.last_quote:
            return None
        return max(0.0, time.time() - self.state.last_quote.quote_ts)

    def is_quote_stale(self) -> bool:
        age = self.quote_age_seconds
        return age is None or age > QUOTE_STALE_SECONDS

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            await asyncio.wait([self._task], timeout=2)

    async def _run(self) -> None:
        base = 1.0
        while not self._stop.is_set():
            self.state.generation += 1
            generation = self.state.generation
            self.state.reconnect_attempt += 1
            try:
                stream = "/".join([f"{s.lower()}@bookTicker" for s in self.symbols] + [f"{s.lower()}@kline_1m" for s in self.symbols] + [f"{s.lower()}@trade" for s in self.symbols])
                url = f"wss://stream.binance.com:9443/stream?streams={stream}"
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.state.connected = True
                    self.state.reconnect_attempt = 0
                    await self.on_info("info", "Feed verbunden", {"url": url})
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        self.state.last_msg_recv_ts = time.time()
                        await self._handle_message(raw, generation)
            except Exception as exc:  # pragma: no cover - network errors in tests
                if self._stop.is_set():
                    break
                self.state.connected = False
                await self.on_info("warn", "Feed reconnect", {"error": str(exc), "attempt": self.state.reconnect_attempt})
                delay = min(30.0, base * (2 ** min(5, self.state.reconnect_attempt - 1)))
                await asyncio.sleep(delay)

    async def _handle_message(self, raw: str, generation: int) -> None:
        if generation != self.state.generation:
            return
        payload = json.loads(raw)
        data = payload.get("data") or {}
        event = data.get("e")
        symbol = data.get("s")
        recv_ts = time.time()
        if event == "bookTicker":
            bid = float(data.get("b", 0))
            ask = float(data.get("a", 0))
            quote_ts = float(data.get("E", 0)) / 1000
            if symbol not in self.symbols or bid <= 0 or ask <= 0 or ask < bid:
                return
            key = (symbol, quote_ts, bid, ask)
            if key in self.quote_seen:
                return
            self.quote_seen.add(key)
            self.quote_order.append(key)
            if len(self.quote_order) > MAX_BUFFER:
                old = self.quote_order.popleft()
                self.quote_seen.discard(old)
            q = Quote(symbol=symbol, bid=bid, ask=ask, quote_ts=quote_ts, recv_ts=recv_ts)
            self.state.last_quote = q
            await self.on_quote(q)
        elif event == "kline":
            k = data.get("k", {})
            if not k.get("x"):
                return
            c = Candle(
                symbol=symbol,
                close_ts=float(k.get("T", 0)) / 1000,
                open=float(k.get("o", 0)),
                high=float(k.get("h", 0)),
                low=float(k.get("l", 0)),
                close=float(k.get("c", 0)),
                volume=float(k.get("v", 0)),
                closed=True,
            )
            self.state.last_candle = c
            self.candle_buffer.append(c)
            if len(self.candle_buffer) > MAX_BUFFER:
                self.candle_buffer = self.candle_buffer[-MAX_BUFFER:]
            await self.on_candle(c)
        elif event == "trade":
            self.state.last_trade_recv_ts = recv_ts
            # Trade events dürfen Quote-Alter nicht zurücksetzen.
