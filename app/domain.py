from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: float
    ask: float
    quote_ts: float
    recv_ts: float


@dataclass(frozen=True)
class Candle:
    symbol: str
    close_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    closed: bool = True


@dataclass(frozen=True)
class Signal:
    action: Literal["open_long", "close_long", "hold"]
    reason: str
    stop_ref: float | None = None
    target_ref: float | None = None


@dataclass
class Position:
    symbol: str
    qty: float
    entry_price: float
    stop_price: float
    target_price: float
    entry_ts: float
    event_id: str
    reserved_usdt: float


@dataclass
class BotRuntime:
    bot_id: str
    name: str
    template: str
    version: str
    symbol: str
    params: dict
    budget_usdt: float
    status: Literal["created", "running", "paused", "stopping", "stopped"] = "created"
    candles: list[Candle] = field(default_factory=list)
    position: Position | None = None
    warmup_complete: bool = False
    signal_reason: str = "Warte auf Warm-up"
    pending_close_reason: str | None = None
    trade_count: int = 0
