from __future__ import annotations

from dataclasses import dataclass
from math import sqrt

from .domain import Candle, Signal


def ema(values: list[float], period: int) -> float:
    k = 2 / (period + 1)
    out = values[0]
    for v in values[1:]:
        out = v * k + out * (1 - k)
    return out


class Strategy:
    name: str
    min_history: int

    def signal(self, candles: list[Candle]) -> Signal:
        raise NotImplementedError


@dataclass
class TrendStrategy(Strategy):
    fast: int = 9
    slow: int = 21
    stop_pct: float = 0.007
    take_pct: float = 0.012
    min_history: int = 25
    name: str = "trend"

    def signal(self, candles: list[Candle]) -> Signal:
        closes = [c.close for c in candles]
        fast_ema = ema(closes[-self.slow :], self.fast)
        slow_ema = ema(closes[-self.slow :], self.slow)
        last = closes[-1]
        if fast_ema > slow_ema and last > fast_ema:
            return Signal("open_long", f"Trend long: EMA{self.fast}>{self.slow}", last * (1 - self.stop_pct), last * (1 + self.take_pct))
        if fast_ema < slow_ema:
            return Signal("close_long", f"Trend exit: EMA{self.fast}<EMA{self.slow}")
        return Signal("hold", "Trend neutral")


@dataclass
class RangeStrategy(Strategy):
    period: int = 20
    std_mult: float = 2.0
    trend_filter_pct: float = 0.004
    stop_pct: float = 0.006
    take_pct: float = 0.009
    min_history: int = 30
    name: str = "range"

    def signal(self, candles: list[Candle]) -> Signal:
        sample = candles[-self.period :]
        closes = [c.close for c in sample]
        mean = sum(closes) / len(closes)
        variance = sum((c - mean) ** 2 for c in closes) / len(closes)
        dev = sqrt(variance)
        upper = mean + self.std_mult * dev
        lower = mean - self.std_mult * dev
        fast = ema(closes, 6)
        slow = ema(closes, 20)
        trend_pct = abs(fast - slow) / max(1e-9, closes[-1])
        if trend_pct > self.trend_filter_pct:
            return Signal("hold", "Range filter: Trend zu stark")
        last = closes[-1]
        if last <= lower:
            return Signal("open_long", "Range long: unteres Band", last * (1 - self.stop_pct), last * (1 + self.take_pct))
        if last >= mean:
            return Signal("close_long", "Range exit: Rückkehr zum Mittel")
        return Signal("hold", "Range neutral")


@dataclass
class BreakoutStrategy(Strategy):
    lookback: int = 20
    volume_mult: float = 1.2
    stop_pct: float = 0.008
    take_pct: float = 0.015
    min_history: int = 30
    name: str = "breakout"

    def signal(self, candles: list[Candle]) -> Signal:
        recent = candles[-(self.lookback + 1) :]
        current = recent[-1]
        previous = recent[:-1]
        previous_high = max(c.high for c in previous)
        avg_volume = sum(c.volume for c in previous) / len(previous)
        if current.close > previous_high and current.volume >= avg_volume * self.volume_mult:
            return Signal(
                "open_long",
                "Breakout long: Hoch + Volumen bestätigt",
                current.close * (1 - self.stop_pct),
                current.close * (1 + self.take_pct),
            )
        if current.close < min(c.close for c in previous[-5:]):
            return Signal("close_long", "Breakout exit: Momentum gebrochen")
        return Signal("hold", "Breakout neutral")


def build_strategy(template: str, params: dict) -> Strategy:
    if template == "trend":
        return TrendStrategy(**{k: v for k, v in params.items() if k in TrendStrategy.__dataclass_fields__})
    if template == "range":
        return RangeStrategy(**{k: v for k, v in params.items() if k in RangeStrategy.__dataclass_fields__})
    if template == "breakout":
        return BreakoutStrategy(**{k: v for k, v in params.items() if k in BreakoutStrategy.__dataclass_fields__})
    raise ValueError("Unknown strategy template")
