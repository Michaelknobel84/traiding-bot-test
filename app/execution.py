from __future__ import annotations

from dataclasses import dataclass

from .config import FEE_RATE, SLIPPAGE_BPS
from .domain import Position, Quote


@dataclass
class FillResult:
    price: float
    fee: float


def open_long_fill(quote: Quote, qty: float, slippage_bps: float = SLIPPAGE_BPS, fee_rate: float = FEE_RATE) -> FillResult:
    fill = quote.ask * (1 + slippage_bps / 10000)
    fee = fill * qty * fee_rate
    return FillResult(fill, fee)


def close_long_fill(quote: Quote, qty: float, slippage_bps: float = SLIPPAGE_BPS, fee_rate: float = FEE_RATE) -> FillResult:
    fill = quote.bid * (1 - slippage_bps / 10000)
    fee = fill * qty * fee_rate
    return FillResult(fill, fee)


def unrealized_net(position: Position, quote: Quote, fee_rate: float = FEE_RATE) -> float:
    gross = (quote.bid - position.entry_price) * position.qty
    est_fee = quote.bid * position.qty * fee_rate
    return gross - est_fee
