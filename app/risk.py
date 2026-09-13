from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RiskDecision:
    allowed: bool
    reason: str


@dataclass
class SharedRiskState:
    total_budget: float
    max_invested_ratio: float = 0.9
    max_asset_ratio: float = 0.7
    session_loss_limit: float = -40.0
    reserved: float = 0.0
    invested_by_asset: dict[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0
    estimated_open_pnl: float = 0.0

    def can_open(self, symbol: str, notional: float, fee_estimate: float) -> RiskDecision:
        needed = notional + fee_estimate
        if self.reserved + needed > self.total_budget * self.max_invested_ratio:
            return RiskDecision(False, "Gesamt-Investitionslimit erreicht")
        asset_total = self.invested_by_asset.get(symbol, 0.0) + notional
        if asset_total > self.total_budget * self.max_asset_ratio:
            return RiskDecision(False, "Asset-Konzentrationslimit erreicht")
        if self.realized_pnl + self.estimated_open_pnl <= self.session_loss_limit:
            return RiskDecision(False, "Session-Verlustlimit erreicht")
        return RiskDecision(True, "OK")

    def reserve(self, symbol: str, notional: float, fee_estimate: float) -> None:
        self.reserved += notional + fee_estimate
        self.invested_by_asset[symbol] = self.invested_by_asset.get(symbol, 0.0) + notional

    def release(self, symbol: str, notional: float, reserved_amount: float, pnl: float) -> None:
        self.reserved = max(0.0, self.reserved - reserved_amount)
        self.invested_by_asset[symbol] = max(0.0, self.invested_by_asset.get(symbol, 0.0) - notional)
        self.realized_pnl += pnl
