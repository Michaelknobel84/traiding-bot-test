from __future__ import annotations

import asyncio
import csv
import io
import json
import time
import uuid
from dataclasses import asdict

from .config import FEE_RATE, LIVE_SESSION_SECONDS, QUOTE_STALE_SECONDS
from .db import Database
from .domain import BotRuntime, Candle, Position, Quote
from .execution import close_long_fill, open_long_fill, unrealized_net
from .risk import SharedRiskState
from .strategies import build_strategy


class TradingWorker:
    def __init__(self, db: Database):
        self.db = db
        self.db.mark_running_runs_interrupted()
        self.bots: dict[str, BotRuntime] = {}
        self.listeners: list[asyncio.Queue] = []
        self.last_quote_by_symbol: dict[str, Quote] = {}
        self.current_run: dict | None = None
        self.risk_compare: dict[str, float] = {}
        self.shared_risk = SharedRiskState(total_budget=200.0)
        self.started = False
        self._event_ids: set[str] = set()

    async def publish(self, payload: dict) -> None:
        for q in list(self.listeners):
            if q.full():
                continue
            await q.put(payload)

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.listeners.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self.listeners:
            self.listeners.remove(q)

    def start(self) -> bool:
        if self.started:
            return False
        self.started = True
        return True

    def create_bot(self, payload: dict) -> dict:
        bot_id = payload.get("id") or str(uuid.uuid4())
        bot = BotRuntime(
            bot_id=bot_id,
            name=payload["name"],
            template=payload["template"],
            version=payload.get("version", "v1"),
            symbol=payload["symbol"],
            params=payload.get("params", {}),
            budget_usdt=float(payload.get("budget_usdt", 50)),
            status="created",
        )
        self.bots[bot_id] = bot
        self.db.save_bot({
            "id": bot.bot_id,
            "name": bot.name,
            "template": bot.template,
            "symbol": bot.symbol,
            "params": bot.params,
            "budget_usdt": bot.budget_usdt,
            "version": bot.version,
            "status": bot.status,
            "created_ts": time.time(),
        })
        return asdict(bot)

    def set_bot_status(self, bot_id: str, status: str) -> BotRuntime:
        bot = self.bots[bot_id]
        bot.status = status
        self.db.update_bot_status(bot_id, status)
        return bot

    def start_live_session(self, mode: str = "shared", duration_seconds: int = LIVE_SESSION_SECONDS) -> dict:
        if self.current_run and self.current_run.get("status") == "running":
            raise ValueError("Eine Session läuft bereits")
        now = time.time()
        total_budget = sum(b.budget_usdt for b in self.bots.values()) or 200.0
        self.shared_risk = SharedRiskState(total_budget=total_budget)
        self._event_ids.clear()
        run = {
            "id": str(uuid.uuid4()),
            "mode": mode,
            "status": "running",
            "start_ts": now,
            "end_ts": None,
            "deadline_ts": now + duration_seconds,
            "impaired": False,
            "notes": "",
        }
        self.current_run = run
        self.db.create_run(run)
        for bot in self.bots.values():
            bot.position = None
            bot.pending_close_reason = None
            bot.signal_reason = "Session gestartet"
            bot.trade_count = 0
            bot.warmup_complete = False
            bot.candles = []
            if bot.status in {"created", "paused"}:
                bot.status = "running"
                self.db.update_bot_status(bot.bot_id, "running")
        return run

    def on_quote(self, quote: Quote) -> None:
        self.last_quote_by_symbol[quote.symbol] = quote
        self._process_quote_for_positions(quote)
        self._refresh_estimated_open_pnl()

    def on_candle(self, candle: Candle) -> None:
        for bot in self.bots.values():
            if bot.symbol != candle.symbol:
                continue
            bot.candles.append(candle)
            if len(bot.candles) > 2000:
                bot.candles = bot.candles[-2000:]
            strat = build_strategy(bot.template, bot.params)
            bot.warmup_complete = len(bot.candles) >= strat.min_history
            if bot.status != "running":
                continue
            if not bot.warmup_complete:
                bot.signal_reason = f"Warm-up: {len(bot.candles)}/{strat.min_history}"
                continue
            signal = strat.signal(bot.candles)
            bot.signal_reason = signal.reason
            if signal.action == "open_long":
                self._attempt_open(bot, signal)
            elif signal.action == "close_long":
                self._attempt_close(bot, reason=signal.reason)

    def _run_is_expired(self) -> bool:
        return bool(self.current_run and time.time() >= self.current_run["deadline_ts"])

    def _quote_is_stale(self, q: Quote | None) -> bool:
        return q is None or (time.time() - q.quote_ts) > QUOTE_STALE_SECONDS

    def _attempt_open(self, bot: BotRuntime, signal) -> None:
        if bot.position or self._run_is_expired() or bot.status != "running":
            return
        q = self.last_quote_by_symbol.get(bot.symbol)
        if self._quote_is_stale(q):
            bot.signal_reason = "Einstieg blockiert: Quote veraltet"
            self._mark_impaired("Veraltete Quote blockiert Einstieg")
            return
        assert q
        risk_per_trade = float(bot.params.get("risk_per_trade", 5.0))
        stop_price = float(signal.stop_ref or (q.bid * 0.99))
        distance = max(0.000001, q.ask - stop_price)
        qty = max(0.0, risk_per_trade / distance)
        fill = open_long_fill(q, qty)
        notional = fill.price * qty
        fee_est = fill.fee
        candle_ts = int(bot.candles[-1].close_ts) if bot.candles else int(time.time())
        event_id = f"{bot.bot_id}:{candle_ts}:open"
        if event_id in self._event_ids:
            return
        if notional + fee_est > bot.budget_usdt:
            bot.signal_reason = "Einstieg abgelehnt: Bot-Budget überschritten"
            return
        self._refresh_estimated_open_pnl()
        shared_reserved = bool(self.current_run and self.current_run["mode"] == "shared")
        if shared_reserved:
            d = self.shared_risk.can_open(bot.symbol, notional, fee_est)
            if not d.allowed:
                bot.signal_reason = f"Einstieg abgelehnt: {d.reason}"
                self.db.add_event(self.current_run["id"], bot.bot_id, None, "warn", bot.signal_reason, {})
                return
            self.shared_risk.reserve(bot.symbol, notional, fee_est)
        self._event_ids.add(event_id)
        bot.position = Position(
            symbol=bot.symbol,
            qty=qty,
            entry_price=fill.price,
            stop_price=stop_price,
            target_price=float(signal.target_ref or (q.ask * 1.01)),
            entry_ts=time.time(),
            event_id=event_id,
            reserved_usdt=notional + fee_est,
            run_id=self.current_run["id"] if self.current_run else "none",
            reserved_shared=shared_reserved,
        )
        self.db.add_trade(
            {
                "id": str(uuid.uuid4()),
                "run_id": bot.position.run_id,
                "bot_id": bot.bot_id,
                "event_id": event_id,
                "symbol": bot.symbol,
                "qty": qty,
                "entry_price": fill.price,
                "entry_ts": time.time(),
                "fees": fill.fee,
                "status": "open",
                "reason_open": signal.reason,
            }
        )

    def _attempt_close(self, bot: BotRuntime, reason: str) -> None:
        if not bot.position:
            return
        q = self.last_quote_by_symbol.get(bot.symbol)
        if self._quote_is_stale(q):
            bot.pending_close_reason = reason
            bot.signal_reason = "Zeit abgelaufen – Schliessung ausstehend" if self._run_is_expired() else "Schliessung ausstehend: Quote veraltet"
            self._mark_impaired("Schliessung verzögert wegen veralteter Quote")
            return
        assert q
        fill = close_long_fill(q, bot.position.qty)
        entry_fee = 0.0
        trade = self.db.get_trade(bot.position.run_id, bot.bot_id, bot.position.event_id)
        if trade:
            entry_fee = float(trade["fees"])
        gross = (fill.price - bot.position.entry_price) * bot.position.qty
        net_pnl = gross - entry_fee - fill.fee
        notional = bot.position.entry_price * bot.position.qty
        if bot.position.reserved_shared:
            self.shared_risk.release(bot.symbol, notional, bot.position.reserved_usdt, net_pnl)
        if trade:
            self.db.add_trade({
                **trade,
                "exit_price": fill.price,
                "exit_ts": time.time(),
                "fees": entry_fee + fill.fee,
                "pnl_net": net_pnl,
                "status": "closed",
                "reason_close": reason,
            })
        bot.position = None
        bot.pending_close_reason = None
        bot.trade_count += 1

    def _process_quote_for_positions(self, quote: Quote) -> None:
        for bot in self.bots.values():
            if not bot.position or bot.symbol != quote.symbol:
                continue
            if quote.bid <= bot.position.stop_price:
                self._attempt_close(bot, "Stop ausgelöst")
            elif quote.bid >= bot.position.target_price:
                self._attempt_close(bot, "Take-Profit ausgelöst")
            elif self._run_is_expired():
                self._attempt_close(bot, "Zeitlimit erreicht")
            elif bot.status == "stopping":
                self._attempt_close(bot, "Manuell gestoppt")

    def _mark_impaired(self, message: str) -> None:
        if self.current_run:
            self.current_run["impaired"] = True
            notes = (self.current_run.get("notes") or "")
            if message not in notes:
                self.current_run["notes"] = (notes + " | " + message).strip(" |")
                self.db.update_run(self.current_run["id"], impaired=1, notes=self.current_run["notes"])

    def tick(self) -> None:
        if not self.current_run:
            return
        if self._run_is_expired():
            for bot in self.bots.values():
                if bot.position:
                    self._attempt_close(bot, "Zeitlimit erreicht")
            for bot in self.bots.values():
                if bot.status == "running" and not bot.position:
                    bot.status = "stopped"
                    self.db.update_bot_status(bot.bot_id, "stopped")
            if all((b.position is None) for b in self.bots.values()):
                self.current_run["status"] = "finished"
                self.current_run["end_ts"] = time.time()
                self.db.update_run(self.current_run["id"], status="finished", end_ts=self.current_run["end_ts"], impaired=int(self.current_run["impaired"]), notes=self.current_run["notes"])

    def _refresh_estimated_open_pnl(self) -> None:
        est_open = 0.0
        for bot in self.bots.values():
            q = self.last_quote_by_symbol.get(bot.symbol)
            if bot.position and q and not self._quote_is_stale(q):
                est_open += unrealized_net(bot.position, q)
        self.shared_risk.estimated_open_pnl = est_open

    def dashboard(self) -> dict:
        open_positions = 0
        self._refresh_estimated_open_pnl()
        for bot in self.bots.values():
            q = self.last_quote_by_symbol.get(bot.symbol)
            if bot.position:
                open_positions += 1
        return {
            "backend": "online",
            "feed_connected": any(self.last_quote_by_symbol.values()),
            "quote_age_seconds": max((time.time() - q.quote_ts for q in self.last_quote_by_symbol.values()), default=None),
            "bots_active": sum(1 for b in self.bots.values() if b.status == "running"),
            "open_positions": open_positions,
            "session_risk": {
                "reserved": self.shared_risk.reserved,
                "realized_pnl": self.shared_risk.realized_pnl,
                "estimated_open_pnl": self.shared_risk.estimated_open_pnl,
                "loss_limit": self.shared_risk.session_loss_limit,
            },
            "events": self.db.latest_events(12),
        }

    def export_run_json(self, run_id: str) -> dict:
        run = self.db.get_run(run_id)
        if not run:
            return {"error": "run_not_found"}
        trades = self.db.list_trades_for_run(run_id)
        return {"run": run, "trades": trades}

    def export_run_csv(self, run_id: str) -> str | None:
        if not self.db.get_run(run_id):
            return None
        trades = self.db.list_trades_for_run(run_id)
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=["id", "run_id", "bot_id", "symbol", "qty", "entry_price", "exit_price", "fees", "pnl_net", "status", "reason_open", "reason_close"])
        writer.writeheader()
        for t in trades:
            writer.writerow({k: t.get(k) for k in writer.fieldnames})
        return out.getvalue()

    def backtest(self, payload: dict) -> dict:
        candles = [Candle(**c) for c in payload["candles"]]
        bot = BotRuntime(
            bot_id="backtest",
            name="backtest",
            template=payload["template"],
            version=payload.get("version", "v1"),
            symbol=payload["symbol"],
            params=payload.get("params", {}),
            budget_usdt=float(payload.get("budget_usdt", 100)),
            status="running",
        )
        strat = build_strategy(bot.template, bot.params)
        simulated_quote = None
        trades = 0
        pending_signal = None
        for c in candles:
            bot.candles.append(c)
            simulated_quote = Quote(c.symbol, c.close * 0.999, c.close * 1.001, c.close_ts, c.close_ts)
            if pending_signal:
                action, pending = pending_signal
                if action == "open_long" and not bot.position:
                    bot.position = Position(
                        c.symbol,
                        1,
                        simulated_quote.ask,
                        pending.stop_ref or c.close * 0.99,
                        pending.target_ref or c.close * 1.01,
                        c.close_ts,
                        f"bt:{c.close_ts}",
                        simulated_quote.ask,
                        "backtest",
                        False,
                    )
                elif action == "close_long" and bot.position:
                    bot.position = None
                    trades += 1
                pending_signal = None
            if len(bot.candles) < strat.min_history:
                continue
            sig = strat.signal(bot.candles)
            if sig.action in {"open_long", "close_long"}:
                pending_signal = (sig.action, sig)
            elif bot.position:
                if c.low <= bot.position.stop_price:
                    bot.position = None
                    trades += 1
                elif c.high >= bot.position.target_price:
                    bot.position = None
                    trades += 1
        if bot.position and simulated_quote:
            bot.position = None
            trades += 1
        return {
            "trades": trades,
            "warmup_bars": strat.min_history,
            "buy_and_hold_return_pct": ((candles[-1].close - candles[strat.min_history].close) / candles[strat.min_history].close) * 100 if len(candles) > strat.min_history else 0,
            "notes": "Backtest nutzt dokumentierte Spread-Annahme 0.2% ohne historische Bid/Ask.",
        }
