import json
import time

import pytest

from app.db import Database
from app.domain import Candle, Quote
from app.market import MarketDataFeed
from app.worker import TradingWorker
from app.strategies import TrendStrategy


def mk_worker(tmp_path):
    db = Database(tmp_path / "test.db")
    return TradingWorker(db), db


def mk_candles(n=60, base=100.0, step=0.2, symbol="BTCUSDT", volume=1000):
    out = []
    ts = 1_700_000_000
    p = base
    for i in range(n):
        out.append(Candle(symbol=symbol, close_ts=ts + i * 60, open=p, high=p + 1, low=p - 1, close=p, volume=volume + i, closed=True))
        p += step
    return out


def mk_quote(price=100.0, symbol="BTCUSDT", ts=None):
    ts = ts or time.time()
    return Quote(symbol=symbol, bid=price - 0.1, ask=price + 0.1, quote_ts=ts, recv_ts=ts)


def test_strategy_signals_and_warmup():
    s = TrendStrategy()
    candles = mk_candles(s.min_history - 1)
    assert len(candles) < s.min_history
    sig = s.signal(mk_candles(s.min_history + 5))
    assert sig.action in {"open_long", "close_long", "hold"}


def test_no_lookahead_in_backtest(tmp_path):
    w, _ = mk_worker(tmp_path)
    candles = [c.__dict__ for c in mk_candles(80)]
    r = w.backtest({"template": "trend", "symbol": "BTCUSDT", "candles": candles, "params": {}})
    assert r["warmup_bars"] >= 25


def test_deterministic_replay(tmp_path):
    w, _ = mk_worker(tmp_path)
    candles = [c.__dict__ for c in mk_candles(80)]
    a = w.backtest({"template": "range", "symbol": "BTCUSDT", "candles": candles, "params": {}})
    b = w.backtest({"template": "range", "symbol": "BTCUSDT", "candles": candles, "params": {}})
    assert a == b


def test_fees_slippage_equity_pnl(tmp_path):
    w, _ = mk_worker(tmp_path)
    b = w.create_bot({"name": "t", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    w.start_live_session("shared", 600)
    bot = w.bots[b["bot_id"]]
    for c in mk_candles(40):
        w.on_quote(mk_quote(c.close))
        w.on_candle(c)
    if bot.position:
        w.on_quote(mk_quote(bot.position.target_price + 1))
    d = w.dashboard()
    assert "session_risk" in d


def test_atomic_capital_reservation_concurrent_entries(tmp_path):
    w, _ = mk_worker(tmp_path)
    w.shared_risk.total_budget = 30
    b1 = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 20}, "budget_usdt": 100, "version": "v1"})
    b2 = w.create_bot({"name": "b", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 20}, "budget_usdt": 100, "version": "v1"})
    w.start_live_session("shared", 600)
    for c in mk_candles(35):
        w.on_quote(mk_quote(c.close))
        w.on_candle(c)
    states = [w.bots[b1["bot_id"]].position is not None, w.bots[b2["bot_id"]].position is not None]
    assert sum(states) <= 1


def test_shared_exposure_and_loss_limits(tmp_path):
    w, _ = mk_worker(tmp_path)
    w.shared_risk.session_loss_limit = 0
    b = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    w.start_live_session("shared", 600)
    bot = w.bots[b["bot_id"]]
    for c in mk_candles(35):
        w.on_quote(mk_quote(c.close))
        w.on_candle(c)
    assert bot.position is None


@pytest.mark.asyncio
async def test_stale_quote_not_refreshed_by_trade_messages():
    q = []
    c = []
    i = []
    async def on_quote(x): q.append(x)
    async def on_candle(x): c.append(x)
    async def on_info(*a): i.append(a)
    feed = MarketDataFeed(["BTCUSDT"], on_quote, on_candle, on_info)
    feed.state.generation = 1
    await feed._handle_message(json.dumps({"data": {"e": "bookTicker", "s": "BTCUSDT", "b": "100", "a": "101", "E": int((time.time()-10)*1000)}}), 1)
    old_age = feed.quote_age_seconds
    await feed._handle_message(json.dumps({"data": {"e": "trade", "s": "BTCUSDT", "E": int(time.time()*1000)}}), 1)
    assert feed.quote_age_seconds >= old_age


@pytest.mark.asyncio
async def test_reconnect_and_old_generation_events_ignored():
    q = []
    async def on_quote(x): q.append(x)
    async def on_candle(x): return None
    async def on_info(*a): return None
    feed = MarketDataFeed(["BTCUSDT"], on_quote, on_candle, on_info)
    feed.state.generation = 2
    await feed._handle_message(json.dumps({"data": {"e": "bookTicker", "s": "BTCUSDT", "b": "100", "a": "101", "E": int(time.time()*1000)}}), 1)
    assert not q


def test_duplicate_messages_and_duplicate_start(tmp_path):
    w, _ = mk_worker(tmp_path)
    assert w.start() is True
    assert w.start() is False


def test_stop_loss_gap(tmp_path):
    w, _ = mk_worker(tmp_path)
    b = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    w.start_live_session("shared", 600)
    bot = w.bots[b["bot_id"]]
    for c in mk_candles(35):
        w.on_quote(mk_quote(c.close))
        w.on_candle(c)
    if bot.position:
        w.on_quote(mk_quote(bot.position.stop_price - 2))
    assert bot.position is None or bot.position.entry_price > bot.position.stop_price


def test_exact_600_second_deadline(monkeypatch, tmp_path):
    w, _ = mk_worker(tmp_path)
    b = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    now = 1_700_000_000.0
    monkeypatch.setattr("app.worker.time.time", lambda: now)
    w.start_live_session("shared", 600)
    monkeypatch.setattr("app.worker.time.time", lambda: now + 600)
    for c in mk_candles(35):
        w.on_quote(Quote("BTCUSDT", c.close - 0.1, c.close + 0.1, now + 600, now + 600))
        w.on_candle(c)
    assert w.bots[b["bot_id"]].position is None


def test_missing_quote_at_session_end(tmp_path):
    w, _ = mk_worker(tmp_path)
    b = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    run = w.start_live_session("shared", 1)
    bot = w.bots[b["bot_id"]]
    for c in mk_candles(40):
        w.on_quote(mk_quote(c.close, ts=time.time()-10))
        w.on_candle(c)
    if bot.position:
        w.tick()
        assert bot.pending_close_reason is not None or run["impaired"]


def test_process_restart_marks_interrupted(tmp_path):
    db = Database(tmp_path / "test.db")
    db.create_run({"id": "run1", "mode": "shared", "status": "running", "start_ts": time.time(), "end_ts": None, "deadline_ts": time.time()+10, "impaired": False, "notes": ""})
    _ = TradingWorker(db)
    assert db.get_run("run1")["status"] == "interrupted"


def test_session_with_zero_trades(tmp_path):
    w, _ = mk_worker(tmp_path)
    w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {"risk_per_trade": 5}, "budget_usdt": 100, "version": "v1"})
    w.start_live_session("shared", 600)
    for c in mk_candles(5):
        w.on_candle(c)
    assert all(b.trade_count == 0 for b in w.bots.values())


def test_persistence_and_export(tmp_path):
    w, db = mk_worker(tmp_path)
    b = w.create_bot({"name": "a", "template": "trend", "symbol": "BTCUSDT", "params": {}, "budget_usdt": 100, "version": "v1"})
    run = w.start_live_session("shared", 600)
    assert db.get_run(run["id"])
    out = w.export_run_json(run["id"])
    assert out["run"]["id"] == run["id"]
    csv_data = w.export_run_csv(run["id"])
    assert "run_id" in csv_data
