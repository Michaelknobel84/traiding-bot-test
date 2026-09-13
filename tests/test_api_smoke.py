from fastapi.testclient import TestClient
import pytest
from starlette.requests import Request

import app.main as main_mod
from app.main import app, stream, worker


@pytest.fixture(autouse=True)
def reset_shared_state():
    worker.bots.clear()
    worker.last_quote_by_symbol.clear()
    worker.current_run = None
    worker._event_ids.clear()
    worker.db.conn.execute("DELETE FROM trades")
    worker.db.conn.execute("DELETE FROM runs")
    worker.db.conn.execute("DELETE FROM events")
    worker.db.conn.execute("DELETE FROM bots")
    worker.db.conn.commit()
    yield


def test_ui_api_smoke_flow():
    with TestClient(app) as client:
        r = client.post('/api/bots', json={"name":"Smoke","template":"trend","symbol":"BTC/USDT","budget_usdt":100,"params":{}})
        assert r.status_code == 200
        bot_id = r.json()["bot_id"]
        r = client.post('/api/lab/live-session', json={"mode":"shared","duration_seconds":600})
        assert r.status_code == 200
        run_id = r.json()["id"]
        r2 = client.post('/api/lab/live-session', json={"mode":"shared","duration_seconds":600})
        assert r2.status_code == 409
        assert client.get('/api/dashboard').status_code == 200
        assert client.post(f'/api/bots/{bot_id}/action', json={"action":"pause"}).status_code == 200
        assert client.post(f'/api/bots/{bot_id}/action', json={"action":"stop"}).status_code == 200
        assert client.get(f'/api/bots/{bot_id}').status_code == 200
        assert client.get(f'/api/export/run/{run_id}.json').status_code == 200
        assert client.get('/api/export/run/does-not-exist.json').status_code == 404
        assert client.get('/api/export/run/does-not-exist.csv').status_code == 404
        assert worker.started is True


@pytest.mark.asyncio
async def test_stream_emits_initial_event():
    req = Request({"type": "http", "method": "GET", "path": "/api/stream", "headers": []})
    resp = await stream(req)
    first = await resp.body_iterator.__anext__()
    text = first.decode() if isinstance(first, (bytes, bytearray)) else str(first)
    assert text.startswith("data: ")


def test_api_token_enforcement(monkeypatch):
    monkeypatch.setattr(main_mod, "API_TOKEN", "secret-token")
    with TestClient(app) as client:
        r = client.post('/api/bots', json={"name":"xx","template":"trend","symbol":"BTC/USDT","budget_usdt":100,"params":{}})
        assert r.status_code == 401
        r = client.post('/api/bots', headers={"x-api-key":"secret-token"}, json={"name":"xx","template":"trend","symbol":"BTC/USDT","budget_usdt":100,"params":{}})
        assert r.status_code == 200
