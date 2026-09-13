from fastapi.testclient import TestClient

from app.main import app, worker


def test_ui_api_smoke_flow():
    with TestClient(app) as client:
        r = client.post('/api/bots', json={"name":"Smoke","template":"trend","symbol":"BTC/USDT","budget_usdt":100,"params":{}})
        assert r.status_code == 200
        bot_id = r.json()["bot_id"]
        r = client.post('/api/lab/live-session', json={"mode":"shared","duration_seconds":600})
        assert r.status_code == 200
        run_id = r.json()["id"]
        assert client.get('/api/dashboard').status_code == 200
        assert client.post(f'/api/bots/{bot_id}/action', json={"action":"pause"}).status_code == 200
        assert client.post(f'/api/bots/{bot_id}/action', json={"action":"stop"}).status_code == 200
        assert client.get(f'/api/bots/{bot_id}').status_code == 200
        assert client.get(f'/api/export/run/{run_id}.json').status_code == 200
        assert worker.started is True
