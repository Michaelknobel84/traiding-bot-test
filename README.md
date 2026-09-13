# Serverseitiges Paper-Trading-Labor (Sprint 1)

Dieses Repository enthält eine lokal ausführbare Sprint-1-Implementierung eines **serverseitigen** Paper-Trading-Labors mit drei Bot-Vorlagen, gemeinsamem Risikomodul, SQLite-Persistenz und mobil bedienbarer Weboberfläche.

## Wichtige Grenzen

- Nur **simulierte** Orders (kein Wallet, keine echten Orders, keine API-Keys).
- Sprint-1 Scope: BTC/USDT, ETH/USDT, Long-only, ohne Hebel.
- Eine Serverinstanz pro Lauf; mehrere Webserver-Worker würden zu konkurrierenden Trading-Workern führen.
- Bei fehlendem Backend zeigt die UI klar „Backend nicht verbunden“.

## Architektur (kurz)

- `app/main.py`: FastAPI-API, SSE-Stream, statisches Frontend.
- `app/worker.py`: serverseitiger Bot-/Session-Worker, Risiko, Ausführung, Backtest.
- `app/market.py`: gemeinsamer Binance-WebSocket-Feed (Quotes, 1m-Kerzen, Trades), Reconnect mit Backoff, Generation-Guard gegen alte Verbindungen.
- `app/strategies.py`: Trend-, Seitwärts-, Breakout-Strategie inkl. Warm-up.
- `app/risk.py`: gemeinsame Limits (Gesamtinvest, Asset-Konzentration, Session-Verlust).
- `app/db.py`: SQLite-Persistenz (Bots, Läufe, Trades, Events).

## Daten- und Ausführungsannahmen

- Live-Fills:
  - Kauf: Ask + Slippage.
  - Verkauf: Bid - Slippage.
  - Gebühren auf Ein- und Ausstieg.
- Trade-Nachrichten aktualisieren **nicht** das Quote-Alter.
- Bei veralteten Quotes:
  - keine neuen Einstiege,
  - keine erzwungene Schließung zu alten Preisen,
  - Lauf als beeinträchtigt markieren.
- Backtest nutzt bei fehlenden historischen Bid/Ask eine dokumentierte Spread-Annahme.

## Lokaler Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Dann im Browser öffnen: `http://127.0.0.1:8000/`

## Tests

```bash
pytest -q
```

Die Tests decken Strategie/Warm-up, deterministischen Replay, Look-ahead-Schutz, Gebühren/Slippage/PnL, Limits, Stale Quotes, Reconnect/alte Events, Duplicate-Start, Gap-Stop, 600s-Deadline, fehlende Quote am Ende, Neustart-Unterbrechung, Null-Trades, Persistenz/Export und API/UI-Smoke ab.

## Hosting-Hinweis

GitHub Pages kann nur das statische Frontend hosten. Für Backend + Worker ist ein laufender Python-Server erforderlich.

## Hinweis zu Live-Feed-Dokumentation

Die offizielle Binance-Dokumentationsdomain war in dieser Sandbox nicht auflösbar; Implementierung orientiert sich an den üblichen Spot-WebSocket-Events (`bookTicker`, `kline_1m`, `trade`) und validiert Nachrichten defensiv.
