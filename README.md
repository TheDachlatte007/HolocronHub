# HolocronHub

HolocronHub is a local single-user dashboard for curated AI tools, home lab services, news, markets, F1, Warframe, and TLDR digests.

## Features
- Tool Hub with quick launch, AI task curation, and Home Lab shortcuts
- Feed, Morning Digest, and saved items with local ingest scheduling
- Markets watchlist and overview with local fallback history
- F1 weekend, live timing, and local history views
- Warframe market, world state, planner, and watchlist views
- TLDR Gmail reader with local SQLite-backed issue storage
- Settings for ingest, API keys, Gmail bridge, and local runtime behavior
- FastAPI backend with local JSON/SQLite data storage

## Status
- Work in progress
- Early public version

## Run Local

### Backend + UI (single port)
```bash
cd backend
python3 -m pip install -r requirements.txt
./run.sh
```
Open: `http://localhost:8787`
API: `http://localhost:8787/api`

### Frontend only (optional)
If you open `frontend/index.html` directly, it automatically calls `http://localhost:8787/api`.

## Docker

```bash
docker compose up -d --build
```
Open: `http://localhost:8787`

Optional env vars (for finance API sources):
- `MARKETAUX_API_KEY`
- `FINNHUB_API_KEY`
- `ALPHAVANTAGE_API_KEY`
- `OPENCLAW_MODEL`
- `F1_SIGNALR_SESSION_KEY`
- `F1_SIGNALR_SESSION_NAME`
- `F1_SIGNALR_INGEST_URL`

You can copy `backend/.env.example` to `backend/.env` for local development.

You can also manage API keys directly in the app under `Settings` -> `API`.

## F1 SignalR Sidecar

There is an experimental secondary F1 live ingest worker at `backend/f1_signalr_ingest.py`.
It connects to F1 Live Timing over SignalR and posts a lightweight live snapshot into the existing secondary ingest API.

One-shot probe:

```bash
python backend/f1_signalr_ingest.py --session-key fallback:2026:2:race --session-name Race --probe-only --once
```

Continuous ingest into the local backend:

```bash
python backend/f1_signalr_ingest.py --session-key fallback:2026:2:race --session-name Race
```

The current backend ingest target is `http://127.0.0.1:8787/api/f1/ingest/session`.

## F1 History Store

The backend now includes a local SQLite-backed F1 history store.

- Database file: `data/f1_history.db`
- Search API: `GET /api/f1/history/search`
- Summary API: `GET /api/f1/history/summary`
- Backfill CLI: `backend/f1_history_backfill.py`
- Default scope: the latest two seasons when `--season` is omitted

Example backfill:

```bash
python backend/f1_history_backfill.py --season 2025
```

Backfill the current and previous season:

```bash
python backend/f1_history_backfill.py
```

Quick test backfill:

```bash
python backend/f1_history_backfill.py --season 2025 --limit 10
```

## TLDR Digest

Morning Digest now supports a dedicated TLDR block that stays out of the main feed.

- Sources live in `data/sources.json` as `type: "newsletter"`
- TLDR Tech is fetched from the public newsletter page on `tldr.tech`
- Last good newsletter snapshots are cached in `data/newsletter_cache/`
- TLDR items are shown only inside `Morning Digest`, not the main `Feed`

## TLDR Gmail Reader

HolocronHub can now import TLDR issues directly from Gmail over IMAP and store them locally in SQLite.

- Database file: `data/tldr_issues.db`
- Local IMAP config: `data/tldr_imap_config.json`
- Status API: `GET /api/tldr/status`
- Save config: `POST /api/tldr/config`
- Sync issues: `POST /api/tldr/sync`

Recommended setup:

1. Enable 2-Step Verification on your Google account
2. Create a Google App Password for Mail
3. Open the `TLDR` tab in HolocronHub
4. Enter your Gmail address and the App Password
5. Click `Save Gmail Login`, then `Sync TLDR`

Optional environment variables:

- `TLDR_GMAIL_ADDRESS`
- `TLDR_GMAIL_APP_PASSWORD`
- `TLDR_IMAP_HOST`
- `TLDR_IMAP_PORT`
- `TLDR_IMAP_MAILBOX`

## Portainer (GitHub)

1. In Portainer: `Stacks` -> `Add stack` -> `Repository`.
2. Repo URL: your HolocronHub GitHub repo.
3. Compose path: `docker-compose.yml`.
4. If needed, set env vars in Portainer (`MARKETAUX_API_KEY`, `FINNHUB_API_KEY`, `ALPHAVANTAGE_API_KEY`).
5. Deploy stack.

Notes:
- Data persists in Docker volume `holocron_data`.
- On Docker Standalone, `build: .` works directly from Git repo checkout.
- On Docker Swarm stacks, `build` is typically not supported; use a prebuilt image in that case.

## Data
- Tools file: `data/tools.json` (auto-created from the bundled seed file, with missing defaults synced on load)
- Sources file: `data/sources.json`
- Feed snapshot: `data/feed_items.json`
- F1 history DB: `data/f1_history.db`
- Market history DB: `data/markets_history.db`
- TLDR issue DB: `data/tldr_issues.db`

## Next (v2)
- n8n integration
- Workflow execution + queue
- Provider adapters

---
## Support

If you find this project useful and want to support development:

☕ Ko-fi: https://ko-fi.com/thedachlatte007

Your support helps with development, testing, and maintenance.

---
## Legal / Disclaimer

This project is provided "as is", without warranty of any kind.

HolocronHub may integrate with or display third-party data, images, or metadata. All respective trademarks, images, and content remain the property of their respective owners.

Users are responsible for ensuring their usage complies with applicable laws and the terms of any third-party services they connect to.

