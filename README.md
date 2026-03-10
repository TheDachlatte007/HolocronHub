# Holocron (MVP v1.1)

Local single-user dashboard for curated AI tools, home network services, and a unified feed.

## Features (v1.1)
- Tool catalog with search + category filter
- Clickable tool links
- Quick Add Tool panel (seed editor)
- Source management + ingest scheduling
- Full Settings page (General, Feed, Ingest, API keys)
- Feed + Morning Digest + Saved items
- JSON-based local data store
- FastAPI backend API

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

You can copy `backend/.env.example` to `backend/.env` for local development.

You can also manage API keys directly in the app under `Settings` -> `API`.

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
- Tools file: `data/tools.json` (auto-created from `tools.sample.json`)
- Sources file: `data/sources.json`
- Feed snapshot: `data/feed_items.json`

## Next (v2)
- n8n integration
- Workflow execution + queue
- Provider adapters

