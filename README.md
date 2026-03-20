# Holocron (MVP v1.1)

Local single-user dashboard for curated AI tools, home network services, and a unified feed.

## Features (v1.1)
- Tool catalog with search + category filter
- Clickable tool links
- Quick Add Tool panel (seed editor)
- Simple agent status visualization
- v2-ready workflow hook info (disabled in v1)
- JSON-based tool registry
- FastAPI backend API

## Tech Stack
- FastAPI
- Static HTML frontend
- JSON file storage
- APScheduler
- Docker / Docker Compose

## Run

### Backend
```bash
cd backend
python3 -m pip install -r requirements.txt
./run.sh
```
API: `http://localhost:8787/api`

### Frontend (static)
Open:
`frontend/index.html`

If needed, use a static server:
```bash
cd frontend
python3 -m http.server 8788
```
Then open `http://localhost:8788`

## Data
- Tools file: `data/tools.json` (auto-created from `tools.sample.json`)

## Status
Early public version.

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

Collectabase may integrate with or display third-party data, images, or metadata. All respective trademarks, images, and content remain the property of their respective owners.

Users are responsible for ensuring their usage complies with applicable laws and the terms of any third-party services they connect to.
