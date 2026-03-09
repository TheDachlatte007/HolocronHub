# AI Personal Hub (MVP v1)

Local single-user dashboard for curated AI tools + simple agent status panel.

## Features (v1)
- Tool catalog with search + category filter
- Clickable tool links
- Quick Add Tool panel (seed editor)
- Simple agent status visualization
- v2-ready workflow hook info (disabled in v1)
- JSON-based tool registry
- FastAPI backend API

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

## Next (v2)
- n8n integration
- Workflow execution + queue
- Provider adapters
