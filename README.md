# HolocronHub

HolocronHub is a local single-user dashboard for curated AI tools, home lab services, news, markets, F1, Warframe, and TLDR digests.

## Features
- Tool Hub with quick launch, AI task curation, and Home Lab shortcuts
- Feed, Morning Digest, and saved items with local ingest scheduling
- Markets watchlist and overview with local fallback history
- F1 weekend, live timing, and local history views
- Warframe market, world state, planner, and watchlist views
- TLDR Gmail reader with local SQLite-backed issue storage
- Learning workspace for daily Engineering English review with persistent progress
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

## Learning

Open `Learning` from navigation or go directly to `/#learning`. The starter deck contains exactly 120 English-only Engineering English cards, with 30 cards in each category: `Measurement & Data`, `Engineering Practice`, `Sustainable Systems`, and `Academic Communication`. Prompts, answers, examples, explanations, and category labels are all English.

Choose a category and click `Start session` to review due cards first, followed by new cards. Use `Reveal answer` (or `Space`) to see the answer, example, and explanation, then rate your recall with `Again`, `Hard`, `Good`, or `Easy` (keyboard `1`–`4`). Ratings schedule the next review and save progress. You can also browse cards using search, category, and scheduling-status filters.

Card content, scheduling progress, and append-only review history persist in `data/learning.db`, including across reloads and container restarts when the data volume is retained. Seed content updates preserve existing progress and review history. Both **Download Data Backup** and **Full Migration Backup** include this database.

Open **Personal cards · Add or import** to create your own cards, edit them, or remove them from the active deck. Seed-managed cards remain read-only. Editing preserves review progress; removing a personal card retains its history in the database.

JSON imports use an array of objects; CSV imports require `prompt,answer` headers. Optional fields are `deck`, `category`, `skill`, `example`, `explanation`, `source`, and `tags` (JSON array or semicolon-separated CSV text). Preview first, then confirm. Files are limited to 1 MiB and 500 cards. Exact duplicates are skipped, not overwritten; conflicting IDs reject the import. Deck and category filters support your own collections.

Learning requires an online connection to the Holocron backend and follows the existing trusted-network, single-user boundary. It does not provide offline review or multi-user accounts.

## Personal Dashboard

Choose **Edit page** (or **Seite bearbeiten** with the German language preference) on Home to arrange the whole dashboard: Welcome, Quick Launch, Weather, Monitoring, AI/Web Quick Access, optional Jellyfin and the Tool Library. Drag a widget header to move a preview of the whole card, or use its accessible move buttons. Remove hides a widget without deleting its data; **Add widget** opens a catalog of the existing widgets with previews and short descriptions. Changes remain a draft until **Save layout**; Cancel and Reset are available. The saved layout persists in `data/settings.json`; the Full Migration Backup includes it. Existing layouts retain their order and visibility preferences. The editing control stays available even if every section is hidden. Searching the directory can temporarily reveal a hidden Tool Library without changing the saved layout. Dragging uses a locally bundled MIT-licensed SortableJS component, with no CDN dependency; see [third-party notices](THIRD_PARTY_NOTICES.md).

Jellyfin is disabled and hidden by default. In **Settings > Homelab**, enable Continue Watching and enter the server URL, API key and user ID. Save, then choose **Edit page > Add widget > Add Jellyfin** on Home and save the layout. It shows up to six resumable videos and opens the original Jellyfin page, with no embedded player. This is **Continue Watching**, not a claim that these videos are currently playing. Resume data is cached for five minutes in `data/jellyfin_dashboard_cache.json`; failures retain the last saved snapshot and respect a retry cooldown. Hidden tiles make no automatic provider requests. Authentication headers and image fetching stay server-side; Settings itself is for trusted users and can display saved credentials.

Home has a small build indicator linking directly to **Settings > General > Running Build**. Build details and provider diagnostics stay in General and are hidden on other Settings tabs. `/api/build-info` identifies the delivered source with a deterministic SHA-256 fingerprint, excluding runtime data and secrets. Docker builds generate a verified manifest with UTC build time; an optional `HOLOCRON_REVISION` build argument supplies the Git revision. Without that argument, the UI reports a code fingerprint rather than guessing a commit. This is the application build identity, not a Docker daemon image digest; no Docker socket access is needed. Local runs without a manifest still show their source identity.

## Warframe Farm Workflows

Under **Farm & trade > Farm Planner**, **Mission drop routes** adds mission nodes, reward rotations, event markers and drop chances from the [WFCD drop tables](https://github.com/WFCD/warframe-drop-data). Sources refresh in the background with a six-hour cache and last-good fallback. On first use, building the snapshot may take a moment. The average `1/p` is a count of reward rolls for that rotation, not guaranteed complete missions, time-to-drop or platinum/hour. Not every item has a mission drop; sets may require searching individual parts or relics.

Open **Personal farm journal** to start/stop a manual session, log quantities and optional estimated unit values, and record confirmed sale proceeds. **Use in journal** copies a mission route into a new session draft, never starts it automatically. Only one session can be active. Previous sessions remain editable for corrections; sale proceeds are a replacement total, not an increment. Unpriced drops stay unknown and estimates are kept separate from actual sales. Records persist in `data/warframe_farm_journal.db`, included in both backup exports. No automatic trades, game hooks or AlecaFrame session tracking are involved.

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
- Normal updates should use `docker compose up -d --build`; this recreates the container but keeps the data volume.
- Do not use `docker compose down -v` unless you intentionally want to delete all local application data.
- Keep the Portainer stack name and volume mapping stable. A changed stack name can create a new empty prefixed volume that looks like lost data.

## Data
- Tools file: `data/tools.json` (auto-created from the bundled seed file, with missing defaults synced on load)
- Sources file: `data/sources.json`
- Feed snapshot: `data/feed_items.json`
- F1 history DB: `data/f1_history.db`
- Market history DB: `data/markets_history.db`
- Warframe market history DB: `data/warframe_market_history.db`
- Warframe world-state history DB: `data/warframe_worldstate.db`
- Warframe personal farm journal DB: `data/warframe_farm_journal.db`
- Optional Jellyfin resume snapshot: `data/jellyfin_dashboard_cache.json`
- TLDR issue DB: `data/tldr_issues.db`
- Learning content, progress, and review history DB: `data/learning.db`

## Homelab Command Center

The `Homelab` view aggregates registered Home Lab services into Overview, Systems, Services, Network, Media and Monitoring sections. `GET /api/homelab/overview` combines the editable Tool Hub registry with optional read-only Uptime Kuma and Beszel adapters. Missing provider configuration is safe: registry reachability checks still work, and no mock health data is generated.

Provider requests authenticate server-side. Connections can be configured in the trusted-user Settings page or with these optional variables in Portainer or Docker Compose:

- `UPTIME_KUMA_URL`, optionally `UPTIME_KUMA_API_KEY` or `UPTIME_KUMA_USERNAME` / `UPTIME_KUMA_PASSWORD`
- `BESZEL_URL`, optionally `BESZEL_API_KEY` or `BESZEL_USERNAME` / `BESZEL_PASSWORD`

The adapter layer caches the last successful provider snapshot in `data/homelab_provider_cache.json`. Provider failures return the last known data with a stale marker instead of blocking the Homelab page. The cache is included in normal runtime backups and is written atomically; existing application databases are not replaced.

### Data Safety

Runtime databases and local state are excluded from Git and Docker images. They live in the persistent `holocron_data` volume or your bind-mounted `/app/data` directory. Schema initialization is additive (missing tables/columns); it does not replace existing databases. Before changing a Portainer stack or volume, export or back up the volume first.

The Settings page includes **Download Data Backup**. It creates a portable ZIP containing application databases, histories, feeds, schedules and tool data. The separate **Full Migration Backup** also includes the effective API settings and TLDR mailbox configuration for a private plug-and-play migration. Treat that file like a password: it contains credentials and must never be shared or committed. To move data to a bind-mounted `/app/data` path, stop the old container, extract the selected ZIP into the new host folder, and deploy without deleting the existing volume.

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

