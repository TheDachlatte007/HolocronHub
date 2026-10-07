# HolocronHub: Current Baseline And Next Features

Reviewed 2026-10-07. These are proposals, not implemented features or a commitment to build every integration.

## Current Baseline

- Daily launcher: compact Home, editable services, curated task-first AI directory, central search and common left navigation.
- Home Lab: canonical local tool catalog, read-only Kuma status, independent editable All services list and provider diagnostics.
- Warframe: market pulse, watchlist, farm/relic planning, inventory/AlecaFrame integration, accumulating local history, stored provider payloads and locally served artwork.
- F1: weekend/live/history views, weather fallback and persistent history.
- Feed, Saved, Digest and TLDR: local ingestion/storage and reading surfaces.
- Learning: Engineering English seed deck, spaced review, persistent progress and review history. User card/deck editing is not available yet.
- JSON/SQLite data and credentials remain under the existing backed-up persistent data directory. No storage migration is needed for the layout fix.

Local tests do not prove that every configured external provider works on the server. The current stage is a usable personal command center with continuing visual polish and integration verification, not a frozen product or a new framework migration.

## Stabilization Before More Widgets

1. Show the running build/version and last successful provider refresh in Settings. This distinguishes an old container image from a code regression.
2. Verify configured adapters individually after deployment; retain real unknown/stale/error states rather than fabricated telemetry.
3. Revisit Beszel before promoting CPU/RAM/container cards. The existing adapter probes `/api/metrics` and `/api/containers`, whereas the [official REST documentation](https://beszel.dev/guide/rest-api) describes PocketBase collections and user authentication. This is a compatibility concern, not proof that the user's particular installation is broken. API structure can change in minor versions; use version-aware fixtures.
4. Keep a browser regression matrix for Home, Warframe, F1 and Learning, with both sidebar states and mobile widths. The rounded hub frame is now covered as well.

## Recommended Feature Candidates

### 1. Personal Warframe Farm Journal

Highest relevance to the original goal: what should I farm to earn platinum efficiently?

- Start/stop a session, choose a target and record obtained items/counts manually first.
- Compare actual time spent, cached estimated item value and confirmed sale proceeds as separate numbers. Unsold inventory value is not realized profit; do not promise guaranteed platinum/hour.
- Build personal route comparisons from multiple sessions, with timestamps and source freshness.
- Reuse the existing planner, prices, inventory and local persistence rather than add another market scraper or a second farm radar.
- Mission rotations and drop probabilities have a usable community data source in [WFCD warframe-drop-data](https://github.com/WFCD/warframe-drop-data). The current planner already handles drop information; the new feature would be the user's own measured session history.

Initial scope: manual logging and history, no screen capture, game-client hooks or automatic trade execution. No additional paid API key is needed for that scope.

### 2. Personal Dashboard Layout

- Choose which existing areas are visible and their order through Settings.
- Offer focused Home presets, such as Gaming, Learn and Home Lab, without creating additional top-level hubs.
- Persist layout choices alongside existing theme/glow/density preferences.
- Keep weather, Quick Launch and favorites compact; hide unused sections rather than populate empty widgets.

Initial scope: toggle/reorder existing sections, not a generic drag-and-drop dashboard framework.

### 3. Own Learning Cards And Deck Imports

- Add and edit personal cards; import CSV/JSON decks with preview and validation.
- Turn a saved article/TLDR issue into a manually reviewed card draft with its source link.
- Separate user-managed cards from the managed Engineering English seed and preserve review history on edits/imports.
- Extend the current Learning store/API; do not replace its scheduling or database.

Initial scope: manual authoring/import. Automatic AI extraction is optional later and should never create paid background calls without a chosen provider and explicit settings.

### 4. Optional Jellyfin Continue Watching

- A compact opt-in media list with artwork/progress and a link into Jellyfin.
- No full embedded player or iframe-first strategy.
- Keep user credentials/token on the backend, with bounded caching and unavailable states.
- Jellyfin's [official Python API client](https://github.com/jellyfin/jellyfin-apiclient-python/blob/master/jellyfin_apiclient_python/api.py) includes resume-item retrieval. That supports feasibility, but the installed server version, user identity and authentication still need verification.

This is a proposed adapter, not a currently working HolocronHub integration.

## Suggested Order

Build/version and adapter verification first; then choose Farm Journal for gaming value or Dashboard Layout for everyday efficiency. Learning authoring follows when personal study content is wanted. Jellyfin remains optional, not another always-open dashboard panel.

Avoid duplicate launchers, a second monitoring system, automatic AI-directory scraping, generic third-party iframes and new database infrastructure until a concrete need justifies them.
