# HolocronHub: Current Baseline And Next Features

Reviewed 2026-10-08. This is a status index and list of possible follow-ups, not a commitment to build every integration. The approved manual workflow baseline below is implemented; external integrations still need deployment verification.

## Current Baseline

- Daily launcher: compact Home, editable services, curated task-first AI directory, central search and common left navigation.
- Home Lab: canonical local tool catalog, read-only Kuma status, independent editable All services list and provider diagnostics.
- Warframe: market pulse, watchlist, farm/relic planning, inventory/AlecaFrame integration, accumulating local history, stored provider payloads and locally served artwork.
- F1: weekend/live/history views, weather fallback and persistent history.
- Feed, Saved, Digest and TLDR: local ingestion/storage and reading surfaces.
- Learning: Engineering English seed deck, personal card authoring, CSV/JSON preview/import, deck filtering, spaced review and persistent history.
- JSON/SQLite data and credentials remain under the existing backed-up persistent data directory. No storage migration is needed for the layout fix.

Local tests do not prove that every configured external provider works on the server. The current stage is a usable personal command center with continuing visual polish and integration verification, not a frozen product or a new framework migration.

## Stabilization Before More Widgets

1. Build identity is now shown on Home and in Settings General only, using verified build metadata/source fingerprints rather than assuming a Git revision. Provider diagnostics stay in General; clearer per-provider freshness remains a follow-up.
2. Verify configured adapters individually after deployment; retain real unknown/stale/error states rather than fabricated telemetry.
3. Revisit Beszel before promoting CPU/RAM/container cards. The existing adapter probes `/api/metrics` and `/api/containers`, whereas the [official REST documentation](https://beszel.dev/guide/rest-api) describes PocketBase collections and user authentication. This is a compatibility concern, not proof that the user's particular installation is broken. API structure can change in minor versions; use version-aware fixtures.
4. Keep a browser regression matrix for Home, Warframe, F1 and Learning, with both sidebar states and mobile widths. The rounded hub frame is now covered as well.

## Recommended Feature Candidates

### 1. Personal Warframe Farm Journal

Implemented baseline: manual start/stop, drop quantities, optional estimated values, separate confirmed sale totals, corrections and paginated history. Mission drop routes are cached and can populate a journal draft. Personal multi-session route comparisons remain a follow-up, not a promised feature.

Highest relevance to the original goal: what should I farm to earn platinum efficiently?

- Start/stop a session, choose a target and record obtained items/counts manually first.
- Compare actual time spent, cached estimated item value and confirmed sale proceeds as separate numbers. Unsold inventory value is not realized profit; do not promise guaranteed platinum/hour.
- Build personal route comparisons from multiple sessions, with timestamps and source freshness.
- Reuse the existing planner, prices, inventory and local persistence rather than add another market scraper or a second farm radar.
- Mission rotations and drop probabilities have a usable community data source in [WFCD warframe-drop-data](https://github.com/WFCD/warframe-drop-data). The current planner already handles drop information; the new feature would be the user's own measured session history.

Initial scope: manual logging and history, no screen capture, game-client hooks or automatic trade execution. No additional paid API key is needed for that scope.

### 2. Personal Dashboard Layout

Implemented baseline: whole-Home editing, including Welcome and the Tool Library; whole-card mouse/touch drag previews, accessible move buttons, an Add widget catalog instead of visibility checkboxes, remove/hide, reset, save/cancel, persisted settings and all-hidden recovery. Presets, arbitrary new widget types and free-form resizing are not included.

- Choose which existing areas are visible and their order through Home's Edit page mode.
- Offer focused Home presets, such as Gaming, Learn and Home Lab, without creating additional top-level hubs.
- Persist layout choices alongside existing theme/glow/density preferences.
- Keep weather, Quick Launch and favorites compact; hide unused sections rather than populate empty widgets.

Initial scope: toggle/reorder existing sections, not a generic drag-and-drop dashboard framework.

### 3. Own Learning Cards And Deck Imports

Implemented baseline: personal CRUD, custom deck/category filters, bounded CSV/JSON preview and atomic import with duplicate/collision protection. Article-to-card drafts and AI extraction remain proposals.

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

The adapter, Settings fields and opt-in dashboard tile are implemented and covered by fixtures. A configured live Jellyfin server has not yet been verified. No credentials are needed to use the other new features.

## Suggested Order

Next: review the new shell and Home editing after redeployment, then version-aware Beszel verification and measured farm comparisons. Jellyfin remains optional, not another always-open dashboard panel. Active "Now Playing" is distinct from the implemented Continue Watching list and remains a follow-up. The detailed implementation record is in [dashboard-qol-implementation.md](dashboard-qol-implementation.md).

Avoid duplicate launchers, a second monitoring system, automatic AI-directory scraping, generic third-party iframes and new database infrastructure until a concrete need justifies them.
