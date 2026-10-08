# Dashboard And Personal Workflows

Approved scope 2026-10-07: editable/reorderable dashboard tiles, Warframe drop information and farm journal, personal Learning cards/imports, optional Jellyfin configured in Settings.

## Work Packages

1. Dashboard layout: edit mode with drag handles, keyboard/touch move controls, hide/show, reset, explicit save/cancel. Persist layout in existing settings; retain existing weather, Kuma, service editing and search behavior.
2. Warframe: keep existing drop planner; expose useful drop probabilities/rotations without another scraper. Add persisted manual farm sessions, elapsed time, quantities, estimated values and separately confirmed sales. No guaranteed platinum/hour, game hooks or automatic trades.
3. Learning: personal card CRUD and CSV/JSON preview/import, deck filtering, progress-preserving updates. Preserve seed-managed cards and existing review scheduling/history.
4. Jellyfin: disabled until URL/API key/user ID are configured in Settings. Read-only resume list, cached/stale states, no iframe/player embedding, no browser-exposed credentials. Include any new persisted store in normal backups.

## Boundaries

- Main agent owns frontend/index.html, backend/main.py, dashboard module/settings, integration and documentation.
- Farm worker owns new farm journal API/store/browser modules and tests only.
- Learning worker owns learning API/store/module/styles/tests only.
- Jellyfin worker owns new resume adapter/API/browser module and tests only.
- Workers do not modify shared shell/settings, commit, push, or change deployed services.
- No new database service, replacement of existing data, automatic paid API calls, or default always-open widget expansion.

## Verification

- Existing backend/browser suite remains green.
- New stores are append/update-only and tested with temporary files; backups include them.
- Layout persistence/reload, drag reorder, keyboard controls, cancel/reset, all-hidden recovery and mobile overflow are checked in browser fixtures.
- Farm values distinguish estimates from confirmed proceeds; no fabricated drop/value data.
- Personal Learning CRUD/import preserves progress and seed content.
- Jellyfin configured/unconfigured/errors/stale states use fixtures; live credentials are optional and not required to finish configuration/UI support.

## Progress

- Initial repository state clean. Research/feature direction recorded in docs/next-features.md.
- Completed: dashboard arrangement/hide/reset/save/cancel with persistent settings and accessible move controls; mission drop routes and manual farm journal; personal Learning CRUD/import; optional Jellyfin Settings and resume tile.
- Data safety: additive schemas, retained Learning progress/history, farm DB and Jellyfin snapshot in backups. Private settings/layout travel with Full Migration Backup. Runtime files and secrets are excluded from Git/Docker image builds.
- Verification: 153 backend/browser tests passed on 2026-10-07. Eight frontend scripts (including the shell) parse successfully. Browser fixtures cover Home reload persistence, mobile widths 320/390, hub/sidebar regression checks, Learning authoring/import, journal persistence/error drafts, drop-query races and Jellyfin failure states. Desktop/mobile Home screenshots were inspected under ignored output/playwright/dashboard-qol/.
- Final focused review fixed keyboard focus during reordering, pagination beyond 200 farm sessions, empty grid space after rearrangement, in-flight Jellyfin configuration changes and asynchronous artwork failure checks.
- Live limitation: no configured Jellyfin server/user credentials were exercised. No deployed services or existing runtime databases were changed during verification.
- Follow-up 2026-10-08: whole-Home editing adds Welcome and Tool Library without resetting saved layouts; clear Edit page entry, global search/Settings access, removal of duplicate top navigation, sticky desktop header, consistent Warframe surfaces/actions and truthful freshness indicators.
- Build identity is implemented on Home and in Settings, including source fingerprint, optional supplied Git revision and verified image build time. Runtime data and secrets are excluded from the fingerprint. The Docker metadata generation command is independently tested; a full local image build is unavailable because Docker Desktop's engine is not running.
- Follow-up verification: 159 tests passed, nine frontend scripts parse, and fixture screenshots for default Home, mobile page editing and Warframe header were inspected. Checks include 320/390/901/1024/1440 widths, desktop scroll persistence, consecutive native drag gestures, hidden-library search recovery and build-stamp generation. No live server credentials or existing databases were modified.
- Remaining follow-ups: version-aware Beszel verification, measured multi-session farm comparisons, optional article-to-card drafts/presets/resizing and active Jellyfin Now Playing (not the existing Continue Watching list). None blocks this approved manual baseline.
