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

## Editor Feedback Pass, 2026-10-08

- Build details and provider diagnostics now belong only to Settings General. The Home build link opens General explicitly, even if another Settings tab was selected previously.
- Whole-card drag previews replace the previous Drag-button-only interaction. Header grips support mouse/touch; accessible earlier/later controls remain. Toolbar actions cannot interrupt an active drag. Reduced-motion preferences disable reorder animation.
- Add widget opens a modal catalog with seven existing widget previews/descriptions. Already enabled widgets are marked On dashboard; Remove hides a widget without deleting data. Add/remove/reorder/reset remain drafts until Save; Cancel, failed-save recovery, legacy layouts and all-hidden recovery retain their existing guarantees.
- The desktop header and sidebar now blend into the selected theme instead of forming opaque black bars. The sticky header gains a subtle translucent background only after scrolling; mobile navigation retains its readable drawer background.
- Dependency decision: USE the pinned, locally bundled SortableJS 1.15.7 MIT component. Homepage/Homarr informed patterns only; no full app code was copied. GridStack/free-form resizing is deferred because replacing the responsive grid/persistence is unnecessary for this feedback. Sources, license and bundle checksum are recorded in THIRD_PARTY_NOTICES.md.
- Verification covers pointer-following real widget content, touch reordering, drag action guards, catalog/add/remove/save/cancel/reset/reload, Settings tab isolation, narrow layouts and exact vendor checksum/license. Desktop/mobile catalog, whole-card drag and shell screenshots are generated under ignored output/playwright/editor-polish/.
- Final verification: 161 tests passed; all ten frontend scripts, including the vendored bundle, parse successfully. The browser QA also caught and fixed a swallowed first click after dropping a card by preserving unchanged DOM drop targets. The local Docker engine remains unavailable; no full container build or live Jellyfin verification is claimed.
- No runtime databases, live services or Jellyfin credentials were changed. Continue Watching is ready for the user's live check; active Now Playing and arbitrary user-defined/resizable widgets are not implemented by this pass.

## Jellyfin Now Playing, 2026-10-08

- Approved bounded follow-up: read-only Now Playing in the existing optional Jellyfin tile, separate from Continue Watching. No player embedding, remote-control commands or additional credentials/settings schema.
- Source contract: [official Jellyfin SessionController](https://github.com/jellyfin/jellyfin/blob/master/Jellyfin.Api/Controllers/SessionController.cs) exposes authenticated GET /Sessions. Backend filters exact normalized UserId, removes idle/inactive sessions, bounds output to six devices, normalizes titles/progress, and strips credentials/private paths/addresses.
- GET /api/dashboard/jellyfin/sessions has an independent 15-second in-memory cache with deduplicated requests, 60-second failure cooldown, five-minute permission cooldown and bounded Retry-After handling. It never writes live sessions into the persistent resume cache. Invalid responses are unavailable, not empty playback.
- UI polls every 20 seconds only on visible Home with the widget enabled. Navigation, hidden browser tabs and hidden draft widgets stop checks and clear live presentation. Expiry clears an old live badge during slow requests; configuration changes invalidate in-flight results. Resume data remains separately cached and can still be shown if session permissions are denied.
- Session rows show cover, episode/title, Playing/Paused, device/client, sampled progress and a deep link. Episode artwork can fall back to its series cover. Images remain allowlisted, server-authenticated and bounded in a five-minute memory cache; no new database or storage migration is introduced.
- Verification includes own-user filtering, separate devices, paused/empty states, malformed/denied/429 responses, failure expiry, configuration changes, image allowlisting/series fallback/cache, unchanged resume persistence, browser visibility/polling/backoff, Settings integration, edit-hide/cancel recovery and mobile layout. Fixture screenshots are under ignored output/playwright/jellyfin-now-playing/.
- Deployment follow-up: verify the user's configured Jellyfin server/client and API-key session permissions. No live server settings or credentials were read/modified for the fixture tests. Measured Warframe route comparisons remain a separate next feature.
- Final verification: 172 backend/browser tests passed and all ten frontend scripts parse. Mobile/desktop fixture screenshots were inspected; the live server and a full Docker image build were not exercised locally.

## Jellyfin 12 Compatibility Fix, 2026-10-09

- Live diagnosis found two independent blockers: a username in the UUID field prevented the hub from making provider requests, and Jellyfin 12.1 rejected the adapter's legacy X-Emby-Token authentication with HTTP 401.
- The same saved API key worked with the recommended Authorization: MediaBrowser scheme. All adapter calls now use that single header with URL-encoded quoted token values; redirects remain disabled and credentials never enter query parameters or frontend artwork URLs. No legacy-auth server option is enabled.
- Invalid user identifiers now return an explicit safe setup error, surfaced in both media sections; Settings labels distinguish IDs from usernames. User identities are not guessed or silently rewritten.
- A live read-only probe of the corrected adapter, using a uniquely matched ID only in memory, returned one playing session, six resume items and HTTP 200 artwork. The probe used a temporary isolated cache and did not change server settings or deployed data.
- Reference: [Jellyfin 12 release notes](https://jellyfin.org/posts/jellyfin-release-12.0/) and [Jellyfin API authorization guidance](https://gist.github.com/nielsvanvelzen/ea047d9028f676185832e51ffaf12a6f). Regression tests cover the modern header, encoded tokens, safe invalid-ID diagnostics and frontend messages.
- Final verification: 174 tests passed and all ten frontend scripts parse. Live adapter requests succeeded with the existing saved key; changing the deployed user-ID setting requires the user's explicit confirmation and is separate from this code fix.
