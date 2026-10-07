# Home Dashboard Refresh Plan

> Updated 2026-10-07: the Home dashboard now includes weather, an optional Kuma snapshot, direct service editing, icon selection and a centered launcher search. System telemetry and storage widgets remain future work.

## Goal

Make the HolocronHub home page a useful personal command center, aligned with the approved dark navy/black and cyan references. Keep the default page compact, launch destinations quickly, and use Uptime Kuma as the service-health source.

## Design Decisions

- Keep the existing application shell, tab routing, and editable tool catalog.
- Use the configured Home Lab catalog as the source of service names, friendly URLs, icons, groups, and favorite state.
- Use the configured Uptime Kuma connection for a short snapshot (counts and up to three monitors) and link to its dashboard. Show unavailable or saved-state labels when necessary. Do not run separate service probes on Home.
- Make the dashboard itself the primary Home view; remove duplicate/hidden welcome content and duplicate launcher surfaces where possible.
- Keep F1 and Warframe content outside this dashboard pass; use a small race-car icon for F1 navigation.
- Keep favorites visible as small launch links; put the larger AI and service directories behind the tool-library disclosure.

## Work Packages

1. **Dashboard structure and content**
   - Build a single welcome/command-center header with the Holocron mark, live local clock/date, and concise status context.
   - Organize the first screen around direct launch, local weather and a compact Uptime Kuma snapshot/shortcut.
   - Remove fake N/A gauges, fake activity events, generic “local adapter” values, and redundant welcome markup.
2. **Service links and status ownership**
   - Render Home Lab launchers directly from the editable tool catalog, using friendly names, links, groups, and icons.
   - Keep reachability probes off the main dashboard so a second, potentially divergent status summary is not shown.
   - Link to the configured Uptime Kuma dashboard and use only its cached adapter data for the Home snapshot.
   - Edit service names, groups, URLs and icons directly from Quick Launch. Save through the existing tool API and update host/port metadata from the URL.
3. **Visual system and navigation affordances**
   - Replace letter-based sidebar glyphs with consistent inline SVG icons while preserving existing tab actions and accessible labels.
   - Improve service icons using the catalog's icon metadata with a deterministic visual fallback; keep service names visible, not IPs.
   - Apply the established black/navy surface, cyan accent, restrained glow, clear hierarchy, and subtle entrance motion; respect reduced-motion preferences.
4. **Responsive and interaction verification**
   - Verify desktop, tablet, and mobile layout, collapsed/expanded navigation, keyboard focus, service links, status states, and no horizontal overflow.
   - At narrow widths, keep the main sidebar reachable through the header menu button as an accessible overlay drawer.
   - Run relevant syntax/build checks and inspect the rendered page in a browser before finalizing.
5. **Weather, search and icon selection**
   - Default to Augsburg; allow location name and coordinates in Settings > Homelab.
   - Cache Open-Meteo weather for 15 minutes and persist the last good reading in the existing backed-up local cache. Serve saved data during background refresh; throttle failed retries for one minute.
   - Show a centered modal search from any hub, with name/tag/provider/group/host/task matching, keyboard selection and direct launch.
   - Keep directory filtering separate from the dashboard launchers.
   - Offer 21 icon choices (automatic, service brands and generic SVG icons) plus custom icon URLs in the service and tool editors.

## Acceptance Checks

- The home view has one clear welcome area and no fake telemetry or fabricated events.
- Configured Home Lab services have friendly names, useful icons/fallbacks, and working configured links without redundant status dots or latency labels.
- A direct Uptime Kuma link is available as the authoritative service-status view.
- Weather and Kuma refresh separately and preserve saved readings/snapshots on failures.
- Service URLs and selected icons survive reload; custom domain names work and their host metadata stays consistent.
- Search opens centrally without scrolling Home, works outside Home, and supports Escape, arrow keys, Enter and Ctrl/Cmd+K.
- The default tool library is collapsed; task lists and full editing remain available when expanded.
- Sidebar icons are consistent and existing navigation remains functional.
- On narrow screens the menu button opens and closes the navigation drawer, and choosing a section dismisses it.
- The layout remains usable at narrow mobile widths and wide desktop widths.

## Deferred From The Larger Reference Dashboard

These are not represented as fake/placeholder cards in this pass. They need a reliable source and a separate implementation/verification pass:

- Beszel CPU, memory, temperature, network throughput and container summaries.
- TrueNAS storage capacity and pool health.
- Media-specific launch panel, if the general service launcher is not sufficient.
- A real recent-events/activity feed.

## Verification Of This Pass

- 49 backend tests passed, including persisted weather, refresh failure, coordinate validation and selective Kuma adapter caching.
- Browser checks covered service URL/icon persistence, validation, failure states, search, direct keyboard launch, filtering, browser Back and mobile navigation.
- Checked widths: 390, 768, 1440 and 1920 pixels, without horizontal page overflow. Desktop favorites fit in a compact row; the catalog stays closed on first load.
- Real Open-Meteo weather was verified locally. Kuma rendering and failure handling were checked with fixtures because the local checkout has no Kuma credentials; the deployed instance uses its existing private connection settings.

## Next Pass: Feedback From 2026-10-07

Implemented in the follow-up pass. No deployment or push was requested; the completed changes are committed locally.

1. **F1 weather reliability and timestamps**
   - Investigate the reported OpenF1 weather 429 for meeting 1296. Reuse cached readings, honor provider cooldown/Retry-After, and avoid duplicate weather calls from refresh, polling and prewarm.
   - The screenshot already shows Open-Meteo fallback data. Keep real problems in diagnostics, but make the user-facing source/fallback state understandable instead of showing a raw URL error above otherwise usable weather.
   - Fix the displayed future freshness label ("Updated in 5h"). The shared Open-Meteo helper currently returns a local timestamp without its UTC offset; F1 forwards it to the browser. Normalize timestamps centrally before rendering age labels.
2. **Header and navigation alignment**
   - Remove the redundant active-section label beside HolocronHub across all tabs; the sidebar already identifies the current section.
   - Keep the sidebar toggle in a stable position beside the brand when switching hubs and screen widths.
3. **One consistent left sidebar**
   - F1 and Warframe currently have explicit right-aligned menu CSS. Replace it with the same left-sided behavior as Home.
   - On desktop, expanding/collapsing navigation should adjust content width; use a left drawer on narrow screens and preserve a useful full-width collapsed view.
4. **Edit Home Lab from the Home Lab page**
   - Reuse the existing service editor for names, URLs/domains/IPs, groups and icons directly in the dedicated Home Lab view.
   - Support monitors that appear only through Kuma by saving local launcher metadata/overrides; keep Kuma as the health-data source.
   - Allow adding services and adjusting their display without coding. Check current grouping: the screenshot places Home Assistant and TrueNAS under Network while Systems/Services are empty.
5. **Appearance settings**
   - Offer saved choices for the darker black/cyan reference and the current navy/neon appearance.
   - Connect the existing Display options button to real settings; consider restrained glow/accent and compactness controls.
   - Persist preferences through reloads and container updates; verify contrast, mobile layout and reduced-motion behavior.

Recommended execution order: weather/timestamp fixes, shared header/sidebar fixes, Home Lab editing, then appearance options. The welcome dashboard and current Warframe presentation are positively received and remain the visual baseline.

## Follow-Up Completion And Verification

- F1 weather requests share a five-minute persisted cache; failed requests have a one-minute retry cache. OpenF1 429 responses honor numeric and HTTP-date Retry-After headers without an immediate retry. Concurrent successes and failures cannot shorten an active cooldown.
- Open-Meteo readings also have a shared persistent cache and last-good fallback. Existing combined F1 snapshots retain weather for the same meeting on partial failures, but not for another race or beyond 24 hours. Saved readings are labeled; provider errors remain in diagnostics rather than raw URL warnings over usable fallback data.
- Open-Meteo measurement times include their actual UTC offset. Fresh F1 and Home Lab overview timestamps are timezone-aware, avoiding false age labels from UTC containers.
- The header no longer displays a redundant hub name. Its navigation button stays beside the brand. All hubs use the same left sidebar: desktop expansion reduces content width, collapse reclaims it, and mobile opens below the current header without obscuring it.
- Home Lab supports Add service and per-service Edit/Customize actions using the existing tool catalog. Kuma-only monitors become local editable launchers with a persisted monitor binding, keeping their original health source after local name/URL edits. Specific group metadata takes precedence over the broad Home Network category.
- Ruling: registered offline services remain visible and editable instead of being silently hidden when Kuma is healthy. This can expose obsolete entries again; their links can be corrected or they can be removed through Manage Tools. No service or stored data was deleted automatically.
- Appearance settings provide Navy / Neon blue and Midnight / Black cyan, optional glow and compact spacing. Preview can be reset; saved choices use the existing backed-up settings file. The header display button opens these settings. Controls wait for settings loading, and failed reads/writes do not replace saved preferences.
- Fixed the existing blank service-icon fallback: a generic SVG stays visible while the remote logo loads and after failure.
- No new database, data migration, credentials replacement, framework rewrite or changes to Markets/Warframe ingestion are required. Existing files remain in the persistent data directory.

Verification:

- `python -m unittest discover -s backend -p 'test_*.py' -q`: 95 tests passed, including 13 new regression tests for this pass. Existing Python/Starlette deprecation warnings remain non-failing.
- Inline frontend JavaScript syntax checked; Git whitespace checks passed.
- Browser checks passed at 390, 768, 1440 and 1920 pixels: consistent header/sidebar positioning, reclaimed content width, mobile drawer placement/dismissal, browser Back, central search, keyboard launch, icon/service persistence and independent filtering.
- Browser checks also covered imported Kuma monitors, adding services, stale-editor protection, appearance save/reload, failed settings/service saves, failed Home Lab refresh, saved widget data and reduced-motion preferences.
- Tests used an isolated temporary copy of writable local data. Kuma/OpenF1 failure states used fixtures, not the deployed server's private credentials. Server-side live integrations still require the user's deployment review.
- Independent read-only review identified three issues (cooldown race, stale editor metadata, incomplete fallback preservation). Each was reproduced and fixed with a failing-then-passing regression check before final verification.
