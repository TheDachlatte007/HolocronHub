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
