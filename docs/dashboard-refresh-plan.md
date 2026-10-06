# Home Dashboard Refresh Plan

> This pass completes the launcher-first Home dashboard and Uptime Kuma shortcut. It is not a pixel-for-pixel implementation of the larger NeoDeck rendering; data-backed system widgets are deferred.

## Goal

Make the HolocronHub home page a useful, visually intentional personal command center, aligned with the approved dark navy/black and cyan-neon references. It should launch useful destinations quickly and link to Uptime Kuma for authoritative service health rather than duplicating potentially inconsistent status data.

## Design Decisions

- Keep the existing application shell, tab routing, and editable tool catalog.
- Use the configured Home Lab catalog as the source of service names, friendly URLs, icons, groups, and favorite state.
- Use the provided Uptime Kuma dashboard URL as the single status destination; do not show duplicate online/offline counts or per-service availability on the Home dashboard.
- Make the dashboard itself the primary Home view; remove duplicate/hidden welcome content and duplicate launcher surfaces where possible.
- Keep F1 and Warframe presentation outside this dashboard-only pass.

## Work Packages

1. **Dashboard structure and content**
   - Build a single welcome/command-center header with the Holocron mark, live local clock/date, and concise status context.
   - Organize the first screen around direct launch: clear sections for service launchers and a compact Uptime Kuma shortcut.
   - Remove fake N/A gauges, fake activity events, generic “local adapter” values, and redundant welcome markup.
2. **Service links and status ownership**
   - Render Home Lab launchers directly from the editable tool catalog, using friendly names, links, groups, and icons.
   - Keep reachability probes off the main dashboard so a second, potentially divergent status summary is not shown.
   - Link directly to Uptime Kuma for live service health; preserve the dedicated Home Lab view separately.
3. **Visual system and navigation affordances**
   - Replace letter-based sidebar glyphs with consistent inline SVG icons while preserving existing tab actions and accessible labels.
   - Improve service icons using the catalog's icon metadata with a deterministic visual fallback; keep service names visible, not IPs.
   - Apply the established black/navy surface, cyan accent, restrained glow, clear hierarchy, and subtle entrance motion; respect reduced-motion preferences.
4. **Responsive and interaction verification**
   - Verify desktop, tablet, and mobile layout, collapsed/expanded navigation, keyboard focus, service links, status states, and no horizontal overflow.
   - At narrow widths, keep the main sidebar reachable through the header menu button as an accessible overlay drawer.
   - Run relevant syntax/build checks and inspect the rendered page in a browser before finalizing.

## Acceptance Checks

- The home view has one clear welcome area and no fake telemetry or fabricated events.
- Configured Home Lab services have friendly names, useful icons/fallbacks, and working configured links without redundant status dots or latency labels.
- A direct Uptime Kuma link is available as the authoritative service-status view.
- Sidebar icons are consistent and existing navigation remains functional.
- On narrow screens the menu button opens and closes the navigation drawer, and choosing a section dismisses it.
- The layout remains usable at narrow mobile widths and wide desktop widths.

## Deferred From The Larger Reference Dashboard

These are not represented as fake/placeholder cards in this pass. They need a reliable source and a separate implementation/verification pass:

- Beszel CPU, memory, temperature, network throughput and container summaries.
- TrueNAS storage capacity and pool health.
- Weather for the home location.
- Media-specific launch panel, if the general service launcher is not sufficient.
- A real recent-events/activity feed.

Uptime Kuma remains the direct destination for service availability instead of duplicating its status counts in HolocronHub.
