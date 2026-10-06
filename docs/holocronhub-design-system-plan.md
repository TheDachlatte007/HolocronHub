# HolocronHub Design System Plan

## Design North Star

HolocronHub becomes a personal operations cockpit with a NeoDeck-inspired density and a distinct HolocronHub identity. The dashboard may use a Welcome Back hero, while specialized hubs use focused workspaces instead of a generic card wall.

## Shared Foundation

- Dark charcoal and navy surfaces, readable slate panels, and restrained cyan/mint glow.
- A small hologram/hexagon motif used as brand language, not as decoration on every card.
- Compact global header with HolocronHub identity, current area, refresh/search actions, and a collapsible navigation control.
- Shared typography scale, spacing, border radius, focus states, status dots, alerts, buttons, and loading/empty states.
- Responsive behavior: navigation collapses outside the dashboard and becomes an overlay on small screens.

## Area Contracts

### Dashboard

- Personal Welcome Back entry point.
- Quick launch and high-level system signals.
- Curated tiles only; no raw provider dump.

### F1

- Race-control layout.
- Race hero, countdown, session timeline, track/weather signals, timing board, and optional live sidebar.
- Cyan base with restrained racing red/amber accents.

### Warframe

- Market-operations layout.
- Item search, platform controls, market snapshot, price history, demand/liquidity leaders, farm signals, and world-state utility panels.
- Existing sections remain available: Overview, Farm & Trade, My Collection, alerts, fissures, invasions, news, relic radar, and trader signals.
- Cyan base with amber and muted void-violet accents.

## Delivery Order

1. Shared design tokens and app shell primitives.
2. Header and collapsible navigation behavior without changing hub data flows.
3. Dashboard composition and Welcome Back treatment.
4. Warframe market workspace, including interactive price-history details.
5. F1 race-control workspace.
6. Responsive pass and visual regression checks at desktop and mobile widths.
7. Ava-ready hologram surface as a later, optional layer; no animated assistant dependency in the first pass.

## Guardrails

- Preserve existing data contracts and API behavior.
- Do not remove existing hub categories while rearranging their presentation.
- Do not add decorative motion that competes with live data.
- Keep real provider errors visible and distinguish them from unknown or stale data.
- Use image assets only where they improve orientation; do not make critical information depend on them.
