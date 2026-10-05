# Homelab Command Center

## Current Architecture

HolocronHub already stores editable Home Lab services in `data/tools.json`. Home services carry group, host/port, status metadata and optional deep links. The backend exposes lightweight reachability data through `/api/tools/home-lab/overview`, while the frontend renders the existing compact launcher cards.

The Command Center reuses that registry and probe layer instead of introducing a second service catalog or reimplementing Uptime Kuma, Beszel, TrueNAS, Home Assistant or Jellyfin.

## Delivery Phases

### Phase 1: Command Center Shell

- Add a dedicated `Homelab` top-level view.
- Add compact subviews for Overview, Systems, Services, Network, Media and Monitoring.
- Aggregate existing registered services into overall status, alerts, groups and latency.
- Keep unknown/unconfigured services visible as unknown; never invent health data.

### Phase 2: Server-Side Adapters

- Add read-only adapters for Uptime Kuma, Beszel, TrueNAS, Home Assistant and Jellyfin.
- Keep credentials server-side through environment variables.
- Apply per-provider timeouts, cached last-good values and explicit stale markers.
- Add provider status to the same aggregator response without making any provider mandatory.

### Phase 3: Storage and Drill-Down

- Persist health snapshots where historical context adds value.
- Add service detail views and external deep links.
- Add selected storage, performance, network and media summaries.

## Safety Rules

- No Docker socket access.
- No write operations to external services in the first implementation.
- No mock values in production; missing credentials produce `unknown` or `unconfigured`.
- Existing Tool Hub, Feed, TLDR, Markets, F1 and Warframe routes remain unchanged.
