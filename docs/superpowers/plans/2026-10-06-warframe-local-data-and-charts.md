# Warframe Local Data and Charts Implementation Plan

**Goal:** Render the last persisted Warframe data immediately after opening or redeploying the hub, update it independently in the background, and make price history readable at every screen size.

**Architecture:** SQLite holds reusable provider payloads, the catalog, and accumulating price samples. A bounded, deduplicated refresh queue separates network work from page requests. Artwork lives in the mounted data directory and is served from the hub after download. Existing API fields and personal data remain compatible.

**Tech stack:** FastAPI, SQLite, requests, stdlib threading, existing vanilla HTML/CSS/JavaScript.

**Spec:** User-approved design references and `docs/holocronhub-design-system-plan.md`.

## Constraints

- Preserve all existing runtime data and uncommitted UI work.
- No startup reset, implicit history deletion, or external API call on the saved-data response path.
- Keep artwork and new databases under `/app/data`; include them in browser backup export.
- Keep source timestamps and distinguish saved, updating, failed, and genuinely empty states.
- Respect request spacing/backoff and deduplicate simultaneous refreshes.
- Use actual history timestamps and prices. Never fabricate graph values or available ranges.

## Work Packages

- [x] Persistent provider store and background queue (`backend/warframe_cache_store.py`, `backend/main.py`): local overview/pulse reads, catalog persistence, startup seeding from last-good records, refresh diagnostics, and bounded refresh batches.
- [x] History retention (`backend/warframe_history_store.py`, `backend/main.py`): stop deleting all but 240 samples; remove dependence on the capped legacy JSON index; retrieve recent chart samples without dropping stored history.
- [x] Local artwork (`backend/warframe_asset_store.py`, `backend/main.py`): atomic trusted-host image downloads, local serving, restart reuse, export inclusion.
- [x] Responsive charts (`frontend/index.html`, `frontend/assets/warframe-chart.js`): timestamp axes, readable price scale, small markers, hover/touch/keyboard detail, source and honest time-range controls, responsive layout.
- [x] Finish reviewed UI details (`frontend/index.html`): one-time/reduced-motion Home entrance, tablet grid, readable unclipped mobile F1 hero.
- [x] Verify persistent restart behavior, failed refreshes, duplicate requests, backups, history retention and append-only samples with 40 backend tests; inspect Warframe, F1 and Home in a real browser at desktop and mobile sizes.
- [x] Review implementation and record deployment notes; commit and push the verified changes under the user's standing authorization.

## Review Focus

1. A new browser after container restart must receive persisted records without waiting for external services.
2. Failed/empty external responses must never replace usable saved market data.
3. Simultaneous page opens must not produce duplicate refresh storms.
4. Existing migrated SQLite histories must work even if the old JSON index is absent.
5. Flat, sparse, long, mobile, and untimestamped history must render honestly and without distorted markers.

## Verification Record

Verified 2026-10-06: all backend tests pass (`python -m unittest discover -s backend -p 'test*.py' -v`, 40 tests). The inline app JavaScript and standalone chart script parse with Node. Browser inspection confirmed the saved Warframe snapshot, image, chart controls, market pulse, mobile Warframe layout, and F1 mobile hero. The F1 weather provider can still return HTTP 429; the page renders and remains usable. New local history begins accumulating from the first successful refresh and therefore starts sparse on a fresh data volume.
