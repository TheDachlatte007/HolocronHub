# Holocron Learning Design

## Intent

Add a mobile-first Learning workspace to Holocron Hub for daily Engineering English practice. Holocron owns card content, scheduling state, and review history. Anki remains an optional future export target and is not part of this first implementation.

The first release is single-user, online, and served by the existing Holocron deployment. It must work in iPhone Safari at the existing Holocron URL without installing a paid application.

## Success criteria

- A `Learning` destination is available from Holocron navigation and by the `#learning` URL.
- The workspace is usable on desktop and a narrow iPhone viewport.
- It ships with exactly 120 reviewed Engineering English cards.
- Every card-facing field is English: prompt, answer, example, explanation, and category label. There are no German translations on cards.
- Cards cover four balanced 30-card groups: `Measurement & Data`, `Engineering Practice`, `Sustainable Systems`, and `Academic Communication`.
- A review session shows due cards before new cards, supports reveal, and records `Again`, `Hard`, `Good`, or `Easy`.
- Progress and an append-only review log survive restarts and are included in Holocron backups.
- Seed updates may correct card content without resetting existing progress.
- Existing Holocron features and current uncommitted dashboard work continue to pass their tests.

## Architecture

### Content

`backend/learning.seed.json` is the versioned source of truth for starter cards. Every record has a stable ID, deck, category, skill, prompt, answer, example, explanation, and tags. The schema is generic enough for later languages, but v1 contains only `Engineering English`.

At startup, the store upserts seed content by stable ID. It updates content metadata but never overwrites review progress or review history.

### Persistence

`backend/learning_store.py` owns `data/learning.db` and uses short-lived SQLite connections with parameterized statements. It creates:

- `learning_cards`: versioned card content.
- `learning_progress`: one scheduling row per card.
- `learning_reviews`: append-only rating history.

All timestamps are UTC ISO-8601 strings. Database initialization is additive. `learning.db` is registered in Holocron's explicit backup database allowlist.

### Scheduling

V1 uses a deterministic spaced-repetition policy with an ease floor of `1.3` and an initial ease of `2.5`:

- `Again`: due in 10 minutes, interval becomes 0 days, ease decreases by 0.20, lapse count increments.
- `Hard`: due after `max(1, round(max(interval, 1) * 1.2))` days, ease decreases by 0.15.
- `Good`: a new card is due in 3 days; otherwise it is due after `max(1, round(interval * ease))` days.
- `Easy`: a new card is due in 7 days; otherwise it is due after `max(2, round(interval * ease * 1.3))` days, and ease increases by 0.15.

Ratings are accepted only as integers 1 through 4. The API returns the calculated next due time after every review. A later FSRS migration can replace this policy behind the store interface without changing card content or the append-only review log.

### API

`backend/learning_api.py` exposes an `APIRouter` under `/api/learning`:

- `GET /summary` returns totals, due/new counts, reviews today, streak, and category progress.
- `GET /session?limit=20&category=` returns due cards first and then unseen cards, capped by `limit`.
- `GET /cards?query=&category=&status=` returns browseable card metadata and progress.
- `POST /reviews` accepts `{card_id, rating}` and returns updated progress.

Unknown cards, invalid ratings, invalid limits, and unavailable stores return explicit 4xx/5xx responses rather than silent empty success.

### Frontend

`frontend/assets/learning.js` is an isolated IIFE exposing `window.HolocronLearning.mount()` and `load()`. `frontend/assets/learning.css` is scoped below `#tab-learning`.

The workspace contains:

- compact progress summary;
- category filters;
- a study card with hidden answer and explicit reveal action;
- four rating buttons shown only after reveal;
- keyboard controls (`Space` reveal, `1`-`4` rate) when no editable control has focus;
- browse panel with search and scheduling status;
- loading, empty, completed-session, and backend-error states.

The Learning UI and all learning content are English. Existing Holocron shell labels may remain unchanged. Mobile navigation must remain accessible below 900 px.

### Safety and operations

- No direct manipulation of Anki collection files.
- No public exposure or new authentication model is added; Holocron retains its current trusted-network, single-user boundary.
- Existing runtime data remains on the mounted `/app/data` volume.
- No reset/delete endpoint ships in v1.

## Testing

- Content contract: exactly 120 unique cards, 30 per required category, English-only fields present, stable IDs and tags valid.
- Store contract: seed updates preserve progress; all four ratings calculate exact intervals; due ordering, filtering, persistence, streak, and append-only logs work.
- API contract: summary/session/cards/reviews happy paths and validation errors.
- Frontend contract: assets and navigation hooks exist, the module is scoped, and mobile/reveal/rating controls are present.
- Regression: the full existing Python unittest suite passes.
- Manual acceptance: load `#learning` at desktop and narrow mobile widths, complete reviews, reload, and verify progress persists.

## Explicit exclusions

- Offline-first caching and conflict resolution.
- Multi-user authentication.
- Automatic AI card generation.
- Audio generation.
- Obsidian ingestion.
- Anki synchronization or export.
- Editing or deleting cards in the UI.
