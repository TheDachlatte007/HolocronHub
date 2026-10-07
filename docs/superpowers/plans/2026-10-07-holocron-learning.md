# Holocron Learning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a mobile-first Holocron Learning workspace with persistent spaced repetition and 120 English Engineering cards.

**Architecture:** A versioned JSON seed feeds a dedicated SQLite store and a small FastAPI router. An isolated vanilla-JavaScript frontend module integrates with Holocron's existing hash navigation and consumes the same-origin API.

**Tech Stack:** Python 3.12+, FastAPI, Pydantic, SQLite, vanilla HTML/CSS/JavaScript, Python `unittest`.

**Spec:** `docs/superpowers/specs/2026-10-07-holocron-learning-design.md`

## Global Constraints

- Ship exactly 120 starter cards: 30 in each of the four spec categories.
- Every card-facing field is English; no German translations appear on cards.
- Preserve existing user changes in `backend/main.py` and `frontend/index.html`.
- Do not alter Anki data or add Anki as a runtime dependency.
- Keep runtime data under the existing persistent `data` directory and include `learning.db` in backups.
- V1 remains single-user and online; do not add auth, offline sync, AI generation, audio, editing, or delete/reset operations.

## Review Focus

- A malformed or duplicate seed card must fail validation rather than partly populate the database; Task 1 tests exact cardinality, uniqueness, and required fields.
- A seed wording correction must not reset due dates or history; Task 2 tests progress preservation across reseeding.
- Repeated or invalid review submissions must not corrupt scheduling; Task 2 tests rating validation and append-only review rows, while Task 3 tests API validation.
- A session with both overdue and unseen cards must return overdue cards first and respect its limit/category; Task 2 owns these tests.
- Narrow mobile navigation and keyboard shortcuts must not trap the user or fire while typing; Task 4 pins the DOM hooks and editable-target guard, followed by manual viewport verification.

---

### Task 1: English Engineering seed manifest

**Files:**
- Create: `backend/learning.seed.json`
- Create: `backend/test_learning_content.py`

**Interfaces:**
- Consumes: the card schema and four categories in the spec.
- Produces: a JSON array of card dictionaries keyed by stable `id`, consumed by `learning_store.sync_seed_cards(path)` in Task 2.

- [ ] **Step 1: Write failing content-contract tests**

Add tests named `test_seed_contains_exactly_120_unique_english_cards`, `test_seed_balances_required_categories`, and `test_seed_records_have_complete_fields_and_tags`. Assert exact count, 30 cards per required category, unique `eng-sse-NNN` IDs, allowed skills (`recognition`, `production`, `cloze`), and non-empty English-facing fields.

- [ ] **Step 2: Run the content tests and verify RED**

Run: `python -m unittest backend.test_learning_content -v`

Expected: FAIL because `backend/learning.seed.json` does not exist.

- [ ] **Step 3: Create the 120-card seed file**

Write technically correct, natural English cards across the four exact categories. Use one assessable language target per card and include a source label such as `Holocron Engineering English v1`.

- [ ] **Step 4: Run the content tests and verify GREEN**

Run: `python -m unittest backend.test_learning_content -v`

Expected: all content-contract tests pass.

### Task 2: SQLite store and scheduler

**Files:**
- Create: `backend/learning_store.py`
- Create: `backend/test_learning_store.py`

**Interfaces:**
- Consumes: `backend/learning.seed.json` from Task 1.
- Produces: `init_learning_db(path)`, `sync_seed_cards(db_path, seed_path)`, `learning_summary(db_path, now=None)`, `learning_session(db_path, limit=20, category=None, now=None)`, `list_learning_cards(...)`, and `record_learning_review(db_path, card_id, rating, now=None)`.

- [ ] **Step 1: Write failing store tests**

Cover additive initialization, seed import, reseed-without-progress-reset, exact four-rating scheduling values, invalid ratings, unknown cards, due-before-new ordering, category filtering, persistent progress, append-only reviews, reviews-today, and streak calculation.

- [ ] **Step 2: Run store tests and verify RED**

Run: `python -m unittest backend.test_learning_store -v`

Expected: FAIL because `backend.learning_store` does not exist.

- [ ] **Step 3: Implement the minimal store API**

Use short-lived SQLite connections, WAL mode, foreign keys, explicit UTC serialization, and transactions around seed sync and review recording. Implement the exact scheduling policy from the spec.

- [ ] **Step 4: Run store tests and verify GREEN**

Run: `python -m unittest backend.test_learning_store -v`

Expected: all store tests pass.

### Task 3: FastAPI router, startup, and backup integration

**Files:**
- Create: `backend/learning_api.py`
- Create: `backend/test_learning_api.py`
- Modify: `backend/main.py`

**Interfaces:**
- Consumes: Task 2 store functions.
- Produces: `create_learning_router(db_path, seed_path) -> APIRouter` and `/api/learning/{summary,session,cards,reviews}`.

- [ ] **Step 1: Write failing API and backup tests**

Exercise all four endpoints through a temporary FastAPI app, including invalid limit/rating/category inputs and unknown cards. Assert `learning.db` is present in Holocron's backup database allowlist and contains committed review data in an export.

- [ ] **Step 2: Run API tests and verify RED**

Run: `python -m unittest backend.test_learning_api -v`

Expected: FAIL because the router and integration do not exist.

- [ ] **Step 3: Implement router and minimally integrate `main.py`**

Add the database/seed paths, initialize and seed at startup, include the router, and add `learning.db` to `_BACKUP_DATABASE_FILES`. Preserve all existing dashboard and provider edits.

- [ ] **Step 4: Run API tests and verify GREEN**

Run: `python -m unittest backend.test_learning_api -v`

Expected: all API and backup tests pass.

### Task 4: Mobile Learning workspace

**Files:**
- Create: `frontend/assets/learning.css`
- Create: `frontend/assets/learning.js`
- Create: `backend/test_learning_frontend.py`
- Modify: `frontend/index.html`

**Interfaces:**
- Consumes: Task 3 JSON endpoints.
- Produces: `window.HolocronLearning.mount(root)` and `window.HolocronLearning.load()` plus `#tab-learning` navigation.

- [ ] **Step 1: Write failing frontend-contract tests**

Assert the shell/top navigation, `HUB_CONTEXT_LABELS`, `tab-learning`, stylesheet/script assets, lazy loader, mobile rule, reveal/rating controls, `Space` and `1`-`4` shortcuts, and editable-target guard are present.

- [ ] **Step 2: Run frontend tests and verify RED**

Run: `python -m unittest backend.test_learning_frontend -v`

Expected: FAIL because Learning assets and navigation hooks do not exist.

- [ ] **Step 3: Implement the isolated frontend module and integration hooks**

Follow the existing dark design tokens, focused-workspace pattern, safe escaping conventions, hash routing, and loading/error states. Keep all learning-facing copy in English and preserve the existing 492-line uncommitted frontend change.

- [ ] **Step 4: Run frontend tests and verify GREEN**

Run: `python -m unittest backend.test_learning_frontend -v`

Expected: all frontend-contract tests pass.

### Task 5: Regression, documentation, and live acceptance

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: documented Learning usage and verified deployable application.

- [ ] **Step 1: Document Learning and its persistence boundary**

Add the `#learning` route, review controls, English-only seed description, `data/learning.db`, backup behavior, and current online/single-user limitation.

- [ ] **Step 2: Run the complete backend suite**

Run: `python -m unittest discover -s backend -p 'test_*.py'`

Expected: all tests pass; the pre-existing Starlette/Python 3.14 deprecation warning may remain.

- [ ] **Step 3: Build the deployment image**

Run: `docker build -t holocronhub:learning .`

Expected: image builds successfully and copies the new backend/frontend assets.

- [ ] **Step 4: Run an isolated container smoke test**

Start the image on a temporary non-production port with a temporary data volume; verify `/api/health`, `/api/learning/summary`, `/api/learning/session`, and `/#learning`, then stop the temporary container.

- [ ] **Step 5: Perform browser acceptance**

Verify desktop and iPhone-sized layouts, answer reveal, all four ratings, persistence after reload, category filtering, completed-session state, and navigation back to Home.

- [ ] **Step 6: Prepare production deployment without overwriting unrelated work**

Review the final diff and deployment mechanism. Deploy only if the release can preserve the repository's pre-existing uncommitted dashboard/provider changes and the persistent Holocron data volume.
