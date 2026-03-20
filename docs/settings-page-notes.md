# Settings Page Notes (v1.2)

## Purpose
Central configuration page for UX, feed behavior, ingest scheduling, and API/model credentials.

## Current sections
- General: language, default feed category, show disabled sources
- Feed & Digest: refresh interval, digest mode
- Ingest: enable schedule, interval hours, run at startup
- API: OpenClaw model, Marketaux/Finnhub/Alpha Vantage keys

## storage
- Backend file: `data/settings.json`
- Schedule remains in: `data/schedule.json`
- Frontend keeps a local UX cache for fallback (`localStorage`)

## behavior
- `GET /api/settings` returns unified settings payload (including schedule + next run)
- `PATCH /api/settings` updates partial sections and applies env vars at runtime
- API keys can be managed from UI (self-hosted/private use)

