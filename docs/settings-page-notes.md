# Settings Page Notes (v1 scope add-on)

## Purpose
Simple local settings to control UX and source behavior without risky auto-execution.

## v1 fields
- default_category (AI/Finance/Gaming/Warframe)
- show_disabled_sources (bool)
- refresh_interval_minutes (int)
- digest_mode (off/daily/twice)
- language (DE/EN)

## storage
- Frontend localStorage first
- optional backend config file in v1.1

## safety
- no secret keys in settings page
- key management remains env-based
