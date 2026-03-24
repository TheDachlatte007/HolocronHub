from __future__ import annotations

import argparse
from datetime import datetime, timezone


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _load_backend_impl():
    try:
        from .main import (
            F1_HISTORY_DB_FILE,
            _archive_f1_session_payload,
            _fetch_openf1_season_sessions,
            _fetch_openf1_session_detail,
        )
        from .f1_history_store import ensure_f1_history_db, get_f1_history_session
    except Exception:
        from main import (
            F1_HISTORY_DB_FILE,
            _archive_f1_session_payload,
            _fetch_openf1_season_sessions,
            _fetch_openf1_session_detail,
        )
        from f1_history_store import ensure_f1_history_db, get_f1_history_session
    return {
        "F1_HISTORY_DB_FILE": F1_HISTORY_DB_FILE,
        "archive": _archive_f1_session_payload,
        "fetch_season_sessions": _fetch_openf1_season_sessions,
        "fetch_session_detail": _fetch_openf1_session_detail,
        "ensure_db": ensure_f1_history_db,
        "get_session": get_f1_history_session,
    }


def backfill_season(season: str, *, force: bool = False, limit: int | None = None) -> dict[str, int]:
    impl = _load_backend_impl()
    history_db_file = impl["F1_HISTORY_DB_FILE"]
    impl["ensure_db"](history_db_file)
    sessions, errors = impl["fetch_season_sessions"](str(season))
    stored = 0
    skipped = 0
    failed = 0
    if limit and limit > 0:
        sessions = sessions[:limit]
    for session in sessions:
        session_key = str(session.get("session_key") or "").strip()
        if not session_key:
            continue
        if not force and impl["get_session"](history_db_file, session_key):
            skipped += 1
            continue
        detail, detail_errors = impl["fetch_session_detail"](session_key)
        if isinstance(detail, dict) and detail:
            payload = {
                "generated_at": _utc_now(),
                "data_as_of": detail.get("data_as_of") or session.get("date_end") or session.get("date_start") or _utc_now(),
                "session_key": session_key,
                "source": detail.get("source") or "openf1",
                "session": detail.get("session") or session,
                "live": bool(detail.get("live")),
                "drivers_count": int(detail.get("drivers_count") or 0),
                "leaderboard": detail.get("leaderboard") or [],
                "race_control": detail.get("race_control") or [],
                "weather": detail.get("weather") or {},
            }
            impl["archive"](session_key, payload)
            stored += 1
            continue
        payload = {
            "generated_at": _utc_now(),
            "data_as_of": session.get("date_end") or session.get("date_start") or _utc_now(),
            "session_key": session_key,
            "source": "openf1_index",
            "session": session,
            "live": False,
            "drivers_count": 0,
            "leaderboard": [],
            "race_control": [],
            "weather": {},
        }
        impl["archive"](session_key, payload)
        failed += 1 if detail_errors else 0
        stored += 1
    return {
        "season": int(season),
        "sessions": len(sessions),
        "stored": stored,
        "skipped": skipped,
        "failed": failed,
        "errors": len(errors),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill F1 history sessions into the local SQLite store.")
    parser.add_argument("--season", action="append", help="Season year, e.g. 2025")
    parser.add_argument(
        "--recent",
        type=int,
        default=2,
        help="When --season is omitted, backfill the last N seasons ending with the current year (default: 2)",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite sessions that already exist in the local history store")
    parser.add_argument("--limit", type=int, default=0, help="Optional cap for quick backfill tests")
    args = parser.parse_args()

    seasons = list(args.season or [])
    if not seasons:
        now_year = datetime.now(timezone.utc).year
        recent = max(1, int(args.recent or 2))
        seasons = [str(now_year - offset) for offset in range(recent - 1, -1, -1)]

    summaries = []
    for season in seasons:
        summary = backfill_season(str(season), force=bool(args.force), limit=(args.limit or None))
        summaries.append(summary)
        print(
            f"[f1-history] season {summary['season']}: sessions={summary['sessions']} "
            f"stored={summary['stored']} skipped={summary['skipped']} failed={summary['failed']} errors={summary['errors']}"
        )

    total_stored = sum(int(item["stored"]) for item in summaries)
    total_skipped = sum(int(item["skipped"]) for item in summaries)
    total_failed = sum(int(item["failed"]) for item in summaries)
    print(f"[f1-history] done: stored={total_stored} skipped={total_skipped} failed={total_failed}")


if __name__ == "__main__":
    main()
