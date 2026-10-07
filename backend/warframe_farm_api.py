"""Opt-in router: caller injects a mounted DB path and owns backup integration."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Callable, TypeVar

from fastapi import APIRouter, HTTPException, Path as ApiPath, Query

try:
    from .warframe_farm_store import DropCreate, FarmConflict, FarmJournalStore, SessionCreate, SessionUpdate
except ImportError:
    from warframe_farm_store import DropCreate, FarmConflict, FarmJournalStore, SessionCreate, SessionUpdate


Result = TypeVar("Result")


def create_farm_router(db_path: Path | str) -> APIRouter:
    """No disk access until a valid request. No main app or external services imported.

    GET sessions: {sessions, active_session, summary, as_of, limit, offset, has_more}.
    Mutations return a complete session, including drops and calculated totals.
    All timestamps are UTC ISO 8601; supplied timestamps need a timezone and may
    not be future-dated. Unknown IDs: 404, active conflict: 409, invalid: 422,
    unavailable disk/SQLite: 503. Use same-origin authentication from the caller.
    """
    router = APIRouter(prefix="/api/warframe/farm-journal", tags=["warframe-farm-journal"])
    store = FarmJournalStore(db_path)

    def call(operation: Callable[[], Result]) -> Result:
        try:
            return operation()
        except FarmConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(404, "Unknown farm session") from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except (sqlite3.Error, OSError) as exc:
            raise HTTPException(503, "Farm journal storage unavailable") from exc

    @router.get("/sessions")
    def sessions(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0, le=1000000)):
        return call(lambda: store.list_sessions(limit=limit, offset=offset))

    @router.post("/sessions", status_code=201)
    def start(payload: SessionCreate):
        return call(lambda: store.create_session(payload.model_dump(exclude_unset=True)))

    @router.patch("/sessions/{session_id}")
    def update(payload: SessionUpdate, session_id: int = ApiPath(ge=1, le=9223372036854775807)):
        return call(lambda: store.update_session(session_id, payload.model_dump(exclude_unset=True)))

    @router.post("/sessions/{session_id}/drops", status_code=201)
    def drop(payload: DropCreate, session_id: int = ApiPath(ge=1, le=9223372036854775807)):
        return call(lambda: store.add_drop(session_id, payload.model_dump(exclude_unset=True)))

    return router
