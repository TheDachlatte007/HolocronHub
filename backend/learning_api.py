from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock
from typing import Callable, Literal, TypeVar

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field, StrictInt

try:
    from .learning_store import (
        learning_session,
        learning_summary,
        list_learning_cards,
        record_learning_review,
        sync_seed_cards,
        create_personal_card, update_personal_card, delete_personal_card,
        import_personal_cards, CardConflict, MAX_IMPORT_BYTES,
    )
except ImportError:
    from learning_store import (
        learning_session,
        learning_summary,
        list_learning_cards,
        record_learning_review,
        sync_seed_cards,
        create_personal_card, update_personal_card, delete_personal_card,
        import_personal_cards, CardConflict, MAX_IMPORT_BYTES,
    )


CardStatus = Literal["", "new", "due", "scheduled", "mastered"]
Result = TypeVar("Result")


class LearningReview(BaseModel):
    card_id: str = Field(strict=True, min_length=1, pattern=r"\S")
    rating: StrictInt = Field(ge=1, le=4)


class LearningImport(BaseModel):
    format: Literal["json", "csv"]
    content: str = Field(strict=True, min_length=1, max_length=MAX_IMPORT_BYTES)


def create_learning_router(db_path: Path, seed_path: Path) -> APIRouter:
    """Create the learning API with injectable persistent and seed paths."""
    router = APIRouter(prefix="/api/learning", tags=["learning"])
    db_path, seed_path = Path(db_path), Path(seed_path)
    initialization_lock = Lock()
    initialized = False

    def call_store(operation: Callable[[], Result]) -> Result:
        nonlocal initialized
        try:
            # Startup normally seeds the runtime DB; lazy initialization also
            # supports isolated apps and retries after temporary seed failures.
            with initialization_lock:
                if not initialized or not db_path.is_file():
                    try:
                        sync_seed_cards(db_path, seed_path)
                    except ValueError as exc:
                        raise HTTPException(503, "Learning store unavailable") from exc
                    initialized = True
            return operation()
        except KeyError as exc:
            raise HTTPException(404, "Unknown learning card") from exc
        except PermissionError as exc:
            raise HTTPException(403, str(exc)) from exc
        except CardConflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        except (sqlite3.Error, OSError) as exc:
            raise HTTPException(503, "Learning store unavailable") from exc

    @router.get("/summary")
    def summary():
        return call_store(lambda: learning_summary(db_path))

    @router.get("/session")
    def session(
        limit: int = Query(default=20, ge=1, le=100),
        category: str | None = Query(default=None, max_length=120),
        deck: str | None = Query(default=None, max_length=120),
    ):
        return call_store(lambda: learning_session(db_path, limit=limit, category=category or None, deck=deck or None))

    @router.get("/cards")
    def cards(
        query: str = "",
        category: str | None = Query(default=None, max_length=120),
        deck: str | None = Query(default=None, max_length=120),
        status: CardStatus | None = None,
    ):
        return call_store(lambda: list_learning_cards(
            db_path, query=query, category=category or None, deck=deck or None, status=status or None,
        ))

    @router.post("/cards", status_code=201)
    def create_card(card: dict = Body(...)):
        return call_store(lambda: create_personal_card(db_path, card))

    @router.patch("/cards/{card_id}")
    def update_card(card_id: str, changes: dict = Body(...)):
        return call_store(lambda: update_personal_card(db_path, card_id, changes))

    @router.delete("/cards/{card_id}")
    def delete_card(card_id: str):
        return call_store(lambda: delete_personal_card(db_path, card_id))

    @router.post("/import/preview")
    def preview_import(payload: LearningImport):
        return call_store(lambda: import_personal_cards(db_path, payload.format, payload.content, preview=True))

    @router.post("/import")
    def import_cards(payload: LearningImport):
        return call_store(lambda: import_personal_cards(db_path, payload.format, payload.content))

    @router.post("/reviews")
    def reviews(review: LearningReview):
        return call_store(lambda: record_learning_review(db_path, review.card_id, review.rating))

    return router
