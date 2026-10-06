from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock
from typing import Callable, Literal, TypeVar

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, StrictInt

try:
    from .learning_store import (
        learning_session,
        learning_summary,
        list_learning_cards,
        record_learning_review,
        sync_seed_cards,
    )
except ImportError:
    from learning_store import (
        learning_session,
        learning_summary,
        list_learning_cards,
        record_learning_review,
        sync_seed_cards,
    )


Category = Literal[
    "", "Measurement & Data", "Engineering Practice",
    "Sustainable Systems", "Academic Communication",
]
CardStatus = Literal["", "new", "due", "scheduled", "mastered"]
Result = TypeVar("Result")


class LearningReview(BaseModel):
    card_id: str = Field(strict=True, min_length=1, pattern=r"\S")
    rating: StrictInt = Field(ge=1, le=4)


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
        category: Category | None = None,
    ):
        return call_store(lambda: learning_session(db_path, limit=limit, category=category or None))

    @router.get("/cards")
    def cards(
        query: str = "",
        category: Category | None = None,
        status: CardStatus | None = None,
    ):
        return call_store(lambda: list_learning_cards(
            db_path, query=query, category=category or None, status=status or None,
        ))

    @router.post("/reviews")
    def reviews(review: LearningReview):
        return call_store(lambda: record_learning_review(db_path, review.card_id, review.rating))

    return router
