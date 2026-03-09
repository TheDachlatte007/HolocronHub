from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def to_iso(dt_struct) -> str:
    try:
        if dt_struct is None:
            return datetime.now(timezone.utc).isoformat()
        return datetime(*dt_struct[:6], tzinfo=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _to_iso_from_unix(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def normalize_rss_entry(source: dict[str, Any], entry: Any) -> dict[str, Any]:
    title = getattr(entry, "title", None) or entry.get("title", "Untitled")
    link = getattr(entry, "link", None) or entry.get("link", "")
    summary = getattr(entry, "summary", None) or entry.get("summary", "")
    published_parsed = getattr(entry, "published_parsed", None) or entry.get("published_parsed")

    tags = list(source.get("tags", []))
    entry_tags = []
    for t in (getattr(entry, "tags", None) or entry.get("tags", []) or []):
        term = getattr(t, "term", None) or (t.get("term") if isinstance(t, dict) else None)
        if term:
            entry_tags.append(term)

    tags = sorted(set(tags + entry_tags))

    score = round(float(source.get("trust_weight", 0.5)) * 100, 2)

    return {
        "id": f"{source.get('id','src')}::{hash((title, link))}",
        "title": title,
        "url": link,
        "summary": summary,
        "category": source.get("category", "General"),
        "source_id": source.get("id"),
        "source_name": source.get("name"),
        "provider": source.get("provider", source.get("name", "unknown")),
        "tags": tags,
        "published_at": to_iso(published_parsed),
        "score": score,
    }


def normalize_api_entry(source: dict[str, Any], entry: dict[str, Any], provider: str) -> dict[str, Any]:
    # marketaux
    if provider == "marketaux":
        title = entry.get("title", "Untitled")
        link = entry.get("url", "")
        summary = entry.get("description", "")
        published = entry.get("published_at") or datetime.now(timezone.utc).isoformat()
        tags = sorted(set(list(source.get("tags", [])) + [str(x) for x in entry.get("entities", []) if isinstance(x, str)]))
        return {
            "id": f"{source.get('id','src')}::{hash((title, link))}",
            "title": title,
            "url": link,
            "summary": summary,
            "category": source.get("category", "General"),
            "source_id": source.get("id"),
            "source_name": source.get("name"),
            "provider": entry.get("source") or source.get("name", "marketaux"),
            "tags": tags,
            "published_at": published,
            "score": round(float(source.get("trust_weight", 0.5)) * 100, 2),
        }

    # finnhub
    if provider == "finnhub":
        title = entry.get("headline", "Untitled")
        link = entry.get("url", "")
        summary = entry.get("summary", "")
        tags = sorted(set(list(source.get("tags", [])) + [x.strip() for x in str(entry.get("related", "")).split(",") if x.strip()]))
        return {
            "id": f"{source.get('id','src')}::{hash((title, link))}",
            "title": title,
            "url": link,
            "summary": summary,
            "category": source.get("category", "General"),
            "source_id": source.get("id"),
            "source_name": source.get("name"),
            "provider": entry.get("source") or source.get("name", "finnhub"),
            "tags": tags,
            "published_at": _to_iso_from_unix(entry.get("datetime")),
            "score": round(float(source.get("trust_weight", 0.5)) * 100, 2),
        }

    # alphavantage
    title = entry.get("title", "Untitled")
    link = entry.get("url", "")
    summary = entry.get("summary", "")
    topics = [t.get("topic", "") for t in entry.get("topics", []) if isinstance(t, dict)]
    tags = sorted(set(list(source.get("tags", [])) + [t for t in topics if t]))

    return {
        "id": f"{source.get('id','src')}::{hash((title, link))}",
        "title": title,
        "url": link,
        "summary": summary,
        "category": source.get("category", "General"),
        "source_id": source.get("id"),
        "source_name": source.get("name"),
        "provider": source.get("name", "alphavantage"),
        "tags": tags,
        "published_at": entry.get("time_published", datetime.now(timezone.utc).isoformat()),
        "score": round(float(source.get("trust_weight", 0.5)) * 100, 2),
    }
