"""Optional read-only Jellyfin dashboard inside the hub's trusted-user boundary.

Integration: app.include_router(create_jellyfin_router(config_getter, cache_path)).
The getter returns jellyfin_enabled (bool), jellyfin_url (server base URL, with
optional mount prefix), jellyfin_api_key, and jellyfin_user_id (Jellyfin UUID).
Keep the JSON cache outside the frontend/static tree. No settings are registered.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Callable
from urllib.parse import quote, quote_plus, urlsplit, urlunsplit

import requests
from fastapi import APIRouter, HTTPException, Response


_API = "/api/dashboard/jellyfin"
_TTL = 300
_MAX_ITEMS = 6
_MAX_BYTES = 1024 * 1024
_TIMEOUT = (2, 4)
_GUID = re.compile(r"(?:[0-9a-fA-F]{32}|[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})\Z")
_NO_STORE = {"Cache-Control": "no-store"}


def _item_id(value) -> str | None:
    return value.lower().replace("-", "") if isinstance(value, str) and _GUID.fullmatch(value) else None


@dataclass(frozen=True)
class _Config:
    url: str
    user: str
    key: str = field(repr=False)

    @property
    def scope(self) -> str:
        # Bind disk data to both the user and credentials without storing either.
        return hashlib.sha256(json.dumps([self.url, self.user, self.key]).encode()).hexdigest()


def _configuration(getter: Callable[[], dict]) -> tuple[str, _Config | None]:
    try:
        raw = getter()
        if not isinstance(raw, dict):
            return "unconfigured", None
        if raw.get("jellyfin_enabled") is not True:
            return "disabled", None
        url, key, user = (raw.get(name) for name in ("jellyfin_url", "jellyfin_api_key", "jellyfin_user_id"))
        if not isinstance(url, str) or not isinstance(key, str) or not _item_id(user):
            return "unconfigured", None
        if not key or len(key) > 1024 or any(ord(c) < 33 or ord(c) > 126 for c in key):
            return "unconfigured", None
        if any(ord(c) <= 32 for c in url) or "\\" in url or key in url or key in user:
            return "unconfigured", None
        parsed = urlsplit(url)
        if (parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or not re.fullmatch(r"[A-Za-z0-9.\-:\[\]]+(?::[0-9]+)?", parsed.netloc)
                or not re.fullmatch(r"[A-Za-z0-9/._~\-]*", parsed.path)
                or any(part in {".", ".."} for part in parsed.path.split("/"))):
            return "unconfigured", None
        parsed.port  # Validate the port before anything can leave the server.
        base = urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))
        return "configured", _Config(base, _item_id(user), key)
    except Exception:
        # Configuration and transport exceptions can contain a credential.
        return "unconfigured", None


def _text(value, key: str, limit=200) -> str:
    if not isinstance(value, str):
        return ""
    for secret in {key, quote(key, safe=""), quote_plus(key)}:
        value = value.replace(secret, "[redacted]")
    return " ".join(value.split())[:limit]


def _number(value, maximum=10**12) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    try:
        return min(maximum, max(0.0, float(value))) if math.isfinite(value) else 0.0
    except (OverflowError, ValueError):
        return 0.0


def _items(rows: list, config: _Config, persisted=False) -> list[dict]:
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        item_id = _item_id(row.get("id" if persisted else "Id"))
        if not item_id or item_id in seen or config.key in item_id:
            continue
        if persisted:
            title = _text(row.get("title"), config.key) or "Untitled item"
            summary = _text(row.get("summary"), config.key, 300)
            position = _number(row.get("position_seconds"))
            duration = _number(row.get("duration_seconds"))
            progress = _number(row.get("progress_percent"), 100)
            has_image = row.get("has_image") is True
        else:
            name = _text(row.get("Name"), config.key) or "Untitled item"
            series = _text(row.get("SeriesName"), config.key)
            episode = row.get("Type") == "Episode"
            title = series if episode and series else name
            season, index = row.get("ParentIndexNumber"), row.get("IndexNumber")
            parts = []
            if episode and isinstance(season, int) and not isinstance(season, bool) and 0 <= season <= 999:
                parts.append(f"S{season:02d}")
            if episode and isinstance(index, int) and not isinstance(index, bool) and 0 <= index <= 9999:
                parts.append(f"E{index:02d}")
            summary = f"{''.join(parts)} - {name}" if parts else (name if title != name else "")
            user_data = row.get("UserData") if isinstance(row.get("UserData"), dict) else {}
            position = _number(user_data.get("PlaybackPositionTicks"), 10**18) / 10_000_000
            duration = _number(row.get("RunTimeTicks"), 10**18) / 10_000_000
            progress = min(100.0, position / duration * 100) if duration else _number(user_data.get("PlayedPercentage"), 100)
            tags = row.get("ImageTags")
            has_image = isinstance(tags, dict) and bool(tags.get("Primary"))
        if duration:
            position = min(position, duration)
        result.append({"id": item_id, "title": title, "summary": summary,
                       "position_seconds": round(position, 1), "duration_seconds": round(duration, 1),
                       "progress_percent": round(progress, 1), "has_image": has_image,
                       "web_url": f"{config.url}/web/index.html#!/details?id={item_id}",
                       "image_url": f"{_API}/items/{item_id}/thumbnail" if has_image else None})
        seen.add(item_id)
        if len(result) == _MAX_ITEMS:
            break
    return result


def _read_remote(config: _Config, path: str, params: dict) -> tuple[bytes, str]:
    deadline = time.monotonic() + 8
    # Never follow even same-host redirects: auth headers must not travel elsewhere.
    with requests.get(f"{config.url}/{path}", params=params, headers={"X-Emby-Token": config.key},
                      timeout=_TIMEOUT, allow_redirects=False, stream=True) as response:
        if response.status_code != 200:
            raise ValueError("Jellyfin unavailable")
        size, chunks = 0, []
        for chunk in response.iter_content(64 * 1024):
            size += len(chunk)
            if size > _MAX_BYTES or time.monotonic() > deadline:
                raise ValueError("Jellyfin response exceeds bounds")
            chunks.append(chunk)
        return b"".join(chunks), response.headers.get("Content-Type", "").split(";")[0].lower()


def _atomic_store(path: Path, record: dict) -> None:
    temporary = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
            temporary = handle.name
            json.dump(record, handle, ensure_ascii=True, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except (OSError, ValueError):
        # Last-good memory remains useful if the filesystem is temporarily full.
        pass
    finally:
        if temporary:
            try:
                Path(temporary).unlink(missing_ok=True)
            except OSError:
                pass


def create_jellyfin_router(config_getter: Callable[[], dict], cache_path: str | Path) -> APIRouter:
    """Export GET dashboard (?force=true) and allowlisted GET item thumbnails.

    A forced request bypasses fresh data, not the five-minute failure cooldown.
    One router instance deduplicates concurrent requests, including forced ones.
    Use one router/cache file per app process; no cross-process locking is implied.
    """
    router = APIRouter(prefix=_API, tags=["jellyfin-dashboard"])
    cache_path = Path(cache_path)
    lock = Lock()
    scope = None
    revision = 0
    record = {"items": None, "updated_at": None, "failed_at": None}

    def activate(config: _Config) -> None:
        nonlocal scope, record
        if scope == config.scope:
            return
        scope = config.scope
        record = {"items": None, "updated_at": None, "failed_at": None}
        try:
            if cache_path.stat().st_size > _MAX_BYTES:
                return
            saved = json.loads(cache_path.read_text(encoding="utf-8"))
            if not isinstance(saved, dict) or saved.get("version") != 1 or saved.get("scope") != scope:
                return
            now = time.time()
            updated, failed = saved.get("updated_at"), saved.get("failed_at")
            if isinstance(updated, (int, float)) and not isinstance(updated, bool) and 0 < updated <= now:
                if isinstance(saved.get("items"), list):
                    record["items"] = _items(saved["items"], config, persisted=True)
                    record["updated_at"] = updated
            if isinstance(failed, (int, float)) and not isinstance(failed, bool) and 0 < failed <= now:
                record["failed_at"] = failed
        except (OSError, ValueError, RecursionError):
            pass

    def persist() -> None:
        _atomic_store(cache_path, {"version": 1, "scope": scope, **record})

    def snapshot(config: _Config, cached: bool) -> dict:
        now = time.time()
        failed = record["failed_at"]
        retry = max(0, math.ceil(_TTL - (now - failed))) if failed is not None else 0
        available = record["items"] is not None
        stale = available and (failed is not None or now - record["updated_at"] >= _TTL)
        state = "stale" if stale else ("ready" if record["items"] else "empty") if available else "unavailable"
        messages = {"ready": "Continue watching in Jellyfin.", "empty": "Nothing to continue watching.",
                    "stale": "Jellyfin is unavailable. Showing the last saved snapshot.",
                    "unavailable": "Jellyfin is unavailable. Check the server and credentials in Settings."}
        return {"state": state, "enabled": True, "configured": True, "message": messages[state],
                "items": record["items"] or [], "cached": cached, "stale": stale,
                "updated_at": record["updated_at"], "retry_after_seconds": retry,
                "settings_url": "#settings", "web_url": f"{config.url}/web/index.html"}

    def inactive(state: str) -> dict:
        return {"state": state, "enabled": state != "disabled", "configured": False,
                "message": "Jellyfin is disabled. Enable it in Settings." if state == "disabled"
                else "Configure a Jellyfin server URL, API key and user ID in Settings.",
                "items": [], "cached": False, "stale": False, "updated_at": None,
                "retry_after_seconds": 0, "settings_url": "#settings", "web_url": None}

    @router.get("")
    def dashboard(response: Response, force: bool = False):
        nonlocal revision
        response.headers.update(_NO_STORE)
        state, config = _configuration(config_getter)
        if config is None:
            return inactive(state)
        observed_revision = revision
        with lock:
            # Re-read after waiting so an in-flight configuration change cannot leak old data.
            state, config = _configuration(config_getter)
            if config is None:
                return inactive(state)
            activate(config)
            now = time.time()
            failed, updated = record["failed_at"], record["updated_at"]
            cooling_down = failed is not None and now - failed < _TTL
            fresh = updated is not None and now - updated < _TTL
            if cooling_down or (fresh and not force) or revision != observed_revision:
                return snapshot(config, cached=True)
            try:
                # Legacy route exists in official 10.8 and is retained in 10.10+.
                body, _ = _read_remote(config, f"Users/{config.user}/Items/Resume", {
                    "Limit": _MAX_ITEMS, "MediaTypes": "Video", "EnableUserData": "true",
                    "EnableImages": "true", "EnableImageTypes": "Primary", "ImageTypeLimit": 1,
                    "EnableTotalRecordCount": "false",
                })
                payload = json.loads(body)
                if not isinstance(payload, dict) or not isinstance(payload.get("Items"), list):
                    raise ValueError("Invalid resume response")
                record.update(items=_items(payload["Items"], config), updated_at=time.time(), failed_at=None)
            except Exception:
                record["failed_at"] = time.time()
            revision += 1
            current_state, current_config = _configuration(config_getter)
            if current_config is None:
                return inactive(current_state)
            if current_config.scope != config.scope:
                activate(current_config)
                return snapshot(current_config, cached=True)
            persist()
            return snapshot(config, cached=False)

    @router.get("/items/{item_id}/thumbnail")
    def thumbnail(item_id: str):
        validated = _item_id(item_id)
        state, config = _configuration(config_getter)
        if not validated or config is None or config.key in validated:
            raise HTTPException(404, "Thumbnail unavailable", headers=_NO_STORE)
        with lock:
            state, config = _configuration(config_getter)
            if config is None:
                raise HTTPException(404, "Thumbnail unavailable", headers=_NO_STORE)
            activate(config)
            if not any(item["id"] == validated and item["has_image"] for item in record["items"] or []):
                raise HTTPException(404, "Thumbnail unavailable", headers=_NO_STORE)
            try:
                body, mime = _read_remote(config, f"Items/{validated}/Images/Primary", {
                    "MaxWidth": 240, "MaxHeight": 360, "Quality": 80, "Format": "Jpg",
                })
                valid_image = ((mime == "image/jpeg" and body.startswith(b"\xff\xd8\xff"))
                               or (mime == "image/png" and body.startswith(b"\x89PNG\r\n\x1a\n"))
                               or (mime == "image/webp" and body.startswith(b"RIFF") and body[8:12] == b"WEBP"))
                if not valid_image or config.key.encode() in body:
                    raise ValueError("Invalid thumbnail")
            except Exception:
                raise HTTPException(502, "Thumbnail unavailable", headers=_NO_STORE) from None
            _, current_config = _configuration(config_getter)
            if current_config is None or current_config.scope != config.scope:
                raise HTTPException(404, "Thumbnail unavailable", headers=_NO_STORE)
        return Response(body, media_type=mime, headers={**_NO_STORE, "X-Content-Type-Options": "nosniff"})

    return router
