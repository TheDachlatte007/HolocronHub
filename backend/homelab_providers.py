"""Small, read-only adapters for the Homelab command center.

Providers are optional. They return normalized snapshots and never raise into
the request handler, so one unavailable service cannot break the dashboard.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


_STATUS_ORDER = {"unknown": 0, "healthy": 1, "degraded": 2, "warning": 3, "critical": 4}
_CACHE_TTL_SECONDS = 30
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _env(name: str) -> str:
    return os.getenv(name, "").strip()


def _url(name: str) -> str:
    return _env(name).rstrip("/")


def _request(method: str, url: str, *, headers: dict[str, str] | None = None,
             auth: tuple[str, str] | None = None, timeout: float = 3.0,
             **kwargs: Any) -> requests.Response:
    response = requests.request(method, url, headers=headers, auth=auth, timeout=timeout, **kwargs)
    response.raise_for_status()
    return response


def _empty_snapshot(provider: str) -> dict[str, Any]:
    return {
        "provider": provider,
        "status": "unknown",
        "checked_at": _now(),
        "stale": False,
        "services": [],
        "metrics": {},
        "storage": [],
        "alerts": [],
        "errors": [],
    }


def _error_snapshot(provider: str, error: Exception) -> dict[str, Any]:
    snapshot = _empty_snapshot(provider)
    snapshot["status"] = "warning"
    snapshot["errors"] = [f"{type(error).__name__}: {str(error)[:240]}"]
    return snapshot


def _parse_prometheus(text: str) -> list[tuple[str, dict[str, str], float]]:
    samples: list[tuple[str, dict[str, str], float]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+0-9.eE]+)", line)
        if not match:
            continue
        labels = {key: bytes(value, "utf-8").decode("unicode_escape") for key, value in _LABEL_RE.findall(match.group(2) or "")}
        try:
            value = float(match.group(3))
        except ValueError:
            continue
        samples.append((match.group(1), labels, value))
    return samples


def _uptime_kuma() -> dict[str, Any] | None:
    base = _url("UPTIME_KUMA_URL")
    if not base:
        return None
    snapshot = _empty_snapshot("uptime_kuma")
    headers: dict[str, str] = {"Accept": "text/plain"}
    api_key = _env("UPTIME_KUMA_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["X-API-Key"] = api_key
    auth = None
    username = _env("UPTIME_KUMA_USERNAME")
    password = _env("UPTIME_KUMA_PASSWORD")
    if username:
        auth = (username, password)
    response = _request("GET", f"{base}/metrics", headers=headers, auth=auth)
    monitors: dict[str, dict[str, Any]] = {}
    for metric, labels, value in _parse_prometheus(response.text):
        monitor_key = labels.get("monitor_name") or labels.get("monitor_id") or labels.get("monitor_url")
        if not monitor_key:
            continue
        item = monitors.setdefault(monitor_key, {
            "id": labels.get("monitor_id") or monitor_key,
            "name": labels.get("monitor_name") or monitor_key,
            "url": labels.get("monitor_url"),
            "type": labels.get("monitor_type"),
            "status": "unknown",
            "latency_ms": None,
            "source": "uptime_kuma",
        })
        if metric in {"monitor_status", "monitor_status_code"}:
            item["status"] = "online" if value == 1 else "offline"
        elif metric in {"monitor_response_time", "monitor_response_time_ms"}:
            item["latency_ms"] = round(value, 2)
    snapshot["services"] = sorted(monitors.values(), key=lambda item: str(item.get("name") or ""))
    statuses = [item["status"] for item in snapshot["services"]]
    if not statuses:
        snapshot["status"] = "unknown"
    elif any(status == "offline" for status in statuses):
        snapshot["status"] = "warning"
    elif any(status == "unknown" for status in statuses):
        snapshot["status"] = "degraded"
    else:
        snapshot["status"] = "healthy"
    return snapshot


def _beszel() -> dict[str, Any] | None:
    base = _url("BESZEL_URL")
    if not base:
        return None
    snapshot = _empty_snapshot("beszel")
    username = _env("BESZEL_USERNAME")
    password = _env("BESZEL_PASSWORD")
    token = _env("BESZEL_API_KEY")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    auth = (username, password) if username else None
    health = _request("GET", f"{base}/health", headers=headers, auth=auth)
    try:
        health_payload = health.json()
    except ValueError:
        health_payload = {"status": "ok"}
    metrics_response = _request("GET", f"{base}/api/metrics", headers=headers, auth=auth)
    metrics = metrics_response.json()
    containers: list[Any] = []
    try:
        containers_response = _request("GET", f"{base}/api/containers", headers=headers, auth=auth)
        containers = containers_response.json()
    except Exception as exc:
        snapshot["errors"].append(f"containers: {str(exc)[:180]}")
    snapshot["metrics"] = {"health": health_payload, "systems": metrics, "containers": containers}
    snapshot["status"] = "healthy" if str(health_payload.get("status", "ok")).lower() in {"ok", "healthy", "up"} else "degraded"
    return snapshot


def _provider_fetchers() -> dict[str, Any]:
    return {"uptime_kuma": _uptime_kuma, "beszel": _beszel}


def _read_cache(cache_file: Path) -> dict[str, Any]:
    try:
        raw = json.loads(cache_file.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_cache(cache_file: Path, cache: dict[str, Any]) -> None:
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="homelab-provider-", suffix=".json", dir=cache_file.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, indent=2, ensure_ascii=False)
        os.replace(temporary, cache_file)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def collect_provider_snapshots(cache_file: Path) -> dict[str, Any]:
    """Fetch configured providers while preserving the last good snapshot."""
    cache = _read_cache(cache_file)
    results: dict[str, Any] = {}
    for provider, fetcher in _provider_fetchers().items():
        if not _url("UPTIME_KUMA_URL" if provider == "uptime_kuma" else "BESZEL_URL"):
            continue
        cached = cache.get(provider) if isinstance(cache.get(provider), dict) else None
        try:
            if cached and time.time() - float(cached.get("stored_at", 0)) < _CACHE_TTL_SECONDS:
                snapshot = dict(cached.get("snapshot") or {})
                snapshot["cached"] = True
            else:
                snapshot = fetcher()
                snapshot = snapshot or _empty_snapshot(provider)
                cache[provider] = {"stored_at": time.time(), "snapshot": snapshot}
                snapshot["cached"] = False
        except Exception as exc:
            snapshot = dict((cached or {}).get("snapshot") or _error_snapshot(provider, exc))
            snapshot["stale"] = bool(cached)
            snapshot["cached"] = bool(cached)
            snapshot.setdefault("errors", []).append(f"stale provider data: {str(exc)[:180]}")
            if not cached:
                snapshot = _error_snapshot(provider, exc)
        results[provider] = snapshot
    if results:
        _write_cache(cache_file, cache)
    return results


def overall_provider_status(snapshots: dict[str, Any]) -> str:
    statuses = [str(snapshot.get("status") or "unknown") for snapshot in snapshots.values()]
    if not statuses:
        return "unknown"
    mapped = {"online": "healthy", "offline": "warning"}
    return max((mapped.get(status, status) for status in statuses), key=lambda status: _STATUS_ORDER.get(status, 0))
