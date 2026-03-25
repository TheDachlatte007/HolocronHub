from __future__ import annotations

import json
import math
import os
import re
import shutil
import socket
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import quote, quote_plus, urlparse
from statistics import median
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field

try:
    from .f1_history_store import (
        ensure_f1_history_db,
        f1_history_summary as load_f1_history_summary,
        get_f1_history_session,
        search_f1_history_sessions,
        upsert_f1_history_session,
    )
except Exception:
    from f1_history_store import (
        ensure_f1_history_db,
        f1_history_summary as load_f1_history_summary,
        get_f1_history_session,
        search_f1_history_sessions,
        upsert_f1_history_session,
    )

try:
    from .tldr_store import (
        ensure_tldr_db,
        get_tldr_issue,
        list_tldr_issues,
        mark_tldr_issue_read,
        tldr_summary as load_tldr_summary,
        upsert_tldr_issue,
    )
except Exception:
    from tldr_store import (
        ensure_tldr_db,
        get_tldr_issue,
        list_tldr_issues,
        mark_tldr_issue_read,
        tldr_summary as load_tldr_summary,
        upsert_tldr_issue,
    )

try:
    from .tldr_gmail import build_gmail_auth_url, exchange_gmail_auth_code, fetch_tldr_messages, gmail_profile
except Exception:
    from tldr_gmail import build_gmail_auth_url, exchange_gmail_auth_code, fetch_tldr_messages, gmail_profile

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "tools.json"
SAMPLE_FILE = BASE_DIR / "data" / "tools.sample.json"
SEED_FILE = BASE_DIR / "backend" / "tools.seed.json"
SOURCES_FILE = BASE_DIR / "data" / "sources.json"
FEED_FILE = BASE_DIR / "data" / "feed_items.json"
SAVED_FILE = BASE_DIR / "data" / "saved_items.json"
WARFRAME_WATCHLIST_FILE = BASE_DIR / "data" / "warframe_watchlist.json"
WARFRAME_MARKET_HISTORY_FILE = BASE_DIR / "data" / "warframe_market_history.json"
SCHEDULE_FILE = BASE_DIR / "data" / "schedule.json"
SETTINGS_FILE = BASE_DIR / "data" / "settings.json"
LAST_GOOD_FILE = BASE_DIR / "data" / "last_good_cache.json"
F1_SESSION_SNAPSHOTS_FILE = BASE_DIR / "data" / "f1_session_snapshots.json"
F1_SECONDARY_INGEST_FILE = BASE_DIR / "data" / "f1_secondary_ingest.json"
F1_SESSION_ARCHIVE_FILE = BASE_DIR / "data" / "f1_session_archive.json"
F1_HISTORY_DB_FILE = BASE_DIR / "data" / "f1_history.db"
TLDR_DB_FILE = BASE_DIR / "data" / "tldr_issues.db"
TLDR_GMAIL_TOKEN_FILE = BASE_DIR / "data" / "tldr_gmail_token.json"
TLDR_GMAIL_CLIENT_SECRET_FILE = BASE_DIR / "backend" / "google_client_secret.json"
FRONTEND_INDEX = BASE_DIR / "frontend" / "index.html"


# ── models ────────────────────────────────────────────────────────────────────

class Tool(BaseModel):
    id: str
    name: str
    category: str
    provider: str
    link: str
    icon_url: Optional[str] = None
    local_or_cloud: str
    auth_type: str
    cost_hint: str
    notes: Optional[str] = ""
    tags: List[str] = Field(default_factory=list)
    rating: Optional[int] = None        # 1-5, personal score
    last_used: Optional[str] = None     # ISO timestamp
    usage_count: int = 0
    group: Optional[str] = ""
    favorite: bool = False
    pinned: bool = False
    host: Optional[str] = ""
    port: Optional[int] = None
    environment: Optional[str] = ""
    status: str = "unknown"
    status_checked_at: Optional[str] = None
    service_kind: Optional[str] = ""
    links: List[dict[str, str]] = Field(default_factory=list)
    ai_tasks: List[str] = Field(default_factory=list)
    featured_rank: Optional[int] = None
    short_hint: Optional[str] = ""


class AgentStatus(BaseModel):
    name: str
    status: str
    current_model: Optional[str] = None
    last_run: Optional[str] = None


class AppStatus(BaseModel):
    app: str
    mode: str
    tools_count: int
    agents: List[AgentStatus]


class F1SecondaryIngestPayload(BaseModel):
    session_key: str
    session: dict[str, Any]
    live: bool = False
    drivers_count: int = 0
    leaderboard: List[dict[str, Any]] = Field(default_factory=list)
    race_control: List[dict[str, Any]] = Field(default_factory=list)
    weather: dict[str, Any] = Field(default_factory=dict)
    source: str = "secondary_ingest"
    data_as_of: Optional[str] = None


# ── app + state ───────────────────────────────────────────────────────────────

app = FastAPI(title="HolocronHub API", version="0.2.0")

_ingest_state: dict = {"running": False, "last_result": None, "started_at": None}
_warframe_market_history_lock = Lock()
_last_good_lock = Lock()
_f1_snapshot_lock = Lock()
_f1_archive_lock = Lock()

_PROVIDER_BREAKERS: dict[str, dict[str, Any]] = {
    "openf1": {"fail_count": 0, "open_until": 0.0, "last_error": None},
}

_F1_SESSION_SNAPSHOTS: dict[str, list[dict[str, Any]]] = {}
_F1_SECONDARY_INGEST: dict[str, dict[str, Any]] = {}
_F1_SESSION_ARCHIVE: dict[str, dict[str, Any]] = {}
_TLDR_GMAIL_AUTH_STATE: dict[str, Any] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── data helpers ──────────────────────────────────────────────────────────────

def _tool_seed_file() -> Path:
    return SEED_FILE if SEED_FILE.exists() else SAMPLE_FILE


def _ensure_data_file() -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        src = _tool_seed_file() if _tool_seed_file().exists() else None
        DATA_FILE.write_text(src.read_text(encoding="utf-8") if src else "[]", encoding="utf-8")


def _parse_tool_host_port(link: str) -> tuple[str, Optional[int]]:
    try:
        parsed = urlparse(str(link or '').strip())
    except Exception:
        return '', None
    return parsed.hostname or '', parsed.port


def _normalize_tool_status(raw: Any) -> str:
    status = str(raw or 'unknown').strip().lower()
    if status in {'online', 'offline', 'unknown'}:
        return status
    return 'unknown'


def _normalize_tool_links(raw: Any) -> list[dict[str, str]]:
    items = raw if isinstance(raw, list) else []
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        label = str(item.get('label') or '').strip()
        url = str(item.get('url') or '').strip()
        if not label or not url:
            continue
        key = (label.lower(), url)
        if key in seen:
            continue
        seen.add(key)
        normalized.append({'label': label[:40], 'url': url})
    return normalized[:6]


def _infer_tool_group(tool: dict[str, Any]) -> str:
    category = str(tool.get('category') or '').strip().lower()
    if 'home' not in category and 'network' not in category:
        return str(tool.get('group') or '').strip()
    existing = str(tool.get('group') or '').strip()
    if existing:
        return existing
    service_kind = str(tool.get('service_kind') or '').strip().lower()
    tags = {str(tag).strip().lower() for tag in tool.get('tags') or [] if str(tag).strip()}
    name = str(tool.get('name') or '').lower()
    if {'media', 'streaming'} & tags or service_kind == 'media' or 'jellyfin' in name:
        return 'Media'
    if {'monitoring', 'dashboard', 'metrics'} & tags or service_kind in {'monitoring', 'observability'}:
        return 'Observability'
    if {'nas', 'storage', 'backup'} & tags or service_kind == 'storage':
        return 'Storage'
    if {'dns', 'network', 'security'} & tags or service_kind in {'dns', 'network'}:
        return 'Network'
    if {'automation', 'smarthome'} & tags or service_kind in {'automation', 'smarthome'}:
        return 'Automation'
    return 'Infra'


def _normalize_tool_record(raw: dict[str, Any]) -> dict[str, Any]:
    tool = dict(raw or {})
    host, port = _parse_tool_host_port(tool.get('link') or '')
    tags = [str(tag).strip() for tag in (tool.get('tags') or []) if str(tag).strip()]
    ai_tasks = [str(task).strip() for task in (tool.get('ai_tasks') or []) if str(task).strip()]
    rating = tool.get('rating')
    try:
        rating = int(rating) if rating not in (None, '') else None
    except Exception:
        rating = None
    if rating is not None:
        rating = max(1, min(5, rating))
    try:
        usage_count = max(0, int(tool.get('usage_count') or 0))
    except Exception:
        usage_count = 0
    try:
        featured_rank = int(tool.get('featured_rank')) if tool.get('featured_rank') not in (None, '') else None
    except Exception:
        featured_rank = None
    normalized = {
        'id': str(tool.get('id') or '').strip(),
        'name': str(tool.get('name') or '').strip(),
        'category': str(tool.get('category') or '').strip(),
        'provider': str(tool.get('provider') or '').strip(),
        'link': str(tool.get('link') or '').strip(),
        'icon_url': str(tool.get('icon_url') or '').strip() or None,
        'local_or_cloud': str(tool.get('local_or_cloud') or 'unknown').strip(),
        'auth_type': str(tool.get('auth_type') or 'none').strip(),
        'cost_hint': str(tool.get('cost_hint') or 'unknown').strip(),
        'notes': str(tool.get('notes') or '').strip(),
        'tags': tags,
        'rating': rating,
        'last_used': str(tool.get('last_used') or '').strip() or None,
        'usage_count': usage_count,
        'group': str(tool.get('group') or '').strip(),
        'favorite': bool(tool.get('favorite')),
        'pinned': bool(tool.get('pinned')),
        'host': str(tool.get('host') or host or '').strip(),
        'port': tool.get('port') if tool.get('port') not in ('', None) else port,
        'environment': str(tool.get('environment') or '').strip(),
        'status': _normalize_tool_status(tool.get('status')),
        'status_checked_at': str(tool.get('status_checked_at') or '').strip() or None,
        'service_kind': str(tool.get('service_kind') or '').strip(),
        'links': _normalize_tool_links(tool.get('links')),
        'ai_tasks': ai_tasks,
        'featured_rank': featured_rank,
        'short_hint': str(tool.get('short_hint') or '').strip(),
    }
    try:
        normalized['port'] = int(normalized['port']) if normalized['port'] is not None else None
    except Exception:
        normalized['port'] = port
    normalized['group'] = _infer_tool_group(normalized)
    return normalized


def _sync_seed_tools() -> None:
    seed_file = _tool_seed_file()
    if not seed_file.exists() or not DATA_FILE.exists():
        return
    try:
        current_raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        sample_raw = json.loads(seed_file.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(current_raw, list) or not isinstance(sample_raw, list):
        return

    current_items = [_normalize_tool_record(item) for item in current_raw if isinstance(item, dict)]
    sample_items = [_normalize_tool_record(item) for item in sample_raw if isinstance(item, dict)]
    by_id: dict[str, dict[str, Any]] = {item["id"]: dict(item) for item in current_items if item.get("id")}
    changed = False

    for seed in sample_items:
        tool_id = str(seed.get("id") or "").strip()
        if not tool_id:
            continue
        existing = by_id.get(tool_id)
        if not existing:
            by_id[tool_id] = dict(seed)
            changed = True
            continue

        # Backfill only new curation metadata when the user has no value yet.
        if not existing.get("ai_tasks") and seed.get("ai_tasks"):
            existing["ai_tasks"] = list(seed["ai_tasks"])
            changed = True
        if existing.get("featured_rank") in (None, "") and seed.get("featured_rank") not in (None, ""):
            existing["featured_rank"] = seed["featured_rank"]
            changed = True
        if not str(existing.get("short_hint") or "").strip() and str(seed.get("short_hint") or "").strip():
            existing["short_hint"] = seed["short_hint"]
            changed = True
        if not str(existing.get("service_kind") or "").strip() and str(seed.get("service_kind") or "").strip():
            existing["service_kind"] = seed["service_kind"]
            changed = True
        if not existing.get("links") and seed.get("links"):
            existing["links"] = list(seed["links"])
            changed = True
        by_id[tool_id] = existing

    if not changed:
        return

    ordered_ids: list[str] = []
    for item in current_items:
        tool_id = str(item.get("id") or "").strip()
        if tool_id and tool_id not in ordered_ids:
            ordered_ids.append(tool_id)
    for item in sample_items:
        tool_id = str(item.get("id") or "").strip()
        if tool_id and tool_id not in ordered_ids:
            ordered_ids.append(tool_id)

    merged = [by_id[tool_id] for tool_id in ordered_ids if tool_id in by_id]
    _save_tools(merged)


def _load_tools() -> list[dict]:
    _ensure_data_file()
    _sync_seed_tools()
    try:
        data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
        return [_normalize_tool_record(item) for item in data if isinstance(item, dict)]
    except Exception:
        return []


def _save_tools(items: list[dict]) -> None:
    normalized = [_normalize_tool_record(item) for item in items if isinstance(item, dict)]
    DATA_FILE.write_text(json.dumps(normalized, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_sources() -> list[dict]:
    if not SOURCES_FILE.exists():
        return []
    try:
        return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_sources(items: list[dict]) -> None:
    SOURCES_FILE.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_feed_payload() -> dict:
    if not FEED_FILE.exists():
        return {"generated_at": None, "count": 0, "items": [], "errors": []}
    try:
        return json.loads(FEED_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"generated_at": None, "count": 0, "items": [], "errors": []}


def _load_saved() -> list[dict]:
    if not SAVED_FILE.exists():
        return []
    try:
        return json.loads(SAVED_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_saved(items: list[dict]) -> None:
    SAVED_FILE.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_warframe_watchlist() -> list[dict]:
    if not WARFRAME_WATCHLIST_FILE.exists():
        return []
    try:
        data = json.loads(WARFRAME_WATCHLIST_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_warframe_watchlist(items: list[dict]) -> None:
    WARFRAME_WATCHLIST_FILE.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")



def _load_warframe_market_history_store() -> dict[str, Any]:
    if not WARFRAME_MARKET_HISTORY_FILE.exists():
        return {"updated_at": None, "platforms": {}}
    try:
        data = json.loads(WARFRAME_MARKET_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"updated_at": None, "platforms": {}}
    if not isinstance(data, dict):
        return {"updated_at": None, "platforms": {}}
    platforms = data.get("platforms")
    if not isinstance(platforms, dict):
        platforms = {}
    return {"updated_at": data.get("updated_at"), "platforms": platforms}


def _save_warframe_market_history_store(store: dict[str, Any]) -> None:
    WARFRAME_MARKET_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    WARFRAME_MARKET_HISTORY_FILE.write_text(
        json.dumps(store, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _record_warframe_market_snapshot(market: dict[str, Any], platform: str = "pc") -> dict[str, Any]:
    slug = str(market.get("slug") or "").strip()
    if not slug:
        return {}
    price = market.get("last_avg_price")
    if price is None:
        price = market.get("best_sell")
    try:
        price_value = float(price) if price is not None else None
    except Exception:
        price_value = None
    try:
        best_sell = float(market.get("best_sell")) if market.get("best_sell") is not None else None
    except Exception:
        best_sell = None
    try:
        best_buy = float(market.get("best_buy")) if market.get("best_buy") is not None else None
    except Exception:
        best_buy = None
    try:
        volume_total = int(market.get("volume_total") or 0)
    except Exception:
        volume_total = 0
    if price_value is None and best_sell is None and volume_total <= 0:
        return {}

    platform_key = str(platform or "pc").strip().lower()
    captured_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    snapshot = {
        "captured_at": captured_at,
        "price": round(price_value, 2) if price_value is not None else None,
        "best_sell": round(best_sell, 2) if best_sell is not None else None,
        "best_buy": round(best_buy, 2) if best_buy is not None else None,
        "volume_total": volume_total,
        "price_change_pct": market.get("price_change_pct"),
        "live_buyers": int(market.get("buy_count_live") or 0),
        "live_sellers": int(market.get("sell_count_live") or 0),
        "source": str(market.get("snapshot_source") or "unavailable"),
    }

    with _warframe_market_history_lock:
        store = _load_warframe_market_history_store()
        platforms = store.setdefault("platforms", {})
        platform_items = platforms.setdefault(platform_key, {})
        entry = platform_items.setdefault(
            slug,
            {
                "name": market.get("canonical_name") or market.get("item") or slug.replace("_", " "),
                "slug": slug,
                "item": market.get("item") or market.get("canonical_name") or slug.replace("_", " "),
                "snapshots": [],
            },
        )
        entry["name"] = market.get("canonical_name") or entry.get("name") or slug.replace("_", " ")
        entry["item"] = market.get("item") or entry.get("item") or entry.get("name")
        snapshots = entry.setdefault("snapshots", [])
        last = snapshots[-1] if snapshots else None
        if isinstance(last, dict):
            try:
                last_dt = _parse_dt(last.get("captured_at"))
                now_dt = _parse_dt(captured_at)
                recent_age = (now_dt - last_dt).total_seconds() if last_dt and now_dt else None
            except Exception:
                recent_age = None
            if (
                recent_age is not None
                and recent_age < 20 * 60
                and last.get("price") == snapshot.get("price")
                and last.get("best_sell") == snapshot.get("best_sell")
                and last.get("best_buy") == snapshot.get("best_buy")
                and int(last.get("volume_total") or 0) == volume_total
                and str(last.get("source") or "") == snapshot["source"]
            ):
                snapshots[-1] = snapshot
            else:
                snapshots.append(snapshot)
        else:
            snapshots.append(snapshot)
        entry["snapshots"] = snapshots[-240:]
        if len(platform_items) > 160:
            ordered = sorted(
                platform_items.items(),
                key=lambda item: str(((item[1] or {}).get("snapshots") or [{}])[-1].get("captured_at") or ""),
                reverse=True,
            )
            platforms[platform_key] = dict(ordered[:160])
        store["updated_at"] = captured_at
        _save_warframe_market_history_store(store)

    return _summarize_warframe_market_history(slug, platform_key)


def _summarize_warframe_market_history(slug: str, platform: str = "pc") -> dict[str, Any]:
    if not slug:
        return {}
    store = _load_warframe_market_history_store()
    platform_items = ((store.get("platforms") or {}).get(str(platform or "pc").strip().lower()) or {})
    entry = platform_items.get(slug)
    if not isinstance(entry, dict):
        return {}
    snapshots = [snap for snap in (entry.get("snapshots") or []) if isinstance(snap, dict)]
    if not snapshots:
        return {}

    prices = [float(snap["price"]) for snap in snapshots if snap.get("price") is not None]
    volumes = [int(snap.get("volume_total") or 0) for snap in snapshots]
    latest = snapshots[-1]
    latest_dt = _parse_dt(latest.get("captured_at"))
    cutoff_24h = datetime.now(timezone.utc) - timedelta(hours=24)
    recent_24h = [snap for snap in snapshots if (_parse_dt(snap.get("captured_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff_24h]
    baseline = next((snap for snap in recent_24h if snap.get("price") is not None), None)
    if baseline is None:
        baseline = next((snap for snap in snapshots if snap.get("price") is not None), None)

    change_pct_24h = None
    change_abs_24h = None
    try:
        if baseline and baseline.get("price") not in (None, 0) and latest.get("price") is not None:
            change_abs_24h = round(float(latest.get("price")) - float(baseline.get("price")), 2)
            change_pct_24h = round((change_abs_24h / float(baseline.get("price"))) * 100.0, 1)
    except Exception:
        change_pct_24h = None
        change_abs_24h = None

    series = [
        {"captured_at": snap.get("captured_at"), "price": snap.get("price")}
        for snap in snapshots[-24:]
        if snap.get("price") is not None
    ]
    return {
        "samples": len(snapshots),
        "samples_24h": len(recent_24h),
        "first_seen_at": snapshots[0].get("captured_at"),
        "last_seen_at": latest.get("captured_at"),
        "last_price": latest.get("price"),
        "price_low": round(min(prices), 2) if prices else None,
        "price_high": round(max(prices), 2) if prices else None,
        "price_avg": round(sum(prices) / len(prices), 2) if prices else None,
        "change_pct_24h": change_pct_24h,
        "change_abs_24h": change_abs_24h,
        "volume_peak": max(volumes) if volumes else 0,
        "source_mix": list(dict.fromkeys(str(snap.get("source") or "unknown") for snap in snapshots[-12:])),
        "series": series,
        "last_source": latest.get("source"),
        "last_live_buyers": latest.get("live_buyers"),
        "last_live_sellers": latest.get("live_sellers"),
        "updated_at": store.get("updated_at") or latest.get("captured_at"),
        "age_minutes": max(0, int((datetime.now(timezone.utc) - latest_dt).total_seconds() // 60)) if latest_dt else None,
    }

def _coerce_watch_priority(raw: Any) -> int:
    try:
        value = int(raw)
    except Exception:
        value = 3
    return max(1, min(5, value))


def _normalize_tag_list(raw: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, list):
        values = [str(part).strip() for part in raw if str(part).strip()]
    seen: set[str] = set()
    out: list[str] = []
    for tag in values:
        norm = _normalize_warframe_item_name(tag)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(tag[:24])
    return out[:8]


_DEFAULT_SCHEDULE = {"enabled": True, "interval_hours": 2, "run_at_startup": False}


def _load_schedule() -> dict:
    if not SCHEDULE_FILE.exists():
        return dict(_DEFAULT_SCHEDULE)
    try:
        data = json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
        return {**_DEFAULT_SCHEDULE, **data}
    except Exception:
        return dict(_DEFAULT_SCHEDULE)


def _save_schedule(cfg: dict) -> None:
    SCHEDULE_FILE.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


_DEFAULT_SETTINGS = {
    "ux": {
        "language": "EN",
        "default_category": "",
        "show_disabled_sources": False,
    },
    "feed": {
        "refresh_interval_minutes": 15,
        "digest_mode": "daily",
    },
    "models": {
        "openclaw_model": "unknown",
    },
    "api_keys": {
        "marketaux_api_key": "",
        "finnhub_api_key": "",
        "alphavantage_api_key": "",
        "twelvedata_api_key": "",
        "alecaframe_user_hash": "",
        "alecaframe_secret_token": "",
        "alecaframe_public_token": "",
    },
}

_ALLOWED_SETTINGS_PATCH = {
    "ux": {"language", "default_category", "show_disabled_sources"},
    "feed": {"refresh_interval_minutes", "digest_mode"},
    "models": {"openclaw_model"},
    "api_keys": {
        "marketaux_api_key",
        "finnhub_api_key",
        "alphavantage_api_key",
        "twelvedata_api_key",
        "alecaframe_user_hash",
        "alecaframe_secret_token",
        "alecaframe_public_token",
    },
}

_API_KEY_ENV_MAP = {
    "marketaux_api_key": "MARKETAUX_API_KEY",
    "finnhub_api_key": "FINNHUB_API_KEY",
    "alphavantage_api_key": "ALPHAVANTAGE_API_KEY",
    "twelvedata_api_key": "TWELVEDATA_API_KEY",
    "alecaframe_user_hash": "ALECAFRAME_USER_HASH",
    "alecaframe_secret_token": "ALECAFRAME_SECRET_TOKEN",
    "alecaframe_public_token": "ALECAFRAME_PUBLIC_TOKEN",
}

_DEFAULT_MARKET_SYMBOLS = [
    "^GSPC",
    "^IXIC",
    "^DJI",
    "BTC-USD",
    "ETH-USD",
    "EURUSD=X",
    "GC=F",
    "CL=F",
]


_DEFAULT_HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
}

_API_CACHE: dict[str, dict[str, Any]] = {}
_LAST_GOOD: dict[str, dict[str, Any]] = {}
_PREWARM_STATE: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "last_run": None,
    "results": {},
}

_provider_health: dict[str, dict[str, Any]] = {
    "markets": {"status": "unknown", "last_checked": None, "cached": False, "errors": [], "summary": {}, "last_duration_ms": None, "avg_duration_ms": None, "max_duration_ms": None, "sample_count": 0, "slow_count": 0},
    "f1": {"status": "unknown", "last_checked": None, "cached": False, "errors": [], "summary": {}, "last_duration_ms": None, "avg_duration_ms": None, "max_duration_ms": None, "sample_count": 0, "slow_count": 0},
    "warframe": {"status": "unknown", "last_checked": None, "cached": False, "errors": [], "summary": {}, "last_duration_ms": None, "avg_duration_ms": None, "max_duration_ms": None, "sample_count": 0, "slow_count": 0},
}


def _record_provider_health(
    name: str,
    *,
    errors: list[str],
    cached: bool,
    summary: Optional[dict[str, Any]] = None,
    duration_ms: Optional[int] = None,
) -> None:
    state = "ok"
    if errors:
        state = "warn"
    prev = _provider_health.get(name) if isinstance(_provider_health.get(name), dict) else {}
    prev_samples = int(prev.get("sample_count") or 0)
    prev_avg = float(prev.get("avg_duration_ms") or 0) if prev_samples else 0.0
    dur = None if duration_ms is None else max(0, int(duration_ms))
    sample_count = prev_samples
    avg_duration_ms = prev.get("avg_duration_ms")
    max_duration_ms = prev.get("max_duration_ms")
    slow_count = int(prev.get("slow_count") or 0)
    if dur is not None:
        sample_count = prev_samples + 1
        avg_duration_ms = round(((prev_avg * prev_samples) + dur) / sample_count)
        prev_max = int(prev.get("max_duration_ms") or 0)
        max_duration_ms = max(prev_max, dur)
        if dur >= 1200:
            slow_count += 1
    _provider_health[name] = {
        "status": state,
        "last_checked": datetime.now().isoformat(timespec="seconds"),
        "cached": bool(cached),
        "errors": list(errors or [])[:12],
        "summary": dict(summary or {}),
        "last_duration_ms": dur,
        "avg_duration_ms": avg_duration_ms,
        "max_duration_ms": max_duration_ms,
        "sample_count": sample_count,
        "slow_count": slow_count,
    }


def _load_last_good_store() -> dict[str, dict[str, Any]]:
    if not LAST_GOOD_FILE.exists():
        return {}
    try:
        raw = json.loads(LAST_GOOD_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    loaded: dict[str, dict[str, Any]] = {}
    for key, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        value = entry.get("value")
        if not isinstance(value, dict):
            continue
        try:
            ts = float(entry.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        if ts <= 0:
            continue
        loaded[str(key)] = {"ts": ts, "value": value}
    return loaded


def _save_last_good_store() -> None:
    LAST_GOOD_FILE.parent.mkdir(parents=True, exist_ok=True)
    ordered_items = sorted(_LAST_GOOD.items(), key=lambda item: float((item[1] or {}).get("ts") or 0.0), reverse=True)[:96]
    payload = {
        str(key): {
            "ts": float((entry or {}).get("ts") or 0.0),
            "value": deepcopy((entry or {}).get("value") or {}),
        }
        for key, entry in ordered_items
        if isinstance(entry, dict) and isinstance(entry.get("value"), dict)
    }
    LAST_GOOD_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_f1_session_snapshots() -> dict[str, list[dict[str, Any]]]:
    if not F1_SESSION_SNAPSHOTS_FILE.exists():
        return {}
    try:
        raw = json.loads(F1_SESSION_SNAPSHOTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    loaded: dict[str, list[dict[str, Any]]] = {}
    for session_key, entries in raw.items():
        if not isinstance(entries, list):
            continue
        trimmed = []
        for entry in entries[:24]:
            if isinstance(entry, dict):
                trimmed.append(entry)
        if trimmed:
            loaded[str(session_key)] = trimmed
    return loaded


def _save_f1_session_snapshots() -> None:
    F1_SESSION_SNAPSHOTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        str(session_key): [deepcopy(entry) for entry in entries[:24] if isinstance(entry, dict)]
        for session_key, entries in _F1_SESSION_SNAPSHOTS.items()
        if isinstance(entries, list) and entries
    }
    F1_SESSION_SNAPSHOTS_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_f1_secondary_ingest() -> dict[str, dict[str, Any]]:
    if not F1_SECONDARY_INGEST_FILE.exists():
        return {}
    try:
        raw = json.loads(F1_SECONDARY_INGEST_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for session_key, payload in raw.items():
        if isinstance(payload, dict):
            out[str(session_key)] = payload
    return out


def _save_f1_secondary_ingest() -> None:
    F1_SECONDARY_INGEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        str(session_key): deepcopy(entry)
        for session_key, entry in _F1_SECONDARY_INGEST.items()
        if isinstance(entry, dict)
    }
    F1_SECONDARY_INGEST_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_f1_session_archive() -> dict[str, dict[str, Any]]:
    if not F1_SESSION_ARCHIVE_FILE.exists():
        return {}
    try:
        raw = json.loads(F1_SESSION_ARCHIVE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for session_key, payload in raw.items():
        if isinstance(payload, dict):
            out[str(session_key)] = payload
    return out


def _save_f1_session_archive() -> None:
    F1_SESSION_ARCHIVE_FILE.parent.mkdir(parents=True, exist_ok=True)
    ordered_items = sorted(
        _F1_SESSION_ARCHIVE.items(),
        key=lambda item: str((item[1] or {}).get("data_as_of") or (item[1] or {}).get("generated_at") or ""),
        reverse=True,
    )[:128]
    payload = {
        str(session_key): deepcopy(entry)
        for session_key, entry in ordered_items
        if isinstance(entry, dict)
    }
    F1_SESSION_ARCHIVE_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _boot_last_good_store() -> None:
    loaded = _load_last_good_store()
    if not loaded:
        return
    with _last_good_lock:
        _LAST_GOOD.clear()
        _LAST_GOOD.update(loaded)


def _boot_f1_session_snapshots() -> None:
    loaded = _load_f1_session_snapshots()
    if not loaded:
        return
    with _f1_snapshot_lock:
        _F1_SESSION_SNAPSHOTS.clear()
        _F1_SESSION_SNAPSHOTS.update(loaded)


def _boot_f1_secondary_ingest() -> None:
    loaded = _load_f1_secondary_ingest()
    if not loaded:
        return
    _F1_SECONDARY_INGEST.clear()
    _F1_SECONDARY_INGEST.update(loaded)


def _boot_f1_session_archive() -> None:
    loaded = _load_f1_session_archive()
    if not loaded:
        return
    with _f1_archive_lock:
        _F1_SESSION_ARCHIVE.clear()
        _F1_SESSION_ARCHIVE.update(loaded)


def _seed_f1_session_archive_from_last_good() -> None:
    with _last_good_lock:
        items = list(_LAST_GOOD.items())
    changed = False
    for cache_key, entry in items:
        if not str(cache_key).startswith("f1:session:"):
            continue
        value = deepcopy((entry or {}).get("value") or {})
        if not isinstance(value, dict):
            continue
        session_key = str(value.get("session_key") or str(cache_key).split("f1:session:", 1)[-1]).strip()
        if not session_key:
            continue
        archive_entry = _f1_archive_entry(session_key, value)
        if not isinstance(archive_entry, dict):
            continue
        with _f1_archive_lock:
            if session_key in _F1_SESSION_ARCHIVE:
                continue
            _F1_SESSION_ARCHIVE[session_key] = archive_entry
            changed = True
    if changed:
        try:
            _save_f1_session_archive()
        except Exception:
            pass


def _sync_f1_history_db_from_archive() -> None:
    with _f1_archive_lock:
        items = [(str(session_key), deepcopy(entry)) for session_key, entry in _F1_SESSION_ARCHIVE.items() if isinstance(entry, dict)]
    for session_key, entry in items:
        payload = {
            "generated_at": entry.get("generated_at"),
            "data_as_of": entry.get("data_as_of"),
            "session_key": session_key,
            "source": entry.get("source"),
            "session": deepcopy(entry.get("session") or {}),
            "live": bool(entry.get("live")),
            "drivers_count": int(entry.get("drivers_count") or 0),
            "leaderboard": deepcopy(entry.get("leaderboard") or []),
            "race_control": deepcopy(entry.get("race_control") or []),
            "weather": deepcopy(entry.get("weather") or {}),
        }
        try:
            upsert_f1_history_session(F1_HISTORY_DB_FILE, entry, payload)
        except Exception:
            continue


def _record_f1_session_snapshot(session_key: str, payload: dict[str, Any]) -> None:
    if not session_key or not isinstance(payload, dict):
        return
    row = {
        "generated_at": payload.get("generated_at") or datetime.now().isoformat(timespec="seconds"),
        "data_as_of": payload.get("data_as_of") or payload.get("generated_at"),
        "live": bool(payload.get("live")),
        "source": payload.get("source"),
        "leaderboard": deepcopy((payload.get("leaderboard") or [])[:12]),
        "race_control": deepcopy((payload.get("race_control") or [])[:10]),
        "weather": deepcopy(payload.get("weather") or {}),
    }
    with _f1_snapshot_lock:
        entries = list(_F1_SESSION_SNAPSHOTS.get(session_key) or [])
        if entries and str(entries[0].get("generated_at") or "") == str(row.get("generated_at") or ""):
            entries[0] = row
        else:
            entries.insert(0, row)
        _F1_SESSION_SNAPSHOTS[session_key] = entries[:24]
        try:
            _save_f1_session_snapshots()
        except Exception:
            pass


def _get_f1_session_snapshots(session_key: str, *, limit: int = 10) -> list[dict[str, Any]]:
    with _f1_snapshot_lock:
        entries = deepcopy(_F1_SESSION_SNAPSHOTS.get(session_key) or [])
    return [entry for entry in entries[: max(1, min(int(limit or 10), 24))] if isinstance(entry, dict)]


def _set_f1_secondary_session(session_key: str, payload: dict[str, Any]) -> None:
    if not session_key or not isinstance(payload, dict):
        return
    entry = deepcopy(payload)
    entry["session_key"] = session_key
    entry["generated_at"] = entry.get("generated_at") or datetime.now().isoformat(timespec="seconds")
    entry["data_as_of"] = entry.get("data_as_of") or entry.get("generated_at")
    _F1_SECONDARY_INGEST[session_key] = entry
    try:
        _save_f1_secondary_ingest()
    except Exception:
        pass


def _get_f1_secondary_session(session_key: str, *, max_age_seconds: int = 4 * 3600) -> tuple[Optional[dict[str, Any]], int]:
    entry = deepcopy(_F1_SECONDARY_INGEST.get(session_key))
    if not isinstance(entry, dict):
        return None, 0
    ts_raw = entry.get("data_as_of") or entry.get("generated_at")
    dt = _parse_dt(ts_raw)
    if dt is None:
        return None, 0
    age = max(0, int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()))
    if age > max_age_seconds:
        return None, age
    return entry, age


def _normalize_search_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _f1_archive_year(session_key: str, session: dict[str, Any], payload: dict[str, Any]) -> Optional[int]:
    raw_year = session.get("year")
    if isinstance(raw_year, int):
        return raw_year
    if isinstance(raw_year, str) and raw_year.isdigit():
        return int(raw_year)
    match = re.search(r"(20\d{2})", str(session_key or ""))
    if match:
        return int(match.group(1))
    for candidate in (session.get("date_start"), session.get("date_start_local"), payload.get("data_as_of"), payload.get("generated_at")):
        dt = _parse_dt(candidate)
        if dt is not None:
            return dt.year
    return None


def _f1_archive_round(session_key: str, session: dict[str, Any]) -> Optional[int]:
    raw_round = session.get("round")
    if isinstance(raw_round, int):
        return raw_round
    if isinstance(raw_round, str) and raw_round.isdigit():
        return int(raw_round)
    match = re.match(r"fallback:(20\d{2}):(\d+):", str(session_key or ""))
    if match:
        return int(match.group(2))
    return None


def _f1_archive_entry(session_key: str, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not session_key or not isinstance(payload, dict):
        return None
    session = deepcopy(payload.get("session") or {})
    if not isinstance(session, dict):
        session = {}
    leaderboard = [row for row in list(payload.get("leaderboard") or [])[:20] if isinstance(row, dict)]
    race_control = [row for row in list(payload.get("race_control") or [])[:20] if isinstance(row, dict)]
    weather = deepcopy(payload.get("weather") or {})
    year = _f1_archive_year(session_key, session, payload)
    round_number = _f1_archive_round(session_key, session)
    meeting_name = str(
        session.get("meeting_name")
        or session.get("meeting_official_name")
        or session.get("country_name")
        or session.get("location")
        or ""
    ).strip()
    entry = {
        "session_key": session_key,
        "season": str(year) if year else "",
        "generated_at": payload.get("generated_at") or datetime.now().isoformat(timespec="seconds"),
        "data_as_of": payload.get("data_as_of") or payload.get("generated_at") or datetime.now().isoformat(timespec="seconds"),
        "source": str(payload.get("source") or "archive").strip() or "archive",
        "live": bool(payload.get("live")),
        "drivers_count": int(payload.get("drivers_count") or len(leaderboard)),
        "year": year,
        "round": round_number,
        "meeting_name": meeting_name,
        "session_name": str(session.get("session_name") or session.get("session_type") or "").strip(),
        "circuit": str(session.get("circuit_short_name") or session.get("circuit_name") or "").strip(),
        "location": str(session.get("location") or "").strip(),
        "country_name": str(session.get("country_name") or "").strip(),
        "date_start": session.get("date_start"),
        "date_end": session.get("date_end"),
        "session": session,
        "leaderboard": leaderboard,
        "race_control": race_control,
        "weather": weather if isinstance(weather, dict) else {},
    }
    return entry


def _archive_f1_session_payload(session_key: str, payload: dict[str, Any]) -> None:
    entry = _f1_archive_entry(session_key, payload)
    if not isinstance(entry, dict):
        return
    db_payload = {
        "generated_at": payload.get("generated_at") or entry.get("generated_at"),
        "data_as_of": payload.get("data_as_of") or entry.get("data_as_of"),
        "session_key": session_key,
        "source": payload.get("source") or entry.get("source"),
        "session": deepcopy(payload.get("session") or entry.get("session") or {}),
        "live": bool(payload.get("live")),
        "drivers_count": int(payload.get("drivers_count") or entry.get("drivers_count") or 0),
        "leaderboard": deepcopy(payload.get("leaderboard") or entry.get("leaderboard") or []),
        "race_control": deepcopy(payload.get("race_control") or entry.get("race_control") or []),
        "weather": deepcopy(payload.get("weather") or entry.get("weather") or {}),
    }
    with _f1_archive_lock:
        existing = deepcopy(_F1_SESSION_ARCHIVE.get(session_key))
        if isinstance(existing, dict):
            if (
                str(existing.get("data_as_of") or "") == str(entry.get("data_as_of") or "")
                and int(existing.get("drivers_count") or 0) == int(entry.get("drivers_count") or 0)
                and len(existing.get("leaderboard") or []) == len(entry.get("leaderboard") or [])
            ):
                return
        _F1_SESSION_ARCHIVE[session_key] = entry
        try:
            _save_f1_session_archive()
        except Exception:
            pass
    try:
        upsert_f1_history_session(F1_HISTORY_DB_FILE, entry, db_payload)
    except Exception:
        pass


def _get_f1_archived_session(session_key: str) -> Optional[dict[str, Any]]:
    with _f1_archive_lock:
        entry = deepcopy(_F1_SESSION_ARCHIVE.get(session_key))
    if not isinstance(entry, dict):
        db_payload = get_f1_history_session(F1_HISTORY_DB_FILE, session_key)
        if isinstance(db_payload, dict):
            return {
                **db_payload,
                "stale": True,
                "stale_age_seconds": max(0, int((datetime.now(timezone.utc) - (_parse_dt(db_payload.get("data_as_of")) or datetime.now(timezone.utc)).astimezone(timezone.utc)).total_seconds())),
                "snapshots": _get_f1_session_snapshots(session_key, limit=10),
            }
        return None
    data_as_of = entry.get("data_as_of") or entry.get("generated_at")
    dt = _parse_dt(data_as_of)
    stale_age_seconds = 0
    if dt is not None:
        stale_age_seconds = max(0, int((datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()))
    return {
        "generated_at": entry.get("generated_at") or datetime.now().isoformat(timespec="seconds"),
        "data_as_of": data_as_of or datetime.now().isoformat(timespec="seconds"),
        "stale": True,
        "stale_age_seconds": stale_age_seconds,
        "session_key": session_key,
        "source": entry.get("source") or "archive",
        "session": deepcopy(entry.get("session") or {}),
        "live": bool(entry.get("live")),
        "drivers_count": int(entry.get("drivers_count") or len(entry.get("leaderboard") or [])),
        "leaderboard": deepcopy(entry.get("leaderboard") or []),
        "race_control": deepcopy(entry.get("race_control") or []),
        "weather": deepcopy(entry.get("weather") or {}),
        "snapshots": _get_f1_session_snapshots(session_key, limit=10),
    }


def _search_f1_session_archive(q: str, *, limit: int = 8) -> list[dict[str, Any]]:
    query = _normalize_search_text(q)
    tokens = [token for token in query.split(" ") if token]
    with _f1_archive_lock:
        entries = [deepcopy(entry) for entry in _F1_SESSION_ARCHIVE.values() if isinstance(entry, dict)]

    ranked: list[tuple[int, str, dict[str, Any]]] = []
    for entry in entries:
        session_key = str(entry.get("session_key") or "").strip()
        session_name = _normalize_search_text(entry.get("session_name"))
        meeting_name = _normalize_search_text(entry.get("meeting_name"))
        circuit = _normalize_search_text(entry.get("circuit"))
        location = _normalize_search_text(entry.get("location"))
        country_name = _normalize_search_text(entry.get("country_name"))
        year = str(entry.get("year") or "").strip()
        round_number = str(entry.get("round") or "").strip()
        haystack = " ".join(part for part in [meeting_name, session_name, circuit, location, country_name, year, round_number, session_key.lower()] if part).strip()
        if not haystack:
            continue
        score = 0
        if not query:
            score = 1
        else:
            if query == meeting_name:
                score += 120
            if query == circuit or query == location or query == country_name:
                score += 100
            if query in haystack:
                score += 70
            for token in tokens:
                if token in haystack:
                    score += 14
                if token == "race" and "race" in session_name:
                    score += 12
                if token.startswith("quali") and "qual" in session_name:
                    score += 12
                if token.startswith("sprint") and "sprint" in session_name:
                    score += 12
                if token.startswith("practice") and "practice" in session_name:
                    score += 8
        if score <= 0:
            continue
        ranked.append((score, str(entry.get("data_as_of") or entry.get("generated_at") or ""), entry))

    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    trimmed = []
    for _, _, entry in ranked[: max(1, min(int(limit or 8), 16))]:
        trimmed.append({
            "session_key": entry.get("session_key"),
            "year": entry.get("year"),
            "round": entry.get("round"),
            "meeting_name": entry.get("meeting_name"),
            "session_name": entry.get("session_name"),
            "circuit": entry.get("circuit"),
            "location": entry.get("location"),
            "country_name": entry.get("country_name"),
            "date_start": entry.get("date_start"),
            "data_as_of": entry.get("data_as_of"),
            "source": entry.get("source"),
            "live": bool(entry.get("live")),
            "drivers_count": int(entry.get("drivers_count") or 0),
            "leaderboard": deepcopy((entry.get("leaderboard") or [])[:5]),
        })
    return trimmed


def _provider_breaker_open(name: str) -> bool:
    state = _PROVIDER_BREAKERS.get(name) if isinstance(_PROVIDER_BREAKERS.get(name), dict) else {}
    return float(state.get("open_until") or 0.0) > time.time()


def _provider_breaker_error(name: str, err: str) -> None:
    state = _PROVIDER_BREAKERS.setdefault(name, {"fail_count": 0, "open_until": 0.0, "last_error": None})
    fail_count = int(state.get("fail_count") or 0) + 1
    cooldown = 120 if fail_count >= 3 else 0
    state["fail_count"] = fail_count
    state["open_until"] = time.time() + cooldown if cooldown else 0.0
    state["last_error"] = str(err or "")[:240]


def _provider_breaker_success(name: str) -> None:
    state = _PROVIDER_BREAKERS.setdefault(name, {"fail_count": 0, "open_until": 0.0, "last_error": None})
    state["fail_count"] = 0
    state["open_until"] = 0.0
    state["last_error"] = None


def _set_last_good(key: str, payload: dict[str, Any]) -> None:
    snap = deepcopy(payload)
    if not isinstance(snap, dict):
        return
    snap["data_as_of"] = snap.get("data_as_of") or snap.get("generated_at")
    snap["stale"] = False
    snap["stale_age_seconds"] = 0
    with _last_good_lock:
        _LAST_GOOD[key] = {"ts": time.time(), "value": snap}
        try:
            _save_last_good_store()
        except Exception:
            pass


def _get_last_good(key: str, *, max_age_seconds: int = 21600) -> tuple[Optional[dict[str, Any]], int]:
    with _last_good_lock:
        ent = deepcopy(_LAST_GOOD.get(key))
    if not isinstance(ent, dict):
        return None, 0

    ts = float(ent.get("ts") or 0.0)
    age_seconds = max(0, int(time.time() - ts))
    if age_seconds > max(1, int(max_age_seconds)):
        return None, age_seconds

    val = deepcopy(ent.get("value"))
    if not isinstance(val, dict):
        return None, age_seconds
    return val, age_seconds


def _mark_payload_stale(payload: dict[str, Any], *, age_seconds: int) -> dict[str, Any]:
    out = deepcopy(payload)
    data_as_of = out.get("data_as_of") or out.get("generated_at")
    out["generated_at"] = datetime.now().isoformat(timespec="seconds")
    out["data_as_of"] = data_as_of
    out["stale"] = True
    out["stale_age_seconds"] = max(0, int(age_seconds))
    return out


def _markets_payload_has_data(payload: dict[str, Any]) -> bool:
    try:
        if int(payload.get("count") or 0) > 0:
            return True
    except Exception:
        pass
    hist = payload.get("history") or {}
    if not isinstance(hist, dict):
        return False
    return any(isinstance(v, list) and len(v) > 0 for v in hist.values())


def _f1_payload_has_data(payload: dict[str, Any]) -> bool:
    try:
        if int(payload.get("standings_count") or 0) > 0:
            return True
    except Exception:
        pass
    return any(payload.get(k) for k in ["next_race", "latest_race", "upcoming_races"])


def _f1_session_payload_has_data(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if isinstance(payload.get("session"), dict) and payload.get("session"):
        return True
    if len(payload.get("leaderboard") or []) > 0:
        return True
    if len(payload.get("race_control") or []) > 0:
        return True
    weather = payload.get("weather") or {}
    return any(weather.get(key) is not None for key in ["air_temperature", "track_temperature", "humidity", "date"])


def _warframe_payload_has_data(payload: dict[str, Any]) -> bool:
    worldstate = payload.get("worldstate") or {}
    market = payload.get("market") or {}
    if (market.get("best_sell") is not None) or (market.get("best_buy") is not None):
        return True
    if len(payload.get("top_sells") or []) > 0:
        return True
    for key in ["alerts", "fissures", "invasions", "events", "news"]:
        if len(worldstate.get(key) or []) > 0:
            return True
    return False



def _warframe_worldstate_has_data(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    for key in ["alerts", "fissures", "invasions", "events", "news"]:
        if len(payload.get(key) or []) > 0:
            return True
    for cycle_name in ["cetus", "vallis", "cambion"]:
        cycle = _dig(payload, "world_cycles", cycle_name) or {}
        if cycle.get("state") or cycle.get("eta"):
            return True
    sortie = payload.get("sortie") or {}
    return bool(sortie.get("eta") or sortie.get("boss"))

def _cache_get(key: str, ttl_seconds: int) -> Optional[Any]:
    ent = _API_CACHE.get(key)
    if not isinstance(ent, dict):
        return None
    ts = float(ent.get("ts") or 0.0)
    if time.time() - ts > max(1, int(ttl_seconds)):
        _API_CACHE.pop(key, None)
        return None
    return deepcopy(ent.get("value"))


def _cache_set(key: str, value: Any) -> None:
    _API_CACHE[key] = {"ts": time.time(), "value": deepcopy(value)}


def _http_get_json(
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 12,
) -> tuple[Optional[Any], Optional[str]]:
    merged_headers = dict(_DEFAULT_HTTP_HEADERS)
    if isinstance(headers, dict):
        merged_headers.update(headers)
    provider_name = "openf1" if "api.openf1.org" in str(url or "") else None
    if provider_name and _provider_breaker_open(provider_name):
        state = _PROVIDER_BREAKERS.get(provider_name) or {}
        retry_in = max(0, int((float(state.get("open_until") or 0.0) - time.time())))
        return None, f"{provider_name}:circuit_open:{retry_in}s"

    last_err: Optional[str] = None
    attempts = 2 if provider_name == "openf1" else 1
    for attempt in range(attempts):
        try:
            r = requests.get(url, params=params, headers=merged_headers, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            if provider_name:
                _provider_breaker_success(provider_name)
            return payload, None
        except Exception as e:
            last_err = str(e)
            if provider_name and attempt + 1 < attempts:
                time.sleep(0.35)
                continue
    if provider_name:
        _provider_breaker_error(provider_name, last_err or "request_failed")
    return None, last_err


def _coerce_symbol_list(raw: Optional[str]) -> list[str]:
    if not raw:
        return list(_DEFAULT_MARKET_SYMBOLS)
    items = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not items:
        return list(_DEFAULT_MARKET_SYMBOLS)
    return items[:30]


def _build_market_quote_from_series(symbol: str, series: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    vals = [float(x.get("close")) for x in series if isinstance(x, dict) and x.get("close") is not None]
    if not vals:
        return None
    latest = vals[-1]
    prev = vals[-2] if len(vals) > 1 else latest
    ch = latest - prev
    pct = (ch / prev * 100.0) if prev else 0.0
    return {
        "symbol": symbol,
        "name": symbol,
        "price": latest,
        "change": ch,
        "change_pct": pct,
        "currency": None,
        "market_state": "fallback",
        "updated_at": None,
    }


def _fallback_market_quotes(symbols: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    out_map: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    safe_symbols = [str(sym or "").strip() for sym in symbols if str(sym or "").strip()]
    if not safe_symbols:
        return [], []

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(safe_symbols)))) as pool:
        futures = {pool.submit(_fetch_market_history, sym, range_key="5d", interval="1d"): sym for sym in safe_symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                series, err = future.result()
            except Exception as exc:
                errors.append(f"fallback:{sym}:{exc}")
                continue
            if err:
                errors.append(f"fallback:{sym}:{err}")
                continue
            q = _build_market_quote_from_series(sym, series)
            if not q:
                errors.append(f"fallback:{sym}:no_series")
                continue
            out_map[sym.upper()] = q

    ordered = [out_map[sym.upper()] for sym in safe_symbols if sym.upper() in out_map]
    return ordered, errors


def _load_api_key(name: str) -> str:
    env_name = _API_KEY_ENV_MAP.get(name)
    if env_name:
        val = os.getenv(env_name, "").strip()
        if val:
            return val
    try:
        return str((_load_settings().get("api_keys", {}) or {}).get(name, "")).strip()
    except Exception:
        return ""


_ALECA_PUBLIC_PARTS = {
    1: "Trades",
    2: "Platinum",
    4: "Ducats",
    8: "Endo",
    16: "Credits",
    32: "Account Data",
    64: "Aya",
    128: "Relics",
}

_ALECA_TRADE_TYPES = {
    0: "sale",
    1: "purchase",
    2: "trade",
}


def _aleca_visible_parts(mask: Any) -> list[str]:
    try:
        value = int(mask or 0)
    except Exception:
        value = 0
    parts: list[str] = []
    for bit, label in _ALECA_PUBLIC_PARTS.items():
        if value & bit:
            parts.append(label)
    return parts


def _aleca_trade_items(raw: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("displayName") or item.get("name") or "Item").strip()
        try:
            count = int(item.get("cnt") or 0)
        except Exception:
            count = 0
        try:
            rank = int(item.get("rank")) if item.get("rank") is not None else None
        except Exception:
            rank = None
        label = f"{count}x {name}" if count not in (0, 1) else name
        if rank is not None and rank >= 0:
            label = f"{label} (R{rank})"
        out.append(
            {
                "name": name,
                "count": count,
                "rank": rank,
                "label": label,
            }
        )
    return out


def _extract_aleca_relic_entries(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ["items", "relics", "inventory", "data", "results"]:
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            for nested_key in ["items", "relics", "inventory", "data", "results"]:
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    return [row for row in nested if isinstance(row, dict)]
    return []


def _summarize_aleca_relic_inventory(payload: Any) -> dict[str, Any]:
    entries = _extract_aleca_relic_entries(payload)
    normalized: list[dict[str, Any]] = []
    for entry in entries:
        name = (
            entry.get("displayName")
            or entry.get("name")
            or entry.get("relicName")
            or entry.get("itemName")
            or entry.get("key")
        )
        if not name:
            continue
        count = 1
        for key in ["count", "quantity", "copies", "owned", "amount"]:
            try:
                if entry.get(key) is not None:
                    count = max(1, int(entry.get(key)))
                    break
            except Exception:
                continue
        plat_value = None
        for key in ["platValue", "platinum", "estimatedPlat", "valuePlat", "value"]:
            try:
                if entry.get(key) is not None:
                    plat_value = round(float(entry.get(key)), 1)
                    break
            except Exception:
                continue
        normalized.append(
            {
                "name": str(name).strip(),
                "count": count,
                "plat_value": plat_value,
            }
        )
    normalized.sort(
        key=lambda row: (
            float(row.get("plat_value") or 0.0),
            int(row.get("count") or 0),
            str(row.get("name") or ""),
        ),
        reverse=True,
    )
    stash_value_estimate = 0.0
    for row in normalized:
        try:
            if row.get("plat_value") is not None:
                stash_value_estimate += float(row.get("plat_value")) * int(row.get("count") or 0)
        except Exception:
            continue
    top_by_copies = sorted(
        normalized,
        key=lambda row: (
            int(row.get("count") or 0),
            float(row.get("plat_value") or 0.0),
            str(row.get("name") or ""),
        ),
        reverse=True,
    )[:8]
    return {
        "available": bool(entries),
        "entry_count": len(normalized),
        "unique_relics": len({str(row.get("name") or "") for row in normalized if str(row.get("name") or "")}),
        "total_copies": sum(int(row.get("count") or 0) for row in normalized),
        "stash_value_estimate": round(stash_value_estimate, 1),
        "top_entries": normalized[:8],
        "top_by_copies": top_by_copies,
    }


def _build_aleca_trade_insights(trades_raw: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    sent_rollup: dict[str, dict[str, Any]] = {}
    received_rollup: dict[str, dict[str, Any]] = {}
    recent_daily: dict[str, dict[str, int]] = {}
    active_days: set[str] = set()
    plat_values: list[int] = []
    sale_values: list[int] = []
    purchase_values: list[int] = []
    trade_values: list[int] = []
    trade_count_7d = 0
    plat_in_7d = 0
    plat_out_7d = 0
    last_trade_at = None

    def bump_rollup(target: dict[str, dict[str, Any]], items: list[dict[str, Any]], trade_type: str, total_plat: int, trade_ts: Any) -> None:
        for item in items:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            entry = target.setdefault(
                name.casefold(),
                {
                    "name": name,
                    "count": 0,
                    "trades": 0,
                    "plat_total": 0,
                    "last_seen_at": None,
                    "trade_types": set(),
                },
            )
            entry["count"] = int(entry.get("count") or 0) + max(1, int(item.get("count") or 0))
            entry["trades"] = int(entry.get("trades") or 0) + 1
            entry["plat_total"] = int(entry.get("plat_total") or 0) + max(0, int(total_plat or 0))
            entry["last_seen_at"] = trade_ts or entry.get("last_seen_at")
            trade_types = entry.get("trade_types")
            if isinstance(trade_types, set):
                trade_types.add(trade_type)

    for trade in trades_raw:
        trade_ts = trade.get("ts")
        trade_dt = _parse_dt(trade_ts)
        if trade_dt:
            day_key = trade_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
            active_days.add(day_key)
            lane = recent_daily.setdefault(day_key, {"count": 0, "plat_in": 0, "plat_out": 0})
            lane["count"] = int(lane.get("count") or 0) + 1
            if last_trade_at is None or trade_dt > (_parse_dt(last_trade_at) or datetime.min.replace(tzinfo=timezone.utc)):
                last_trade_at = trade_ts
        trade_type = _ALECA_TRADE_TYPES.get(int(trade.get("type") or 0), "trade")
        try:
            total_plat = int(trade.get("totalPlat") or 0)
        except Exception:
            total_plat = 0
        plat_values.append(total_plat)
        if trade_type == "sale":
            sale_values.append(total_plat)
        elif trade_type == "purchase":
            purchase_values.append(total_plat)
        else:
            trade_values.append(total_plat)
        if trade_dt and (now - trade_dt).total_seconds() <= 7 * 24 * 3600:
            trade_count_7d += 1
            if trade_type == "sale":
                plat_in_7d += total_plat
            elif trade_type == "purchase":
                plat_out_7d += total_plat
        tx_items = _aleca_trade_items(trade.get("tx"))
        rx_items = _aleca_trade_items(trade.get("rx"))
        if trade_type == "sale":
            bump_rollup(sent_rollup, tx_items, trade_type, total_plat, trade_ts)
        elif trade_type == "purchase":
            bump_rollup(received_rollup, rx_items, trade_type, total_plat, trade_ts)
        else:
            bump_rollup(sent_rollup, tx_items, trade_type, total_plat, trade_ts)
            bump_rollup(received_rollup, rx_items, trade_type, total_plat, trade_ts)

    def finalize_rollup(source: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        rows = []
        for row in source.values():
            trade_types = row.get("trade_types")
            rows.append(
                {
                    "name": row.get("name"),
                    "count": int(row.get("count") or 0),
                    "trades": int(row.get("trades") or 0),
                    "plat_total": int(row.get("plat_total") or 0),
                    "avg_plat_per_trade": round(int(row.get("plat_total") or 0) / max(1, int(row.get("trades") or 0)), 1),
                    "last_seen_at": row.get("last_seen_at"),
                    "trade_types": sorted(trade_types) if isinstance(trade_types, set) else [],
                }
            )
        rows.sort(
            key=lambda row: (
                int(row.get("plat_total") or 0),
                int(row.get("count") or 0),
                int(row.get("trades") or 0),
                str(row.get("name") or ""),
            ),
            reverse=True,
        )
        return rows[:8]

    recent_activity = []
    for day_key in sorted(recent_daily.keys(), reverse=True)[:7]:
        recent_activity.append({"day": day_key, **recent_daily[day_key]})

    return {
        "trade_count_7d": trade_count_7d,
        "plat_in_7d": plat_in_7d,
        "plat_out_7d": plat_out_7d,
        "net_plat_7d": plat_in_7d - plat_out_7d,
        "active_days": len(active_days),
        "last_trade_at": last_trade_at,
        "avg_plat_per_trade": round(sum(plat_values) / len(plat_values), 1) if plat_values else None,
        "avg_sale_plat": round(sum(sale_values) / len(sale_values), 1) if sale_values else None,
        "avg_purchase_plat": round(sum(purchase_values) / len(purchase_values), 1) if purchase_values else None,
        "avg_barter_plat": round(sum(trade_values) / len(trade_values), 1) if trade_values else None,
        "top_sent_items": finalize_rollup(sent_rollup),
        "top_received_items": finalize_rollup(received_rollup),
        "recent_activity": recent_activity,
    }


def _build_alecaframe_payload() -> tuple[dict[str, Any], list[str]]:
    cache_key = "warframe:alecaframe:summary"
    cached = _cache_get(cache_key, ttl_seconds=10 * 60)
    if isinstance(cached, dict):
        return cached, list(cached.get("errors") or [])

    user_hash = _load_api_key("alecaframe_user_hash")
    secret_token = _load_api_key("alecaframe_secret_token")
    public_token = _load_api_key("alecaframe_public_token")
    if not user_hash and not public_token:
        payload = {
            "configured": False,
            "mode": "disabled",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "errors": [],
        }
        _cache_set(cache_key, payload)
        return payload, []

    errors: list[str] = []
    stats_payload: Any = None
    mode = "public"
    headers = {"Accept": "application/json", "User-Agent": "HolocronHub/0.5"}
    if user_hash:
        mode = "private" if secret_token else "user_hash"
        stats_payload, err = _http_get_json(
            f"https://stats.alecaframe.com/api/stats/{quote(str(user_hash), safe='')}",
            params={"secretToken": secret_token} if secret_token else None,
            headers=headers,
            timeout=18,
        )
        if err:
            errors.append(f"alecaframe:stats:{err}")
    elif public_token:
        stats_payload, err = _http_get_json(
            "https://stats.alecaframe.com/api/stats/public",
            params={"token": public_token},
            headers=headers,
            timeout=18,
        )
        if err:
            errors.append(f"alecaframe:public:{err}")

    if not isinstance(stats_payload, dict):
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=24 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(errors or []) + [f"fallback:last_good:alecaframe:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["errors"] = fallback_errors[:24]
            _cache_set(cache_key, stale_payload)
            return stale_payload, fallback_errors[:24]
        payload = {
            "configured": True,
            "mode": mode,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "errors": errors[:24],
        }
        _cache_set(cache_key, payload)
        return payload, errors[:24]

    data_points = [point for point in (stats_payload.get("generalDataPoints") or []) if isinstance(point, dict)]
    data_points.sort(key=lambda row: str(row.get("ts") or ""))
    latest = data_points[-1] if data_points else {}
    previous = data_points[-2] if len(data_points) >= 2 else {}
    resources: list[dict[str, Any]] = []
    for key, label, suffix in [
        ("plat", "Platinum", "p"),
        ("ducats", "Ducats", ""),
        ("aya", "Aya", ""),
        ("endo", "Endo", ""),
        ("credits", "Credits", ""),
        ("relicOpened", "Relics Opened", ""),
        ("trades", "Trades", ""),
        ("mr", "Mastery", ""),
        ("percentageCompletion", "Completion", "%"),
    ]:
        try:
            latest_value = latest.get(key)
            prev_value = previous.get(key)
            delta = None
            if latest_value is not None and prev_value is not None:
                delta = round(float(latest_value) - float(prev_value), 1)
            resources.append(
                {
                    "key": key,
                    "label": label,
                    "value": latest_value,
                    "suffix": suffix,
                    "delta": delta,
                }
            )
        except Exception:
            continue

    trades_raw = [trade for trade in (stats_payload.get("trades") or []) if isinstance(trade, dict)]
    trades_raw.sort(key=lambda row: str(row.get("ts") or ""), reverse=True)
    recent_trades: list[dict[str, Any]] = []
    sale_count = 0
    purchase_count = 0
    barter_count = 0
    plat_in = 0
    plat_out = 0
    for trade in trades_raw:
        trade_type = _ALECA_TRADE_TYPES.get(int(trade.get("type") or 0), "trade")
        if trade_type == "sale":
            sale_count += 1
        elif trade_type == "purchase":
            purchase_count += 1
        else:
            barter_count += 1
        try:
            total_plat = int(trade.get("totalPlat") or 0)
        except Exception:
            total_plat = 0
        if trade_type == "sale":
            plat_in += total_plat
        elif trade_type == "purchase":
            plat_out += total_plat
        tx_items = _aleca_trade_items(trade.get("tx"))
        rx_items = _aleca_trade_items(trade.get("rx"))
        recent_trades.append(
            {
                "ts": trade.get("ts"),
                "user": trade.get("user"),
                "type": trade_type,
                "total_plat": total_plat,
                "tx": tx_items,
                "rx": rx_items,
                "headline": " -> ".join(
                    [
                        ", ".join(item.get("label") or item.get("name") or "Item" for item in tx_items[:2]) or "nothing sent",
                        ", ".join(item.get("label") or item.get("name") or "Item" for item in rx_items[:2]) or "nothing received",
                    ]
                ),
            }
        )

    relic_summary = {"available": False, "entry_count": 0, "unique_relics": 0, "total_copies": 0, "top_entries": []}
    if public_token:
        relic_payload, relic_err = _http_get_json(
            "https://stats.alecaframe.com/api/stats/public/getRelicInventory",
            params={"publicToken": public_token},
            headers=headers,
            timeout=18,
        )
        if relic_err:
            errors.append(f"alecaframe:relics:{relic_err}")
        else:
            relic_summary = _summarize_aleca_relic_inventory(relic_payload)
    trade_insights = _build_aleca_trade_insights(trades_raw)

    payload = {
        "configured": True,
        "mode": mode,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "last_update": stats_payload.get("lastUpdate"),
        "user_hash": stats_payload.get("userHash"),
        "username": stats_payload.get("usernameWhenPublic"),
        "visible_parts": _aleca_visible_parts(stats_payload.get("publicParts")),
        "data_points_count": len(data_points),
        "resources": resources,
        "trade_summary": {
            "total": len(trades_raw),
            "sales": sale_count,
            "purchases": purchase_count,
            "barters": barter_count,
            "plat_in": plat_in,
            "plat_out": plat_out,
        },
        "trade_insights": trade_insights,
        "recent_trades": recent_trades[:8],
        "relic_inventory": relic_summary,
        "errors": errors[:24],
    }
    _cache_set(cache_key, payload)
    _set_last_good(cache_key, payload)
    return payload, errors[:24]


def _alpha_vantage_series_key(payload: dict[str, Any]) -> Optional[str]:
    for key in payload.keys():
        if "Time Series" in str(key):
            return str(key)
    return None


def _fetch_market_history_twelvedata(symbol: str, *, range_key: str = "1mo", interval: str = "1d") -> tuple[list[dict[str, Any]], Optional[str]]:
    api_key = _load_api_key("twelvedata_api_key")
    if not api_key:
        return [], "twelvedata:no_api_key"

    interval_map = {
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "1d": "1day",
        "1wk": "1week",
    }
    outputsize_map = {
        "5d": 5,
        "1mo": 31,
        "3mo": 93,
        "6mo": 186,
        "1y": 366,
    }
    params = {
        "symbol": str(symbol or "").strip(),
        "interval": interval_map.get(str(interval or "1d").lower(), "1day"),
        "outputsize": outputsize_map.get(str(range_key or "1mo").lower(), 31),
        "apikey": api_key,
        "format": "JSON",
    }
    data, err = _http_get_json("https://api.twelvedata.com/time_series", params=params, timeout=18)
    if err:
        return [], f"twelvedata:{err}"
    if not isinstance(data, dict):
        return [], "twelvedata:invalid_payload"
    if str(data.get("status") or "").lower() == "error":
        return [], f"twelvedata:{data.get('message') or 'error'}"

    values = data.get("values") or []
    if not isinstance(values, list) or not values:
        return [], "twelvedata:empty_series"

    out: list[dict[str, Any]] = []
    for row in reversed(values):
        if not isinstance(row, dict):
            continue
        dt_raw = str(row.get("datetime") or "").strip()
        raw_close = row.get("close")
        if not dt_raw or raw_close is None:
            continue
        try:
            close_val = float(raw_close)
        except Exception:
            continue
        iso_dt = dt_raw.replace(" ", "T")
        if len(iso_dt) == 10:
            iso_dt = f"{iso_dt}T00:00:00"
        if not iso_dt.endswith("Z") and "+" not in iso_dt:
            iso_dt = f"{iso_dt}+00:00"
        out.append({"datetime": iso_dt, "close": close_val})

    if not out:
        return [], "twelvedata:no_points"
    return out, None


def _fetch_market_history_alpha(symbol: str, *, range_key: str = "1mo") -> tuple[list[dict[str, Any]], Optional[str]]:
    api_key = _load_api_key("alphavantage_api_key")
    if not api_key:
        return [], "alphavantage:no_api_key"

    sym = str(symbol or "").strip().upper()
    if not sym:
        return [], "alphavantage:invalid_symbol"

    params: dict[str, Any]
    close_key = "4. close"
    kind = "equity"
    if sym.endswith("=X") and len(sym) >= 7:
        pair = sym[:-2]
        if len(pair) != 6:
            return [], f"alphavantage:unsupported_symbol:{sym}"
        params = {
            "function": "FX_DAILY",
            "from_symbol": pair[:3],
            "to_symbol": pair[3:],
            "outputsize": "compact",
            "apikey": api_key,
        }
        kind = "fx"
    elif "-" in sym and len(sym.split("-", 1)[0]) >= 2:
        base, market = sym.split("-", 1)
        params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": base,
            "market": market,
            "apikey": api_key,
        }
        close_key = f"4a. close ({market.upper()})"
        kind = "crypto"
    else:
        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": sym,
            "outputsize": "compact",
            "apikey": api_key,
        }

    data, err = _http_get_json("https://www.alphavantage.co/query", params=params, timeout=18)
    if err:
        return [], f"alphavantage:{err}"
    if not isinstance(data, dict):
        return [], "alphavantage:invalid_payload"
    if data.get("Error Message"):
        return [], f"alphavantage:{data.get('Error Message')}"
    if data.get("Information"):
        return [], f"alphavantage:{data.get('Information')}"
    if data.get("Note"):
        return [], f"alphavantage:{data.get('Note')}"

    series_key = _alpha_vantage_series_key(data)
    if not series_key:
        return [], f"alphavantage:no_series:{kind}"

    raw_series = data.get(series_key) or {}
    if not isinstance(raw_series, dict):
        return [], "alphavantage:invalid_series"

    max_points_map = {"5d": 5, "1mo": 31, "3mo": 93, "6mo": 186, "1y": 366}
    max_points = max_points_map.get(range_key, 31)
    out: list[dict[str, Any]] = []
    for day in sorted(raw_series.keys())[-max_points:]:
        row = raw_series.get(day) if isinstance(raw_series, dict) else None
        if not isinstance(row, dict):
            continue
        raw_close = row.get(close_key) or row.get("4. close")
        try:
            close_val = float(raw_close)
        except Exception:
            continue
        out.append(
            {
                "datetime": datetime.fromisoformat(f"{day}T00:00:00+00:00").isoformat(),
                "close": close_val,
            }
        )

    if not out:
        return [], "alphavantage:empty_series"
    return out, None


def _fetch_market_quotes(symbols: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []

    payload_data, req_err = _http_get_json(
        "https://query1.finance.yahoo.com/v7/finance/quote",
        params={"symbols": ",".join(symbols)},
        timeout=12,
    )

    # If query1 returns 401/error, retry against query2 before falling back
    if req_err:
        payload_data2, req_err2 = _http_get_json(
            "https://query2.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
            timeout=12,
        )
        if not req_err2 and payload_data2:
            payload_data, req_err = payload_data2, None
        else:
            fb_quotes, fb_errors = _fallback_market_quotes(symbols)
            if fb_quotes:
                return fb_quotes, [f"quote_primary:{req_err}", *fb_errors[:20]]
            return [], [str(req_err)]

    payload = _dig(payload_data, "quoteResponse", "result") or []
    if not isinstance(payload, list):
        payload = []

    out: list[dict[str, Any]] = []
    found = {str(q.get("symbol", "")).upper() for q in payload if isinstance(q, dict)}
    for sym in symbols:
        q = next((x for x in payload if isinstance(x, dict) and str(x.get("symbol", "")).upper() == sym.upper()), None)
        if not q:
            errors.append(f"symbol_not_found:{sym}")
            continue

        out.append(
            {
                "symbol": q.get("symbol", sym),
                "name": q.get("shortName") or q.get("longName") or q.get("symbol", sym),
                "price": q.get("regularMarketPrice"),
                "change": q.get("regularMarketChange"),
                "change_pct": q.get("regularMarketChangePercent"),
                "currency": q.get("currency"),
                "market_state": q.get("marketState"),
                "updated_at": q.get("regularMarketTime"),
            }
        )

    missing = [s for s in symbols if s.upper() not in found]
    if missing:
        fb_quotes, fb_errors = _fallback_market_quotes(missing)
        out.extend(fb_quotes)
        errors.extend(fb_errors)

    return out, errors


def _fetch_market_history(symbol: str, *, range_key: str = "1mo", interval: str = "1d") -> tuple[list[dict[str, Any]], Optional[str]]:
    safe_symbol = quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{safe_symbol}"
    data, err = _http_get_json(url, params={"range": range_key, "interval": interval}, timeout=15)
    if err:
        td_series, td_err = _fetch_market_history_twelvedata(symbol, range_key=range_key, interval=interval)
        if td_series:
            return td_series, f"yahoo:{err}"
        alpha_series, alpha_err = _fetch_market_history_alpha(symbol, range_key=range_key)
        if alpha_series:
            suffix = f" | {td_err}" if td_err else ""
            return alpha_series, f"yahoo:{err}{suffix}"
        combined = []
        combined.append(str(err))
        if td_err:
            combined.append(td_err)
        if alpha_err:
            combined.append(alpha_err)
        return [], " | ".join(combined)

    result = _dig(data, "chart", "result") or []
    if not isinstance(result, list) or not result:
        td_series, td_err = _fetch_market_history_twelvedata(symbol, range_key=range_key, interval=interval)
        if td_series:
            return td_series, "yahoo:empty_chart_result"
        alpha_series, alpha_err = _fetch_market_history_alpha(symbol, range_key=range_key)
        if alpha_series:
            suffix = f" | {td_err}" if td_err else ""
            return alpha_series, f"yahoo:empty_chart_result{suffix}"
        combined = ["empty_chart_result"]
        if td_err:
            combined.append(td_err)
        if alpha_err:
            combined.append(alpha_err)
        return [], " | ".join(combined)

    r0 = result[0] if isinstance(result[0], dict) else {}
    timestamps = r0.get("timestamp") or []
    closes = _dig(r0, "indicators", "quote") or []
    q0 = closes[0] if isinstance(closes, list) and closes else {}
    close_list = q0.get("close") if isinstance(q0, dict) else []

    points: list[dict[str, Any]] = []
    for idx, ts in enumerate(timestamps):
        try:
            ts_int = int(ts)
        except Exception:
            continue
        close_val = close_list[idx] if isinstance(close_list, list) and idx < len(close_list) else None
        if close_val is None:
            continue
        try:
            close_float = float(close_val)
        except Exception:
            continue
        points.append(
            {
                "datetime": datetime.fromtimestamp(ts_int, timezone.utc).isoformat(),
                "close": close_float,
            }
        )

    if points:
        return points, None

    td_series, td_err = _fetch_market_history_twelvedata(symbol, range_key=range_key, interval=interval)
    if td_series:
        return td_series, "yahoo:empty_points"
    alpha_series, alpha_err = _fetch_market_history_alpha(symbol, range_key=range_key)
    if alpha_series:
        suffix = f" | {td_err}" if td_err else ""
        return alpha_series, f"yahoo:empty_points{suffix}"
    combined = ["empty_points"]
    if td_err:
        combined.append(td_err)
    if alpha_err:
        combined.append(alpha_err)
    return [], " | ".join(combined)


def _fetch_markets_history(
    symbols: list[str],
    *,
    range_key: str = "1mo",
    interval: str = "1d",
    max_symbols: int = 15,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    out: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    target_symbols = [str(sym or "").strip() for sym in symbols[:max_symbols] if str(sym or "").strip()]
    if not target_symbols:
        return out, errors

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(target_symbols)))) as pool:
        futures = {pool.submit(_fetch_market_history, sym, range_key=range_key, interval=interval): sym for sym in target_symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                series, err = future.result()
            except Exception as exc:
                out[sym] = []
                errors.append(f"{sym}:{exc}")
                continue
            out[sym] = series
            if err:
                errors.append(f"{sym}:{err}")
    return out, errors


def _safe_iso_utc(date_raw: Optional[str], time_raw: Optional[str]) -> Optional[str]:
    if not date_raw:
        return None
    t = (time_raw or "00:00:00Z").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(f"{date_raw}T{t}").astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def _dig(data: Any, *path: str) -> Any:
    cur = data
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _build_f1_circuit_links(circuit: Optional[str], locality: Optional[str], country: Optional[str]) -> dict[str, Optional[str]]:
    parts = [str(x).strip() for x in [circuit, locality, country] if str(x or "").strip()]
    if not parts:
        return {"map_url": None, "layout_url": None, "wiki_url": None}

    query = " ".join(parts)
    layout_query = quote_plus(f"{query} circuit layout")
    map_query = quote_plus(query)
    wiki_name = str(circuit or "").strip().replace(" ", "_")
    wiki_url = f"https://en.wikipedia.org/wiki/{quote(wiki_name, safe='_')}" if wiki_name else None

    return {
        "map_url": f"https://www.google.com/maps/search/?api=1&query={map_query}",
        "layout_url": f"https://www.google.com/search?q={layout_query}",
        "wiki_url": wiki_url,
    }


def _f1_schedule_race_coords(race: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    if not isinstance(race, dict):
        return None, None
    lat = _safe_float(_dig(race, "Circuit", "Location", "lat"))
    lon = _safe_float(_dig(race, "Circuit", "Location", "long"))
    return lat, lon


def _find_f1_schedule_race_match(
    schedule_races: list[dict[str, Any]],
    next_race: Optional[dict[str, Any]] = None,
    latest_race: Optional[dict[str, Any]] = None,
    meeting: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    if not schedule_races:
        return None
    target_name = _f1_norm_text((meeting or {}).get("meeting_name") or (meeting or {}).get("meeting_official_name"))
    target_country = _f1_norm_text((meeting or {}).get("country_name"))
    target_location = _f1_norm_text((meeting or {}).get("location"))
    target_circuit = _f1_norm_text((meeting or {}).get("circuit_short_name"))

    chosen = next_race if isinstance(next_race, dict) and next_race else latest_race if isinstance(latest_race, dict) else None
    if isinstance(chosen, dict):
        target_name = target_name or _f1_norm_text(chosen.get("race_name"))
        target_country = target_country or _f1_norm_text(chosen.get("country"))
        target_location = target_location or _f1_norm_text(chosen.get("locality"))
        target_circuit = target_circuit or _f1_norm_text(chosen.get("circuit"))

    best_match: Optional[dict[str, Any]] = None
    best_score = 0.0
    for race in schedule_races:
        if not isinstance(race, dict):
            continue
        score = 0.0
        name = _f1_norm_text(race.get("raceName"))
        country = _f1_norm_text(_dig(race, "Circuit", "Location", "country"))
        location = _f1_norm_text(_dig(race, "Circuit", "Location", "locality"))
        circuit = _f1_norm_text(_dig(race, "Circuit", "circuitName"))
        if target_name and (target_name in name or name in target_name):
            score += 70.0
        if target_country and target_country == country:
            score += 25.0
        if target_location and (target_location == location or target_location in location or location in target_location):
            score += 20.0
        if target_circuit and (target_circuit == circuit or target_circuit in circuit or circuit in target_circuit):
            score += 20.0
        if score > best_score:
            best_score = score
            best_match = race
    return best_match if best_score >= 25.0 else None


def _fetch_open_meteo_weather(latitude: Optional[float], longitude: Optional[float]) -> tuple[dict[str, Any], Optional[str]]:
    if latitude is None or longitude is None:
        return {}, "open_meteo:coords_missing"
    data, err = _http_get_json(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,surface_pressure",
            "timezone": "auto",
        },
        timeout=12,
    )
    if err:
        return {}, f"open_meteo:{err}"
    current = data.get("current") if isinstance(data, dict) and isinstance(data.get("current"), dict) else {}
    if not current:
        return {}, "open_meteo:invalid_payload"
    return {
        "air_temperature": current.get("temperature_2m"),
        "track_temperature": None,
        "humidity": current.get("relative_humidity_2m"),
        "rainfall": current.get("precipitation"),
        "wind_direction": None,
        "wind_speed": current.get("wind_speed_10m"),
        "pressure": current.get("surface_pressure"),
        "date": current.get("time"),
        "source": "open_meteo",
        "utc_offset_seconds": data.get("utc_offset_seconds") if isinstance(data, dict) else None,
    }, None


def _f1_weather_has_data(weather: Optional[dict[str, Any]]) -> bool:
    if not isinstance(weather, dict):
        return False
    return any(weather.get(key) is not None for key in ["air_temperature", "track_temperature", "humidity", "rainfall", "wind_speed", "pressure", "date"])


def _format_gmt_offset(offset_seconds: Any) -> str:
    try:
        total = int(offset_seconds or 0)
    except Exception:
        total = 0
    sign = "+" if total >= 0 else "-"
    abs_total = abs(total)
    hh = abs_total // 3600
    mm = (abs_total % 3600) // 60
    ss = abs_total % 60
    return f"{sign}{hh:02d}:{mm:02d}:{ss:02d}"


def _apply_f1_session_offset(session: dict[str, Any], gmt_offset: str) -> dict[str, Any]:
    out = deepcopy(session)
    start_dt = _parse_dt(out.get("date_start"))
    end_dt = _parse_dt(out.get("date_end"))
    out["date_start_local"] = _fmt_local_offset(start_dt, gmt_offset)
    out["date_end_local"] = _fmt_local_offset(end_dt, gmt_offset)
    return out


def _f1_status_is_finish(status: str) -> bool:
    st = str(status or "").strip().lower()
    if not st:
        return False
    if st.startswith("finished"):
        return True
    return "lap" in st and "retired" not in st


def _parse_dt(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _fmt_local_offset(dt: Optional[datetime], offset_raw: Any) -> Optional[str]:
    if dt is None:
        return None
    offset = str(offset_raw or "").strip()
    if not offset:
        return dt.isoformat()
    try:
        sign = 1 if offset[0] != "-" else -1
        hh, mm, ss = (offset[1:] if offset[0] in "+-" else offset).split(":")
        delta = timedelta(hours=int(hh), minutes=int(mm), seconds=int(ss or 0))
        tz = timezone(sign * delta)
        return dt.astimezone(tz).isoformat()
    except Exception:
        return dt.isoformat()


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _f1_norm_text(value: Any) -> str:
    text = str(value or "").lower().strip()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _openf1_get(path: str, *, params: Optional[dict[str, Any]] = None, timeout: int = 15) -> tuple[list[dict[str, Any]], Optional[str]]:
    data, err = _http_get_json(f"https://api.openf1.org/v1/{path}", params=params, timeout=timeout)
    if err:
        return [], f"{path}:{err}"
    if not isinstance(data, list):
        return [], f"{path}:invalid_payload"
    return [row for row in data if isinstance(row, dict)], None


def _openf1_session_status(session: dict[str, Any], now_utc: Optional[datetime] = None) -> str:
    now = now_utc or datetime.now(timezone.utc)
    start_dt = _parse_dt(session.get("date_start"))
    end_dt = _parse_dt(session.get("date_end"))
    if start_dt and now < start_dt:
        return "upcoming"
    if start_dt and end_dt and start_dt <= now <= end_dt:
        return "live"
    if end_dt and end_dt < now <= end_dt + timedelta(minutes=30):
        return "cooldown"
    if end_dt and now > end_dt:
        return "finished"
    return "scheduled"


def _summarize_openf1_session(session: dict[str, Any], now_utc: Optional[datetime] = None) -> dict[str, Any]:
    start_dt = _parse_dt(session.get("date_start"))
    end_dt = _parse_dt(session.get("date_end"))
    local_start = _fmt_local_offset(start_dt, session.get("gmt_offset"))
    local_end = _fmt_local_offset(end_dt, session.get("gmt_offset"))
    return {
        "session_key": session.get("session_key"),
        "meeting_key": session.get("meeting_key"),
        "session_name": session.get("session_name"),
        "session_type": session.get("session_type"),
        "location": session.get("location"),
        "country_name": session.get("country_name"),
        "circuit_short_name": session.get("circuit_short_name"),
        "date_start": session.get("date_start"),
        "date_end": session.get("date_end"),
        "date_start_local": local_start,
        "date_end_local": local_end,
        "status": _openf1_session_status(session, now_utc=now_utc),
    }


def _f1_rows_data_as_of(*row_groups: Any) -> Optional[str]:
    latest_dt: Optional[datetime] = None
    latest_raw: Optional[str] = None
    for group in row_groups:
        if not isinstance(group, list):
            continue
        for row in group:
            if not isinstance(row, dict):
                continue
            for key in ("date", "Utc", "utc", "Timestamp", "timestamp"):
                dt = _parse_dt(row.get(key))
                if dt is None:
                    continue
                if latest_dt is None or dt > latest_dt:
                    latest_dt = dt
                    latest_raw = row.get(key)
    if latest_raw not in (None, ""):
        return str(latest_raw)
    if latest_dt is not None:
        return latest_dt.astimezone(timezone.utc).isoformat()
    return None


def _score_openf1_meeting(meeting: dict[str, Any], target: Optional[dict[str, Any]]) -> float:
    if not isinstance(target, dict):
        return 0.0

    score = 0.0
    meeting_name = _f1_norm_text(meeting.get("meeting_name"))
    official_name = _f1_norm_text(meeting.get("meeting_official_name"))
    meeting_country = _f1_norm_text(meeting.get("country_name"))
    meeting_location = _f1_norm_text(meeting.get("location"))
    meeting_circuit = _f1_norm_text(meeting.get("circuit_short_name"))

    target_name = _f1_norm_text(target.get("race_name"))
    target_country = _f1_norm_text(target.get("country"))
    target_location = _f1_norm_text(target.get("locality"))
    target_circuit = _f1_norm_text(target.get("circuit"))
    target_dt = _parse_dt(target.get("datetime_utc")) or _parse_dt(_safe_iso_utc(target.get("date"), target.get("time")))
    meeting_start = _parse_dt(meeting.get("date_start"))

    if target_name and (target_name in meeting_name or target_name in official_name):
        score += 70.0
    if target_country and target_country == meeting_country:
        score += 30.0
    if target_location and (target_location == meeting_location or target_location in meeting_location or meeting_location in target_location):
        score += 25.0
    if target_circuit and (target_circuit == meeting_circuit or target_circuit in meeting_circuit or meeting_circuit in target_circuit):
        score += 25.0
    if target_dt and meeting_start:
        delta_days = abs((meeting_start - target_dt).total_seconds()) / 86400.0
        score += max(0.0, 20.0 - min(20.0, delta_days * 2.0))
    return score


def _pick_openf1_meeting(meetings: list[dict[str, Any]], next_race: Optional[dict[str, Any]], latest_race: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not meetings:
        return None

    now = datetime.now(timezone.utc)
    ordered = sorted(meetings, key=lambda m: _parse_dt(m.get("date_start")) or datetime.max.replace(tzinfo=timezone.utc))
    target = next_race if isinstance(next_race, dict) and next_race else latest_race if isinstance(latest_race, dict) else None

    if target:
        scored = sorted(
            ((meeting, _score_openf1_meeting(meeting, target)) for meeting in ordered),
            key=lambda item: item[1],
            reverse=True,
        )
        if scored and scored[0][1] >= 35.0:
            return scored[0][0]

    live_meeting = next(
        (
            meeting
            for meeting in ordered
            if (_parse_dt(meeting.get("date_start")) or now) - timedelta(hours=6) <= now <= (_parse_dt(meeting.get("date_end")) or now) + timedelta(hours=4)
        ),
        None,
    )
    if live_meeting:
        return live_meeting

    upcoming = [meeting for meeting in ordered if (_parse_dt(meeting.get("date_start")) or now) >= now - timedelta(hours=12)]
    if upcoming:
        return upcoming[0]

    return ordered[-1]


_F1_SCHEDULE_SESSION_SPECS: tuple[tuple[str, str, timedelta], ...] = (
    ("FirstPractice", "Practice 1", timedelta(hours=1)),
    ("SecondPractice", "Practice 2", timedelta(hours=1)),
    ("ThirdPractice", "Practice 3", timedelta(hours=1)),
    ("SprintQualifying", "Sprint Qualifying", timedelta(hours=1)),
    ("Sprint", "Sprint", timedelta(hours=1)),
    ("Qualifying", "Qualifying", timedelta(hours=1)),
    ("Race", "Race", timedelta(hours=2)),
)


def _build_f1_schedule_sessions(season: str, race: dict[str, Any], now_utc: Optional[datetime] = None) -> list[dict[str, Any]]:
    if not isinstance(race, dict):
        return []
    round_id = str(race.get("round") or "0").strip() or "0"
    meeting_key = f"fallback:{season}:{round_id}"
    locality = _dig(race, "Circuit", "Location", "locality")
    country = _dig(race, "Circuit", "Location", "country")
    circuit_name = _dig(race, "Circuit", "circuitName")
    rows: list[tuple[datetime, dict[str, Any]]] = []
    now_ref = now_utc or datetime.now(timezone.utc)
    for field_name, label, duration in _F1_SCHEDULE_SESSION_SPECS:
        node = race if field_name == "Race" else (race.get(field_name) if isinstance(race.get(field_name), dict) else {})
        date_raw = race.get("date") if field_name == "Race" else node.get("date")
        time_raw = race.get("time") if field_name == "Race" else node.get("time")
        start_iso = _safe_iso_utc(date_raw, time_raw)
        start_dt = _parse_dt(start_iso)
        if not start_iso or start_dt is None:
            continue
        end_dt = start_dt + duration
        session = {
            "session_key": f"{meeting_key}:{field_name.lower()}",
            "meeting_key": meeting_key,
            "session_name": label,
            "session_type": label,
            "location": locality,
            "country_name": country,
            "circuit_short_name": circuit_name,
            "date_start": start_iso,
            "date_end": end_dt.isoformat(),
            "gmt_offset": "+00:00:00",
        }
        rows.append((start_dt, _summarize_openf1_session(session, now_utc=now_ref)))
    rows.sort(key=lambda item: item[0])
    return [item[1] for item in rows]


def _pick_f1_schedule_weekend_race(season: str, races: list[dict[str, Any]]) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    candidates: list[tuple[datetime, datetime, dict[str, Any], list[dict[str, Any]]]] = []
    for race in races:
        if not isinstance(race, dict):
            continue
        sessions = _build_f1_schedule_sessions(season, race, now_utc=now)
        if not sessions:
            continue
        start_dt = _parse_dt(sessions[0].get("date_start"))
        end_dt = _parse_dt((sessions[-1].get("date_end") or sessions[-1].get("date_start")))
        if start_dt is None or end_dt is None:
            continue
        candidates.append((start_dt, end_dt, race, sessions))

    if not candidates:
        return None, []

    candidates.sort(key=lambda item: item[0])
    active = next((item for item in candidates if item[0] - timedelta(hours=6) <= now <= item[1] + timedelta(hours=4)), None)
    if active:
        return active[2], active[3]

    upcoming = [item for item in candidates if item[0] >= now - timedelta(hours=12)]
    if upcoming:
        return upcoming[0][2], upcoming[0][3]

    latest = candidates[-1]
    return latest[2], latest[3]


def _build_f1_schedule_fallback_context(
    season: str,
    races: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[str]]:
    race, sessions_summary = _pick_f1_schedule_weekend_race(season, races)
    if not isinstance(race, dict) or not sessions_summary:
        return {}, ["schedule_fallback:no_match"]

    locality = _dig(race, "Circuit", "Location", "locality")
    country = _dig(race, "Circuit", "Location", "country")
    circuit_name = _dig(race, "Circuit", "circuitName")
    lat, lon = _f1_schedule_race_coords(race)
    weather, weather_err = _fetch_open_meteo_weather(lat, lon)
    gmt_offset = _format_gmt_offset(weather.get("utc_offset_seconds")) if isinstance(weather, dict) else "+00:00:00"
    sessions_summary = [_apply_f1_session_offset(session, gmt_offset) for session in sessions_summary]
    current_session = next((session for session in sessions_summary if session.get("status") == "live"), None)
    next_session = next((session for session in sessions_summary if session.get("status") == "upcoming"), None)
    latest_session = next((session for session in reversed(sessions_summary) if session.get("status") in {"cooldown", "finished"}), None)
    focus_session = current_session or next_session or latest_session or sessions_summary[0]

    phase = "offseason"
    if current_session:
        phase = "live"
    elif next_session:
        phase = "upcoming"
    elif latest_session:
        phase = "completed"

    weekend_start = sessions_summary[0]
    weekend_end = sessions_summary[-1]
    headline = race.get("raceName") or "F1 Weekend"
    if phase == "live" and current_session:
        headline = f"{headline} | {current_session.get('session_name')}"
    elif phase == "upcoming" and next_session:
        headline = f"{headline} | Next up: {next_session.get('session_name')}"
    elif phase == "completed" and latest_session:
        headline = f"{headline} | Last session: {latest_session.get('session_name')}"

    fallback_errors = ["openf1:using_schedule_fallback"]
    if weather_err:
        fallback_errors.append(weather_err)

    return {
        "source": "schedule_fallback",
        "meeting": {
            "meeting_key": f"fallback:{season}:{race.get('round') or '0'}",
            "meeting_name": race.get("raceName"),
            "meeting_official_name": race.get("raceName"),
            "location": locality,
            "country_name": country,
            "country_code": None,
            "date_start": weekend_start.get("date_start"),
            "date_end": weekend_end.get("date_end") or weekend_end.get("date_start"),
            "gmt_offset": gmt_offset,
            "date_start_local": weekend_start.get("date_start_local") or weekend_start.get("date_start"),
            "date_end_local": weekend_end.get("date_end_local") or weekend_end.get("date_end") or weekend_end.get("date_start"),
        },
        "phase": phase,
        "headline": headline,
        "track": {
            "circuit_key": _dig(race, "Circuit", "circuitId"),
            "name": circuit_name,
            "type": None,
            "location": locality,
            "country_name": country,
            "country_code": None,
            "country_flag": None,
            "image_url": None,
            "info_url": race.get("url") or _dig(race, "Circuit", "url"),
            "links": _build_f1_circuit_links(circuit_name, locality, country),
        },
        "sessions": sessions_summary,
        "current_session_key": current_session.get("session_key") if isinstance(current_session, dict) else None,
        "focus_session_key": focus_session.get("session_key") if isinstance(focus_session, dict) else None,
        "current_session": current_session,
        "next_session": next_session,
        "latest_session": latest_session,
        "weather": {
            "air_temperature": weather.get("air_temperature"),
            "track_temperature": weather.get("track_temperature"),
            "humidity": weather.get("humidity"),
            "rainfall": weather.get("rainfall"),
            "wind_direction": weather.get("wind_direction"),
            "wind_speed": weather.get("wind_speed"),
            "pressure": weather.get("pressure"),
            "date": weather.get("date"),
            "source": weather.get("source") or "unavailable",
        },
    }, fallback_errors[:10]


def _fetch_f1_schedule_races(season: str) -> tuple[list[dict[str, Any]], list[str], str]:
    errors: list[str] = []
    source = "unknown"
    schedule_urls = [
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/races/"),
        ("ergast", f"https://ergast.com/api/f1/{season}.json"),
    ]
    for src, url in schedule_urls:
        data, err = _http_get_json(url, timeout=15)
        if err:
            errors.append(f"schedule:{src}:{err}")
            continue
        races = _dig(data, "MRData", "RaceTable", "Races") or []
        if isinstance(races, list):
            return [row for row in races if isinstance(row, dict)], errors[:12], src
        errors.append(f"schedule:{src}:invalid_payload")
    return [], errors[:12], source


def _fetch_openf1_season_sessions(season: str) -> tuple[list[dict[str, Any]], list[str]]:
    season_year = _safe_int(season) or datetime.now(timezone.utc).year
    errors: list[str] = []
    meetings, err = _openf1_get("meetings", params={"year": season_year}, timeout=20)
    if err:
        return [], [err]

    rows: list[dict[str, Any]] = []
    for meeting in meetings:
        meeting_key = meeting.get("meeting_key")
        if not meeting_key:
            continue
        sessions, err = _openf1_get("sessions", params={"meeting_key": meeting_key}, timeout=20)
        if err:
            errors.append(err)
            continue
        for session in sessions:
            summary = _summarize_openf1_session(session)
            summary["meeting_name"] = meeting.get("meeting_name") or meeting.get("meeting_official_name")
            summary["meeting_official_name"] = meeting.get("meeting_official_name")
            summary["year"] = season_year
            summary["round"] = meeting.get("meeting_key")
            rows.append(summary)
    rows.sort(key=lambda row: _parse_dt(row.get("date_start")) or datetime.min.replace(tzinfo=timezone.utc))
    return rows, errors[:24]


def _latest_by_key(rows: list[dict[str, Any]], key_name: str = "driver_number") -> dict[Any, dict[str, Any]]:
    latest: dict[Any, dict[str, Any]] = {}
    for row in rows:
        key = row.get(key_name)
        if key is None:
            continue
        latest[key] = row
    return latest


def _fetch_openf1_weekend_context(
    season: str,
    next_race: Optional[dict[str, Any]],
    latest_race: Optional[dict[str, Any]],
    schedule_races: Optional[list[dict[str, Any]]] = None,
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    season_year = _safe_int(season) or datetime.now(timezone.utc).year
    meetings, err = _openf1_get("meetings", params={"year": season_year}, timeout=20)
    if err:
        fallback, fallback_errors = _build_f1_schedule_fallback_context(season, list(schedule_races or []))
        if fallback:
            return fallback, [err, *fallback_errors][:10]
        return {}, [err]

    meeting = _pick_openf1_meeting(meetings, next_race, latest_race)
    if not isinstance(meeting, dict):
        fallback, fallback_errors = _build_f1_schedule_fallback_context(season, list(schedule_races or []))
        if fallback:
            return fallback, ["meetings:no_match", *fallback_errors][:10]
        return {}, ["meetings:no_match"]

    meeting_key = meeting.get("meeting_key")
    sessions, err = _openf1_get("sessions", params={"meeting_key": meeting_key}, timeout=20)
    if err:
        errors.append(err)
    now = datetime.now(timezone.utc)
    sessions_summary = [_summarize_openf1_session(session, now_utc=now) for session in sorted(sessions, key=lambda s: _parse_dt(s.get("date_start")) or now)]

    current_session = next((session for session in sessions_summary if session.get("status") == "live"), None)
    next_session = next((session for session in sessions_summary if session.get("status") == "upcoming"), None)
    latest_session = next((session for session in reversed(sessions_summary) if session.get("status") in {"cooldown", "finished"}), None)
    focus_session = current_session or next_session or latest_session or (sessions_summary[0] if sessions_summary else None)

    weather_rows, err = _openf1_get("weather", params={"meeting_key": meeting_key}, timeout=15)
    if err and "404" not in str(err):
        errors.append(err)
    latest_weather = weather_rows[-1] if weather_rows else {}
    matched_schedule_race = _find_f1_schedule_race_match(list(schedule_races or []), next_race, latest_race, meeting)
    weather_source = "openf1" if latest_weather else "unavailable"
    if not latest_weather and isinstance(matched_schedule_race, dict):
        lat, lon = _f1_schedule_race_coords(matched_schedule_race)
        backup_weather, weather_err = _fetch_open_meteo_weather(lat, lon)
        if _f1_weather_has_data(backup_weather):
            latest_weather = backup_weather
            weather_source = backup_weather.get("source") or "open_meteo"
        elif weather_err:
            errors.append(weather_err)

    phase = "offseason"
    if current_session:
        phase = "live"
    elif next_session:
        phase = "upcoming"
    elif latest_session:
        phase = "completed"

    track = {
        "circuit_key": meeting.get("circuit_key"),
        "name": meeting.get("circuit_short_name"),
        "type": meeting.get("circuit_type"),
        "location": meeting.get("location"),
        "country_name": meeting.get("country_name"),
        "country_code": meeting.get("country_code"),
        "country_flag": meeting.get("country_flag"),
        "image_url": meeting.get("circuit_image"),
        "info_url": meeting.get("circuit_info_url"),
        "links": _build_f1_circuit_links(meeting.get("circuit_short_name"), meeting.get("location"), meeting.get("country_name")),
    }

    headline = meeting.get("meeting_name") or meeting.get("meeting_official_name") or "F1 Weekend"
    if phase == "live" and current_session:
        headline = f"{headline} | {current_session.get('session_name')}"
    elif phase == "upcoming" and next_session:
        headline = f"{headline} | Next up: {next_session.get('session_name')}"
    elif phase == "completed" and latest_session:
        headline = f"{headline} | Last session: {latest_session.get('session_name')}"

    return {
        "source": "openf1",
        "meeting": {
            "meeting_key": meeting.get("meeting_key"),
            "meeting_name": meeting.get("meeting_name"),
            "meeting_official_name": meeting.get("meeting_official_name"),
            "location": meeting.get("location"),
            "country_name": meeting.get("country_name"),
            "country_code": meeting.get("country_code"),
            "date_start": meeting.get("date_start"),
            "date_end": meeting.get("date_end"),
            "gmt_offset": meeting.get("gmt_offset"),
            "date_start_local": _fmt_local_offset(_parse_dt(meeting.get("date_start")), meeting.get("gmt_offset")),
            "date_end_local": _fmt_local_offset(_parse_dt(meeting.get("date_end")), meeting.get("gmt_offset")),
        },
        "phase": phase,
        "headline": headline,
        "track": track,
        "sessions": sessions_summary,
        "current_session_key": current_session.get("session_key") if isinstance(current_session, dict) else None,
        "focus_session_key": focus_session.get("session_key") if isinstance(focus_session, dict) else None,
        "current_session": current_session,
        "next_session": next_session,
        "latest_session": latest_session,
        "weather": {
            "air_temperature": latest_weather.get("air_temperature"),
            "track_temperature": latest_weather.get("track_temperature"),
            "humidity": latest_weather.get("humidity"),
            "rainfall": latest_weather.get("rainfall"),
            "wind_direction": latest_weather.get("wind_direction"),
            "wind_speed": latest_weather.get("wind_speed"),
            "pressure": latest_weather.get("pressure"),
            "date": latest_weather.get("date"),
            "source": weather_source,
        },
    }, errors[:10]


def _fetch_f1_schedule_session_detail(session_key: str) -> tuple[dict[str, Any], list[str]]:
    parts = str(session_key or "").split(":")
    if len(parts) != 4 or parts[0] != "fallback":
        return {}, ["schedule_fallback:invalid_session_key"]

    _, season, round_id, field_name = parts
    schedule_races, schedule_errors, _ = _fetch_f1_schedule_races(season)
    race = next((row for row in schedule_races if str(row.get("round") or "").strip() == round_id), None)
    if not isinstance(race, dict):
        return {}, [*schedule_errors, f"schedule_fallback:round_not_found:{round_id}"][:12]

    sessions = _build_f1_schedule_sessions(season, race)
    session = next((row for row in sessions if str(row.get("session_key") or "").strip() == session_key), None)
    if not isinstance(session, dict):
        return {}, [*schedule_errors, f"schedule_fallback:session_not_found:{field_name}"][:12]

    return {
        "source": "schedule_fallback",
        "session": session,
        "live": str(session.get("status") or "").lower() in {"live", "cooldown"},
        "drivers_count": 0,
        "leaderboard": [],
        "race_control": [],
        "snapshots": _get_f1_session_snapshots(session_key, limit=10),
        "weather": {
            "air_temperature": None,
            "track_temperature": None,
            "humidity": None,
            "rainfall": None,
            "wind_direction": None,
            "wind_speed": None,
            "pressure": None,
            "date": None,
            "source": "unavailable",
        },
    }, [*schedule_errors, "openf1:session_schedule_fallback"][:12]


def _secondary_f1_session_payload_has_data(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if len(payload.get("leaderboard") or []) > 0:
        return True
    if len(payload.get("race_control") or []) > 0:
        return True
    return _f1_weather_has_data(payload.get("weather"))


def _fetch_openf1_session_detail(session_key: str) -> tuple[dict[str, Any], list[str]]:
    if str(session_key or "").startswith("fallback:"):
        return _fetch_f1_schedule_session_detail(session_key)

    errors: list[str] = []
    session_rows, err = _openf1_get("sessions", params={"session_key": session_key}, timeout=15)
    if err:
        return {}, [err]
    session = session_rows[0] if session_rows else {}
    if not isinstance(session, dict) or not session:
        return {}, ["sessions:not_found"]

    session_summary = _summarize_openf1_session(session)
    session_status = session_summary.get("status")

    drivers, err = _openf1_get("drivers", params={"session_key": session_key}, timeout=15)
    if err:
        errors.append(err)

    result_rows: list[dict[str, Any]] = []
    position_rows: list[dict[str, Any]] = []
    interval_rows: list[dict[str, Any]] = []
    stint_rows: list[dict[str, Any]] = []
    race_control_rows: list[dict[str, Any]] = []
    weather_rows: list[dict[str, Any]] = []

    # Upcoming sessions usually do not expose timing/result feeds yet.
    # Skip the expensive 404-heavy probes so the hub renders instantly.
    if session_status not in {"upcoming"}:
        result_rows, err = _openf1_get("session_result", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
        position_rows, err = _openf1_get("position", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
        interval_rows, err = _openf1_get("intervals", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
        stint_rows, err = _openf1_get("stints", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
        race_control_rows, err = _openf1_get("race_control", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
        weather_rows, err = _openf1_get("weather", params={"session_key": session_key}, timeout=15)
        if err:
            errors.append(err)
    driver_map = {
        driver.get("driver_number"): {
            "driver_number": driver.get("driver_number"),
            "driver": driver.get("full_name") or " ".join([str(driver.get("first_name") or "").strip(), str(driver.get("last_name") or "").strip()]).strip(),
            "code": driver.get("name_acronym"),
            "team": driver.get("team_name"),
            "team_color": driver.get("team_colour"),
            "headshot_url": driver.get("headshot_url"),
            "country_code": driver.get("country_code"),
        }
        for driver in drivers
        if driver.get("driver_number") is not None
    }

    latest_positions = _latest_by_key(position_rows)
    latest_intervals = _latest_by_key(interval_rows)
    latest_stints = _latest_by_key(stint_rows)

    leaderboard: list[dict[str, Any]] = []
    result_rows_sorted = sorted(
        [row for row in result_rows if row.get("driver_number") is not None],
        key=lambda row: (_safe_int(row.get("position")) or 999, _safe_int(row.get("driver_number")) or 999),
    )

    if result_rows_sorted:
        for row in result_rows_sorted:
            number = row.get("driver_number")
            driver = driver_map.get(number, {})
            interval = latest_intervals.get(number, {})
            stint = latest_stints.get(number, {})
            leaderboard.append(
                {
                    "position": row.get("position"),
                    "driver_number": number,
                    "driver": driver.get("driver"),
                    "code": driver.get("code"),
                    "team": driver.get("team"),
                    "team_color": driver.get("team_color"),
                    "grid_position": row.get("grid_position") or row.get("grid"),
                    "points": row.get("points"),
                    "status": "DSQ" if row.get("dsq") else "DNS" if row.get("dns") else "DNF" if row.get("dnf") else None,
                    "gap_to_leader": interval.get("gap_to_leader"),
                    "interval": interval.get("interval"),
                    "best_lap_time": row.get("best_lap_time"),
                    "time": row.get("time"),
                    "current_tyre": stint.get("compound"),
                    "stint": stint.get("stint_number"),
                }
            )
    else:
        merged_numbers = set(driver_map.keys()) | set(latest_positions.keys())
        provisional = []
        for number in merged_numbers:
            driver = driver_map.get(number, {})
            pos_row = latest_positions.get(number, {})
            interval = latest_intervals.get(number, {})
            stint = latest_stints.get(number, {})
            provisional.append(
                {
                    "position": pos_row.get("position"),
                    "driver_number": number,
                    "driver": driver.get("driver"),
                    "code": driver.get("code"),
                    "team": driver.get("team"),
                    "team_color": driver.get("team_color"),
                    "gap_to_leader": interval.get("gap_to_leader"),
                    "interval": interval.get("interval"),
                    "current_tyre": stint.get("compound"),
                    "stint": stint.get("stint_number"),
                    "time": pos_row.get("date"),
                }
            )
        leaderboard = sorted(provisional, key=lambda row: (_safe_int(row.get("position")) or 999, _safe_int(row.get("driver_number")) or 999))

    race_control_rows = sorted(race_control_rows, key=lambda row: _parse_dt(row.get("date")) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    race_control = [
        {
            "date": row.get("date"),
            "category": row.get("category"),
            "flag": row.get("flag"),
            "message": row.get("message"),
            "scope": row.get("scope"),
            "driver_number": row.get("driver_number"),
            "lap_number": row.get("lap_number"),
        }
        for row in race_control_rows[:12]
    ]

    latest_weather = weather_rows[-1] if weather_rows else {}
    weather_source = "openf1" if latest_weather else "unavailable"
    if not latest_weather:
        session_year = str((_parse_dt(session.get("date_start")) or datetime.now(timezone.utc)).year)
        schedule_races, schedule_errors, _ = _fetch_f1_schedule_races(session_year)
        errors.extend(schedule_errors[:2])
        matched_schedule_race = _find_f1_schedule_race_match(
            schedule_races,
            meeting={
                "location": session.get("location"),
                "country_name": session.get("country_name"),
                "circuit_short_name": session.get("circuit_short_name"),
            },
        )
        if isinstance(matched_schedule_race, dict):
            lat, lon = _f1_schedule_race_coords(matched_schedule_race)
            backup_weather, weather_err = _fetch_open_meteo_weather(lat, lon)
            if _f1_weather_has_data(backup_weather):
                latest_weather = backup_weather
                weather_source = backup_weather.get("source") or "open_meteo"
            elif weather_err:
                errors.append(weather_err)

    data_as_of = _f1_rows_data_as_of(
        position_rows,
        interval_rows,
        race_control_rows,
        weather_rows,
        result_rows,
    ) or latest_weather.get("date")

    return {
        "source": "openf1",
        "data_as_of": data_as_of,
        "session": session_summary,
        "live": session_status in {"live", "cooldown"},
        "drivers_count": len(driver_map),
        "leaderboard": leaderboard[:20],
        "race_control": race_control,
        "snapshots": _get_f1_session_snapshots(session_key, limit=10),
        "weather": {
            "air_temperature": latest_weather.get("air_temperature"),
            "track_temperature": latest_weather.get("track_temperature"),
            "humidity": latest_weather.get("humidity"),
            "rainfall": latest_weather.get("rainfall"),
            "wind_direction": latest_weather.get("wind_direction"),
            "wind_speed": latest_weather.get("wind_speed"),
            "pressure": latest_weather.get("pressure"),
            "date": latest_weather.get("date"),
            "source": weather_source,
        },
    }, errors[:12]


def _f1_driver_key(driver: dict[str, Any]) -> str:
    return str(driver.get("driverId") or driver.get("code") or driver.get("familyName") or "unknown").strip().lower()


def _fetch_f1_overview(season: str) -> tuple[dict[str, Any], list[str], str]:
    errors: list[str] = []
    source = "unknown"

    standings_urls = [
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/driverStandings.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/driverStandings/"),
        ("ergast", f"https://ergast.com/api/f1/{season}/driverStandings.json"),
    ]
    schedule_urls = [
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/races/"),
        ("ergast", f"https://ergast.com/api/f1/{season}.json"),
    ]
    results_urls = [
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/results.json?limit=500"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/results/?limit=500"),
        ("ergast", f"https://ergast.com/api/f1/{season}/results.json?limit=500"),
    ]

    standings_payload: Optional[dict[str, Any]] = None
    schedule_payload: Optional[dict[str, Any]] = None
    results_payload: Optional[dict[str, Any]] = None

    for src, url in standings_urls:
        data, err = _http_get_json(url, timeout=15)
        if err:
            errors.append(f"standings:{src}:{err}")
            continue
        if isinstance(_dig(data, "MRData", "StandingsTable"), dict):
            standings_payload = data
            source = src
            break
        errors.append(f"standings:{src}:invalid_payload")

    for src, url in schedule_urls:
        data, err = _http_get_json(url, timeout=15)
        if err:
            errors.append(f"schedule:{src}:{err}")
            continue
        if isinstance(_dig(data, "MRData", "RaceTable"), dict):
            schedule_payload = data
            if source == "unknown":
                source = src
            break
        errors.append(f"schedule:{src}:invalid_payload")

    for src, url in results_urls:
        data, err = _http_get_json(url, timeout=15)
        if err:
            errors.append(f"results:{src}:{err}")
            continue
        if isinstance(_dig(data, "MRData", "RaceTable"), dict):
            results_payload = data
            if source == "unknown":
                source = src
            break
        errors.append(f"results:{src}:invalid_payload")

    standings_out: list[dict[str, Any]] = []
    standings_lists = _dig(standings_payload, "MRData", "StandingsTable", "StandingsLists") or []
    first_list = standings_lists[0] if isinstance(standings_lists, list) and standings_lists else {}
    if isinstance(first_list, dict):
        for row in first_list.get("DriverStandings", [])[:20]:
            drv = row.get("Driver", {}) if isinstance(row.get("Driver"), dict) else {}
            constructors = row.get("Constructors", [])
            team = ""
            if isinstance(constructors, list) and constructors:
                c0 = constructors[0] if isinstance(constructors[0], dict) else {}
                team = str(c0.get("name", ""))

            standings_out.append(
                {
                    "position": row.get("position"),
                    "driver": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "code": drv.get("code"),
                    "driver_id": drv.get("driverId"),
                    "nationality": drv.get("nationality"),
                    "date_of_birth": drv.get("dateOfBirth"),
                    "team": team,
                    "points": row.get("points"),
                    "wins": row.get("wins"),
                    "profile_key": _f1_driver_key(drv),
                }
            )

    races = _dig(schedule_payload, "MRData", "RaceTable", "Races") or []
    upcoming: list[dict[str, Any]] = []
    track_guide: list[dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc)
    next_race: Optional[dict[str, Any]] = None

    for race in races:
        if not isinstance(race, dict):
            continue
        iso = _safe_iso_utc(race.get("date"), race.get("time"))
        circuit_name = _dig(race, "Circuit", "circuitName")
        locality = _dig(race, "Circuit", "Location", "locality")
        country = _dig(race, "Circuit", "Location", "country")
        race_item = {
            "round": race.get("round"),
            "race_name": race.get("raceName"),
            "datetime_utc": iso,
            "circuit": circuit_name,
            "circuit_id": _dig(race, "Circuit", "circuitId"),
            "locality": locality,
            "country": country,
            "links": _build_f1_circuit_links(circuit_name, locality, country),
        }

        track_guide.append(race_item)
        if iso:
            try:
                dt = datetime.fromisoformat(iso)
                if dt >= now_utc and next_race is None:
                    next_race = race_item
                if dt >= now_utc:
                    upcoming.append(race_item)
            except Exception:
                pass

    if not next_race and upcoming:
        next_race = upcoming[0]

    results_races = _dig(results_payload, "MRData", "RaceTable", "Races") or []
    latest_race_raw = results_races[-1] if isinstance(results_races, list) and results_races else None

    driver_profiles: dict[str, dict[str, Any]] = {}
    latest_by_driver: dict[str, dict[str, Any]] = {}

    for race in results_races:
        if not isinstance(race, dict):
            continue

        try:
            round_num = int(race.get("round") or 0)
        except Exception:
            round_num = 0

        for res in race.get("Results", []) or []:
            if not isinstance(res, dict):
                continue
            drv = res.get("Driver", {}) if isinstance(res.get("Driver"), dict) else {}
            constructor = res.get("Constructor", {}) if isinstance(res.get("Constructor"), dict) else {}
            key = _f1_driver_key(drv)

            profile = driver_profiles.setdefault(
                key,
                {
                    "profile_key": key,
                    "driver": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "code": drv.get("code"),
                    "driver_id": drv.get("driverId"),
                    "nationality": drv.get("nationality"),
                    "date_of_birth": drv.get("dateOfBirth"),
                    "team": constructor.get("name"),
                    "points": None,
                    "wins": 0,
                    "podiums": 0,
                    "top10": 0,
                    "dnf_count": 0,
                    "finish_count": 0,
                    "race_count": 0,
                    "avg_grid": None,
                    "avg_finish": None,
                    "finish_rate": None,
                    "championship_position": None,
                    "recent_results": [],
                    "points_history": [],
                    "_sum_grid": 0.0,
                    "_cnt_grid": 0,
                    "_sum_finish": 0.0,
                    "_cnt_finish": 0,
                    "_season_points_acc": 0.0,
                },
            )

            status = str(res.get("status") or "")
            try:
                points_gain = float(res.get("points") or 0.0)
            except Exception:
                points_gain = 0.0

            profile["_season_points_acc"] += points_gain
            profile["race_count"] = int(profile.get("race_count") or 0) + 1

            pos_raw = res.get("position")
            try:
                pos_int = int(pos_raw)
            except Exception:
                pos_int = None

            grid_raw = res.get("grid")
            try:
                grid_int = int(grid_raw)
            except Exception:
                grid_int = None

            if pos_int is not None:
                if pos_int <= 3:
                    profile["podiums"] = int(profile.get("podiums") or 0) + 1
                if pos_int <= 10:
                    profile["top10"] = int(profile.get("top10") or 0) + 1
                profile["_sum_finish"] += pos_int
                profile["_cnt_finish"] = int(profile.get("_cnt_finish") or 0) + 1

            if grid_int is not None and grid_int >= 0:
                profile["_sum_grid"] += grid_int
                profile["_cnt_grid"] = int(profile.get("_cnt_grid") or 0) + 1

            if _f1_status_is_finish(status):
                profile["finish_count"] = int(profile.get("finish_count") or 0) + 1
            else:
                profile["dnf_count"] = int(profile.get("dnf_count") or 0) + 1

            result_item = {
                "round": race.get("round"),
                "race_name": race.get("raceName"),
                "position": res.get("position"),
                "grid": res.get("grid"),
                "status": status,
                "points": res.get("points"),
                "time": _dig(res, "Time", "time"),
            }
            profile["recent_results"].append(result_item)
            profile["points_history"].append({"round": round_num, "points": round(profile["_season_points_acc"], 1)})
            latest_by_driver[key] = result_item

    latest_race_out: Optional[dict[str, Any]] = None
    if isinstance(latest_race_raw, dict):
        latest_results = []
        for res in latest_race_raw.get("Results", []) or []:
            if not isinstance(res, dict):
                continue
            drv = res.get("Driver", {}) if isinstance(res.get("Driver"), dict) else {}
            constructor = res.get("Constructor", {}) if isinstance(res.get("Constructor"), dict) else {}
            latest_results.append(
                {
                    "position": res.get("position"),
                    "driver": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                    "code": drv.get("code"),
                    "team": constructor.get("name"),
                    "grid": res.get("grid"),
                    "status": res.get("status"),
                    "points": res.get("points"),
                    "time": _dig(res, "Time", "time"),
                }
            )

        latest_race_out = {
            "round": latest_race_raw.get("round"),
            "race_name": latest_race_raw.get("raceName"),
            "date": latest_race_raw.get("date"),
            "time": latest_race_raw.get("time"),
            "circuit": _dig(latest_race_raw, "Circuit", "circuitName"),
            "locality": _dig(latest_race_raw, "Circuit", "Location", "locality"),
            "country": _dig(latest_race_raw, "Circuit", "Location", "country"),
            "results": latest_results[:20],
        }

    for row in standings_out:
        key = str(row.get("profile_key") or "")
        if not key:
            continue
        profile = driver_profiles.setdefault(
            key,
            {
                "profile_key": key,
                "driver": row.get("driver"),
                "code": row.get("code"),
                "driver_id": row.get("driver_id"),
                "nationality": row.get("nationality"),
                "date_of_birth": row.get("date_of_birth"),
                "team": row.get("team"),
                "points": None,
                "wins": 0,
                "podiums": 0,
                "top10": 0,
                "dnf_count": 0,
                "finish_count": 0,
                "race_count": 0,
                "avg_grid": None,
                "avg_finish": None,
                "finish_rate": None,
                "championship_position": None,
                "recent_results": [],
                "points_history": [],
                "_sum_grid": 0.0,
                "_cnt_grid": 0,
                "_sum_finish": 0.0,
                "_cnt_finish": 0,
                "_season_points_acc": 0.0,
            },
        )

        profile["points"] = row.get("points")
        profile["wins"] = int(row.get("wins") or 0)
        profile["team"] = row.get("team") or profile.get("team")
        try:
            profile["championship_position"] = int(row.get("position") or 999)
        except Exception:
            profile["championship_position"] = 999

    driver_profiles_list: list[dict[str, Any]] = []
    for profile in driver_profiles.values():
        race_count = int(profile.get("race_count") or 0)
        cnt_grid = int(profile.get("_cnt_grid") or 0)
        cnt_finish = int(profile.get("_cnt_finish") or 0)
        finish_count = int(profile.get("finish_count") or 0)

        profile["avg_grid"] = round(profile["_sum_grid"] / cnt_grid, 2) if cnt_grid else None
        profile["avg_finish"] = round(profile["_sum_finish"] / cnt_finish, 2) if cnt_finish else None
        profile["finish_rate"] = round((finish_count / race_count) * 100.0, 1) if race_count else None
        profile["latest_result"] = latest_by_driver.get(str(profile.get("profile_key") or ""))
        profile["recent_results"] = list(reversed(profile.get("recent_results", [])[-5:]))

        profile.pop("_sum_grid", None)
        profile.pop("_cnt_grid", None)
        profile.pop("_sum_finish", None)
        profile.pop("_cnt_finish", None)
        profile.pop("_season_points_acc", None)

        driver_profiles_list.append(profile)

    driver_profiles_list = sorted(
        driver_profiles_list,
        key=lambda x: (
            int(x.get("championship_position") or 999),
            -float(x.get("points") or 0.0),
            x.get("driver") or "",
        ),
    )

    return {
        "standings": standings_out,
        "next_race": next_race,
        "upcoming_races": upcoming[:8],
        "track_guide": track_guide[:14],
        "latest_race": latest_race_out,
        "driver_profiles": driver_profiles_list,
        "_schedule_races": [race for race in races if isinstance(race, dict)],
    }, errors[:20], source


def _normalize_warframe_item_name(item: str) -> str:
    s = str(item or "").lower().strip()
    s = s.replace("&", " and ").replace("'", " ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


_WARFRAME_MARKET_WATCHLIST = [
    "Arcane Energize",
    "Arcane Grace",
    "Arcane Avenger",
    "Arcane Fury",
    "Arcane Strike",
    "Arcane Guardian",
    "Arcane Blessing",
    "Arcane Nullifier",
    "Molt Augmented",
    "Primary Merciless",
    "Melee Duplicate",
    "Revenant Prime Set",
    "Wisp Prime Set",
    "Gauss Prime Set",
    "Saryn Prime Set",
    "Nekros Prime Set",
    "Xaku Prime Set",
    "Glaive Prime Set",
]


def _fetch_warframe_market_catalog() -> tuple[list[dict[str, Any]], list[str]]:
    cache_key = "warframe:market:catalog:v2"
    cached = _cache_get(cache_key, ttl_seconds=24 * 3600)
    if isinstance(cached, dict):
        return list(cached.get("items") or []), list(cached.get("errors") or [])

    raw, err = _http_get_json("https://api.warframe.market/v2/items", timeout=20)
    if err:
        return [], [f"catalog:{err}"]

    payload = raw.get("data") if isinstance(raw, dict) else []
    if not isinstance(payload, list):
        return [], ["catalog:invalid_payload"]

    items: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        i18n = item.get("i18n") or {}
        en = i18n.get("en") if isinstance(i18n, dict) else {}
        if not isinstance(en, dict):
            en = {}
        name = en.get("name") or item.get("slug") or "Unknown Item"
        items.append(
            {
                "id": item.get("id"),
                "slug": item.get("slug"),
                "name": name,
                "thumb": en.get("thumb"),
                "icon": en.get("icon"),
                "rarity": item.get("rarity"),
                "max_rank": item.get("maxRank"),
                "tradable": bool(item.get("tradable", True)),
                "tags": item.get("tags") if isinstance(item.get("tags"), list) else [],
            }
        )

    _cache_set(cache_key, {"items": items, "errors": []})
    return items, []


def _find_warframe_market_item(catalog: list[dict[str, Any]], item: str) -> Optional[dict[str, Any]]:
    query = str(item or "").strip()
    if not query:
        return None
    q_slug = _normalize_warframe_item_name(query)
    q_name = query.casefold()

    for entry in catalog:
        slug = str(entry.get("slug") or "")
        name = str(entry.get("name") or "")
        if slug == q_slug or name.casefold() == q_name:
            return entry

    for entry in catalog:
        slug = str(entry.get("slug") or "")
        name = str(entry.get("name") or "")
        if q_slug in slug or q_name in name.casefold():
            return entry

    return None


def _extract_warframe_stats_history(stats_payload: Any, max_rank: Optional[int] = None) -> tuple[list[dict[str, Any]], Optional[str], Optional[int]]:
    stats_closed = _dig(stats_payload, "payload", "statistics_closed") or {}
    if not isinstance(stats_closed, dict):
        return [], None, None

    period_key = "48hours" if isinstance(stats_closed.get("48hours"), list) and stats_closed.get("48hours") else None
    if not period_key and isinstance(stats_closed.get("90days"), list) and stats_closed.get("90days"):
        period_key = "90days"
    raw_points = stats_closed.get(period_key, []) if period_key else []
    if not isinstance(raw_points, list):
        return [], period_key, None

    rank_volume: dict[int, int] = {}
    has_rank_dimension = False
    for point in raw_points:
        if not isinstance(point, dict):
            continue
        if point.get("mod_rank") is None:
            continue
        has_rank_dimension = True
        try:
            rank = int(point.get("mod_rank") or 0)
        except Exception:
            rank = 0
        try:
            volume = int(point.get("volume") or 0)
        except Exception:
            volume = 0
        rank_volume[rank] = rank_volume.get(rank, 0) + volume

    selected_rank: Optional[int] = None
    if has_rank_dimension and rank_volume:
        if max_rank is not None and int(max_rank) in rank_volume:
            selected_rank = int(max_rank)
        else:
            selected_rank = max(rank_volume.items(), key=lambda item: (item[1], item[0]))[0]

    history: list[dict[str, Any]] = []
    for point in raw_points[-60:]:
        if not isinstance(point, dict):
            continue
        if selected_rank is not None:
            try:
                point_rank = int(point.get("mod_rank") or 0)
            except Exception:
                point_rank = 0
            if point_rank != selected_rank:
                continue
        avg_price = point.get("avg_price")
        if avg_price is None:
            continue
        history.append(
            {
                "datetime": point.get("datetime"),
                "avg_price": avg_price,
                "volume": point.get("volume"),
                "min_price": point.get("min_price"),
                "max_price": point.get("max_price"),
                "closed_price": point.get("closed_price"),
                "wa_price": point.get("wa_price"),
                "median": point.get("median"),
                "mod_rank": point.get("mod_rank"),
            }
        )

    return history, period_key, selected_rank


def _summarize_warframe_statistics(stats_payload: Any, *, max_rank: Optional[int] = None) -> dict[str, Any]:
    history, period_key, selected_rank = _extract_warframe_stats_history(stats_payload, max_rank=max_rank)
    latest = history[-1] if history else {}
    first = history[0] if history else {}

    first_avg = first.get("avg_price")
    latest_avg = latest.get("avg_price")
    price_change_pct = None
    try:
        if first_avg not in (None, 0) and latest_avg is not None:
            price_change_pct = round(((float(latest_avg) - float(first_avg)) / float(first_avg)) * 100.0, 1)
    except Exception:
        price_change_pct = None

    volume_total = 0
    for point in history:
        try:
            volume_total += int(point.get("volume") or 0)
        except Exception:
            continue

    return {
        "history": history,
        "history_period": period_key,
        "selected_rank": selected_rank,
        "last_avg_price": latest.get("avg_price"),
        "last_closed_price": latest.get("closed_price"),
        "last_volume": latest.get("volume"),
        "price_floor_estimate": latest.get("min_price"),
        "price_ceiling_estimate": latest.get("max_price"),
        "price_change_pct": price_change_pct,
        "volume_total": volume_total,
    }


def _fetch_warframe_hot_items(platform: str = "pc") -> tuple[list[dict[str, Any]], list[str]]:
    cache_key = f"warframe:hot_items:{platform}"
    cached = _cache_get(cache_key, ttl_seconds=10 * 60)
    if isinstance(cached, dict):
        return list(cached.get("items") or []), list(cached.get("errors") or [])

    catalog, catalog_errors = _fetch_warframe_market_catalog()
    selected_items: list[dict[str, Any]] = []
    for name in _WARFRAME_MARKET_WATCHLIST:
        found = _find_warframe_market_item(catalog, name)
        if found and found not in selected_items:
            selected_items.append(found)

    errors = list(catalog_errors)
    hot_items: list[dict[str, Any]] = []

    def load_item(entry: dict[str, Any]) -> tuple[Optional[dict[str, Any]], list[str]]:
        slug = str(entry.get("slug") or "")
        raw, err = _http_get_json(
            f"https://api.warframe.market/v1/items/{slug}/statistics",
            params={"platform": platform},
            headers={"Accept": "application/json", "Language": "en", "User-Agent": "HolocronHub/0.5"},
            timeout=12,
        )
        if err:
            return None, [f"hot:{slug}:{err}"]
        summary = _summarize_warframe_statistics(raw, max_rank=entry.get("max_rank"))
        if not summary.get("history"):
            return None, [f"hot:{slug}:no_history"]
        hot = {
            "name": entry.get("name") or slug.replace("_", " ").title(),
            "slug": slug,
            "rarity": entry.get("rarity"),
            "thumb": entry.get("thumb"),
            "icon": entry.get("icon"),
            "selected_rank": summary.get("selected_rank"),
            "last_avg_price": summary.get("last_avg_price"),
            "last_closed_price": summary.get("last_closed_price"),
            "price_change_pct": summary.get("price_change_pct"),
            "volume_48h": summary.get("volume_total") or 0,
            "history": summary.get("history") or [],
            "history_period": summary.get("history_period"),
        }
        local_history = _record_warframe_market_snapshot(
            {
                "item": entry.get("name") or slug.replace("_", " ").title(),
                "canonical_name": entry.get("name") or slug.replace("_", " ").title(),
                "slug": slug,
                "last_avg_price": summary.get("last_avg_price"),
                "best_sell": summary.get("price_floor_estimate"),
                "best_buy": None,
                "volume_total": summary.get("volume_total"),
                "price_change_pct": summary.get("price_change_pct"),
                "buy_count_live": 0,
                "sell_count_live": 0,
                "snapshot_source": "statistics_fallback",
            },
            platform,
        )
        if local_history:
            hot["local_history"] = local_history
        return hot, []

    with ThreadPoolExecutor(max_workers=min(6, max(1, len(selected_items)))) as pool:
        futures = {pool.submit(load_item, entry): entry for entry in selected_items}
        for future in as_completed(futures):
            try:
                item_payload, item_errors = future.result()
            except Exception as exc:
                errors.append(f"hot:worker:{exc}")
                continue
            errors.extend(item_errors[:2])
            if item_payload:
                hot_items.append(item_payload)

    hot_items.sort(
        key=lambda item: (
            int(item.get("volume_48h") or 0),
            float(item.get("last_avg_price") or 0.0),
            str(item.get("name") or ""),
        ),
        reverse=True,
    )
    hot_items = hot_items[:16]
    trimmed_errors = errors[:16]
    payload = {"items": hot_items, "errors": trimmed_errors}
    if hot_items:
        _cache_set(cache_key, payload)
        _set_last_good(cache_key, payload)
        return hot_items, trimmed_errors

    stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
    if isinstance(stale_payload, dict):
        fallback_errors = list(trimmed_errors or []) + [f"fallback:last_good:hot_items:{age}s"]
        stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
        stale_payload["errors"] = fallback_errors[:16]
        _cache_set(cache_key, stale_payload)
        return list(stale_payload.get("items") or []), fallback_errors[:16]



def _clean_warframe_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\|[A-Z0-9_]+\|", "", text)
    text = text.replace("\n", " ").replace("\r", " ")
    return re.sub(r"\s+", " ", text).strip()


def _warframe_image_url(image_name: Any) -> Optional[str]:
    image = str(image_name or "").strip()
    if not image:
        return None
    return f"https://cdn.warframestat.us/img/{quote(image)}"


def _fetch_warframe_arsenal(query: str = "") -> tuple[dict[str, Any], list[str]]:
    cache_key = "warframe:arsenal:catalog"
    cached = _cache_get(cache_key, ttl_seconds=12 * 3600)
    errors: list[str] = []
    frames_raw: list[dict[str, Any]] = []

    if isinstance(cached, dict):
        frames_raw = list(cached.get("frames") or [])
        errors = list(cached.get("errors") or [])
    else:
        raw, err = _http_get_json(
            "https://raw.githubusercontent.com/WFCD/warframe-items/master/data/json/Warframes.json",
            timeout=20,
        )
        if err:
            return {"query": query, "count": 0, "total": 0, "items": []}, [f"arsenal:{err}"]
        if not isinstance(raw, list):
            return {"query": query, "count": 0, "total": 0, "items": []}, ["arsenal:invalid_payload"]
        frames_raw = [item for item in raw if isinstance(item, dict)]
        _cache_set(cache_key, {"frames": frames_raw, "errors": []})

    market_catalog, market_errors = _fetch_warframe_market_catalog()
    errors.extend(market_errors[:4])
    frames: list[dict[str, Any]] = []
    q_norm = _normalize_warframe_item_name(query)
    for item in frames_raw:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        name_norm = _normalize_warframe_item_name(name)
        if q_norm and q_norm not in name_norm:
            continue

        abilities = []
        for ability in item.get("abilities") or []:
            if not isinstance(ability, dict):
                continue
            abilities.append(
                {
                    "name": ability.get("name"),
                    "description": _clean_warframe_text(ability.get("description")),
                    "image_url": _warframe_image_url(ability.get("imageName")),
                }
            )

        components = []
        for component in item.get("components") or []:
            if not isinstance(component, dict):
                continue
            comp_name = str(component.get("name") or component.get("itemCount") or "").strip()
            if comp_name:
                components.append(comp_name)

        market_set = None
        market_candidates = [f"{name} Set"]
        if not bool(item.get("isPrime")) and "prime" not in name.casefold():
            market_candidates.insert(0, f"{name} Prime Set")
        for candidate in market_candidates:
            market_set = _find_warframe_market_item(market_catalog, candidate)
            if market_set:
                break

        frames.append(
            {
                "name": name,
                "is_prime": bool(item.get("isPrime")),
                "description": _clean_warframe_text(item.get("description")),
                "passive": _clean_warframe_text(item.get("passiveDescription")),
                "image_url": _warframe_image_url(item.get("imageName")),
                "wiki_url": item.get("wikiaUrl"),
                "health": item.get("health"),
                "shield": item.get("shield"),
                "armor": item.get("armor"),
                "energy": item.get("power"),
                "sprint": item.get("sprintSpeed") or item.get("sprint"),
                "aura": item.get("aura"),
                "mastery_req": item.get("masteryReq"),
                "introduced": item.get("introduced") or item.get("releaseDate"),
                "polarities": item.get("polarities") if isinstance(item.get("polarities"), list) else [],
                "market_cost": item.get("marketCost"),
                "build_time": item.get("buildTime"),
                "components": components[:8],
                "market_set_slug": (market_set or {}).get("slug"),
                "abilities": abilities[:4],
            }
        )

    frames.sort(key=lambda frame: (0 if q_norm and _normalize_warframe_item_name(frame.get("name") or "") == q_norm else 1, str(frame.get("name") or "")))
    total = len(frames)
    limit = 18 if not q_norm else 24
    return {
        "query": query,
        "count": min(total, limit),
        "total": total,
        "items": frames[:limit],
    }, errors[:8]


def _fetch_warframe_drop_dataset(dataset_name: str) -> tuple[list[dict[str, Any]], list[str]]:
    cache_key = f"warframe:drops:{dataset_name}"
    cached = _cache_get(cache_key, ttl_seconds=6 * 3600)
    if isinstance(cached, dict):
        return list(cached.get("items") or []), list(cached.get("errors") or [])

    raw, err = _http_get_json(f"https://drops.warframestat.us/data/{dataset_name}.json", timeout=20)
    if err:
        return [], [f"drops:{dataset_name}:{err}"]
    payload = raw.get(dataset_name) if isinstance(raw, dict) else None
    if not isinstance(payload, list):
        return [], [f"drops:{dataset_name}:invalid_payload"]
    items = [item for item in payload if isinstance(item, dict)]
    _cache_set(cache_key, {"items": items, "errors": []})
    return items, []


def _aggregate_relic_states(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    order = {"Intact": 0, "Exceptional": 1, "Flawless": 2, "Radiant": 3}
    for row in rows:
        key = (str(row.get("relic") or ""), str(row.get("item_name") or ""), str(row.get("rarity") or ""))
        ent = grouped.setdefault(key, {
            "relic": row.get("relic"),
            "item_name": row.get("item_name"),
            "rarity": row.get("rarity"),
            "best_state": row.get("state"),
            "best_chance": row.get("chance"),
            "states": [],
        })
        ent["states"].append({"state": row.get("state"), "chance": row.get("chance")})
        cur_order = order.get(str(row.get("state") or ""), -1)
        best_order = order.get(str(ent.get("best_state") or ""), -1)
        try:
            row_chance = float(row.get("chance") or 0)
            best_chance = float(ent.get("best_chance") or 0)
        except Exception:
            row_chance = 0.0
            best_chance = 0.0
        if row_chance > best_chance or (row_chance == best_chance and cur_order > best_order):
            ent["best_state"] = row.get("state")
            ent["best_chance"] = row.get("chance")

    for ent in grouped.values():
        ent["states"] = sorted(
            ent.get("states") or [],
            key=lambda row: (float(row.get("chance") or 0.0), order.get(str(row.get("state") or ""), -1)),
            reverse=True,
        )

    out = list(grouped.values())
    out.sort(key=lambda row: (float(row.get("best_chance") or 0.0), str(row.get("relic") or "")), reverse=True)
    return out[:10]


def _fetch_warframe_value_snapshot(item: str, platform: str = "pc", *, catalog: Optional[list[dict[str, Any]]] = None) -> tuple[dict[str, Any], list[str]]:
    item_key = _normalize_warframe_item_name(item)
    cache_key = f"warframe:value:{platform}:{item_key}"
    cached = _cache_get(cache_key, ttl_seconds=10 * 60)
    if isinstance(cached, dict):
        return cached, []

    catalog_errors: list[str] = []
    catalog_data = catalog
    if catalog_data is None:
        catalog_data, catalog_errors = _fetch_warframe_market_catalog()
    item_meta = _find_warframe_market_item(catalog_data or [], item)
    slug = str((item_meta or {}).get("slug") or item_key)
    canonical_name = str((item_meta or {}).get("name") or item or "Unknown Item")
    max_rank = (item_meta or {}).get("max_rank")
    if not slug:
        return {"item": item, "canonical_name": canonical_name, "slug": "", "last_avg_price": None, "volume_total": 0}, ["invalid_item"]

    req_headers = {"Accept": "application/json", "Language": "en", "User-Agent": "HolocronHub/0.5"}
    stats_data, stats_err = _http_get_json(
        f"https://api.warframe.market/v1/items/{slug}/statistics",
        params={"platform": platform},
        headers=req_headers,
        timeout=15,
    )
    stats_summary = _summarize_warframe_statistics(stats_data, max_rank=max_rank)
    price = stats_summary.get("last_avg_price")
    if price is None:
        price = stats_summary.get("price_floor_estimate")
    if price is None:
        price = stats_summary.get("last_closed_price")
    payload = {
        "item": item,
        "canonical_name": canonical_name,
        "slug": slug,
        "selected_rank": stats_summary.get("selected_rank"),
        "last_avg_price": price,
        "volume_total": stats_summary.get("volume_total") or 0,
        "price_change_pct": stats_summary.get("price_change_pct"),
    }
    _cache_set(cache_key, payload)
    errors = list(catalog_errors)
    if stats_err:
        errors.append(f"statistics:{stats_err}")
    return payload, errors[:8]


def _compute_relic_value_profiles(relic_names: list[str], *, platform: str = "pc", target_item: str = "") -> tuple[dict[str, dict[str, Any]], list[str]]:
    target_set = {str(name or "").strip() for name in relic_names if str(name or "").strip()}
    if not target_set:
        return {}, []

    relics, relic_errors = _fetch_warframe_drop_dataset("relics")
    catalog, catalog_errors = _fetch_warframe_market_catalog()
    order = {"Intact": 0, "Exceptional": 1, "Flawless": 2, "Radiant": 3}
    target_norm = _normalize_warframe_item_name(target_item)

    relic_state_map: dict[str, dict[str, list[dict[str, Any]]]] = {}
    reward_names: set[str] = set()
    for relic in relics:
        relic_name = f"{relic.get('tier') or ''} {relic.get('relicName') or ''}".strip()
        if relic_name not in target_set:
            continue
        state = str(relic.get("state") or "") or "Intact"
        rewards_out: list[dict[str, Any]] = []
        for reward in relic.get("rewards") or []:
            if not isinstance(reward, dict):
                continue
            item_name = str(reward.get("itemName") or "").strip()
            if not item_name:
                continue
            reward_names.add(item_name)
            rewards_out.append(
                {
                    "item_name": item_name,
                    "rarity": reward.get("rarity"),
                    "chance": reward.get("chance"),
                }
            )
        relic_state_map.setdefault(relic_name, {})[state] = rewards_out

    reward_values: dict[str, dict[str, Any]] = {}
    errors: list[str] = list(relic_errors) + list(catalog_errors)

    def load_reward_value(name: str) -> tuple[str, dict[str, Any], list[str]]:
        payload, reward_errors = _fetch_warframe_value_snapshot(name, platform, catalog=catalog)
        return name, payload, reward_errors

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(reward_names)))) as pool:
        futures = [pool.submit(load_reward_value, name) for name in sorted(reward_names)]
        for future in as_completed(futures):
            try:
                reward_name, payload, reward_errors = future.result()
            except Exception as exc:
                errors.append(f"relic_value:worker:{exc}")
                continue
            reward_values[reward_name] = payload
            errors.extend((reward_errors or [])[:2])

    profiles: dict[str, dict[str, Any]] = {}
    for relic_name, states in relic_state_map.items():
        state_values: list[dict[str, Any]] = []
        jackpot: Optional[dict[str, Any]] = None
        best_value_state: Optional[dict[str, Any]] = None
        best_target_state: Optional[dict[str, Any]] = None

        for state_name in sorted(states.keys(), key=lambda key: order.get(key, 99)):
            rewards = states.get(state_name) or []
            expected_value = 0.0
            target_chance = 0.0
            target_expected_value = 0.0
            priced_rewards: list[dict[str, Any]] = []
            for reward in rewards:
                value = reward_values.get(str(reward.get("item_name") or ""), {})
                try:
                    chance = float(reward.get("chance") or 0.0)
                except Exception:
                    chance = 0.0
                try:
                    price = float(value.get("last_avg_price") or 0.0)
                except Exception:
                    price = 0.0
                expected_value += (chance / 100.0) * price
                reward_out = {
                    "item_name": reward.get("item_name"),
                    "rarity": reward.get("rarity"),
                    "chance": chance,
                    "price": round(price, 2),
                    "slug": value.get("slug"),
                }
                priced_rewards.append(reward_out)
                if (not jackpot) or price > float(jackpot.get("price") or 0.0):
                    jackpot = reward_out
                reward_norm = _normalize_warframe_item_name(reward.get("item_name") or "")
                if target_norm and (target_norm in reward_norm or reward_norm in target_norm):
                    target_chance = chance
                    target_expected_value = (chance / 100.0) * price

            priced_rewards.sort(key=lambda reward: (float(reward.get("price") or 0.0), float(reward.get("chance") or 0.0)), reverse=True)
            summary = {
                "state": state_name,
                "expected_value": round(expected_value, 2),
                "target_chance": round(target_chance, 2),
                "target_expected_value": round(target_expected_value, 2),
                "top_rewards": priced_rewards[:3],
            }
            state_values.append(summary)

            if (best_value_state is None) or float(summary.get("expected_value") or 0.0) > float(best_value_state.get("expected_value") or 0.0):
                best_value_state = summary
            if (best_target_state is None) or float(summary.get("target_expected_value") or 0.0) > float(best_target_state.get("target_expected_value") or 0.0):
                best_target_state = summary

        profiles[relic_name] = {
            "states": state_values,
            "best_value_state": best_value_state,
            "best_target_state": best_target_state,
            "jackpot_reward": jackpot,
        }

    return profiles, errors[:24]


def _build_warframe_relic_profit_radar(platform: str = "pc") -> tuple[dict[str, Any], list[str]]:
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"

    cache_key = f"warframe:relic_profit:{platform_key}"
    cached = _cache_get(cache_key, ttl_seconds=15 * 60)
    if isinstance(cached, dict):
        return cached, list(cached.get("errors") or [])

    tracked_names = [str(item.get("name") or "").strip() for item in _load_warframe_watchlist() if str(item.get("name") or "").strip()]
    target_names = list(dict.fromkeys([*tracked_names, *_WARFRAME_MARKET_WATCHLIST]))
    target_norms = [_normalize_warframe_item_name(name) for name in target_names if _normalize_warframe_item_name(name)]

    relics, relic_errors = _fetch_warframe_drop_dataset("relics")
    candidate_relics: set[str] = set()
    relic_hits: dict[str, set[str]] = {}
    for relic in relics:
        relic_name = f"{relic.get('tier') or ''} {relic.get('relicName') or ''}".strip()
        hits = relic_hits.setdefault(relic_name, set())
        for reward in relic.get("rewards") or []:
            if not isinstance(reward, dict):
                continue
            reward_norm = _normalize_warframe_item_name(reward.get("itemName") or "")
            if any(target in reward_norm or reward_norm in target for target in target_norms):
                candidate_relics.add(relic_name)
                hits.add(str(reward.get("itemName") or ""))

    candidate_list = sorted(candidate_relics)[:60]
    profiles, profile_errors = _compute_relic_value_profiles(candidate_list, platform=platform_key, target_item="")

    cards: list[dict[str, Any]] = []
    for relic_name in candidate_list:
        profile = profiles.get(relic_name) or {}
        best_value = profile.get("best_value_state") or {}
        jackpot = profile.get("jackpot_reward") or {}
        hits = sorted(relic_hits.get(relic_name) or [])[:4]
        if not best_value:
            continue
        cards.append(
            {
                "relic": relic_name,
                "best_state": best_value.get("state"),
                "best_ev": best_value.get("expected_value"),
                "state_values": profile.get("states") or [],
                "jackpot_reward": jackpot,
                "tracked_hits": hits,
                "tracked_hit_count": len(relic_hits.get(relic_name) or []),
            }
        )

    cards.sort(
        key=lambda item: (
            float(item.get("best_ev") or 0.0),
            int(item.get("tracked_hit_count") or 0),
            float((item.get("jackpot_reward") or {}).get("price") or 0.0),
            str(item.get("relic") or ""),
        ),
        reverse=True,
    )
    payload = {
        "platform": platform_key,
        "count": min(len(cards), 12),
        "items": cards[:12],
        "errors": (list(relic_errors) + list(profile_errors))[:24],
    }
    _cache_set(cache_key, payload)
    return payload, list(payload.get("errors") or [])


def _build_warframe_farm_plan(query: str, platform: str = "pc") -> tuple[dict[str, Any], list[str]]:
    q = str(query or "").strip()
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"
    q_norm = _normalize_warframe_item_name(q)
    if not q_norm:
        return {"query": q, "platform": platform_key, "relic_targets": [], "blueprints": [], "mods": []}, []

    relics, relic_errors = _fetch_warframe_drop_dataset("relics")
    blueprints, blueprint_errors = _fetch_warframe_drop_dataset("blueprintLocations")
    mods, mod_errors = _fetch_warframe_drop_dataset("modLocations")

    relic_rows: list[dict[str, Any]] = []
    for relic in relics:
        rewards = relic.get("rewards") or []
        for reward in rewards:
            if not isinstance(reward, dict):
                continue
            item_name = str(reward.get("itemName") or "")
            if q_norm not in _normalize_warframe_item_name(item_name):
                continue
            relic_rows.append(
                {
                    "relic": f"{relic.get('tier') or ''} {relic.get('relicName') or ''}".strip(),
                    "item_name": item_name,
                    "rarity": reward.get("rarity"),
                    "state": relic.get("state"),
                    "chance": reward.get("chance"),
                }
            )

    relic_targets = _aggregate_relic_states(relic_rows)
    target_market, target_market_errors = _fetch_warframe_value_snapshot(q, platform_key)
    relic_value_profiles, relic_value_errors = _compute_relic_value_profiles([row.get("relic") for row in relic_targets], platform=platform_key, target_item=q)
    for row in relic_targets:
        value_profile = relic_value_profiles.get(str(row.get("relic") or ""))
        row["value_lab"] = value_profile or {}
        row["target_market_price"] = target_market.get("last_avg_price")

    blueprint_rows: list[dict[str, Any]] = []
    for bp in blueprints:
        item_name = str(bp.get("itemName") or bp.get("blueprintName") or "")
        if q_norm not in _normalize_warframe_item_name(item_name):
            continue
        enemies = []
        for enemy in (bp.get("enemies") or [])[:8]:
            if not isinstance(enemy, dict):
                continue
            enemies.append(
                {
                    "enemy_name": enemy.get("enemyName"),
                    "rarity": enemy.get("rarity"),
                    "chance": enemy.get("chance"),
                }
            )
        enemies = sorted(enemies, key=lambda enemy: float(enemy.get("chance") or 0.0), reverse=True)
        blueprint_rows.append(
            {
                "item_name": item_name,
                "blueprint_name": bp.get("blueprintName"),
                "enemies": enemies,
                "best_chance": max([float(e.get("chance") or 0) for e in enemies] or [0.0]),
            }
        )
    blueprint_rows.sort(key=lambda row: (float(row.get("best_chance") or 0.0), str(row.get("item_name") or "")), reverse=True)

    mod_rows: list[dict[str, Any]] = []
    for mod in mods:
        mod_name = str(mod.get("modName") or "")
        if q_norm not in _normalize_warframe_item_name(mod_name):
            continue
        enemies = []
        for enemy in (mod.get("enemies") or [])[:10]:
            if not isinstance(enemy, dict):
                continue
            enemies.append(
                {
                    "enemy_name": enemy.get("enemyName"),
                    "rarity": enemy.get("rarity"),
                    "chance": enemy.get("chance"),
                }
            )
        enemies = sorted(enemies, key=lambda enemy: float(enemy.get("chance") or 0.0), reverse=True)
        mod_rows.append(
            {
                "mod_name": mod_name,
                "enemies": enemies,
                "best_chance": max([float(e.get("chance") or 0) for e in enemies] or [0.0]),
            }
        )
    mod_rows.sort(key=lambda row: (float(row.get("best_chance") or 0.0), str(row.get("mod_name") or "")), reverse=True)

    payload = {
        "query": q,
        "platform": platform_key,
        "target_market": target_market,
        "relic_targets": relic_targets[:10],
        "blueprints": blueprint_rows[:8],
        "mods": mod_rows[:8],
    }
    errors = list(relic_errors) + list(blueprint_errors) + list(mod_errors) + list(target_market_errors) + list(relic_value_errors)
    return payload, errors[:24]


def _build_warframe_watchlist_payload(platform: str = "pc", sort_by: str = "priority") -> tuple[list[dict[str, Any]], list[str]]:
    platform_key = str(platform or "pc").strip().lower()
    cache_key = f"warframe:watchlist:{platform_key}:{sort_by}"
    cached = _cache_get(cache_key, ttl_seconds=5 * 60)
    if isinstance(cached, dict):
        return list(cached.get("items") or []), list(cached.get("errors") or [])

    items = _load_warframe_watchlist()
    if not items:
        payload = {"items": [], "errors": []}
        _cache_set(cache_key, payload)
        return [], []

    out: list[dict[str, Any]] = []
    errors: list[str] = []

    def load_item(item: dict[str, Any]) -> tuple[Optional[dict[str, Any]], list[str]]:
        name = str(item.get("name") or "").strip()
        if not name:
            return None, []
        market, market_errors = _fetch_warframe_market(name, platform_key)
        plan, plan_errors = _build_warframe_farm_plan(name, platform=platform_key)
        priority = _coerce_watch_priority(item.get("priority"))
        price_value = float(market.get("last_avg_price") or 0.0)
        volume_value = float(market.get("volume_total") or 0.0)
        best_relic = (plan.get("relic_targets") or [{}])[0]
        market_signal = _build_warframe_market_signal(market, tracked=True)
        payload = {
            **item,
            "priority": priority,
            "tags": _normalize_tag_list(item.get("tags") or []),
            "market": {
                "best_sell": market.get("best_sell"),
                "best_buy": market.get("best_buy"),
                "last_avg_price": market.get("last_avg_price"),
                "volume_total": market.get("volume_total"),
                "price_change_pct": market.get("price_change_pct"),
                "slug": market.get("slug"),
                "snapshot_source": market.get("snapshot_source"),
                "buy_count_live": market.get("buy_count_live"),
                "sell_count_live": market.get("sell_count_live"),
                "local_history": market.get("local_history") or {},
            },
            "planner": {
                "relic_count": len(plan.get("relic_targets") or []),
                "best_relic": best_relic,
                "blueprint_count": len(plan.get("blueprints") or []),
                "mod_count": len(plan.get("mods") or []),
            },
            "signal": market_signal,
            "score": round(priority * 1000 + price_value * 10 + min(volume_value, 1000), 2),
        }
        return payload, [*(market_errors or [])[:2], *(plan_errors or [])[:2]]

    subset = items[:24]
    with ThreadPoolExecutor(max_workers=min(6, max(1, len(subset)))) as pool:
        futures = [pool.submit(load_item, item) for item in subset]
        for future in as_completed(futures):
            try:
                item_payload, item_errors = future.result()
            except Exception as exc:
                errors.append(f"watchlist:worker:{exc}")
                continue
            errors.extend(item_errors[:4])
            if item_payload:
                out.append(item_payload)

    if sort_by == "value":
        out.sort(key=lambda item: (float((item.get("market") or {}).get("last_avg_price") or 0.0), int(item.get("priority") or 0), str(item.get("name") or "")), reverse=True)
    elif sort_by == "volume":
        out.sort(key=lambda item: (float((item.get("market") or {}).get("volume_total") or 0.0), int(item.get("priority") or 0), str(item.get("name") or "")), reverse=True)
    elif sort_by == "recent":
        out.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
    else:
        out.sort(key=lambda item: (int(item.get("priority") or 0), float((item.get("market") or {}).get("last_avg_price") or 0.0), str(item.get("created_at") or "")), reverse=True)

    trimmed_errors = errors[:24]
    payload = {"items": out, "errors": trimmed_errors}
    if out:
        _cache_set(cache_key, payload)
        _set_last_good(cache_key, payload)
        return out, trimmed_errors

    stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
    if isinstance(stale_payload, dict):
        fallback_errors = list(trimmed_errors or []) + [f"fallback:last_good:watchlist:{age}s"]
        stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
        stale_payload["errors"] = fallback_errors[:24]
        _cache_set(cache_key, stale_payload)
        return list(stale_payload.get("items") or []), fallback_errors[:24]

    _cache_set(cache_key, payload)
    return out, trimmed_errors


def _parse_warframe_worldstate(data: dict[str, Any], platform: str) -> dict[str, Any]:
    news_out: list[dict[str, Any]] = []
    for n in (data.get("news") or [])[:10]:
        if not isinstance(n, dict):
            continue
        title = n.get("message") or n.get("title") or _dig(n, "translations", "en") or "News"
        news_out.append(
            {
                "title": title,
                "url": n.get("link") or n.get("url"),
                "published_at": n.get("date") or n.get("eta"),
                "eta": n.get("eta"),
                "importance": n.get("priority"),
            }
        )

    alerts_out: list[dict[str, Any]] = []
    for a in (data.get("alerts") or [])[:12]:
        if not isinstance(a, dict):
            continue
        mission = a.get("mission", {}) if isinstance(a.get("mission"), dict) else {}
        reward = mission.get("reward", {}) if isinstance(mission.get("reward"), dict) else {}
        reward_name = (
            reward.get("asString")
            or reward.get("itemString")
            or ", ".join(x.get("itemType", "") for x in reward.get("items", []) if isinstance(x, dict) and x.get("itemType"))
            or reward.get("credits")
            or "Reward"
        )
        alerts_out.append(
            {
                "node": mission.get("node"),
                "faction": mission.get("faction"),
                "type": mission.get("type"),
                "reward": reward_name,
                "eta": a.get("eta"),
            }
        )

    fissures_out: list[dict[str, Any]] = []
    for f in (data.get("fissures") or [])[:12]:
        if not isinstance(f, dict):
            continue
        if f.get("expired"):
            continue
        fissures_out.append(
            {
                "tier": f.get("tier"),
                "mission_type": f.get("missionType"),
                "node": f.get("node"),
                "is_storm": f.get("isStorm"),
                "is_hard": f.get("isHard"),
                "eta": f.get("eta"),
            }
        )

    invasions_out: list[dict[str, Any]] = []
    for inv in (data.get("invasions") or [])[:10]:
        if not isinstance(inv, dict) or inv.get("completed"):
            continue
        attacker = inv.get("attacker", {}) if isinstance(inv.get("attacker"), dict) else {}
        defender = inv.get("defender", {}) if isinstance(inv.get("defender"), dict) else {}
        invasions_out.append(
            {
                "node": inv.get("node"),
                "attacker": attacker.get("faction"),
                "defender": defender.get("faction"),
                "attacker_reward": _dig(attacker, "reward", "asString") or _dig(attacker, "reward", "itemString"),
                "defender_reward": _dig(defender, "reward", "asString") or _dig(defender, "reward", "itemString"),
                "eta": inv.get("eta"),
            }
        )

    events_out: list[dict[str, Any]] = []
    for ev in (data.get("events") or [])[:6]:
        if not isinstance(ev, dict):
            continue
        if ev.get("expired"):
            continue
        events_out.append(
            {
                "id": ev.get("id"),
                "description": ev.get("description") or ev.get("tooltip"),
                "eta": ev.get("eta"),
                "progress": ev.get("progress"),
            }
        )

    world_cycles = {
        "cetus": {"is_day": _dig(data, "cetusCycle", "isDay"), "state": _dig(data, "cetusCycle", "state"), "eta": _dig(data, "cetusCycle", "timeLeft") or _dig(data, "cetusCycle", "shortString")},
        "vallis": {"is_warm": _dig(data, "vallisCycle", "isWarm"), "state": _dig(data, "vallisCycle", "state"), "eta": _dig(data, "vallisCycle", "timeLeft") or _dig(data, "vallisCycle", "shortString")},
        "cambion": {"active": _dig(data, "cambionCycle", "active"), "state": _dig(data, "cambionCycle", "state"), "eta": _dig(data, "cambionCycle", "timeLeft") or _dig(data, "cambionCycle", "shortString")},
    }

    return {
        "platform": platform,
        "timestamp": data.get("timestamp"),
        "news": news_out,
        "alerts": alerts_out,
        "fissures": fissures_out,
        "invasions": invasions_out,
        "events": events_out,
        "sortie": data.get("sortie", {}),
        "nightwave": data.get("nightwave", {}),
        "arbitration": data.get("arbitration", {}),
        "steel_path": data.get("steelPath", {}),
        "world_cycles": world_cycles,
    }


def _official_wf_date(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        value = _dig(value, "$date", "$numberLong") or value.get("$date") or value.get("sec")
    if value in (None, ""):
        return None
    try:
        ms = int(value)
        if ms > 10_000_000_000:
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
        return datetime.fromtimestamp(ms, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _official_wf_text(raw: Any) -> str:
    text = str(raw or "").split("/")[-1]
    text = re.sub(r"(?<!^)([A-Z])", r" \1", text).replace("_", " ").strip()
    return text or "Reward"


def _official_wf_reward_name(payload: Any) -> str:
    if isinstance(payload, dict):
        counted = payload.get("countedItems") or []
        if counted and isinstance(counted[0], dict):
            item_type = counted[0].get("ItemType")
            count = counted[0].get("ItemCount")
            label = _official_wf_text(item_type)
            return f"{count}x {label}" if count not in (None, 1) else label
        items = payload.get("items") or []
        if items and isinstance(items[0], str):
            return ", ".join(_official_wf_text(item) for item in items[:2])
        credits = payload.get("credits")
        if credits:
            return f"{credits} credits"
    return "Reward"


def _parse_official_warframe_worldstate(data: dict[str, Any], platform: str) -> dict[str, Any]:
    alerts_out: list[dict[str, Any]] = []
    for alert in (data.get("Alerts") or [])[:12]:
        if not isinstance(alert, dict):
            continue
        mission = alert.get("MissionInfo") or {}
        alerts_out.append(
            {
                "node": mission.get("location") or mission.get("node") or alert.get("Node"),
                "faction": mission.get("faction"),
                "type": mission.get("missionType"),
                "reward": _official_wf_reward_name(mission.get("missionReward") or {}),
                "eta": _official_wf_date(alert.get("Expiry")) or _official_wf_date(alert.get("Activation")),
            }
        )

    fissures_out: list[dict[str, Any]] = []
    for mission in (data.get("ActiveMissions") or [])[:20]:
        if not isinstance(mission, dict):
            continue
        modifier = str(mission.get("Modifier") or "")
        if not modifier.startswith("Void"):
            continue
        fissures_out.append(
            {
                "tier": modifier,
                "mission_type": mission.get("MissionType"),
                "node": mission.get("Node"),
                "is_storm": False,
                "is_hard": bool(mission.get("Hard")),
                "eta": _official_wf_date(mission.get("Expiry")),
            }
        )

    invasions_out: list[dict[str, Any]] = []
    for inv in (data.get("Invasions") or [])[:10]:
        if not isinstance(inv, dict) or inv.get("Completed"):
            continue
        progress = None
        try:
            progress = f"{int(inv.get('Count') or 0)}/{int(inv.get('Goal') or 0)}"
        except Exception:
            progress = None
        invasions_out.append(
            {
                "node": inv.get("Node"),
                "attacker": inv.get("Faction"),
                "defender": inv.get("DefenderFaction"),
                "attacker_reward": _official_wf_reward_name(inv.get("AttackerReward") or {}),
                "defender_reward": _official_wf_reward_name(inv.get("DefenderReward") or {}),
                "eta": progress or _official_wf_date(inv.get("Activation")),
            }
        )

    sortie = {}
    sorties = data.get("Sorties") or []
    if sorties and isinstance(sorties[0], dict):
        sortie = {
            "eta": _official_wf_date(sorties[0].get("Expiry")),
            "boss": sorties[0].get("Boss"),
        }

    return {
        "platform": platform,
        "timestamp": _official_wf_date(data.get("Time")),
        "news": [],
        "alerts": alerts_out,
        "fissures": fissures_out,
        "invasions": invasions_out,
        "events": [],
        "sortie": sortie,
        "nightwave": {},
        "arbitration": {},
        "steel_path": {},
        "world_cycles": {"cetus": {"state": None, "eta": None}, "vallis": {"state": None, "eta": None}, "cambion": {"state": None, "eta": None}},
    }


def _fetch_warframe_worldstate(platform: str) -> tuple[dict[str, Any], list[str]]:
    platform_key = str(platform or "pc").strip().lower()
    cache_key = f"warframe:worldstate:{platform_key}"
    cached = _cache_get(cache_key, ttl_seconds=90)
    if isinstance(cached, dict):
        return cached, list(cached.get("errors") or [])

    errors: list[str] = []
    headers = {"Accept": "application/json"}
    candidates = [
        (f"https://api.warframestat.us/{platform_key}", None),
        (f"https://api.warframestat.us/{platform_key}", {"language": "en"}),
        (f"https://api.warframestat.us/{platform_key}/", {"language": "en"}),
    ]

    data: Optional[dict[str, Any]] = None
    for url, params in candidates:
        raw, err = _http_get_json(url, params=params, headers=headers, timeout=15)
        if err:
            errors.append(err)
            continue
        if isinstance(raw, dict):
            data = raw
            break
        errors.append("invalid_worldstate_payload")

    parsed_candidates: list[dict[str, Any]] = []
    if isinstance(data, dict):
        parsed_candidates.append(_parse_warframe_worldstate(data, platform_key))

    official_raw, official_err = _http_get_json("https://content.warframe.com/dynamic/worldState.php", headers=headers, timeout=20)
    if isinstance(official_raw, dict):
        parsed_candidates.append(_parse_official_warframe_worldstate(official_raw, platform_key))
    elif official_err:
        errors.append(f"official:{official_err}")

    for parsed in parsed_candidates:
        if _warframe_worldstate_has_data(parsed):
            payload = {**parsed, "errors": errors[:16]}
            _cache_set(cache_key, payload)
            _set_last_good(cache_key, payload)
            return payload, errors[:16]

    segment_payload: dict[str, Any] = {"timestamp": None, "sortie": {}, "nightwave": {}, "arbitration": {}, "steelPath": {}}
    list_segments = ["news", "alerts", "fissures", "invasions", "events"]
    object_segments = {
        "sortie": "sortie",
        "nightwave": "nightwave",
        "arbitration": "arbitration",
        "steel_path": "steelPath",
        "cetus_cycle": "cetusCycle",
        "vallis_cycle": "vallisCycle",
        "cambion_cycle": "cambionCycle",
    }
    for seg in list_segments:
        raw, err = _http_get_json(f"https://api.warframestat.us/{platform_key}/{seg}", params={"language": "en"}, headers=headers, timeout=15)
        if err:
            errors.append(f"{seg}:{err}")
            segment_payload[seg] = []
            continue
        if isinstance(raw, list):
            segment_payload[seg] = raw
            if segment_payload.get("timestamp") is None and raw:
                first = raw[0] if isinstance(raw[0], dict) else {}
                segment_payload["timestamp"] = first.get("date") or first.get("activation") or first.get("expiry")
        else:
            segment_payload[seg] = []
            errors.append(f"{seg}:invalid_payload")

    for endpoint, target_key in object_segments.items():
        raw, err = _http_get_json(f"https://api.warframestat.us/{platform_key}/{endpoint}", params={"language": "en"}, headers=headers, timeout=15)
        if err:
            errors.append(f"{endpoint}:{err}")
            continue
        if isinstance(raw, dict):
            segment_payload[target_key] = raw
            if segment_payload.get("timestamp") is None:
                segment_payload["timestamp"] = raw.get("expiry") or raw.get("activation") or raw.get("date")
        else:
            errors.append(f"{endpoint}:invalid_payload")

    parsed = _parse_warframe_worldstate(segment_payload, platform_key)
    if _warframe_worldstate_has_data(parsed):
        payload = {**parsed, "errors": errors[:20]}
        _cache_set(cache_key, payload)
        _set_last_good(cache_key, payload)
        return payload, errors[:20]

    stale_payload, age = _get_last_good(cache_key, max_age_seconds=6 * 3600)
    if isinstance(stale_payload, dict):
        fallback_errors = list(errors or []) + [f"fallback:last_good:worldstate:{age}s"]
        stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
        stale_payload["errors"] = fallback_errors[:20]
        _cache_set(cache_key, stale_payload)
        return stale_payload, fallback_errors[:20]

    payload = {**parsed, "errors": errors[:20]}
    _cache_set(cache_key, payload)
    return payload, errors[:20]


def _fetch_warframe_market(item: str, platform: str = "pc") -> tuple[dict[str, Any], list[str]]:
    item_key = _normalize_warframe_item_name(item)
    cache_key = f"warframe:market_snapshot:{platform}:{item_key}"
    cached = _cache_get(cache_key, ttl_seconds=10 * 60)
    if isinstance(cached, dict):
        cached_payload = dict(cached.get("payload") or {})
        local_history = _summarize_warframe_market_history(str(cached_payload.get("slug") or item_key), platform)
        if local_history:
            cached_payload["local_history"] = local_history
        return cached_payload, list(cached.get("errors") or [])

    catalog, catalog_errors = _fetch_warframe_market_catalog()
    item_meta = _find_warframe_market_item(catalog, item)
    slug = str((item_meta or {}).get("slug") or _normalize_warframe_item_name(item))
    canonical_name = str((item_meta or {}).get("name") or item or "Unknown Item")
    max_rank = (item_meta or {}).get("max_rank")
    if not slug:
        return {"item": item, "canonical_name": canonical_name, "slug": "", "best_sell": None, "best_buy": None}, ["invalid_item"]

    req_headers = {"Accept": "application/json", "Language": "en", "User-Agent": "HolocronHub/0.5"}
    orders_url = f"https://api.warframe.market/v1/items/{slug}/orders"

    errors: list[str] = list(catalog_errors)
    orders_restricted = False
    data: Optional[dict[str, Any]] = None
    for params in [{"platform": platform}, {"platform": platform, "include": "item"}, None]:
        raw, err = _http_get_json(orders_url, params=params, headers=req_headers, timeout=12)
        if err:
            err_text = str(err)
            if "403" in err_text:
                orders_restricted = True
                break
            errors.append(f"orders:{err_text}")
            continue
        if isinstance(raw, dict):
            data = raw
            break
        errors.append("orders:invalid_payload")

    orders = _dig(data, "payload", "orders") or []
    sells_live: list[int] = []
    buys_live: list[int] = []
    sells_all: list[int] = []
    buys_all: list[int] = []
    sell_rows_live: list[dict[str, Any]] = []
    buy_rows_live: list[dict[str, Any]] = []
    sell_rows_all: list[dict[str, Any]] = []
    buy_rows_all: list[dict[str, Any]] = []

    for o in orders:
        if not isinstance(o, dict):
            continue
        if not o.get("visible", True):
            continue

        user = o.get("user", {}) if isinstance(o.get("user"), dict) else {}
        status = str(user.get("status", "")).lower()
        is_live = status in {"ingame", "online"}

        plat = o.get("platinum")
        if plat is None:
            continue
        try:
            plat_int = int(round(float(plat)))
        except Exception:
            continue

        row = {
            "price": plat_int,
            "user": user.get("ingame_name"),
            "status": status or "offline",
            "quantity": o.get("quantity"),
            "mod_rank": o.get("mod_rank"),
        }
        order_type = str(o.get("order_type", "")).lower()

        if order_type == "sell":
            sells_all.append(plat_int)
            sell_rows_all.append(row)
            if is_live:
                sells_live.append(plat_int)
                sell_rows_live.append(row)
        elif order_type == "buy":
            buys_all.append(plat_int)
            buy_rows_all.append(row)
            if is_live:
                buys_live.append(plat_int)
                buy_rows_live.append(row)

    sell_base = sorted(sells_live) if sells_live else sorted(sells_all)
    buy_base = sorted(buys_live, reverse=True) if buys_live else sorted(buys_all, reverse=True)

    stats_data, stats_err = _http_get_json(
        f"https://api.warframe.market/v1/items/{slug}/statistics",
        params={"platform": platform},
        headers=req_headers,
        timeout=15,
    )
    stats_summary = _summarize_warframe_statistics(stats_data, max_rank=max_rank)

    if stats_err:
        errors.append(f"statistics:{stats_err}")
    elif orders_restricted and not stats_summary.get("history"):
        errors.append("orders:403_forbidden")

    price_floor = stats_summary.get("price_floor_estimate")
    price_ceiling = stats_summary.get("price_ceiling_estimate")
    last_avg = stats_summary.get("last_avg_price")
    best_sell = sell_base[0] if sell_base else price_floor
    best_buy = buy_base[0] if buy_base else None
    source_mode = "orders_live" if sell_base or buy_base else ("statistics_fallback" if stats_summary.get("history") else "unavailable")

    payload = {
        "item": item,
        "canonical_name": canonical_name,
        "slug": slug,
        "thumb": (item_meta or {}).get("thumb"),
        "icon": (item_meta or {}).get("icon"),
        "rarity": (item_meta or {}).get("rarity"),
        "selected_rank": stats_summary.get("selected_rank"),
        "best_sell": best_sell,
        "best_buy": best_buy,
        "median_sell": float(median(sell_base[:20])) if sell_base else last_avg,
        "median_buy": float(median(buy_base[:20])) if buy_base else None,
        "sample_sell_orders": sorted(sell_rows_live, key=lambda x: x["price"])[:8] if sell_rows_live else sorted(sell_rows_all, key=lambda x: x["price"])[:8],
        "sample_buy_orders": sorted(buy_rows_live, key=lambda x: x["price"], reverse=True)[:8] if buy_rows_live else sorted(buy_rows_all, key=lambda x: x["price"], reverse=True)[:8],
        "sell_count_live": len(sells_live),
        "buy_count_live": len(buys_live),
        "sell_count_total": len(sells_all),
        "buy_count_total": len(buys_all),
        "history_period": stats_summary.get("history_period"),
        "history": stats_summary.get("history") or [],
        "selected_scope": "live" if sells_live or buys_live else ("stats" if stats_summary.get("history") else "all_visible"),
        "snapshot_source": source_mode,
        "last_avg_price": last_avg,
        "last_closed_price": stats_summary.get("last_closed_price"),
        "last_volume": stats_summary.get("last_volume"),
        "volume_total": stats_summary.get("volume_total"),
        "price_floor_estimate": price_floor,
        "price_ceiling_estimate": price_ceiling,
        "price_change_pct": stats_summary.get("price_change_pct"),
    }
    local_history = _record_warframe_market_snapshot(payload, platform)
    if local_history:
        payload["local_history"] = local_history
    trimmed_errors = errors[:16]
    _cache_set(cache_key, {"payload": payload, "errors": trimmed_errors})
    return payload, trimmed_errors


def _build_warframe_market_signal(market: dict[str, Any], *, tracked: bool = False) -> dict[str, Any]:
    try:
        best_sell = float(market.get("best_sell")) if market.get("best_sell") is not None else None
    except Exception:
        best_sell = None
    try:
        best_buy = float(market.get("best_buy")) if market.get("best_buy") is not None else None
    except Exception:
        best_buy = None
    local_history = market.get("local_history") if isinstance(market.get("local_history"), dict) else {}
    raw_trend = market.get("price_change_pct")
    try:
        trend = float(raw_trend) if raw_trend is not None else 0.0
    except Exception:
        trend = 0.0
    try:
        local_trend = float(local_history.get("change_pct_24h")) if local_history.get("change_pct_24h") is not None else None
    except Exception:
        local_trend = None
    if raw_trend is None and local_trend is not None:
        trend = local_trend
    try:
        volume = float(market.get("volume_total") or 0.0)
    except Exception:
        volume = 0.0

    buy_live = int(market.get("buy_count_live") or 0)
    sell_live = int(market.get("sell_count_live") or 0)
    buy_total = int(market.get("buy_count_total") or 0)
    sell_total = int(market.get("sell_count_total") or 0)
    history_samples = int(local_history.get("samples_24h") or local_history.get("samples") or 0)

    spread = None
    spread_pct = None
    if best_sell is not None and best_buy is not None:
        spread = round(max(best_sell - best_buy, 0.0), 2)
        if best_sell > 0:
            spread_pct = round((spread / best_sell) * 100.0, 1)

    demand_score = (
        min(48.0, math.log10(volume + 1.0) * 18.0)
        + min(20.0, buy_live * 3.5)
        + min(12.0, max(trend, 0.0) * 1.4)
        + min(8.0, history_samples * 0.8)
        + (8.0 if best_buy and best_sell and best_buy >= best_sell * 0.9 else 0.0)
        - min(14.0, max(sell_live - buy_live, 0) * 1.6)
    )
    liquidity_score = (
        min(42.0, math.log10(volume + 1.0) * 16.0)
        + min(18.0, (buy_live + sell_live) * 2.4)
        + min(6.0, history_samples * 0.5)
        + max(0.0, 22.0 - min(spread_pct if spread_pct is not None else 18.0, 18.0))
        + (8.0 if str(market.get("snapshot_source") or "") == "orders_live" else 0.0)
    )
    demand_score = round(max(0.0, min(100.0, demand_score)), 1)
    liquidity_score = round(max(0.0, min(100.0, liquidity_score)), 1)

    pressure_ratio = None
    if sell_live > 0:
        pressure_ratio = round(buy_live / max(sell_live, 1), 2)
    elif buy_live > 0:
        pressure_ratio = 9.99

    if tracked and trend <= -10.0 and volume < 120:
        signal = "dump"
    elif tracked and demand_score >= 68.0 and liquidity_score >= 55.0:
        signal = "sell"
    elif tracked and (demand_score >= 52.0 or liquidity_score >= 52.0):
        signal = "hold"
    else:
        signal = "watch"

    if demand_score >= 75.0:
        momentum = "surging"
    elif demand_score >= 55.0:
        momentum = "active"
    else:
        momentum = "quiet"

    return {
        "spread": spread,
        "spread_pct": spread_pct,
        "demand_score": demand_score,
        "liquidity_score": liquidity_score,
        "pressure_ratio": pressure_ratio,
        "momentum": momentum,
        "signal": signal,
        "tracked": tracked,
        "live_buyers": buy_live,
        "live_sellers": sell_live,
        "total_buyers": buy_total,
        "total_sellers": sell_total,
    }


def _build_warframe_market_pulse(platform: str = "pc") -> tuple[dict[str, Any], list[str]]:
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"

    cache_key = f"warframe:market_pulse:{platform_key}"
    cached = _cache_get(cache_key, ttl_seconds=15 * 60)
    if isinstance(cached, dict):
        return cached, list(cached.get("errors") or [])

    tracked_names = [
        str(item.get("name") or "").strip()
        for item in _load_warframe_watchlist()
        if str(item.get("name") or "").strip()
    ]
    tracked_set = {name.casefold() for name in tracked_names}
    seed_names = list(dict.fromkeys([*tracked_names[:16], *_WARFRAME_MARKET_WATCHLIST]))

    errors: list[str] = []
    cards: list[dict[str, Any]] = []

    def load_item(name: str) -> tuple[Optional[dict[str, Any]], list[str]]:
        market, market_errors = _fetch_warframe_market(name, platform_key)
        if not isinstance(market, dict):
            return None, market_errors
        if market.get("best_sell") is None and not market.get("volume_total"):
            return None, market_errors
        metrics = _build_warframe_market_signal(market, tracked=name.casefold() in tracked_set)
        return {
            "name": market.get("canonical_name") or name,
            "slug": market.get("slug"),
            "thumb": market.get("thumb"),
            "icon": market.get("icon"),
            "price": market.get("last_avg_price") if market.get("last_avg_price") is not None else market.get("best_sell"),
            "best_sell": market.get("best_sell"),
            "best_buy": market.get("best_buy"),
            "volume_total": market.get("volume_total") or 0,
            "price_change_pct": market.get("price_change_pct"),
            "snapshot_source": market.get("snapshot_source"),
            "history": market.get("history") or [],
            "local_history": market.get("local_history") or {},
            **metrics,
        }, market_errors

    with ThreadPoolExecutor(max_workers=min(8, max(1, len(seed_names)))) as pool:
        futures = [pool.submit(load_item, name) for name in seed_names]
        for future in as_completed(futures):
            try:
                item_payload, item_errors = future.result()
            except Exception as exc:
                errors.append(f"market_pulse:worker:{exc}")
                continue
            errors.extend((item_errors or [])[:2])
            if item_payload:
                cards.append(item_payload)

    demand_board = sorted(
        cards,
        key=lambda item: (
            float(item.get("demand_score") or 0.0),
            float(item.get("volume_total") or 0.0),
            float(item.get("price_change_pct") or 0.0),
        ),
        reverse=True,
    )[:8]
    liquidity_board = sorted(
        cards,
        key=lambda item: (
            float(item.get("liquidity_score") or 0.0),
            float(item.get("volume_total") or 0.0),
            -float(item.get("spread_pct") or 999.0),
        ),
        reverse=True,
    )[:8]

    signal_rank = {"sell": 0, "hold": 1, "watch": 2, "dump": 3}
    tracked_signals = sorted(
        [item for item in cards if item.get("tracked")],
        key=lambda item: (
            signal_rank.get(str(item.get("signal") or "watch"), 9),
            -float(item.get("demand_score") or 0.0),
            -float(item.get("liquidity_score") or 0.0),
        ),
    )[:12]

    history_ready_count = sum(1 for item in cards if int(((item.get("local_history") or {}).get("samples") or 0)) >= 2)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "platform": platform_key,
        "demand_leaders": demand_board,
        "liquidity_leaders": liquidity_board,
        "watch_signals": tracked_signals,
        "coverage_count": len(cards),
        "tracked_count": len(tracked_signals),
        "history_ready_count": history_ready_count,
        "errors": errors[:24],
    }
    if cards:
        _cache_set(cache_key, payload)
        _set_last_good(cache_key, payload)
        return payload, list(payload.get("errors") or [])

    stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
    if isinstance(stale_payload, dict):
        fallback_errors = list(errors or []) + [f"fallback:last_good:market_pulse:{age}s"]
        stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
        stale_payload["errors"] = fallback_errors[:24]
        _cache_set(cache_key, stale_payload)
        return stale_payload, fallback_errors[:24]

    _cache_set(cache_key, payload)
    return payload, list(payload.get("errors") or [])


def _build_warframe_personal_opportunities(platform: str = "pc") -> tuple[dict[str, Any], list[str]]:
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"
    cache_key = f"warframe:opportunities:{platform_key}"
    cached = _cache_get(cache_key, ttl_seconds=15 * 60)
    if isinstance(cached, dict):
        return cached, list(cached.get("errors") or [])

    aleca_payload, aleca_errors = _build_alecaframe_payload()
    errors = list(aleca_errors)
    if not isinstance(aleca_payload, dict) or not aleca_payload.get("configured"):
        payload = {
            "configured": False,
            "platform": platform_key,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "market_items": [],
            "relic_items": [],
            "summary": {"market_count": 0, "relic_count": 0},
            "errors": errors[:24],
        }
        _cache_set(cache_key, payload)
        return payload, errors[:24]

    trade_insights = aleca_payload.get("trade_insights") if isinstance(aleca_payload.get("trade_insights"), dict) else {}
    sent_rows = [row for row in (trade_insights.get("top_sent_items") or []) if isinstance(row, dict)]
    received_rows = [row for row in (trade_insights.get("top_received_items") or []) if isinstance(row, dict)]
    sent_map = {str(row.get("name") or "").casefold(): row for row in sent_rows if str(row.get("name") or "").strip()}
    received_map = {str(row.get("name") or "").casefold(): row for row in received_rows if str(row.get("name") or "").strip()}
    candidate_names = list(
        dict.fromkeys(
            [str(row.get("name") or "").strip() for row in [*sent_rows[:8], *received_rows[:8]] if str(row.get("name") or "").strip()]
        )
    )

    market_cards: list[dict[str, Any]] = []

    def load_market_candidate(name: str) -> tuple[Optional[dict[str, Any]], list[str]]:
        market, market_errors = _fetch_warframe_market(name, platform_key)
        if not isinstance(market, dict):
            return None, market_errors
        if market.get("best_sell") is None and market.get("last_avg_price") is None and not market.get("volume_total"):
            return None, market_errors
        signal = _build_warframe_market_signal(market, tracked=True)
        sent = sent_map.get(name.casefold()) or {}
        received = received_map.get(name.casefold()) or {}
        sent_count = int(sent.get("count") or 0)
        recv_count = int(received.get("count") or 0)
        sent_plat = int(sent.get("plat_total") or 0)
        recv_plat = int(received.get("plat_total") or 0)
        trend = float(market.get("price_change_pct") or 0.0)
        demand = float(signal.get("demand_score") or 0.0)
        liquidity = float(signal.get("liquidity_score") or 0.0)
        volume = float(market.get("volume_total") or 0.0)

        action = "watch"
        note = "Monitor this lane."
        if sent_count > 0 and demand >= 62 and liquidity >= 50:
            action = "restock"
            note = "You already move this item and demand is strong."
        elif sent_count > 0 and signal.get("signal") == "sell":
            action = "sell"
            note = "You have recent sales and the market is still cooperative."
        elif recv_count > sent_count and trend >= 4 and demand >= 56:
            action = "flip"
            note = "Recent buys line up with positive price momentum."
        elif recv_count > 0 and trend <= -6:
            action = "wait"
            note = "You bought into weakness; better to wait for recovery."
        elif volume >= 180 and liquidity >= 58:
            action = "liquid"
            note = "This lane looks liquid even without a huge move."

        score = round(
            demand * 0.75
            + liquidity * 0.55
            + min(volume / 10.0, 24.0)
            + min(sent_count * 4.0, 16.0)
            + min(recv_count * 3.0, 12.0)
            + (8.0 if action in {"restock", "sell", "flip"} else 0.0),
            1,
        )
        return {
            "name": market.get("canonical_name") or name,
            "slug": market.get("slug"),
            "price": market.get("last_avg_price") if market.get("last_avg_price") is not None else market.get("best_sell"),
            "best_sell": market.get("best_sell"),
            "best_buy": market.get("best_buy"),
            "volume_total": market.get("volume_total"),
            "price_change_pct": market.get("price_change_pct"),
            "local_history": market.get("local_history") or {},
            "history": market.get("history") or [],
            "snapshot_source": market.get("snapshot_source"),
            "sent_count": sent_count,
            "received_count": recv_count,
            "sent_plat_total": sent_plat,
            "received_plat_total": recv_plat,
            "action": action,
            "note": note,
            "score": score,
            **signal,
        }, market_errors

    if candidate_names:
        with ThreadPoolExecutor(max_workers=min(6, max(1, len(candidate_names)))) as pool:
            futures = [pool.submit(load_market_candidate, name) for name in candidate_names]
            for future in as_completed(futures):
                try:
                    payload, item_errors = future.result()
                except Exception as exc:
                    errors.append(f"opportunities:market:{exc}")
                    continue
                errors.extend((item_errors or [])[:2])
                if payload:
                    market_cards.append(payload)

    market_cards.sort(
        key=lambda row: (
            float(row.get("score") or 0.0),
            float(row.get("demand_score") or 0.0),
            float(row.get("liquidity_score") or 0.0),
            str(row.get("name") or ""),
        ),
        reverse=True,
    )

    relic_summary = aleca_payload.get("relic_inventory") if isinstance(aleca_payload.get("relic_inventory"), dict) else {}
    relic_source = [row for row in (relic_summary.get("top_entries") or []) if isinstance(row, dict)]
    relic_bulk = {str(row.get("name") or "").casefold(): row for row in (relic_summary.get("top_by_copies") or []) if isinstance(row, dict)}
    relic_cards: list[dict[str, Any]] = []
    for entry in relic_source[:8]:
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        bulk = relic_bulk.get(name.casefold()) or entry
        count = int(bulk.get("count") or entry.get("count") or 0)
        plat_value = entry.get("plat_value")
        try:
            plat_value_num = float(plat_value) if plat_value is not None else None
        except Exception:
            plat_value_num = None
        stash_value = round((plat_value_num or 0.0) * max(count, 1), 1) if plat_value_num is not None else None
        if plat_value_num is not None and count >= 4 and stash_value >= 45:
            action = "sell stack"
            note = "You have enough copies for a meaningful relic stash sale."
        elif plat_value_num is not None and stash_value >= 80:
            action = "protect"
            note = "High-value relic stash worth holding for strong windows."
        elif count >= 6:
            action = "crack"
            note = "Large stack; consider opening in focused runs or radshares."
        else:
            action = "watch"
            note = "Useful inventory lane, but not urgent yet."
        score = round((stash_value or 0.0) + min(count * 3.0, 24.0), 1)
        relic_cards.append(
            {
                "name": name,
                "count": count,
                "plat_value": plat_value_num,
                "stash_value": stash_value,
                "action": action,
                "note": note,
                "score": score,
            }
        )
    relic_cards.sort(key=lambda row: (float(row.get("score") or 0.0), str(row.get("name") or "")), reverse=True)

    payload = {
        "configured": True,
        "platform": platform_key,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "market_items": market_cards[:8],
        "relic_items": relic_cards[:8],
        "summary": {
            "market_count": len(market_cards),
            "relic_count": len(relic_cards),
            "net_plat_7d": trade_insights.get("net_plat_7d"),
            "trade_count_7d": trade_insights.get("trade_count_7d"),
        },
        "errors": errors[:24],
    }
    _cache_set(cache_key, payload)
    _set_last_good(cache_key, payload)
    return payload, errors[:24]


def _normalize_settings(raw: dict[str, Any]) -> dict[str, Any]:
    cfg = deepcopy(_DEFAULT_SETTINGS)
    if not isinstance(raw, dict):
        return cfg

    for section, keys in _ALLOWED_SETTINGS_PATCH.items():
        incoming = raw.get(section)
        if not isinstance(incoming, dict):
            continue
        for key in keys:
            if key in incoming:
                cfg[section][key] = incoming[key]

    cfg["ux"]["language"] = "DE" if str(cfg["ux"].get("language", "EN")).upper() == "DE" else "EN"
    cfg["ux"]["default_category"] = str(cfg["ux"].get("default_category", "")).strip()
    cfg["ux"]["show_disabled_sources"] = bool(cfg["ux"].get("show_disabled_sources", False))

    try:
        refresh = int(cfg["feed"].get("refresh_interval_minutes", 15))
    except Exception:
        refresh = 15
    cfg["feed"]["refresh_interval_minutes"] = max(1, min(refresh, 240))

    digest_mode = str(cfg["feed"].get("digest_mode", "daily")).lower()
    if digest_mode not in {"off", "daily", "twice"}:
        digest_mode = "daily"
    cfg["feed"]["digest_mode"] = digest_mode

    model_name = str(cfg["models"].get("openclaw_model", "unknown")).strip()
    cfg["models"]["openclaw_model"] = model_name or "unknown"

    for key in _API_KEY_ENV_MAP:
        cfg["api_keys"][key] = str(cfg["api_keys"].get(key, "")).strip()

    return cfg


def _load_settings() -> dict[str, Any]:
    if SETTINGS_FILE.exists():
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
    else:
        data = {}

    cfg = _normalize_settings(data)

    env_model = os.getenv("OPENCLAW_MODEL", "").strip()
    if env_model and cfg["models"].get("openclaw_model") in {"", "unknown"}:
        cfg["models"]["openclaw_model"] = env_model

    for settings_key, env_name in _API_KEY_ENV_MAP.items():
        if not cfg["api_keys"].get(settings_key):
            cfg["api_keys"][settings_key] = os.getenv(env_name, "").strip()

    return cfg


def _save_settings(cfg: dict[str, Any]) -> None:
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_FILE.write_text(
        json.dumps(_normalize_settings(cfg), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _patch_settings(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(current)
    if not isinstance(patch, dict):
        return _normalize_settings(merged)

    for section, keys in _ALLOWED_SETTINGS_PATCH.items():
        incoming = patch.get(section)
        if not isinstance(incoming, dict):
            continue
        for key in keys:
            if key in incoming:
                merged.setdefault(section, {})
                merged[section][key] = incoming[key]

    return _normalize_settings(merged)


def _apply_settings_env(cfg: dict[str, Any]) -> None:
    model_name = str(cfg.get("models", {}).get("openclaw_model", "")).strip()
    if model_name:
        os.environ["OPENCLAW_MODEL"] = model_name

    api_cfg = cfg.get("api_keys", {})
    for settings_key, env_name in _API_KEY_ENV_MAP.items():
        value = str(api_cfg.get(settings_key, "")).strip()
        if value:
            os.environ[env_name] = value
        else:
            os.environ.pop(env_name, None)


def _settings_response(cfg: dict[str, Any]) -> dict[str, Any]:
    schedule_cfg = _load_schedule()
    job = _scheduler.get_job(_schedule_job_id)
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return {**cfg, "schedule": {**schedule_cfg, "next_run": next_run}}


def _tldr_gmail_client_creds() -> tuple[str, str]:
    return (
        os.getenv("GOOGLE_OAUTH_CLIENT_ID", "").strip(),
        os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "").strip(),
    )


def _tldr_auth_redirect_uri() -> str:
    return "http://127.0.0.1:8000/api/tldr/auth/callback"


def _tldr_auth_status() -> dict[str, Any]:
    client_id, client_secret = _tldr_gmail_client_creds()
    has_client_secret = TLDR_GMAIL_CLIENT_SECRET_FILE.exists() or bool(client_id and client_secret)
    has_token = TLDR_GMAIL_TOKEN_FILE.exists()
    profile = None
    auth_error = None
    if has_token:
        try:
            profile = gmail_profile(TLDR_GMAIL_TOKEN_FILE)
        except Exception as exc:
            auth_error = str(exc)
    return {
        "has_client_secret": has_client_secret,
        "has_token": has_token,
        "profile": profile,
        "auth_error": auth_error,
        "summary": load_tldr_summary(TLDR_DB_FILE),
        "oauth_redirect_uri": _tldr_auth_redirect_uri(),
        "client_secret_path": str(TLDR_GMAIL_CLIENT_SECRET_FILE),
    }


def _validate_tldr_client_secret_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("OAuth client JSON must be an object.")
    candidate = None
    if isinstance(payload.get("installed"), dict):
        candidate = payload["installed"]
    elif isinstance(payload.get("web"), dict):
        candidate = payload["web"]
    else:
        raise ValueError("Expected Google OAuth JSON with `installed` or `web` root.")
    client_id = str(candidate.get("client_id") or "").strip()
    client_secret = str(candidate.get("client_secret") or "").strip()
    auth_uri = str(candidate.get("auth_uri") or "").strip()
    token_uri = str(candidate.get("token_uri") or "").strip()
    if not client_id or not client_secret:
        raise ValueError("Google OAuth JSON is missing `client_id` or `client_secret`.")
    if not auth_uri or not token_uri:
        raise ValueError("Google OAuth JSON is missing Google auth/token URIs.")
    return payload


def _sync_tldr_from_gmail(*, max_results: int = 25, query: str | None = None) -> dict[str, Any]:
    if not TLDR_GMAIL_TOKEN_FILE.exists():
        raise RuntimeError("TLDR Gmail token missing. Connect Gmail first.")
    actual_query = str(query or "subject:TLDR newer_than:180d").strip()
    issues = fetch_tldr_messages(
        TLDR_GMAIL_TOKEN_FILE,
        query=actual_query,
        max_results=max_results,
    )
    newsletters: dict[str, int] = {}
    for issue in issues:
        upsert_tldr_issue(TLDR_DB_FILE, issue)
        slug = str(issue.get("newsletter_slug") or "unknown")
        newsletters[slug] = newsletters.get(slug, 0) + 1
    profile = None
    try:
        profile = gmail_profile(TLDR_GMAIL_TOKEN_FILE)
    except Exception:
        profile = None
    return {
        "ok": True,
        "stored": len(issues),
        "query": actual_query,
        "newsletters": newsletters,
        "profile": profile,
        "summary": load_tldr_summary(TLDR_DB_FILE),
    }


# ── ingest core ───────────────────────────────────────────────────────────────

def _build_cost_guard(selected_items: list[dict], total_limit: int) -> dict:
    text_blob = "\n".join(
        f"{i.get('title','')}\n{str(i.get('summary',''))[:600]}" for i in selected_items
    )
    est_tokens = max(0, int(len(text_blob) / 4))
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        load1, load5, load15 = 0.0, 0.0, 0.0
    try:
        du = shutil.disk_usage("/")
        disk_used_pct = round((du.used / du.total) * 100, 2) if du.total else 0.0
    except Exception:
        disk_used_pct = 0.0
    risk = "low"
    if est_tokens > 5000 or total_limit > 20 or disk_used_pct > 85 or load5 > 4.0:
        risk = "high"
    elif est_tokens > 3000 or total_limit > 14 or load5 > 2.0:
        risk = "medium"
    return {
        "risk": risk,
        "estimated_tokens": est_tokens,
        "selected_items": len(selected_items),
        "load_avg": {"1m": round(load1, 2), "5m": round(load5, 2), "15m": round(load15, 2)},
        "disk_used_pct": disk_used_pct,
        "suggestions": {
            "recommended_total_limit": 10 if risk == "high" else (12 if risk == "medium" else total_limit),
            "recommended_per_category": 2 if risk == "high" else 3,
        },
    }


def _run_ingest_bg() -> None:
    if _ingest_state["running"]:
        return
    _ingest_state["running"] = True
    _ingest_state["last_result"] = None
    try:
        try:
            from backend.ingest import run_ingest
        except ImportError:
            from ingest import run_ingest
        result = run_ingest()
        _ingest_state["last_result"] = {
            "ok": True,
            "count": result.get("count", 0),
            "error_count": len(result.get("errors", [])),
            "breaking_count": result.get("breaking_count", 0),
            "generated_at": result.get("generated_at"),
        }
    except Exception as e:
        _ingest_state["last_result"] = {"ok": False, "error": str(e)}
    finally:
        _ingest_state["running"] = False


# ── scheduler ─────────────────────────────────────────────────────────────────

_scheduler = BackgroundScheduler(daemon=True)
_schedule_job_id = "auto_ingest"
_warframe_refresh_job_id = "warframe_refresh"
_WARFRAME_REFRESH_INTERVAL_MINUTES = 15
_WARFRAME_REFRESH_STATE: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "last_run": None,
    "results": {},
}


def _apply_schedule(cfg: dict) -> None:
    """Remove existing job and re-add with current config if enabled."""
    if _scheduler.get_job(_schedule_job_id):
        _scheduler.remove_job(_schedule_job_id)
    if cfg.get("enabled"):
        hours = max(1, int(cfg.get("interval_hours", 2)))
        _scheduler.add_job(
            _run_ingest_bg,
            "interval",
            hours=hours,
            id=_schedule_job_id,
            replace_existing=True,
        )


def _run_warframe_refresh_cycle(platform: str = "pc") -> None:
    if _WARFRAME_REFRESH_STATE.get("running"):
        return

    platform_key = str(platform or "pc").strip().lower() or "pc"
    _WARFRAME_REFRESH_STATE["running"] = True
    _WARFRAME_REFRESH_STATE["started_at"] = datetime.now().isoformat(timespec="seconds")
    results: dict[str, Any] = {}

    def run_step(name: str, fn) -> Any:
        t0 = time.time()
        try:
            payload = fn()
            results[name] = {
                "ok": True,
                "duration_ms": int((time.time() - t0) * 1000),
            }
            if isinstance(payload, dict):
                if payload.get("stale") is not None:
                    results[name]["stale"] = bool(payload.get("stale"))
                if payload.get("cached") is not None:
                    results[name]["cached"] = bool(payload.get("cached"))
            elif isinstance(payload, list):
                results[name]["count"] = len(payload)
            return payload
        except Exception as exc:
            results[name] = {
                "ok": False,
                "duration_ms": int((time.time() - t0) * 1000),
                "error": str(exc)[:160],
            }
            return None

    try:
        tracked_names = [
            str(item.get("name") or "").strip()
            for item in _load_warframe_watchlist()
            if str(item.get("name") or "").strip()
        ]
        seed_names = list(
            dict.fromkeys(
                [
                    "arcane energize",
                    *tracked_names[:16],
                    *_WARFRAME_MARKET_WATCHLIST[:12],
                ]
            )
        )

        run_step("worldstate", lambda: _fetch_warframe_worldstate(platform_key)[0])
        run_step("overview", lambda: warframe_overview("arcane energize", platform_key))
        run_step("hot_items", lambda: {"items": _fetch_warframe_hot_items(platform_key)[0]})
        run_step("market_pulse", lambda: _build_warframe_market_pulse(platform_key)[0])
        run_step("watchlist", lambda: {"items": _build_warframe_watchlist_payload(platform_key, sort_by="priority")[0]})

        if seed_names:
            refreshed = 0

            def refresh_item(name: str) -> bool:
                payload, _errors = _fetch_warframe_market(name, platform_key)
                return isinstance(payload, dict) and (
                    payload.get("last_avg_price") is not None or payload.get("best_sell") is not None
                )

            t0 = time.time()
            with ThreadPoolExecutor(max_workers=min(6, max(1, len(seed_names)))) as pool:
                futures = [pool.submit(refresh_item, name) for name in seed_names]
                for future in as_completed(futures):
                    try:
                        if future.result():
                            refreshed += 1
                    except Exception:
                        continue
            results["market_snapshots"] = {
                "ok": True,
                "duration_ms": int((time.time() - t0) * 1000),
                "count": refreshed,
            }
    finally:
        _WARFRAME_REFRESH_STATE["running"] = False
        _WARFRAME_REFRESH_STATE["last_run"] = datetime.now().isoformat(timespec="seconds")
        _WARFRAME_REFRESH_STATE["started_at"] = None
        _WARFRAME_REFRESH_STATE["results"] = results


def _prewarm_provider_caches() -> None:
    if _PREWARM_STATE.get("running"):
        return

    _PREWARM_STATE["running"] = True
    started_at = datetime.now().isoformat(timespec="seconds")
    _PREWARM_STATE["started_at"] = started_at

    results: dict[str, Any] = {}
    for name, fn in [
        ("markets", lambda: markets_overview()),
        ("f1", lambda: f1_overview("current")),
        ("warframe", lambda: warframe_overview("arcane energize", "pc")),
        ("warframe_pulse", lambda: _build_warframe_market_pulse("pc")[0]),
        ("warframe_hot", lambda: {"items": _fetch_warframe_hot_items("pc")[0]}),
    ]:
        t0 = time.time()
        try:
            payload = fn() or {}
            results[name] = {
                "ok": True,
                "duration_ms": int((time.time() - t0) * 1000),
                "cached": bool(payload.get("cached")),
                "stale": bool(payload.get("stale")),
            }
        except Exception as e:
            results[name] = {
                "ok": False,
                "duration_ms": int((time.time() - t0) * 1000),
                "error": str(e),
            }

    _PREWARM_STATE["running"] = False
    _PREWARM_STATE["last_run"] = datetime.now().isoformat(timespec="seconds")
    _PREWARM_STATE["started_at"] = None
    _PREWARM_STATE["results"] = results


@app.on_event("startup")
def _startup() -> None:
    ensure_f1_history_db(F1_HISTORY_DB_FILE)
    ensure_tldr_db(TLDR_DB_FILE)
    _boot_last_good_store()
    _boot_f1_session_snapshots()
    _boot_f1_secondary_ingest()
    _boot_f1_session_archive()
    _seed_f1_session_archive_from_last_good()
    _sync_f1_history_db_from_archive()
    settings_cfg = _load_settings()
    _apply_settings_env(settings_cfg)

    cfg = _load_schedule()
    _apply_schedule(cfg)
    _scheduler.start()
    _scheduler.add_job(
        _run_warframe_refresh_cycle,
        "interval",
        minutes=_WARFRAME_REFRESH_INTERVAL_MINUTES,
        id=_warframe_refresh_job_id,
        replace_existing=True,
    )

    import threading

    threading.Thread(target=_prewarm_provider_caches, daemon=True).start()
    threading.Thread(target=_run_warframe_refresh_cycle, daemon=True).start()

    if cfg.get("run_at_startup") and cfg.get("enabled"):
        threading.Thread(target=_run_ingest_bg, daemon=True).start()


@app.on_event("shutdown")
def _shutdown() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)


# ── health ────────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/")
def index():
    if FRONTEND_INDEX.exists():
        return FileResponse(FRONTEND_INDEX)
    raise HTTPException(status_code=404, detail="Frontend index not found")


# ── tools ─────────────────────────────────────────────────────────────────────

@app.get("/api/tools", response_model=List[Tool])
def list_tools(
    category: Optional[str] = None,
    q: Optional[str] = None,
    sort: Optional[str] = None,
):
    tools = _load_tools()
    if category:
        tools = [t for t in tools if t.get("category", "").lower() == category.lower()]
    if q:
        ql = q.lower()
        tools = [
            t for t in tools
            if ql in t.get("name", "").lower()
            or ql in t.get("provider", "").lower()
            or ql in t.get("category", "").lower()
            or ql in str(t.get("group") or "").lower()
            or ql in str(t.get("service_kind") or "").lower()
            or ql in str(t.get("environment") or "").lower()
            or ql in str(t.get("host") or "").lower()
            or ql in str(t.get("status") or "").lower()
            or ql in str(t.get("short_hint") or "").lower()
            or any(ql in tag.lower() for tag in t.get("tags", []))
            or any(ql in str(task).lower() for task in t.get("ai_tasks", []))
            or any(ql in str(link.get("label") or "").lower() or ql in str(link.get("url") or "").lower() for link in t.get("links", []))
        ]
    if sort == "rating":
        tools = sorted(tools, key=lambda t: (t.get("rating") or 0, int(t.get("usage_count") or 0)), reverse=True)
    elif sort == "last_used":
        tools = sorted(tools, key=lambda t: t.get("last_used") or "", reverse=True)
    elif sort == "popular":
        tools = sorted(
            tools,
            key=lambda t: (int(t.get("usage_count") or 0), t.get("last_used") or ""),
            reverse=True,
        )
    elif sort == "favorites":
        tools = sorted(
            tools,
            key=lambda t: (bool(t.get("pinned")), bool(t.get("favorite")), int(t.get("usage_count") or 0), t.get("last_used") or ""),
            reverse=True,
        )
    return tools


@app.post("/api/tools", response_model=Tool)
def add_tool(tool: Tool):
    tools = _load_tools()
    if any(t.get("id") == tool.id for t in tools):
        raise HTTPException(status_code=409, detail="Tool id already exists")
    normalized = Tool(**_normalize_tool_record(tool.model_dump())).model_dump()
    tools.append(normalized)
    _save_tools(tools)
    return normalized


@app.patch("/api/tools/{tool_id}", response_model=Tool)
async def update_tool(tool_id: str, request: Request):
    patch = await request.json()
    tools = _load_tools()
    idx = next((i for i, t in enumerate(tools) if t.get("id") == tool_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Tool not found")
    patch.pop("id", None)
    merged = dict(tools[idx])
    merged.update(patch)
    tools[idx] = Tool(**_normalize_tool_record(merged)).model_dump()
    _save_tools(tools)
    return tools[idx]


@app.post("/api/tools/{tool_id}/use")
def mark_tool_used(tool_id: str):
    """Record last_used timestamp when user opens a tool."""
    tools = _load_tools()
    idx = next((i for i, t in enumerate(tools) if t.get("id") == tool_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Tool not found")
    tools[idx]["last_used"] = datetime.now().isoformat(timespec="seconds")
    tools[idx]["usage_count"] = int(tools[idx].get("usage_count") or 0) + 1
    tools[idx] = _normalize_tool_record(tools[idx])
    _save_tools(tools)
    return {"ok": True, "usage_count": tools[idx]["usage_count"]}


@app.delete("/api/tools/{tool_id}")
def delete_tool(tool_id: str):
    tools = _load_tools()
    new_tools = [t for t in tools if t.get("id") != tool_id]
    if len(new_tools) == len(tools):
        raise HTTPException(status_code=404, detail="Tool not found")
    _save_tools(new_tools)
    return {"ok": True, "deleted": tool_id}


# ── sources ───────────────────────────────────────────────────────────────────

@app.get("/api/sources")
def list_sources(category: Optional[str] = None, enabled_only: bool = False):
    sources = _load_sources()
    if category:
        sources = [s for s in sources if s.get("category", "").lower() == category.lower()]
    if enabled_only:
        sources = [s for s in sources if s.get("enabled", False)]
    return sources


@app.patch("/api/sources/{source_id}")
async def update_source(source_id: str, request: Request):
    patch = await request.json()
    sources = _load_sources()
    idx = next((i for i, s in enumerate(sources) if s.get("id") == source_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Source not found")
    patch.pop("id", None)
    sources[idx].update(patch)
    _save_sources(sources)
    return sources[idx]


@app.post("/api/sources")
async def add_source(request: Request):
    source = await request.json()
    if not source.get("id") or not source.get("url"):
        raise HTTPException(status_code=422, detail="id and url are required")
    sources = _load_sources()
    if any(s.get("id") == source["id"] for s in sources):
        raise HTTPException(status_code=409, detail="Source id already exists")
    source.setdefault("type", "rss")
    source.setdefault("enabled", True)
    source.setdefault("trust_weight", 0.7)
    source.setdefault("tags", [])
    sources.append(source)
    _save_sources(sources)
    return source


@app.delete("/api/sources/{source_id}")
def delete_source(source_id: str):
    sources = _load_sources()
    new_sources = [s for s in sources if s.get("id") != source_id]
    if len(new_sources) == len(sources):
        raise HTTPException(status_code=404, detail="Source not found")
    _save_sources(new_sources)
    return {"ok": True, "deleted": source_id}


# ── categories ────────────────────────────────────────────────────────────────

@app.get("/api/categories")
def list_categories():
    tools = _load_tools()
    cats = sorted({t.get("category", "") for t in tools if t.get("category")})
    return {"categories": cats}


# ── feed ──────────────────────────────────────────────────────────────────────

@app.get("/api/feed")
def get_feed(category: Optional[str] = None, q: Optional[str] = None, limit: int = 50):
    payload = _load_feed_payload()
    items = [i for i in payload.get("items", []) if not bool(i.get("digest_only", False))]
    if category:
        items = [i for i in items if str(i.get("category", "")).lower() == category.lower()]
    if q:
        ql = q.lower()
        items = [
            i for i in items
            if ql in str(i.get("title", "")).lower()
            or ql in str(i.get("summary", "")).lower()
            or any(ql in str(t).lower() for t in i.get("tags", []))
        ]
    return {
        "generated_at": payload.get("generated_at"),
        "count": len(items),
        "errors": payload.get("errors", []),
        "items": items[: max(1, min(limit, 500))],
    }


# ── saved items ───────────────────────────────────────────────────────────────

@app.get("/api/saved")
def get_saved():
    return _load_saved()


@app.post("/api/saved")
async def save_item(request: Request):
    item = await request.json()
    if not item.get("url") and not item.get("id"):
        raise HTTPException(status_code=422, detail="url or id required")
    saved = _load_saved()
    key = item.get("url") or item.get("id")
    if any((s.get("url") or s.get("id")) == key for s in saved):
        raise HTTPException(status_code=409, detail="Already saved")
    item["saved_at"] = datetime.now().isoformat(timespec="seconds")
    saved.insert(0, item)
    _save_saved(saved)
    return item


@app.delete("/api/saved/{item_id:path}")
def delete_saved(item_id: str):
    saved = _load_saved()
    new_saved = [s for s in saved if (s.get("url") or s.get("id")) != item_id]
    if len(new_saved) == len(saved):
        raise HTTPException(status_code=404, detail="Item not found")
    _save_saved(new_saved)
    return {"ok": True}


# ── digest ────────────────────────────────────────────────────────────────────

@app.get("/api/digest/morning")
def get_morning_digest(
    total_limit: int = 12,
    per_category: int = 3,
    include_breaking: bool = True,
):
    payload = _load_feed_payload()
    items = payload.get("items", [])
    tldr_items = [i for i in items if str(i.get("newsletter_group", "")).lower() == "tldr"]
    core_items = [i for i in items if str(i.get("newsletter_group", "")).lower() != "tldr"]

    items_sorted = sorted(
        core_items,
        key=lambda i: (
            bool(i.get("breaking", False)),
            float(i.get("daniel_score", 0.0)),
            str(i.get("published_at", "")),
        ),
        reverse=True,
    )

    categories = ["AI", "World", "Politics", "Gaming", "Finance"]
    picked: list[dict] = []

    def already(url_or_id: str) -> bool:
        return any((p.get("url") or p.get("id")) == url_or_id for p in picked)

    base_budget = max(1, min(total_limit, 100))

    def cat_items(cat: str) -> list[dict]:
        if cat.lower() == "gaming":
            return [
                x for x in items_sorted
                if str(x.get("category", "")).lower() in {"gaming", "warframe"}
                and not already(x.get("url") or x.get("id"))
            ]
        return [
            x for x in items_sorted
            if str(x.get("category", "")).lower() == cat.lower()
            and not already(x.get("url") or x.get("id"))
        ]

    for cat in categories:
        if len(picked) >= base_budget:
            break
        c_items = cat_items(cat)
        if c_items:
            picked.append(c_items[0])

    if per_category > 1 and len(picked) < base_budget:
        for round_idx in range(2, per_category + 1):
            for cat in categories:
                if len(picked) >= base_budget:
                    break
                c_items = cat_items(cat)
                if len(c_items) >= round_idx:
                    picked.append(c_items[round_idx - 1])
            if len(picked) >= base_budget:
                break

    if include_breaking:
        breaking = [x for x in items_sorted if x.get("breaking", False) and not already(x.get("url") or x.get("id"))]

        def cat_key(it: dict) -> str:
            c = str(it.get("category", "")).lower()
            return "Gaming" if c == "warframe" else str(it.get("category", "")).title()

        for b in breaking:
            if len(picked) < base_budget:
                picked.append(b)
                continue
            non_breaking_idx = [idx for idx, it in enumerate(picked) if not it.get("breaking", False)]
            if not non_breaking_idx:
                break
            counts: dict[str, int] = {}
            for it in picked:
                k = cat_key(it)
                counts[k] = counts.get(k, 0) + 1
            protected_idx = [
                idx for idx in non_breaking_idx
                if cat_key(picked[idx]) in categories and counts.get(cat_key(picked[idx]), 0) <= 1
            ]
            candidates = [i for i in non_breaking_idx if i not in protected_idx]
            if not candidates:
                continue
            worst_idx = min(candidates, key=lambda idx: float(picked[idx].get("daniel_score", 0.0)))
            if float(b.get("daniel_score", 0.0)) > float(picked[worst_idx].get("daniel_score", 0.0)):
                picked[worst_idx] = b

    picked = sorted(
        picked,
        key=lambda i: (
            bool(i.get("breaking", False)),
            float(i.get("daniel_score", 0.0)),
            str(i.get("published_at", "")),
        ),
        reverse=True,
    )[:base_budget]

    by_cat: dict[str, list[dict]] = {k: [] for k in categories}
    for p in picked:
        c_raw = str(p.get("category", "")).lower()
        if c_raw in {"gaming", "warframe"}:
            by_cat["Gaming"].append(p)
        elif c_raw == "ai":
            by_cat["AI"].append(p)
        elif c_raw == "world":
            by_cat["World"].append(p)
        elif c_raw == "politics":
            by_cat["Politics"].append(p)
        elif c_raw == "finance":
            by_cat["Finance"].append(p)

    finance_summary = payload.get("finance_summary", {})
    coverage = {k: len(by_cat.get(k, [])) for k in categories}
    missing = [k for k, n in coverage.items() if n == 0]

    tldr_issue_rows = list_tldr_issues(TLDR_DB_FILE, limit=12)
    if tldr_issue_rows:
        tldr_sorted = []
        for issue in tldr_issue_rows:
            top_item = ((issue.get("items") or [None])[0] or {})
            tldr_sorted.append(
                {
                    "title": top_item.get("title") or issue.get("subject") or issue.get("newsletter_name") or "TLDR Issue",
                    "url": top_item.get("url") or "",
                    "summary": issue.get("snippet") or issue.get("subject") or "",
                    "issue_date": issue.get("received_at"),
                    "published_at": issue.get("received_at"),
                    "newsletter_label": issue.get("newsletter_name") or "TLDR",
                    "source_name": issue.get("newsletter_name") or "TLDR",
                    "reading_time": "mail issue",
                    "stale": False,
                }
            )
    else:
        tldr_sorted = sorted(
            tldr_items,
            key=lambda i: (
                str(i.get("issue_date") or i.get("published_at") or ""),
                float(i.get("daniel_score", 0.0)),
                str(i.get("published_at", "")),
            ),
            reverse=True,
        )
    tldr_by_newsletter: dict[str, list[dict]] = {}
    for item in tldr_sorted:
        label = str(item.get("newsletter_label") or item.get("source_name") or "TLDR").strip()
        bucket = tldr_by_newsletter.setdefault(label, [])
        if len(bucket) < 3:
            bucket.append(item)
    tldr_total = sum(len(v) for v in tldr_by_newsletter.values())

    return {
        "generated_at": payload.get("generated_at"),
        "breaking_count": int(payload.get("breaking_count", 0)),
        "selected_count": len(picked),
        "errors": payload.get("errors", []),
        "cost_guard": _build_cost_guard(picked, base_budget),
        "finance_summary": finance_summary,
        "finance_watchlist": finance_summary.get("watchlist", [])[:5],
        "cluster_summary": payload.get("cluster_summary", []),
        "digest_coverage": {
            "per_category": coverage,
            "missing_categories": missing,
            "all_core_topics_covered": len(missing) == 0,
        },
        "items": picked,
        "by_category": by_cat,
        "tldr_count": tldr_total,
        "by_newsletter": tldr_by_newsletter,
    }


# ── TLDR mail ─────────────────────────────────────────────────────────────────

@app.get("/api/tldr/status")
def get_tldr_status():
    return _tldr_auth_status()


@app.post("/api/tldr/auth/start")
def start_tldr_auth():
    client_id, client_secret = _tldr_gmail_client_creds()
    try:
        auth_url, state = build_gmail_auth_url(
            TLDR_GMAIL_CLIENT_SECRET_FILE,
            _tldr_auth_redirect_uri(),
            client_id=client_id or None,
            client_secret=client_secret or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _TLDR_GMAIL_AUTH_STATE.clear()
    _TLDR_GMAIL_AUTH_STATE.update(
        {
            "state": state,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    return {"auth_url": auth_url, "state": state}


@app.post("/api/tldr/auth/client-secret")
async def upload_tldr_client_secret(request: Request):
    try:
        payload = await request.json()
        validated = _validate_tldr_client_secret_payload(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    TLDR_GMAIL_CLIENT_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
    TLDR_GMAIL_CLIENT_SECRET_FILE.write_text(
        json.dumps(validated, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {"ok": True, "status": _tldr_auth_status()}


@app.get("/api/tldr/auth/callback", response_class=HTMLResponse)
def tldr_auth_callback(request: Request, state: str = "", code: str = ""):
    expected_state = str(_TLDR_GMAIL_AUTH_STATE.get("state") or "").strip()
    if not expected_state or state != expected_state:
        raise HTTPException(status_code=400, detail="Invalid or expired Gmail OAuth state.")
    if not code:
        raise HTTPException(status_code=400, detail="Missing Gmail OAuth code.")
    client_id, client_secret = _tldr_gmail_client_creds()
    try:
        exchange_gmail_auth_code(
            TLDR_GMAIL_CLIENT_SECRET_FILE,
            _tldr_auth_redirect_uri(),
            authorization_response=str(request.url),
            state=state,
            token_file=TLDR_GMAIL_TOKEN_FILE,
            client_id=client_id or None,
            client_secret=client_secret or None,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    _TLDR_GMAIL_AUTH_STATE.clear()
    return HTMLResponse(
        "<html><body style='font-family:Arial,sans-serif;padding:24px;background:#0f172a;color:#e2e8f0'>"
        "<h2>TLDR Gmail connected</h2>"
        "<p>You can close this tab and return to HolocronHub.</p>"
        "</body></html>"
    )


@app.get("/api/tldr/issues")
def get_tldr_issues(
    newsletter: Optional[str] = None,
    q: Optional[str] = None,
    unread_only: bool = False,
    limit: int = 50,
):
    return {
        "summary": load_tldr_summary(TLDR_DB_FILE),
        "issues": list_tldr_issues(
            TLDR_DB_FILE,
            newsletter=newsletter,
            q=q,
            unread_only=bool(unread_only),
            limit=limit,
        ),
    }


@app.get("/api/tldr/issues/{issue_id:path}")
def get_tldr_issue_detail(issue_id: str):
    issue = get_tldr_issue(TLDR_DB_FILE, issue_id)
    if not issue:
        raise HTTPException(status_code=404, detail="Issue not found")
    return issue


@app.post("/api/tldr/issues/{issue_id:path}/read")
def set_tldr_issue_read(issue_id: str, read: bool = True):
    ok = mark_tldr_issue_read(TLDR_DB_FILE, issue_id, read=bool(read))
    if not ok:
        raise HTTPException(status_code=404, detail="Issue not found")
    issue = get_tldr_issue(TLDR_DB_FILE, issue_id)
    return {"ok": True, "issue": issue, "summary": load_tldr_summary(TLDR_DB_FILE)}


@app.post("/api/tldr/sync")
def sync_tldr_mail(max_results: int = 25, q: Optional[str] = None):
    try:
        return _sync_tldr_from_gmail(max_results=max_results, query=q)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ── settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
def get_settings():
    cfg = _load_settings()
    _apply_settings_env(cfg)
    return _settings_response(cfg)


@app.patch("/api/settings")
async def update_settings(request: Request):
    patch = await request.json()

    cfg = _load_settings()
    updated = _patch_settings(cfg, patch)
    _save_settings(updated)
    _apply_settings_env(updated)

    if isinstance(patch, dict):
        sched_patch = patch.get("schedule")
    else:
        sched_patch = None

    if isinstance(sched_patch, dict):
        sched_cfg = _load_schedule()
        sched_cfg.update({
            k: v for k, v in sched_patch.items()
            if k in {"enabled", "interval_hours", "run_at_startup"}
        })
        try:
            sched_cfg["interval_hours"] = max(1, int(sched_cfg.get("interval_hours", 2)))
        except Exception:
            sched_cfg["interval_hours"] = 2
        _save_schedule(sched_cfg)
        _apply_schedule(sched_cfg)

    return _settings_response(updated)


# ── markets ───────────────────────────────────────────────────────────────────

@app.get("/api/markets/overview")
def markets_overview(symbols: Optional[str] = None, history_range: str = "1mo", history_interval: str = "1d"):
    started = time.perf_counter()
    symbol_list = _coerce_symbol_list(symbols)

    range_key = str(history_range or "1mo").strip().lower()
    if range_key not in {"5d", "1mo", "3mo", "6mo", "1y"}:
        range_key = "1mo"

    interval_key = str(history_interval or "1d").strip().lower()
    if interval_key not in {"15m", "30m", "1h", "1d", "1wk"}:
        interval_key = "1d"

    cache_key = f"markets:{','.join(symbol_list)}:{range_key}:{interval_key}"
    cached = _cache_get(cache_key, ttl_seconds=180)
    if isinstance(cached, dict):
        duration_ms = int((time.perf_counter() - started) * 1000)
        _record_provider_health(
            "markets",
            errors=list((cached.get("errors") or [])) + list((cached.get("history_errors") or [])),
            cached=True,
            summary={"quotes": int(cached.get("count") or 0)},
            duration_ms=duration_ms,
        )
        return {**cached, "cached": True}

    quotes, errors = _fetch_market_quotes(symbol_list)
    history, history_errors = _fetch_markets_history(symbol_list, range_key=range_key, interval=interval_key)

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "data_as_of": generated_at,
        "stale": False,
        "stale_age_seconds": 0,
        "count": len(quotes),
        "requested_symbols": symbol_list,
        "errors": errors,
        "history_errors": history_errors,
        "quotes": quotes,
        "history": history,
        "history_range": range_key,
        "history_interval": interval_key,
    }

    if _markets_payload_has_data(payload):
        _set_last_good(cache_key, payload)
    else:
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=6 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(errors or []) + list(history_errors or []) + [f"fallback:last_good:markets:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["errors"] = fallback_errors[:24]
            stale_payload["history_errors"] = list(history_errors or [])[:24]
            _cache_set(cache_key, stale_payload)
            duration_ms = int((time.perf_counter() - started) * 1000)
            _record_provider_health(
                "markets",
                errors=fallback_errors,
                cached=False,
                summary={"quotes": int(stale_payload.get("count") or 0)},
                duration_ms=duration_ms,
            )
            return {**stale_payload, "cached": False}

    _cache_set(cache_key, payload)
    duration_ms = int((time.perf_counter() - started) * 1000)
    _record_provider_health(
        "markets",
        errors=list(errors or []) + list(history_errors or []),
        cached=False,
        summary={"quotes": len(quotes)},
        duration_ms=duration_ms,
    )
    return {**payload, "cached": False}


@app.get("/api/f1/overview")
def f1_overview(season: Optional[str] = None, force: bool = False):
    started = time.perf_counter()
    season_key = str(season or "current").strip().lower()
    if season_key != "current" and (not season_key.isdigit() or len(season_key) != 4):
        raise HTTPException(status_code=422, detail="season must be 'current' or YYYY")

    cache_key = f"f1:{season_key}"
    cached = None if force else _cache_get(cache_key, ttl_seconds=300)
    if isinstance(cached, dict):
        duration_ms = int((time.perf_counter() - started) * 1000)
        _record_provider_health(
            "f1",
            errors=list(cached.get("errors") or []),
            cached=True,
            summary={"standings": int(cached.get("standings_count") or 0)},
            duration_ms=duration_ms,
        )
        return {**cached, "cached": True}

    overview, errors, source = _fetch_f1_overview(season_key)
    schedule_races = overview.pop("_schedule_races", [])
    weekend, weekend_errors = _fetch_openf1_weekend_context(
        season_key,
        overview.get("next_race"),
        overview.get("latest_race"),
        schedule_races if isinstance(schedule_races, list) else [],
    )
    standings = overview.get("standings", [])
    all_errors = list(errors or []) + list(weekend_errors or [])

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "data_as_of": generated_at,
        "stale": False,
        "stale_age_seconds": 0,
        "season": season_key,
        "source": source,
        "weekend_source": weekend.get("source") if isinstance(weekend, dict) else "openf1",
        "standings_count": len(standings),
        **overview,
        "weekend": weekend,
        "errors": all_errors[:24],
    }

    if _f1_payload_has_data(payload):
        _set_last_good(cache_key, payload)
    else:
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(all_errors or []) + [f"fallback:last_good:f1:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["errors"] = fallback_errors[:24]
            _cache_set(cache_key, stale_payload)
            duration_ms = int((time.perf_counter() - started) * 1000)
            _record_provider_health(
                "f1",
                errors=fallback_errors,
                cached=False,
                summary={"standings": int(stale_payload.get("standings_count") or 0)},
                duration_ms=duration_ms,
            )
            return {**stale_payload, "cached": False}

    _cache_set(cache_key, payload)
    duration_ms = int((time.perf_counter() - started) * 1000)
    _record_provider_health(
        "f1",
        errors=list(all_errors or []),
        cached=False,
        summary={"standings": len(standings)},
        duration_ms=duration_ms,
    )
    return {**payload, "cached": False}


@app.get("/api/f1/session")
def f1_session(session_key: str, force: bool = False):
    started = time.perf_counter()
    session_key = str(session_key or "").strip()
    if not session_key:
        raise HTTPException(status_code=422, detail="session_key is required")

    cache_key = f"f1:session:{session_key}"
    cached = None if force else _cache_get(cache_key, ttl_seconds=45)
    if isinstance(cached, dict):
        return {**cached, "cached": True}

    detail, errors = _fetch_openf1_session_detail(session_key)
    secondary_payload, secondary_age = _get_f1_secondary_session(session_key, max_age_seconds=4 * 3600)
    if isinstance(detail, dict) and detail and isinstance(secondary_payload, dict):
        secondary_session = secondary_payload.get("session") if isinstance(secondary_payload.get("session"), dict) else {}
        if secondary_session:
            detail = {
                **detail,
                "session": {
                    **secondary_session,
                    **(detail.get("session") if isinstance(detail.get("session"), dict) else {}),
                },
            }
        detail_has_live_rows = bool((detail.get("leaderboard") or []) or (detail.get("race_control") or []) or _f1_weather_has_data(detail.get("weather")))
        secondary_has_live_rows = _secondary_f1_session_payload_has_data(secondary_payload)
        if secondary_has_live_rows and not detail_has_live_rows:
            detail = {
                **detail,
                "source": secondary_payload.get("source") or "secondary_ingest",
                "data_as_of": secondary_payload.get("data_as_of") or detail.get("data_as_of"),
                "session": {
                    **secondary_session,
                    **(detail.get("session") if isinstance(detail.get("session"), dict) else {}),
                },
                "live": bool(secondary_payload.get("live")) or bool(detail.get("live")),
                "drivers_count": int(secondary_payload.get("drivers_count") or detail.get("drivers_count") or 0),
                "leaderboard": deepcopy(secondary_payload.get("leaderboard") or []),
                "race_control": deepcopy(secondary_payload.get("race_control") or []),
                "weather": deepcopy(secondary_payload.get("weather") or detail.get("weather") or {}),
            }
            errors = list(errors or []) + [f"fallback:secondary_ingest:f1_session:{secondary_age}s"]
    if not detail:
        if isinstance(secondary_payload, dict):
            fallback_errors = list(errors or []) + [f"fallback:secondary_ingest:f1_session:{secondary_age}s"]
            payload = {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "data_as_of": secondary_payload.get("data_as_of") or secondary_payload.get("generated_at"),
                "stale": secondary_age > 120,
                "stale_age_seconds": secondary_age,
                "session_key": session_key,
                "source": secondary_payload.get("source") or "secondary_ingest",
                "session": deepcopy(secondary_payload.get("session") or {}),
                "live": bool(secondary_payload.get("live")),
                "drivers_count": int(secondary_payload.get("drivers_count") or 0),
                "leaderboard": deepcopy(secondary_payload.get("leaderboard") or []),
                "race_control": deepcopy(secondary_payload.get("race_control") or []),
                "weather": deepcopy(secondary_payload.get("weather") or {}),
                "snapshots": _get_f1_session_snapshots(session_key, limit=10),
                "errors": fallback_errors[:24],
                "duration_ms": int((time.perf_counter() - started) * 1000),
            }
            _archive_f1_session_payload(session_key, payload)
            _cache_set(cache_key, payload)
            return {**payload, "cached": False}
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(errors or []) + [f"fallback:last_good:f1_session:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["snapshots"] = _get_f1_session_snapshots(session_key, limit=10)
            stale_payload["errors"] = fallback_errors[:24]
            _cache_set(cache_key, stale_payload)
            return {**stale_payload, "cached": False}
        archived_payload = _get_f1_archived_session(session_key)
        if isinstance(archived_payload, dict):
            archived_errors = list(errors or []) + ["fallback:archive:f1_session"]
            archived_payload["errors"] = archived_errors[:24]
            archived_payload["duration_ms"] = int((time.perf_counter() - started) * 1000)
            _cache_set(cache_key, archived_payload)
            return {**archived_payload, "cached": False}
        raise HTTPException(status_code=404, detail="session not found")

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "data_as_of": detail.get("data_as_of") or generated_at,
        "stale": False,
        "stale_age_seconds": 0,
        "session_key": session_key,
        **detail,
        "errors": errors[:24],
        "duration_ms": int((time.perf_counter() - started) * 1000),
    }
    if _f1_session_payload_has_data(payload):
        _set_last_good(cache_key, payload)
        _record_f1_session_snapshot(session_key, payload)
        _archive_f1_session_payload(session_key, payload)
    else:
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=12 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(errors or []) + [f"fallback:last_good:f1_session:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["snapshots"] = _get_f1_session_snapshots(session_key, limit=10)
            stale_payload["errors"] = fallback_errors[:24]
            _cache_set(cache_key, stale_payload)
            return {**stale_payload, "cached": False}
    payload["snapshots"] = _get_f1_session_snapshots(session_key, limit=10)
    _cache_set(cache_key, payload)
    return {**payload, "cached": False}


@app.get("/api/f1/history/search")
def f1_history_search(q: str = "", limit: int = Query(default=8, ge=1, le=16)):
    results = search_f1_history_sessions(F1_HISTORY_DB_FILE, q=q, limit=limit)
    if not results:
        results = _search_f1_session_archive(q, limit=limit)
    return {
        "q": q,
        "count": len(results),
        "results": results,
        "source": "f1_history_db" if results else "f1_session_archive",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


@app.get("/api/f1/history/summary")
def f1_history_summary(limit: int = Query(default=6, ge=1, le=12)):
    summary = load_f1_history_summary(F1_HISTORY_DB_FILE, limit=limit)
    return {
        **summary,
        "source": "f1_history_db",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


@app.get("/api/warframe/arsenal")
def warframe_arsenal(q: str = ""):
    payload, errors = _fetch_warframe_arsenal(q)
    return {**payload, "errors": errors}


@app.get("/api/warframe/planner")
def warframe_planner(q: str = "", platform: str = "pc"):
    payload, errors = _build_warframe_farm_plan(q, platform=platform)
    return {**payload, "errors": errors}


@app.get("/api/warframe/relics/profit")
def warframe_relic_profit(platform: str = "pc"):
    payload, errors = _build_warframe_relic_profit_radar(platform)
    return {**payload, "errors": errors}


@app.get("/api/warframe/marketpulse")
def warframe_market_pulse(platform: str = "pc"):
    payload, errors = _build_warframe_market_pulse(platform)
    return {**payload, "errors": errors}


@app.get("/api/warframe/watchlist")
def get_warframe_watchlist(platform: str = "pc", sort_by: str = "priority"):
    items, errors = _build_warframe_watchlist_payload(platform, sort_by=sort_by)
    return {"items": items, "count": len(items), "sort_by": sort_by, "errors": errors}


@app.get("/api/warframe/aleca")
def warframe_aleca():
    payload, errors = _build_alecaframe_payload()
    return {**payload, "errors": errors}


@app.get("/api/warframe/opportunities")
def warframe_opportunities(platform: str = "pc"):
    payload, errors = _build_warframe_personal_opportunities(platform)
    return {**payload, "errors": errors}


@app.post("/api/warframe/watchlist")
async def add_warframe_watchlist_item(request: Request):
    item = await request.json()
    name = str(item.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="name required")
    items = _load_warframe_watchlist()
    item_id = _normalize_warframe_item_name(name)
    if any(str(entry.get("id") or "") == item_id for entry in items):
        raise HTTPException(status_code=409, detail="Already tracked")
    record = {
        "id": item_id,
        "name": name,
        "kind": str(item.get("kind") or "item").strip() or "item",
        "priority": _coerce_watch_priority(item.get("priority")),
        "tags": _normalize_tag_list(item.get("tags") or []),
        "note": str(item.get("note") or "").strip()[:180],
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    items.insert(0, record)
    _save_warframe_watchlist(items)
    return record


@app.delete("/api/warframe/watchlist/{item_id}")
def delete_warframe_watchlist_item(item_id: str):
    items = _load_warframe_watchlist()
    new_items = [entry for entry in items if str(entry.get("id") or "") != item_id]
    if len(new_items) == len(items):
        raise HTTPException(status_code=404, detail="Item not found")
    _save_warframe_watchlist(new_items)
    return {"ok": True}


@app.get("/api/warframe/overview")
def warframe_overview(item: str = "arcane energize", platform: str = "pc", force: bool = False):
    started = time.perf_counter()
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"

    item_key = str(item or "arcane energize").strip().lower()
    cache_key = f"warframe:{platform_key}:{item_key}"
    cached = None if force else _cache_get(cache_key, ttl_seconds=75)
    if isinstance(cached, dict):
        ws = cached.get("worldstate") or {}
        market = cached.get("market") or {}
        hot_items = cached.get("top_sells") or []
        duration_ms = int((time.perf_counter() - started) * 1000)
        _record_provider_health(
            "warframe",
            errors=list(cached.get("errors") or []),
            cached=True,
            summary={
                "alerts": len(ws.get("alerts") or []),
                "fissures": len(ws.get("fissures") or []),
                "best_sell": market.get("best_sell"),
                "hot_items": len(hot_items),
            },
            duration_ms=duration_ms,
        )
        return {**cached, "cached": True}

    with ThreadPoolExecutor(max_workers=3) as pool:
        world_future = pool.submit(_fetch_warframe_worldstate, platform_key)
        market_future = pool.submit(_fetch_warframe_market, item, platform_key)
        hot_future = pool.submit(_fetch_warframe_hot_items, platform_key)
        worldstate, world_errors = world_future.result()
        market, market_errors = market_future.result()
        top_sells, hot_errors = hot_future.result()

    generated_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "generated_at": generated_at,
        "data_as_of": generated_at,
        "stale": False,
        "stale_age_seconds": 0,
        "platform": platform_key,
        "item_query": item,
        "worldstate": worldstate,
        "market": market,
        "top_sells": top_sells,
        "errors": world_errors + market_errors + hot_errors,
    }

    if _warframe_payload_has_data(payload):
        _set_last_good(cache_key, payload)
    else:
        stale_payload, age = _get_last_good(cache_key, max_age_seconds=3 * 3600)
        if isinstance(stale_payload, dict):
            fallback_errors = list(world_errors or []) + list(market_errors or []) + list(hot_errors or []) + [f"fallback:last_good:warframe:{age}s"]
            stale_payload = _mark_payload_stale(stale_payload, age_seconds=age)
            stale_payload["errors"] = fallback_errors[:24]
            _cache_set(cache_key, stale_payload)
            ws = stale_payload.get("worldstate") or {}
            mk = stale_payload.get("market") or {}
            hot_items = stale_payload.get("top_sells") or []
            duration_ms = int((time.perf_counter() - started) * 1000)
            _record_provider_health(
                "warframe",
                errors=fallback_errors,
                cached=False,
                summary={
                    "alerts": len(ws.get("alerts") or []),
                    "fissures": len(ws.get("fissures") or []),
                    "best_sell": mk.get("best_sell"),
                    "hot_items": len(hot_items),
                },
                duration_ms=duration_ms,
            )
            return {**stale_payload, "cached": False}

    _cache_set(cache_key, payload)
    duration_ms = int((time.perf_counter() - started) * 1000)
    _record_provider_health(
        "warframe",
        errors=list(world_errors or []) + list(market_errors or []) + list(hot_errors or []),
        cached=False,
        summary={
            "alerts": len((worldstate or {}).get("alerts") or []),
            "fissures": len((worldstate or {}).get("fissures") or []),
            "best_sell": (market or {}).get("best_sell"),
            "hot_items": len(top_sells),
        },
        duration_ms=duration_ms,
    )
    return {**payload, "cached": False}


_SERVICE_SURFACE_LABELS = {
    "ai_chat": "Chat surface",
    "local_ai_ui": "Local AI UI",
    "local_llm": "Local model runtime",
    "dns": "DNS and ad blocking",
    "smarthome": "Devices and automations",
    "vm_host": "Virtual machines and containers",
    "storage": "Pools and shares",
    "container_admin": "Stacks and containers",
    "monitoring": "Dashboards and alerts",
    "media": "Library and streaming",
}


def _is_home_tool_record(tool: dict[str, Any]) -> bool:
    category = str(tool.get("category") or "").lower()
    return any(token in category for token in ("home", "network", "nas", "infra"))


def _tool_probe_target(tool: dict[str, Any]) -> tuple[str, Optional[int]]:
    host = str(tool.get("host") or "").strip()
    port = tool.get("port")
    link = str(tool.get("link") or "").strip()
    try:
        parsed = urlparse(link)
    except Exception:
        parsed = None
    if not host and parsed:
        host = parsed.hostname or ""
    if port in (None, "") and parsed:
        port = parsed.port
        if port is None and parsed.scheme in {"http", "https"}:
            port = 443 if parsed.scheme == "https" else 80
    try:
        port = int(port) if port is not None else None
    except Exception:
        port = None
    return host, port


def _home_tool_surface_label(tool: dict[str, Any]) -> str:
    service_kind = str(tool.get("service_kind") or "").strip().lower()
    if service_kind in _SERVICE_SURFACE_LABELS:
        return _SERVICE_SURFACE_LABELS[service_kind]
    group = str(tool.get("group") or "").strip()
    if group:
        return f"{group} service"
    category = str(tool.get("category") or "").strip()
    return category or "Home service"


def _probe_home_tool(tool: dict[str, Any]) -> dict[str, Any]:
    host, port = _tool_probe_target(tool)
    checked_at = datetime.now().isoformat(timespec="seconds")
    payload = {
        "id": tool.get("id"),
        "status": "unknown",
        "status_checked_at": checked_at,
        "latency_ms": None,
        "probe_host": host,
        "probe_port": port,
        "surface": _home_tool_surface_label(tool),
        "link_count": len(tool.get("links") or []),
    }
    if not host or port is None:
        return payload
    started = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=0.6):
            latency_ms = max(1, int((time.perf_counter() - started) * 1000))
            payload["status"] = "online"
            payload["latency_ms"] = latency_ms
    except Exception as exc:
        payload["status"] = "offline"
        payload["error"] = str(exc)[:120]
    return payload


@app.get("/api/tools/home-lab/overview")
def home_lab_overview():
    tools = [tool for tool in _load_tools() if _is_home_tool_record(tool)]
    generated_at = datetime.now().isoformat(timespec="seconds")
    if not tools:
        return {
            "generated_at": generated_at,
            "summary": {"total": 0, "online": 0, "offline": 0, "unknown": 0, "deep_links": 0},
            "groups": [],
            "service_kinds": [],
            "services": [],
        }

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(8, len(tools))) as executor:
        future_map = {executor.submit(_probe_home_tool, tool): tool for tool in tools}
        for future in as_completed(future_map):
            tool = future_map[future]
            probe = future.result()
            results.append({**tool, **probe})

    results.sort(key=lambda item: (str(item.get("group") or ""), str(item.get("name") or "")))
    summary = {
        "total": len(results),
        "online": sum(1 for item in results if item.get("status") == "online"),
        "offline": sum(1 for item in results if item.get("status") == "offline"),
        "unknown": sum(1 for item in results if item.get("status") == "unknown"),
        "deep_links": sum(int(item.get("link_count") or 0) for item in results),
    }
    group_counts = sorted(
        [
            {"name": name, "count": count}
            for name, count in {
                str(item.get("group") or "General"): sum(1 for row in results if str(row.get("group") or "General") == str(item.get("group") or "General"))
                for item in results
            }.items()
        ],
        key=lambda item: (-int(item.get("count") or 0), str(item.get("name") or "")),
    )
    kind_counts = sorted(
        [
            {"name": name, "count": count}
            for name, count in {
                str(item.get("service_kind") or "service"): sum(1 for row in results if str(row.get("service_kind") or "service") == str(item.get("service_kind") or "service"))
                for item in results
            }.items()
        ],
        key=lambda item: (-int(item.get("count") or 0), str(item.get("name") or "")),
    )
    return {
        "generated_at": generated_at,
        "summary": summary,
        "groups": group_counts,
        "service_kinds": kind_counts,
        "services": results,
    }


@app.get("/api/debug/providers")
def debug_providers():
    cache_counts = {
        "entries": len(_API_CACHE),
        "markets": sum(1 for k in _API_CACHE.keys() if k.startswith("markets:")),
        "f1": sum(1 for k in _API_CACHE.keys() if k.startswith("f1:")),
        "warframe": sum(1 for k in _API_CACHE.keys() if k.startswith("warframe:")),
    }
    last_good_counts = {
        "entries": len(_LAST_GOOD),
        "markets": sum(1 for k in _LAST_GOOD.keys() if k.startswith("markets:")),
        "f1": sum(1 for k in _LAST_GOOD.keys() if k.startswith("f1:")),
        "warframe": sum(1 for k in _LAST_GOOD.keys() if k.startswith("warframe:")),
    }
    slowest = sorted(
        [
            {
                "name": name,
                "status": state.get("status"),
                "cached": state.get("cached"),
                "last_duration_ms": state.get("last_duration_ms"),
                "avg_duration_ms": state.get("avg_duration_ms"),
                "max_duration_ms": state.get("max_duration_ms"),
                "sample_count": state.get("sample_count"),
                "slow_count": state.get("slow_count"),
            }
            for name, state in _provider_health.items()
        ],
        key=lambda item: float(item.get("avg_duration_ms") or item.get("last_duration_ms") or 0),
        reverse=True,
    )
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "providers": _provider_health,
        "provider_breakers": deepcopy(_PROVIDER_BREAKERS),
        "cache": cache_counts,
        "last_good": last_good_counts,
        "f1_secondary_ingest": {
            "sessions": len(_F1_SECONDARY_INGEST),
            "live_sessions": sum(1 for entry in _F1_SECONDARY_INGEST.values() if isinstance(entry, dict) and entry.get("live")),
        },
        "f1_snapshots": {
            "sessions": len(_F1_SESSION_SNAPSHOTS),
            "entries": sum(len(rows) for rows in _F1_SESSION_SNAPSHOTS.values()),
        },
        "prewarm": deepcopy(_PREWARM_STATE),
        "slowest": slowest[:3],
    }


@app.get("/api/schedule")
def get_schedule():
    cfg = _load_schedule()
    job = _scheduler.get_job(_schedule_job_id)
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return {**cfg, "next_run": next_run}


@app.patch("/api/schedule")
async def update_schedule(request: Request):
    patch = await request.json()
    cfg = _load_schedule()
    cfg.update({k: v for k, v in patch.items() if k in {"enabled", "interval_hours", "run_at_startup"}})
    try:
        cfg["interval_hours"] = max(1, int(cfg.get("interval_hours", 2)))
    except Exception:
        cfg["interval_hours"] = 2
    _save_schedule(cfg)
    _apply_schedule(cfg)
    job = _scheduler.get_job(_schedule_job_id)
    next_run = job.next_run_time.isoformat() if job and job.next_run_time else None
    return {**cfg, "next_run": next_run}


# ── ingest ────────────────────────────────────────────────────────────────────

@app.post("/api/ingest")
def trigger_ingest(background_tasks: BackgroundTasks):
    if _ingest_state["running"]:
        raise HTTPException(status_code=409, detail="Ingest already running")
    _ingest_state["started_at"] = datetime.now().isoformat(timespec="seconds")
    background_tasks.add_task(_run_ingest_bg)
    return {"ok": True, "status": "started", "started_at": _ingest_state["started_at"]}


@app.get("/api/ingest/status")
def ingest_status():
    return {
        "running": _ingest_state["running"],
        "started_at": _ingest_state["started_at"],
        "last_result": _ingest_state["last_result"],
    }


@app.post("/api/f1/ingest/session")
def ingest_f1_session(payload: F1SecondaryIngestPayload):
    session_key = str(payload.session_key or "").strip()
    if not session_key:
        raise HTTPException(status_code=422, detail="session_key is required")

    data_as_of = payload.data_as_of or datetime.now(timezone.utc).isoformat()
    entry = {
        "source": str(payload.source or "secondary_ingest").strip() or "secondary_ingest",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_as_of": data_as_of,
        "session": deepcopy(payload.session),
        "live": bool(payload.live),
        "drivers_count": int(payload.drivers_count or 0),
        "leaderboard": deepcopy(payload.leaderboard),
        "race_control": deepcopy(payload.race_control),
        "weather": deepcopy(payload.weather),
    }
    _set_f1_secondary_session(session_key, entry)

    snapshot_payload = {
        "generated_at": entry["generated_at"],
        "data_as_of": data_as_of,
        "source": entry["source"],
        "live": entry["live"],
        "leaderboard": deepcopy(entry["leaderboard"]),
        "race_control": deepcopy(entry["race_control"]),
        "weather": deepcopy(entry["weather"]),
    }
    if _secondary_f1_session_payload_has_data(entry):
        _record_f1_session_snapshot(session_key, snapshot_payload)
        _set_last_good(f"f1:session:{session_key}", {
            "generated_at": entry["generated_at"],
            "data_as_of": data_as_of,
            "session_key": session_key,
            "source": entry["source"],
            "session": deepcopy(entry["session"]),
            "live": entry["live"],
            "drivers_count": entry["drivers_count"],
            "leaderboard": deepcopy(entry["leaderboard"]),
            "race_control": deepcopy(entry["race_control"]),
            "weather": deepcopy(entry["weather"]),
            "snapshots": _get_f1_session_snapshots(session_key, limit=10),
            "errors": [],
        })
        _archive_f1_session_payload(session_key, {
            "generated_at": entry["generated_at"],
            "data_as_of": data_as_of,
            "session_key": session_key,
            "source": entry["source"],
            "session": deepcopy(entry["session"]),
            "live": entry["live"],
            "drivers_count": entry["drivers_count"],
            "leaderboard": deepcopy(entry["leaderboard"]),
            "race_control": deepcopy(entry["race_control"]),
            "weather": deepcopy(entry["weather"]),
        })

    return {
        "ok": True,
        "session_key": session_key,
        "source": entry["source"],
        "drivers_count": entry["drivers_count"],
        "leaderboard_rows": len(entry["leaderboard"]),
        "race_control_rows": len(entry["race_control"]),
        "data_as_of": data_as_of,
    }


@app.get("/api/f1/ingest/status")
def f1_ingest_status():
    now = datetime.now(timezone.utc)
    sessions = []
    for session_key, payload in _F1_SECONDARY_INGEST.items():
        if not isinstance(payload, dict):
            continue
        dt = _parse_dt(payload.get("data_as_of") or payload.get("generated_at"))
        age_seconds = None
        if dt is not None:
            age_seconds = max(0, int((now - dt.astimezone(timezone.utc)).total_seconds()))
        sessions.append(
            {
                "session_key": session_key,
                "source": payload.get("source"),
                "live": bool(payload.get("live")),
                "drivers_count": int(payload.get("drivers_count") or 0),
                "leaderboard_rows": len(payload.get("leaderboard") or []),
                "race_control_rows": len(payload.get("race_control") or []),
                "data_as_of": payload.get("data_as_of") or payload.get("generated_at"),
                "age_seconds": age_seconds,
            }
        )
    sessions.sort(key=lambda row: (row.get("age_seconds") is None, row.get("age_seconds") or 0))
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(sessions),
        "sessions": sessions[:24],
    }


# ── status ────────────────────────────────────────────────────────────────────

@app.get("/api/status", response_model=AppStatus)
def status():
    tools = _load_tools()
    current_model = os.getenv("OPENCLAW_MODEL", "unknown")
    now = datetime.now().isoformat(timespec="seconds")
    return AppStatus(
        app="holocronhub",
        mode="single-user-local",
        tools_count=len(tools),
        agents=[
            AgentStatus(name="research", status="idle", current_model=current_model, last_run=now),
            AgentStatus(name="planning", status="idle", current_model=current_model, last_run=now),
            AgentStatus(name="builder", status="idle", current_model=current_model, last_run=now),
            AgentStatus(name="review", status="idle", current_model=current_model, last_run=now),
        ],
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8787, reload=True)


















