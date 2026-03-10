from __future__ import annotations

import json
import os
import re
import shutil
import requests
from urllib.parse import quote, quote_plus
from statistics import median
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "tools.json"
SAMPLE_FILE = BASE_DIR / "data" / "tools.sample.json"
SOURCES_FILE = BASE_DIR / "data" / "sources.json"
FEED_FILE = BASE_DIR / "data" / "feed_items.json"
SAVED_FILE = BASE_DIR / "data" / "saved_items.json"
SCHEDULE_FILE = BASE_DIR / "data" / "schedule.json"
SETTINGS_FILE = BASE_DIR / "data" / "settings.json"
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


# ── app + state ───────────────────────────────────────────────────────────────

app = FastAPI(title="Holocron API", version="0.2.0")

_ingest_state: dict = {"running": False, "last_result": None, "started_at": None}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── data helpers ──────────────────────────────────────────────────────────────

def _ensure_data_file() -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not DATA_FILE.exists():
        src = SAMPLE_FILE if SAMPLE_FILE.exists() else None
        DATA_FILE.write_text(src.read_text(encoding="utf-8") if src else "[]", encoding="utf-8")


def _load_tools() -> list[dict]:
    _ensure_data_file()
    try:
        return json.loads(DATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_tools(items: list[dict]) -> None:
    DATA_FILE.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")


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
    },
}

_ALLOWED_SETTINGS_PATCH = {
    "ux": {"language", "default_category", "show_disabled_sources"},
    "feed": {"refresh_interval_minutes", "digest_mode"},
    "models": {"openclaw_model"},
    "api_keys": {"marketaux_api_key", "finnhub_api_key", "alphavantage_api_key"},
}

_API_KEY_ENV_MAP = {
    "marketaux_api_key": "MARKETAUX_API_KEY",
    "finnhub_api_key": "FINNHUB_API_KEY",
    "alphavantage_api_key": "ALPHAVANTAGE_API_KEY",
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


def _http_get_json(
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 12,
) -> tuple[Optional[Any], Optional[str]]:
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def _coerce_symbol_list(raw: Optional[str]) -> list[str]:
    if not raw:
        return list(_DEFAULT_MARKET_SYMBOLS)
    items = [s.strip().upper() for s in raw.split(",") if s.strip()]
    if not items:
        return list(_DEFAULT_MARKET_SYMBOLS)
    return items[:30]


def _fetch_market_quotes(symbols: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    errors: list[str] = []
    try:
        r = requests.get(
            "https://query1.finance.yahoo.com/v7/finance/quote",
            params={"symbols": ",".join(symbols)},
            timeout=12,
        )
        r.raise_for_status()
        payload = r.json().get("quoteResponse", {}).get("result", [])
    except Exception as e:
        return [], [str(e)]

    out: list[dict[str, Any]] = []
    found = {str(q.get("symbol", "")).upper() for q in payload}
    for sym in symbols:
        q = next((x for x in payload if str(x.get("symbol", "")).upper() == sym.upper()), None)
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
    for m in missing:
        if f"symbol_not_found:{m}" not in errors:
            errors.append(f"symbol_not_found:{m}")

    return out, errors


def _fetch_market_history(symbol: str, *, range_key: str = "1mo", interval: str = "1d") -> tuple[list[dict[str, Any]], Optional[str]]:
    safe_symbol = quote(symbol, safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{safe_symbol}"
    data, err = _http_get_json(url, params={"range": range_key, "interval": interval}, timeout=15)
    if err:
        return [], err

    result = _dig(data, "chart", "result") or []
    if not isinstance(result, list) or not result:
        return [], "empty_chart_result"

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

    return points, None


def _fetch_markets_history(
    symbols: list[str],
    *,
    range_key: str = "1mo",
    interval: str = "1d",
    max_symbols: int = 15,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    out: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    for sym in symbols[:max_symbols]:
        series, err = _fetch_market_history(sym, range_key=range_key, interval=interval)
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


def _f1_status_is_finish(status: str) -> bool:
    st = str(status or "").strip().lower()
    if not st:
        return False
    if st.startswith("finished"):
        return True
    return "lap" in st and "retired" not in st


def _f1_driver_key(driver: dict[str, Any]) -> str:
    return str(driver.get("driverId") or driver.get("code") or driver.get("familyName") or "unknown").strip().lower()


def _fetch_f1_overview(season: str) -> tuple[dict[str, Any], list[str], str]:
    errors: list[str] = []
    source = "unknown"

    standings_urls = [
        ("ergast", f"https://ergast.com/api/f1/{season}/driverStandings.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/driverStandings.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/driverStandings/"),
    ]
    schedule_urls = [
        ("ergast", f"https://ergast.com/api/f1/{season}.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}.json"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/races/"),
    ]
    results_urls = [
        ("ergast", f"https://ergast.com/api/f1/{season}/results.json?limit=500"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/results.json?limit=500"),
        ("jolpica", f"https://api.jolpi.ca/ergast/f1/{season}/results/?limit=500"),
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
    }, errors[:20], source


def _normalize_warframe_item_name(item: str) -> str:
    s = str(item or "").lower().strip()
    s = s.replace("&", " and ").replace("'", " ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def _fetch_warframe_worldstate(platform: str) -> tuple[dict[str, Any], list[str]]:
    data, err = _http_get_json(f"https://api.warframestat.us/{platform}", timeout=15)
    if err or not isinstance(data, dict):
        return {
            "platform": platform,
            "timestamp": None,
            "news": [],
            "alerts": [],
            "fissures": [],
            "invasions": [],
            "events": [],
            "sortie": {},
            "nightwave": {},
            "world_cycles": {},
        }, [err or "invalid_worldstate_payload"]

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
    }, []


def _fetch_warframe_market(item: str) -> tuple[dict[str, Any], list[str]]:
    slug = _normalize_warframe_item_name(item)
    if not slug:
        return {"item": item, "slug": "", "best_sell": None, "best_buy": None}, ["invalid_item"]

    req_headers = {"Accept": "application/json", "Language": "en", "User-Agent": "HolocronHub/0.3"}
    data, err = _http_get_json(
        f"https://api.warframe.market/v1/items/{slug}/orders",
        headers=req_headers,
        timeout=15,
    )
    if err or not isinstance(data, dict):
        return {"item": item, "slug": slug, "best_sell": None, "best_buy": None}, [err or "invalid_market_payload"]

    orders = _dig(data, "payload", "orders") or []
    sells_live: list[int] = []
    buys_live: list[int] = []
    sells_all: list[int] = []
    buys_all: list[int] = []
    sell_rows_live: list[dict[str, Any]] = []
    buy_rows_live: list[dict[str, Any]] = []

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
            plat_int = int(plat)
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
            if is_live:
                sells_live.append(plat_int)
                sell_rows_live.append(row)
        elif order_type == "buy":
            buys_all.append(plat_int)
            if is_live:
                buys_live.append(plat_int)
                buy_rows_live.append(row)

    sell_base = sorted(sells_live) if sells_live else sorted(sells_all)
    buy_base = sorted(buys_live, reverse=True) if buys_live else sorted(buys_all, reverse=True)

    stats_data, stats_err = _http_get_json(
        f"https://api.warframe.market/v1/items/{slug}/statistics",
        headers=req_headers,
        timeout=15,
    )
    stats_closed = _dig(stats_data, "payload", "statistics_closed") or {}
    period_key = "48hours" if isinstance(stats_closed, dict) and stats_closed.get("48hours") else ("90days" if isinstance(stats_closed, dict) and stats_closed.get("90days") else None)
    history_raw = stats_closed.get(period_key, []) if period_key else []
    history: list[dict[str, Any]] = []
    for point in history_raw[-40:]:
        if not isinstance(point, dict):
            continue
        avg_price = point.get("avg_price")
        if avg_price is None:
            continue
        history.append(
            {
                "datetime": point.get("datetime"),
                "avg_price": avg_price,
                "volume": point.get("volume"),
            }
        )

    errors: list[str] = []
    if stats_err:
        errors.append(f"statistics:{stats_err}")

    return {
        "item": item,
        "slug": slug,
        "best_sell": sell_base[0] if sell_base else None,
        "best_buy": buy_base[0] if buy_base else None,
        "median_sell": float(median(sell_base[:20])) if sell_base else None,
        "median_buy": float(median(buy_base[:20])) if buy_base else None,
        "sample_sell_orders": sorted(sell_rows_live, key=lambda x: x["price"])[:8],
        "sample_buy_orders": sorted(buy_rows_live, key=lambda x: x["price"], reverse=True)[:8],
        "sell_count_live": len(sells_live),
        "buy_count_live": len(buys_live),
        "sell_count_total": len(sells_all),
        "buy_count_total": len(buys_all),
        "history_period": period_key,
        "history": history,
        "selected_scope": "live" if sells_live or buys_live else "all_visible",
    }, errors


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


@app.on_event("startup")
def _startup() -> None:
    settings_cfg = _load_settings()
    _apply_settings_env(settings_cfg)

    cfg = _load_schedule()
    _apply_schedule(cfg)
    _scheduler.start()
    if cfg.get("run_at_startup") and cfg.get("enabled"):
        import threading
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
            or any(ql in tag.lower() for tag in t.get("tags", []))
        ]
    if sort == "rating":
        tools = sorted(tools, key=lambda t: t.get("rating") or 0, reverse=True)
    elif sort == "last_used":
        tools = sorted(tools, key=lambda t: t.get("last_used") or "", reverse=True)
    elif sort == "popular":
        tools = sorted(
            tools,
            key=lambda t: (int(t.get("usage_count") or 0), t.get("last_used") or ""),
            reverse=True,
        )
    return tools


@app.post("/api/tools", response_model=Tool)
def add_tool(tool: Tool):
    tools = _load_tools()
    if any(t.get("id") == tool.id for t in tools):
        raise HTTPException(status_code=409, detail="Tool id already exists")
    tools.append(tool.model_dump())
    _save_tools(tools)
    return tool


@app.patch("/api/tools/{tool_id}", response_model=Tool)
async def update_tool(tool_id: str, request: Request):
    patch = await request.json()
    tools = _load_tools()
    idx = next((i for i, t in enumerate(tools) if t.get("id") == tool_id), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Tool not found")
    patch.pop("id", None)
    tools[idx].update(patch)
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
    items = payload.get("items", [])
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

    items_sorted = sorted(
        items,
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
    }


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
    symbol_list = _coerce_symbol_list(symbols)
    quotes, errors = _fetch_market_quotes(symbol_list)

    range_key = str(history_range or "1mo").strip().lower()
    if range_key not in {"5d", "1mo", "3mo", "6mo", "1y"}:
        range_key = "1mo"

    interval_key = str(history_interval or "1d").strip().lower()
    if interval_key not in {"15m", "30m", "1h", "1d", "1wk"}:
        interval_key = "1d"

    history, history_errors = _fetch_markets_history(symbol_list, range_key=range_key, interval=interval_key)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(quotes),
        "requested_symbols": symbol_list,
        "errors": errors,
        "history_errors": history_errors,
        "quotes": quotes,
        "history": history,
        "history_range": range_key,
        "history_interval": interval_key,
    }


@app.get("/api/f1/overview")
def f1_overview(season: Optional[str] = None):
    season_key = str(season or "current").strip().lower()
    if season_key != "current" and (not season_key.isdigit() or len(season_key) != 4):
        raise HTTPException(status_code=422, detail="season must be 'current' or YYYY")

    overview, errors, source = _fetch_f1_overview(season_key)
    standings = overview.get("standings", [])
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "season": season_key,
        "source": source,
        "standings_count": len(standings),
        **overview,
        "errors": errors,
    }


@app.get("/api/warframe/overview")
def warframe_overview(item: str = "arcane energize", platform: str = "pc"):
    platform_key = str(platform or "pc").strip().lower()
    if platform_key not in {"pc", "ps4", "xb1", "swi"}:
        platform_key = "pc"

    worldstate, world_errors = _fetch_warframe_worldstate(platform_key)
    market, market_errors = _fetch_warframe_market(item)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "platform": platform_key,
        "item_query": item,
        "worldstate": worldstate,
        "market": market,
        "errors": world_errors + market_errors,
    }


# ── schedule ──────────────────────────────────────────────────────────────────

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


# ── status ────────────────────────────────────────────────────────────────────

@app.get("/api/status", response_model=AppStatus)
def status():
    tools = _load_tools()
    current_model = os.getenv("OPENCLAW_MODEL", "unknown")
    now = datetime.now().isoformat(timespec="seconds")
    return AppStatus(
        app="holocron",
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













