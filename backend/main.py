from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_FILE = BASE_DIR / "data" / "tools.json"
SAMPLE_FILE = BASE_DIR / "data" / "tools.sample.json"
SOURCES_FILE = BASE_DIR / "data" / "sources.json"
FEED_FILE = BASE_DIR / "data" / "feed_items.json"
SAVED_FILE = BASE_DIR / "data" / "saved_items.json"
SCHEDULE_FILE = BASE_DIR / "data" / "schedule.json"


# ── models ────────────────────────────────────────────────────────────────────

class Tool(BaseModel):
    id: str
    name: str
    category: str
    provider: str
    link: str
    local_or_cloud: str
    auth_type: str
    cost_hint: str
    notes: Optional[str] = ""
    tags: List[str] = Field(default_factory=list)
    rating: Optional[int] = None        # 1-5, personal score
    last_used: Optional[str] = None     # ISO timestamp


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
    _save_tools(tools)
    return {"ok": True}


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
    cfg["interval_hours"] = max(1, int(cfg.get("interval_hours", 2)))
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
