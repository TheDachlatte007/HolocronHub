from __future__ import annotations

import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any

import feedparser
import requests

from normalize import normalize_rss_entry, normalize_api_entry

BASE_DIR = Path(__file__).resolve().parents[1]
SOURCES_FILE = BASE_DIR / "data" / "sources.json"
FEED_FILE = BASE_DIR / "data" / "feed_items.json"
ENV_FILE = Path(__file__).resolve().parent / ".env"


def _load_local_env() -> None:
    """Load backend/.env without external deps.

    Supports common variants like `KEY=value` and `KEY= value`.
    Existing process env vars keep precedence.
    """
    if not ENV_FILE.exists():
        return

    for raw in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip()
        if not key:
            continue
        os.environ.setdefault(key, val)


def load_sources() -> list[dict]:
    if not SOURCES_FILE.exists():
        return []
    return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))


def _finance_markers(item: dict[str, Any]) -> list[dict[str, str]]:
    text = f"{item.get('title','')} {item.get('summary','')}".lower()
    rules = [
        ("rate-cut", ["rate cut", "interest rate cut", "fed cuts", "ecb cuts"], "watch"),
        ("rate-hike", ["rate hike", "interest rate hike", "fed hikes", "ecb hikes"], "watch"),
        ("earnings-surprise", ["beats estimates", "misses estimates", "earnings"], "watch"),
        ("market-volatility", ["selloff", "surge", "plunge", "volatility", "vix"], "watch"),
        ("liquidity-risk", ["liquidity", "default", "downgrade", "bank run"], "critical"),
        ("geopolitical-shock", ["sanctions", "war", "strait", "tariff"], "watch"),
    ]
    out: list[dict[str, str]] = []
    for marker_id, needles, severity in rules:
        if any(n in text for n in needles):
            out.append({"id": marker_id, "severity": severity})
    return out


def _price_markers(item: dict[str, Any]) -> list[dict[str, str]]:
    text = f"{item.get('title','')} {item.get('summary','')}".lower()
    rules = [
        ("btc-move", ["bitcoin", "btc", "ethereum", "eth"], "watch"),
        ("stocks-gap", ["futures", "pre-market", "premarket", "after-hours"], "watch"),
        ("vol-spike", ["vix", "fear index", "implied volatility"], "watch"),
        ("bond-yield-shift", ["bond yield", "treasury yield", "bund yield"], "watch"),
    ]
    out: list[dict[str, str]] = []
    for marker_id, needles, severity in rules:
        if any(n in text for n in needles):
            out.append({"id": marker_id, "severity": severity})
    return out


def _daniel_score(item: dict[str, Any]) -> tuple[float, list[str]]:
    # Personal relevance profile (lightweight v1)
    # Scale 0..100
    title = str(item.get("title", "")).lower()
    summary = str(item.get("summary", "")).lower()
    text = f"{title} {summary}"
    category = str(item.get("category", "")).lower()

    score = 20.0
    reasons: list[str] = []

    category_boost = {
        "ai": 22,
        "gaming": 20,
        "warframe": 28,
        "world": 14,
        "politics": 12,
        "finance": 10,
    }
    if category in category_boost:
        score += category_boost[category]
        reasons.append(f"category:{category}")

    keywords = {
        "warframe": 18,
        "self-host": 10,
        "self-hosted": 10,
        "docker": 8,
        "ollama": 12,
        "openclaw": 12,
        "nvidia": 6,
        "gpu": 6,
        "linux": 8,
        "security": 5,
        "steam": 6,
        "pc gaming": 8,
        "retro": 5,
        "emulation": 5,
        "ai model": 8,
        "llm": 8,
    }

    for kw, w in keywords.items():
        if kw in text:
            score += w
            reasons.append(f"kw:{kw}")

    # Penalize obvious low-value commercial listicles/deals for morning focus
    low_signal = ["best deals", "discount", "coupon", "buy now", "sponsored"]
    if any(x in text for x in low_signal):
        score -= 12
        reasons.append("low-signal:commercial")

    # Small trust influence
    trust = float(item.get("score", 50.0)) / 100.0  # from normalize baseline
    score = score * (0.85 + 0.3 * trust)

    score = max(0.0, min(100.0, round(score, 2)))
    return score, reasons[:8]


def _item_confidence(item: dict[str, Any]) -> tuple[float, str, list[str]]:
    """Confidence score for summary reliability (not relevance)."""
    score = 50.0
    reasons: list[str] = []

    score += float(item.get("score", 70.0)) * 0.35  # source trust influence

    title = str(item.get("title", ""))
    summary = str(item.get("summary", ""))
    provider = str(item.get("provider", "")).lower()
    text = f"{title} {summary}".lower()

    if len(summary) >= 80:
        score += 6
        reasons.append("summary:rich")
    else:
        score -= 6
        reasons.append("summary:short")

    if re.search(r"\b(reuters|ap|bbc|tagesschau|dw|ft|wsj|bloomberg|cnbc)\b", text):
        score += 5
        reasons.append("wire_or_major_media")

    # Provider quality tilt for finance noise reduction
    low_signal_providers = ["marketbeat", "stockinvest", "tradingview", "finviz"]
    if any(p in provider for p in low_signal_providers):
        score -= 12
        reasons.append("provider:low_signal")

    low_signal_patterns = [
        "how to buy",
        "given consensus rating",
        "upgraded to",
        "lowered to",
        "shares sold",
        "acquired by",
    ]
    if any(p in text for p in low_signal_patterns):
        score -= 10
        reasons.append("pattern:rating_or_filing_noise")

    if any(x in text for x in ["deal", "buy now", "sponsored", "affiliate"]):
        score -= 10
        reasons.append("commercial_noise")

    if item.get("markers") and any(m.get("severity") == "critical" for m in item.get("markers", [])):
        score += 3
        reasons.append("critical_marker")

    score = max(0.0, min(100.0, round(score, 2)))
    if score >= 80:
        label = "high"
    elif score >= 60:
        label = "medium"
    else:
        label = "low"
    return score, label, reasons[:6]


def _marketaux_items(src: dict, max_per_source: int) -> list[dict]:
    key = os.getenv("MARKETAUX_API_KEY")
    if not key:
        raise RuntimeError("MARKETAUX_API_KEY not set")
    r = requests.get(
        "https://api.marketaux.com/v1/news/all",
        params={"api_token": key, "language": "en", "limit": max_per_source},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    return [normalize_api_entry(src, e, provider="marketaux") for e in data]


def _finnhub_items(src: dict, max_per_source: int) -> list[dict]:
    key = os.getenv("FINNHUB_API_KEY")
    if not key:
        raise RuntimeError("FINNHUB_API_KEY not set")
    r = requests.get(
        "https://finnhub.io/api/v1/news",
        params={"category": "general", "token": key},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()[:max_per_source]
    return [normalize_api_entry(src, e, provider="finnhub") for e in data]


def _alphavantage_items(src: dict, max_per_source: int) -> list[dict]:
    key = os.getenv("ALPHAVANTAGE_API_KEY")
    if not key:
        raise RuntimeError("ALPHAVANTAGE_API_KEY not set")
    r = requests.get(
        "https://www.alphavantage.co/query",
        params={"function": "NEWS_SENTIMENT", "apikey": key, "limit": max_per_source},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json().get("feed", [])[:max_per_source]
    return [normalize_api_entry(src, e, provider="alphavantage") for e in data]


def _fetch_api_items(src: dict, max_per_source: int) -> list[dict]:
    sid = src.get("id", "")
    if sid == "finance-marketaux":
        return _marketaux_items(src, max_per_source)
    if sid == "finance-finnhub":
        return _finnhub_items(src, max_per_source)
    if sid == "finance-alpha-vantage":
        return _alphavantage_items(src, max_per_source)
    raise RuntimeError(f"No adapter for api source '{sid}'")


def _is_newer(a: dict[str, Any], b: dict[str, Any]) -> bool:
    return str(a.get("published_at", "")) > str(b.get("published_at", ""))


def _parse_published_at(value: str) -> datetime | None:
    if not value:
        return None
    v = value.strip()
    try:
        # ISO-like timestamps
        if "+" in v or "-" in v or v.endswith("Z"):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        # AlphaVantage style: 20260307T095853
        if len(v) >= 15 and "T" in v:
            dt = datetime.strptime(v[:15], "%Y%m%dT%H%M%S")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    return None


def _is_recent_item(item: dict[str, Any], hours: int = 96) -> bool:
    dt = _parse_published_at(str(item.get("published_at", "")))
    if not dt:
        return False
    now = datetime.now(timezone.utc)
    return dt >= now - timedelta(hours=hours)


def _finance_provider_weight(item: dict[str, Any]) -> float:
    provider = str(item.get("provider", "")).lower()
    text = f"{item.get('title','')} {item.get('summary','')}".lower()

    high_signal = ["reuters", "associated press", "ap ", "bbc", "bloomberg", "financial times", "ft", "wsj", "cnbc", "tagesschau", "dw"]
    low_signal = ["marketbeat", "stockinvest", "tradingview", "finviz", "zacks", "wall street zen"]

    score = 50.0
    if any(s in provider for s in high_signal) or any(s in text for s in high_signal):
        score += 25.0
    if any(s in provider for s in low_signal) or any(s in text for s in low_signal):
        score -= 20.0

    # Source adapter-level tilt
    source_id = str(item.get("source_id", ""))
    if source_id == "finance-finnhub":
        score += 8.0
    elif source_id == "finance-marketaux":
        score += 5.0
    elif source_id == "finance-alpha-vantage":
        score -= 8.0

    return max(0.0, min(100.0, round(score, 2)))


def _finance_interest_tags(item: dict[str, Any]) -> list[str]:
    text = f"{item.get('title','')} {item.get('summary','')}".lower()
    tags: list[str] = []
    rules = {
        "ai_infra": ["ai", "gpu", "nvidia", "datacenter", "cloud"],
        "energy": ["oil", "gas", "energy", "brent", "wti", "fuel"],
        "defense": ["defense", "missile", "airspace", "drone", "military"],
        "semiconductors": ["semiconductor", "chip", "tsmc", "foundry"],
        "gaming_consumer": ["gaming", "steam", "console", "nintendo", "xbox"],
        "macro_rates": ["inflation", "rate", "fed", "ecb", "yield", "treasury"],
    }
    for k, needles in rules.items():
        if any(n in text for n in needles):
            tags.append(k)
    return tags


def _build_finance_summary(items: list[dict[str, Any]], errors: list[dict[str, Any]]) -> dict[str, Any]:
    fin = [i for i in items if str(i.get("category", "")).lower() == "finance"]
    recent = [i for i in fin if _is_recent_item(i, hours=96)]

    driver_counts: dict[str, int] = {}
    theme_counts: dict[str, int] = {}

    for it in recent:
        for m in it.get("markers", []):
            mk = str(m.get("id", "unknown"))
            driver_counts[mk] = driver_counts.get(mk, 0) + 1
        for t in _finance_interest_tags(it):
            theme_counts[t] = theme_counts.get(t, 0) + 1

    top_drivers = sorted(driver_counts.items(), key=lambda x: x[1], reverse=True)[:6]
    top_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)[:6]

    pointers = []
    if theme_counts.get("ai_infra", 0) > 0:
        pointers.append("AI-Infrastruktur/Compute beobachten (Cloud, GPU, Datacenter-Nachfrage)")
    if theme_counts.get("energy", 0) > 0:
        pointers.append("Energie- und Ölpreis-Sensitivität beobachten (Krieg/Transportwege als Treiber)")
    if theme_counts.get("defense", 0) > 0:
        pointers.append("Defense-/Sicherheitssektor als geopolitisch getriebener Faktor im Blick behalten")
    if theme_counts.get("gaming_consumer", 0) > 0:
        pointers.append("Gaming-/Consumer-Tech auf Lieferkette + Preisdruck prüfen")

    ranked = sorted(
        recent,
        key=lambda x: (
            float(x.get("finance_signal_score", 0.0)),
            float(x.get("confidence_score", 0.0)),
            str(x.get("published_at", "")),
        ),
        reverse=True,
    )
    watchlist = []
    for it in ranked:
        if float(it.get("confidence_score", 0.0)) < 55:
            continue
        watchlist.append(
            {
                "title": it.get("title"),
                "source": it.get("source_name"),
                "provider": it.get("provider"),
                "published_at": it.get("published_at"),
                "finance_signal_score": it.get("finance_signal_score", 0.0),
                "confidence": it.get("confidence"),
                "drivers": [m.get("id") for m in it.get("markers", [])],
                "themes": _finance_interest_tags(it),
                "url": it.get("url"),
            }
        )
        if len(watchlist) >= 8:
            break

    return {
        "items_total": len(fin),
        "items_recent_96h": len(recent),
        "confidence": {
            "high": sum(1 for i in recent if i.get("confidence") == "high"),
            "medium": sum(1 for i in recent if i.get("confidence") == "medium"),
            "low": sum(1 for i in recent if i.get("confidence") == "low"),
        },
        "critical_count": sum(
            1 for i in recent if any(m.get("severity") == "critical" for m in i.get("markers", []))
        ),
        "top_drivers": [{"driver": k, "count": v} for k, v in top_drivers],
        "top_themes": [{"theme": k, "count": v} for k, v in top_themes],
        "profile_pointers": pointers[:4],
        "watchlist": watchlist,
        "error_sources": [e.get("source") for e in errors],
    }


def _topic_cluster(item: dict[str, Any]) -> tuple[str | None, list[str]]:
    text = f"{item.get('title','')} {item.get('summary','')}".lower()
    category = str(item.get("category", "")).lower()

    if any(k in text for k in ["iran", "israel", "hormuz", "gulf", "tehran"]):
        return "geo:middle-east-escalation", ["iran", "middle-east", "energy-route"]
    if any(k in text for k in ["ukraine", "russia", "kharkiv", "kyiv"]):
        return "geo:ukraine-war", ["ukraine", "security", "europe"]
    if category == "finance" and any(k in text for k in ["oil", "wti", "brent", "fuel", "energy"]):
        return "macro:energy-price", ["energy", "macro", "inflation"]
    return None, []


def _actionability(item: dict[str, Any]) -> tuple[str, str]:
    conf = float(item.get("confidence_score", 0.0))
    rel = float(item.get("daniel_score", 0.0))
    breaking = bool(item.get("breaking", False))
    cat = str(item.get("category", "")).lower()

    if breaking and conf >= 75:
        return "act", "breaking/high-confidence"
    if cat == "finance" and conf >= 70 and rel >= 33:
        return "watch", "finance-driver-monitoring"
    if rel >= 50 and conf >= 65:
        return "watch", "high-profile-relevance"
    if conf < 50:
        return "ignore", "low-confidence"
    return "watch", "default-monitor"


def run_ingest(max_per_source: int = 20) -> dict:
    _load_local_env()
    sources = load_sources()
    out: list[dict] = []
    errors: list[dict] = []

    # Snapshot previous payload for finance fallback policy
    prev_payload: dict[str, Any] = {}
    if FEED_FILE.exists():
        try:
            prev_payload = json.loads(FEED_FILE.read_text(encoding="utf-8"))
        except Exception:
            prev_payload = {}

    for src in sources:
        if not src.get("enabled", False):
            continue

        try:
            if src.get("type") == "rss":
                url = src.get("url")
                if not url:
                    continue
                d = feedparser.parse(url)
                entries = d.entries[:max_per_source]
                for e in entries:
                    out.append(normalize_rss_entry(src, e))
            elif src.get("type") == "api":
                out.extend(_fetch_api_items(src, max_per_source))
        except Exception as e:
            errors.append({"source": src.get("id"), "error": str(e)})

    raw_count = len(out)

    dedup: dict[str, dict[str, Any]] = {}
    dup_stats: dict[str, dict[str, Any]] = {}
    dedup_removed = 0

    for item in out:
        key = str(item.get("url") or item.get("id"))
        if key in dedup:
            dedup_removed += 1
            s = dup_stats.setdefault(key, {"occurrences": 1, "sources": {str(dedup[key].get("source_id", "unknown"))}})
            s["occurrences"] += 1
            s["sources"].add(str(item.get("source_id", "unknown")))

            current = dedup[key]
            better = item if (float(item.get("score", 0)) > float(current.get("score", 0)) or _is_newer(item, current)) else current
            dedup[key] = better
        else:
            dedup[key] = item

    items = sorted(dedup.values(), key=lambda x: x.get("published_at", ""), reverse=True)

    breaking_count = 0
    for item in items:
        markers: list[dict[str, str]] = []

        if str(item.get("category", "")).lower() == "finance":
            markers.extend(_finance_markers(item))
            markers.extend(_price_markers(item))

        if markers:
            uniq = {(m["id"], m["severity"]): m for m in markers}
            item["markers"] = list(uniq.values())

        d_score, reasons = _daniel_score(item)
        item["daniel_score"] = d_score
        item["daniel_reasons"] = reasons

        c_score, c_label, c_reasons = _item_confidence(item)
        item["confidence_score"] = c_score
        item["confidence"] = c_label
        item["confidence_reasons"] = c_reasons

        if str(item.get("category", "")).lower() == "finance":
            provider_weight = _finance_provider_weight(item)
            item["finance_provider_weight"] = provider_weight
            signal = (0.45 * c_score) + (0.35 * d_score) + (0.20 * provider_weight)
            if any(m.get("severity") == "critical" for m in item.get("markers", [])):
                signal += 8.0
            item["finance_signal_score"] = max(0.0, min(100.0, round(signal, 2)))

        cluster_id, cluster_tags = _topic_cluster(item)
        if cluster_id:
            item["cluster_id"] = cluster_id
            item["cluster_tags"] = cluster_tags

        action, action_reason = _actionability(item)
        item["actionability"] = action
        item["actionability_reason"] = action_reason

        txt = f"{item.get('title','')} {item.get('summary','')}".lower()
        breaking_terms = ["breaking", "urgent", "escalation", "airspace closed", "state of emergency"]
        is_breaking = _is_recent_item(item) and float(item.get("confidence_score", 0.0)) >= 70 and (
            any(m.get("severity") == "critical" for m in item.get("markers", []))
            or any(t in txt for t in breaking_terms)
        )
        if is_breaking:
            item["breaking"] = True
            breaking_count += 1

    # Finance fallback policy: keep digest usable even when finance APIs degrade
    finance_sources = [s for s in sources if s.get("enabled") and str(s.get("category", "")).lower() == "finance"]
    finance_source_ids = [str(s.get("id")) for s in finance_sources]
    finance_errors = [e for e in errors if str(e.get("source")) in finance_source_ids]

    finance_items = [i for i in items if str(i.get("category", "")).lower() == "finance"]
    finance_degraded = bool(finance_errors) or len(finance_items) < max(3, len(finance_source_ids))
    finance_fallback_applied = False

    if finance_degraded and prev_payload.get("items"):
        prev_finance = [
            i for i in prev_payload.get("items", [])
            if str(i.get("category", "")).lower() == "finance"
        ]
        existing_keys = {str(i.get("url") or i.get("id")) for i in items}
        for pi in prev_finance[:12]:
            k = str(pi.get("url") or pi.get("id"))
            if k in existing_keys:
                continue
            clone = dict(pi)
            clone["stale"] = True
            clone["stale_reason"] = "finance_fallback_previous_snapshot"
            items.append(clone)
            existing_keys.add(k)
            finance_fallback_applied = True

        items = sorted(items, key=lambda x: x.get("published_at", ""), reverse=True)

    dedup_examples = []
    for k, v in sorted(dup_stats.items(), key=lambda kv: kv[1]["occurrences"], reverse=True)[:10]:
        dedup_examples.append(
            {
                "key": k,
                "occurrences": int(v["occurrences"]),
                "sources": sorted(v["sources"]),
            }
        )

    cluster_counts: dict[str, int] = {}
    for it in items:
        cid = it.get("cluster_id")
        if cid:
            cluster_counts[str(cid)] = cluster_counts.get(str(cid), 0) + 1
    top_clusters = [
        {"cluster_id": c, "count": n}
        for c, n in sorted(cluster_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    ]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "raw_count": raw_count,
        "count": len(items),
        "unique_count": len(dedup),
        "dedup_removed": dedup_removed,
        "dedup_ratio": round((dedup_removed / raw_count) if raw_count else 0.0, 4),
        "dedup_examples": dedup_examples,
        "breaking_count": breaking_count,
        "items": items,
        "errors": errors,
        "finance_status": {
            "policy": "degraded_keep_last_good",
            "degraded": finance_degraded,
            "configured_sources": finance_source_ids,
            "error_sources": [e.get("source") for e in finance_errors],
            "fresh_items": len(finance_items),
            "fallback_applied": finance_fallback_applied,
        },
        "finance_summary": _build_finance_summary(items, finance_errors),
        "cluster_summary": top_clusters,
    }
    FEED_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


if __name__ == "__main__":
    result = run_ingest()
    print(
        f"ingested: {result['count']} items, errors: {len(result['errors'])}, "
        f"breaking: {result.get('breaking_count',0)}, dedup_removed: {result.get('dedup_removed',0)}"
    )
