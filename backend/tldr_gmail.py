from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

_NEWSLETTER_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("ai", "TLDR AI", re.compile(r"\btldr\s+ai\b", re.I)),
    ("founders", "TLDR Founders", re.compile(r"\btldr\s+founders?\b", re.I)),
    ("marketing", "TLDR Marketing", re.compile(r"\btldr\s+marketing\b", re.I)),
    ("infosec", "TLDR InfoSec", re.compile(r"\btldr\s+(?:infosec|security)\b", re.I)),
    ("crypto", "TLDR Crypto", re.compile(r"\btldr\s+crypto\b", re.I)),
    ("hardware", "TLDR Hardware", re.compile(r"\btldr\s+hardware\b", re.I)),
    ("dev", "TLDR Dev", re.compile(r"\btldr\s+(?:web\s*dev|dev(?:elopment)?|programming)\b", re.I)),
    ("tech", "TLDR Tech", re.compile(r"\btldr(?:\s+tech)?\b", re.I)),
]


def _load_google_auth_modules():
    try:
        from google.auth.transport.requests import Request as GoogleRequest
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import Flow
    except Exception as exc:
        raise RuntimeError(
            "Google auth dependencies missing. Install google-auth and google-auth-oauthlib."
        ) from exc
    return GoogleRequest, Credentials, Flow


def _resolve_client_config(
    client_secret_file: Path,
    redirect_uri: str,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict[str, Any]:
    if client_id and client_secret:
        return {
            "web": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [redirect_uri],
            }
        }
    if client_secret_file.exists():
        return json.loads(client_secret_file.read_text(encoding="utf-8"))
    raise RuntimeError(
        f"Gmail OAuth client config missing. Expected {client_secret_file} or GOOGLE_OAUTH_CLIENT_ID/SECRET."
    )


def _build_flow(
    client_secret_file: Path,
    redirect_uri: str,
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
):
    _, _, Flow = _load_google_auth_modules()
    config = _resolve_client_config(
        client_secret_file,
        redirect_uri,
        client_id=client_id,
        client_secret=client_secret,
    )
    flow = Flow.from_client_config(config, scopes=GMAIL_SCOPES)
    flow.redirect_uri = redirect_uri
    return flow


def build_gmail_auth_url(
    client_secret_file: Path,
    redirect_uri: str,
    *,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> tuple[str, str]:
    flow = _build_flow(
        client_secret_file,
        redirect_uri,
        client_id=client_id,
        client_secret=client_secret,
    )
    auth_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent select_account",
    )
    return auth_url, state


def exchange_gmail_auth_code(
    client_secret_file: Path,
    redirect_uri: str,
    *,
    authorization_response: str,
    state: str,
    token_file: Path,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> dict[str, Any]:
    flow = _build_flow(
        client_secret_file,
        redirect_uri,
        client_id=client_id,
        client_secret=client_secret,
    )
    flow.oauth2session.state = state
    flow.fetch_token(authorization_response=authorization_response)
    creds = flow.credentials
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text(creds.to_json(), encoding="utf-8")
    return {"saved": True, "scopes": list(creds.scopes or [])}


def load_gmail_credentials(token_file: Path):
    GoogleRequest, Credentials, _ = _load_google_auth_modules()
    if not token_file.exists():
        raise RuntimeError("No Gmail token found yet.")
    creds = Credentials.from_authorized_user_file(str(token_file), GMAIL_SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(GoogleRequest())
            token_file.write_text(creds.to_json(), encoding="utf-8")
        else:
            raise RuntimeError("Gmail token expired and cannot be refreshed.")
    return creds


def gmail_api_get(token_file: Path, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    creds = load_gmail_credentials(token_file)
    response = requests.get(
        f"{GMAIL_API_BASE}/{path.lstrip('/')}",
        params=params or {},
        headers={"Authorization": f"Bearer {creds.token}"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def gmail_profile(token_file: Path) -> dict[str, Any]:
    return gmail_api_get(token_file, "profile")


def _decode_gmail_body(data: str | None) -> str:
    if not data:
        return ""
    raw = data.encode("utf-8")
    padding = b"=" * ((4 - len(raw) % 4) % 4)
    decoded = base64.urlsafe_b64decode(raw + padding)
    return decoded.decode("utf-8", errors="ignore")


def _walk_parts(part: dict[str, Any]) -> list[dict[str, Any]]:
    parts = [part]
    for child in part.get("parts", []) or []:
        if isinstance(child, dict):
            parts.extend(_walk_parts(child))
    return parts


def _extract_message_bodies(payload: dict[str, Any]) -> tuple[str, str]:
    html = ""
    text = ""
    for part in _walk_parts(payload):
        mime = str(part.get("mimeType") or "").lower()
        body = part.get("body") or {}
        data = _decode_gmail_body(body.get("data"))
        if not data:
            continue
        if mime == "text/html" and len(data) > len(html):
            html = data
        elif mime == "text/plain" and len(data) > len(text):
            text = data
    if not html and payload.get("body", {}).get("data"):
        html = _decode_gmail_body(payload.get("body", {}).get("data"))
    return html, text


def _headers_map(payload: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for header in payload.get("headers", []) or []:
        if not isinstance(header, dict):
            continue
        name = str(header.get("name") or "").strip().lower()
        value = str(header.get("value") or "").strip()
        if name and value:
            out[name] = value
    return out


def _detect_newsletter(subject: str, html: str, text: str) -> tuple[str, str]:
    corpus = f"{subject}\n{text[:1000]}\n{html[:2000]}"
    for slug, label, pattern in _NEWSLETTER_PATTERNS:
        if pattern.search(corpus):
            return slug, label
    return "tech", "TLDR Tech"


def _extract_issue_items(soup: BeautifulSoup, max_items: int = 40) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "").strip()
        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not href.startswith("http"):
            continue
        if href in seen:
            continue
        if not title or len(title) < 12:
            continue
        if any(blocked in href.lower() for blocked in ["unsubscribe", "mailto:", "/settings", "/newsletters"]):
            continue
        seen.add(href)
        items.append({"title": title[:240], "url": href})
        if len(items) >= max_items:
            break
    return items


def _extract_sections(soup: BeautifulSoup, max_sections: int = 18) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for node in soup.find_all(["h1", "h2", "h3", "strong"]):
        title = " ".join(node.get_text(" ", strip=True).split())
        if len(title) < 3:
            continue
        if title.lower() in {"unsubscribe", "subscribe", "read more"}:
            continue
        if any(existing["title"].lower() == title.lower() for existing in sections):
            continue
        sections.append({"title": title[:180]})
        if len(sections) >= max_sections:
            break
    return sections


def _clean_reader_html(html: str) -> tuple[str, str, list[dict[str, Any]], list[dict[str, Any]]]:
    if not html:
        plain = ""
        reader_html = (
            "<!doctype html><html><head><meta charset='utf-8'></head>"
            "<body><p>No HTML body found for this issue.</p></body></html>"
        )
        return reader_html, plain, [], []

    soup = BeautifulSoup(html, "html.parser")
    for tag_name in ["script", "iframe", "object", "embed", "form", "input", "button", "meta", "link"]:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for image in soup.find_all("img"):
        width = str(image.get("width") or "").strip()
        height = str(image.get("height") or "").strip()
        if width in {"1", "0"} or height in {"1", "0"}:
            image.decompose()
            continue
        image["loading"] = "lazy"
        image["style"] = "max-width:100%;height:auto;"

    body = soup.body or soup
    sections = _extract_sections(body)
    items = _extract_issue_items(body)
    plain = " ".join(body.get_text("\n", strip=True).split())
    reader_html = (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<base target='_blank'>"
        "<style>"
        "body{margin:0;padding:24px;background:#ffffff;color:#111827;font:16px/1.55 Arial,sans-serif;}"
        "img{max-width:100%;height:auto;}a{color:#0f62fe;}table{max-width:100%;}"
        "</style></head><body>"
        f"{str(body)}"
        "</body></html>"
    )
    return reader_html, plain, sections, items


def _to_iso_millis(epoch_ms: Any) -> str:
    try:
        return datetime.fromtimestamp(float(epoch_ms) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _issue_id_for(gmail_id: str, newsletter_slug: str) -> str:
    return f"{newsletter_slug}:{gmail_id}"


def _message_to_issue(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("payload") or {}
    headers = _headers_map(payload)
    subject = headers.get("subject", "TLDR Issue")
    sender = headers.get("from", "")
    html_body, text_body = _extract_message_bodies(payload)
    newsletter_slug, newsletter_name = _detect_newsletter(subject, html_body, text_body)
    reader_html, plain_text, sections, items = _clean_reader_html(html_body or "")
    label_ids = {str(label).upper() for label in message.get("labelIds", []) or []}
    received_at = _to_iso_millis(message.get("internalDate"))
    return {
        "issue_id": _issue_id_for(str(message.get("id") or ""), newsletter_slug),
        "gmail_id": str(message.get("id") or ""),
        "gmail_thread_id": str(message.get("threadId") or ""),
        "message_id": headers.get("message-id", ""),
        "newsletter_slug": newsletter_slug,
        "newsletter_name": newsletter_name,
        "subject": subject,
        "sender": sender,
        "received_at": received_at,
        "snippet": str(message.get("snippet") or "")[:500],
        "html_content": html_body,
        "reader_html": reader_html,
        "text_content": text_body or plain_text,
        "sections_json": json.dumps(sections, ensure_ascii=False),
        "items_json": json.dumps(items, ensure_ascii=False),
        "unread": 1 if "UNREAD" in label_ids else 0,
        "starred": 1 if "STARRED" in label_ids else 0,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_tldr_messages(
    token_file: Path,
    *,
    query: str = 'subject:TLDR newer_than:180d',
    max_results: int = 25,
) -> list[dict[str, Any]]:
    listing = gmail_api_get(
        token_file,
        "messages",
        params={
            "q": query,
            "maxResults": max(1, min(int(max_results or 25), 100)),
        },
    )
    messages = listing.get("messages", []) or []
    issues: list[dict[str, Any]] = []
    for meta in messages:
        msg_id = str((meta or {}).get("id") or "").strip()
        if not msg_id:
            continue
        raw_message = gmail_api_get(token_file, f"messages/{msg_id}", params={"format": "full"})
        issues.append(_message_to_issue(raw_message))
    return issues
