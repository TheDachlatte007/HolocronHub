from __future__ import annotations

import imaplib
import json
import re
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from typing import Any

from bs4 import BeautifulSoup


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


def _decode_bytes(payload: bytes | None, charset: str | None = None) -> str:
    if not payload:
        return ""
    for candidate in [charset, "utf-8", "latin-1", "cp1252"]:
        if not candidate:
            continue
        try:
            return payload.decode(candidate, errors="ignore")
        except Exception:
            continue
    return payload.decode("utf-8", errors="ignore")


def _extract_message_bodies(message) -> tuple[str, str]:
    html = ""
    text = ""
    if message.is_multipart():
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            disposition = str(part.get("Content-Disposition") or "").lower()
            if "attachment" in disposition:
                continue
            mime = str(part.get_content_type() or "").lower()
            payload = part.get_payload(decode=True)
            decoded = _decode_bytes(payload, part.get_content_charset())
            if mime == "text/html" and len(decoded) > len(html):
                html = decoded
            elif mime == "text/plain" and len(decoded) > len(text):
                text = decoded
    else:
        mime = str(message.get_content_type() or "").lower()
        payload = message.get_payload(decode=True)
        decoded = _decode_bytes(payload, message.get_content_charset())
        if mime == "text/html":
            html = decoded
        else:
            text = decoded
    return html, text


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


def _coerce_received_at(message) -> str:
    raw_date = str(message.get("Date") or "").strip()
    if raw_date:
        try:
            parsed = parsedate_to_datetime(raw_date)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


def _message_to_issue(uid: str, message, flags_blob: str) -> dict[str, Any]:
    subject = str(message.get("Subject") or "TLDR Issue").strip()
    sender = str(message.get("From") or "").strip()
    html_body, text_body = _extract_message_bodies(message)
    newsletter_slug, newsletter_name = _detect_newsletter(subject, html_body, text_body)
    reader_html, plain_text, sections, items = _clean_reader_html(html_body or "")
    snippet_source = text_body or plain_text or BeautifulSoup(html_body or "", "html.parser").get_text(" ", strip=True)
    snippet = " ".join(str(snippet_source or "").split())[:500]
    flags = str(flags_blob or "").upper()
    received_at = _coerce_received_at(message)
    return {
        "issue_id": f"{newsletter_slug}:{uid}",
        "gmail_id": uid,
        "gmail_thread_id": "",
        "message_id": str(message.get("Message-ID") or "").strip(),
        "newsletter_slug": newsletter_slug,
        "newsletter_name": newsletter_name,
        "subject": subject,
        "sender": sender,
        "received_at": received_at,
        "snippet": snippet,
        "html_content": html_body,
        "reader_html": reader_html,
        "text_content": text_body or plain_text,
        "sections_json": json.dumps(sections, ensure_ascii=False),
        "items_json": json.dumps(items, ensure_ascii=False),
        "unread": 0 if "\\SEEN" in flags else 1,
        "starred": 1 if "\\FLAGGED" in flags else 0,
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_tldr_messages_imap(
    email_address: str,
    app_password: str,
    *,
    imap_host: str = "imap.gmail.com",
    imap_port: int = 993,
    mailbox: str = "INBOX",
    query: str = "TLDR",
    max_results: int = 25,
) -> list[dict[str, Any]]:
    host = str(imap_host or "imap.gmail.com").strip()
    port = int(imap_port or 993)
    folder = str(mailbox or "INBOX").strip() or "INBOX"
    search_subject = str(query or "TLDR").strip() or "TLDR"
    password = str(app_password or "").replace(" ", "").strip()
    if not email_address or not password:
        raise RuntimeError("Gmail address and app password are required.")

    conn = imaplib.IMAP4_SSL(host, port)
    try:
        conn.login(str(email_address).strip(), password)
        status, _ = conn.select(folder, readonly=True)
        if status != "OK":
            raise RuntimeError(f"Could not open mailbox `{folder}`.")
        status, data = conn.uid("search", None, "SUBJECT", f'"{search_subject}"')
        if status != "OK":
            raise RuntimeError("IMAP search failed.")
        uids = [uid for uid in (data[0] or b"").split() if uid]
        if not uids:
            return []
        selected = list(reversed(uids[-max(1, min(int(max_results or 25), 100)):]))
        issues: list[dict[str, Any]] = []
        for uid in selected:
            status, parts = conn.uid("fetch", uid, "(BODY.PEEK[] FLAGS)")
            if status != "OK":
                continue
            raw_message = b""
            flags_blob = ""
            for part in parts or []:
                if isinstance(part, tuple):
                    if isinstance(part[0], bytes):
                        flags_blob += part[0].decode("utf-8", errors="ignore")
                    else:
                        flags_blob += str(part[0])
                    if isinstance(part[1], (bytes, bytearray)):
                        raw_message += bytes(part[1])
            if not raw_message:
                continue
            message = BytesParser(policy=policy.default).parsebytes(raw_message)
            issues.append(_message_to_issue(uid.decode("utf-8", errors="ignore"), message, flags_blob))
        return issues
    except imaplib.IMAP4.error as exc:
        raise RuntimeError(f"Gmail IMAP login failed: {exc}") from exc
    finally:
        try:
            conn.logout()
        except Exception:
            pass
