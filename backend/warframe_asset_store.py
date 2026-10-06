"""Persistent, asynchronous cache for Warframe Market's static raster artwork.

Pass the application's mounted data directory, not the assets subdirectory.
``local_url`` returns an empty string for untrusted input, a canonical remote
URL while a fetch is pending, or a local API URL after publication. Serve the
path/MIME returned by ``get_file`` and call ``close`` at application shutdown.

Disk layout is warframe_assets/<sha256(canonical URL)>.<png|jpg|webp|gif>.
The extension is the persisted MIME metadata; no index or database is needed.
Only complete files are published. Back up this directory with application data.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import re
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests


_BASE = "https://warframe.market/static/assets/"
_LOCAL = "/api/warframe/assets/"
_MAX_BYTES = 5 * 1024 * 1024
_QUEUE_SIZE = 128
_FAILURE_LIMIT = 1024
_FAILURE_COOLDOWN = 300.0
_DOWNLOAD_SECONDS = 30.0
_MIMES = {".png": "image/png", ".jpg": "image/jpeg", ".webp": "image/webp", ".gif": "image/gif"}
_KEY = re.compile(r"[0-9a-f]{64}\Z")
_SEGMENT = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*\Z")
_LOG = logging.getLogger(__name__)


def _canonical_url(value: str) -> str | None:
    # Reject encoded separators/dot segments rather than relying on a server's
    # (possibly repeated) percent decoding. WFM artwork names need no escapes.
    if not isinstance(value, str) or not value or len(value) > 2048:
        return None
    if re.search(r"[\x00-\x20\x7f\\%?#]", value):
        return None
    try:
        parts = urlsplit(value)
    except ValueError:
        return None
    if parts.scheme or parts.netloc:
        if parts.scheme != "https" or parts.netloc.lower() not in {"warframe.market", "warframe.market:443"}:
            return None
        if not parts.path.startswith("/static/assets/"):
            return None
        path = parts.path[len("/static/assets/"):]
    elif value.startswith("/static/assets/"):
        path = value[len("/static/assets/"):]
    elif value.startswith("static/assets/"):
        path = value[len("static/assets/"):]
    else:
        path = value
    if not all(_SEGMENT.fullmatch(segment) for segment in path.split("/")):
        return None
    if Path(path).suffix.lower() not in {*_MIMES, ".jpeg"}:
        return None
    return _BASE + path


def _image_mime(head: bytes, tail: bytes, size: int) -> str | None:
    """Check raster signatures and minimal framing, without an image decoder."""
    if not 0 < size <= _MAX_BYTES:
        return None
    if (size >= 45 and head.startswith(b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR")
            and int.from_bytes(head[16:20], "big") > 0
            and int.from_bytes(head[20:24], "big") > 0
            and tail.endswith(b"\x00\x00\x00\x00IEND\xaeB`\x82")):
        return "image/png"
    if size >= 14 and head.startswith(b"\xff\xd8\xff") and tail.endswith(b"\xff\xd9"):
        return "image/jpeg"
    if (size >= 14 and head[:6] in {b"GIF87a", b"GIF89a"}
            and int.from_bytes(head[6:8], "little") > 0
            and int.from_bytes(head[8:10], "little") > 0 and tail.endswith(b";")):
        return "image/gif"
    if (size >= 26 and head[:4] == b"RIFF" and head[8:12] == b"WEBP"
            and head[12:16] in {b"VP8 ", b"VP8L", b"VP8X"}
            and int.from_bytes(head[4:8], "little") + 8 == size):
        return "image/webp"
    return None


class WarframeAssetStore:
    def __init__(self, data_dir: Path):
        self._directory = Path(data_dir) / "warframe_assets"
        self._directory.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue[tuple[str, str]] = queue.Queue(maxsize=_QUEUE_SIZE)
        self._pending: set[str] = set()
        self._failures: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._worker = threading.Thread(target=self._run, name="warframe-assets", daemon=True)
        self._worker.start()

    def local_url(self, value: str) -> str:
        """Read disk and enqueue a miss without doing or waiting on network I/O."""
        if isinstance(value, str) and value.startswith(_LOCAL):
            return value if self.get_file(value[len(_LOCAL):]) else ""
        url = _canonical_url(value)
        if url is None:
            return ""
        key = hashlib.sha256(url.encode("utf-8")).hexdigest()
        if self.get_file(key) is not None:
            return _LOCAL + key
        with self._lock:
            if self._stopped.is_set() or key in self._pending:
                return url
            if self._failures.get(key, 0) > time.monotonic():
                return url
            try:
                self._queue.put_nowait((key, url))
            except queue.Full:
                return url
            self._pending.add(key)
        return url

    def get_file(self, key: str) -> tuple[Path, str] | None:
        """Resolve a completed raster locally; never enqueue or fetch anything."""
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            return None
        for suffix, mime in _MIMES.items():
            path = self._directory / (key + suffix)
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                with path.open("rb") as stream:
                    size = os.fstat(stream.fileno()).st_size
                    if not 0 < size <= _MAX_BYTES:
                        continue
                    head = stream.read(32)
                    stream.seek(max(0, size - 12))
                    tail = stream.read(12)
                if _image_mime(head, tail, size) == mime:
                    return path, mime
            except OSError:
                continue
        return None

    def warm(self, values: list[str]) -> None:
        for value in values:
            self.local_url(value)

    def close(self) -> None:
        """Stop scheduling, discard queued misses, and stop the worker.

        An in-flight request has a bounded read timeout. Shutdown waits at most
        15 seconds; a still-running daemon cannot publish after close returns.
        Already cached files remain readable after close.
        """
        with self._lock:
            self._stopped.set()
            while True:
                try:
                    key, _ = self._queue.get_nowait()
                except queue.Empty:
                    break
                self._pending.discard(key)
                self._queue.task_done()
        self._worker.join(timeout=15)

    def _run(self) -> None:
        with requests.Session() as session:
            # No implicit proxy routing, .netrc credentials, or environment auth.
            session.trust_env = False
            while not self._stopped.is_set():
                try:
                    key, url = self._queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    if not self._stopped.is_set() and self.get_file(key) is None:
                        body, mime = self._download(session, url)
                        self._publish(key, body, mime)
                    with self._lock:
                        self._failures.pop(key, None)
                except Exception as exc:
                    # One bad image or unavailable disk must not kill the worker.
                    with self._lock:
                        self._failures[key] = time.monotonic() + _FAILURE_COOLDOWN
                        self._failures.move_to_end(key)
                        while len(self._failures) > _FAILURE_LIMIT:
                            self._failures.popitem(last=False)
                    _LOG.debug("Warframe asset %s unavailable: %s", key, exc)
                finally:
                    with self._lock:
                        self._pending.discard(key)
                    self._queue.task_done()

    def _download(self, session: requests.Session, url: str) -> tuple[bytes, str]:
        deadline = time.monotonic() + _DOWNLOAD_SECONDS
        for _ in range(4):
            if self._stopped.is_set() or time.monotonic() > deadline:
                raise ValueError("Download cancelled or timed out")
            with session.get(url, stream=True, allow_redirects=False, timeout=(3, 10)) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location", "")
                    # Validate before urljoin, which otherwise hides dot traversal.
                    if _canonical_url(location) is None:
                        raise ValueError("Untrusted asset redirect")
                    next_url = _canonical_url(urljoin(url, location))
                    if next_url is None:
                        raise ValueError("Untrusted asset redirect")
                    url = next_url
                    continue
                if response.status_code != 200:
                    raise ValueError("Unexpected asset status")
                mime = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                if mime not in _MIMES.values():
                    raise ValueError("Unsupported asset MIME type")
                length = response.headers.get("Content-Length")
                if length is not None and not 0 < int(length) <= _MAX_BYTES:
                    raise ValueError("Invalid asset size")
                body = bytearray()
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if self._stopped.is_set() or time.monotonic() > deadline:
                        raise ValueError("Download cancelled or timed out")
                    if len(body) + len(chunk) > _MAX_BYTES:
                        raise ValueError("Asset exceeds size limit")
                    body.extend(chunk)
                if _image_mime(bytes(body[:32]), bytes(body[-12:]), len(body)) != mime:
                    raise ValueError("Asset MIME/magic mismatch")
                return bytes(body), mime
        raise ValueError("Too many asset redirects")

    def _publish(self, key: str, body: bytes, mime: str) -> None:
        suffix = next(ext for ext, value in _MIMES.items() if value == mime)
        destination = self._directory / (key + suffix)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._directory, prefix=".pending-", suffix=".tmp", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(body)
                stream.flush()
                os.fsync(stream.fileno())
            # A single rename commits bytes and MIME metadata together. Never
            # overwrite a valid cached image, including one another worker wrote.
            with self._lock:
                if not self._stopped.is_set() and self.get_file(key) is None:
                    os.replace(temporary, destination)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
