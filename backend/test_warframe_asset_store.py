import base64
import hashlib
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from . import warframe_asset_store as assets
except ImportError:
    import warframe_asset_store as assets


URL = "https://warframe.market/static/assets/items/example.png"
KEY = hashlib.sha256(URL.encode("utf-8")).hexdigest()
LOCAL = "/api/warframe/assets/" + KEY
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aM1sAAAAASUVORK5CYII="
)
GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==")
# Minimal signature fixtures: this cache checks MIME/magic, not image decoding.
JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00" + b"\x00" * 10 + b"\xff\xd9"
WEBP = b"RIFF" + (18).to_bytes(4, "little") + b"WEBPVP8L" + b"\x05\x00\x00\x00\x2f\x00\x00\x00\x00\x00"


class Response:
    def __init__(self, body=PNG, mime="image/png", status=200, headers=None):
        self.status_code = status
        self.headers = {"Content-Type": mime, **(headers or {})}
        self.body = body
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]


class WarframeAssetStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.fetch = self.enterContext(patch.object(assets.requests.Session, "get", return_value=Response()))
        self.store = assets.WarframeAssetStore(self.root)
        self.addCleanup(self.store.close)

    def idle(self, store=None):
        store = store or self.store
        deadline = time.monotonic() + 3
        while store._queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.005)
        self.assertEqual(store._queue.unfinished_tasks, 0, "worker did not finish")

    def download(self):
        self.store.warm([URL])
        self.idle()
        result = self.store.get_file(KEY)
        self.assertIsNotNone(result)
        return result

    def test_missing_returns_canonical_url_then_local_after_background_fetch(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        caller = threading.get_ident()

        def fetch(*args, **kwargs):
            self.assertNotEqual(threading.get_ident(), caller)
            self.assertFalse(kwargs["allow_redirects"])
            self.assertTrue(kwargs["stream"])
            entered.set()
            release.wait(2)
            return Response()

        self.fetch.side_effect = fetch
        self.assertEqual(self.store.local_url("items/example.png"), URL)
        self.assertTrue(entered.wait(1))
        self.assertIsNone(self.store.get_file(KEY))
        release.set()
        self.idle()
        path, mime = self.store.get_file(KEY)
        self.assertEqual(path.parent, self.root / "warframe_assets")
        self.assertEqual(path.read_bytes(), PNG)
        self.assertEqual(mime, "image/png")
        self.assertEqual(self.store.local_url(URL), LOCAL)
        self.assertEqual(self.store.local_url(LOCAL), LOCAL)

    def test_restart_uses_disk_without_fetch(self):
        original = self.download()
        self.store.close()
        restarted = assets.WarframeAssetStore(self.root)
        self.addCleanup(restarted.close)
        self.fetch.reset_mock()
        self.assertEqual(restarted.get_file(KEY), original)
        self.assertEqual(restarted.local_url(URL), LOCAL)
        self.fetch.assert_not_called()

    def test_invalid_urls_are_not_returned_or_fetched(self):
        invalid = [
            "", "http://warframe.market/static/assets/a.png", "https://evil.test/a.png",
            "https://warframe.market.evil.test/static/assets/a.png",
            "https://user:password@warframe.market/static/assets/a.png",
            "https://warframe.market:8443/static/assets/a.png", "//evil.test/a.png",
            "https://warframe.market/api/a.png", "../a.png", "items/../a.png",
            "items/%2e%2e/a.png", "items/%252e%252e/a.png", "items/%2fa.png",
            "items\\a.png", "items/a.png?redirect=https://evil.test", "items/a.png#x",
            "items/a.svg", "items/a.html", "items/\na.png", "items/./a.png",
            "https://warframe.market/static/assets//evil.test/a.png",
            "/api/warframe/assets/" + "a" * 64,
        ]
        for value in invalid:
            with self.subTest(value=value):
                self.assertEqual(self.store.local_url(value), "")
        self.store.warm(invalid)
        self.idle()
        self.fetch.assert_not_called()

    def test_invalid_keys_never_resolve(self):
        self.download()
        for key in ["", "../" + KEY, KEY.upper(), KEY + ".png", "a" * 63, "g" * 64, None]:
            with self.subTest(key=key):
                self.assertIsNone(self.store.get_file(key))

    def test_relative_and_absolute_aliases_deduplicate_pending(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def fetch(*args, **kwargs):
            entered.set()
            release.wait(2)
            return Response()

        self.fetch.side_effect = fetch
        self.store.warm([URL])
        self.assertTrue(entered.wait(1))
        for _ in range(20):
            self.store.warm([URL, "items/example.png", "/static/assets/items/example.png",
                             "static/assets/items/example.png", "https://WARFRAME.MARKET:443/static/assets/items/example.png"])
        release.set()
        self.idle()
        self.assertEqual(self.fetch.call_count, 1)
        self.assertEqual(self.store.local_url(URL), LOCAL)

    def test_accepts_only_matching_raster_mime_and_magic(self):
        for i, (body, mime) in enumerate([(PNG, "image/png"), (JPEG, "image/jpeg"),
                                         (WEBP, "image/webp"), (GIF, "image/gif")]):
            with self.subTest(mime=mime):
                url = URL.replace("example", str(i))
                self.fetch.return_value = Response(body, mime + "; charset=binary")
                self.store.warm([url])
                self.idle()
                path, actual = self.store.get_file(hashlib.sha256(url.encode()).hexdigest())
                self.assertEqual(actual, mime)
                self.assertEqual(path.read_bytes(), body)

    def test_invalid_content_and_oversize_are_not_persisted(self):
        responses = [Response(b"<html>bad</html>"), Response(b"<svg/>", "image/svg+xml"),
                     Response(PNG, "text/html"), Response(PNG, "image/jpeg"), Response(b""),
                     Response(b"\x89PNG\r\n\x1a\n"), Response(PNG, "application/octet-stream"),
                     Response(PNG, headers={"Content-Length": "5242881"}),
                     Response(PNG, headers={"Content-Length": "garbage"}),
                     Response(PNG + b"x" * 5242880), Response(PNG, status=404),
                     Response(PNG, status=206)]
        for i, response in enumerate(responses):
            with self.subTest(i=i):
                self.fetch.return_value = response
                url = URL.replace("example", "bad" + str(i))
                self.store.warm([url])
                self.idle()
                self.assertIsNone(self.store.get_file(hashlib.sha256(url.encode()).hexdigest()))
                self.assertTrue(response.closed)
        self.assertEqual(list((self.root / "warframe_assets").iterdir()), [])

    def test_exact_size_limit_is_accepted(self):
        body = JPEG[:-2] + b"\x00" * (5242880 - len(JPEG)) + JPEG[-2:]
        self.fetch.return_value = Response(body, "image/jpeg", headers={"Content-Length": "5242880"})
        path, mime = self.download()
        self.assertEqual(path.stat().st_size, 5242880)
        self.assertEqual(mime, "image/jpeg")

    def test_interrupted_stream_never_publishes_partial_file(self):
        class InterruptedResponse(Response):
            def iter_content(self, chunk_size):
                yield PNG[:32]
                raise assets.requests.ConnectionError("interrupted stream")

        self.fetch.return_value = InterruptedResponse()
        self.store.warm([URL])
        self.idle()
        self.assertIsNone(self.store.get_file(KEY))
        self.assertEqual(list((self.root / "warframe_assets").iterdir()), [])

    def test_completed_file_is_not_visible_before_atomic_rename(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        replace = assets.os.replace

        def delayed_replace(source, destination):
            entered.set()
            release.wait(2)
            replace(source, destination)

        with patch.object(assets.os, "replace", side_effect=delayed_replace):
            self.store.warm([URL])
            self.assertTrue(entered.wait(1))
            self.assertIsNone(self.store.get_file(KEY))
            release.set()
            self.idle()
        self.assertEqual(self.store.get_file(KEY)[0].read_bytes(), PNG)

    def test_concurrent_callers_share_one_download(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def fetch(*args, **kwargs):
            entered.set()
            release.wait(2)
            return Response()

        self.fetch.side_effect = fetch
        threads = [threading.Thread(target=self.store.warm, args=([URL] * 10,)) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        self.assertTrue(entered.wait(1))
        release.set()
        self.idle()
        self.assertEqual(self.fetch.call_count, 1)
        self.assertEqual(self.store.local_url(URL), LOCAL)

    def test_redirect_outside_whitelist_is_never_requested(self):
        for i, target in enumerate(["https://evil.test/a.png", "http://warframe.market/static/assets/a.png",
                                    "/api/a.png", "/static/assets/../a.png"]):
            self.fetch.return_value = Response(status=302, headers={"Location": target})
            self.store.warm([URL.replace("example", "redirect" + str(i))])
            self.idle()
        self.assertEqual(self.fetch.call_count, 4)
        self.assertEqual(list((self.root / "warframe_assets").iterdir()), [])

    def test_trusted_redirect_is_checked_and_keeps_original_key(self):
        self.fetch.side_effect = [Response(status=302, headers={"Location": "/static/assets/items/other.png"}), Response()]
        self.download()
        self.assertEqual(self.fetch.call_args_list[1].args[0], URL.replace("example", "other"))

    def test_redirect_loop_is_bounded(self):
        self.fetch.return_value = Response(status=302, headers={"Location": URL})
        self.store.warm([URL])
        self.idle()
        self.assertLessEqual(self.fetch.call_count, 5)
        self.assertIsNone(self.store.get_file(KEY))

    def test_failure_cooldown_then_retry(self):
        self.fetch.side_effect = assets.requests.ConnectionError("offline")
        with patch.object(assets.time, "monotonic", return_value=100.0):
            self.store.warm([URL])
            self.store._queue.join()
            self.assertEqual(self.store.local_url(URL), URL)
            self.assertEqual(self.fetch.call_count, 1)
        self.fetch.side_effect = None
        with patch.object(assets.time, "monotonic", return_value=10000.0):
            self.store.warm([URL])
            self.store._queue.join()
        self.assertEqual(self.store.local_url(URL), LOCAL)

    def test_failed_download_and_failed_atomic_write_preserve_existing_asset(self):
        path, _ = self.download()
        self.fetch.side_effect = assets.requests.ConnectionError("offline")
        self.store.warm([URL, URL.replace("example", "offline")])
        self.idle()
        self.assertEqual(path.read_bytes(), PNG)
        self.fetch.side_effect = None
        with patch.object(assets.os, "replace", side_effect=OSError("disk full")):
            self.store.warm([URL.replace("example", "diskfull")])
            self.idle()
        self.assertEqual(self.store.get_file(KEY), (path, "image/png"))
        self.assertEqual(list(path.parent.iterdir()), [path])

    def test_corrupt_disk_content_is_not_served(self):
        path, _ = self.download()
        path.write_bytes(b"<html>corrupt</html>")
        self.assertIsNone(self.store.get_file(KEY))
        self.store.close()
        restarted = assets.WarframeAssetStore(self.root)
        self.addCleanup(restarted.close)
        self.assertIsNone(restarted.get_file(KEY))

    def test_wrong_extension_and_oversize_disk_files_are_not_served(self):
        path, _ = self.download()
        path.write_bytes(GIF)
        self.assertIsNone(self.store.get_file(KEY))
        path.write_bytes(PNG + b"x" * 5242880)
        self.assertIsNone(self.store.get_file(KEY))

    def test_symlink_is_not_served(self):
        path, _ = self.download()
        outside = self.root / "outside.png"
        path.rename(outside)
        try:
            path.symlink_to(outside)
        except OSError:
            self.skipTest("symlinks not permitted on this host")
        self.assertIsNone(self.store.get_file(KEY))

    def test_queue_is_bounded_and_dropped_work_can_be_retried(self):
        self.store.close()
        with patch.object(assets, "_QUEUE_SIZE", 2):
            store = assets.WarframeAssetStore(self.root)
        self.addCleanup(store.close)
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def fetch(*args, **kwargs):
            entered.set()
            release.wait(2)
            return Response()

        self.fetch.side_effect = fetch
        store.warm([URL])
        self.assertTrue(entered.wait(1))
        urls = [URL.replace("example", str(i)) for i in range(20)]
        store.warm(urls)
        self.assertEqual(store._queue.qsize(), 2)
        release.set()
        self.idle(store)
        self.assertEqual(self.fetch.call_count, 3)
        store.warm([urls[-1]])
        self.idle(store)
        self.assertTrue(store.local_url(urls[-1]).startswith("/api/warframe/assets/"))

    def test_close_is_idempotent_and_does_not_schedule_more_work(self):
        path, mime = self.download()
        self.store.close()
        self.store.close()
        self.fetch.reset_mock()
        self.assertEqual(self.store.get_file(KEY), (path, mime))
        self.assertEqual(self.store.local_url(URL.replace("example", "closed")), URL.replace("example", "closed"))
        self.store.warm([URL.replace("example", "closed")])
        self.fetch.assert_not_called()

    def test_close_discards_queue_and_prevents_inflight_publication(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def fetch(*args, **kwargs):
            entered.set()
            release.wait(2)
            return Response()

        self.fetch.side_effect = fetch
        self.store.warm([URL])
        self.assertTrue(entered.wait(1))
        self.store.warm([URL.replace("example", "queued")])
        closer = threading.Thread(target=self.store.close)
        closer.start()
        self.assertTrue(self.store._stopped.wait(1))
        release.set()
        closer.join(2)
        self.assertFalse(closer.is_alive())
        self.assertFalse(self.store._worker.is_alive())
        self.assertEqual(self.store._queue.unfinished_tasks, 0)
        self.assertIsNone(self.store.get_file(KEY))
        self.assertEqual(list((self.root / "warframe_assets").iterdir()), [])
        self.assertEqual(self.fetch.call_count, 1)


if __name__ == "__main__":
    unittest.main()
