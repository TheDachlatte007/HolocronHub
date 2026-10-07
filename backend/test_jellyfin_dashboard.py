"""Isolated Jellyfin contracts: no live server or application startup needed."""
import importlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
USER = "a" * 32
ITEM = "b" * 32
KEY = "fixture-private-key-do-not-leak"
API = "/api/dashboard/jellyfin"


def resume_item(item_id=ITEM, **extra):
    return {"Id": item_id, "Name": "The Arrival", "Type": "Episode",
            "SeriesName": "Example Series", "ParentIndexNumber": 2, "IndexNumber": 3,
            "RunTimeTicks": 36000000000, "UserData": {"PlaybackPositionTicks": 9000000000},
            "ImageTags": {"Primary": "image-tag"}, "ServerId": "server-id",
            "Path": "/private/library/file.mkv", **extra}


def remote(payload=None, status=200, content_type="application/json", body=None):
    response = requests.Response()
    response.status_code = status
    response.headers["Content-Type"] = content_type
    response._content = body if body is not None else json.dumps(payload).encode()
    response._content_consumed = True
    return response


class JellyfinDashboardTests(unittest.TestCase):
    def setUp(self):
        self.module = importlib.import_module("backend.jellyfin_dashboard")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.cache = Path(self.temp.name) / "jellyfin.json"
        self.config = {"jellyfin_enabled": True, "jellyfin_url": "http://jellyfin.test:8096/base/",
                       "jellyfin_api_key": KEY, "jellyfin_user_id": USER}
        self.client = self.make_client()
        self.clock = 1800000000.0
        self.clock_patch = patch.object(self.module.time, "time", side_effect=lambda: self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.network = patch.object(self.module.requests, "get", return_value=remote({"Items": [resume_item()]})).start()
        self.addCleanup(patch.stopall)

    def make_client(self, getter=None, cache=None):
        app = FastAPI()
        app.include_router(self.module.create_jellyfin_router(getter or (lambda: dict(self.config)), cache or self.cache))
        client = TestClient(app)
        self.addCleanup(client.close)
        return client

    def get(self, force=False, client=None):
        response = (client or self.client).get(API, params={"force": str(force).lower()})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn(KEY, response.text)
        return response.json()

    def test_disabled_and_incomplete_make_no_calls_even_when_forced(self):
        for config, state in [({}, "disabled"), ({"jellyfin_enabled": "false"}, "disabled"),
                              ({"jellyfin_enabled": True}, "unconfigured")]:
            with self.subTest(state=state):
                self.config = config
                result = self.get(True)
                self.assertEqual(result["state"], state)
                self.assertEqual(result["items"], [])
                self.assertEqual(result["settings_url"], "#settings")
                self.assertTrue(result["message"])
        self.network.assert_not_called()

    def test_invalid_configuration_is_not_sent_or_reflected(self):
        for field, value in [("jellyfin_url", "https://user:password@jellyfin.test"),
                             ("jellyfin_url", f"http://jellyfin.test/?api_key={KEY}"),
                             ("jellyfin_url", "javascript:alert(1)"),
                             ("jellyfin_url", f"http://jellyfin.test/{KEY}"),
                             ("jellyfin_url", "http://jellyfin.test/base/%2e%2e/else"),
                             ("jellyfin_user_id", "../../admin"),
                             ("jellyfin_api_key", "bad\r\nheader")]:
            with self.subTest(field=field, value=value):
                old = self.config[field]
                self.config[field] = value
                self.assertEqual(self.get(True)["state"], "unconfigured")
                self.config[field] = old
        self.network.assert_not_called()

    def test_resume_request_uses_header_auth_get_and_bounded_options(self):
        result = self.get()
        self.assertEqual(result["state"], "ready")
        url = self.network.call_args.args[0]
        options = self.network.call_args.kwargs
        self.assertEqual(url, f"http://jellyfin.test:8096/base/Users/{USER}/Items/Resume")
        self.assertNotIn(KEY, url + json.dumps(options["params"]))
        self.assertEqual(options["headers"]["X-Emby-Token"], KEY)
        self.assertFalse(options["allow_redirects"])
        self.assertTrue(options["stream"])
        self.assertEqual(options["params"]["Limit"], 6)
        self.assertEqual(options["params"]["EnableUserData"], "true")
        self.assertLessEqual(sum(options["timeout"]), 10)
        item = result["items"][0]
        self.assertEqual(item["title"], "Example Series")
        self.assertIn("S02E03", item["summary"])
        self.assertIn("The Arrival", item["summary"])
        self.assertEqual(item["progress_percent"], 25)
        self.assertEqual(item["position_seconds"], 900)
        self.assertEqual(item["duration_seconds"], 3600)
        self.assertEqual(item["web_url"], f"http://jellyfin.test:8096/base/web/index.html#!/details?id={ITEM}")
        self.assertEqual(item["image_url"], f"{API}/items/{ITEM}/thumbnail")
        self.assertNotIn("Path", json.dumps(result))
        self.assertNotIn("ServerId", json.dumps(result))

    def test_response_limits_and_filters_bad_ids_and_redacts_echoed_credentials(self):
        rows = [resume_item("../../admin"), resume_item(Name=f"<img> {KEY}", SeriesName=KEY),
                *[resume_item(f"{i:032x}") for i in range(1, 10)]]
        self.network.return_value = remote({"Items": rows, "api_key": KEY})
        result = self.get()
        self.assertEqual(len(result["items"]), 6)
        self.assertNotIn(KEY, self.cache.read_text())
        self.assertNotIn("../../admin", json.dumps(result))
        self.assertNotIn("private/library", self.cache.read_text())

    def test_empty_is_a_last_good_payload(self):
        self.network.return_value = remote({"Items": []})
        self.assertEqual(self.get()["state"], "empty")
        self.assertEqual(self.get()["items"], [])
        self.network.assert_called_once()
        self.assertTrue(self.cache.is_file())

    def test_cache_five_minutes_force_and_atomic_reopen(self):
        with patch.object(self.module.os, "replace", wraps=os.replace) as replace:
            first = self.get()
            replace.assert_called_once()
            self.assertEqual(Path(replace.call_args.args[1]), self.cache)
            self.assertNotEqual(Path(replace.call_args.args[0]), self.cache)
        self.clock += 299
        self.assertTrue(self.get()["cached"])
        reopened = self.make_client()
        self.assertEqual(self.get(client=reopened)["items"], first["items"])
        self.network.assert_called_once()
        self.get(True)
        self.assertEqual(self.network.call_count, 2)
        self.clock += 300
        self.get()
        self.assertEqual(self.network.call_count, 3)
        self.assertEqual(list(self.cache.parent.iterdir()), [self.cache])

    def test_errors_keep_last_good_and_force_cannot_bypass_failed_cooldown(self):
        first = self.get()
        self.clock += 301
        self.network.side_effect = requests.Timeout(f"secret={KEY}")
        with self.assertNoLogs(self.module.__name__, level="WARNING"):
            result = self.get(True)
        self.assertEqual(result["state"], "stale")
        self.assertEqual(result["items"], first["items"])
        self.assertTrue(result["stale"])
        self.assertEqual(result["retry_after_seconds"], 300)
        self.get(True)
        self.get(True, self.make_client())
        self.assertEqual(self.network.call_count, 2)
        self.clock += 300
        self.network.side_effect = None
        self.assertEqual(self.get(True)["state"], "ready")
        self.assertEqual(self.network.call_count, 3)

    def test_cold_errors_and_malformed_payloads_return_unavailable_not_empty(self):
        for response in [remote({}, 401), remote({}, 500), remote({}, 302), remote({}),
                         remote({"Items": None}), remote({"Items": "wrong"}),
                         remote(body=b"not-json"), remote(body=b"x" * (1024 * 1024 + 1))]:
            with self.subTest(status=response.status_code):
                self.network.return_value = response
                client = self.make_client(cache=self.cache.parent / f"cold-{id(response)}.json")
                result = self.get(True, client)
                self.assertEqual(result["state"], "unavailable")
                self.assertEqual(result["items"], [])
                self.assertTrue(result["message"])

    def test_cache_scope_changes_never_show_previous_user_or_server(self):
        self.get()
        self.network.side_effect = requests.ConnectionError(KEY)
        for field, value in [("jellyfin_user_id", "c" * 32),
                             ("jellyfin_url", "https://other.test"),
                             ("jellyfin_api_key", "different-key")]:
            with self.subTest(field=field):
                self.config[field] = value
                result = self.get()
                self.assertEqual(result["items"], [])
                self.assertEqual(result["state"], "unavailable")
        self.config["jellyfin_enabled"] = False
        self.assertEqual(self.get()["items"], [])

    def test_configuration_change_during_fetch_does_not_return_old_user_snapshot(self):
        def fetch(*args, **kwargs):
            self.config['jellyfin_user_id'] = 'c' * 32
            return remote({'Items': [resume_item()]})
        self.network.side_effect = fetch
        result = self.get()
        self.assertEqual(result['items'], [])
        self.assertEqual(result['state'], 'unavailable')


    def test_corrupt_disk_cache_is_ignored_and_disk_write_failure_keeps_memory(self):
        self.cache.write_text("{broken", encoding="utf-8")
        client = self.make_client()
        with patch.object(self.module.os, "replace", side_effect=OSError("disk full")):
            result = self.get(client=client)
        self.assertEqual(result["state"], "ready")
        self.assertEqual(self.get(client=client)["items"], result["items"])
        self.network.assert_called_once()
        self.assertEqual(list(self.cache.parent.iterdir()), [self.cache])

    def test_disk_cache_is_resanitized_on_reopen(self):
        self.get()
        store = json.loads(self.cache.read_text())
        store["items"][0].update({"title": KEY, "web_url": f"https://evil.test/?key={KEY}",
                                  "image_url": "https://evil.test/pixel", "private": KEY})
        self.cache.write_text(json.dumps(store), encoding="utf-8")
        result = self.get(client=self.make_client())
        self.assertNotIn("evil.test", json.dumps(result))
        self.assertNotIn("private", json.dumps(result))
        self.assertEqual(result["items"][0]["image_url"], f"{API}/items/{ITEM}/thumbnail")
        self.network.assert_called_once()

    def test_concurrent_force_refreshes_share_one_result(self):
        entered, release = threading.Event(), threading.Event()
        clients = [self.make_client() for _ in range(1)]
        client = clients[0]

        def fetch(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(3))
            return remote({"Items": [resume_item()]})

        self.network.side_effect = fetch
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(self.get, True, client)]
            self.assertTrue(entered.wait(2))
            futures.extend(pool.submit(self.get, True, client) for _ in range(5))
            time.sleep(.2)
            release.set()
            results = [future.result(timeout=5) for future in futures]
        self.network.assert_called_once()
        self.assertTrue(all(result["items"] == results[0]["items"] for result in results))

    def test_thumbnail_only_returns_safe_bytes_with_server_side_auth(self):
        self.get()
        self.network.reset_mock()
        png = b"\x89PNG\r\n\x1a\nfixture"
        self.network.return_value = remote(body=png, content_type="image/png")
        response = self.client.get(f"{API}/items/{ITEM}/thumbnail")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, png)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertNotIn(KEY, str(response.headers))
        options = self.network.call_args.kwargs
        self.assertEqual(options["headers"]["X-Emby-Token"], KEY)
        self.assertFalse(options["allow_redirects"])
        self.assertEqual(self.network.call_args.args[0], f"http://jellyfin.test:8096/base/Items/{ITEM}/Images/Primary")
        self.assertNotIn(KEY, self.network.call_args.args[0])

    def test_thumbnail_rejects_arbitrary_ids_urls_disabled_and_bad_content(self):
        self.get()
        self.network.reset_mock()
        for path in [f"{API}/items/not-an-id/thumbnail", f"{API}/items/{'c' * 32}/thumbnail"]:
            self.assertEqual(self.client.get(path).status_code, 404)
        self.config["jellyfin_enabled"] = False
        self.assertEqual(self.client.get(f"{API}/items/{ITEM}/thumbnail").status_code, 404)
        self.network.assert_not_called()
        self.config["jellyfin_enabled"] = True
        for response in [remote({}, 302), remote({}, 401), remote(body=b"<svg/>", content_type="image/svg+xml"),
                         remote(body=b"<html>secret</html>", content_type="image/png"),
                         remote(body=b"x" * (1024 * 1024 + 1), content_type="image/png")]:
            self.network.return_value = response
            result = self.client.get(f"{API}/items/{ITEM}/thumbnail")
            self.assertEqual(result.status_code, 502)
            self.assertNotIn(KEY, result.text)
            self.assertNotIn("location", result.headers)


class JellyfinFrontendTests(unittest.TestCase):
    def test_real_browser_states_safety_idempotence_and_mobile(self):
        node = shutil.which("node")
        modules = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
        edge = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft/Edge/Application/msedge.exe"
        if not node or not modules.is_dir() or not edge.is_file():
            self.skipTest("Bundled Playwright / Edge unavailable")
        result = subprocess.run([node, "-e", BROWSER_CONTRACT], capture_output=True, text=True, timeout=90,
                                env={**os.environ, "JF_ROOT": str(ROOT), "JF_PLAYWRIGHT": str(modules), "JF_BROWSER": str(edge)})
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


BROWSER_CONTRACT = r"""
const assert = require('node:assert/strict');
const path = require('node:path');
const {chromium} = require(process.env.JF_PLAYWRIGHT);
(async () => {
  const browser = await chromium.launch({executablePath:process.env.JF_BROWSER,headless:true});
  try {
    const page = await browser.newPage({viewport:{width:390,height:844}});
    const errors=[]; page.on('pageerror', e=>errors.push(e.message));
    let calls=0, fail=false, mode='disabled', force=false;
    const item={id:'b'.repeat(32),title:'<img src=x onerror=alert(1)>',summary:'S02E03 - Arrival',
      progress_percent:25,position_seconds:900,duration_seconds:3600,
      web_url:'http://jellyfin.test/web/index.html#!/details?id='+'b'.repeat(32),
      image_url:'/api/dashboard/jellyfin/items/'+'b'.repeat(32)+'/thumbnail'};
    await page.route('http://hub.test/**', async route=> {
      const url=new URL(route.request().url());
      if(url.pathname==='/api/dashboard/jellyfin') {
        calls++; force=url.searchParams.get('force')==='true';
        await new Promise(r=>setTimeout(r,60));
        if(fail) return route.fulfill({status:503,body:'bad'});
        return route.fulfill({contentType:'application/json',body:JSON.stringify({state:mode,
          items:['ready','stale'].includes(mode)?[item]:[],stale:mode==='stale',settings_url:'#settings',
          message:mode==='disabled'?'Jellyfin is disabled.':'',updated_at:1800000000})});
      }
      if(url.pathname.includes('/thumbnail')) return route.fulfill({status:404,body:''});
      return route.fulfill({contentType:'text/html',body:'<div id="home-jellyfin-content"></div><div id="other"></div>'});
    });
    await page.goto('http://hub.test/');
    await page.addStyleTag({path:path.join(process.env.JF_ROOT,'frontend/assets/jellyfin-dashboard.css')});
    await page.addScriptTag({path:path.join(process.env.JF_ROOT,'frontend/assets/jellyfin-dashboard.js')});
    await page.evaluate(()=>{
      window.HolocronJellyfinDashboard.mount(document.getElementById('home-jellyfin-content'));
      window.HolocronJellyfinDashboard.mount(document.getElementById('home-jellyfin-content'));
    });
    assert.equal(calls,0,'mount must not poll or fetch');
    await page.evaluate(()=>Promise.all([window.HolocronJellyfinDashboard.load(),window.HolocronJellyfinDashboard.load()]));
    assert.equal(calls,1,'parallel loads deduplicated');
    assert.equal(await page.locator('#home-jellyfin-content a[href="#settings"]').count(),1);
    assert.equal(await page.getByRole('button',{name:'Refresh Jellyfin'}).count(),1);
    mode='ready';
    await page.evaluate(()=>window.HolocronJellyfinDashboard.load(true)); assert.equal(force,true);
    assert.ok((await page.locator('#home-jellyfin-content').innerText()).includes(item.title));
    assert.equal(await page.locator('img[onerror]').count(),0);
    assert.equal(await page.locator('progress').getAttribute('value'),'25');
    assert.equal(await page.locator('a.jellyfin-item').getAttribute('rel'),'noopener noreferrer');
    assert.equal(await page.locator('iframe,video,audio').count(),0);
    await page.waitForFunction(()=>document.querySelector('.jellyfin-art')?.hidden === true);
    assert.equal(await page.locator('img:not([hidden])').count(),0,'failed artwork hides without blocking title');
    const fits=await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth);
    assert.equal(fits,true,'mobile has no horizontal overflow');
    mode='stale'; await page.evaluate(()=>window.HolocronJellyfinDashboard.load());
    assert.match(await page.locator('#home-jellyfin-content').innerText(),/saved|stale/i);
    fail=true; await page.evaluate(()=>window.HolocronJellyfinDashboard.load());
    assert.ok((await page.locator('#home-jellyfin-content').innerText()).includes(item.title),'saved items survive fetch error');
    fail=false; mode='unconfigured'; await page.evaluate(()=>window.HolocronJellyfinDashboard.load());
    assert.equal(await page.locator('a.jellyfin-item').count(),0);
    fail=true; await page.evaluate(()=>window.HolocronJellyfinDashboard.load());
    assert.equal(await page.locator('a.jellyfin-item').count(),0,'disabled settings clear previous data');
    assert.match(await page.locator('#home-jellyfin-content').innerText(),/unavailable/i);
    fail=false; mode='ready'; item.web_url='javascript:alert(1)'; item.image_url='https://evil.test/pixel';
    await page.evaluate(()=>window.HolocronJellyfinDashboard.load());
    assert.equal(await page.locator('a[href^="javascript:"]').count(),0);
    assert.equal(await page.locator('img[src^="https://evil.test"]').count(),0);
    await page.setViewportSize({width:1200,height:800});
    await page.evaluate(()=>{window.HolocronJellyfinDashboard.mount('#other');});
    assert.equal(await page.locator('#other button').count(),1,'root selector supported');
    assert.deepEqual(errors,[]);
  } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
"""


if __name__ == "__main__":
    unittest.main()
