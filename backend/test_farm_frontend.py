"""Real-browser journal checks against a real router and temporary database."""
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]


class FarmFrontendTests(unittest.TestCase):
    def test_manual_journal_browser_persistence_errors_safe_dom_and_mobile(self):
        for name in ("warframe-journal.js", "warframe-journal.css"):
            self.assertTrue((ROOT / "frontend/assets" / name).is_file(), f"Missing journal asset: {name}")
        node = shutil.which("node")
        module = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
        edge = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft/Edge/Application/msedge.exe"
        if not node or not module.is_dir() or not edge.is_file():
            self.skipTest("Node, bundled Playwright or Edge unavailable for browser verification")
        from backend.warframe_farm_api import create_farm_router
        with tempfile.TemporaryDirectory() as folder:
            app = FastAPI()
            app.include_router(create_farm_router(Path(folder) / "mounted/farm.db"))
            with TestClient(app) as client:
                class Handler(BaseHTTPRequestHandler):
                    def do_GET(self):
                        self.handle_request()

                    def do_POST(self):
                        self.handle_request()

                    def do_PATCH(self):
                        self.handle_request()

                    def log_message(self, *args):
                        pass

                    def handle_request(self):
                        if self.path.startswith("/api/"):
                            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                            response = client.request(self.command, self.path, content=body, headers={"Content-Type": "application/json"})
                            code, mime, content = response.status_code, "application/json", response.content
                        elif self.path in ("/assets/warframe-journal.js", "/assets/warframe-journal.css"):
                            code, mime = 200, "text/javascript" if self.path.endswith(".js") else "text/css"
                            content = (ROOT / "frontend" / self.path.lstrip("/")).read_bytes()
                        elif self.path == "/":
                            code, mime = 200, "text/html"
                            content = b'''<!doctype html><meta name="viewport" content="width=device-width, initial-scale=1">
                                <link rel="stylesheet" href="/assets/warframe-journal.css">
                                <main id="journal"></main><script src="/assets/warframe-journal.js"></script>'''
                        else:
                            code, mime, content = 404, "text/plain", b"Not found"
                        self.send_response(code)
                        self.send_header("Content-Type", mime)
                        self.send_header("Content-Length", str(len(content)))
                        self.end_headers()
                        self.wfile.write(content)

                server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    env = {**os.environ, "FARM_PLAYWRIGHT": str(module), "FARM_BROWSER": str(edge),
                           "FARM_URL": f"http://127.0.0.1:{server.server_port}"}
                    result = subprocess.run([node, "-e", BROWSER_CHECK], env=env, capture_output=True, text=True, timeout=90)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    persisted = client.get("/api/warframe/farm-journal/sessions").json()
                    self.assertEqual(persisted["summary"]["session_count"], 2)
                    self.assertEqual(persisted["summary"]["confirmed_sale_platinum"], 12.34)
                    self.assertEqual(persisted["summary"]["estimated_drop_platinum"], 15)
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join(timeout=5)


BROWSER_CHECK = r"""
const assert = require('node:assert/strict');
const {chromium} = require(process.env.FARM_PLAYWRIGHT);
(async () => {
  const browser = await chromium.launch({executablePath: process.env.FARM_BROWSER, headless: true});
  try {
    const page = await browser.newPage({viewport: {width: 390, height: 844}});
    const errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(process.env.FARM_URL);
    const root = page.locator('#journal');
    await page.evaluate(() => {
      window.journal = HolocronFarmJournal.mount(document.querySelector('#journal'));
      assertMount = HolocronFarmJournal.mount(document.querySelector('#journal'));
      if (assertMount !== journal) throw new Error('Duplicate mount');
      HolocronFarmJournal.setTarget({name: 'Forma'});
    });
    assert.equal(await root.getByLabel('Target', {exact: true}).inputValue(), 'Forma');
    await page.evaluate(() => HolocronFarmJournal.load());
    await root.getByLabel('Route', {exact: true}).fill('Void survival');
    await root.getByRole('button', {name: 'Start session', exact: true}).click();
    const active = root.locator('[data-active-session]');
    await active.getByRole('button', {name: 'Log drop', exact: true}).waitFor();
    const hostile = '<img src=x onerror="window.injected=true">';
    await active.getByLabel('Item', {exact: true}).fill(hostile);
    await active.getByLabel('Quantity', {exact: true}).fill('3');
    await active.getByLabel('Estimated unit platinum', {exact: true}).fill('5');
    await active.getByRole('button', {name: 'Log drop', exact: true}).click();
    await active.getByText(hostile, {exact: false}).waitFor();
    assert.equal(await root.locator('img').count(), 0);
    assert.equal(await page.evaluate(() => Boolean(window.injected)), false);
    await active.getByLabel('Confirmed sale proceeds (total platinum)', {exact: true}).fill('12.34');
    await active.getByRole('button', {name: 'Save session', exact: true}).click();
    await page.waitForFunction(() => document.querySelector('[data-summary]').textContent.includes('12.34'));
    // A failed write must preserve the input and must not append a phantom drop.
    await page.route('**/sessions/*/drops', route => route.fulfill({status: 503, contentType: 'application/json', body: JSON.stringify({detail: 'Storage offline'})}));
    await active.getByLabel('Item', {exact: true}).fill('Keep my draft');
    await active.getByRole('button', {name: 'Log drop', exact: true}).click();
    await root.getByRole('alert').filter({hasText: 'Storage offline'}).waitFor();
    assert.equal(await active.getByLabel('Item', {exact: true}).inputValue(), 'Keep my draft');
    await page.unroute('**/sessions/*/drops');
    await page.evaluate(() => HolocronFarmJournal.load());
    assert.equal(await active.getByLabel('Item', {exact: true}).inputValue(), 'Keep my draft');
    await active.getByRole('button', {name: 'Stop session', exact: true}).click();
    await root.getByRole('button', {name: 'Start session', exact: true}).waitFor({state: 'visible'});
    await root.getByRole('button', {name: 'Start session', exact: true}).click();
    await active.getByRole('button', {name: 'Stop session', exact: true}).waitFor();
    await page.reload();
    await page.evaluate(async () => {
      HolocronFarmJournal.mount(document.querySelector('#journal'));
      await HolocronFarmJournal.load();
    });
    await root.locator('[data-history] > summary').click();
    const recorded = root.locator('[data-history-list] > details');
    assert.equal(await recorded.count(), 1);
    await recorded.locator('summary').click();
    await recorded.getByLabel('Route', {exact: true}).fill('Corrected route');
    await recorded.getByRole('button', {name: 'Save session', exact: true}).click();
    await recorded.getByText('Corrected route', {exact: false}).first().waitFor();
    // Unpriced entries remain explicitly unknown and do not become sale proceeds.
    await recorded.getByLabel('Item', {exact: true}).fill('Unpriced resource');
    await recorded.getByRole('button', {name: 'Log drop', exact: true}).click();
    await recorded.getByText('Unpriced resource', {exact: false}).waitFor();
    assert.match(await root.locator('[data-summary]').textContent(), /unvalued/i);
    // Correcting a finished entry must leave the newer active session running.
    assert.equal(await root.locator('[data-active-session]').getByRole('button', {name: 'Stop session'}).count(), 1);
    // A read failure preserves saved data and offers explicit retry.
    await page.route('**/farm-journal/sessions?*', route => route.fulfill({status: 503, contentType: 'application/json', body: '{"detail":"Read offline"}'}));
    await page.evaluate(() => HolocronFarmJournal.load());
    await root.getByRole('alert').filter({hasText: 'Read offline'}).waitFor();
    assert.match(await recorded.textContent(), /Unpriced resource/);
    await page.unroute('**/farm-journal/sessions?*');
    await root.getByRole('button', {name: 'Refresh journal', exact: true}).click();
    await page.waitForFunction(() => !document.querySelector('#journal [role="alert"]').textContent);
    for (const width of [390, 320, 1280]) {
      await page.setViewportSize({width, height: 844});
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `Overflow at ${width}`);
    }
    assert.deepEqual(errors, []);
    console.log('Browser: persisted start/stop, drop/sale separation, corrections, safe DOM, drafts, errors and responsive layout passed.');
  } finally {
    await browser.close();
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
"""


if __name__ == "__main__":
    unittest.main()
