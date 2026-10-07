"""Launcher regressions; optional browser checks use isolated API fixtures."""
import os
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LauncherFrontendTests(unittest.TestCase):
    def test_brand_header_has_no_redundant_workspace_label(self):
        shell = (ROOT / "frontend/index.html").read_text(encoding="utf-8")
        self.assertFalse('id="shell-context"' in shell, "Redundant header label still exists")

    def test_learning_is_with_workspaces_not_below_settings(self):
        shell = (ROOT / "frontend/index.html").read_text(encoding="utf-8")
        nav = shell.split('<div class="nav"', 1)[1].split('<div class="holocron-content"', 1)[0]
        self.assertLess(nav.index("showTab('homelab'"), nav.index("showTab('learning'"))
        self.assertLess(nav.index("showTab('learning'"), nav.index("showTab('feed'"))

    def test_services_editing_survives_slow_or_failed_health_and_navigation(self):
        node = shutil.which("node")
        module = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
        edge = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft/Edge/Application/msedge.exe"
        if not node or not module.is_dir() or not edge.is_file():
            self.skipTest("Optional bundled Playwright / Edge unavailable")
        env = {**os.environ, "LAUNCHER_ROOT": str(ROOT), "LAUNCHER_PLAYWRIGHT": str(module), "LAUNCHER_BROWSER": str(edge)}
        result = subprocess.run([node, "-e", BROWSER_CONTRACT], env=env, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


BROWSER_CONTRACT = r"""
const assert = require('node:assert/strict');
const path = require('node:path'), fs = require('node:fs');
const {chromium} = require(process.env.LAUNCHER_PLAYWRIGHT);
(async () => {
  const browser = await chromium.launch({executablePath: process.env.LAUNCHER_BROWSER, headless: true});
  let releaseHealth;
  const healthGate = new Promise(resolve => releaseHealth = resolve);
  try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
    page.setDefaultTimeout(5000);
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    let failToolReads = false;
    let tools = Array.from({length: 9}, (_, i) => ({id: 'svc-' + i, name: i ? 'Service ' + i : 'TrueNAS',
      category: 'Home Network', provider: 'Self-hosted', local_or_cloud: 'local', auth_type: 'local', cost_hint: '',
      link: 'https://service-' + i + '.local', group: 'Storage', tags: [], status: 'unknown'}));
    await page.route('**/*', async route => {
      const req = route.request(), url = new URL(req.url());
      if (url.hostname !== 'launcher.test') return route.abort();
      if (url.pathname.startsWith('/assets/')) {
        const file = path.join(process.env.LAUNCHER_ROOT, 'frontend', url.pathname);
        if (!fs.existsSync(file)) return route.fulfill({status:404, body:''});
        return route.fulfill({contentType: url.pathname.endsWith('.js') ? 'text/javascript' : url.pathname.endsWith('.css') ? 'text/css' : 'image/svg+xml', body:fs.readFileSync(file)});
      }
      if (url.pathname === '/') return route.fulfill({contentType: 'text/html', body: fs.readFileSync(path.join(process.env.LAUNCHER_ROOT, 'frontend/index.html'))});
      if (url.pathname === '/api/homelab/overview') {
        await healthGate;
        return route.fulfill({status:503, json:{detail:'Fixture health unavailable'}});
      }
      if (url.pathname === '/api/categories') return route.fulfill({json: {categories:['Home Network']}});
      if (url.pathname === '/api/tools' && req.method() === 'POST') {
        const tool = req.postDataJSON(); tools.push(tool); return route.fulfill({json:tool});
      }
      if (url.pathname === '/api/tools') return route.fulfill(failToolReads ? {status:503,json:{detail:'Fixture reload failure'}} : {json:tools});
      if (url.pathname.startsWith('/api/tools/') && req.method() === 'PATCH') {
        const id = decodeURIComponent(url.pathname.split('/').pop());
        const tool = tools.find(t => t.id === id); Object.assign(tool, req.postDataJSON()); return route.fulfill({json:tool});
      }
      return route.fulfill({json:{errors:[], items:[], standings:[], sessions:[], history:[], ux:{theme:'navy-neon'}}});
    });
    await page.goto('http://launcher.test/');
    await page.waitForFunction(() => _toolCache.length === 9, null, {timeout:5000}).catch(error => {throw Error(error.message + '\n' + errors.join('\n'));});
    await page.getByRole('button', {name: 'All services', exact: false}).first().click();
    await page.waitForFunction(() => document.querySelectorAll('#homelab-command-content .homelab-service-row').length === 9, null, {timeout: 5000});
    assert.equal(await page.locator('#shell-context').count(), 0);
    await page.getByRole('searchbox', {name: 'Filter services'}).fill('truenas');
    assert.equal(await page.locator('#homelab-command-content .homelab-service-row').count(), 1);
    await page.getByRole('button', {name:'Edit TrueNAS', exact:true}).click();
    await page.locator('#service-name').fill('Vault NAS');
    await page.locator('#service-link').fill('https://vault.local');
    failToolReads = true;
    await page.locator('#service-save').click();
    await page.locator('#service-editor').waitFor({state:'hidden'});
    await page.getByRole('searchbox', {name:'Filter services'}).fill('');
    await page.getByRole('button', {name:'Edit Vault NAS', exact:true}).waitFor();
    failToolReads = false;
    releaseHealth();
    await page.waitForFunction(() => document.querySelector('#homelab-command-meta').textContent.includes('unavailable'));
    assert.equal(await page.locator('#homelab-command-content .homelab-service-row').count(), 9);
    await page.getByRole('button', {name:'Add service', exact:true}).click();
    await page.locator('#service-name').fill('New local app');
    await page.locator('#service-link').fill('https://new.local');
    await page.locator('#service-save').click();
    await page.locator('#service-editor').waitFor({state:'hidden'}).catch(async error => {throw Error(error.message + '\n' + await page.locator('#service-editor-status').textContent());});
    await page.getByRole('button', {name:'Edit New local app', exact:true}).waitFor();
    assert.equal(tools.length, 10);
    await page.evaluate(() => {
      const local = _toolCache.find(t => t.id === 'svc-0');
      _homelabCommandData = {overall_status:'healthy', systems:[
        {id:'kuma:19', kuma_monitor_id:'19', name:'Kuma NAS', link:local.link, status:'online'},
        {id:'deleted-registry', name:'Removed service', link:'https://removed.local', status:'offline'}
      ]};
      renderHomelabCommandCenter();
    });
    assert.equal(await page.locator('#homelab-command-content .homelab-service-row').count(), 10, 'late health duplicates or resurrects services');
    assert.equal(await page.locator('#homelab-overall-status .homelab-status-dot.unknown').count(), 1, 'unavailable health must not have a healthy indicator');
    await page.getByRole('button', {name:'Edit Vault NAS', exact:true}).click();
    assert.equal(await page.locator('#service-editor').getAttribute('data-monitor-id'), '19', 'inferred monitor binding missing from editor');
    await page.locator('#service-editor').getByRole('button', {name:'Cancel',exact:true}).click();
    await page.evaluate(() => {
      showTab('tools');
      _toolCache.find(t => t.id === 'svc-0').name = 'Renamed on Home';
      showTab('homelab');
    });
    await page.getByRole('button', {name:'Edit Renamed on Home',exact:true}).waitFor();
    await page.evaluate(() => { _toolCache.find(t => t.id === 'svc-0').name = 'Vault NAS'; renderHomelabCommandCenter(); });
    if (process.env.LAUNCHER_QA_OUTPUT) {
      fs.mkdirSync(process.env.LAUNCHER_QA_OUTPUT, {recursive:true});
      await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'all-services-desktop.png'),fullPage:true});
    }
    for (const width of [1440, 390]) {
      await page.setViewportSize({width, height:900});
      for (const hub of ['warframe', 'f1']) {
        await page.evaluate(name => showTab(name), hub);
        const toggle = page.locator('#shell-nav-toggle');
        if (await toggle.getAttribute('aria-expanded') !== 'true') await toggle.click();
        const nav = await page.locator('.holocron-shell > .nav').boundingBox();
        const header = await page.locator('.app-shell-header').boundingBox();
        const content = await page.locator('.holocron-content').boundingBox();
        assert(nav.x < 50);
        if (width > 900) assert(content.x >= nav.x + nav.width);
        else assert(nav.y >= header.y + header.height);
        await toggle.click();
        assert.equal(await page.locator('.holocron-shell > .nav').isVisible(), false);
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      }
    }
    await page.evaluate(() => openAllServices());
    await page.getByRole('button', {name:'Edit Vault NAS', exact:true}).waitFor();
    assert.equal(await page.locator('.homelab-command-tab.active').textContent(), 'All services');
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'mobile service list must fit viewport');
    assert.deepEqual(errors, []);
  } finally {releaseHealth(); await browser.close();}
})().catch(error => {console.error(error); process.exit(1);});
"""
