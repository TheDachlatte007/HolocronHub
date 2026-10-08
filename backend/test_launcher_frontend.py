"""Launcher regressions; optional browser checks use isolated API fixtures."""
import os
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LauncherFrontendTests(unittest.TestCase):
    def test_build_details_belong_to_general_settings_only(self):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup((ROOT / 'frontend/index.html').read_text(encoding='utf-8'), 'html.parser')
        self.assertIsNotNone(soup.select_one('.build-info-settings').find_parent(id='settings-panel-general'))
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
    let failToolReads = false, mediaCalls = 0;
    let settings = {ux:{theme:'navy-neon',dashboard_order:['launch','weather','monitoring','favorites','jellyfin'],dashboard_hidden:['jellyfin']},homelab:{jellyfin_enabled:false}};
    let tools = Array.from({length: 9}, (_, i) => ({id: 'svc-' + i, name: i ? 'Service ' + i : 'TrueNAS',
      category: 'Home Network', provider: 'Self-hosted', local_or_cloud: 'local', auth_type: 'local', cost_hint: '',
      link: 'https://service-' + i + '.local', group: 'Storage', tags: [], status: 'unknown', favorite: i === 0}));
    await page.route('**/*', async route => {
      const req = route.request(), url = new URL(req.url());
      if (url.hostname !== 'launcher.test') return route.abort();
      if (url.pathname.startsWith('/assets/')) {
        const file = path.join(process.env.LAUNCHER_ROOT, 'frontend', url.pathname);
        if (!fs.existsSync(file)) return route.fulfill({status:404, body:''});
        const mime={'.js':'text/javascript','.css':'text/css','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.woff2':'font/woff2'}[path.extname(file)]||'application/octet-stream';
        return route.fulfill({contentType:mime, body:fs.readFileSync(file)});
      }
      if (url.pathname === '/') return route.fulfill({contentType: 'text/html', body: fs.readFileSync(path.join(process.env.LAUNCHER_ROOT, 'frontend/index.html'))});
      if (url.pathname === '/api/settings') {
        if (req.method() === 'PATCH') for (const [section, values] of Object.entries(req.postDataJSON())) settings[section]={...settings[section],...values};
        return route.fulfill({json:settings});
      }
      if (url.pathname === '/api/dashboard/jellyfin') {mediaCalls++;return route.fulfill({json:{state:'disabled',items:[]}});}
      if (url.pathname === '/api/build-info') return route.fulfill({json:{version:'0.2.0',source_id:'c'.repeat(64),revision:null,built_at:'2026-10-08T12:00:00Z',source:'image-build'}});
      if (url.pathname === '/api/dashboard/weather') return route.fulfill({json:{location:'Augsburg',air_temperature:11,weather_code:45,is_day:true,feels_like:8,wind_speed:4,humidity:90,rainfall:0,date:'2026-10-08T12:00:00Z'}});
      if (url.pathname === '/api/dashboard/kuma') return route.fulfill({json:{configured:true,summary:{total:2,online:2,offline:0},services:[{name:'Fixture NAS',status:'online'},{name:'Fixture Media',status:'online'}],checked_at:'2026-10-08T12:00:00Z'}});
      if (url.pathname === '/api/warframe/farm-journal/sessions') return route.fulfill({json:{sessions:[],active_session:null,summary:{session_count:0},has_more:false}});
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
    assert.equal(await page.locator('.shell-topnav').count(),0,'duplicate workspace navigation removed');
    await page.locator('#home-build-info').getByText(/cccccccccccc/).waitFor();
    await page.locator('#home-build-info').click();
    await page.locator('#settings-panel-general.active .build-info-settings').waitFor();
    for (const panel of ['appearance','feed','ingest','api','homelab']) {
      await page.evaluate(name=>showSettingsPanel(name),panel);
      assert.equal(await page.locator('.build-info-settings').isVisible(),false,'build details do not follow '+panel);
    }
    await page.evaluate(()=>showTab('tools'));
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
        await page.locator('.app-shell-header').getByRole('button',{name:'Settings',exact:true}).waitFor();
        await page.getByRole('button',{name:'Search Hub',exact:true}).click();
        await page.locator('#tool-search-panel').waitFor({state:'visible'});
        await page.keyboard.press('Escape');
        const shellCard = page.locator(`#tab-${hub} > .card`);
        const shellStyle = await shellCard.evaluate(el => {
          const css = getComputedStyle(el);
          return {radius:parseFloat(css.borderTopLeftRadius), left:parseFloat(css.borderLeftWidth), right:parseFloat(css.borderRightWidth)};
        });
        assert(shellStyle.radius >= 12 && shellStyle.left >= 1 && shellStyle.right >= 1, `${hub}: hub frame must match rounded dashboard cards`);
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
        const collapsedRadius = await shellCard.evaluate(el => parseFloat(getComputedStyle(el).borderTopLeftRadius));
        assert(collapsedRadius >= 12, `${hub}: collapsed navigation must preserve card rounding`);
        assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
        if(width>900){
          await page.evaluate(()=>{const spacer=document.createElement('div');spacer.id='qa-spacer';spacer.style.height='1700px';document.querySelector('.holocron-content').append(spacer);window.scrollTo(0,200);});
          await page.waitForTimeout(50);
          const sticky = await page.locator('.app-shell-header').boundingBox();
          assert(sticky.y>=0 && sticky.y<10,'header remains visible on desktop scroll');
          await page.evaluate(()=>{document.getElementById('qa-spacer').remove();window.scrollTo(0,0);});
        }
      }
    }
    await page.evaluate(() => openAllServices());
    await page.getByRole('button', {name:'Edit Vault NAS', exact:true}).waitFor();
    assert.equal(await page.locator('.homelab-command-tab.active').textContent(), 'All services');
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'mobile service list must fit viewport');
    await page.evaluate(() => showTab('tools'));
    if(process.env.LAUNCHER_QA_OUTPUT){
      await page.setViewportSize({width:1440,height:1000});
      await page.waitForFunction(()=>!document.querySelector('.home-arriving'));
      await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'home-default.png'),fullPage:true});
    }
    assert.equal(mediaCalls,0,'hidden optional tile performs no requests');
    await page.getByRole('button',{name:'Edit page',exact:true}).click();
    if(process.env.LAUNCHER_QA_OUTPUT){
      await page.setViewportSize({width:1440,height:1000});
      const handle=page.getByRole('button',{name:'Drag Weather',exact:true});await handle.hover();
      const from=await handle.boundingBox();
      await page.mouse.move(from.x+30,from.y+20);await page.mouse.down();
      await page.mouse.move(from.x+75,from.y+60,{steps:10});
      await page.locator('.dashboard-tile-float').waitFor({state:'visible'});
      await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'home-drag-preview.png'),fullPage:false});
      await page.mouse.up();
      await page.getByRole('button',{name:'Cancel layout changes',exact:true}).click();
      await page.getByRole('button',{name:'Edit page',exact:true}).click();
      await page.setViewportSize({width:390,height:1000});
      const bar=await page.locator('[data-dashboard-tile="welcome"] .dashboard-tile-controls').boundingBox();
      const welcome=await page.locator('[data-dashboard-tile="welcome"]').boundingBox();
      assert(bar.width>welcome.width-45,'welcome editor header uses the available mobile width');
      await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'home-editor-mobile.png'),fullPage:true});
    }
    await page.getByRole('button',{name:'Remove Welcome',exact:true}).click();
    await page.getByRole('button',{name:'Remove Tool library',exact:true}).click();
    await page.getByRole('button',{name:'Move Weather earlier',exact:true}).click();
    await page.getByRole('button',{name:'Remove Monitoring',exact:true}).click();
    await page.getByRole('button',{name:'Add widget',exact:true}).click();
    if(process.env.LAUNCHER_QA_OUTPUT){
      for(const width of [1440,390]){
        await page.setViewportSize({width,height:1000});
        await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'widget-catalog-'+width+'.png'),fullPage:false});
      }
    }
    await page.getByRole('dialog',{name:'Add a dashboard widget'}).getByRole('button',{name:'Add Jellyfin',exact:true}).click();
    await page.getByRole('button',{name:'Save layout',exact:true}).click();
    await page.getByRole('button',{name:'Edit page',exact:true}).waitFor();
    await page.getByText('Jellyfin is disabled. Enable it in Settings.',{exact:true}).waitFor();
    assert.equal(settings.ux.dashboard_order.filter(id=>id!=='welcome')[0],'weather');
    await page.reload();
    await page.getByRole('button',{name:'Edit page',exact:true}).waitFor();
    await page.waitForFunction(()=>_appSettings?.ux.dashboard_order.filter(id=>id!=='welcome')[0]==='weather');
    await page.waitForFunction(()=>!document.querySelector('.home-arriving'));
    assert.equal(await page.locator('[data-dashboard-tile]').first().getAttribute('data-dashboard-tile'),'welcome');
    assert.equal(await page.locator('[data-dashboard-tile="welcome"]').isVisible(),false,'welcome can be hidden');
    assert.equal(await page.locator('[data-dashboard-tile="library"]').isVisible(),false,'library can be hidden');
    assert.equal(await page.locator('[data-dashboard-tile="monitoring"]').isVisible(),false,'hide survives reload');
    assert.equal(await page.locator('[data-dashboard-tile="favorites"]').count(),1);
    assert.equal(await page.locator('.home-dashboard-grid > [data-dashboard-tile="favorites"]').count(),1,'quick access is a real arranged tile');
    for (const width of [1440,1024,901,390,320]) {
      await page.setViewportSize({width,height:1000});
      assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'dashboard fits '+width);
      if(process.env.LAUNCHER_QA_OUTPUT)await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'dashboard-'+width+'.png'),fullPage:true});
    }
    await page.evaluate(()=>{showTab('warframe');toggleToolSearchPanel(true);});
    await page.locator('#q').fill('Vault');
    await page.getByRole('button',{name:'Filter directory',exact:true}).click();
    await page.locator('[data-dashboard-tile="library"]').waitFor({state:'visible'});
    assert(settings.ux.dashboard_hidden.includes('library'),'search reveal does not overwrite saved layout');
    await page.evaluate(()=>clearToolSearch());
    await page.locator('[data-dashboard-tile="library"]').waitFor({state:'hidden'});
    await page.evaluate(()=>{showTab('warframe');setWarframeWorkspace('planner');document.getElementById('wf-journal-details').open=true;});
    await page.locator('#wf-farm-journal').getByRole('button',{name:'Start session',exact:true}).waitFor();
    await page.evaluate(()=>updateWarframeHeroStatus({stale:true,market:{last_avg_price:78},data_as_of:'2026-10-08T12:00:00Z'}));
    assert.equal(await page.locator('#wf-market-hero-status').getAttribute('data-state'),'stale');
    await page.evaluate(()=>updateWarframeHeroStatus({refresh_error:'Timeout',market:{}}));
    assert.equal(await page.locator('#wf-market-hero-status').getAttribute('data-state'),'error');
    await page.setViewportSize({width:1440,height:1000});
    await page.evaluate(()=>setGlobalNavCollapsed(false));
    await page.evaluate(()=>updateWarframeHeroStatus({stale:true,market:{last_avg_price:78},data_as_of:'2026-10-08T12:00:00Z'}));
    if(process.env.LAUNCHER_QA_OUTPUT)await page.screenshot({path:path.join(process.env.LAUNCHER_QA_OUTPUT,'warframe-header.png'),fullPage:false});
    assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'journal integration fits mobile Warframe');
    assert.deepEqual(errors, []);
  } finally {releaseHealth(); await browser.close();}
})().catch(error => {console.error(error); process.exit(1);});
"""
