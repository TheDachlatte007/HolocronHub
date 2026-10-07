import os
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class LayoutBrowserTests(unittest.TestCase):
    def test_dashboard_arrangement_is_editable_and_persistent(self):
        self.assertTrue((ROOT / "frontend/assets/dashboard-layout.js").is_file(), "Dashboard layout module missing")
        node = shutil.which("node")
        module = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright"
        edge = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft/Edge/Application/msedge.exe"
        if not node or not module.is_dir() or not edge.is_file():
            self.skipTest("Optional Playwright / Edge unavailable")
        result = subprocess.run([node, "-e", BROWSER], env={**os.environ, "LAYOUT_ROOT":str(ROOT), "LAYOUT_PLAYWRIGHT":str(module), "LAYOUT_BROWSER":str(edge)}, text=True, capture_output=True, timeout=70)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


BROWSER = r"""
const assert=require('node:assert/strict'), path=require('node:path');
const {chromium}=require(process.env.LAYOUT_PLAYWRIGHT);
(async()=>{
 const browser=await chromium.launch({executablePath:process.env.LAYOUT_BROWSER,headless:true});
 try {
  const page=await browser.newPage({viewport:{width:1440,height:1000}}); page.setDefaultTimeout(5000);
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent(`<section id="dash" class="home-dashboard"><div class="home-dashboard-grid">${['launch','weather','monitoring','favorites','jellyfin'].map(id=>`<section class="home-panel" data-dashboard-tile="${id}"><h3>${id}</h3><p>Real content</p></section>`).join('')}</div></section>`);
  await page.addStyleTag({path:path.join(process.env.LAYOUT_ROOT,'frontend/assets/dashboard-layout.css')});
  await page.addScriptTag({path:path.join(process.env.LAYOUT_ROOT,'frontend/assets/dashboard-layout.js')});
  await page.evaluate(()=>{ window.saved={dashboard_order:['launch','weather','monitoring','favorites','jellyfin'],dashboard_hidden:['jellyfin']}; window.failSave=false;HolocronDashboardLayout.mount(document.getElementById('dash'),{get:()=>saved,save:async value=>{if(failSave)throw Error('Fixture failure');saved=structuredClone(value);}});});
  assert.equal(await page.locator('[data-dashboard-tile="jellyfin"]').isVisible(),false);
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).click();
  await page.getByRole('button',{name:'Move Weather later',exact:true}).click();
  assert.equal(await page.getByRole('button',{name:'Move Weather later',exact:true}).evaluate(el=>el===document.activeElement),true,'keyboard move retains focus');
  const order=()=>page.locator('[data-dashboard-tile]').evaluateAll(els=>els.map(el=>el.dataset.dashboardTile));
  assert.deepEqual(await order(),['launch','monitoring','weather','favorites','jellyfin']);
  await page.getByRole('button',{name:'Cancel layout changes',exact:true}).click();
  assert.deepEqual(await order(),['launch','weather','monitoring','favorites','jellyfin']);
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).click();
  await page.getByRole('button',{name:'Drag Weather',exact:true}).dragTo(page.locator('[data-dashboard-tile="launch"]'));
  assert.equal((await order())[0],'weather');
  await page.getByRole('checkbox',{name:'Show Monitoring',exact:true}).uncheck();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).waitFor();
  assert.equal(await page.locator('[data-dashboard-tile="monitoring"]').isVisible(),false);
  assert.equal(await page.evaluate(()=>saved.dashboard_order[0]),'weather');
  await page.evaluate(()=>HolocronDashboardLayout.apply(saved));
  assert.equal((await order())[0],'weather');
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).click();
  await page.getByRole('button',{name:'Move Weather later',exact:true}).click();
  await page.evaluate(()=>{failSave=true;});
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  await page.getByRole('status').filter({hasText:'Could not save'}).waitFor();
  assert.equal(await page.getByRole('button',{name:'Save layout',exact:true}).isVisible(),true);
  await page.evaluate(()=>{failSave=false;});
  await page.getByRole('button',{name:'Reset layout',exact:true}).click();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  assert.deepEqual(await page.evaluate(()=>saved.dashboard_hidden),['jellyfin']);
  await page.setViewportSize({width:390,height:844});
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).click();
  for(const name of ['Quick Launch','Weather','Monitoring','Quick Access','Jellyfin'])await page.getByRole('checkbox',{name:'Show '+name,exact:true}).uncheck();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  assert.equal(await page.locator('[data-dashboard-tile]:visible').count(),0);
  await page.getByRole('button',{name:'Arrange dashboard',exact:true}).click();
  await page.getByRole('checkbox',{name:'Show Quick Launch',exact:true}).check();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  assert.equal(await page.locator('[data-dashboard-tile="launch"]').isVisible(),true);
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  assert.deepEqual(errors,[]);
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
"""
