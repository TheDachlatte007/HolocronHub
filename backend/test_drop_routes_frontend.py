"""Mission rewards are separate from relic prices and never claim guaranteed runs."""
import os
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class DropFrontendTests(unittest.TestCase):
    def test_routes_safe_dom_query_races_journal_and_mobile(self):
        self.assertTrue((ROOT / 'frontend/assets/warframe-drops.js').is_file())
        node = shutil.which('node')
        module = Path.home() / '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright'
        edge = Path(os.environ.get('PROGRAMFILES(X86)', 'C:/Program Files (x86)')) / 'Microsoft/Edge/Application/msedge.exe'
        if not node or not module.is_dir() or not edge.is_file():
            self.skipTest('Optional browser runtime unavailable')
        env = {**os.environ, 'DROP_ROOT': str(ROOT), 'DROP_PLAYWRIGHT': str(module), 'DROP_BROWSER': str(edge)}
        result = subprocess.run([node, '-e', CONTRACT], env=env, capture_output=True, text=True, timeout=40)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


CONTRACT = r"""
const assert=require('node:assert/strict'), path=require('node:path');
const {chromium}=require(process.env.DROP_PLAYWRIGHT);
(async()=>{
 const browser=await chromium.launch({executablePath:process.env.DROP_BROWSER,headless:true});
 try {
  const page=await browser.newPage({viewport:{width:390,height:844}});page.setDefaultTimeout(4000);
  const errors=[];page.on('pageerror',error=>errors.push(error.message));
  let slow, calls=0, fail=false;
  await page.route('http://drops.test/**',async route=>{
   const url=new URL(route.request().url());
   if(url.pathname==='/')return route.fulfill({contentType:'text/html',body:'<meta name="viewport" content="width=device-width, initial-scale=1"><details id="wf-journal-details"><summary>Journal</summary></details><main id="drops"></main>'});
   calls++;
   if(fail)return route.fulfill({status:503,json:{detail:'Unavailable'}});
   if(url.searchParams.get('q')==='slow')await new Promise(resolve=>slow=resolve);
   return route.fulfill({json:{items:[{item_name:'<img src=x onerror=alert(1)>',node:'Everest',planet:'Earth',game_mode:'Excavation',rotation:'B',chance:10,expected_reward_checks:10,is_event:true}],total:1,stale:true,refreshing:false,fetched_at:'2026-10-07T10:00:00Z'}});
  });
  await page.goto('http://drops.test/');
  await page.addStyleTag({path:path.join(process.env.DROP_ROOT,'frontend/assets/warframe-journal.css')});
  await page.addScriptTag({path:path.join(process.env.DROP_ROOT,'frontend/assets/warframe-drops.js')});
  await page.evaluate(()=>{HolocronFarmJournal={load:async()=>{},setTarget:value=>window.journalTarget=value};HolocronWarframeDrops.mount(document.querySelector('#drops'));});
  assert.equal(calls,0,'mount performs no fetch');
  await page.evaluate(()=>HolocronWarframeDrops.load('slow'));
  await page.waitForFunction(()=>true);
  await page.evaluate(()=>HolocronWarframeDrops.load('mod'));
  await page.getByText('Everest', {exact:false}).first().waitFor();
  slow();
  await page.waitForTimeout(100);
  assert.equal(await page.locator('#drops img').count(),0);
  await page.getByText('Event mission',{exact:true}).waitFor();
  assert.match(await page.locator('#drops').innerText(),/reward rolls/);
  await page.getByRole('button',{name:'Use in journal'}).click();
  await page.waitForFunction(()=>Boolean(window.journalTarget));
  const target=await page.evaluate(()=>window.journalTarget);
  assert.equal(target.route,'Earth / Everest / Rotation B');
  assert.equal(await page.locator('#wf-journal-details').getAttribute('open'),'');
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  fail=true;await page.evaluate(()=>HolocronWarframeDrops.load('offline'));
  await page.getByText('Mission drops unavailable. Retry to load saved routes.',{exact:true}).waitFor();
  await page.evaluate(()=>HolocronWarframeDrops.load(''));
  await page.getByText('Search a target to find missions and reward rotations.',{exact:true}).waitFor();
  assert.deepEqual(errors,[]);
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
"""
