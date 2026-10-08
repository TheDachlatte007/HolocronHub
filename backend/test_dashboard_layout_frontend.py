import hashlib
import os
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class LayoutBrowserTests(unittest.TestCase):
    def test_sortable_bundle_is_pinned_and_licensed(self):
        vendor = ROOT / 'frontend/assets/vendor'
        digest = hashlib.sha256((vendor / 'Sortable-1.15.7.min.js').read_bytes()).hexdigest()
        self.assertEqual(digest, 'bf4241bc73fef7f11c59a283a69fe8051cdd31c6d8ff5a2b9ba219e7831fcf76')
        self.assertIn('MIT License', (vendor / 'Sortable.LICENSE.txt').read_text(encoding='utf-8'))

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
  const page=await browser.newPage({viewport:{width:1440,height:1000},hasTouch:true}); page.setDefaultTimeout(5000);
  const touch=await page.context().newCDPSession(page);
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.setContent(`<section id="dash" class="home-dashboard"><div class="home-dashboard-grid">${['welcome','launch','weather','monitoring','favorites','jellyfin','library'].map(id=>`<section class="home-panel" data-dashboard-tile="${id}"><h3>${id}</h3><p>Real content</p></section>`).join('')}</div></section>`);
  await page.addStyleTag({path:path.join(process.env.LAYOUT_ROOT,'frontend/assets/dashboard-layout.css')});
  await page.addScriptTag({path:path.join(process.env.LAYOUT_ROOT,'frontend/assets/vendor/Sortable-1.15.7.min.js')});
  await page.addScriptTag({path:path.join(process.env.LAYOUT_ROOT,'frontend/assets/dashboard-layout.js')});
  await page.evaluate(()=>{ window.saved={dashboard_order:['launch','weather','monitoring','favorites','jellyfin'],dashboard_hidden:['jellyfin']}; window.failSave=false;HolocronDashboardLayout.mount(document.getElementById('dash'),{get:()=>saved,save:async value=>{if(failSave)throw Error('Fixture failure');saved=structuredClone(value);}});});
  assert.equal(await page.locator('[data-dashboard-tile="jellyfin"]').isVisible(),false);
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
  await page.getByRole('button',{name:'Add widget',exact:true}).waitFor();
  await page.getByRole('button',{name:'Move Weather later',exact:true}).click();
  assert.equal(await page.getByRole('button',{name:'Move Weather later',exact:true}).evaluate(el=>el===document.activeElement),true,'keyboard move retains focus');
  const order=()=>page.locator('[data-dashboard-tile]').evaluateAll(els=>els.map(el=>el.dataset.dashboardTile));
  assert.deepEqual(await order(),['welcome','launch','monitoring','weather','favorites','jellyfin','library']);
  await page.getByRole('button',{name:'Cancel layout changes',exact:true}).click();
  assert.deepEqual(await order(),['welcome','launch','weather','monitoring','favorites','jellyfin','library']);
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
  const drag=async(name,target,preview=false,useTouch=false)=>{
    const handle=page.getByRole('button',{name:'Drag '+name,exact:true});await handle.hover();
    const from=await handle.boundingBox(),to=await page.locator('.home-dashboard-grid>[data-dashboard-tile="'+target+'"]').boundingBox();
    const move=async(x,y)=>useTouch?touch.send('Input.dispatchTouchEvent',{type:'touchMove',touchPoints:[{x,y}]}):page.mouse.move(x,y,{steps:15});
    if(useTouch)await touch.send('Input.dispatchTouchEvent',{type:'touchStart',touchPoints:[{x:from.x+from.width/2,y:from.y+from.height/2}]});
    else{await page.mouse.move(from.x+from.width/2,from.y+from.height/2);await page.mouse.down();}
    await move(from.x+from.width/2+12,from.y+from.height/2+12);
    const floating=page.locator('.dashboard-tile-float');await floating.waitFor({state:'visible'});
    if(preview){
      assert.match(await floating.innerText(),/Real content/,'whole widget follows pointer, not just a drag button');
      await page.getByRole('button',{name:'Cancel layout changes',exact:true}).evaluate(el=>el.click());
      await page.getByRole('button',{name:'Save layout',exact:true}).evaluate(el=>el.click());
      assert.equal(await page.getByRole('button',{name:'Save layout',exact:true}).isVisible(),true,'dragging cannot discard the active draft');
      const first=await floating.boundingBox();await page.mouse.move(from.x+from.width/2+50,from.y+from.height/2+50,{steps:5});
      const next=await floating.boundingBox();assert(next.x>first.x+20 && next.y>first.y+20,'card preview follows pointer');
    }
    await move(to.x+to.width/2,to.y+to.height/2);await page.waitForTimeout(220);
    if(useTouch)await touch.send('Input.dispatchTouchEvent',{type:'touchEnd',touchPoints:[]});else await page.mouse.up();
    await floating.waitFor({state:'hidden'});
  };
  await drag('Weather','launch',true);
  assert.equal((await order())[1],'weather');
  await drag('Welcome','library');
  assert.equal((await order()).at(-1),'welcome','welcome can be moved independently');
  await page.getByRole('button',{name:'Remove Monitoring',exact:true}).click();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  await page.getByRole('button',{name:'Edit page',exact:true}).waitFor();
  assert.equal(await page.locator('[data-dashboard-tile="monitoring"]').isVisible(),false);
  assert.equal(await page.evaluate(()=>saved.dashboard_order[0]),'weather');
  await page.evaluate(()=>HolocronDashboardLayout.apply(saved));
  assert.equal((await order())[0],'weather');
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
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
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
  await drag('Weather','launch',false,true);
  assert.equal((await order())[1],'weather','touch drags reorder the whole widget');
  await page.getByRole('button',{name:'Cancel layout changes',exact:true}).click();
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
  for(const name of ['Welcome','Quick Launch','Weather','Monitoring','Quick Access','Tool library'])await page.getByRole('button',{name:'Remove '+name,exact:true}).click();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  assert.equal(await page.locator('[data-dashboard-tile]:visible').count(),0);
  await page.getByRole('button',{name:'Edit page',exact:true}).click();
  await page.getByRole('button',{name:'Add widget',exact:true}).click();
  const catalog=page.getByRole('dialog',{name:'Add a dashboard widget'});
  assert.equal(await catalog.locator('input[type="checkbox"]').count(),0,'catalog replaces checkbox wall');
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),'catalog fits mobile');
  await catalog.getByRole('button',{name:'Add Quick Launch',exact:true}).click();
  await page.getByRole('button',{name:'Save layout',exact:true}).click();
  assert.equal(await page.locator('[data-dashboard-tile="launch"]').isVisible(),true);
  assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
  assert.deepEqual(errors,[]);
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
"""
