"""Learning integration contracts and optional real-browser interaction checks."""
import os
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LearningFrontendTests(unittest.TestCase):
    def source(self, name):
        path = ROOT / name
        self.assertTrue(path.is_file(), f"Missing Learning asset: {name}")
        return path.read_text(encoding="utf-8")

    def test_shell_navigation_and_hash_context_register_learning(self):
        shell = self.source("frontend/index.html")
        self.assertNotIn('class="shell-topnav"', shell)
        self.assertRegex(shell, r'class="nav-tab"[^>]*showTab\(\'learning\'')
        self.assertRegex(shell, r"learning:\s*'Learning'")
        self.assertIn('id="tab-learning"', shell)

    def test_assets_are_loaded_and_learning_is_lazy_mounted_by_router(self):
        shell = self.source("frontend/index.html")
        self.assertIn('href="assets/learning.css"', shell)
        self.assertIn('src="assets/learning.js"', shell)
        self.assertRegex(shell, r"if\s*\(name\s*===\s*'learning'\)")
        self.assertIn("HolocronLearning.mount", shell)
        self.assertIn("HolocronLearning.load", shell)

    def test_mobile_navigation_does_not_require_tools_workspace(self):
        shell = self.source("frontend/index.html")
        self.assertRegex(shell, r"@media\s*\(max-width:\s*900px\)\s*\{\s*body\.nav-menu-open\s+\.holocron-shell>\.nav")

    def test_module_exposes_mount_and_load_without_injecting_api_html(self):
        module = self.source("frontend/assets/learning.js")
        self.assertIn("window.HolocronLearning", module)
        self.assertIn("textContent", module)
        self.assertNotIn("innerHTML", module)
        self.assertNotIn("insertAdjacentHTML", module)

    def test_keyboard_controls_guard_editable_targets(self):
        module = self.source("frontend/assets/learning.js")
        self.assertIn("keydown", module)
        self.assertIn("Space", module)
        self.assertIn("[1-4]", module)
        self.assertIn("isContentEditable", module)
        self.assertIn("textarea", module)
        self.assertIn("select", module)

    def test_styles_are_scoped_and_controls_support_touch(self):
        css = self.source("frontend/assets/learning.css")
        self.assertIn("#tab-learning", css)
        self.assertRegex(css, r"min-height:\s*(44|46|48)px")
        self.assertIn("focus-visible", css)
        self.assertIn("16px", css)

    def test_browser_study_browse_keyboard_errors_and_mobile_layout(self):
        self.source("frontend/assets/learning.js")
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js unavailable for optional browser test")
        modules = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules"
        module = modules / "playwright"
        edge = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)")) / "Microsoft/Edge/Application/msedge.exe"
        if not module.is_dir() or not edge.is_file():
            self.skipTest("Bundled Playwright / Edge unavailable for optional browser test")
        env = {**os.environ, "LEARNING_PLAYWRIGHT": str(module), "LEARNING_BROWSER": str(edge), "LEARNING_ROOT": str(ROOT)}
        result = subprocess.run([node, "-e", BROWSER_CONTRACT], env=env, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_browser_personal_authoring_import_and_filters(self):
        node = shutil.which('node')
        module = Path.home() / '.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright'
        edge = Path(os.environ.get('PROGRAMFILES(X86)', 'C:/Program Files (x86)')) / 'Microsoft/Edge/Application/msedge.exe'
        if not node or not module.is_dir() or not edge.is_file():
            self.skipTest('Bundled Playwright / Edge unavailable')
        env = {**os.environ, 'LEARNING_PLAYWRIGHT': str(module), 'LEARNING_BROWSER': str(edge), 'LEARNING_ROOT': str(ROOT)}
        result = subprocess.run([node, '-e', AUTHORING_CONTRACT], env=env, capture_output=True, text=True, timeout=90)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


BROWSER_CONTRACT = r"""
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const {chromium} = require(process.env.LEARNING_PLAYWRIGHT);
(async () => {
  const browser = await chromium.launch({executablePath: process.env.LEARNING_BROWSER, headless: true});
  try {
    const page = await browser.newPage({viewport: {width: 390, height: 844}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const card = {id:'eng-001', deck:'Engineering English', category:'Measurement & Data', skill:'Vocabulary',
      prompt:'Explain <img src=x onerror=alert(1)> uncertainty.', answer:'A quantified doubt.',
      example:'Report the uncertainty.', explanation:'Measurements have limits.', tags:['data'], source:'seed',
      state:'new', due_at:null, interval_days:0, ease:2.5, review_count:0, lapse_count:0, last_review_at:null, status:'new'};
    let reviews = 0, sessionRequests = 0, failReview = false, failSummary = false, fullShell = false;
    const progress = {card_id:card.id, state:'review', due_at:'2026-10-10T12:00:00Z', interval_days:3,
      ease:2.5, review_count:1, lapse_count:0, last_review_at:'2026-10-07T12:00:00Z'};
    await page.route('http://learning.test/**', async route => {
      const req = route.request(), url = new URL(req.url());
      let body, status = 200;
      if (url.pathname === '/api/learning/summary') {
        if (failSummary) { status=503; body={detail:'Learning store unavailable'}; }
        else body={total_cards:120,new_cards:120-reviews,due_cards:0,mastered_cards:0,reviews_today:reviews,streak_days:reviews?1:0,
          categories:[{category:'Measurement & Data',total_cards:30,reviewed_cards:reviews,due_cards:0}]};
      } else if (url.pathname === '/api/learning/session') {
        sessionRequests++;
        assert.equal(url.searchParams.get('limit'),'10');
        body=reviews ? [] : [card];
      } else if (url.pathname === '/api/learning/cards') {
        const q=url.searchParams.get('query');
        if (q === 'slow') await new Promise(resolve=>setTimeout(resolve,600));
        body=q && q!=='slow' ? [] : [{...card,status:reviews?'scheduled':'new',due_at:reviews?progress.due_at:null}];
      } else if (url.pathname === '/api/learning/reviews') {
        assert.deepEqual(req.postDataJSON(),{card_id:'eng-001',rating:3});
        await new Promise(resolve=>setTimeout(resolve,80));
        if (failReview) { status=503; body={detail:'Learning store unavailable'}; }
        else { reviews++; body=progress; }
      } else {
        if (fullShell) {
          if (url.pathname.startsWith('/assets/')) {
            const asset = path.join(process.env.LEARNING_ROOT,'frontend',url.pathname);
            if (fs.existsSync(asset)) return route.fulfill({contentType:url.pathname.endsWith('.js')?'text/javascript':'text/css',body:fs.readFileSync(asset)});
            return route.fulfill({status:404,body:''});
          }
          if (url.pathname.startsWith('/api/')) return route.fulfill({contentType:'application/json',body:url.pathname==='/api/tools'?'[]':'{}'});
          return route.fulfill({contentType:'text/html',body:fs.readFileSync(path.join(process.env.LEARNING_ROOT,'frontend/index.html'))});
        }
        return route.fulfill({contentType:'text/html',body:'<body data-hub="learning"><main id="tab-learning"></main></body>'});
      }
      return route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
    });
    await page.goto('http://learning.test/#learning');
    await page.addStyleTag({path:path.join(process.env.LEARNING_ROOT,'frontend/assets/learning.css')});
    await page.addScriptTag({path:path.join(process.env.LEARNING_ROOT,'frontend/assets/learning.js')});
    await page.evaluate(async () => {
      window.HolocronLearning.mount(document.querySelector('#tab-learning'));
      window.HolocronLearning.mount(document.querySelector('#tab-learning'));
      await window.HolocronLearning.load();
    });
    assert.equal(await page.locator('[data-learning="start"]').count(),1,'mount is idempotent');
    assert.equal(sessionRequests,0,'overview must not start a review session');
    await page.locator('[data-learning="length"]').selectOption('10');
    await page.locator('[data-learning="start"]').click();
    await page.locator('[data-learning="reveal"]').waitFor({state:'visible'});
    assert.equal(await page.locator('[data-learning="answer"]').isVisible(),false);
    assert.equal(await page.locator('[data-rating="3"]').count(),0,'ratings absent before reveal');
    assert.equal(await page.locator('#tab-learning img').count(),0,'API text cannot create HTML');
    await page.locator('[data-learning="search"]').focus();
    await page.keyboard.press('Space');
    assert.equal(await page.locator('[data-learning="answer"]').isVisible(),false,'editable guard');
    await page.locator('[data-learning="search"]').fill('');
    await page.evaluate(()=>document.activeElement.blur());
    await page.keyboard.press('Space');
    assert.equal(await page.locator('[data-learning="answer"]').isVisible(),true);
    await page.evaluate(()=>window.HolocronLearning.load());
    assert.equal(await page.locator('[data-learning="answer"]').isVisible(),true,'load preserves session');
    failReview=true;
    await page.keyboard.press('3');
    await page.locator('[data-learning="study-error"]').waitFor({state:'visible'});
    assert.equal(reviews,0);
    assert.equal(await page.locator('[data-learning="answer"]').isVisible(),true,'failed save retains card');
    failReview=false;
    await page.keyboard.press('3');
    await page.keyboard.press('3');
    await page.getByText('Session complete', {exact:true}).waitFor();
    assert.equal(reviews,1,'busy guard prevents duplicate reviews');
    await page.getByText('1 card reviewed', {exact:true}).waitFor({timeout:3000});
    await page.locator('[data-learning="start"]').click();
    await page.getByText('You are all caught up', {exact:true}).waitFor();
    await page.locator('[data-learning="search"]').fill('slow');
    await page.waitForTimeout(320);
    await page.locator('[data-learning="search"]').fill('missing');
    await page.getByText('No cards match your filters', {exact:true}).waitFor();
    await page.waitForTimeout(650);
    assert.equal(await page.getByText('No cards match your filters',{exact:true}).isVisible(),true,'stale browse ignored');
    failSummary=true;
    await page.evaluate(()=>window.HolocronLearning.load());
    assert.equal(await page.locator('[data-learning="summary-error"]').isVisible(),true);
    failSummary=false;
    await page.evaluate(()=>window.HolocronLearning.load());
    assert.equal(await page.locator('[data-learning="summary-error"]').isVisible(),false);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true,'mobile must not overflow');
    assert.deepEqual(errors,[]);
    fullShell=true;
    await page.goto('http://learning.test/?shell=1#learning');
    await page.locator('[data-learning="start"]').waitFor();
    assert.equal(await page.locator('#shell-context').count(),0,'workspace context belongs in navigation, not next to the brand');
    assert.equal(await page.locator('.holocron-shell > .nav .nav-tab.active').innerText(),'Learning');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true,'full shell must fit mobile');
    await page.locator('#shell-nav-toggle').click();
    const learningNav=page.locator('.holocron-shell > .nav').getByRole('button',{name:'Learning',exact:true});
    assert.equal(await learningNav.isVisible(),true,'mobile navigation must expose Learning');
    await learningNav.click();
    assert.equal(new URL(page.url()).hash,'#learning');
    await page.evaluate(()=>showTab('homelab'));
    await page.locator('#shell-nav-toggle').click();
    assert.equal(await learningNav.isVisible(),true,'Learning reachable outside Home on mobile');
    await learningNav.click();
    await page.setViewportSize({width:1440,height:1000});
    assert.equal(await page.locator('#tab-learning').isVisible(),true);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),true,'desktop shell must fit');
    assert.deepEqual(errors,[],'full shell must not raise JavaScript errors');
    console.log('Browser contract passed: reveal, ratings, keyboard, safe text, errors, stale browse, mobile.');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exit(1);});
"""


AUTHORING_CONTRACT = r"""
const assert = require('node:assert/strict');
const path = require('node:path');
const {chromium} = require(process.env.LEARNING_PLAYWRIGHT);
(async () => {
  const browser = await chromium.launch({executablePath:process.env.LEARNING_BROWSER,headless:true});
  try {
    const page = await browser.newPage({viewport:{width:390,height:844}});
    page.setDefaultTimeout(5000);
    const errors=[]; page.on('pageerror',error=>errors.push(error.message));
    let personal=null, imports=0, previews=0, reviews=0, lastBrowse;
    const seed={id:'seed',ownership:'seed',deck:'English',category:'Measurement & Data',skill:'Recognition',prompt:'Seed',answer:'Answer',tags:[],status:'new'};
    await page.route('http://learning.test/**',async route=>{
      const req=route.request(), url=new URL(req.url()); let body, status=200;
      if(url.pathname==='/api/learning/summary') body={total_cards:personal?2:1,new_cards:2,due_cards:0,mastered_cards:0,reviews_today:reviews,streak_days:0,
        categories:[{category:'Measurement & Data',total_cards:1,reviewed_cards:0,due_cards:0},...(personal?[{category:personal.category,total_cards:1,reviewed_cards:0,due_cards:0}]:[])],
        decks:[{deck:'English',total_cards:1},...(personal?[{deck:personal.deck,total_cards:1}]:[])]};
      else if(url.pathname==='/api/learning/cards' && req.method()==='GET') {lastBrowse=url;body=[seed,...(personal?[personal]:[])].filter(c=>(!url.searchParams.get('deck')||c.deck===url.searchParams.get('deck'))&&(!url.searchParams.get('category')||c.category===url.searchParams.get('category')));}
      else if(url.pathname==='/api/learning/cards' && req.method()==='POST') {personal={...req.postDataJSON(),id:'personal-test',ownership:'personal',status:'scheduled',review_count:2,due_at:'2026-11-01T12:00:00Z'};body=personal;status=201;}
      else if(url.pathname==='/api/learning/cards/personal-test' && req.method()==='PATCH') {personal={...personal,...req.postDataJSON()};body=personal;}
      else if(url.pathname==='/api/learning/cards/personal-test' && req.method()==='DELETE') {personal=null;body={deleted:true};}
      else if(url.pathname==='/api/learning/import/preview') {
        previews++; const payload=req.postDataJSON();
        if(payload.content==='invalid'){status=422;body={detail:'Invalid JSON or CSV file'};}
        else body={importable:1,skipped:0,cards:[{prompt:'<img src=x onerror=alert(1)>',answer:'Imported',deck:'Imported',category:'Personal',action:'import'}]};
      } else if(url.pathname==='/api/learning/import') {imports++;body={imported:1,skipped:0};}
      else if(url.pathname==='/api/learning/session') body=[seed];
      else if(url.pathname==='/api/learning/reviews') {reviews++;body={card_id:'seed',due_at:'2026-11-01T12:00:00Z',review_count:1};}
      else return route.fulfill({contentType:'text/html',body:'<body data-hub="learning"><main id="tab-learning"></main></body>'});
      return route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
    });
    await page.goto('http://learning.test/');
    await page.addStyleTag({path:path.join(process.env.LEARNING_ROOT,'frontend/assets/learning.css')});
    await page.addScriptTag({path:path.join(process.env.LEARNING_ROOT,'frontend/assets/learning.js')});
    await page.evaluate(async()=>{HolocronLearning.mount(document.querySelector('#tab-learning'));await HolocronLearning.load();});
    assert.equal(await page.locator('[data-learning="authoring"]').getAttribute('open'),null,'authoring starts collapsed');
    await page.locator('.learning-browse-card summary').first().click();
    assert.equal(await page.getByRole('button',{name:'Edit card',exact:true}).count(),0,'seed is read-only');
    await page.locator('[data-learning="authoring"] > summary').click();
    await page.locator('[data-learning="edit-deck"]').fill('My deck');
    await page.locator('[data-learning="edit-category"]').fill('Custom category');
    await page.locator('[data-learning="edit-prompt"]').fill('<img src=x onerror=alert(1)>');
    await page.locator('[data-learning="edit-answer"]').fill('My answer');
    await page.getByRole('button',{name:'Add card',exact:true}).click();
    await page.getByRole('button',{name:'Edit card',exact:true,includeHidden:true}).waitFor({state:'attached'});
    assert.equal(await page.locator('#tab-learning img').count(),0);
    await page.locator('[data-learning="deck"]').selectOption('My deck');
    await page.locator('[data-learning="category"]').selectOption('Custom category');
    await page.waitForTimeout(200);
    assert.equal(lastBrowse.searchParams.get('deck'),'My deck');
    assert.equal(lastBrowse.searchParams.get('category'),'Custom category');
    await page.locator('.learning-browse-card summary').first().click();
    await page.getByRole('button',{name:'Edit card',exact:true}).click();
    await page.locator('[data-learning="edit-prompt"]').fill('Revised prompt');
    await page.getByRole('button',{name:'Save changes',exact:true}).click();
    await page.getByText('Revised prompt',{exact:true}).waitFor();
    assert.equal(personal.review_count,2,'editing does not send schedule fields');
    await page.locator('.learning-browse-card summary').first().click();
    await page.getByRole('button',{name:'Delete card',exact:true}).click();
    assert.notEqual(personal,null,'delete needs confirmation');
    await page.getByRole('button',{name:'Confirm delete',exact:true}).click();
    await page.getByText('No cards match your filters',{exact:true}).waitFor();
    await page.locator('[data-learning="import-file"]').setInputFiles({name:'cards.json',mimeType:'application/json',buffer:Buffer.from('[]')});
    await page.locator('[data-learning="confirm-import"]').waitFor({state:'visible'});
    assert.equal(imports,0,'preview cannot import');
    assert.equal(previews,1);
    assert.equal(await page.locator('#tab-learning img').count(),0,'preview uses safe DOM');
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'expanded authoring fits mobile');
    await page.locator('[data-learning="confirm-import"]').click();
    await page.getByText('Imported 1 card; skipped 0 duplicates.',{exact:true}).waitFor();
    assert.equal(imports,1);
    await page.locator('[data-learning="import-file"]').setInputFiles({name:'bad.json',mimeType:'application/json',buffer:Buffer.from('invalid')});
    await page.locator('[data-learning="author-error"]').waitFor({state:'visible'});
    assert.equal(await page.locator('[data-learning="confirm-import"]').count(),0,'invalid file revokes previous preview');
    await page.locator('[data-learning="import-file"]').setInputFiles({name:'big.json',mimeType:'application/json',buffer:Buffer.alloc(1024*1024+1)});
    await page.getByText('Import file must be at most 1 MiB.',{exact:true}).waitFor();
    assert.equal(previews,2,'oversized files never leave browser');
    await page.setViewportSize({width:1440,height:1000});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.deepEqual(errors,[]);
    console.log('Personal browser contract passed: CRUD, confirmation, safe DOM, filters, imports, file limits, mobile.');
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
"""

if __name__ == "__main__":
    unittest.main()
