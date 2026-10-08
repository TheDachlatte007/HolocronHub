(function () {
  'use strict';

  const API = '/api/dashboard/jellyfin';
  const ID = /^[0-9a-f]{32}$/;
  let root, ui, pending, lastGood, lastResponse;
  let active=false, livePending, liveController, resumeController, liveData, pollTimer, expiryTimer, epoch=0, retryAt=0;

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  }

  function webLink(value, itemId) {
    try {
      const url = new URL(value);
      if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search) return null;
      if (!ID.test(itemId) || url.hash !== `#!/details?id=${itemId}`) return null;
      return url.href;
    } catch (_) {
      return null;
    }
  }

  function minutes(value) {
    return Number.isFinite(value) && value >= 0 ? Math.floor(value / 60) : 0;
  }

  function render(data) {
    if (!ui) return;
    ui.items.replaceChildren();
    ui.status.className = 'jellyfin-status';
    const state = data.state;
    const setup = state === 'disabled' || state === 'unconfigured';
    ui.settings.hidden = !setup;
    const messages = {
      idle: 'Load your Continue Watching list from Jellyfin.',
      disabled: 'Jellyfin is disabled. Enable it in Settings.',
      unconfigured: 'Add your Jellyfin server, API key and user ID in Settings.',
      unavailable: 'Jellyfin is unavailable. Try again later or check Settings.',
      empty: 'Nothing to continue watching.',
      ready: 'Your next watch, ready when you are.',
      stale: 'Jellyfin is unavailable. Showing the last saved snapshot.'
    };
    ui.status.textContent = data.error_code === 'invalid_user_id'
      ? 'Enter the Jellyfin user ID (UUID), not the username. Find it in Jellyfin Dashboard > Users.'
      : messages[state] || messages.unavailable;
    if (data.stale || state === 'stale') ui.status.classList.add('jellyfin-stale');
    if (Number.isFinite(data.updated_at) && ['stale', 'ready', 'empty'].includes(state)) {
      const date = new Date(data.updated_at * 1000);
      if (!Number.isNaN(date.getTime())) {
        ui.status.append(element('span', 'jellyfin-saved', `Updated ${date.toLocaleString([], {
          month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        })}`));
      }
    }
    if (setup) {stopLive();liveController=livePending=null;liveData={state,items:[],error_code:data.error_code};renderLive(liveData);updateBusy();}
    if (setup || state === 'unavailable') return;
    renderItems(ui.items, data.items, false);
  }

  function timestamp(value) {
    const seconds=Math.floor(Number.isFinite(value)?Math.max(0,value):0);
    return `${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')}`;
  }

  function renderItems(target, values, live) {
    target.replaceChildren();
    const items = Array.isArray(values) ? values.slice(0, 6) : [];
    items.forEach(item => {
      if (!item || !ID.test(item.id)) return;
      const href = webLink(item.web_url, item.id);
      const row = element(href ? 'a' : 'div', 'jellyfin-item' + (live?' jellyfin-playing-item':''));
      if (href) {
        row.href = href;
        row.target = '_blank';
        row.rel = 'noopener noreferrer';
        row.setAttribute('aria-label', `Open ${item.title || 'item'} in Jellyfin`);
      }
      // Only the same-origin, ID-bound proxy is allowed to supply artwork.
      if (item.image_url === `${API}/items/${item.id}/thumbnail`) {
        const image = element('img', 'jellyfin-art');
        image.alt = '';
        image.loading = 'lazy';
        image.width = 40;
        image.height = 60;
        image.referrerPolicy = 'no-referrer';
        image.addEventListener('error', () => { image.hidden = true; }, {once: true});
        image.src = item.image_url;
        row.append(image);
      }
      const copy = element('div', 'jellyfin-copy');
      copy.append(element('strong', 'jellyfin-title', item.title || 'Untitled item'));
      if (item.summary) copy.append(element('span', 'jellyfin-summary', item.summary));
      if(live){
        const state=item.playback_state==='paused'?'Paused':item.playback_state==='playing'?'Playing':'Status unknown';
        const details=element('div','jellyfin-device');
        const badge=element('span','jellyfin-playback-badge',state);badge.dataset.state=item.playback_state;
        details.append(badge,element('span','',item.device||'Unknown device'));
        if(item.client)details.append(element('span','jellyfin-client',item.client));
        copy.append(details);
      }
      const value = Number.isFinite(item.progress_percent) ? Math.max(0, Math.min(100, item.progress_percent)) : 0;
      const progress = element('progress', 'jellyfin-progress');
      progress.max = 100;
      progress.value = value;
      progress.setAttribute('aria-label', `${item.title || 'Item'} watch progress`);
      const timing = item.duration_seconds > 0
        ? ` | ${minutes(item.position_seconds)} / ${minutes(item.duration_seconds)} min` : '';
      copy.append(progress, element('span', 'jellyfin-timing', live
        ? `${timestamp(item.position_seconds)}${item.duration_seconds>0?' / '+timestamp(item.duration_seconds):' played'} | ${Math.round(value)}%`
        : `${Math.round(value)}% watched${timing}`));
      row.append(copy);
      if (href) row.append(element('span', 'jellyfin-open', 'Open'));
      target.append(row);
    });
  }

  function renderLive(data) {
    if(!ui)return;
    ui.liveItems.replaceChildren();
    const messages={idle:'Playback is checked when this dashboard is visible.',loading:'Checking current playback...',
      ready:'Your current sessions. Read-only.',empty:'Nothing is playing for your configured user.',
      stale:'Playback status is out of date. Waiting for a fresh check.',
      unavailable:'Playback status unavailable. Continue Watching is independent.',
      disabled:'Enable Jellyfin in Settings to see current playback.',unconfigured:'Configure Jellyfin in Settings to see current playback.'};
    ui.liveStatus.textContent=data.error_code==='invalid_user_id'?'Enter the Jellyfin user ID (UUID), not the username. Find it in Jellyfin Dashboard > Users.'
      :data.error_code==='permission_denied'?'Jellyfin denied session access. Check API key permissions in Settings.'
      :data.error_code==='rate_limited'?'Jellyfin rate limit reached. Retrying later.':messages[data.state]||messages.unavailable;
    ui.liveStatus.classList.toggle('jellyfin-stale',['unavailable','stale'].includes(data.state));
    if(data.state==='ready')renderItems(ui.liveItems,data.items,true);
  }

  function mount(target = document.getElementById('home-jellyfin-content')) {
    const next = typeof target === 'string' ? document.querySelector(target) : target;
    if (!next || typeof next.replaceChildren !== 'function') return null;
    if (next === root && ui && root.contains(ui.section)) return root;
    root = next;
    const section = element('section', 'jellyfin-dashboard');
    section.setAttribute('aria-label', 'Jellyfin media');
    const head = element('div', 'jellyfin-head');
    const heading = element('div', 'jellyfin-heading');
    heading.append(element('span', 'jellyfin-kicker', 'JELLYFIN'), element('h3', '', 'Media'));
    const refresh = element('button', 'jellyfin-refresh', 'Refresh');
    refresh.type = 'button';
    refresh.setAttribute('aria-label', 'Refresh Jellyfin');
    refresh.addEventListener('click', () => { load(true);loadSessions(true); });
    head.append(heading, refresh);
    const status = element('p', 'jellyfin-status');
    status.setAttribute('role', 'status');
    const settings = element('a', 'jellyfin-settings', 'Open Settings');
    settings.href = '#settings';
    const items = element('div', 'jellyfin-items');
    const now=element('section','jellyfin-now');now.setAttribute('aria-label','Now Playing');
    const liveStatus=element('p','jellyfin-now-status');liveStatus.setAttribute('role','status');
    const liveItems=element('div','jellyfin-now-items');
    now.append(element('h4','','Now Playing'),liveStatus,liveItems);
    const resume=element('section','jellyfin-resume');resume.setAttribute('aria-label','Continue Watching');
    resume.append(element('h4','','Continue Watching'),status,settings,items);
    section.append(head,now,resume);
    root.replaceChildren(section);
    ui = {section, status, settings, items, refresh, liveStatus, liveItems};
    section.setAttribute('aria-busy', pending ? 'true' : 'false');
    refresh.disabled = Boolean(pending);
    render(lastResponse || {state: 'idle', items: []});
    renderLive(liveData||{state:'idle',items:[]});
    return root;
  }

  function load(force = false) {
    if (!ui && !mount()) return Promise.resolve(null);
    if (pending) return pending;
    ui.section.setAttribute('aria-busy', 'true');
    ui.refresh.disabled = true;
    ui.status.textContent = 'Loading Continue Watching...';
    const controller = new AbortController();
    const generation=epoch;resumeController=controller;
    const timeout = window.setTimeout(() => controller.abort(), 12000);
    pending = (async () => {
      try {
        const response = await fetch(`${API}${force ? '?force=true' : ''}`, {
          method: 'GET', signal: controller.signal, credentials: 'same-origin', cache: 'no-store'
        });
        if (!response.ok) throw new Error('Unavailable');
        const data = await response.json();
        if(generation!==epoch)return null;
        if (!data || !['ready', 'empty', 'stale', 'disabled', 'unconfigured', 'unavailable'].includes(data.state)
            || !Array.isArray(data.items)) throw new Error('Invalid dashboard');
        lastGood = ['ready', 'empty', 'stale'].includes(data.state) ? data : null;
        lastResponse = data;
      } catch (_) {
        if(generation!==epoch)return null;
        lastResponse = lastGood ? {...lastGood, state: 'stale', stale: true} : {state: 'unavailable', items: []};
      } finally {
        window.clearTimeout(timeout);
        if(generation===epoch){pending = null;resumeController=null;updateBusy();}
      }
      if(generation===epoch)render(lastResponse);
      return lastResponse;
    })();
    return pending;
  }

  function updateBusy(){if(ui){ui.refresh.disabled=Boolean(pending||livePending);ui.section.setAttribute('aria-busy',String(Boolean(pending||livePending)));}}
  function visible(){return active && !document.hidden && root?.isConnected && !root.closest('[data-dashboard-tile]')?.hidden;}
  function stopLive(){window.clearTimeout(pollTimer);window.clearTimeout(expiryTimer);pollTimer=expiryTimer=null;liveController?.abort();}
  function schedule(){
    window.clearTimeout(pollTimer);
    if(!visible()||['disabled','unconfigured'].includes(liveData?.state))return;
    pollTimer=window.setTimeout(()=>{if(visible())loadSessions();},Math.max(20000,retryAt-Date.now()));
  }
  function loadSessions(force=false){
    if(!ui&&!mount())return Promise.resolve(null);
    if(livePending)return livePending;
    window.clearTimeout(pollTimer);
    if(!liveData||liveData.state!=='ready')renderLive({state:'loading'});
    const generation=epoch,controller=new AbortController();liveController=controller;
    const timeout=window.setTimeout(()=>controller.abort(),12000);
    livePending=(async()=>{
      try{
        const response=await fetch(`${API}/sessions${force?'?force=true':''}`,{signal:controller.signal,credentials:'same-origin',cache:'no-store'});
        if(!response.ok)throw Error('Unavailable');
        const data=await response.json();
        if(generation!==epoch||controller!==liveController)return null;
        if(!data||!['ready','empty','disabled','unconfigured','unavailable'].includes(data.state)||!Array.isArray(data.items))throw Error('Invalid sessions');
        liveData=data;
        retryAt=Date.now()+Math.min(300,Math.max(0,Number(data.retry_after_seconds)||0))*1000;
        window.clearTimeout(expiryTimer);
        if(['ready','empty'].includes(data.state)){
          const ttl=Math.min(30,Math.max(0,Number(data.fresh_for_seconds)||0))*1000;
          if(!ttl)liveData={state:'stale',items:[]};
          else expiryTimer=window.setTimeout(()=>{liveData={state:'stale',items:[]};renderLive(liveData);},ttl);
        }
      }catch(_){
        if(generation!==epoch||controller!==liveController)return null;
        window.clearTimeout(expiryTimer);liveData={state:'unavailable',items:[]};retryAt=Date.now()+60000;
      }finally{
        window.clearTimeout(timeout);
        if(generation===epoch&&controller===liveController){livePending=null;liveController=null;updateBusy();schedule();}
      }
      renderLive(liveData);return liveData;
    })();
    updateBusy();return livePending;
  }
  function syncActivity(){
    stopLive();
    liveController=null;livePending=null;liveData=null;
    renderLive({state:'idle',items:[]});updateBusy();
    if(visible())loadSessions();
  }
  function setActive(value){const changed=active!==Boolean(value);active=Boolean(value);if(changed)syncActivity();}
  function reset(){epoch++;stopLive();resumeController?.abort();pending=livePending=resumeController=liveController=null;
    lastGood=lastResponse=liveData=null;retryAt=0;render({state:'idle',items:[]});renderLive({state:'idle',items:[]});updateBusy();}
  document.addEventListener('visibilitychange',syncActivity);
  window.HolocronJellyfinDashboard = {mount, load, loadSessions, setActive, reset};
})();
