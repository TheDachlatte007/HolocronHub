(function () {
  'use strict';

  const API = '/api/dashboard/jellyfin';
  const ID = /^[0-9a-f]{32}$/;
  let root, ui, pending, lastGood, lastResponse;

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
    ui.status.textContent = messages[state] || messages.unavailable;
    if (data.stale || state === 'stale') ui.status.classList.add('jellyfin-stale');
    if (Number.isFinite(data.updated_at) && ['stale', 'ready', 'empty'].includes(state)) {
      const date = new Date(data.updated_at * 1000);
      if (!Number.isNaN(date.getTime())) {
        ui.status.append(element('span', 'jellyfin-saved', `Updated ${date.toLocaleString([], {
          month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
        })}`));
      }
    }
    if (setup || state === 'unavailable') return;
    const items = Array.isArray(data.items) ? data.items.slice(0, 6) : [];
    items.forEach(item => {
      if (!item || !ID.test(item.id)) return;
      const href = webLink(item.web_url, item.id);
      const row = element(href ? 'a' : 'div', 'jellyfin-item');
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
      const value = Number.isFinite(item.progress_percent) ? Math.max(0, Math.min(100, item.progress_percent)) : 0;
      const progress = element('progress', 'jellyfin-progress');
      progress.max = 100;
      progress.value = value;
      progress.setAttribute('aria-label', `${item.title || 'Item'} watch progress`);
      const timing = item.duration_seconds > 0
        ? ` | ${minutes(item.position_seconds)} / ${minutes(item.duration_seconds)} min` : '';
      copy.append(progress, element('span', 'jellyfin-timing', `${Math.round(value)}% watched${timing}`));
      row.append(copy);
      if (href) row.append(element('span', 'jellyfin-open', 'Open'));
      ui.items.append(row);
    });
  }

  function mount(target = document.getElementById('home-jellyfin-content')) {
    const next = typeof target === 'string' ? document.querySelector(target) : target;
    if (!next || typeof next.replaceChildren !== 'function') return null;
    if (next === root && ui && root.contains(ui.section)) return root;
    root = next;
    const section = element('section', 'jellyfin-dashboard');
    section.setAttribute('aria-label', 'Jellyfin Continue Watching');
    const head = element('div', 'jellyfin-head');
    const heading = element('div', 'jellyfin-heading');
    heading.append(element('span', 'jellyfin-kicker', 'JELLYFIN'), element('h3', '', 'Continue Watching'));
    const refresh = element('button', 'jellyfin-refresh', 'Refresh');
    refresh.type = 'button';
    refresh.setAttribute('aria-label', 'Refresh Jellyfin');
    refresh.addEventListener('click', () => { load(true); });
    head.append(heading, refresh);
    const status = element('p', 'jellyfin-status');
    status.setAttribute('role', 'status');
    const settings = element('a', 'jellyfin-settings', 'Open Settings');
    settings.href = '#settings';
    const items = element('div', 'jellyfin-items');
    section.append(head, status, settings, items);
    root.replaceChildren(section);
    ui = {section, status, settings, items, refresh};
    section.setAttribute('aria-busy', pending ? 'true' : 'false');
    refresh.disabled = Boolean(pending);
    render(lastResponse || {state: 'idle', items: []});
    return root;
  }

  function load(force = false) {
    if (!ui && !mount()) return Promise.resolve(null);
    if (pending) return pending;
    ui.section.setAttribute('aria-busy', 'true');
    ui.refresh.disabled = true;
    ui.status.textContent = 'Loading Continue Watching...';
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 12000);
    pending = (async () => {
      try {
        const response = await fetch(`${API}${force ? '?force=true' : ''}`, {
          method: 'GET', signal: controller.signal, credentials: 'same-origin', cache: 'no-store'
        });
        if (!response.ok) throw new Error('Unavailable');
        const data = await response.json();
        if (!data || !['ready', 'empty', 'stale', 'disabled', 'unconfigured', 'unavailable'].includes(data.state)
            || !Array.isArray(data.items)) throw new Error('Invalid dashboard');
        lastGood = ['ready', 'empty', 'stale'].includes(data.state) ? data : null;
        lastResponse = data;
      } catch (_) {
        lastResponse = lastGood ? {...lastGood, state: 'stale', stale: true} : {state: 'unavailable', items: []};
      } finally {
        window.clearTimeout(timeout);
        pending = null;
        ui.section.setAttribute('aria-busy', 'false');
        ui.refresh.disabled = false;
      }
      render(lastResponse);
      return lastResponse;
    })();
    return pending;
  }

  window.HolocronJellyfinDashboard = {mount, load};
})();
