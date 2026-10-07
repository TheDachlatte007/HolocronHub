(() => {
  'use strict';
  let root, query = '', generation = 0, controller, retry;
  const element = (tag, text, className) => {
    const node = document.createElement(tag);
    if (text != null) node.textContent = String(text);
    if (className) node.className = className;
    return node;
  };
  function retryButton() {
    const button = element('button', 'Retry', 'sm');
    button.type = 'button'; button.addEventListener('click', () => load(query));
    return button;
  }
  function render(data) {
    const intro = element('p', 'Chance applies to one reward roll in the listed rotation, not a complete mission. Average reward rolls (1/p) are not a guarantee.', 'farm-note');
    const state = element('p', null, 'farm-note');
    const date = new Date(data.fetched_at);
    state.textContent = (data.stale ? 'Saved snapshot' : 'Drop tables')
      + (data.fetched_at && !Number.isNaN(date.getTime()) ? ' · ' + date.toLocaleString() : '')
      + (data.refreshing ? ' · Updating in background...' : '')
      + (data.total > 40 ? ' · Top 40 of ' + data.total + ' matches; narrow your search.' : '');
    const source = element('a', 'Source: Warframe Community Developers drop tables');
    source.href = 'https://drops.warframestat.us/'; source.target = '_blank'; source.rel = 'noopener noreferrer';
    root.replaceChildren(intro, state, source);
    const rows = Array.isArray(data.items) ? data.items : [];
    if (!rows.length) root.append(element('p', data.loading ? 'Loading mission rewards for the first time...' : 'No mission rewards match this target. Try a part, mod or relic name.', 'muted'));
    const list = element('div', null, 'wf-drop-list');
    for (const row of rows.slice(0, 40)) {
      const card = element('article', null, 'wf-drop-route');
      const copy = element('div');
      copy.append(element('strong', row.item_name), element('p', `${row.planet} / ${row.node} · ${row.game_mode} · Rotation ${row.rotation}`));
      if (row.is_event) copy.append(element('span', 'Event mission', 'wf-drop-event'));
      const metrics = element('div', null, 'wf-drop-chance');
      metrics.append(element('strong', Number(row.chance).toLocaleString(undefined, {maximumFractionDigits: 2}) + '% chance'), element('small', Number(row.expected_reward_checks).toLocaleString(undefined, {maximumFractionDigits: 1}) + ' average reward rolls'));
      const use = element('button', 'Use in journal', 'sm'); use.type = 'button';
      use.addEventListener('click', async () => {
        const details = document.getElementById('wf-journal-details');
        if (details) details.open = true;
        await window.HolocronFarmJournal?.load();
        window.HolocronFarmJournal?.setTarget({name: row.item_name, route: `${row.planet} / ${row.node} / Rotation ${row.rotation}`});
        details?.scrollIntoView({block: 'nearest'});
      });
      card.append(copy, metrics, use); list.append(card);
    }
    root.append(list);
    if (data.error) root.append(element('p', data.error, 'farm-note'), retryButton());
  }
  async function fetchRoutes(token, attempts) {
    const requestController = new AbortController();
    controller = requestController;
    const timeout = window.setTimeout(() => requestController.abort(), 12000);
    try {
      const response = await fetch('/api/warframe/drop-routes?q=' + encodeURIComponent(query), {signal: requestController.signal});
      if (!response.ok) throw Error('Unavailable');
      const data = await response.json();
      if (token !== generation) return;
      render(data);
      if (data.refreshing && attempts < 10) retry = window.setTimeout(() => fetchRoutes(token, attempts + 1), 2000);
      else if (data.refreshing) root.append(element('p', 'Update is still running. Retry in a moment.', 'farm-note'), retryButton());
    } catch (error) {
      if (token !== generation) return;
      root.replaceChildren(element('p', 'Mission drops unavailable. Retry to load saved routes.', 'farm-note'), retryButton());
    } finally { window.clearTimeout(timeout); }
  }
  function load(value) {
    if (!root) return;
    generation++; query = String(value || '').trim();
    window.clearTimeout(retry); controller?.abort();
    root.replaceChildren(element('p', query ? 'Loading mission drop routes...' : 'Search a target to find missions and reward rotations.', 'muted'));
    if (query) void fetchRoutes(generation, 0);
  }
  function mount(target) { root = target; }
  window.HolocronWarframeDrops = {mount, load};
})();
