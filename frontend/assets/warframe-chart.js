/* Timestamp-based charts; market statistics and local snapshots stay separate. */
(() => {
  'use strict';
  const instances = new WeakMap();
  const hour = 3600000;
  const windows = [['6h', 6 * hour], ['24h', 24 * hour], ['7d', 168 * hour], ['30d', 720 * hour]];
  const escape = value => String(value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const price = value => value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  const date = time => new Date(time).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  const clock = time => new Date(time).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false });
  const stamp = time => new Date(time).toLocaleString(undefined, { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit', timeZoneName: 'short' });

  function normalize(rows) {
    const samples = new Map();
    for (const row of Array.isArray(rows) ? rows : []) {
      if (!row || typeof row !== 'object') continue;
      const raw = row.price ?? row.avg_price ?? row.closed_price ?? row.value;
      const at = row.captured_at ?? row.datetime ?? row.date ?? row.timestamp ?? row.updated_at;
      if (raw == null || raw === '' || typeof raw === 'boolean' || at == null || at === '') continue;
      const value = Number(raw);
      const time = typeof at === 'number' ? (at < 1e12 ? at * 1000 : at) : Date.parse(at);
      if (!Number.isFinite(value) || value < 0 || !Number.isFinite(time)) continue;
      samples.set(time, { time, value });
    }
    return [...samples.values()].sort((a, b) => a.time - b.time);
  }

  function ranges(points) {
    const span = points.length > 1 ? points.at(-1).time - points[0].time : 0;
    return [...windows.filter(([, duration]) => duration < span), ['All', 0]];
  }

  function scale(points) {
    const values = points.map(point => point.value);
    const min = Math.min(...values), max = Math.max(...values);
    const padding = Math.max((max - min) * 0.15, max * 0.025, 1);
    const bottom = Math.max(0, min - padding), top = max + padding;
    const rough = (top - bottom) / 4;
    const magnitude = 10 ** Math.floor(Math.log10(rough));
    const step = [1, 2, 2.5, 5, 10].find(n => n * magnitude >= rough) * magnitude;
    const low = Math.floor(bottom / step) * step, high = Math.ceil(top / step) * step;
    const ticks = Array.from({ length: Math.round((high - low) / step) + 1 }, (_, i) => low + i * step);
    return { low, high, ticks };
  }

  function draw(host, state) {
    const focus = document.activeElement;
    const focusedControl = host.contains(focus) ? focus?.dataset?.choice : null;
    const focusedPlot = host.contains(focus) && focus?.classList?.contains('wf-history-svg');
    const sources = Object.keys(state.sources).filter(key => state.sources[key].length);
    if (!sources.includes(state.source)) state.source = sources[0] || 'market';
    const all = state.sources[state.source];
    const choices = ranges(all);
    if (!choices.some(([, duration]) => duration === state.range)) state.range = 0;
    const points = state.range ? all.filter(p => p.time >= all.at(-1).time - state.range) : all;
    const sourceName = key => key === 'market' ? 'Market statistics' : 'Local snapshots';
    host.classList.add('wf-history-chart');
    if (!points.length) {
      host.innerHTML = '<div class="wf-history-empty" role="status">No timestamped price history yet. New snapshots will appear after a successful refresh.</div>';
      return;
    }
    // The SVG coordinate space matches CSS pixels, including after a hidden pane opens.
    const width = Math.max(180, host.clientWidth), height = 250;
    const domain = scale(points);
    const left = Math.max(52, Math.max(...domain.ticks.map(n => price(n).length)) * 7 + 24);
    const right = width - 12, top = 16, bottom = height - 48;
    const start = points[0].time, end = points.at(-1).time;
    const x = time => end === start ? (left + right) / 2 : left + (time - start) / (end - start) * (right - left);
    const y = value => bottom - (value - domain.low) / (domain.high - domain.low) * (bottom - top);
    const coords = points.map(p => `${x(p.time)},${y(p.value)}`).join(' ');
    const yTicks = domain.ticks.map(value => `<line class="wf-history-grid" x1="${left}" x2="${right}" y1="${y(value)}" y2="${y(value)}"/><text x="${left - 9}" y="${y(value) + 4}" text-anchor="end">${escape(price(value))} p</text>`).join('');
    const tickCount = end === start ? 1 : Math.max(2, Math.min(5, Math.floor((right - left) / 110) + 1));
    const xTicks = Array.from({ length: tickCount }, (_, i) => {
      const time = tickCount === 1 ? start : start + (end - start) * i / (tickCount - 1);
      const anchor = tickCount === 1 ? 'middle' : i === 0 ? 'start' : i === tickCount - 1 ? 'end' : 'middle';
      return `<text x="${x(time)}" y="${bottom + 20}" text-anchor="${anchor}">${escape(date(time))}<tspan x="${x(time)}" dy="15">${escape(clock(time))}</tspan></text>`;
    }).join('');
    const markerStride = Math.max(1, Math.ceil(points.length / 100));
    const markers = points.filter((_, index) => index % markerStride === 0 || index === points.length - 1);
    const summary = `${sourceName(state.source)}: ${points.length} ${points.length === 1 ? 'sample' : 'samples'}, ${price(points[0].value)} to ${price(points.at(-1).value)} platinum. ${stamp(start)} to ${stamp(end)}.`;
    host.innerHTML = `
      <div class="wf-history-controls">
        <div role="group" aria-label="History source">${sources.map(key => `<button type="button" data-choice="${key}" data-source="${key}" aria-pressed="${state.source === key}">${sourceName(key)}</button>`).join('')}</div>
        <div role="group" aria-label="History range ending at latest sample">${choices.map(([label, duration]) => `<button type="button" data-choice="${duration}" data-range="${duration}" aria-pressed="${state.range === duration}" title="${duration ? 'Last ' + label + ' ending at latest recorded sample' : 'All available samples'}">${label}</button>`).join('')}</div>
      </div>
      <div class="wf-history-plot">
        <svg class="wf-history-svg" viewBox="0 0 ${width} ${height}" role="group" tabindex="0" aria-label="${escape(summary)} Use Left and Right arrows to inspect samples; Home and End for first and last; Escape to dismiss.">
          ${yTicks}${xTicks}
          ${points.length > 1 ? `<polygon points="${x(start)},${bottom} ${coords} ${x(end)},${bottom}" fill="rgba(103,232,249,.045)"/><polyline class="wf-history-line" points="${coords}"/>` : ''}
          ${markers.map(p => `<circle class="wf-history-dot" cx="${x(p.time)}" cy="${y(p.value)}" r="${points.length === 1 ? 3 : 1.6}"/>`).join('')}
          <g class="wf-history-inspector" visibility="hidden" aria-hidden="true"><line class="wf-history-crosshair" y1="${top}" y2="${bottom}"/><circle class="wf-history-dot" r="3.5"/></g>
        </svg>
        <div class="wf-history-tip" hidden></div>
      </div>
      <div class="wf-history-caption">${escape(sourceName(state.source))} / ${points.length} recorded ${points.length === 1 ? 'sample (no trend yet)' : 'samples'} / ${state.range ? 'Window ends at latest sample. ' : ''}Local time. Hover, tap or use arrow keys.</div>
      <div class="wf-history-caption wf-history-readout" role="status" aria-live="polite">Latest: ${escape(price(points.at(-1).value))} p / ${escape(stamp(end))}</div>`;

    host.querySelectorAll('[data-source]').forEach(button => button.addEventListener('click', () => {
      state.source = button.dataset.source;
      state.range = 0;
      state.index = null;
      draw(host, state);
    }));
    host.querySelectorAll('[data-range]').forEach(button => button.addEventListener('click', () => {
      state.range = Number(button.dataset.range);
      state.index = null;
      draw(host, state);
    }));
    const svg = host.querySelector('svg');
    const inspector = host.querySelector('.wf-history-inspector');
    const crosshair = inspector.querySelector('line');
    const dot = inspector.querySelector('circle');
    const tip = host.querySelector('.wf-history-tip');
    const readout = host.querySelector('.wf-history-readout');
    function inspect(index, announce = false) {
      state.index = Math.max(0, Math.min(points.length - 1, index));
      const point = points[state.index];
      const px = x(point.time), py = y(point.value);
      inspector.setAttribute('visibility', 'visible');
      crosshair.setAttribute('x1', px);
      crosshair.setAttribute('x2', px);
      dot.setAttribute('cx', px);
      dot.setAttribute('cy', py);
      const label = `${price(point.value)} platinum\n${stamp(point.time)}`;
      tip.textContent = label;
      tip.hidden = false;
      tip.style.left = `${Math.max(8, Math.min(width - tip.offsetWidth - 8, px + 12))}px`;
      if (announce) readout.textContent = label;
    }
    function dismiss() {
      inspector.setAttribute('visibility', 'hidden');
      tip.hidden = true;
    }
    function pointer(event) {
      const box = svg.getBoundingClientRect();
      const px = (event.clientX - box.left) * width / box.width;
      let nearest = 0;
      points.forEach((p, i) => { if (Math.abs(x(p.time) - px) < Math.abs(x(points[nearest].time) - px)) nearest = i; });
      inspect(nearest, event.type === 'pointerdown');
    }
    svg.addEventListener('pointermove', pointer);
    svg.addEventListener('pointerdown', pointer);
    svg.addEventListener('pointerleave', dismiss);
    svg.addEventListener('pointercancel', dismiss);
    svg.addEventListener('blur', dismiss);
    svg.addEventListener('focus', () => inspect(state.index ?? points.length - 1, true));
    svg.addEventListener('keydown', event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End', 'Escape'].includes(event.key)) return;
      event.preventDefault();
      if (event.key === 'Escape') return dismiss();
      const index = state.index ?? points.length - 1;
      inspect(event.key === 'Home' ? 0 : event.key === 'End' ? points.length - 1 : index + (event.key === 'ArrowLeft' ? -1 : 1), true);
    });
    if (focusedControl != null) host.querySelector(`[data-choice="${focusedControl}"]`)?.focus({ preventScroll: true });
    if (focusedPlot) svg.focus({ preventScroll: true });
  }

  function render(host, market, key = '') {
    if (!host) return;
    let state = instances.get(host);
    if (!state) {
      state = { source: 'market', range: 0, key, width: host.clientWidth, index: null };
      instances.set(host, state);
      const observer = new ResizeObserver(() => {
        if (host.clientWidth === state.width || !host.clientWidth) return;
        state.width = host.clientWidth;
        if (state.sources) draw(host, state);
      });
      observer.observe(host);
    }
    if (state.key !== key) {
      state.source = 'market';
      state.range = 0;
      state.index = null;
      state.key = key;
    }
    state.sources = { market: normalize(market.history), local: normalize(market.local_history?.series) };
    draw(host, state);
  }

  window.WarframeChart = { render, normalize, ranges, scale };
})();
