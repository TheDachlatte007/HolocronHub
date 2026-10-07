(function () {
  'use strict';

  const API = '/api/learning';
  const CATEGORIES = ['Measurement & Data', 'Engineering Practice', 'Sustainable Systems', 'Academic Communication'];
  const RATINGS = ['Again', 'Hard', 'Good', 'Easy'];
  let root, ui, searchTimer;
  let summaryVersion = 0, browseVersion = 0;
  const state = {cards: [], index: 0, revealed: false, started: false, sessionBusy: false, reviewBusy: false, reviewed: 0, lastDue: null};
  const author = {id: null, busy: false, preview: null, fileVersion: 0};
  const EDIT_FIELDS = ['deck', 'category', 'skill', 'prompt', 'answer', 'example', 'explanation', 'source', 'tags'];

  // All API and user text is inserted as text, never parsed as markup.
  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = String(text);
    return node;
  }
  function named(node, name) { node.dataset.learning = name; return node; }
  function button(text, action, className = '') {
    const node = element('button', className, text);
    node.type = 'button';
    node.addEventListener('click', action);
    return node;
  }
  function select(label, values, name) {
    const field = element('label', 'learning-field');
    field.append(element('span', '', label));
    const control = named(element('select'), name);
    values.forEach(([value, text]) => {
      const option = element('option', '', text);
      option.value = value;
      control.append(option);
    });
    field.append(control);
    return {field, control};
  }
  function errorPanel(name) {
    const panel = named(element('div', 'learning-error'), name);
    panel.setAttribute('role', 'alert');
    panel.hidden = true;
    return panel;
  }
  function showError(panel, message, retry) {
    panel.replaceChildren(element('span', '', message));
    if (retry) panel.append(button('Try again', retry));
    panel.hidden = false;
  }
  function message(title, text) {
    const panel = element('div', 'learning-state');
    panel.append(element('h3', '', title), element('p', '', text));
    return panel;
  }
  function dueText(value) {
    if (!value) return 'Not reviewed yet';
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? 'Schedule unavailable' : new Intl.DateTimeFormat('en-GB', {month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'}).format(date);
  }
  async function request(endpoint, options = {}) {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(`${API}${endpoint}`, {...options, signal: controller.signal});
      if (!response.ok) {
        const error = await response.json().catch(() => null);
        throw new Error(typeof error?.detail === 'string' ? error.detail : `Request failed (${response.status}).`);
      }
      return await response.json();
    } catch (error) {
      if (error.name === 'AbortError') throw new Error('The request timed out. Check your connection.');
      throw error;
    } finally { window.clearTimeout(timeout); }
  }
  function validateCards(value) {
    if (!Array.isArray(value) || !value.every(card => card && typeof card.id === 'string' && typeof card.prompt === 'string' && typeof card.answer === 'string')) {
      throw new Error('The server returned an invalid card list.');
    }
    return value;
  }

  function mount(target) {
    if (!target || target === root) return;
    // The workspace has one shell-owned root for the lifetime of the page.
    if (root) return;
    root = target;
    root.lang = 'en';
    const heading = element('header', 'learning-hero');
    const copy = element('div');
    copy.append(element('p', 'learning-kicker', 'LEARNING / ENGINEERING ENGLISH'), element('h1', '', 'Build fluency. One card at a time.'), element('p', 'learning-intro', 'Precise language for the work you do. A little practice, every day.'));
    heading.append(copy, element('span', 'learning-deck-badge', 'English only · Spaced practice'));

    const metrics = named(element('div', 'learning-metrics'), 'metrics');
    metrics.setAttribute('aria-label', 'Learning progress');
    const summaryError = errorPanel('summary-error');
    const categories = element('div', 'learning-categories');
    const overview = element('section', 'learning-overview');
    overview.append(metrics, summaryError, categories);

    const study = element('section', 'learning-panel learning-study');
    study.setAttribute('aria-label', 'Study session');
    const sectionHead = element('div', 'learning-section-head');
    sectionHead.append(element('h2', '', 'Your daily practice'), element('span', 'learning-muted', 'Due cards first, then new cards'));
    const category = select('Category', [['', 'All categories'], ...CATEGORIES.map(value => [value, value])], 'category');
    const deck = select('Deck', [['', 'All decks']], 'deck');
    const length = select('Session length', [['5', '5 cards'], ['10', '10 cards'], ['20', '20 cards'], ['30', '30 cards']], 'length');
    length.control.value = '20';
    const start = named(button('Start session', startSession, 'learning-primary'), 'start');
    const controls = element('div', 'learning-session-controls');
    controls.append(deck.field, category.field, length.field, start);
    const studyError = errorPanel('study-error');
    const studyBody = named(element('div', 'learning-study-body'), 'study');
    studyBody.setAttribute('aria-live', 'polite');
    studyBody.setAttribute('aria-atomic', 'true');
    study.append(sectionHead, controls, studyError, studyBody);

    const browse = element('section', 'learning-panel learning-browse');
    browse.setAttribute('aria-label', 'Browse cards');
    const browseHead = element('div', 'learning-section-head');
    const browseCount = element('span', 'learning-muted');
    browseHead.append(element('h2', '', 'Explore the deck'), browseCount);
    const searchField = element('label', 'learning-field learning-search');
    searchField.append(element('span', '', 'Search cards'));
    const search = named(element('input'), 'search');
    search.type = 'search';
    search.placeholder = 'Search a term, prompt, or example…';
    searchField.append(search);
    const status = select('Scheduling status', [['', 'All statuses'], ['new', 'New'], ['due', 'Due'], ['scheduled', 'Scheduled'], ['mastered', 'Mastered']], 'status');
    const browseFilters = element('div', 'learning-browse-filters');
    browseFilters.append(searchField, status.field);
    const browseError = errorPanel('browse-error');
    const browseBody = named(element('div', 'learning-browse-list'), 'browse');
    browseBody.setAttribute('aria-live', 'polite');
    browse.append(browseHead, browseFilters, browseError, browseBody);

    ui = {metrics, categories, summaryError, category: category.control, deck: deck.control, length: length.control, start, studyError, studyBody, search, status: status.control, browseError, browseBody, browseCount};
    root.replaceChildren(heading, overview, study, buildAuthoring(), browse);
    ui.category.addEventListener('change', loadBrowse);
    ui.deck.addEventListener('change', loadBrowse);
    ui.status.addEventListener('change', loadBrowse);
    ui.search.addEventListener('input', () => {
      // Invalidate in-flight results as soon as the query changes, before debounce.
      browseVersion++;
      window.clearTimeout(searchTimer);
      searchTimer = window.setTimeout(loadBrowse, 250);
    });
    document.addEventListener('keydown', onKeydown);
    renderStudy();
  }

  async function loadSummary() {
    const version = ++summaryVersion;
    ui.summaryError.hidden = true;
    ui.metrics.setAttribute('aria-busy', 'true');
    if (!ui.metrics.children.length) ui.metrics.append(element('p', 'learning-muted', 'Loading your progress…'));
    try {
      const summary = await request('/summary');
      const fields = ['total_cards', 'new_cards', 'due_cards', 'mastered_cards', 'reviews_today', 'streak_days'];
      if (!summary || !Array.isArray(summary.categories) || !fields.every(key => Number.isFinite(summary[key]))) throw new Error('The server returned invalid progress.');
      if (version !== summaryVersion) return;
      const metrics = [['Due now', summary.due_cards], ['New cards', summary.new_cards], ['Reviews today', summary.reviews_today], ['Day streak', summary.streak_days]];
      ui.metrics.replaceChildren(...metrics.map(([label, value]) => {
        const metric = element('div', 'learning-metric');
        metric.append(element('strong', '', value), element('span', '', label));
        return metric;
      }));
      const total = element('p', 'learning-collection-note', `${summary.total_cards} cards in your deck · ${summary.mastered_cards} mastered`);
      const rows = summary.categories.map(row => {
        const item = element('div', 'learning-category');
        const head = element('div', 'learning-category-head');
        head.append(element('span', '', row.category), element('small', '', `${row.reviewed_cards} / ${row.total_cards} explored`));
        const progress = element('progress');
        progress.max = Math.max(1, Number(row.total_cards) || 1);
        progress.value = Number(row.reviewed_cards) || 0;
        progress.setAttribute('aria-label', `${row.category} cards explored`);
        item.append(head, progress, element('small', 'learning-muted', `${row.due_cards} due`));
        return item;
      });
      ui.categories.replaceChildren(total, ...rows);
      updateFilter(ui.category, 'All categories', [...CATEGORIES, ...summary.categories.map(row => row.category)]);
      updateFilter(ui.deck, 'All decks', (summary.decks || []).map(row => row.deck));
      [['deck', summary.decks || []], ['category', summary.categories]].forEach(([field, values]) => {
        ui.suggestions[field].replaceChildren(...values.map(row => {
          const option = element('option'); option.value = row[field]; return option;
        }));
      });
    } catch (error) {
      if (version === summaryVersion) showError(ui.summaryError, `Could not load progress. ${error.message}`, loadSummary);
    } finally {
      if (version === summaryVersion) ui.metrics.setAttribute('aria-busy', 'false');
    }
  }

  function syncControls() {
    const busy = state.sessionBusy || state.reviewBusy || author.busy;
    ui.start.disabled = busy;
    ui.category.disabled = busy;
    ui.deck.disabled = busy;
    ui.length.disabled = busy;
    ui.start.textContent = state.sessionBusy ? 'Loading session…' : state.index < state.cards.length ? 'Continue session' : 'Start session';
    ui.studyBody.setAttribute('aria-busy', String(busy));
    if (ui.authoring) {
      ui.authoring.querySelectorAll('input, textarea, button').forEach(node => { node.disabled = busy; });
      root.querySelectorAll('[data-personal-action]').forEach(node => { node.disabled = busy; });
    }
  }
  function focusStudy() { ui.studyBody.querySelector('[data-learning="reveal"], [data-rating="3"], h3')?.focus({preventScroll: true}); }
  async function startSession() {
    if (state.sessionBusy || state.reviewBusy || author.busy) return;
    if (state.index < state.cards.length) { ui.studyBody.scrollIntoView({behavior: 'smooth', block: 'nearest'}); focusStudy(); return; }
    state.sessionBusy = true;
    ui.studyError.hidden = true;
    syncControls();
    ui.studyBody.replaceChildren(message('Preparing your session', 'Finding due and new cards…'));
    try {
      const params = new URLSearchParams({limit: ui.length.value, category: ui.category.value, deck: ui.deck.value});
      const cards = validateCards(await request(`/session?${params}`));
      Object.assign(state, {cards, index: 0, revealed: false, started: true, reviewed: 0, lastDue: null});
      renderStudy();
      focusStudy();
    } catch (error) {
      ui.studyBody.replaceChildren(message('Session unavailable', 'Your progress is safe. Try loading a session again.'));
      showError(ui.studyError, `Could not start the session. ${error.message}`, startSession);
    } finally { state.sessionBusy = false; syncControls(); }
  }
  function renderStudy() {
    syncControls();
    const card = state.cards[state.index];
    if (!card) {
      let title = 'A few minutes of focused practice', text = 'Choose a category and session length, then start when you are ready.';
      if (state.started && state.reviewed) { title = 'Session complete'; text = `${state.reviewed} ${state.reviewed === 1 ? 'card' : 'cards'} reviewed`; }
      else if (state.started) { title = 'You are all caught up'; text = 'No due or new cards in this category. Try another category or come back later.'; }
      const panel = message(title, text);
      panel.querySelector('h3').tabIndex = -1;
      if (state.lastDue) panel.append(element('p', 'learning-muted', `Last card next due: ${dueText(state.lastDue)}`));
      ui.studyBody.replaceChildren(panel);
      return;
    }
    const paper = element('article', 'learning-flashcard');
    const meta = element('div', 'learning-card-meta');
    meta.append(element('span', 'learning-chip', card.category), element('span', 'learning-muted', `Card ${state.index + 1} of ${state.cards.length} · ${card.status === 'due' ? 'Due' : 'New'}`));
    paper.append(meta, element('p', 'learning-skill', card.skill), element('h3', 'learning-prompt', card.prompt));
    const answer = named(element('div', 'learning-answer'), 'answer');
    answer.hidden = !state.revealed;
    answer.append(element('p', 'learning-kicker', 'ANSWER'), element('p', 'learning-answer-main', card.answer), element('p', 'learning-kicker', 'IN CONTEXT'), element('p', 'learning-example', card.example), element('p', 'learning-explanation', card.explanation));
    paper.append(answer);
    if (!state.revealed) {
      paper.append(named(button('Reveal answer', reveal, 'learning-primary learning-reveal'), 'reveal'), element('p', 'learning-shortcuts', 'Think it through, then reveal. Keyboard: Space'));
    } else {
      paper.append(element('p', 'learning-rating-label', 'How well did you recall it?'));
      const ratings = element('div', 'learning-ratings');
      RATINGS.forEach((label, index) => {
        const control = button(label, () => rate(index + 1), `learning-rating learning-rating-${index + 1}`);
        control.dataset.rating = String(index + 1);
        control.disabled = state.reviewBusy || author.busy;
        control.append(element('small', '', `${index + 1}`));
        ratings.append(control);
      });
      paper.append(ratings, element('p', 'learning-shortcuts', state.reviewBusy ? 'Saving your review…' : 'Keyboard: 1 Again · 2 Hard · 3 Good · 4 Easy'));
    }
    ui.studyBody.replaceChildren(paper);
  }
  function reveal() {
    if (state.revealed || state.sessionBusy || state.reviewBusy || author.busy || !state.cards[state.index]) return;
    state.revealed = true;
    renderStudy();
    focusStudy();
  }
  async function rate(rating) {
    if (!state.revealed || state.reviewBusy || state.sessionBusy || author.busy || !state.cards[state.index]) return;
    const card = state.cards[state.index];
    state.reviewBusy = true;
    ui.studyError.hidden = true;
    renderStudy();
    try {
      const progress = await request('/reviews', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({card_id: card.id, rating})});
      if (!progress || progress.card_id !== card.id || typeof progress.due_at !== 'string' || !Number.isFinite(progress.review_count)) throw new Error('The server did not confirm this review.');
      state.lastDue = progress.due_at;
      state.index++;
      state.reviewed++;
      state.revealed = false;
      // Refresh side panels separately; an overview failure cannot undo a saved review.
      loadSummary();
      loadBrowse();
    } catch (error) {
      showError(ui.studyError, `Review was not confirmed. ${error.message} Check your connection before retrying.`);
    } finally {
      state.reviewBusy = false;
      renderStudy();
      focusStudy();
    }
  }

  async function loadBrowse() {
    window.clearTimeout(searchTimer);
    const version = ++browseVersion;
    ui.browseError.hidden = true;
    ui.browseBody.setAttribute('aria-busy', 'true');
    ui.browseCount.textContent = 'Loading cards…';
    const params = new URLSearchParams({query: ui.search.value.trim(), category: ui.category.value, deck: ui.deck.value, status: ui.status.value});
    try {
      const cards = validateCards(await request(`/cards?${params}`));
      if (version !== browseVersion) return;
      ui.browseCount.textContent = `${cards.length} cards`;
      ui.browseBody.replaceChildren(...cards.map(card => {
        const row = element('details', 'learning-browse-card');
        const summary = element('summary');
        const text = element('span', 'learning-browse-copy');
        text.append(element('strong', '', card.prompt), element('small', '', `${card.deck} · ${card.category} · ${card.skill}`));
        const status = ['new', 'due', 'scheduled', 'mastered'].includes(card.status) ? card.status : 'unknown';
        summary.append(text, element('span', `learning-chip learning-status-${status}`, status === 'unknown' ? 'Unknown' : status[0].toUpperCase() + status.slice(1)));
        const body = element('div', 'learning-browse-answer');
        body.append(element('p', 'learning-answer-main', card.answer), element('p', 'learning-example', card.example), element('p', 'learning-explanation', card.explanation), element('p', 'learning-muted', `${Number(card.review_count) || 0} reviews · ${card.due_at ? 'Next due: ' : ''}${dueText(card.due_at)}`));
        if (card.ownership === 'personal') {
          const actions = element('div', 'learning-author-actions');
          const edit = button('Edit card', () => editCard(card));
          const remove = button('Delete card', () => confirmDelete(card, actions));
          [edit, remove].forEach(node => { node.dataset.personalAction = ''; node.disabled = author.busy || state.reviewBusy || state.sessionBusy; });
          actions.append(edit, remove); body.append(actions);
        } else body.append(element('p', 'learning-muted', 'Seed-managed card · Read-only'));
        row.append(summary, body);
        return row;
      }));
      if (!cards.length) ui.browseBody.append(message('No cards match your filters', 'Try a different search, category, or scheduling status.'));
    } catch (error) {
      if (version !== browseVersion) return;
      ui.browseCount.textContent = 'Cards unavailable';
      ui.browseBody.replaceChildren();
      showError(ui.browseError, `Could not load cards. ${error.message}`, loadBrowse);
    } finally {
      if (version === browseVersion) ui.browseBody.setAttribute('aria-busy', 'false');
    }
  }
  function updateFilter(control, label, values) {
    const current = control.value;
    const options = ['', ...new Set([...values, ...(current ? [current] : [])])];
    control.replaceChildren(...options.map(value => {
      const option = element('option', '', value || label); option.value = value; return option;
    }));
    control.value = current;
  }
  function buildAuthoring() {
    const panel = named(element('details', 'learning-panel learning-authoring'), 'authoring');
    panel.append(element('summary', '', 'Personal cards · Add or import'));
    const body = element('div', 'learning-author-body');
    const form = named(element('form', 'learning-editor'), 'editor');
    const title = element('h3', '', 'Add a personal card');
    const fields = {}, suggestions = {};
    form.append(title, element('p', 'learning-muted', 'Your cards are editable. Seed-managed cards stay read-only. Editing keeps your review progress.'));
    const grid = element('div', 'learning-editor-grid');
    const labels = {deck: 'Deck', category: 'Category', skill: 'Skill', prompt: 'Prompt', answer: 'Answer', example: 'Example (optional)', explanation: 'Explanation (optional)', source: 'Source (optional)', tags: 'Tags (optional, separated by ;)'};
    EDIT_FIELDS.forEach(field => {
      const label = element('label', `learning-field learning-edit-${field}`);
      label.append(element('span', '', labels[field]));
      const long = ['prompt', 'answer', 'example', 'explanation'].includes(field);
      const input = named(element(long ? 'textarea' : 'input'), `edit-${field}`);
      if (long) input.rows = 2; else input.type = 'text';
      input.maxLength = long ? 4000 : field === 'tags' ? 2430 : field === 'source' ? 500 : 120;
      input.required = ['deck', 'category', 'skill', 'prompt', 'answer'].includes(field);
      if (field === 'deck' || field === 'category') {
        const list = element('datalist'); list.id = `learning-${field}-suggestions`;
        input.setAttribute('list', list.id); suggestions[field] = list; label.append(list);
      }
      fields[field] = input; label.append(input); grid.append(label);
    });
    const actions = element('div', 'learning-author-actions');
    const save = named(element('button', 'learning-primary', 'Add card'), 'save-card'); save.type = 'submit';
    actions.append(save, button('Cancel / clear', () => resetEditor()));
    form.append(grid, actions);
    form.addEventListener('submit', saveCard);
    const importPanel = element('div', 'learning-import');
    importPanel.append(element('h3', '', 'Import cards'), element('p', 'learning-muted', 'JSON: an array of card objects. CSV: a header row with prompt and answer; optional deck, category, skill, example, explanation, source and tags. Tags: a JSON array or semicolon-separated CSV text. Maximum 1 MiB / 500 rows. Preview first; duplicates are skipped, never replaced.'));
    const fileLabel = element('label', 'learning-field'); fileLabel.append(element('span', '', 'Choose a JSON or CSV file'));
    const file = named(element('input'), 'import-file'); file.type = 'file'; file.accept = '.json,.csv,application/json,text/csv';
    file.addEventListener('change', previewFile); fileLabel.append(file);
    const preview = named(element('div', 'learning-import-preview'), 'import-preview');
    const notice = named(element('p', 'learning-muted'), 'author-notice'); notice.setAttribute('role', 'status');
    const error = errorPanel('author-error');
    importPanel.append(fileLabel, preview);
    body.append(form, importPanel, notice, error); panel.append(body);
    Object.assign(ui, {authoring: panel, editFields: fields, editTitle: title, saveCard: save, suggestions, importFile: file, importPreview: preview, authorNotice: notice, authorError: error});
    resetEditor();
    return panel;
  }
  function resetEditor(card = null) {
    author.id = card?.id || null;
    EDIT_FIELDS.forEach(field => {
      ui.editFields[field].value = field === 'tags' ? (card?.tags || []).join('; ') : card?.[field] ?? ({deck: 'Personal', category: 'Personal', skill: 'General'}[field] || '');
    });
    ui.editTitle.textContent = card ? 'Edit your card' : 'Add a personal card';
    ui.saveCard.textContent = card ? 'Save changes' : 'Add card';
  }
  function editCard(card) {
    if (author.busy || state.reviewBusy || state.sessionBusy) return;
    resetEditor(card); ui.authoring.open = true; ui.authorError.hidden = true;
    ui.editFields.prompt.focus();
  }
  async function mutate(operation, success) {
    if (author.busy || state.reviewBusy || state.sessionBusy) return;
    author.busy = true; ui.authorError.hidden = true; ui.authorNotice.textContent = 'Saving...';
    syncControls();
    try {
      const result = await operation(); success(result);
      await Promise.all([loadSummary(), loadBrowse()]);
    } catch (error) { ui.authorNotice.textContent = ''; showError(ui.authorError, `${error.message} Nothing was confirmed. Check before retrying.`); }
    finally { author.busy = false; syncControls(); renderStudy(); }
  }
  function saveCard(event) {
    event.preventDefault();
    const payload = {};
    EDIT_FIELDS.forEach(field => { payload[field] = field === 'tags' ? ui.editFields.tags.value.split(';').map(tag => tag.trim()).filter(Boolean) : ui.editFields[field].value.trim(); });
    const identifier = author.id;
    mutate(() => request(identifier ? `/cards/${encodeURIComponent(identifier)}` : '/cards', {method: identifier ? 'PATCH' : 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)}), card => {
      state.cards = state.cards.map(old => old.id === card.id ? {...old, ...card} : old);
      resetEditor(); ui.authorNotice.textContent = identifier ? 'Card updated. Review progress kept.' : 'Personal card added.';
    });
  }
  function confirmDelete(card, actions) {
    if (author.busy || state.reviewBusy || state.sessionBusy) return;
    const confirm = button('Confirm delete', () => mutate(() => request(`/cards/${encodeURIComponent(card.id)}`, {method: 'DELETE'}), () => {
      state.cards = state.cards.filter((old, index) => old.id !== card.id || index < state.index);
      if (author.id === card.id) resetEditor();
      ui.authorNotice.textContent = 'Card deleted. Review history kept.';
    }));
    confirm.dataset.personalAction = '';
    actions.replaceChildren(element('span', 'learning-muted', 'Delete this personal card?'), confirm, button('Cancel', loadBrowse));
  }
  async function previewFile() {
    const version = ++author.fileVersion;
    author.preview = null; ui.importPreview.replaceChildren(); ui.authorError.hidden = true; ui.authorNotice.textContent = '';
    const file = ui.importFile.files[0];
    if (!file) return;
    if (file.size > 1024 * 1024) { showError(ui.authorError, 'Import file must be at most 1 MiB.'); return; }
    const format = file.name.toLowerCase().endsWith('.json') ? 'json' : file.name.toLowerCase().endsWith('.csv') ? 'csv' : null;
    if (!format) { showError(ui.authorError, 'Choose a .json or .csv file.'); return; }
    ui.authorNotice.textContent = 'Validating file...';
    try {
      const payload = {format, content: await file.text()};
      if (version !== author.fileVersion) return;
      const preview = await request('/import/preview', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
      if (version !== author.fileVersion) return;
      if (!Array.isArray(preview?.cards) || !Number.isInteger(preview.importable) || !Number.isInteger(preview.skipped)) throw new Error('Invalid import preview.');
      author.preview = payload;
      ui.authorNotice.textContent = '';
      ui.importPreview.append(element('p', 'learning-muted', `${preview.importable} to import; ${preview.skipped} duplicates to skip. Showing the first 20 rows.`));
      const list = element('ol', 'learning-preview-list');
      preview.cards.slice(0, 20).forEach(card => {
        const row = element('li'); row.append(element('strong', '', card.prompt), element('span', '', `${card.answer} · ${card.deck} / ${card.category} · ${card.action}`)); list.append(row);
      });
      ui.importPreview.append(list);
      if (preview.importable > 0) ui.importPreview.append(named(button('Import these cards', importPreview, 'learning-primary'), 'confirm-import'));
      syncControls();
    } catch (error) { if (version === author.fileVersion) { ui.authorNotice.textContent = ''; showError(ui.authorError, error.message); } }
  }
  function importPreview() {
    const payload = author.preview;
    if (!payload) return;
    mutate(() => request('/import', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)}), result => {
      author.fileVersion++; author.preview = null; ui.importFile.value = ''; ui.importPreview.replaceChildren();
      ui.authorNotice.textContent = `Imported ${result.imported} ${result.imported === 1 ? 'card' : 'cards'}; skipped ${result.skipped} duplicates.`;
    });
  }
  function onKeydown(event) {
    if (!root || document.body.dataset.hub !== 'learning' || root.style.display === 'none' || event.repeat || event.ctrlKey || event.altKey || event.metaKey || event.shiftKey || event.defaultPrevented) return;
    const target = event.target;
    if (target.isContentEditable || target.closest('input, textarea, select, [contenteditable], [role="textbox"], dialog')) return;
    // Native Space activation of a focused button must not also reveal a card.
    if (event.code === 'Space' || event.key === ' ') {
      if (target.closest('button, a, summary')) return;
      if (!state.cards[state.index] || state.revealed || state.sessionBusy || state.reviewBusy) return;
      event.preventDefault();
      reveal();
    } else if (/^[1-4]$/.test(event.key) && state.revealed && !state.reviewBusy && !state.sessionBusy && state.cards[state.index]) {
      event.preventDefault();
      rate(Number(event.key));
    }
  }
  async function load() {
    if (!root) return;
    await Promise.all([loadSummary(), loadBrowse()]);
  }
  window.HolocronLearning = {mount, load};
})();
