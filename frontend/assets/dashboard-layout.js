(() => {
  'use strict';
  const names = {launch:'Quick Launch', weather:'Weather', monitoring:'Monitoring', favorites:'Quick Access', jellyfin:'Jellyfin'};
  const defaults = {dashboard_order:Object.keys(names), dashboard_hidden:['jellyfin']};
  let root, grid, options, toolbar, choices, status, panels, controls;
  let editing = false, busy = false, draft, original, dragging = null, touch = null;
  const element = (tag, text, className) => {
    const node = document.createElement(tag);
    if (text) node.textContent = text;
    if (className) node.className = className;
    return node;
  };
  function normalized(input = {}) {
    const order = Array.isArray(input.dashboard_order) ? input.dashboard_order : defaults.dashboard_order;
    const hidden = Array.isArray(input.dashboard_hidden) ? input.dashboard_hidden : defaults.dashboard_hidden;
    const valid = [...new Set(order.filter(id => Object.hasOwn(names,id)))];
    return {dashboard_order:[...valid,...Object.keys(names).filter(id=>!valid.includes(id))], dashboard_hidden:[...new Set(hidden.filter(id=>Object.hasOwn(names,id)))]};
  }
  function button(label, handler, className = '') {
    const node = element('button',label,className);
    node.type = 'button';
    node.addEventListener('click',handler);
    return node;
  }
  function render() {
    if (!root) return;
    const focused = document.activeElement;
    const choice = focused?.dataset.layoutChoice;
    const layout = editing ? draft : normalized(options.get());
    grid.dataset.launchStack = String(layout.dashboard_order[0] === 'launch' && !layout.dashboard_hidden.includes('weather') && !layout.dashboard_hidden.includes('monitoring'));
    root.classList.toggle('dashboard-editing',editing);
    for (const id of layout.dashboard_order) {
      const panel = panels.get(id);
      if (!panel) continue;
      grid.append(panel);
      panel.hidden = !editing && (layout.dashboard_hidden.includes(id) || panel.dataset.layoutEmpty === 'true');
      controls.get(id).hidden = !editing;
    }
    toolbar.replaceChildren();
    if (!editing) {
      toolbar.append(button('Arrange dashboard',start,'sm'));
      choices.hidden = true;
      options.onVisibility?.(layout.dashboard_hidden);
      return;
    }
    const save = button('Save layout',saveLayout,'primary');
    save.disabled = busy;
    const cancel = button('Cancel layout changes',cancelLayout,'sm');
    const reset = button('Reset layout',() => {draft=normalized(defaults);status.textContent='Default layout preview. Save to keep it.';render();},'sm');
    cancel.disabled = reset.disabled = busy;
    toolbar.append(save,cancel,reset,element('span','Drag a handle or use the move buttons.','muted'));
    choices.hidden = false;
    choices.replaceChildren();
    for (const [id,title] of Object.entries(names)) {
      if (!panels.has(id)) continue;
      const label = element('label');
      const checkbox = element('input');
      checkbox.type='checkbox';checkbox.checked=!draft.dashboard_hidden.includes(id);checkbox.disabled=busy;
      checkbox.dataset.layoutChoice=id;
      checkbox.addEventListener('change',() => {
        draft.dashboard_hidden = checkbox.checked ? draft.dashboard_hidden.filter(value=>value!==id) : [...draft.dashboard_hidden,id];
        status.textContent='Preview only. Save to keep these changes.';
        render();
      });
      label.append(checkbox,element('span','Show '+title));choices.append(label);
    }
    if (choice) choices.querySelector(`[data-layout-choice="${choice}"]`)?.focus({preventScroll:true});
    else if (focused && root.contains(focused)) focused.focus({preventScroll:true});
  }
  function start() {original=normalized(options.get());draft=normalized(original);editing=true;status.textContent='Preview only. Save to keep these changes.';render();}
  function cancelLayout() {editing=false;draft=null;status.textContent='';render();}
  async function saveLayout() {
    if (busy) return;
    busy=true;status.textContent='Saving layout...';render();
    try {
      await options.save(normalized(draft));
      editing=false;status.textContent='Layout saved.';
    } catch (error) {status.textContent='Could not save: '+error.message+'. Your draft is still here.';}
    finally {busy=false;render();}
  }
  function move(id, to) {
    if (!editing || busy || !panels.has(id)) return;
    const from = draft.dashboard_order.indexOf(id);
    const destination = Math.max(0,Math.min(draft.dashboard_order.length-1,to));
    if (from===destination) return;
    draft.dashboard_order.splice(from,1);draft.dashboard_order.splice(destination,0,id);
    status.textContent=names[id]+' moved. Save to keep this order.';render();
  }
  function panelAt(target) {
    const panel = target?.closest('[data-dashboard-tile]');
    return panel && grid.contains(panel) ? panel : null;
  }
  function addControls(id, panel) {
    const bar = element('div',null,'dashboard-tile-controls');bar.hidden=true;
    const handle = button('Drag',()=>{},'dashboard-drag-handle');
    handle.setAttribute('aria-label','Drag '+names[id]);handle.draggable=true;
    handle.addEventListener('dragstart',event => {
      if (!editing || busy) {event.preventDefault();return;}
      dragging=id;event.dataTransfer.effectAllowed='move';event.dataTransfer.setData('text/plain',id);
      panel.classList.add('dashboard-dragging');
    });
    handle.addEventListener('dragend',() => {dragging=null;panel.classList.remove('dashboard-dragging');});
    handle.addEventListener('pointerdown',event => {
      if (event.pointerType==='mouse' || !editing || busy) return;
      event.preventDefault();touch={id,pointer:event.pointerId};handle.setPointerCapture(event.pointerId);
    });
    handle.addEventListener('pointermove',event => {
      if (!touch || touch.pointer!==event.pointerId) return;
      const target=panelAt(document.elementFromPoint(event.clientX,event.clientY));
      if (target && target.dataset.dashboardTile!==id) move(id,draft.dashboard_order.indexOf(target.dataset.dashboardTile));
    });
    const endTouch=event=>{if(touch?.pointer===event.pointerId){if(handle.hasPointerCapture(event.pointerId))handle.releasePointerCapture(event.pointerId);touch=null;}};
    handle.addEventListener('pointerup',endTouch);handle.addEventListener('pointercancel',endTouch);
    const earlier=button('Up',()=>move(id,draft.dashboard_order.indexOf(id)-1),'sm');
    const later=button('Down',()=>move(id,draft.dashboard_order.indexOf(id)+1),'sm');
    earlier.setAttribute('aria-label','Move '+names[id]+' earlier');later.setAttribute('aria-label','Move '+names[id]+' later');
    bar.append(handle,earlier,later);panel.prepend(bar);controls.set(id,bar);
  }
  function mount(target, configuration) {
    if (!target || root) return;
    root=target;options=configuration;grid=root.querySelector('.home-dashboard-grid');
    if (!grid) return;
    grid.dataset.layoutGrid='true';panels=new Map();controls=new Map();
    document.querySelectorAll('[data-dashboard-tile]').forEach(panel=>{
      const id=panel.dataset.dashboardTile;
      if (!Object.hasOwn(names,id)) return;
      panels.set(id,panel);grid.append(panel);addControls(id,panel);
    });
    root.querySelector('.home-side-stack')?.remove();
    toolbar=element('div',null,'dashboard-layout-toolbar');choices=element('div',null,'dashboard-layout-choices');
    status=element('div',null,'dashboard-layout-status');status.setAttribute('role','status');
    grid.before(toolbar,choices,status);
    grid.addEventListener('dragover',event=>{if(dragging && editing){event.preventDefault();event.dataTransfer.dropEffect='move';}});
    grid.addEventListener('drop',event=>{
      if (!dragging || !editing) return;
      event.preventDefault();const target=panelAt(event.target);
      if(target)move(dragging,draft.dashboard_order.indexOf(target.dataset.dashboardTile));
      dragging=null;
    });
    render();
  }
  function apply() {if(root && !editing)render();}
  window.HolocronDashboardLayout={mount,apply};
})();
