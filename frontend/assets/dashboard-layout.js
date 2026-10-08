(() => {
  'use strict';
  const names = {welcome:'Welcome', launch:'Quick Launch', weather:'Weather', monitoring:'Monitoring', favorites:'Quick Access', jellyfin:'Jellyfin', library:'Tool library'};
  const defaults = {dashboard_order:Object.keys(names), dashboard_hidden:['jellyfin']};
  const descriptions = {welcome:'Greeting, local time and date.',launch:'Your editable Home Lab service shortcuts.',weather:'Local weather and conditions.',monitoring:'Uptime Kuma snapshot and direct access.',favorites:'Pinned AI and web tools. Pin a tool to populate this widget.',jellyfin:'Now Playing and Continue Watching from your configured Jellyfin server.',library:'Curated AI directory, Home Lab groups and tool management.'};
  const icons = {welcome:'M12 3 20 7v10l-8 4-8-4V7z',launch:'M4 4h6v6H4z M14 4h6v6h-6z M4 14h6v6H4z M14 14h6v6h-6z',weather:'M6 17a4 4 0 0 1 0-8 6 6 0 0 1 11-2 5 5 0 0 1 1 10z',monitoring:'M3 12h4l3-8 4 16 3-8h4',favorites:'m12 3 3 6 7 1-5 5 1 7-6-3-6 3 1-7-5-5 7-1z',jellyfin:'M4 4h16v16H4z M10 8l6 4-6 4z',library:'m12 3 9 5-9 5-9-5z M3 12l9 5 9-5 M3 16l9 5 9-5'};
  let root, grid, options, toolbar, status, panels, controls, catalog, catalogGrid, catalogTrigger, sortable;
  let editing = false, busy = false, draft, dragActive = false;
  const revealed = new Set();
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
    if (!valid.includes('welcome')) valid.unshift('welcome');
    return {dashboard_order:[...valid,...Object.keys(names).filter(id=>!valid.includes(id))], dashboard_hidden:[...new Set(hidden.filter(id=>Object.hasOwn(names,id)))]};
  }
  function button(label, handler, className = '') {
    const node = element('button',label,className);
    node.type = 'button';
    node.addEventListener('click',handler);
    return node;
  }
  function icon(path) {
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
    svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');
    const shape=document.createElementNS(svg.namespaceURI,'path');shape.setAttribute('d',path);svg.append(shape);return svg;
  }
  function iconButton(label,path,handler) {
    const node=button('',handler,'dashboard-icon-button');
    node.setAttribute('aria-label',label);node.title=label;node.append(icon(path));return node;
  }
  function renderCatalog() {
    if(!catalogGrid||!draft)return;
    catalogGrid.replaceChildren();
    for(const [id,title] of Object.entries(names)) {
      if(!panels.has(id))continue;
      const available=draft.dashboard_hidden.includes(id);
      const card=element('article',null,'dashboard-widget-option');
      const preview=element('div',null,'dashboard-widget-preview');preview.dataset.widget=id;
      preview.append(icon(icons[id]),element('span',title));
      const add=button(available?'Add '+title:'On dashboard',()=>{
        draft.dashboard_hidden=draft.dashboard_hidden.filter(value=>value!==id);
        draft.dashboard_order=draft.dashboard_order.filter(value=>value!==id).concat(id);
        catalog.close();status.textContent=title+' added. Save to keep this widget.';render();
        panels.get(id).scrollIntoView({block:'nearest',behavior:'auto'});
        controls.get(id).querySelector('.dashboard-tile-handle').focus({preventScroll:true});
      },'dashboard-widget-add');
      add.disabled=!available||busy;
      card.append(preview,element('h3',title),element('p',descriptions[id]),add);catalogGrid.append(card);
    }
  }
  function openCatalog() {if(!editing||busy||dragActive)return;renderCatalog();if(!catalog.open)catalog.showModal();}
  function buildCatalog() {
    catalog=element('dialog',null,'dashboard-widget-catalog');catalog.setAttribute('aria-label','Add a dashboard widget');
    const head=element('div',null,'dashboard-catalog-head');
    head.append(element('div','Make this dashboard yours.','dashboard-catalog-kicker'),iconButton('Close widget library','M6 6l12 12M18 6 6 18',()=>catalog.close()));
    catalogGrid=element('div',null,'dashboard-widget-options');
    catalog.append(head,element('h2','Add a widget'),element('p','Choose an existing widget. Connections and tool contents remain editable in their own settings.','dashboard-catalog-help'),catalogGrid);
    catalog.addEventListener('click',event=>{if(event.target===catalog){const rect=catalog.getBoundingClientRect();if(event.clientX<rect.left||event.clientX>rect.right||event.clientY<rect.top||event.clientY>rect.bottom)catalog.close();}});
    catalog.addEventListener('close',()=>catalogTrigger?.focus({preventScroll:true}));document.body.append(catalog);
  }
  function render() {
    if (!root) return;
    const focused = document.activeElement;
    const layout = editing ? draft : normalized(options.get());
    grid.dataset.launchStack = String(!editing && layout.dashboard_order.filter(id=>!['welcome','library','favorites','jellyfin'].includes(id))[0] === 'launch' && !layout.dashboard_hidden.includes('weather') && !layout.dashboard_hidden.includes('monitoring'));
    root.classList.toggle('dashboard-editing',editing);
    sortable?.option('disabled',!editing||busy);
    let nextPanel=grid.firstElementChild;
    for (const id of layout.dashboard_order) {
      const panel = panels.get(id);
      if (!panel) continue;
      // Preserve the drop target so the browser can finish its release/click event.
      if(panel===nextPanel)nextPanel=nextPanel.nextElementSibling;
      else grid.insertBefore(panel,nextPanel);
      panel.hidden = (layout.dashboard_hidden.includes(id) && (editing || !revealed.has(id))) || (!editing && panel.dataset.layoutEmpty === 'true');
      controls.get(id).hidden = !editing;
      controls.get(id).querySelectorAll('button').forEach(node=>node.disabled=busy);
    }
    toolbar.replaceChildren();
    options.onVisibility?.(layout.dashboard_hidden);
    if (!editing) {
      toolbar.append(button(options.get()?.language === 'DE' ? 'Seite bearbeiten' : 'Edit page',start,'sm'));
      if(catalog.open)catalog.close();
      return;
    }
    const save = button('Save layout',saveLayout,'primary');
    save.disabled = busy;
    const cancel = button('Cancel layout changes',cancelLayout,'sm');
    const reset = button('Reset layout',() => {if(dragActive)return;draft=normalized(defaults);status.textContent='Default layout preview. Save to keep it.';render();},'dashboard-secondary');
    cancel.disabled = reset.disabled = busy;
    catalogTrigger=button('Add widget',openCatalog,'dashboard-add-widget');catalogTrigger.prepend(icon('M12 5v14M5 12h14'));catalogTrigger.disabled=busy;
    const heading=element('div',null,'dashboard-edit-heading');heading.append(element('strong','Edit your dashboard'),element('span','Drag a widget header. Remove or add widgets without deleting their data.'));
    toolbar.append(heading,catalogTrigger,reset,cancel,save);
    if(catalog.open)renderCatalog();
    if(focused && root.contains(focused))focused.focus({preventScroll:true});
  }
  function start() {revealed.clear();draft=normalized(options.get());editing=true;status.textContent='Preview only. Save to keep these changes.';render();}
  function cancelLayout() {if(dragActive)return;editing=false;draft=null;status.textContent='';render();}
  async function saveLayout() {
    if (!editing || busy || dragActive) return;
    busy=true;status.textContent='Saving layout...';render();
    try {
      await options.save(normalized(draft));
      editing=false;status.textContent='Layout saved.';
    } catch (error) {status.textContent='Could not save: '+error.message+'. Your draft is still here.';}
    finally {busy=false;render();}
  }
  function move(id, to) {
    if (!editing || busy || dragActive || !panels.has(id)) return;
    const from = draft.dashboard_order.indexOf(id);
    const destination = Math.max(0,Math.min(draft.dashboard_order.length-1,to));
    if (from===destination) return;
    draft.dashboard_order.splice(from,1);draft.dashboard_order.splice(destination,0,id);
    status.textContent=names[id]+' moved. Save to keep this order.';render();
  }
  function addControls(id, panel) {
    const bar = element('div',null,'dashboard-tile-controls');bar.hidden=true;
    const handle=button('',()=>{},'dashboard-tile-handle');handle.setAttribute('aria-label','Drag '+names[id]);
    handle.append(icon('M8 5h.01M16 5h.01M8 12h.01M16 12h.01M8 19h.01M16 19h.01'),element('strong',names[id]));
    const earlier=iconButton('Move '+names[id]+' earlier','m7 14 5-5 5 5',()=>move(id,draft.dashboard_order.indexOf(id)-1));
    const later=iconButton('Move '+names[id]+' later','m7 10 5 5 5-5',()=>move(id,draft.dashboard_order.indexOf(id)+1));
    const remove=iconButton('Remove '+names[id],'M6 6l12 12M18 6 6 18',()=>{
      if(!editing||busy||dragActive)return;
      draft.dashboard_hidden=[...new Set([...draft.dashboard_hidden,id])];status.textContent=names[id]+' removed from the dashboard, not deleted. Save to keep this layout.';render();catalogTrigger.focus({preventScroll:true});
    });
    bar.append(handle,earlier,later,remove);panel.prepend(bar);controls.set(id,bar);
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
    toolbar=element('div',null,'dashboard-layout-toolbar');buildCatalog();
    status=element('div',null,'dashboard-layout-status');status.setAttribute('role','status');
    grid.before(toolbar,status);
    if(window.Sortable)sortable=new window.Sortable(grid,{
      disabled:true,draggable:'[data-dashboard-tile]:not([hidden])',handle:'.dashboard-tile-handle',
      dataIdAttr:'data-dashboard-tile',forceFallback:true,fallbackOnBody:true,fallbackTolerance:5,
      fallbackClass:'dashboard-tile-float',ghostClass:'dashboard-tile-placeholder',chosenClass:'dashboard-tile-chosen',
      animation:window.matchMedia('(prefers-reduced-motion: reduce)').matches?0:160,
      direction:(_event,target,dragged)=>{
        const width=grid.getBoundingClientRect().width;
        return target && dragged && target.getBoundingClientRect().width<width*.75 && dragged.getBoundingClientRect().width<width*.75?'horizontal':'vertical';
      },
      onStart:()=>{
        dragActive=true;root.classList.add('dashboard-is-dragging');
        toolbar.querySelectorAll('button').forEach(node=>node.disabled=true);
        document.querySelectorAll('.dashboard-tile-float').forEach(clone=>{clone.setAttribute('aria-hidden','true');clone.inert=true;clone.removeAttribute('id');clone.querySelectorAll('[id]').forEach(node=>node.removeAttribute('id'));});
      },
      onEnd:event=>{
        dragActive=false;root.classList.remove('dashboard-is-dragging');
        if(!editing||!draft){render();return;}
        draft.dashboard_order=Array.from(grid.children).filter(node=>panels.get(node.dataset.dashboardTile)===node).map(node=>node.dataset.dashboardTile);
        status.textContent=names[event.item.dataset.dashboardTile]+' moved. Save to keep this order.';render();
      }
    });
    render();
  }
  function apply() {if(root && !editing)render();}
  function reveal(id, show=true) {if(show)revealed.add(id);else revealed.delete(id);apply();}
  window.HolocronDashboardLayout={mount,apply,reveal};
})();
