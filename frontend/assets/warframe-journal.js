(() => {
  'use strict';
  const API='/api/warframe/farm-journal';
  let root, ui, data, pending=null, busy=false, nextOffset=0;
  const views=new Map(), history=new Map();
  const el=(tag,text,className)=>{const node=document.createElement(tag);if(text!=null)node.textContent=String(text);if(className)node.className=className;return node;};
  const button=(text,handler)=>{const node=el('button',text);node.type='button';node.addEventListener('click',handler);return node;};
  const field=(label,type='text',value='')=>{
    const wrapper=el('label',null,'farm-field');const input=el('input');input.type=type;input.value=value;
    if(type==='number'){input.min='0';input.step='0.01';}
    wrapper.append(el('span',label),input);return {wrapper,input};
  };
  const money=value=>value==null?'Unknown':Number(value).toFixed(2)+'p';
  const duration=seconds=>Math.floor(seconds/60)+'m '+Math.floor(seconds%60)+'s';
  async function request(path,options){
    const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),12000);
    try{
      const response=await fetch(API+path,{...options,signal:controller.signal});
      const payload=await response.json();
      if(!response.ok)throw Error(typeof payload.detail==='string'?payload.detail:'Request failed ('+response.status+')');
      return payload;
    }finally{clearTimeout(timer);}
  }
  function setBusy(value){busy=value;if(root)root.querySelectorAll('button').forEach(node=>node.disabled=value);}
  async function mutate(path,payload,success){
    if(busy)return;setBusy(true);ui.error.textContent='';
    try{
      const saved=await request(path,{method:path.endsWith('/drops')||path==='/sessions'?'POST':'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
      if(success)success(saved);
      await load(true);
    }catch(error){ui.error.textContent=error.message||'Could not save journal. Your draft is still here.';}
    finally{setBusy(false);}
  }
  function createView(session){
    const box=el('section',null,'farm-session');box.dataset.sessionId=session.id;
    const title=el('h3'),metrics=el('p',null,'farm-metrics');
    const route=field('Route','text',session.route||''),target=field('Target','text',session.target||'');
    const sale=field('Confirmed sale proceeds (total platinum)','number',session.confirmed_sale_platinum||0);sale.input.max='1000000';
    const editing=el('div',null,'farm-fields');editing.append(route.wrapper,target.wrapper,sale.wrapper);
    const save=button('Save session',()=>mutate('/sessions/'+session.id,{route:route.input.value.trim()||null,target:target.input.value.trim()||null,confirmed_sale_platinum:Number(sale.input.value)},()=>{view.dirty=false;}));
    const stop=button('Stop session',()=>mutate('/sessions/'+session.id,{finish:true}));
    const actions=el('div',null,'farm-actions');actions.append(save,stop);
    const item=field('Item'),quantity=field('Quantity','number','1'),price=field('Estimated unit platinum','number');
    item.input.maxLength=200;quantity.input.min='1';quantity.input.max='1000000';quantity.input.step='1';price.input.max='1000000';
    const dropForm=el('form',null,'farm-drop-form');const dropFields=el('div',null,'farm-fields');dropFields.append(item.wrapper,quantity.wrapper,price.wrapper);
    const log=el('button','Log drop');log.type='submit';dropForm.append(dropFields,log);
    dropForm.addEventListener('submit',event=>{
      event.preventDefault();
      if(!item.input.value.trim()){ui.error.textContent='Enter an item name.';return;}
      mutate('/sessions/'+session.id+'/drops',{item:item.input.value.trim(),quantity:Number(quantity.input.value),estimated_unit_platinum:price.input.value===''?null:Number(price.input.value)},()=>{item.input.value='';quantity.input.value='1';price.input.value='';});
    });
    const drops=el('ul',null,'farm-drop-list');
    box.append(title,metrics,editing,actions,dropForm,drops);
    const view={box,title,metrics,route,target,sale,stop,drops,dirty:false,session};
    [route.input,target.input,sale.input].forEach(input=>input.addEventListener('input',()=>view.dirty=true));
    views.set(session.id,view);return view;
  }
  function updateView(view,session){
    view.session=session;
    view.title.textContent=(session.route||session.target||'Farm session')+' · '+session.status;
    view.metrics.textContent=duration(session.elapsed_seconds||0)+' · Estimated drops '+money(session.estimated_drop_platinum)+' · Confirmed sales '+money(session.confirmed_sale_platinum)+(session.unvalued_quantity?' · '+session.unvalued_quantity+' unvalued drops':'');
    view.stop.hidden=session.status!=='active';
    if(!view.dirty){view.route.input.value=session.route||'';view.target.input.value=session.target||'';view.sale.input.value=session.confirmed_sale_platinum||0;}
    view.drops.replaceChildren(...(session.drops||[]).map(drop=>el('li',drop.item+' × '+drop.quantity+' · estimated unit '+money(drop.estimated_unit_platinum))));
  }
  function render(payload){
    data=payload;const summary=payload.summary||{},active=payload.active_session;
    ui.summary.textContent=(summary.session_count||0)+' sessions · '+duration(summary.elapsed_seconds||0)+' · Estimated drops '+money(summary.estimated_drop_platinum)+' · Confirmed sales '+money(summary.confirmed_sale_platinum)+' · '+(summary.unvalued_quantity||0)+' unvalued items';
    ui.startForm.hidden=Boolean(active);
    if(active){const view=views.get(active.id)||createView(active);updateView(view,active);if(ui.active.firstElementChild!==view.box)ui.active.replaceChildren(view.box);}
    else ui.active.replaceChildren();
    const finished=(payload.sessions||[]).filter(session=>session.status==='finished');
    for(const session of finished){
      const view=views.get(session.id)||createView(session);updateView(view,session);
      let details=history.get(session.id);
      if(!details){details=el('details');details.append(el('summary'),view.box);history.set(session.id,details);}
      details.querySelector('summary').textContent=(session.route||session.target||'Farm session')+' · '+new Date(session.started_at).toLocaleDateString()+' · '+money(session.confirmed_sale_platinum)+' sold';
      ui.historyList.append(details);
    }
    [...history.entries()].sort((a,b)=>Number(b[0])-Number(a[0])).forEach(([,details])=>ui.historyList.append(details));
    ui.more.hidden=!payload.has_more;
  }
  function load(force=false,more=false){
    if(!root)return Promise.resolve(null);
    if(pending)return force?pending.then(()=>load()):pending;
    const offset=more?nextOffset:0;
    pending=request('/sessions?limit=50&offset='+offset).then(payload=>{nextOffset=offset+(payload.sessions||[]).length;ui.error.textContent='';render(payload);return payload;}).catch(error=>{ui.error.textContent=error.message||'Journal unavailable. Saved entries are retained.';return null;}).finally(()=>{pending=null;});
    return pending;
  }
  function mount(target){
    if(root)return root;if(!target)return null;root=target;root.classList.add('farm-journal');
    const heading=el('div',null,'farm-heading');heading.append(el('h2','Personal farm journal'),button('Refresh journal',()=>load()));
    const note=el('p','Manual records. Estimated inventory value is not sale proceeds or guaranteed platinum/hour.','farm-note');
    const summary=el('p',null,'farm-metrics');summary.dataset.summary='true';
    const error=el('p',null,'farm-error');error.setAttribute('role','alert');
    const startForm=el('form'),targetField=field('Target'),routeField=field('Route');
    const fields=el('div',null,'farm-fields');fields.append(targetField.wrapper,routeField.wrapper);
    const start=el('button','Start session');start.type='submit';startForm.append(fields,start);
    startForm.addEventListener('submit',event=>{event.preventDefault();mutate('/sessions',{target:targetField.input.value.trim()||null,route:routeField.input.value.trim()||null});});
    const active=el('div');active.dataset.activeSession='true';
    const recorded=el('details');recorded.dataset.history='true';recorded.append(el('summary','Previous sessions'));
    const historyList=el('div');historyList.dataset.historyList='true';const more=button('Show more sessions',()=>load(false,true));more.hidden=true;
    recorded.append(historyList,more);root.replaceChildren(heading,note,summary,error,startForm,active,recorded);
    ui={summary,error,startForm,target:targetField.input,route:routeField.input,active,historyList,more};return root;
  }
  function setTarget(value){
    if(!ui)return;
    if(data?.active_session){if(value?.route)ui.error.textContent='You already have an active session. Stop it before starting another route.';return;}
    ui.target.value=typeof value==='string'?value:String(value?.name||'');
    if(value?.route)ui.route.value=value.route;
  }
  window.HolocronFarmJournal={mount,load,setTarget};
})();
