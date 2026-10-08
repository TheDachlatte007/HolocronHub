(() => {
  'use strict';
  let pending;
  const el=(tag,text)=>{const node=document.createElement(tag);node.textContent=String(text);return node;};
  function render(data){
    const badge=document.getElementById('home-build-info'), details=document.getElementById('build-info-details');
    const fingerprint=typeof data.source_id==='string' && /^[a-f0-9]{64}$/.test(data.source_id)?data.source_id:null;
    const revision=typeof data.revision==='string' && /^[a-f0-9]{7,64}$/.test(data.revision)?data.revision:null;
    const version=typeof data.version==='string'?data.version:'unknown';
    if(badge)badge.textContent='Build '+version+' · '+(revision?'commit '+revision.slice(0,7):fingerprint?'code '+fingerprint.slice(0,12):'identity unavailable');
    if(!details)return;
    const built=new Date(data.built_at);
    const rows=[['Version',version],['Git revision',revision||'Not supplied by image build'],['Code fingerprint',fingerprint||'Unavailable'],['Image built',data.built_at&&!Number.isNaN(built.getTime())?built.toLocaleString():'No image-build timestamp'],['Identity source',data.source==='image-build'?'Verified image build manifest':data.source==='source-files'?'Delivered source files':'Unavailable']];
    details.replaceChildren(...rows.map(([label,value])=>{const row=document.createElement('div');row.className='small-row';row.append(el('strong',label),el('span',value));return row;}));
  }
  function load(){
    if(pending)return pending;
    const controller=new AbortController(), timer=setTimeout(()=>controller.abort(),8000);
    pending=(async()=>{
      try{
        const response=await fetch('/api/build-info',{signal:controller.signal,cache:'no-store'});
        if(!response.ok)throw Error('Unavailable');
        const data=await response.json();
        if(!data||typeof data.version!=='string'||!['image-build','source-files','unavailable'].includes(data.source))throw Error('Invalid metadata');
        render(data);
      }catch(error){
        for(const id of ['home-build-info','build-info-details']){const node=document.getElementById(id);if(node)node.textContent='Build information unavailable. Retry in Settings.';}
      }finally{clearTimeout(timer);pending=null;}
    })();
    return pending;
  }
  window.HolocronBuildInfo={load};
})();
