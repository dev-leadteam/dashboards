// SWEAT440 Dashboard — Shared Utilities
// Import this file in every dashboard HTML
// Each dashboard must define applyFilters() in its own <script> block

function _applyFilters() {
  if (typeof applyFilters === 'function') applyFilters();
}


// ── Chart instance registry ────────────────────────────────────────────────
const _areaCharts = {};

// ── Source color map ───────────────────────────────────────────────────────
const SRC_COLORS = ['#00A3E0','#00C9A7','#9aab00','#f4a021','#e05a5a','#7c5cbf',
  '#00bcd4','#8bc34a','#ff7043','#5c6bc0','#26a69a','#ef5350','#ab47bc','#78909c'];

const SRC_COLOR_MAP = {
  'Business Mode':          '#00A3E0',
  'Google Ads':             '#4285F4',
  'Local Listings':         '#00BCD4',
  'Meta Ads':               '#1877F2',
  'MindBody App':           '#FF6B35',
  'N/A':                    '#607D8B',
  'Other':                  '#9E9E9E',
  'Print Ads / Signs':      '#FF9800',
  'SWEAT440 App':           '#9C27B0',
  'Social Media Organic':   '#E040FB',
  'Website (unattributed)': '#00C9A7',
  'Word of Mouth':          '#F44336',
};

// ── Default exclusions (can be overridden per dashboard) ───────────────────
const DEFAULT_EXCL_SOURCES = ['ClassPass / Platforms','Grassroots'];
const DEFAULT_EXCL_STUDIOS = [
  'SWEAT440 Dallas - Prestonwood','SWEAT440 Herriman','SWEAT440 Naples - Mercato',
  'SWEAT440 Nashville - Capitol View','SWEAT440 Pinecrest - Palmetto Bay','SWEAT440 Reston'
];

function localDateStr(d) { return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0'); }
document.getElementById('fFrom').value = localDateStr(firstOfPrevMonth);
document.getElementById('fTo').value   = localDateStr(lastOfPrevMonth);
// After data loads, if default range has no data, fall back to last available month
window._defaultFrom = localDateStr(firstOfPrevMonth);
window._defaultTo   = localDateStr(lastOfPrevMonth);

// Multi-select

function toggleDropdown(id) {
  document.querySelectorAll('.ms-menu').forEach(m => { if (m.id !== id+'Menu') m.classList.remove('open'); });
  document.getElementById(id+'Menu').classList.toggle('open');
}
document.addEventListener('click', e => {
  if (!e.target.closest('.multi-select')) document.querySelectorAll('.ms-menu').forEach(m => m.classList.remove('open'));
});

function buildMultiSelect(menuId, labelId, items, defaultExcluded) {
  const menu = document.getElementById(menuId);
  const allDiv = document.createElement('div');
  allDiv.className = 'ms-item ms-select-all';
  allDiv.innerHTML = `<input type="checkbox" id="${menuId}_all"> <label for="${menuId}_all" style="cursor:pointer">Select all</label>`;
  menu.appendChild(allDiv);
  const div0 = document.createElement('div'); div0.className = 'ms-divider'; menu.appendChild(div0);

  items.forEach(item => {
    const div = document.createElement('div');
    div.className = 'ms-item';
    const checked = !defaultExcluded.includes(item);
    const safeId = `ms_${menuId}_${item.replace(/[^a-z0-9]/gi,'_')}`;
    div.innerHTML = `<input type="checkbox" id="${safeId}" value="${item}" ${checked?'checked':''}> <label for="${safeId}" style="cursor:pointer">${item}</label>`;
    div.querySelector('input').addEventListener('change', () => { syncSelectAll(menuId); updateLabel(menuId, labelId, items); _applyFilters(); });
    menu.appendChild(div);
  });

  const allChk = document.getElementById(menuId+'_all');
  syncSelectAll(menuId);
  allChk.addEventListener('change', () => {
    menu.querySelectorAll('input[value]').forEach(c => c.checked = allChk.checked);
    updateLabel(menuId, labelId, items); _applyFilters();
  });
  updateLabel(menuId, labelId, items);
}

function syncSelectAll(menuId) {
  const menu = document.getElementById(menuId);
  const all  = [...menu.querySelectorAll('input[value]')];
  const allChk = document.getElementById(menuId+'_all');
  const n = all.filter(c=>c.checked).length;
  allChk.checked = n === all.length;
  allChk.indeterminate = n > 0 && n < all.length;
}

function updateLabel(menuId, labelId, items) {
  const checked = [...document.getElementById(menuId).querySelectorAll('input[value]:checked')].map(c=>c.value);
  const label = document.getElementById(labelId);
  if      (checked.length === 0)            label.textContent = 'None selected';
  else if (checked.length === items.length) label.textContent = 'All';
  else if (checked.length <= 2)             label.textContent = checked.join(', ');
  else                                      label.textContent = checked.length + ' selected';
}

function getSelected(menuId) {
  const menu = document.getElementById(menuId);
  if (!menu) return null;
  return [...menu.querySelectorAll('input[value]:checked')].map(c=>c.value);
}

// Granularity

function getQuarterBounds() {
  const now = new Date();
  const qStart = new Date(now.getFullYear(), Math.floor(now.getMonth()/3)*3, 1);
  const prevQStart = new Date(qStart); prevQStart.setMonth(prevQStart.getMonth()-3);
  return { dailyFrom: prevQStart, dailyTo: now };
}

function setGran(gran) {
  GRAN = gran;
  ['daily','weekly','monthly'].forEach(g => {
    const btn = document.getElementById('gran'+g.charAt(0).toUpperCase()+g.slice(1));
    if (btn) btn.classList.toggle('active', g === gran);
  });
  _applyFilters();
}

function updateGranButtons(from, to) {
  // Daily data covers: start of prev quarter → today
  // We check if the requested range overlaps with our daily data window at all
  const { dailyFrom, dailyTo } = getQuarterBounds();
  // Allow daily if there's any overlap between [from,to] and [dailyFrom, dailyTo]
  const hasDaily = from <= dailyTo && to >= dailyFrom;
  const daysDiff = (to - from) / 86400000;
  const btnD = document.getElementById('granDaily');
  const btnW = document.getElementById('granWeekly');
  if (btnD) btnD.disabled = !hasDaily;
  if (btnW) btnW.disabled = daysDiff > 366;
  if (GRAN === 'daily'  && btnD && btnD.disabled) { GRAN = 'weekly';  setGran('weekly'); }
  if (GRAN === 'weekly' && btnW && btnW.disabled) { GRAN = 'monthly'; setGran('monthly'); }
}

function getRowDate(r) { return new Date((r.date||r.month)+'T00:00:00Z'); }

function filterRows(arr, fromDate, toDate, studios, sources) {
  return arr.filter(r => {
    const d = getRowDate(r);
    if (fromDate && d < fromDate) return false;
    if (toDate   && d > toDate)   return false;
    if (studios  && !studios.includes(r.studio)) return false;
    if (sources  && !sources.includes(r.source)) return false;
    return true;
  });
}

function sumRows(rows) {
  return rows.reduce((acc,r) => {
    acc.leads += r.signups||0; acc.ft += r.first_visits||0; acc.mem += r.first_activations||0;
    return acc;
  }, {leads:0,ft:0,mem:0});
}

function computeWindows(from, to) {
  const ms = to - from;
  const ppTo   = new Date(from.getTime()-86400000);
  const ppFrom = new Date(ppTo.getTime()-ms);
  const pyFrom = new Date(from); pyFrom.setFullYear(pyFrom.getFullYear()-1);
  const pyTo   = new Date(to);   pyTo.setFullYear(pyTo.getFullYear()-1);
  return {ppFrom,ppTo,pyFrom,pyTo};
}

function fmtDate(d) { return d.toLocaleString('en-US',{month:'short',year:'numeric'}); }

function fmtMonthLabel(iso) { return new Date(iso+'T00:00:00Z').toLocaleString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'}); }

function fmtDayLabel(iso)   { return new Date(iso+'T00:00:00Z').toLocaleString('en-US',{month:'short',day:'numeric',timeZone:'UTC'}); }

function toTimeSeries(dailyRows, monthlyRows) {
  if (GRAN === 'daily') {
    const map = {};
    dailyRows.forEach(r => {
      if (!map[r.date]) map[r.date]={key:r.date,label:fmtDayLabel(r.date),signups:0,first_visits:0,first_activations:0};
      map[r.date].signups+=r.signups||0; map[r.date].first_visits+=r.first_visits||0; map[r.date].first_activations+=r.first_activations||0;
    });
    return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
  }
  if (GRAN === 'weekly') {
    const map = {};
    [...dailyRows,...monthlyRows].forEach(r => {
      const d = new Date((r.date||r.month)+'T00:00:00Z');
      const day = d.getUTCDay()||7;
      const ws = new Date(d); ws.setUTCDate(d.getUTCDate()-day+1);
      const key = ws.toISOString().slice(0,10);
      if (!map[key]) map[key]={key,label:fmtDayLabel(key),signups:0,first_visits:0,first_activations:0};
      map[key].signups+=r.signups||0; map[key].first_visits+=r.first_visits||0; map[key].first_activations+=r.first_activations||0;
    });
    return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
  }
  // monthly
  const map = {};
  [...dailyRows,...monthlyRows].forEach(r => {
    const key=(r.date||r.month).slice(0,7)+'-01';
    if (!map[key]) map[key]={key,label:fmtMonthLabel(key),signups:0,first_visits:0,first_activations:0};
    map[key].signups+=r.signups||0; map[key].first_visits+=r.first_visits||0; map[key].first_activations+=r.first_activations||0;
  });
  return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
}

function calcDelta(curr,prev) { if(!prev||prev===0) return null; return (curr-prev)/prev*100; }

function srcColor(src,i){return SRC_COLOR_MAP[src]||SRC_COLORS[i%SRC_COLORS.length];}

// Chart instance registry  tracks all area chart instances for destroy/recreate
const _areaCharts = {};

function buildAreaChart(canvasId, togglesId, series, srcList, valueKey){
  const ctx = document.getElementById(canvasId);
  if (!ctx) return null;

  // Destroy existing chart instance
  if (_areaCharts[canvasId]) { _areaCharts[canvasId].destroy(); _areaCharts[canvasId] = null; }

  const timeKeys = [...new Set(series.map(r=>r.key))].sort();

  // Sort biggest first = bottom of stack
  const srcTotals = srcList.map(src => series.filter(r=>r.source===src).reduce((s,r)=>s+(r[valueKey]||0),0));
  const sortedSrcs = [...srcList.keys()].sort((a,b)=>srcTotals[b]-srcTotals[a]).map(i=>srcList[i]);

  const datasets = sortedSrcs.map(src => {
    const c = srcColor(src, srcList.indexOf(src));
    return { label:src, data:timeKeys.map(tk=>series.filter(r=>r.key===tk&&r.source===src).reduce((s,r)=>s+(r[valueKey]||0),0)),
      borderColor:c, backgroundColor:c+'cc', borderWidth:1, pointRadius:0, pointHoverRadius:5, tension:.35, fill:true };
  });

  const isMob = window.innerWidth<=768;
  const labels = timeKeys.map(k => k.length===10 && k.slice(8)!=='01'
    ? new Date(k+'T00:00:00Z').toLocaleString('en-US',{month:'short',day:'numeric',timeZone:'UTC'})
    : new Date(k.slice(0,7)+'-01T00:00:00Z').toLocaleString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'}));

  const chart = new Chart(ctx, {
    type:'line', data:{labels, datasets},
    options:{
      responsive:true, maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:false}, tooltip:{mode:'index',intersect:false,
        callbacks:{
          label: item => ' '+item.dataset.label+': '+item.parsed.y.toLocaleString(),
          afterBody: items => { const t=items.filter(i=>!i.dataset.hidden).reduce((s,i)=>s+i.parsed.y,0); return ['','Total: '+t.toLocaleString()]; }
        }
      }},
      scales:{
        x:{grid:{color:'rgba(0,0,0,.05)'},ticks:{font:{size:10},color:'#5a7a8a',autoSkip:true,maxTicksLimit:isMob?5:12,maxRotation:0}},
        y:{grid:{color:'rgba(0,0,0,.05)'},ticks:{font:{size:11},color:'#5a7a8a'},stacked:true,min:0}
      }
    }
  });
  _areaCharts[canvasId] = chart;

  // Build toggle buttons  reference chart via registry, not closure
  const tg = document.getElementById(togglesId);
  if (tg) {
    tg.innerHTML = '';
    sortedSrcs.forEach((src, btnI) => {
      const c = srcColor(src, srcList.indexOf(src));
      const btn = document.createElement('button');
      btn.className = 'tog-btn on';
      btn.style.cssText = 'border-color:'+c+';background:'+c+';color:#fff;font-size:11px;padding:4px 10px';
      btn.innerHTML = '<span class="dot" style="background:#fff"></span>'+src;
      btn.dataset.canvasId = canvasId;
      btn.dataset.dsIdx = btnI;
      btn.onclick = function() {
        const ch = _areaCharts[this.dataset.canvasId];
        if (!ch) return;
        const ds = ch.data.datasets[parseInt(this.dataset.dsIdx)];
        ds.hidden = !ds.hidden;
        this.classList.toggle('on');
        if (this.classList.contains('on')) { this.style.background=c; this.style.borderColor=c; this.style.color='#fff'; }
        else { this.style.background='#f4f9fd'; this.style.borderColor='#c0d4df'; this.style.color='#5a7a8a'; }
        ch.update();
      };
      tg.appendChild(btn);
    });
  }
  return chart;
}

function buildRingChart(canvasId,legendId,labels,values){
  const ctx=document.getElementById(canvasId);if(!ctx)return;
  if(_areaCharts[canvasId]){_areaCharts[canvasId].destroy();_areaCharts[canvasId]=null;}
  const total=values.reduce((s,v)=>s+v,0);
  const colors=labels.map((l,i)=>srcColor(l,i));
  _areaCharts[canvasId]=new Chart(ctx,{type:'doughnut',data:{labels,datasets:[{data:values,backgroundColor:colors,borderColor:'#fff',borderWidth:2,hoverOffset:6}]},options:{responsive:true,maintainAspectRatio:true,plugins:{legend:{display:false}},cutout:'65%'}});
  const leg=document.getElementById(legendId);
  if(leg)leg.innerHTML=labels.map((l,i)=>{const pct=total?(values[i]/total*100).toFixed(1):0;return '<div class="ring-legend-item"><span class="ring-legend-dot" style="background:'+colors[i]+'"></span><span>'+l+'</span><span style="margin-left:auto;font-weight:600;color:#1a1a2e">'+pct+'%</span></div>';}).join('');
}

function buildStudioRankTable(tableId,rows,valueKey){
  const tbody=document.querySelector('#'+tableId+' tbody');if(!tbody)return;
  const byS={};rows.forEach(r=>{byS[r.studio]=(byS[r.studio]||0)+(r[valueKey]||0);});
  const sorted=Object.entries(byS).sort((a,b)=>b[1]-a[1]);
  const total=sorted.reduce((s,[,v])=>s+v,0);
  tbody.innerHTML=sorted.map(([studio,val])=>{const pct=total?(val/total*100).toFixed(1):0;const bw=total?Math.round(val/total*80):0;return '<tr><td><span class="bar-mini" style="width:'+bw+'px"></span>'+studio.replace('SWEAT440 ','')+'</td><td class="num">'+val.toLocaleString()+'</td><td class="pct">'+pct+'%</td></tr>';}).join('');
}

function kpiCard(label,val,pp,py,fmt){
  const f=fmt||(v=>Number.isFinite(v)?v.toLocaleString():v);
  const dpp=pp!=null&&pp!==0?(val-pp)/pp*100:null;
  const dpy=py!=null&&py!==0?(val-py)/py*100:null;
  const ppH=dpp!=null?'<div class="kpi-card-delta '+(dpp>=0?'pos':'neg')+'">'+(dpp>=0?'&#9650;':'&#9660;')+' '+Math.abs(dpp).toFixed(1)+'% prev period</div>':'';
  const pyH=dpy!=null?'<div class="kpi-card-delta '+(dpy>=0?'pos':'neg')+'">'+(dpy>=0?'&#9650;':'&#9660;')+' '+Math.abs(dpy).toFixed(1)+'% prev year</div>':'';
  return '<div class="kpi-card"><div class="kpi-card-label">'+label+'</div><div class="kpi-card-val">'+f(val)+'</div>'+ppH+pyH+'</div>';
}

function toSourceTimeSeries(dailyRows,monthlyRows,gran){
  const g=gran||GRAN;
  const map={};
  const allRows=g==='daily'?dailyRows:[...dailyRows,...monthlyRows];
  allRows.forEach(r=>{
    let key;
    if(g==='daily')key=r.date;
    else if(g==='weekly'){const d=new Date((r.date||r.month)+'T00:00:00Z');const day=d.getUTCDay()||7;const ws=new Date(d);ws.setUTCDate(d.getUTCDate()-day+1);key=ws.toISOString().slice(0,10);}
    else key=(r.date||r.month).slice(0,7)+'-01';
    const mk=key+'|'+r.source;
    if(!map[mk])map[mk]={key,source:r.source,signups:0,first_visits:0,first_activations:0};
    map[mk].signups+=r.signups||0;map[mk].first_visits+=r.first_visits||0;map[mk].first_activations+=r.first_activations||0;
  });
  return Object.values(map).sort((a,b)=>a.key.localeCompare(b.key));
}

function cprKpiCard(label,val,pp,py,fmt){
  // For cost metrics: decrease is GOOD (green), increase is BAD (red)
  const f=fmt||(v=>Number.isFinite(v)?v.toLocaleString():v);
  const dpp=pp!=null&&pp!==0?(val-pp)/pp*100:null;
  const dpy=py!=null&&py!==0?(val-py)/py*100:null;
  const ppH=dpp!=null?'<div class="kpi-card-delta '+(dpp<=0?'pos':'neg')+'">'+(dpp<=0?'&#9660;':'&#9650;')+' '+Math.abs(dpp).toFixed(1)+'% prev period</div>':'';
  const pyH=dpy!=null?'<div class="kpi-card-delta '+(dpy<=0?'pos':'neg')+'">'+(dpy<=0?'&#9660;':'&#9650;')+' '+Math.abs(dpy).toFixed(1)+'% prev year</div>':'';
  return '<div class="kpi-card"><div class="kpi-card-label">'+label+'</div><div class="kpi-card-val">'+f(val)+'</div>'+ppH+pyH+'</div>';
}

function getMock() {
  const studios = [
    'SWEAT440 Miami Beach','SWEAT440 Brickell','SWEAT440 Wynwood',
    'SWEAT440 Coral Gables','SWEAT440 Doral','SWEAT440 Miami Lakes'
  ];
  const sources = [
    'Website (unattributed)','Business Mode','Meta Ads','Google Ads',
    'SWEAT440 App','MindBody App','Social Media Organic','Local Listings',
    'Word of Mouth','Print Ads / Signs','N/A','Other'
  ];
  // Deterministic seed — no Math.random()
  function seed(studio, source, month) {
    const si = studios.indexOf(studio), ri = sources.indexOf(source);
    const mi = parseInt(month.slice(5,7));
    return (si+1)*7 + (ri+1)*3 + mi;
  }
  const monthly = [];
  for (let y=2023; y<=2025; y++) {
    for (let mo=1; mo<=12; mo++) {
      const month = y+'-'+String(mo).padStart(2,'0')+'-01';
      studios.forEach(st => {
        sources.forEach(sr => {
          const b = seed(st, sr, month);
          monthly.push({ month, studio:st, source:sr,
            signups:           b,
            first_visits:      Math.floor(b*0.7),
            first_activations: Math.floor(b*0.15),
            first_sales:       Math.floor(b*0.08),
          });
        });
      });
    }
  }
  // Daily: last 90 days, also deterministic
  const daily = [];
  const baseMs = new Date('2026-01-20T00:00:00Z').getTime(); // JS will compute this correctly at runtime
  for (let i=89; i>=0; i--) {
    const d = new Date(baseMs - i*86400000);
    const ds = d.getUTCFullYear()+'-'+String(d.getUTCMonth()+1).padStart(2,'0')+'-'+String(d.getUTCDate()).padStart(2,'0');
    studios.forEach((st,si) => {
      sources.forEach((sr,ri) => {
        const b = (si+1)*2 + (ri+1) + (i%7);
        daily.push({ date:ds, studio:st, source:sr,
          signups:           b,
          first_visits:      Math.floor(b*0.7),
          first_activations: Math.floor(b*0.15),
          first_sales:       Math.floor(b*0.08),
        });
      });
    });
  }
  return { studios, sources, monthly_detail:monthly, daily_detail:daily };
}


loadData();
