/**
 * main.js 
 */

const API = 'http://127.0.0.1:5000/api';

const charts    = {};
let currentPage  = 1;
let tableFilters = {};
let totalTrips   = 0;
let mapLoaded    = false;
let _chartData   = {};

/* 
   Boot
 */
document.addEventListener('DOMContentLoaded', async () => {
  setupNav();
  setupFilters();
  setupTabs();
  setupTheme();
  setupUploadModal();

  document.getElementById('loadingMsg').textContent = 'Checking server…';
  const alive = await pingHealth();
  if (!alive) {
    document.getElementById('loadingScreen').classList.add('gone');
    document.getElementById('errorScreen').style.display = 'flex';
    return;
  }

  // We await the warmup before loading charts so that all 11 chart
  // requests hit the server-side cache instead of running cold GROUP BY
  // queries simultaneously on 1.4M rows. Previously warmup was called
  // fire-and-forget which meant charts arrived before warmup finished.
  document.getElementById('loadingMsg').textContent = 'Warming up…';
  await fetch(`${API}/warmup`).catch(() => {});

  document.getElementById('loadingScreen').classList.add('gone');
  get('/overview').then(fillKpis).catch(e => console.warn('KPI:', e));
  loadTable(1);
  loadAllCharts();
});

/* 
   Chart loading  ALL endpoints in parallel
 */
function loadAllCharts() {
  Promise.all([
    get('/hourly').catch(() => []),
    get('/weekday').catch(() => []),
    get('/monthly').catch(() => []),
    get('/vendors').catch(() => []),
    get('/passengers').catch(() => []),
    get('/speed-dist').catch(() => []),
    get('/distance-dist').catch(() => []),
    get('/time-category').catch(() => []),
    get('/weekend-weekday').catch(() => []),
    get('/rush-hour-insight').catch(() => []),
    get('/excluded-stats').catch(() => []),
  ]).then(([hourly, weekday, monthly, vendors, passengers,
            speedDist, distDist, timeCat, ww, rush, excluded]) => {
    _chartData = { hourly, weekday, monthly, vendors, passengers,
                   speedDist, distDist, timeCat, ww, rush, excluded };
    fillVendorBars(vendors);
    destroyAll();
    _buildCharts(_chartData);
    fillInsights(hourly, ww, rush);
    const hasAny = hourly?.length || weekday?.length || vendors?.length;
    if (!hasAny) showUploadBanner();
  }).catch(e => console.warn('Charts:', e));
}

/* 
   Health check
 */
async function pingHealth(retries = 2) {
  for (let i = 0; i <= retries; i++) {
    try {
      const r = await fetch(`${API}/health`, { signal: AbortSignal.timeout(4000) });
      if (r.ok) return true;
    } catch (_) {}
    if (i < retries) await sleep(800);
  }
  return false;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/* 
   Upload banner
 */
function showUploadBanner() {
  const b = document.getElementById('uploadBanner');
  if (b) b.style.display = 'flex';
}
function hideUploadBanner() {
  const b = document.getElementById('uploadBanner');
  if (b) b.style.display = 'none';
}

/* 
   Upload modal
 */
function setupUploadModal() {
  document.querySelectorAll('[data-open-upload]').forEach(el =>
    el.addEventListener('click', openUploadModal));
  document.getElementById('uploadModalClose')?.addEventListener('click', closeUploadModal);
  document.getElementById('uploadModalOverlay')?.addEventListener('click', e => {
    if (e.target === e.currentTarget) closeUploadModal();
  });
  document.getElementById('uploadFileInput')?.addEventListener('change', e => {
    setText('uploadFileName', e.target.files[0]?.name || 'No file selected');
  });
  document.getElementById('uploadForm')?.addEventListener('submit', handleUpload);
}

function openUploadModal() {
  const o = document.getElementById('uploadModalOverlay');
  if (o) o.style.display = 'flex';
}

function closeUploadModal() {
  const o = document.getElementById('uploadModalOverlay');
  if (o) o.style.display = 'none';
  const btn = document.getElementById('uploadSubmit');
  if (btn) { btn.disabled = false; btn.textContent = 'Run Pipeline'; }
  const bw = document.getElementById('uploadBarWrap');
  if (bw) bw.style.display = 'none';
  const bf = document.getElementById('uploadBarFill');
  if (bf) bf.style.width = '0%';
  const st = document.getElementById('uploadStatus');
  if (st) st.style.display = 'none';
}

async function handleUpload(e) {
  e.preventDefault();
  const fileInput  = document.getElementById('uploadFileInput');
  const sampleInput = document.getElementById('uploadSample');
  const btn        = document.getElementById('uploadSubmit');
  const barFill    = document.getElementById('uploadBarFill');
  const barWrap    = document.getElementById('uploadBarWrap');

  if (!fileInput?.files[0]) {
    setUploadStatus('Please select a CSV file first.', 'error'); return;
  }
  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  const sample = parseInt(sampleInput?.value || '0', 10);
  if (sample > 0) fd.append('sample', sample);

  if (btn) { btn.disabled = true; btn.textContent = 'Uploading…'; }
  setUploadStatus('Sending file…', 'info');
  if (barWrap) barWrap.style.display = 'block';
  if (barFill) barFill.style.width = '5%';

  try {
    const res  = await fetch(`${API}/upload`, { method: 'POST', body: fd });
    const json = await res.json();
    if (!res.ok) {
      setUploadStatus(json.error || 'Upload failed.', 'error');
      if (btn) { btn.disabled = false; btn.textContent = 'Run Pipeline'; }
      return;
    }
    setUploadStatus('File received  running data pipeline…', 'info');
    pollPipeline(btn, barFill, barWrap);
  } catch {
    setUploadStatus('Could not reach the server. Is it running?', 'error');
    if (btn) { btn.disabled = false; btn.textContent = 'Run Pipeline'; }
  }
}

function setUploadStatus(msg, type) {
  const el = document.getElementById('uploadStatus');
  if (!el) return;
  el.textContent = msg;
  el.className   = 'upload-status ' + (type || '');
  el.style.display = 'block';
}

function pollPipeline(btn, barFill, barWrap) {
  let n = 0;
  const timer = setInterval(async () => {
    if (++n > 300) {
      clearInterval(timer);
      setUploadStatus('Timed out  check server logs.', 'error');
      if (btn) { btn.disabled = false; btn.textContent = 'Retry'; }
      return;
    }
    try {
      const st = await get('/pipeline-status');
      if (barFill) barFill.style.width = (st.progress || 5) + '%';
      if (st.message) setUploadStatus(st.message, st.state === 'error' ? 'error' : 'info');
      if (st.state === 'done') {
        clearInterval(timer);
        setUploadStatus('✓ Done! Dashboard is refreshing…', 'success');
        if (barFill) barFill.style.width = '100%';
        if (btn) btn.textContent = 'Done';
        setTimeout(() => {
          closeUploadModal(); hideUploadBanner();
          get('/overview').then(fillKpis).catch(() => {});
          loadTable(1); loadAllCharts();
          if (btn) { btn.disabled = false; btn.textContent = 'Run Pipeline'; }
        }, 1200);
      } else if (st.state === 'error') {
        clearInterval(timer);
        setUploadStatus('Error: ' + (st.message || 'Unknown'), 'error');
        if (btn) { btn.disabled = false; btn.textContent = 'Retry'; }
      }
    } catch (_) {}
  }, 1500);
}

/* 
   Navigation
 */
function setupNav() {
  document.querySelectorAll('.nav-item[data-page]').forEach(item =>
    item.addEventListener('click', () => showPage(item.dataset.page)));
  showPage('overview');
}

function showPage(pageId) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + pageId)?.classList.add('active');
  document.querySelector(`.nav-item[data-page="${pageId}"]`)?.classList.add('active');
  const titles = { overview:'Overview', time:'Time Patterns',
                   geography:'City Map', insights:'Key Insights', explorer:'Trip Explorer' };
  setText('topbarTitle', titles[pageId] || pageId);
  if (pageId === 'geography' && !mapLoaded) { mapLoaded = true; loadMap(); }

  // Tell Chart.js to re-measure all canvases now that this page is visible
  setTimeout(() => window.dispatchEvent(new Event('resize')), 50);
}

/* 
   Theme
 */
function setupTheme() {
  const html = document.documentElement;
  const btn  = document.getElementById('themeToggle');
  const icon = document.getElementById('themeIcon');
  const saved = localStorage.getItem('urbex-theme') || 'light';
  applyTheme(saved);
  btn.addEventListener('click', () => {
    const next = html.dataset.theme === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    localStorage.setItem('urbex-theme', next);
    rebuildChartsForTheme();
  });
  function applyTheme(t) {
    html.dataset.theme = t;
    icon.setAttribute('data-lucide', t === 'dark' ? 'sun' : 'moon');
    lucide.createIcons();
    btn.title = t === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
  }
}

/* 
   API
 */
async function get(path, params = {}) {
  const url = new URL(API + path);
  Object.entries(params).forEach(([k, v]) => {
    if (v !== '' && v !== null && v !== undefined) url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`API ${res.status} on ${path}`);
  return res.json();
}

/* 
   KPIs
 */
function fillKpis(d) {
  setText('kpiTrips',      fmt(d.total_trips));
  setText('kpiSpeed',      d.avg_speed_kmh);
  setText('kpiDuration',   d.avg_duration_mins);
  setText('kpiDistance',   d.avg_distance_km);
  setText('kpiPassengers', fmt(d.total_passengers));
  setText('kpiExcluded',   fmt(d.excluded_count));
}

/* 
   Vendor bars
 */
function fillVendorBars(vendors) {
  if (!vendors?.length) return;
  const total  = vendors.reduce((s, v) => s + (v.trip_count || 0), 0);
  const colors = ['#F5A623', '#2563EB'];
  document.getElementById('vendorBars').innerHTML = vendors.map((v, i) => {
    const pct   = total > 0 ? (v.trip_count / total * 100).toFixed(1) : '0.0';
    const name  = (v.vendor_name || v.vendor || 'Unknown').split(',')[0].trim();
    return `
      <div class="vendor-bar-row">
        <div class="vendor-bar-header">
          <span class="vendor-bar-name">${name}</span>
          <span class="vendor-bar-count">${fmt(v.trip_count)} · ${pct}%</span>
        </div>
        <div class="vbar-track">
          <div class="vbar-fill" style="width:${pct}%;background:${colors[i % colors.length]};"></div>
        </div>
        <div class="vendor-stats">
          <div class="v-stat"><div class="v-stat-val">${v.avg_speed ?? ''} km/h</div><div class="v-stat-lbl">Avg speed</div></div>
          <div class="v-stat"><div class="v-stat-val">${v.avg_distance ?? ''} km</div><div class="v-stat-lbl">Avg distance</div></div>
          <div class="v-stat"><div class="v-stat-val">${v.avg_duration_mins ?? ''} min</div><div class="v-stat-lbl">Avg duration</div></div>
        </div>
      </div>`;
  }).join('');
}

/* 
   Charts
 */
function destroyAll() {
  Object.values(charts).forEach(c => { try { c?.destroy(); } catch(_) {} });
}

function _buildCharts(d) {
  const safe = (fn, ...args) => { try { return fn(...args); } catch(e) { console.warn(fn.name, e); return null; } };
  charts.hourly       = safe(buildHourlyChart,       d.hourly);
  charts.speedHour    = safe(buildSpeedHourChart,     d.hourly);
  charts.durationHour = safe(buildDurationHourChart,  d.hourly);
  charts.weekday      = safe(buildWeekdayChart,       d.weekday);
  charts.monthly      = safe(buildMonthlyChart,       d.monthly);
  charts.monthlySpd   = safe(buildMonthlySpeedChart,  d.monthly);
  charts.speedDist    = safe(buildSpeedDistChart,     d.speedDist);
  charts.distDist     = safe(buildDistDistChart,      d.distDist);
  charts.timeCat      = safe(buildTimeCatChart,       d.timeCat);
  charts.passenger    = safe(buildPassengerChart,     d.passengers);
  charts.ww           = safe(buildWWChart,            d.ww);
  charts.wwDetail     = safe(buildWWDetailChart,      d.ww);
  charts.excluded     = safe(buildExcludedChart,      d.excluded);
  charts.insight1     = safe(buildInsight1Chart,      d.rush);
  charts.insight2     = safe(buildInsight2Chart,      d.ww);
  charts.insight3     = safe(buildInsight3Chart,      d.hourly);
  charts.rushFull     = safe(buildRushFullChart,      d.hourly);
}

function rebuildChartsForTheme() {
  if (!_chartData.hourly) return;
  destroyAll();
  _buildCharts(_chartData);
}

/* 
   Insights
 */
function fillInsights(hourly, ww, rush) {
  const rh  = rush?.find(r => r.period === 'Rush Hour');
  const off = rush?.find(r => r.period === 'Off-Peak');
  if (rh && off && off.avg_speed > 0)
    setText('ins1Stat', `−${((off.avg_speed - rh.avg_speed) / off.avg_speed * 100).toFixed(1)}%`);
  const wknd = ww?.find(r => r.period === 'Weekend');
  const wkdy = ww?.find(r => r.period === 'Weekday');
  if (wknd && wkdy)
    setText('ins2Stat', `+${((wknd.avg_distance || 0) - (wkdy.avg_distance || 0)).toFixed(2)} km`);
  const night   = hourly?.find(h => h.hour === 2);
  const morning = hourly?.find(h => h.hour === 8);
  if (night && morning && morning.avg_speed > 0)
    setText('ins3Stat', `${(night.avg_speed / morning.avg_speed).toFixed(1)}×`);
}

/* 
   Map (lazy)
 */
let _leafletMap = null;

async function loadMap() {
  if (_leafletMap) return;
  _leafletMap = L.map('map').setView([40.75, -73.97], 11);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    { attribution: '© OpenStreetMap contributors', maxZoom: 19 }).addTo(_leafletMap);
  try {
    const [pts, zones] = await Promise.all([get('/map-points'), get('/top-zones')]);
    pts.forEach(p => L.circleMarker([p.lat, p.lon],
      { radius: 2.5, fillColor: '#F5A623', color: 'transparent', fillOpacity: 0.55, weight: 0 }
    ).addTo(_leafletMap));
    renderTopZones(zones);
  } catch (e) { console.warn('Map:', e); }
}

function renderTopZones(zones) {
  const el = document.getElementById('topZonesTable');
  if (!el) return;
  el.innerHTML = zones.map((z, i) => `
    <div class="zone-row">
      <span class="zone-rank">#${i + 1}</span>
      <span class="zone-coords">${z.lat.toFixed(2)}°N, ${Math.abs(z.lon).toFixed(2)}°W</span>
      <span class="zone-count">${fmt(z.count)} trips</span>
    </div>`).join('');
}

/* 
   Trip Table
 */
function showSkeleton(rows = 10) {
  document.getElementById('tripBody').innerHTML =
    Array.from({length: rows}).map(() =>
      `<tr class="skeleton-row">${Array.from({length: 8}).map(() =>
        '<td><div class="skel-cell"></div></td>').join('')}</tr>`).join('');
}

async function loadTable(page = 1) {
  currentPage = page;
  showSkeleton(25);
  document.getElementById('paginationBtns').innerHTML = '';
  setText('pgInfoChip', 'Loading…');
  try {
    const res = await get('/trips', { page, per_page: 25, ...tableFilters });
    totalTrips = res.total;
    renderRows(res.data);
    renderPagination(res.total, res.page, res.per_page);
    setText('pgInfoChip', `${fmt(res.total)} trips`);
  } catch (_) {
    document.getElementById('tripBody').innerHTML =
      `<tr><td colspan="8" class="tbl-placeholder" style="color:var(--red);">
        Could not load trips. Check the server is running.
      </td></tr>`;
    setText('pgInfoChip', 'Error');
  }
}

function renderRows(rows) {
  const tbody = document.getElementById('tripBody');
  if (!rows?.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="tbl-placeholder">No trips match the current filters.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(r => `
    <tr>
      <td>${r.id}</td>
      <td>${fmtDt(r.pickup_datetime)}</td>
      <td><span class="badge ${r.vendor?.includes('VeriFone') ? 'b-blue' : 'b-yellow'}">
        ${(r.vendor || '').split(',')[0] || ''}
      </span></td>
      <td>${r.passenger_count}</td>
      <td>${fmtSecs(r.trip_duration_secs)}</td>
      <td>${r.distance_km} km</td>
      <td>${r.speed_kmh} km/h</td>
      <td><span class="badge ${catBadge(r.time_category)}">${r.time_category || ''}</span></td>
    </tr>`).join('');
}

function renderPagination(total, page, perPage) {
  const pages = Math.ceil(total / (perPage || 25));
  const el    = document.getElementById('paginationBtns');
  const info  = document.getElementById('pgInfo');
  if (info) info.textContent = `Page ${page} of ${pages.toLocaleString()}`;
  const start = Math.max(1, page - 2);
  const end   = Math.min(pages, page + 2);
  const btns  = [];
  btns.push(mkBtn('‹', page > 1, () => loadTable(page - 1)));
  if (start > 1) { btns.push(mkBtn('1', true, () => loadTable(1))); if (start > 2) btns.push(mkEllipsis()); }
  for (let p = start; p <= end; p++) btns.push(mkBtn(p, true, () => loadTable(p), p === page));
  if (end < pages) { if (end < pages - 1) btns.push(mkEllipsis()); btns.push(mkBtn(pages, true, () => loadTable(pages))); }
  btns.push(mkBtn('›', page < pages, () => loadTable(page + 1)));
  el.innerHTML = '';
  btns.forEach(b => el.appendChild(b));
}

function mkBtn(label, enabled, onClick, active = false) {
  const b = document.createElement('button');
  b.className   = 'pg-btn' + (active ? ' active' : '');
  b.textContent = label;
  b.disabled    = !enabled;
  if (enabled) b.onclick = onClick;
  return b;
}

function mkEllipsis() {
  const s = document.createElement('span');
  s.textContent = '…';
  s.style.cssText = 'padding:0 4px;color:var(--text-m);font-family:var(--font-mono);font-size:13px;';
  return s;
}

/* 
   Filters
 */
function setupFilters() {
  document.getElementById('btnApply')?.addEventListener('click', () => {
    tableFilters = {};
    const v  = document.getElementById('fVendor')?.value;
    const wk = document.getElementById('fWeekend')?.value;
    const h  = document.getElementById('fHour')?.value;
    const mn = document.getElementById('fMinSpeed')?.value;
    const mx = document.getElementById('fMaxSpeed')?.value;
    if (v)  tableFilters.vendor_id  = v;
    if (wk) tableFilters.is_weekend = wk;
    if (h)  tableFilters.hour       = h;
    if (mn) tableFilters.min_speed  = mn;
    if (mx) tableFilters.max_speed  = mx;
    loadTable(1);
  });
  document.getElementById('btnReset')?.addEventListener('click', () => {
    tableFilters = {};
    ['fVendor','fWeekend','fHour','fMinSpeed','fMaxSpeed']
      .forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    loadTable(1);
  });
}

/* 
   Tabs
 */
function setupTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const group = btn.dataset.group;
      document.querySelectorAll(`[data-group="${group}"].tab-btn`).forEach(b => b.classList.remove('active'));
      document.querySelectorAll(`[data-tabs="${group}"] .tab-content`).forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(btn.dataset.tab)?.classList.add('active');
    });
  });
}

/* 
   Utilities
 */
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = (val !== null && val !== undefined) ? val : '';
}
function fmt(n) {
  const num = Number(n);
  return isNaN(num) ? '' : num.toLocaleString('en-US');
}
function fmtSecs(s) {
  const total = Math.round(Number(s));
  return `${Math.floor(total / 60)}m ${total % 60}s`;
}
function fmtDt(dt) { return dt ? String(dt).slice(0, 16) : ''; }
function catBadge(c) {
  return { morning: 'b-yellow', afternoon: 'b-blue', evening: 'b-green', night: 'b-red' }[c] ?? 'b-grey';
}

// Mobile sidebar toggle
const menuToggle      = document.getElementById('menuToggle');
const sidebar         = document.querySelector('.sidebar');
const sidebarOverlay  = document.getElementById('sidebarOverlay');

function openSidebar() {
  sidebar.classList.add('open');
  sidebarOverlay.classList.add('visible');
  menuToggle.classList.add('open');
  document.body.style.overflow = 'hidden'; // prevent scroll behind drawer
}

function closeSidebar() {
  sidebar.classList.remove('open');
  sidebarOverlay.classList.remove('visible');
  menuToggle.classList.remove('open');
  document.body.style.overflow = '';
}

menuToggle.addEventListener('click', () => {
  sidebar.classList.contains('open') ? closeSidebar() : openSidebar();
});

// Close when clicking the overlay
sidebarOverlay.addEventListener('click', closeSidebar);

// Close sidebar automatically when a nav item is tapped on mobile
document.querySelectorAll('.nav-item').forEach(item => {
  item.addEventListener('click', () => {
    if (window.innerWidth <= 900) closeSidebar();
  });
});