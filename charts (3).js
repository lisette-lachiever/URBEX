/**
 * charts.js 
 *
 * Every chart function reads CSS variables so colours update
 * automatically when the user switches between light and dark themes.
 * Every function has a null / empty-data guard  an empty dataset
 * renders a polite placeholder rather than crashing.
 */

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function palette() {
  return {
    yellow:     cssVar('--yellow')      || '#F5A623',
    yellowFill: cssVar('--yellow-mid')  || 'rgba(245,166,35,0.22)',
    blue:       cssVar('--blue')        || '#2563EB',
    blueFill:   cssVar('--blue-soft')   || 'rgba(37,99,235,0.10)',
    green:      cssVar('--green')       || '#059669',
    red:        cssVar('--red')         || '#DC2626',
    purple:     cssVar('--purple')      || '#7C3AED',
    gridLine:   cssVar('--chart-grid')  || 'rgba(0,0,0,0.05)',
    text:       cssVar('--chart-text')  || '#6B7280',
  };
}

Chart.defaults.font.family = "'Inter', sans-serif";
Chart.defaults.font.size   = 11;
Chart.defaults.plugins.legend.display = false;

function hasData(data) {
  return Array.isArray(data) && data.length > 0;
}

function drawNoData(canvasId) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return null;
  const w = canvas.offsetWidth || 300;
  const h = canvas.offsetHeight || 120;
  canvas.width  = w;
  canvas.height = h;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle   = cssVar('--text-m') || '#9CA3AF';
  ctx.font        = "13px 'Inter', sans-serif";
  ctx.textAlign   = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText('Upload a dataset to see this chart.', w / 2, h / 2);
  return null;
}

function baseScales(yLabel = '', xLabel = '') {
  const C = palette();
  return {
    x: {
      grid: { color: C.gridLine, drawBorder: false },
      ticks: { color: C.text },
      title: xLabel ? { display: true, text: xLabel, color: C.text, font: { size: 11 } } : { display: false },
    },
    y: {
      grid: { color: C.gridLine, drawBorder: false },
      ticks: { color: C.text },
      title: yLabel ? { display: true, text: yLabel, color: C.text, font: { size: 11 } } : { display: false },
    },
  };
}

function tooltipDefaults() {
  return {
    backgroundColor: cssVar('--surface') || '#fff',
    titleColor:      cssVar('--text-h')  || '#0F1629',
    bodyColor:       cssVar('--text-s')  || '#6B7280',
    borderColor:     cssVar('--border')  || '#E8EBF4',
    borderWidth:     1, padding: 10, cornerRadius: 8,
  };
}

/*  Hourly trip volume  */
function buildHourlyChart(data) {
  if (!hasData(data)) return drawNoData('chartHourly');
  const C   = palette();
  const ctx = document.getElementById('chartHourly')?.getContext('2d');
  if (!ctx) return null;
  const colours = data.map(d => {
    const h = d.hour;
    return (h >= 7 && h <= 9) || (h >= 16 && h <= 19) ? C.yellow : C.blue;
  });
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels:   data.map(d => `${String(d.hour).padStart(2,'0')}h`),
      datasets: [{ label: 'Trips', data: data.map(d => d.trip_count),
        backgroundColor: colours, borderRadius: 4, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toLocaleString()} trips` } } },
      scales: baseScales('Trips'),
    },
  });
}

/*  Avg speed by hour*/
function buildSpeedHourChart(data) {
  if (!hasData(data)) return drawNoData('chartSpeedHour');
  const C   = palette();
  const ctx = document.getElementById('chartSpeedHour')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => `${String(d.hour).padStart(2,'0')}h`),
      datasets: [{ label: 'Avg speed', data: data.map(d => d.avg_speed),
        borderColor: C.yellow, borderWidth: 2.5, pointRadius: 3,
        pointBackgroundColor: C.yellow, tension: 0.4, fill: true, backgroundColor: C.yellowFill }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(1)} km/h` } } },
      scales: baseScales('km/h'),
    },
  });
}

/* Avg duration by hour */
function buildDurationHourChart(data) {
  if (!hasData(data)) return drawNoData('chartDurationHour');
  const C   = palette();
  const ctx = document.getElementById('chartDurationHour')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => `${String(d.hour).padStart(2,'0')}h`),
      datasets: [{ label: 'Avg duration (min)', data: data.map(d => d.avg_duration_mins),
        backgroundColor: data.map(d => {
          const h = d.hour;
          return (h >= 7 && h <= 9) || (h >= 16 && h <= 19) ? C.yellow : (C.blueFill || 'rgba(37,99,235,0.15)');
        }), borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(1)} min` } } },
      scales: baseScales('Minutes'),
    },
  });
}

/* Day of week */
function buildWeekdayChart(data) {
  if (!hasData(data)) return drawNoData('chartWeekday');
  const C   = palette();
  const ctx = document.getElementById('chartWeekday')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => (d.day_name || '').slice(0, 3)),
      datasets: [{ label: 'Trips', data: data.map(d => d.trip_count),
        backgroundColor: data.map(d => ((d.dow ?? d.day_of_week) >= 5) ? C.yellow : C.blue),
        borderRadius: 4, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toLocaleString()} trips` } } },
      scales: baseScales('Trips'),
    },
  });
}

/*  Monthly trend */
function buildMonthlyChart(data) {
  if (!hasData(data)) return drawNoData('chartMonthly');
  const C   = palette();
  const ctx = document.getElementById('chartMonthly')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => d.month_name),
      datasets: [{ label: 'Trips', data: data.map(d => d.trip_count),
        borderColor: C.blue, borderWidth: 2.5, pointRadius: 5,
        pointBackgroundColor: C.blue, tension: 0.3, fill: true, backgroundColor: C.blueFill }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults() } },
      scales: baseScales('Trips'),
    },
  });
}

/* Monthly speed  */
function buildMonthlySpeedChart(data) {
  if (!hasData(data)) return drawNoData('chartMonthlySpeed');
  const C   = palette();
  const ctx = document.getElementById('chartMonthlySpeed')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.map(d => d.month_name),
      datasets: [{ label: 'Avg speed (km/h)', data: data.map(d => d.avg_speed),
        borderColor: C.green, borderWidth: 2.5, pointRadius: 5,
        pointBackgroundColor: C.green, tension: 0.3, fill: true,
        backgroundColor: 'rgba(5,150,105,0.08)' }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(1)} km/h` } } },
      scales: baseScales('km/h'),
    },
  });
}

/* Speed histogram  */
function buildSpeedDistChart(data) {
  if (!hasData(data)) return drawNoData('chartSpeedDist');
  const C   = palette();
  const ctx = document.getElementById('chartSpeedDist')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => `${d.bucket_start}–${d.bucket_start + 5}`),
      datasets: [{ label: 'Trips', data: data.map(d => d.count),
        backgroundColor: C.yellowFill || 'rgba(245,166,35,0.2)',
        borderColor: C.yellow, borderWidth: 1, borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults() } },
      scales: baseScales('Trips', 'km/h'),
    },
  });
}

/* Distance histogram  */
function buildDistDistChart(data) {
  if (!hasData(data)) return drawNoData('chartDistDist');
  const C   = palette();
  const ctx = document.getElementById('chartDistDist')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => `${d.bucket_km} km`),
      datasets: [{ label: 'Trips', data: data.map(d => d.count),
        backgroundColor: C.blueFill || 'rgba(37,99,235,0.12)',
        borderColor: C.blue, borderWidth: 1, borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults() } },
      scales: baseScales('Trips', 'km'),
    },
  });
}

/*Time-of-day doughnut */
function buildTimeCatChart(data) {
  if (!hasData(data)) return drawNoData('chartTimeCat');
  const C   = palette();
  const ctx = document.getElementById('chartTimeCat')?.getContext('2d');
  if (!ctx) return null;
  const pal = [C.blue, C.yellow, C.green, C.purple];
  // Safe label  category could be null if the join returned nothing
  const safeLabel = s => (s && typeof s === 'string')
    ? s.charAt(0).toUpperCase() + s.slice(1) : 'Unknown';
  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: data.map(d => safeLabel(d.category)),
      datasets: [{
        data: data.map(d => d.trip_count),
        backgroundColor: pal,
        borderWidth: 3, borderColor: cssVar('--surface') || '#fff', hoverOffset: 6,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '68%',
      plugins: {
        legend: { display: true, position: 'bottom',
          labels: { boxWidth: 10, padding: 12, color: C.text, font: { size: 11 } } },
        tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.label}: ${c.parsed.toLocaleString()}` } },
      },
    },
  });
}

/* Passenger bar */
function buildPassengerChart(data) {
  if (!hasData(data)) return drawNoData('chartPassenger');
  const C   = palette();
  const ctx = document.getElementById('chartPassenger')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => `${d.pax} pax`),
      datasets: [{ label: 'Trips', data: data.map(d => d.trip_count),
        backgroundColor: data.map((_, i) => i === 0 ? C.yellow : C.blue),
        borderRadius: 4, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults() } },
      scales: baseScales('Trips'),
    },
  });
}

/*  Weekend vs Weekday bar */
function buildWWChart(data) {
  if (!hasData(data)) return drawNoData('chartWW');
  const C   = palette();
  const ctx = document.getElementById('chartWW')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.period),
      datasets: [{ label: 'Trips', data: data.map(d => d.trip_count),
        backgroundColor: [C.yellow, C.blue], borderRadius: 6, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.x.toLocaleString()} trips` } } },
      scales: {
        x: { grid: { color: C.gridLine }, ticks: { color: C.text } },
        y: { grid: { color: 'transparent' }, ticks: { color: C.text } },
      },
    },
  });
}

/*  Weekend vs Weekday detail */
function buildWWDetailChart(data) {
  if (!hasData(data)) return drawNoData('chartWWDetail');
  const C   = palette();
  const ctx = document.getElementById('chartWWDetail')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.period),
      datasets: [
        { label: 'Avg speed (km/h)', data: data.map(d => d.avg_speed),
          backgroundColor: C.yellow, borderRadius: 4, borderSkipped: false },
        { label: 'Avg distance (km)', data: data.map(d => d.avg_distance),
          backgroundColor: C.blue, borderRadius: 4, borderSkipped: false },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: true, position: 'bottom',
          labels: { boxWidth: 10, padding: 12, color: palette().text, font: { size: 11 } } },
        tooltip: { ...tooltipDefaults() },
      },
      scales: baseScales('Value'),
    },
  });
}

/*  Excluded records bar */
function buildExcludedChart(data) {
  if (!hasData(data)) return drawNoData('chartExcluded');
  const C   = palette();
  const ctx = document.getElementById('chartExcluded')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.reason || 'Unknown'),
      datasets: [{ label: 'Records', data: data.map(d => d.count),
        backgroundColor: C.red, borderRadius: 4, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, indexAxis: 'y',
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.x.toLocaleString()} records` } } },
      scales: {
        x: { grid: { color: C.gridLine }, ticks: { color: C.text } },
        y: { grid: { color: 'transparent' }, ticks: { color: C.text, font: { size: 10 } } },
      },
    },
  });
}

/*  Insight 1: Rush vs off-peak speed  */
function buildInsight1Chart(data) {
  if (!hasData(data)) return drawNoData('chartInsight1');
  const C   = palette();
  const ctx = document.getElementById('chartInsight1')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: data.map(d => d.period),
      datasets: [{ data: data.map(d => d.avg_speed),
        backgroundColor: [C.red, C.green], borderWidth: 2, borderColor: cssVar('--surface') || '#fff' }],
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '60%',
      plugins: {
        legend: { display: true, position: 'bottom', labels: { boxWidth: 10, padding: 8, color: C.text, font: { size: 10 } } },
        tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.toFixed(1)} km/h avg` } },
      },
    },
  });
}

/* Insight 2: Weekend vs weekday distance  */
function buildInsight2Chart(data) {
  if (!hasData(data)) return drawNoData('chartInsight2');
  const C   = palette();
  const ctx = document.getElementById('chartInsight2')?.getContext('2d');
  if (!ctx) return null;
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => d.period),
      datasets: [{ label: 'Avg distance (km)', data: data.map(d => d.avg_distance),
        backgroundColor: [C.yellow, C.blue], borderRadius: 6, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(2)} km` } } },
      scales: baseScales('km'),
    },
  });
}

/* Insight 3: Speed curve (night vs day) */
function buildInsight3Chart(data) {
  if (!hasData(data)) return drawNoData('chartInsight3');
  const C      = palette();
  const ctx    = document.getElementById('chartInsight3')?.getContext('2d');
  if (!ctx) return null;
  const subset = data.filter(d => d.hour <= 12);
  return new Chart(ctx, {
    type: 'line',
    data: {
      labels: subset.map(d => `${String(d.hour).padStart(2,'0')}h`),
      datasets: [{ label: 'Avg speed (km/h)', data: subset.map(d => d.avg_speed),
        borderColor: C.purple, borderWidth: 2.5, pointRadius: 4,
        pointBackgroundColor: C.purple, tension: 0.4, fill: true,
        backgroundColor: 'rgba(124,58,237,0.08)' }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(1)} km/h` } } },
      scales: baseScales('km/h'),
    },
  });
}

/* Rush-hour full breakdown */
function buildRushFullChart(data) {
  if (!hasData(data)) return drawNoData('chartRushFull');
  const C   = palette();
  const ctx = document.getElementById('chartRushFull')?.getContext('2d');
  if (!ctx) return null;
  const colours = data.map(d => {
    const h = d.hour;
    return (h >= 7 && h <= 9) || (h >= 16 && h <= 19) ? C.yellow : C.blue;
  });
  return new Chart(ctx, {
    type: 'bar',
    data: {
      labels: data.map(d => `${String(d.hour).padStart(2,'0')}h`),
      datasets: [{ label: 'Avg speed (km/h)', data: data.map(d => d.avg_speed),
        backgroundColor: colours, borderRadius: 3, borderSkipped: false }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { ...tooltipDefaults(), callbacks: { label: c => ` ${c.parsed.y.toFixed(1)} km/h` } } },
      scales: baseScales('km/h'),
    },
  });
}
