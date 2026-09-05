// query_engine web UI (query-ui epic). Dependency-free ES module served by the
// node it observes. Reads /queries, /queries/{id}, /stats, /tables, /cluster,
// /readyz and posts to /sql. Hash router; polling pauses when the tab is hidden.

const REFRESH_MS = 2000;
const view = document.getElementById('view');
const banner = document.getElementById('banner');
const tooltip = document.getElementById('tooltip');
const autoRefresh = document.getElementById('auto-refresh');
const refreshState = document.getElementById('refresh-state');

// ---------------------------------------------------------------- helpers
function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === 'class') el.className = v;
    else if (k === 'html') el.innerHTML = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}
const svgNS = 'http://www.w3.org/2000/svg';
function s(tag, attrs, ...children) {
  const el = document.createElementNS(svgNS, tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === 'class') el.setAttribute('class', v);
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) if (c != null) el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return el;
}
async function api(path, init) {
  const r = await fetch(path, init);
  const text = await r.text();
  let body = null;
  try { body = JSON.parse(text); } catch { body = { error: text }; }
  return { ok: r.ok, status: r.status, body, headers: r.headers };
}
const fmt = {
  num(n) { return n == null ? '—' : Number(n).toLocaleString(); },
  bytes(b) {
    if (b == null) return '—';
    if (b === 0) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(u.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
    const v = b / Math.pow(1024, i);
    return `${v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)} ${u[i]}`;
  },
  ms(ms) {
    if (ms == null) return '—';
    if (ms < 1) return `${ms.toFixed(2)} ms`;
    if (ms < 1000) return `${ms.toFixed(ms < 10 ? 2 : 1)} ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(2)} s`;
    return `${(ms / 60000).toFixed(1)} min`;
  },
  secs(sec) {
    if (sec == null) return '—';
    if (sec < 60) return `${Math.round(sec)} s`;
    if (sec < 3600) return `${Math.floor(sec / 60)} min`;
    if (sec < 86400) return `${(sec / 3600).toFixed(1)} h`;
    return `${(sec / 86400).toFixed(1)} d`;
  },
  rel(unixMs) {
    if (!unixMs) return '—';
    const d = (Date.now() - unixMs) / 1000;
    if (d < 5) return 'just now';
    if (d < 60) return `${Math.round(d)} s ago`;
    if (d < 3600) return `${Math.round(d / 60)} min ago`;
    if (d < 86400) return `${(d / 3600).toFixed(1)} h ago`;
    return new Date(unixMs).toLocaleString();
  },
  time(iso) { return iso ? new Date(iso).toLocaleTimeString() : '—'; },
  pct(x) { return x == null ? '—' : `${(x * 100).toFixed(1)}%`; },
  ratio(x) { return x == null ? '—' : Number(x).toFixed(3); },
};
function pill(state) { return h('span', { class: `pill ${state}` }, state); }
function showTooltip(ev, node) {
  tooltip.replaceChildren(node);
  tooltip.hidden = false;
  const pad = 12;
  let x = ev.clientX + pad, y = ev.clientY + pad;
  const w = tooltip.offsetWidth, hh = tooltip.offsetHeight;
  if (x + w > window.innerWidth - 8) x = ev.clientX - w - pad;
  if (y + hh > window.innerHeight - 8) y = ev.clientY - hh - pad;
  tooltip.style.left = `${x}px`; tooltip.style.top = `${y}px`;
}
function hideTooltip() { tooltip.hidden = true; }
function tipRows(rows) {
  return h('div', null, rows.map(([k, v]) => h('div', { class: 'row' }, h('span', { class: 'muted' }, k), h('b', null, v))));
}
function setBanner(text) {
  if (!text) { banner.hidden = true; return; }
  banner.replaceChildren(h('span', { class: 'icon' }, '⚠'), text);
  banner.hidden = false;
}
async function checkReady() {
  try {
    const r = await api('/readyz');
    if (r.ok) { setBanner(null); return; }
    setBanner(`Node not ready: ${r.body.reason || r.body.error || `HTTP ${r.status}`}. Queries will be refused until it is; the log and statistics still work.`);
  } catch (e) { setBanner(`Cannot reach the node: ${e.message}`); }
}

// ---------------------------------------------------------------- charts
// Column chart of the last 60 minutes: total queries (series 1) with the
// failed subset (status critical) drawn from the baseline. One axis.
function perMinuteChart(buckets, { height = 140 } = {}) {
  const W = 900, H = height, padL = 34, padB = 20, padT = 8;
  const n = buckets.length || 1;
  const max = Math.max(1, ...buckets.map(b => b.count));
  const plotW = W - padL - 6, plotH = H - padB - padT;
  const slot = plotW / n, bw = Math.min(24, Math.max(2, slot - 2));
  const y = v => padT + plotH - (v / max) * plotH;
  const svg = s('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': 'Queries per minute, last 60 minutes' });
  const ticks = max <= 4 ? [...Array(max + 1).keys()] : [0, Math.round(max / 2), max];
  for (const t of ticks) {
    svg.append(s('line', { class: 'grid-line', x1: padL, x2: W - 6, y1: y(t), y2: y(t) }));
    svg.append(s('text', { class: 'axis-text', x: padL - 6, y: y(t) + 4, 'text-anchor': 'end' }, fmt.num(t)));
  }
  buckets.forEach((b, i) => {
    const x = padL + i * slot + (slot - bw) / 2;
    const g = s('g');
    if (b.count > 0) {
      const top = y(b.count), hgt = Math.max(1, padT + plotH - top);
      g.append(s('path', { class: 'bar', d: roundedTop(x, top, bw, hgt, 4) }));
      if (b.failed > 0) g.append(s('rect', { class: 'bar failed', x, y: y(b.failed), width: bw, height: Math.max(1, padT + plotH - y(b.failed)) }));
    }
    const hit = s('rect', { class: 'hit', x: padL + i * slot, y: padT, width: slot, height: plotH,
      onmousemove: ev => showTooltip(ev, tipRows([
        ['minute', new Date(b.minute_unix * 1000).toLocaleTimeString()],
        ['queries', fmt.num(b.count)], ['failed', fmt.num(b.failed)],
        ['running', fmt.num(b.running)], ['p95', b.count ? fmt.ms(b.p95_ms) : '—']])),
      onmouseleave: hideTooltip });
    g.append(hit);
    svg.append(g);
    if (i % 10 === 0 || i === n - 1) {
      svg.append(s('text', { class: 'axis-text', x: padL + i * slot + slot / 2, y: H - 5, 'text-anchor': 'middle' },
        i === n - 1 ? 'now' : `-${n - 1 - i} min`));
    }
  });
  const legend = h('div', { class: 'legend' },
    h('span', null, h('span', { class: 'key box', style: 'background: var(--s1)' }), 'queries'),
    h('span', null, h('span', { class: 'key box', style: 'background: var(--critical)' }), '✕ failed'));
  return h('div', { class: 'chart' }, svg, legend);
}
function roundedTop(x, y, w, hgt, r) {
  r = Math.min(r, w / 2, hgt);
  return `M${x},${y + hgt} V${y + r} Q${x},${y} ${x + r},${y} H${x + w - r} Q${x + w},${y} ${x + w},${y + r} V${y + hgt} Z`;
}
// Latency histogram over log-spaced buckets; single series, no legend needed.
const LAT_EDGES = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000];
const LAT_LABELS = ['<1ms', '1–2ms', '2–5ms', '5–10ms', '10–20ms', '20–50ms', '50–100ms', '100–200ms', '200–500ms', '0.5–1s', '1–2s', '2–5s', '5–10s', '10–30s', '>30s'];
function latencyHistogram(samples) {
  const labels = LAT_LABELS;
  const counts = new Array(labels.length).fill(0);
  for (const v of samples) {
    let i = LAT_EDGES.findIndex(e => v < e);
    if (i < 0) i = labels.length - 1;
    counts[i]++;
  }
  const W = 900, H = 170, padL = 34, padB = 34, padT = 8;
  const n = labels.length, max = Math.max(1, ...counts);
  const plotW = W - padL - 6, plotH = H - padB - padT, slot = plotW / n, bw = Math.min(24, slot - 6);
  const y = v => padT + plotH - (v / max) * plotH;
  const svg = s('svg', { viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': 'Latency distribution' });
  for (const t of [0, Math.round(max / 2), max]) {
    svg.append(s('line', { class: 'grid-line', x1: padL, x2: W - 6, y1: y(t), y2: y(t) }));
    svg.append(s('text', { class: 'axis-text', x: padL - 6, y: y(t) + 4, 'text-anchor': 'end' }, fmt.num(t)));
  }
  counts.forEach((c, i) => {
    const x = padL + i * slot + (slot - bw) / 2;
    if (c > 0) svg.append(s('path', { class: 'bar', d: roundedTop(x, y(c), bw, Math.max(1, padT + plotH - y(c)), 4) }));
    svg.append(s('rect', { class: 'hit', x: padL + i * slot, y: padT, width: slot, height: plotH,
      onmousemove: ev => showTooltip(ev, tipRows([['bucket', labels[i]], ['queries', fmt.num(c)], ['share', samples.length ? fmt.pct(c / samples.length) : '—']])),
      onmouseleave: hideTooltip }));
    // Staggered two-row labels: no rotation, nothing clipped at the edges.
    svg.append(s('text', { class: 'axis-text', x: padL + i * slot + slot / 2, y: H - (i % 2 ? 4 : 16), 'text-anchor': 'middle' }, labels[i]));
  });
  return h('div', { class: 'chart' }, svg);
}
// Phase timeline of one query: four fixed categorical slots + legend with values.
function phaseBar(d) {
  const phases = [['parse', d.parse_ms], ['plan', d.plan_ms], ['optimize', d.optimize_ms], ['execute', d.execute_ms]]
    .filter(([, v]) => v != null);
  const total = d.elapsed_ms || phases.reduce((a, [, v]) => a + v, 0) || 1;
  const accounted = phases.reduce((a, [, v]) => a + v, 0);
  const other = Math.max(0, total - accounted);
  const segs = phases.concat(other > total * 0.005 ? [['other', other]] : []);
  const bar = h('div', { class: 'phases' }, segs.map(([k, v]) => h('div', {
    class: `seg ${k}`, style: `flex: ${Math.max(v, total * 0.002)} 0 0`,
    onmousemove: ev => showTooltip(ev, tipRows([[k, fmt.ms(v)], ['share', fmt.pct(v / total)]])), onmouseleave: hideTooltip })));
  const legend = h('div', { class: 'legend' }, segs.map(([k, v]) => h('span', null, h('span', { class: `key box ${k}` }), `${k} ${fmt.ms(v)}`)));
  return h('div', null, bar, legend);
}

// ---------------------------------------------------------------- tables
function sortableTable(columns, rows, { key = 'seq', dir = -1, onRow, emptyText = 'Nothing yet.' } = {}) {
  const state = { key, dir };
  const wrap = h('div');
  function render() {
    const sorted = [...rows].sort((a, b) => {
      const col = columns.find(c => c.key === state.key) || columns[0];
      const va = col.sort ? col.sort(a) : a[state.key], vb = col.sort ? col.sort(b) : b[state.key];
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      return (va < vb ? -1 : va > vb ? 1 : 0) * state.dir;
    });
    const thead = h('thead', null, h('tr', null, columns.map(c => h('th', {
      class: `${c.num ? 'num ' : ''}${c.sortable === false ? '' : 'sortable'}`,
      onclick: c.sortable === false ? null : () => { if (state.key === c.key) state.dir = -state.dir; else { state.key = c.key; state.dir = c.num ? -1 : 1; } render(); },
    }, c.label, state.key === c.key ? h('span', { class: 'arrow' }, state.dir < 0 ? '▼' : '▲') : null))));
    const tbody = h('tbody', null, sorted.map(r => h('tr', { class: onRow ? 'clickable' : null, onclick: onRow ? () => onRow(r) : null },
      columns.map(c => h('td', { class: `${c.num ? 'num' : ''} ${c.class || ''}`, title: c.title ? c.title(r) : null }, c.render ? c.render(r) : r[c.key])))));
    wrap.replaceChildren(rows.length ? h('table', { class: 'data' }, thead, tbody) : h('div', { class: 'empty' }, emptyText));
  }
  render();
  return wrap;
}
const queryColumns = (opts = {}) => [
  { key: 'state', label: 'State', render: r => pill(r.state) },
  { key: 'submitted_unix_ms', label: 'Submitted', num: true, class: 'nowrap', render: r => fmt.rel(r.submitted_unix_ms), title: r => r.submitted_at },
  { key: 'elapsed_ms', label: 'Elapsed', num: true, render: r => fmt.ms(r.elapsed_ms) },
  { key: 'rows', label: 'Rows', num: true, render: r => fmt.num(r.rows) },
  { key: 'peak_memory_bytes', label: 'Pool peak', num: true, render: r => fmt.bytes(r.peak_memory_bytes), title: () => 'high-water mark of the memory pool (operator reservations) while this query ran' },
  { key: 'flags', label: 'Flags', sortable: false, render: r => [
      r.distributed ? h('span', { class: 'flag', title: 'ran distributed' }, 'dist') : null,
      r.spilled_bytes > 0 ? h('span', { class: 'flag', title: `spilled ${fmt.bytes(r.spilled_bytes)}` }, 'spill') : null,
      h('span', { class: 'flag', title: 'front door' }, r.front_door)] },
  { key: 'sql', label: 'SQL', class: 'sql', render: r => r.error_message ? h('span', null, r.sql, ' ', h('span', { class: 'muted' }, `— ${r.error_kind}: ${r.error_message}`)) : r.sql, title: r => r.sql },
  ...(opts.tables ? [{ key: 'tables', label: 'Tables', sortable: false, render: r => (r.tables || []).join(', ') }] : []),
];
const openQuery = r => { location.hash = `#/query/${r.query_id}`; };

// ---------------------------------------------------------------- views
const views = {};

views.overview = {
  title: 'Overview',
  async render() {
    const [stats, list] = await Promise.all([api('/stats'), api('/queries?limit=10')]);
    if (!stats.ok) return h('div', { class: 'empty' }, `GET /stats failed: ${stats.body.error}`);
    const st = stats.body, q = st.queries, lat = st.latency_ms;
    const lastMinute = st.per_minute[st.per_minute.length - 1] || { count: 0 };
    const errRate = q.total ? q.failed / q.total : 0;
    const tiles = h('div', { class: 'tiles' },
      tile('Running', q.running, null, q.running > 0 ? 'active' : null),
      tile('Queries, last minute', lastMinute.count),
      tile('p95 latency', lat.samples ? fmt.ms(lat.p95_ms) : '—', lat.samples ? `p50 ${fmt.ms(lat.p50_ms)} · max ${fmt.ms(lat.max_ms)}` : 'no samples'),
      tile('Error rate', fmt.pct(errRate), `${fmt.num(q.failed)} of ${fmt.num(q.total)} in log`, errRate > 0.1 ? 'alert' : null),
      tile('Spilled', fmt.bytes(st.spilled_bytes_total), `${fmt.num(st.spill_queries)} queries spilled`),
      tile('Pool reservations', fmt.bytes(st.memory.used), st.memory.max ? `peak ${fmt.bytes(st.memory.peak)} · limit ${fmt.bytes(st.memory.max)}` : `peak ${fmt.bytes(st.memory.peak)} · no limit`),
      tile('Cluster', `${st.cluster.up} / ${st.cluster.members}`, st.cluster.ready ? 'members up · ready' : 'not ready'),
      tile('Uptime', fmt.secs(st.uptime_s), `${fmt.num(q.lifetime_total)} queries since start`));
    return h('div', null,
      h('h1', null, 'Overview', h('span', { class: 'sub' }, `node ${st.node_id}`)),
      tiles,
      h('h2', null, 'Queries per minute', h('span', { class: 'sub' }, 'last 60 minutes')),
      h('div', { class: 'card' }, perMinuteChart(st.per_minute)),
      h('h2', null, 'Recent queries', h('span', { class: 'sub' }, h('a', { href: '#/queries' }, 'all →'))),
      sortableTable(queryColumns(), list.ok ? list.body.queries : [], { onRow: openQuery, emptyText: 'No queries yet. Try the SQL console.' }));
  },
};
function tile(label, value, sub, cls) {
  return h('div', { class: `tile ${cls || ''}` }, h('div', { class: 'label' }, label), h('div', { class: 'value' }, value), sub ? h('div', { class: 'delta' }, sub) : null);
}

const queriesState = { limit: '100', state: '', door: '', q: '' };
views.queries = {
  title: 'Queries',
  async render() {
    const p = new URLSearchParams();
    p.set('limit', queriesState.limit);
    if (queriesState.state) p.set('state', queriesState.state);
    if (queriesState.door) p.set('door', queriesState.door);
    if (queriesState.q) p.set('q', queriesState.q);
    const r = await api(`/queries?${p}`);
    if (!r.ok) return h('div', { class: 'empty' }, `GET /queries failed: ${r.body.error}`);
    const filters = h('div', { class: 'filters' },
      h('input', { type: 'text', placeholder: 'filter by SQL, table, error text or id…', value: queriesState.q,
        onkeydown: ev => { if (ev.key === 'Enter') { queriesState.q = ev.target.value; refresh(); } },
        onchange: ev => { queriesState.q = ev.target.value; refresh(); } }),
      select([['', 'any state'], ['running', 'running'], ['finished', 'finished'], ['failed', 'failed']], queriesState.state, v => { queriesState.state = v; refresh(); }),
      select([['', 'any door'], ['http', 'http'], ['flight', 'flight'], ['fragment', 'fragment']], queriesState.door, v => { queriesState.door = v; refresh(); }),
      select([['50', 'last 50'], ['100', 'last 100'], ['500', 'last 500'], ['all', 'all in log']], queriesState.limit, v => { queriesState.limit = v; refresh(); }),
      h('span', { class: 'muted small' }, `${fmt.num(r.body.matched)} matching · ${fmt.num(r.body.total)} in log (capacity ${fmt.num(r.body.capacity)})`));
    return h('div', null, h('h1', null, 'Queries'), filters,
      sortableTable(queryColumns({ tables: true }), r.body.queries, { onRow: openQuery, emptyText: 'No queries match.' }));
  },
};
function select(options, value, onchange) {
  return h('select', { onchange: ev => onchange(ev.target.value) }, options.map(([v, label]) => h('option', { value: v, selected: v === value ? '' : null }, label)));
}

views.query = {
  title: 'Query',
  poll: true,
  async render(id) {
    const r = await api(`/queries/${encodeURIComponent(id)}`);
    if (!r.ok) return h('div', null, h('h1', null, 'Query'), h('div', { class: 'empty' }, r.body.error || `HTTP ${r.status}`));
    const d = r.body;
    const fact = (k, v, cls) => h('div', { class: `fact ${cls || ''}` }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v == null || v === '' ? '—' : v));
    const facts = h('div', { class: 'facts' },
      fact('state', pill(d.state)), fact('submitted', `${new Date(d.submitted_at).toLocaleString()}`), fact('finished', d.finished_at ? new Date(d.finished_at).toLocaleString() : d.state === 'running' ? 'still running' : null),
      fact('elapsed', fmt.ms(d.elapsed_ms)), fact('rows', fmt.num(d.rows)), fact('batches', fmt.num(d.batches)),
      fact('result', d.result_bytes != null ? `${fmt.bytes(d.result_bytes)} ${d.result_format || ''}` : d.result_format),
      fact('front door', `${d.front_door}${d.client_addr ? ` from ${d.client_addr}` : ''}`), fact('statement', d.statement_kind), fact('requested mode', d.requested_mode),
      fact('pool peak', fmt.bytes(d.peak_memory_bytes)), fact('memory limit', d.memory_limit_bytes ? fmt.bytes(d.memory_limit_bytes) : 'none'),
      fact('concurrent at start', `${d.concurrent_at_start}${d.concurrent_at_start ? ' (peak is pool-wide)' : ''}`),
      fact('spill', d.spill ? `${fmt.bytes(d.spill.bytes)} · ${d.spill.partitions} partitions · ${d.spill.files} files · read-back ${fmt.ms(d.spill.read_back_ms)}` : 'none'),
      fact('files pruned', `${fmt.num(d.files_pruned_by_stats)} by stats · ${fmt.num(d.files_pruned_by_partition)} by partition`),
      fact('rollup answered', d.rollup_answered && d.rollup_answered.length ? d.rollup_answered.join(', ') : 'no'),
      fact('tables', (d.tables || []).join(', ')), fact('distributed', d.distributed ? `yes · ${d.distribution ? d.distribution.shard_count + ' shards' : ''}` : `no${d.fallback_reason ? ` — ${d.fallback_reason}` : ''}`),
      d.shard ? fact('shard', `${d.shard.index + 1} of ${d.shard.count} on ${d.shard.table}${d.initiator ? ` · initiator ${d.initiator}` : ''}`) : null,
      fact('node', d.node_id), fact('query id', h('span', { class: 'mono' }, d.query_id)));
    const parts = [
      h('h1', null, 'Query ', h('span', { class: 'sub mono' }, d.query_id.slice(0, 8)), ' ', pill(d.state)),
      d.error ? h('div', { class: 'card errbox' }, h('div', { class: 'k' }, `${d.error.kind}`), h('pre', null, d.error.message)) : null,
      h('h2', null, 'SQL'), sqlBox(d.sql, d.sql_truncated),
      d.elapsed_ms != null ? h('div', null, h('h2', null, 'Timeline', h('span', { class: 'sub' }, fmt.ms(d.elapsed_ms))), phaseBar(d)) : null,
      h('h2', null, 'Facts'), facts,
    ];
    if (d.distribution) {
      const dist = d.distribution;
      parts.push(h('h2', null, 'Distribution', h('span', { class: 'sub' }, `${dist.shape || ''} over ${dist.table} · imbalance ${fmt.ratio(dist.imbalance)} · wall-time spread ${fmt.ratio(dist.wall_time_spread)}`)));
      parts.push(sortableTable([
        { key: 'node_id', label: 'Node', num: true }, { key: 'address', label: 'Address' },
        { key: 'splits', label: 'Splits', num: true, render: n => fmt.num(n.splits) },
        { key: 'bytes', label: 'Bytes', num: true, render: n => fmt.bytes(n.bytes) },
        { key: 'rows', label: 'Rows', num: true, render: n => fmt.num(n.rows) },
        { key: 'elapsed_ms', label: 'Elapsed', num: true, render: n => fmt.ms(n.elapsed_ms) },
      ], dist.nodes || [], { key: 'node_id', dir: 1 }));
      if (dist.partial_sql) parts.push(h('details', { class: 'plan' }, h('summary', null, 'Partial SQL sent to workers'), h('pre', null, dist.partial_sql), dist.final_sql ? h('pre', null, `-- final\n${dist.final_sql}`) : null));
    }
    parts.push(h('h2', null, 'Plans'));
    parts.push(h('details', { class: 'plan', open: '' }, h('summary', null, 'Physical plan'), h('pre', null, d.physical_plan || 'not captured (distributed run or failure before planning)')));
    parts.push(h('details', { class: 'plan' }, h('summary', null, 'Optimized logical plan'), h('pre', null, d.optimized_plan || 'not captured')));
    parts.push(h('details', { class: 'plan' }, h('summary', null, 'Raw JSON'), h('pre', null, JSON.stringify(d, null, 2))));
    return h('div', null, parts);
  },
};
function sqlBox(sql, truncated) {
  const pre = h('pre', null, sql, truncated ? h('span', { class: 'muted' }, '\n(truncated)') : null);
  const btn = h('button', { class: 'btn copy', onclick: async () => { try { await navigator.clipboard.writeText(sql); btn.textContent = 'copied'; setTimeout(() => (btn.textContent = 'copy'), 1200); } catch { btn.textContent = 'select & copy'; } } }, 'copy');
  return h('div', { class: 'sqlbox' }, pre, btn);
}

views.stats = {
  title: 'Statistics',
  async render() {
    const [stats, all] = await Promise.all([api('/stats'), api('/queries?limit=all')]);
    if (!stats.ok) return h('div', { class: 'empty' }, `GET /stats failed: ${stats.body.error}`);
    const st = stats.body, q = st.queries, lat = st.latency_ms;
    const samples = all.ok ? all.body.queries.filter(x => x.elapsed_ms != null).map(x => x.elapsed_ms) : [];
    const tiles = h('div', { class: 'tiles' },
      tile('In log', fmt.num(q.total), `capacity ${fmt.num(st.log_capacity)} · lifetime ${fmt.num(q.lifetime_total)}`),
      tile('Finished', fmt.num(q.finished), `${fmt.num(q.local)} local · ${fmt.num(q.distributed)} distributed`),
      tile('Failed', fmt.num(q.failed), `lifetime ${fmt.num(q.lifetime_failed)}`, q.failed ? 'alert' : null),
      tile('Running', fmt.num(q.running)), tile('Fragments served', fmt.num(q.fragments)),
      tile('Rows returned', fmt.num(st.rows_total)), tile('Bytes returned', fmt.bytes(st.bytes_total)),
      tile('Spill', fmt.bytes(st.spilled_bytes_total), `${fmt.num(st.spill_queries)} queries`),
      tile('p50 latency', lat.samples ? fmt.ms(lat.p50_ms) : '—', lat.samples ? `mean ${fmt.ms(lat.mean_ms)} · n=${lat.samples}` : 'no samples'),
      tile('p95 latency', lat.samples ? fmt.ms(lat.p95_ms) : '—'),
      tile('p99 latency', lat.samples ? fmt.ms(lat.p99_ms) : '—', lat.samples ? `max ${fmt.ms(lat.max_ms)}` : null),
      tile('Pool reservations', fmt.bytes(st.memory.used), `peak ${fmt.bytes(st.memory.peak)} · ${st.memory.max ? `limit ${fmt.bytes(st.memory.max)}` : 'no limit'}`));
    const tables = Object.entries(st.tables).map(([name, count]) => ({ name, count })).sort((a, b) => b.count - a.count);
    const errors = Object.entries(st.errors_by_kind).map(([kind, count]) => ({ kind, count })).sort((a, b) => b.count - a.count);
    return h('div', null,
      h('h1', null, 'Statistics', h('span', { class: 'sub' }, `node ${st.node_id} · generated ${fmt.time(st.generated_at)}`)),
      tiles,
      h('h2', null, 'Queries per minute', h('span', { class: 'sub' }, 'last 60 minutes')),
      h('div', { class: 'card' }, perMinuteChart(st.per_minute)),
      h('h2', null, 'Latency distribution', h('span', { class: 'sub' }, `${fmt.num(samples.length)} completed queries in log`)),
      h('div', { class: 'card' }, latencyHistogram(samples)),
      h('div', { class: 'grid two' },
        h('div', null, h('h2', null, 'Slowest'), sortableTable([
          { key: 'elapsed_ms', label: 'Elapsed', num: true, render: r => fmt.ms(r.elapsed_ms) },
          { key: 'state', label: 'State', render: r => pill(r.state) },
          { key: 'sql_preview', label: 'SQL', class: 'sql', title: r => r.sql_preview },
        ], st.slowest, { key: 'elapsed_ms', dir: -1, onRow: openQuery })),
        h('div', null, h('h2', null, 'Errors by kind'), sortableTable([
          { key: 'kind', label: 'Kind' }, { key: 'count', label: 'Count', num: true, render: r => fmt.num(r.count) },
        ], errors, { key: 'count', dir: -1, onRow: r => { queriesState.q = r.kind; queriesState.state = 'failed'; location.hash = '#/queries'; }, emptyText: 'No failures in the log.' }))),
      h('h2', null, 'Tables by query count'),
      sortableTable([{ key: 'name', label: 'Table', class: 'mono' }, { key: 'count', label: 'Queries', num: true, render: r => fmt.num(r.count) }], tables,
        { key: 'count', dir: -1, onRow: r => { queriesState.q = r.name; location.hash = '#/queries'; }, emptyText: 'No table has been read yet.' }));
  },
};

views.cluster = {
  title: 'Cluster',
  async render() {
    const r = await api('/cluster');
    if (!r.ok) return h('div', { class: 'empty' }, `GET /cluster failed: ${r.body.error}`);
    const c = r.body, node = c.node, disc = c.discovery;
    const facts = h('div', { class: 'facts' },
      f('this node', `${node.id} at ${node.address}`), f('flight', node.flight || 'disabled'), f('ready', node.ready ? 'yes' : 'no'),
      f('uptime', fmt.secs(node.uptime_ms / 1000)), f('queries', `${fmt.num(node.queries_total)} total · ${fmt.num(node.queries_failed)} failed`),
      f('tables', `${node.tables.length}`), f('discovery', [disc.mode, disc.source].filter(Boolean).join(' · ')), f('resolved', `${disc.resolved ? 'yes' : 'no'} · generation ${disc.generation}${disc.last_error ? ` · last error: ${disc.last_error}` : ''}`));
    const members = sortableTable([
      { key: 'node_id', label: 'Node', num: true, render: m => m.node_id == null ? '?' : `${m.node_id}${m.is_self ? ' (this)' : ''}` },
      { key: 'address', label: 'Address', class: 'mono' },
      { key: 'flight', label: 'Flight', class: 'mono', render: m => m.flight || '—' },
      { key: 'status', label: 'Status', render: m => { const st = String(m.status).toLowerCase(); return h('span', { class: `pill ${st === 'up' || m.is_self ? 'finished' : st === 'down' ? 'failed' : 'running'}` }, m.is_self ? 'self' : st); } },
      { key: 'last_seen_unix_ms', label: 'Last seen', num: true, render: m => m.is_self ? '—' : fmt.rel(m.last_seen_unix_ms) },
      { key: 'ui', label: 'UI', sortable: false, render: m => m.is_self ? null : h('a', { href: `http://${m.address}/ui`, target: '_blank', rel: 'noopener' }, 'open ↗') },
      { key: 'last_error', label: 'Last error', render: m => m.last_error || '' },
    ], c.members, { key: 'address', dir: 1 });
    return h('div', null, h('h1', null, 'Cluster', h('span', { class: 'sub' }, `${c.member_count} members`)), facts, h('h2', null, 'Members'), members);
    function f(k, v) { return h('div', { class: 'fact' }, h('div', { class: 'k' }, k), h('div', { class: 'v' }, v)); }
  },
};

views.tables = {
  title: 'Tables',
  async render() {
    const [t, st] = await Promise.all([api('/tables'), api('/stats')]);
    if (!t.ok) return h('div', { class: 'empty' }, `GET /tables failed: ${t.body.error}`);
    const counts = st.ok ? st.body.tables : {};
    const list = t.body.tables.map(tb => h('details', { class: 'plan' },
      h('summary', null, h('span', { class: 'mono' }, tb.name), h('span', { class: 'muted small' }, ` · ${tb.column_count} columns · ${fmt.num(counts[tb.name] || 0)} queries`)),
      h('table', { class: 'data' }, h('thead', null, h('tr', null, h('th', null, 'Column'), h('th', null, 'Type'), h('th', null, 'Nullable'))),
        h('tbody', null, tb.columns.map(c => h('tr', null, h('td', { class: 'mono' }, c.name), h('td', { class: 'mono' }, c.data_type), h('td', null, c.nullable ? 'yes' : 'no')))))));
    return h('div', null, h('h1', null, 'Tables', h('span', { class: 'sub' }, t.body.ready ? `${t.body.table_count} registered` : `not loaded${t.body.load_error ? `: ${t.body.load_error}` : ''}`)),
      list.length ? list : h('div', { class: 'empty' }, 'No tables registered.'));
  },
};

const consoleState = { sql: 'SELECT COUNT(*) AS n FROM lineitem', mode: 'auto', last: null };
views.sql = {
  title: 'SQL',
  poll: false,
  async render() {
    const ta = h('textarea', { spellcheck: 'false', oninput: ev => { consoleState.sql = ev.target.value; }, onkeydown: ev => { if ((ev.ctrlKey || ev.metaKey) && ev.key === 'Enter') run(); } }, consoleState.sql);
    const modeSel = select([['auto', 'distributed=auto'], ['1', 'distributed=1 (force)'], ['0', 'distributed=0 (local)']], consoleState.mode, v => { consoleState.mode = v; });
    const runBtn = h('button', { class: 'btn primary', onclick: run }, 'Run (Ctrl+Enter)');
    const status = h('span', { class: 'kv' });
    const out = h('div');
    if (consoleState.last) renderResult(consoleState.last);
    async function run() {
      runBtn.disabled = true; status.textContent = 'running…'; out.replaceChildren();
      const t0 = performance.now();
      try {
        const r = await api(`/sql?format=json&distributed=${consoleState.mode}`, { method: 'POST', body: consoleState.sql });
        consoleState.last = { r, clientMs: performance.now() - t0 };
        renderResult(consoleState.last);
      } catch (e) { status.textContent = `request failed: ${e.message}`; }
      runBtn.disabled = false;
    }
    function renderResult({ r, clientMs }) {
      const id = r.headers.get('x-qe-query-id');
      const link = id ? h('a', { href: `#/query/${id}` }, 'open query detail →') : null;
      if (!r.ok) {
        status.replaceChildren(h('span', null, h('b', null, `HTTP ${r.status}`)), link);
        out.replaceChildren(h('div', { class: 'card errbox' }, h('pre', null, r.body.error || JSON.stringify(r.body))));
        return;
      }
      const rows = Array.isArray(r.body) ? r.body : [];
      const dist = r.headers.get('x-qe-distributed') === 'true';
      status.replaceChildren(
        h('span', null, h('b', null, fmt.num(r.headers.get('x-qe-rows'))), ' rows'),
        h('span', null, h('b', null, fmt.ms(parseFloat(r.headers.get('x-qe-elapsed-ms')))), ' engine'),
        h('span', null, h('b', null, fmt.ms(clientMs)), ' round trip'),
        h('span', null, dist ? `distributed over ${r.headers.get('x-qe-shards')} shards` : `local${r.headers.get('x-qe-distributed-skipped') ? ` (${r.headers.get('x-qe-distributed-skipped')})` : ''}`),
        link);
      if (!rows.length) { out.replaceChildren(h('div', { class: 'empty' }, 'Query returned no rows.')); return; }
      const cols = Object.keys(rows[0]);
      const shown = rows.slice(0, 1000);
      out.replaceChildren(h('div', { class: 'results' }, h('table', { class: 'data' },
        h('thead', null, h('tr', null, cols.map(c => h('th', null, c)))),
        h('tbody', null, shown.map(row => h('tr', null, cols.map(c => h('td', { class: typeof row[c] === 'number' ? 'num' : '' }, row[c] == null ? h('span', { class: 'muted' }, 'null') : typeof row[c] === 'object' ? JSON.stringify(row[c]) : String(row[c])))))))),
        rows.length > shown.length ? h('div', { class: 'muted small' }, `showing ${shown.length} of ${rows.length} rows`) : null);
    }
    return h('div', { class: 'console' }, h('h1', null, 'SQL console'), ta, h('div', { class: 'row' }, modeSel, runBtn, status), out);
  },
};

// ---------------------------------------------------------------- router
let current = { name: null, arg: null };
let timer = null, rendering = false;
function route() {
  const hash = location.hash || '#/';
  const m = hash.match(/^#\/([a-z]*)\/?(.*)$/);
  const name = m && m[1] ? m[1] : 'overview';
  const arg = m ? decodeURIComponent(m[2] || '') : '';
  current = { name: views[name] ? name : 'overview', arg };
  for (const a of document.querySelectorAll('#nav a')) a.classList.toggle('active', a.dataset.route === current.name || (current.name === 'query' && a.dataset.route === 'queries'));
  document.title = `${views[current.name].title} · query_engine`;
  refresh(true);
}
async function refresh(scroll = false) {
  if (rendering) return;
  rendering = true;
  const v = views[current.name];
  try {
    const [node] = await Promise.all([v.render(current.arg), checkReady()]);
    const y = window.scrollY;
    view.replaceChildren(node);
    if (!scroll) window.scrollTo(0, y);
    refreshState.textContent = `updated ${new Date().toLocaleTimeString()}`;
  } catch (e) {
    view.replaceChildren(h('div', { class: 'empty' }, `render failed: ${e.message}`));
    console.error(e);
  } finally { rendering = false; }
}
function schedule() {
  clearInterval(timer);
  timer = setInterval(() => {
    const v = views[current.name];
    if (!autoRefresh.checked || document.hidden || v.poll === false) return;
    if (tooltip.hidden === false) return; // never yank a chart out from under the pointer
    refresh();
  }, REFRESH_MS);
}
autoRefresh.addEventListener('change', () => { refreshState.textContent = autoRefresh.checked ? '' : 'paused'; });
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
window.addEventListener('hashchange', route);
api('/healthz').then(r => { if (r.ok) document.getElementById('node-badge').textContent = `node ${r.body.node_id ?? r.body.id ?? '?'}`; }).catch(() => {});
route();
schedule();
