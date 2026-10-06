import { api, live } from '../api.js';
import { h, clear, loading, empty, num, toast, when } from '../ui.js';

const STATES = ['READY', 'RUNNING', 'SCHEDULED', 'FAILED', 'DEAD'];

export function render(root) {
  const body = h('div', null, loading('Waiting for the first snapshot'));
  root.append(
    h('div', { class: 'page-head' },
      h('div', null, h('h1', null, 'Overview'), h('p', null, 'Live view of every queue, worker and recent attempt in the cluster.'))),
    body);

  const draw = (snapshot) => {
    clear(body).append(kpis(snapshot), h('div', { class: 'grid two section' }, throughputCard(snapshot), workersCard(snapshot)),
      h('div', { class: 'section' }, queuesCard(snapshot)), parkedCard(snapshot));
  };
  const off = live.subscribe(draw);
  // If the stream is blocked (proxy, offline), a one-shot fetch still fills the page.
  const fallback = setTimeout(() => {
    if (!live.last) api.get('/api/overview').then(draw).catch((e) => clear(body).append(h('div', { class: 'error-box' }, e.message)));
  }, 2500);
  return () => { off(); clearTimeout(fallback); };
}

function total(snapshot, state) {
  return snapshot.queues.reduce((sum, q) => sum + (q.counts[state] || 0), 0);
}

function lastHour(snapshot, outcome) {
  return snapshot.queues.reduce((sum, q) => sum + (q.lastHour[outcome] || 0), 0);
}

function kpi(label, value, sub, cls) {
  return h('div', { class: 'card kpi' }, h('div', { class: 'label' }, label),
    h('div', { class: 'value', ...(cls ? { style: 'color: var(--' + cls + ')' } : {}) }, num(value)), h('div', { class: 'sub' }, sub));
}

function kpis(s) {
  const alive = s.workers.filter((w) => w.alive).length;
  return h('div', { class: 'grid kpis' },
    kpi('Ready', total(s, 'READY'), 'waiting for a worker'),
    kpi('Running', total(s, 'RUNNING'), 'leased to a worker'),
    kpi('Scheduled / retrying', total(s, 'SCHEDULED') + total(s, 'FAILED'), 'due later'),
    kpi('Dead letters', total(s, 'DEAD'), 'need an operator', total(s, 'DEAD') ? 'bad' : null),
    kpi('Succeeded, last hour', lastHour(s, 'SUCCEEDED'), lastHour(s, 'FAILED') + ' failed attempts'),
    kpi('Workers alive', alive, s.workers.length - alive ? (s.workers.length - alive) + ' silent' : 'all heartbeating'));
}

// Stacked bars per 5 second bucket over the last 5 minutes: succeeded, failed attempts, lease problems.
function throughputCard(s) {
  const buckets = 60;
  const end = Math.floor(new Date(s.now).getTime() / 5000) * 5000;
  const cells = Array.from({ length: buckets }, () => ({ ok: 0, bad: 0, warn: 0 }));
  for (const p of s.throughput) {
    const index = buckets - 1 - Math.round((end - new Date(p.bucket).getTime()) / 5000);
    if (index < 0 || index >= buckets) continue;
    const c = cells[index];
    c.ok += p.byOutcome.SUCCEEDED || 0;
    c.bad += (p.byOutcome.FAILED || 0) + (p.byOutcome.TIMED_OUT || 0);
    c.warn += (p.byOutcome.LEASE_EXPIRED || 0) + (p.byOutcome.RELEASED || 0);
  }
  const max = Math.max(5, ...cells.map((c) => c.ok + c.bad + c.warn));
  const w = 600, hgt = 140, bw = w / buckets;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('viewBox', `0 0 ${w} ${hgt + 16}`);
  svg.setAttribute('class', 'chart');
  svg.setAttribute('role', 'img');
  const totalDone = cells.reduce((a, c) => a + c.ok + c.bad + c.warn, 0);
  svg.setAttribute('aria-label', `Attempts finished in the last 5 minutes: ${totalDone}`);
  const rect = (x, y, width, height, cls) => {
    const r = document.createElementNS(svg.namespaceURI, 'rect');
    r.setAttribute('x', x); r.setAttribute('y', y); r.setAttribute('width', width);
    r.setAttribute('height', Math.max(0, height)); r.setAttribute('class', cls); r.setAttribute('rx', 1);
    svg.append(r);
  };
  cells.forEach((c, i) => {
    let y = hgt;
    for (const [key, cls] of [['ok', 'bar-ok'], ['warn', 'bar-warn'], ['bad', 'bar-bad']]) {
      const height = (c[key] / max) * hgt;
      if (height > 0) { y -= height; rect(i * bw + 1, y, bw - 2, height, cls); }
    }
  });
  const axis = document.createElementNS(svg.namespaceURI, 'line');
  axis.setAttribute('x1', 0); axis.setAttribute('x2', w); axis.setAttribute('y1', hgt + .5); axis.setAttribute('y2', hgt + .5);
  axis.setAttribute('class', 'axis');
  svg.append(axis);
  for (const [x, label, anchor] of [[0, '5 min ago', 'start'], [w, 'now', 'end']]) {
    const t = document.createElementNS(svg.namespaceURI, 'text');
    t.setAttribute('x', x); t.setAttribute('y', hgt + 13); t.setAttribute('text-anchor', anchor);
    t.textContent = label; svg.append(t);
  }
  return h('div', { class: 'card' }, h('h2', null, 'Throughput'),
    totalDone === 0 ? empty('No attempts finished in the last 5 minutes.') : svg,
    h('div', { class: 'legend' },
      h('span', null, h('i', { style: 'background: var(--ok)' }), 'succeeded'),
      h('span', null, h('i', { style: 'background: var(--bad)' }), 'failed or timed out'),
      h('span', null, h('i', { style: 'background: var(--warn)' }), 'lease expired or released'),
      h('span', null, 'peak ' + max + ' per 5 s')));
}

function workersCard(s) {
  return h('div', { class: 'card' }, h('h2', null, 'Workers'),
    s.workers.length === 0 ? empty('No worker has registered. Start one with pgscheduler.worker.enabled=true.') :
      h('div', { class: 'table-wrap' }, h('table', null,
        h('thead', null, h('tr', null, h('th', null, 'Worker'), h('th', null, 'Heartbeat'), h('th', { class: 'num' }, 'Running'))),
        h('tbody', null, s.workers.map((w) => h('tr', null,
          h('td', null, h('a', { href: '#/workers' }, w.id), w.status !== 'RUNNING' && h('span', { class: 'muted' }, ' ' + w.status.toLowerCase())),
          h('td', null, h('span', { class: 'badge ' + (w.alive ? 'ok' : 'bad') }, w.alive ? 'alive' : 'silent'), ' ',
            h('span', { class: 'muted small' }, w.millisSinceHeartbeat >= 0 ? (w.millisSinceHeartbeat / 1000).toFixed(1) + ' s ago' : 'gone')),
          h('td', { class: 'num' }, w.running.length)))))));
}

function queuesCard(s) {
  return h('div', { class: 'card' }, h('h2', null, 'Queues'),
    s.queues.length === 0 ? empty('No queue has seen a job yet.') :
      h('div', { class: 'table-wrap' }, h('table', null,
        h('thead', null, h('tr', null, h('th', null, 'Queue'), STATES.map((st) => h('th', { class: 'num' }, st.toLowerCase())),
          h('th', { class: 'num' }, 'ok / h'), h('th', { class: 'num' }, 'failed / h'), h('th', null, ''))),
        h('tbody', null, s.queues.map((q) => h('tr', null,
          h('td', null, h('a', { href: '#/jobs?queue=' + encodeURIComponent(q.name) }, q.name),
            q.paused && h('span', { class: 'badge warn' }, 'paused'),
            q.maxConcurrency != null && h('span', { class: 'muted small' }, ' limit ' + q.maxConcurrency)),
          STATES.map((st) => h('td', { class: 'num' }, num(q.counts[st] || 0))),
          h('td', { class: 'num' }, num(q.lastHour.SUCCEEDED || 0)),
          h('td', { class: 'num' }, num((q.lastHour.FAILED || 0) + (q.lastHour.TIMED_OUT || 0))),
          h('td', { class: 'actions' }, h('button', { class: 'small', onclick: () => togglePause(q) }, q.paused ? 'Resume' : 'Pause'))))))));
}

async function togglePause(queue) {
  try {
    await api.post(`/api/queues/${encodeURIComponent(queue.name)}/${queue.paused ? 'resume' : 'pause'}`);
    toast(`Queue ${queue.name} ${queue.paused ? 'resumed' : 'paused'}`);
  } catch (e) { toast(e.message, true); }
}

function parkedCard(s) {
  const parked = Object.entries(s.parkedTypes || {});
  if (parked.length === 0) return h('span');
  return h('div', { class: 'card section' }, h('h2', null, 'Parked jobs'),
    h('p', { class: 'muted' }, 'No live worker has a handler for these job types. The jobs are kept, not lost, and start as soon as a worker that knows the type joins.'),
    h('div', { class: 'running-jobs' }, parked.map(([type, n]) => h('span', { class: 'badge warn' }, `${type}: ${n}`))));
}
