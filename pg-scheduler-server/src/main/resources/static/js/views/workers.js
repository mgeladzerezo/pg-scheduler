import { live } from '../api.js';
import { h, clear, loading, empty, when } from '../ui.js';

export function render(root) {
  const body = h('div', null, loading('Waiting for the first snapshot'));
  root.append(h('div', { class: 'page-head' },
    h('div', null, h('h1', null, 'Workers'),
      h('p', null, 'A worker is alive while it heartbeats within its lease. If one stops, its jobs return to the queue when the lease expires.'))), body);

  const draw = (snapshot) => {
    if (snapshot.workers.length === 0) {
      clear(body).append(h('div', { class: 'card' }, empty('No worker has registered yet.')));
      return;
    }
    clear(body).append(...snapshot.workers.map(card));
  };
  return live.subscribe(draw);
}

function card(w) {
  const status = w.status === 'GONE' ? 'gone' : w.alive ? 'alive' : 'silent';
  return h('div', { class: 'card' },
    h('div', { class: 'toolbar', style: 'justify-content:space-between' },
      h('h2', { style: 'margin:0' }, w.id, ' ', h('span', { class: 'badge ' + (w.alive ? 'ok' : 'bad') }, status),
        w.status === 'STOPPING' && h('span', { class: 'badge warn' }, 'draining')),
      h('span', { class: 'muted' }, w.millisSinceHeartbeat >= 0 ? 'last heartbeat ' + (w.millisSinceHeartbeat / 1000).toFixed(1) + ' s ago' : 'no longer registered')),
    h('dl', { class: 'props', style: 'margin-top:12px' },
      h('dt', null, 'Host'), h('dd', null, w.hostname),
      h('dt', null, 'Queues'), h('dd', null, Object.entries(w.queues).map(([q, n]) => q + ' (' + n + ' slots)').join(', ') || '-'),
      h('dt', null, 'Job types'), h('dd', null, w.jobTypes.join(', ') || '-'),
      h('dt', null, 'Started'), h('dd', null, when(w.startedAt))),
    h('h3', { style: 'margin-top:16px' }, 'Running now (' + w.running.length + ')'),
    w.running.length === 0 ? h('p', { class: 'muted' }, 'Idle.') :
      h('div', { class: 'table-wrap' }, h('table', null,
        h('thead', null, h('tr', null, ['Job', 'Type', 'Queue', 'Attempt', 'Started', 'Lease until'].map((t) => h('th', null, t)))),
        h('tbody', null, w.running.map((j) => h('tr', null,
          h('td', null, h('a', { href: '#/jobs/' + j.id }, '#' + j.id)), h('td', null, j.type), h('td', null, j.queue),
          h('td', null, j.attempt), h('td', null, when(j.startedAt)), h('td', null, when(j.lockedUntil))))))));
}
