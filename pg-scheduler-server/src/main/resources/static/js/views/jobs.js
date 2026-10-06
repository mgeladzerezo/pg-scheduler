import { api } from '../api.js';
import { h, clear, loading, empty, errorBox, badge, when, toast } from '../ui.js';

const STATES = ['', 'READY', 'RUNNING', 'SCHEDULED', 'FAILED', 'SUCCEEDED', 'DEAD', 'CANCELLED'];

/** The job list, or with {dead: true} the dead-letter view, which is the same list pinned to DEAD. */
export function render(root, params, options = {}) {
  const dead = !!options.dead;
  const filters = {
    state: dead ? 'DEAD' : params.get('state') || '',
    queue: params.get('queue') || '',
    type: params.get('type') || '',
    q: params.get('q') || '',
  };
  let rows = [];
  let next = null;
  let timer = null;

  const stateSelect = h('select', { 'aria-label': 'State', onchange: (e) => { filters.state = e.target.value; reload(); } },
    STATES.map((s) => h('option', { value: s, selected: s === filters.state }, s ? s.toLowerCase() : 'any state')));
  const input = (key, label, placeholder) => h('input', {
    type: 'search', 'aria-label': label, placeholder, value: filters[key],
    onchange: (e) => { filters[key] = e.target.value.trim(); reload(); },
  });
  const list = h('div', null, loading());
  const more = h('div', { class: 'toolbar', style: 'justify-content:center; margin-top:12px' });

  root.append(
    h('div', { class: 'page-head' },
      h('div', null, h('h1', null, dead ? 'Dead letters' : 'Jobs'),
        h('p', null, dead ? 'Jobs that ran out of attempts or failed in a way that must not be retried. Retry them once the cause is fixed.'
          : 'Search by state, queue, type, job id or unique key.')),
      h('div', { class: 'toolbar' }, !dead && stateSelect, input('queue', 'Queue', 'queue'), input('type', 'Job type', 'job type'),
        !dead && input('q', 'Job id or unique key', 'id or unique key'),
        h('button', { onclick: reload }, 'Refresh'))),
    !dead && enqueueForm(reload),
    h('div', { class: 'card' }, list), more);

  async function load(append) {
    const query = new URLSearchParams();
    if (filters.state) query.set('state', filters.state);
    if (filters.queue) query.set('queue', filters.queue);
    if (filters.type) query.set('type', filters.type);
    if (filters.q) query.set('q', filters.q);
    if (append && next) query.set('before', next);
    try {
      const page = await api.get('/api/jobs?' + query);
      rows = append ? rows.concat(page.jobs) : page.jobs;
      next = page.nextBeforeId;
      draw();
    } catch (e) {
      clear(list).append(errorBox(e, reload));
    }
  }

  function reload() { load(false); }

  function draw() {
    clear(more);
    if (rows.length === 0) {
      clear(list).append(empty(dead ? 'No dead letters. Everything that failed was retried or succeeded.' : 'No job matches these filters.'));
      return;
    }
    clear(list).append(h('div', { class: 'table-wrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Id', 'Type', 'Queue', 'State', 'Attempt', 'Run at', dead ? 'Last error' : 'Updated', ''].map((t) => h('th', null, t)))),
      h('tbody', null, rows.map((job) => h('tr', { class: 'clickable', onclick: () => { location.hash = '#/jobs/' + job.id; } },
        h('td', null, h('a', { href: '#/jobs/' + job.id, onclick: (e) => e.stopPropagation() }, '#' + job.id)),
        h('td', null, job.type), h('td', null, job.queue), h('td', null, badge(job.state)),
        h('td', null, job.attempt + ' / ' + job.maxAttempts), h('td', null, when(job.runAt)),
        h('td', null, dead ? h('span', { class: 'muted small' }, (job.lastError || '').split('\n')[0].slice(0, 90)) : when(job.updatedAt)),
        h('td', { class: 'actions' }, rowActions(job))))))));
    if (next) more.append(h('button', { onclick: () => load(true) }, 'Load more'));
  }

  function rowActions(job) {
    const stop = (fn) => (e) => { e.stopPropagation(); fn(); };
    if (job.state === 'DEAD' || job.state === 'CANCELLED') return h('button', { class: 'small', onclick: stop(() => act(job, 'retry')) }, 'Retry');
    if (job.state === 'SCHEDULED' || job.state === 'FAILED') return h('button', { class: 'small', onclick: stop(() => act(job, 'run-now')) }, 'Run now');
    return null;
  }

  async function act(job, action) {
    try {
      await api.post(`/api/jobs/${job.id}/${action}`);
      toast(`Job #${job.id}: ${action} done`);
      reload();
    } catch (e) { toast(e.message, true); }
  }

  reload();
  timer = setInterval(() => { if (!next || rows.length <= 50) reload(); }, 4000);
  return () => clearInterval(timer);
}

function enqueueForm(onDone) {
  const type = h('select', { id: 'enq-type' }, ['email.send', 'flaky.job', 'slow.job', 'always.fail'].map((t) => h('option', { value: t }, t)));
  const payload = h('textarea', { rows: 2, 'aria-label': 'Payload JSON', placeholder: '{"to":"a@example.com","subject":"Hello"}' });
  const delay = h('input', { type: 'number', min: 0, value: 0 });
  const key = h('input', { placeholder: 'optional' });
  const submit = async () => {
    let body;
    try { body = payload.value.trim() ? JSON.parse(payload.value) : null; } catch (e) { toast('Payload is not valid JSON', true); return; }
    try {
      const r = await api.post('/api/jobs', { type: type.value, payload: body, delaySeconds: Number(delay.value) || 0, uniqueKey: key.value || null });
      toast(r.created ? `Enqueued job #${r.jobId}` : `Same unique key already pending: job #${r.jobId}`);
      onDone();
    } catch (e) { toast(e.message, true); }
  };
  return h('details', { class: 'card section', style: 'margin-bottom:16px' }, h('summary', null, 'Enqueue a job by hand'),
    h('div', { class: 'form-grid', style: 'margin-top:12px' },
      h('label', { class: 'field' }, 'Type', type), h('label', { class: 'field' }, 'Delay (seconds)', delay),
      h('label', { class: 'field' }, 'Unique key', key),
      h('label', { class: 'field wide' }, 'Payload (JSON)', payload),
      h('div', null, h('button', { class: 'primary', onclick: submit }, 'Enqueue'))));
}
