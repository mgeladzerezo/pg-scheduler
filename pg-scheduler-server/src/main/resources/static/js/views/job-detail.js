import { api } from '../api.js';
import { h, clear, loading, errorBox, badge, when, json, duration, toast, toLocalInput } from '../ui.js';

export function render(root, params, id) {
  const body = h('div', null, loading());
  root.append(h('p', null, h('a', { href: '#/jobs' }, '← All jobs')), body);
  let timer = null;

  async function load() {
    try {
      draw(await api.get('/api/jobs/' + id));
    } catch (e) {
      clear(body).append(errorBox(e, load));
    }
  }

  async function act(action, payload) {
    try {
      const method = action === 'run-at' ? api.put : api.post;
      draw(await method(`/api/jobs/${id}/${action}`, payload));
      toast('Done');
    } catch (e) { toast(e.message, true); }
  }

  function draw({ job, attempts }) {
    const canRetry = job.state === 'DEAD' || job.state === 'CANCELLED';
    const canCancel = !['SUCCEEDED', 'DEAD', 'CANCELLED'].includes(job.state);
    const waiting = job.state === 'SCHEDULED' || job.state === 'FAILED';
    const runAt = h('input', { type: 'datetime-local', value: job.runAt ? toLocalInput(job.runAt) : '', 'aria-label': 'Run at' });
    clear(body).append(
      h('div', { class: 'page-head' },
        h('div', null, h('h1', null, '#' + job.id + ' ', h('span', { class: 'muted' }, job.type), ' ', badge(job.state)),
          h('p', null, 'Queue ', h('code', null, job.queue), ' · attempt ' + job.attempt + ' of ' + job.maxAttempts)),
        h('div', { class: 'toolbar' },
          canRetry && h('button', { class: 'primary', onclick: () => act('retry') }, 'Retry'),
          waiting && h('button', { onclick: () => act('run-now') }, 'Run now'),
          canCancel && h('button', { class: 'danger', onclick: () => act('cancel') }, 'Cancel'))),
      h('div', { class: 'grid two' },
        h('div', { class: 'card' }, h('h2', null, 'Details'), h('dl', { class: 'props' },
          prop('Run at', when(job.runAt)), prop('Priority', job.priority), prop('Timeout', duration(job.timeoutMs)),
          prop('Unique key', job.uniqueKey || '-'), prop('Locked by', job.lockedBy ? h('span', null, job.lockedBy, ' until ', when(job.lockedUntil)) : '-'),
          prop('Created', when(job.createdAt)), prop('Started', when(job.startedAt)), prop('Finished', when(job.finishedAt)),
          job.scheduleId != null && prop('Schedule', h('a', { href: '#/schedules' }, '#' + job.scheduleId + ' fire ' + (job.fireTime || ''))),
          job.parentId != null && prop('Continues', h('a', { href: '#/jobs/' + job.parentId }, 'job #' + job.parentId)),
          job.hasContinuation && prop('Continuation', 'runs another job when this one succeeds')),
          (waiting || job.state === 'READY') && h('div', { class: 'toolbar', style: 'margin-top:12px' }, runAt,
            h('button', { onclick: () => { if (runAt.value) act('run-at', { runAt: new Date(runAt.value).toISOString() }); } }, 'Change run time'))),
        h('div', { class: 'card' }, h('h2', null, 'Payload'), h('pre', { class: 'code' }, json(job.payload)),
          job.result != null && h('div', null, h('h3', { style: 'margin-top:16px' }, 'Result'), h('pre', { class: 'code' }, json(job.result))))),
      job.lastError && h('div', { class: 'card section' }, h('h2', null, 'Last error'), h('pre', { class: 'code' }, job.lastError)),
      h('div', { class: 'card section' }, h('h2', null, 'Attempts'),
        attempts.length === 0 ? h('p', { class: 'muted' }, 'No attempt has finished yet.') :
          h('ol', { class: 'timeline' }, attempts.map((a) => h('li', null,
            h('div', null, h('strong', null, '#' + a.attempt), h('div', null, badge(a.outcome))),
            h('div', null, h('div', null, 'on ', h('code', null, a.workerId || '?'), ' · started ', when(a.startedAt), ' · ',
              a.startedAt && a.finishedAt ? duration(new Date(a.finishedAt) - new Date(a.startedAt)) : '-'),
              a.error && h('pre', { class: 'code', style: 'margin-top:8px' }, a.error)))))));
  }

  load();
  timer = setInterval(() => { if (!body.querySelector('input:focus')) load(); }, 3000);
  return () => clearInterval(timer);
}

const prop = (label, value) => [h('dt', null, label), h('dd', null, value)];
