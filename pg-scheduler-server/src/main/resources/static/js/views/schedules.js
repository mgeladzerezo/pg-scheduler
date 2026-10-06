import { api } from '../api.js';
import { h, clear, loading, empty, errorBox, when, toast } from '../ui.js';

export function render(root) {
  const list = h('div', null, loading());
  root.append(
    h('div', { class: 'page-head' }, h('div', null, h('h1', null, 'Schedules'),
      h('p', null, 'Cron schedules, evaluated in their own time zone. Several scheduler instances may run; each fire time becomes exactly one job.'))),
    createForm(load), h('div', { class: 'card section' }, list));

  async function load() {
    try {
      draw(await api.get('/api/schedules'));
    } catch (e) {
      clear(list).append(errorBox(e, load));
    }
  }

  async function act(label, fn) {
    try { await fn(); toast(label); load(); } catch (e) { toast(e.message, true); }
  }

  function draw(schedules) {
    if (schedules.length === 0) {
      clear(list).append(empty('No schedules yet. Create one above.'));
      return;
    }
    clear(list).append(h('div', { class: 'table-wrap' }, h('table', null,
      h('thead', null, h('tr', null, ['Name', 'Cron', 'Zone', 'Job type', 'Next five fire times', 'Last fired', ''].map((t) => h('th', null, t)))),
      h('tbody', null, schedules.map((s) => h('tr', null,
        h('td', null, s.name, s.paused && h('span', { class: 'badge warn' }, 'paused'), h('div', { class: 'muted small' }, s.misfirePolicy.toLowerCase().replace('_', ' ') + ' on misfire')),
        h('td', null, h('code', null, s.cron)), h('td', null, s.zone), h('td', null, s.jobType),
        h('td', null, s.paused ? h('span', { class: 'muted' }, 'paused') : s.upcoming.map((f) => h('div', { class: 'small mono', title: f.instant }, f.local))),
        h('td', null, when(s.lastFireTime)),
        h('td', { class: 'actions' },
          h('button', { class: 'small', onclick: () => act('Job enqueued', () => api.post(`/api/schedules/${s.id}/trigger`)) }, 'Trigger now'), ' ',
          h('button', { class: 'small', onclick: () => act(s.paused ? 'Resumed' : 'Paused', () => api.post(`/api/schedules/${s.id}/${s.paused ? 'resume' : 'pause'}`)) }, s.paused ? 'Resume' : 'Pause'), ' ',
          h('button', { class: 'small danger', onclick: () => { if (confirm(`Delete schedule ${s.name}?`)) act('Deleted', () => api.del('/api/schedules/' + s.id)); } }, 'Delete'))))))));
  }

  load();
  const timer = setInterval(load, 5000);
  return () => clearInterval(timer);
}

const ZONES = ['UTC', 'Europe/Berlin', 'Europe/London', 'America/New_York', 'America/Los_Angeles', 'Asia/Tokyo', 'Asia/Kolkata', 'Australia/Sydney'];

function createForm(onCreated) {
  const name = h('input', { placeholder: 'nightly-report' });
  const cron = h('input', { value: '*/5 * * * *', class: 'mono', 'aria-label': 'Cron expression' });
  const zone = h('input', { value: 'UTC', list: 'zones' });
  const type = h('select', null, ['email.send', 'flaky.job', 'slow.job', 'always.fail'].map((t) => h('option', { value: t }, t)));
  const misfire = h('select', null, ['FIRE_ONCE', 'SKIP', 'CATCH_UP'].map((t) => h('option', { value: t }, t.toLowerCase().replace('_', ' '))));
  const payload = h('textarea', { rows: 2, placeholder: '{"to":"a@example.com","subject":"Hello"}' });
  const preview = h('div', { class: 'hint', 'aria-live': 'polite' });
  let seq = 0;

  async function refreshPreview() {
    const mine = ++seq;
    try {
      const times = await api.get(`/api/cron/preview?cron=${encodeURIComponent(cron.value)}&zone=${encodeURIComponent(zone.value)}&count=5`);
      if (mine !== seq) return;
      clear(preview).append(h('strong', null, 'Next five: '), times.map((t) => h('div', { class: 'mono small' }, t.local)));
    } catch (e) {
      if (mine === seq) clear(preview).append(h('span', { style: 'color: var(--bad)' }, e.message));
    }
  }
  cron.addEventListener('input', refreshPreview);
  zone.addEventListener('input', refreshPreview);
  refreshPreview();

  async function submit() {
    let body = null;
    try { body = payload.value.trim() ? JSON.parse(payload.value) : null; } catch (e) { toast('Payload is not valid JSON', true); return; }
    try {
      await api.post('/api/schedules', { name: name.value.trim(), cron: cron.value.trim(), zone: zone.value.trim(),
        jobType: type.value, payload: body, misfirePolicy: misfire.value });
      toast('Schedule created');
      name.value = '';
      onCreated();
    } catch (e) { toast(e.message, true); }
  }

  return h('details', { class: 'card' }, h('summary', null, 'New schedule'),
    h('div', { class: 'form-grid', style: 'margin-top:12px' },
      h('label', { class: 'field' }, 'Name', name), h('label', { class: 'field' }, 'Cron (5 fields)', cron),
      h('label', { class: 'field' }, 'Time zone', zone, h('datalist', { id: 'zones' }, ZONES.map((z) => h('option', { value: z })))),
      h('label', { class: 'field' }, 'Job type', type), h('label', { class: 'field' }, 'If fire times were missed', misfire),
      h('label', { class: 'field wide' }, 'Payload (JSON)', payload),
      h('div', { class: 'wide' }, preview),
      h('div', null, h('button', { class: 'primary', onclick: submit }, 'Create schedule'))));
}
