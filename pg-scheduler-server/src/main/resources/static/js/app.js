import { live } from './api.js';
import { clear, errorBox } from './ui.js';
import * as overview from './views/overview.js';
import * as jobs from './views/jobs.js';
import * as jobDetail from './views/job-detail.js';
import * as workers from './views/workers.js';
import * as schedules from './views/schedules.js';

const routes = [
  { name: '', pattern: /^$/, view: overview },
  { name: 'jobs', pattern: /^jobs$/, view: jobs },
  { name: 'dead', pattern: /^dead$/, view: { render: (root, p) => jobs.render(root, p, { dead: true }) } },
  { name: 'jobs', pattern: /^jobs\/(\d+)$/, view: jobDetail },
  { name: 'workers', pattern: /^workers$/, view: workers },
  { name: 'schedules', pattern: /^schedules$/, view: schedules },
];

const root = document.getElementById('view');
let cleanup = null;

function route() {
  if (cleanup) { try { cleanup(); } catch (e) { console.error(e); } cleanup = null; }
  const [path, query = ''] = location.hash.replace(/^#\/?/, '').split('?');
  const params = new URLSearchParams(query);
  const match = routes.map((r) => ({ r, m: path.match(r.pattern) })).find((x) => x.m);
  document.querySelectorAll('#nav a').forEach((a) =>
    a.classList.toggle('active', match && a.dataset.route === match.r.name));
  clear(root);
  if (!match) {
    root.append(errorBox('There is no page here. Try the overview.'));
    return;
  }
  try {
    cleanup = match.r.view.render(root, params, ...match.m.slice(1)) || null;
  } catch (e) {
    console.error(e);
    root.append(errorBox(e));
  }
  root.focus({ preventScroll: true });
}

window.addEventListener('hashchange', route);

// Theme: follows the system until the user picks one.
document.getElementById('theme').addEventListener('click', () => {
  const current = document.documentElement.dataset.theme
    || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  const next = current === 'dark' ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem('theme', next); } catch (e) { /* storage unavailable */ }
});

const liveBadge = document.getElementById('live');
live.onStatus((state) => {
  liveBadge.textContent = state;
  liveBadge.className = 'pill ' + (state === 'live' ? 'ok' : 'bad');
});
live.start();
route();
