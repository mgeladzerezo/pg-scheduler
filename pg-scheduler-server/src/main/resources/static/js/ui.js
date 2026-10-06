// Small DOM and formatting helpers shared by the views.

/** Creates an element: h('div', {class: 'x', onclick: fn}, child, 'text', ...). Falsy children are skipped. */
export function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value == null || value === false) continue;
    if (key.startsWith('on')) el.addEventListener(key.slice(2), value);
    else if (key === 'class') el.className = value;
    else if (key === 'value') el.value = value;
    else el.setAttribute(key, value === true ? '' : value);
  }
  append(el, children);
  return el;
}

function append(el, children) {
  for (const child of children.flat(Infinity)) {
    if (child == null || child === false) continue;
    el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

export function clear(el) { el.replaceChildren(); return el; }

export function toast(message, bad = false) {
  const el = h('div', { class: 'toast' + (bad ? ' bad' : '') }, message);
  document.getElementById('toasts').append(el);
  setTimeout(() => el.remove(), bad ? 6000 : 3000);
}

export const loading = (text = 'Loading') => h('div', { class: 'loading' }, h('span', { class: 'spinner' }), text);
export const empty = (text) => h('div', { class: 'empty' }, text);
export const errorBox = (e, retry) => h('div', { class: 'error-box' },
  h('div', null, 'Could not load this: ' + (e && e.message ? e.message : e)),
  retry && h('p', null, h('button', { onclick: retry }, 'Try again')));

export const badge = (state) => h('span', { class: 'badge ' + state }, state.replace('_', ' ').toLowerCase());

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto', style: 'short' });

/** "3 s ago" / "in 2 min"; the exact time is in the title. */
export function when(iso) {
  if (!iso) return h('span', { class: 'muted' }, '-');
  const t = new Date(iso);
  const seconds = Math.round((t - Date.now()) / 1000);
  const abs = Math.abs(seconds);
  let text;
  if (abs < 60) text = rtf.format(seconds, 'second');
  else if (abs < 3600) text = rtf.format(Math.round(seconds / 60), 'minute');
  else if (abs < 86400) text = rtf.format(Math.round(seconds / 3600), 'hour');
  else text = t.toLocaleString();
  return h('span', { title: t.toLocaleString() + ' (' + iso + ')' }, text);
}

export function duration(ms) {
  if (ms == null) return '-';
  if (ms < 1000) return ms + ' ms';
  if (ms < 60000) return (ms / 1000).toFixed(1) + ' s';
  return Math.floor(ms / 60000) + ' min ' + Math.round((ms % 60000) / 1000) + ' s';
}

export const num = (n) => (n ?? 0).toLocaleString();

export function json(value) {
  return value == null ? '-' : JSON.stringify(value, null, 2);
}

/** Converts an ISO instant to the value of a datetime-local input (local time). */
export function toLocalInput(iso) {
  const d = new Date(iso);
  d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
  return d.toISOString().slice(0, 16);
}
