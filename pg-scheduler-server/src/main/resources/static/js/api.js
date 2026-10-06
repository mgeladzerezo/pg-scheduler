// JSON API client. Credentials are the browser's cached HTTP Basic login, so there is no token handling here.

async function request(method, path, body) {
  // Absolute URL without credentials: a page opened as http://user:pass@host/ would make fetch refuse a relative path.
  const response = await fetch(location.origin + path, {
    method,
    headers: body === undefined ? {} : { 'Content-Type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    let message = response.status + ' ' + response.statusText;
    try { message = (await response.json()).error || message; } catch (e) { /* not JSON */ }
    throw new Error(message);
  }
  return response.status === 204 ? null : response.json();
}

export const api = {
  get: (path) => request('GET', path),
  post: (path, body) => request('POST', path, body ?? {}),
  put: (path, body) => request('PUT', path, body),
  del: (path) => request('DELETE', path),
};

/**
 * One shared live feed for the overview: server-sent events with automatic reconnect. Each subscriber gets
 * the latest snapshot immediately if there is one.
 */
export const live = (() => {
  const listeners = new Set();
  const statusListeners = new Set();
  let last = null;
  let source = null;

  function status(state) { statusListeners.forEach((fn) => fn(state)); }

  function open() {
    source = new EventSource(location.origin + '/api/stream');
    source.addEventListener('overview', (event) => {
      last = JSON.parse(event.data);
      status('live');
      listeners.forEach((fn) => fn(last));
    });
    source.onerror = () => status(source.readyState === EventSource.CLOSED ? 'offline' : 'reconnecting');
  }

  return {
    start() { if (!source) open(); },
    subscribe(fn) { listeners.add(fn); if (last) fn(last); return () => listeners.delete(fn); },
    onStatus(fn) { statusListeners.add(fn); },
    get last() { return last; },
  };
})();
