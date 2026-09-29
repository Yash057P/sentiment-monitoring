const TOKEN_KEY = 'brandscope_token';
const PROFILE_KEY = 'brandscope_profile';

export function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

export function getProfile() {
  try {
    return JSON.parse(localStorage.getItem(PROFILE_KEY));
  } catch {
    return null;
  }
}

export function saveSession(data) {
  localStorage.setItem(TOKEN_KEY, data.token);
  localStorage.setItem(
    PROFILE_KEY,
    JSON.stringify({
      username: data.username,
      role: data.role,
      company: data.company,
      display: data.display,
    })
  );
}

export function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(PROFILE_KEY);
}

export async function api(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  const res = await fetch(path, { ...options, headers });
  if (res.status === 401) {
    clearSession();
    window.location.hash = '#/login';
    throw new Error('Session expired');
  }
  if (!res.ok) {
    let msg = `Request failed (${res.status})`;
    try {
      const body = await res.json();
      msg = body.message || msg;
    } catch {}
    throw new Error(msg);
  }
  return res.json();
}

export async function login(username, password) {
  return api('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });
}

export async function resetLiveData(wipeFiles = true) {
  return api('/api/admin/reset', {
    method: 'POST',
    body: JSON.stringify({ wipe_files: wipeFiles }),
  });
}

export function formatTime(ms) {
  if (!ms) return '—';
  return new Date(+ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function relativeTime(ms) {
  if (!ms) return '—';
  const s = Math.max(0, Math.round((Date.now() - +ms) / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.floor(m / 60)}h ${m % 60}m ago`;
}