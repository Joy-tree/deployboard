// [FIX] v5 — the fetch handler below previously intercepted and cached
// EVERY GET request on the origin with no path filtering at all,
// including everything under /api/ (auth exchange, session, profile,
// etc.). Cache Storage keys responses by URL only, with no concept of
// "which user" made the request -- so any interrupted/failed API fetch
// could fail over to a stale cached response, potentially from a
// different account entirely. This SW now only ever touches the exact
// static app-shell files in CORE_ASSETS (matched by pathname, ignoring
// query strings) -- every other request, all of /api/*, and the OAuth
// callback landing on "/" with query params, goes straight to the
// network exactly as if this service worker didn't exist, and is never
// read from or written to Cache Storage.
const CACHE_NAME = 'joytree-shell-v5';
const CORE_ASSETS = ['/', '/index.html', '/theme-override.css', '/manifest.webmanifest', '/favicon_256.png', '/favicon_192.png', '/favicon_512.png', '/favicon_512_maskable.png'];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => Promise.all(CORE_ASSETS.map((url) =>
        fetch(url, { cache: 'no-store' }).then((res) => cache.put(url, res)).catch(() => {})
      )))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;

  let pathname;
  try { pathname = new URL(event.request.url).pathname; } catch (_) { return; }

  // Not one of our known static shell files -> hands-off, straight to
  // network, never cached, never intercepted. Covers all /api/* calls
  // and any query-string request (including the OAuth callback).
  if (!CORE_ASSETS.includes(pathname)) return;

  event.respondWith(
    fetch(event.request, { cache: 'no-store' })
      .then((res) => {
        const copy = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy)).catch(() => {});
        return res;
      })
      .catch(() => caches.match(event.request).then((cached) => cached || caches.match('/index.html')))
  );
});

