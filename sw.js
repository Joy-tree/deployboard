// [FIX] v3 — the previous fetch handler called fetch(event.request) with the
// default cache mode, which still lets the BROWSER's own HTTP cache (not the
// SW's Cache Storage) silently satisfy the request without ever reaching the
// network. That's why a genuinely new deploy could take several manual
// reloads to show up: the SW *thought* it was going network-first, but the
// browser was quietly handing back a stale disk-cached response underneath
// it. Every request now explicitly passes {cache:'no-store'}, which bypasses
// HTTP caching entirely and guarantees a real network round-trip.
const CACHE_NAME = 'joytree-shell-v3';
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
