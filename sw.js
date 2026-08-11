// [FIX] v6 — every previous version of this file, however carefully
// scoped, still had a Cache Storage fallback: on ANY network hiccup
// (a single dropped request, not even an outage), it would hand back
// whatever copy of index.html/theme-override.css happened to be sitting
// in the cache from a previous visit — potentially hours or days old,
// predating whatever theme/auth/UI fix shipped since. That's what
// produced the "light theme sometimes loads ugly-dark" report: not a
// long-open stale tab (that's the controllerchange reload's job), but a
// completely fresh page load that hit one bad fetch and silently fell
// back to a stale shell instead of just... trying the network again or
// showing an error.
//
// This same caching layer has now been the root cause of four separate,
// unrelated bug classes across earlier versions of this file: stuck
// logins, a cross-account response leak, a stuck loading screen, and
// this stale-theme flash. That's not a sign any particular guard was
// wrong — it's a sign the caching itself isn't worth what it costs here.
// This app is not meant to work offline, and the "faster repeat loads"
// benefit isn't worth a class of bugs where the page can silently show
// old UI with no indication anything is stale.
//
// So: no Cache Storage, at all. This service worker now exists only so
// the app is installable as a PWA (a manifest + an active, non-empty
// service worker registration is what most browsers require for the
// install prompt) — every request, without exception, goes straight to
// the network, exactly as if this file weren't here.
self.addEventListener('install', () => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  // Clean up any Cache Storage entries left behind by earlier versions
  // of this service worker, so nothing stale can ever be read from them
  // again, then take control of already-open tabs immediately.
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Intentionally no 'fetch' listener — every request falls through to the
// browser's own default network handling, untouched.
