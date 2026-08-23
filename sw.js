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
//
// [FEATURE] Push notifications ARE handled here (below) -- that's a
// separate concern from the caching decision above. Showing a push
// notification and intercepting fetches are unrelated capabilities of a
// service worker; keeping push does not reintroduce any of the caching
// bugs described above, since no response is ever read from or written
// to Cache Storage anywhere in this file.
self.addEventListener('install', () => {
  self.skipWaiting();
});

// [FIX] Respond to the page explicitly asking a waiting worker to
// activate immediately, rather than only relying on self.skipWaiting()
// running automatically on install (which a browser can still delay
// activating for an already-open tab until that tab's next navigation).
// This is what index.html's registration logic posts to a worker sitting
// in "waiting" state, so a fresh browser session that happens to still
// have an old worker registered picks up THIS version's no-cache-storage
// behavior as early as possible, instead of only on some future visit.
self.addEventListener('message', (event) => {
  if (event.data && event.data.type === 'SKIP_WAITING') {
    self.skipWaiting();
  }
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

// ── Push notifications: deployment completion ──────────────────────────
// Fired when the backend sends a push via web-push (see
// sendDeployPushNotification in server.js) after a deploy finishes,
// success or failure. Shows the JoyTree icon with a short status line,
// same idea as how Claude's own app notifies when a task finishes.
self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (_) {}

  const title = data.title || 'JoyTree';
  const options = {
    body: data.body || '',
    icon: data.icon || '/favicon_192.png',
    badge: data.badge || '/favicon_192.png',
    // tag + renotify: a second deploy notification for the SAME project
    // replaces the first instead of stacking up a pile of old ones, but
    // still alerts the user (renotify) since the new one matters.
    tag: data.tag || 'joytree-deploy',
    renotify: true,
    // [FIX] silent defaults to false per spec, but leaving it implicit
    // meant behavior varied across browsers/devices -- explicit here so
    // the system notification sound always plays. vibrate adds a haptic
    // cue on mobile alongside the sound, same as a normal phone
    // notification (not the app being focused or not -- this is a real
    // OS-level notification either way, so it always surfaces wherever
    // the person currently is, same as any other app's notifications).
    silent: false,
    vibrate: [200, 100, 200],
    // [FIX] Without this, Android/Chrome can auto-dismiss the notification
    // after a few seconds even if the person didn't see it in time (e.g.
    // screen was off and only just turned on). requireInteraction keeps it
    // sitting in the shade/lock screen until they actually dismiss or tap
    // it, instead of it quietly vanishing before they get a chance to look.
    requireInteraction: true,
    data: { url: data.url || '/dashboard' },
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

// Tapping the notification focuses an already-open JoyTree tab and
// navigates it to the deploy's logs, or opens a new tab if none is open.
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || '/dashboard';

  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if (client.url.startsWith(self.location.origin) && 'focus' in client) {
          client.navigate(url);
          return client.focus();
        }
      }
      if (self.clients.openWindow) return self.clients.openWindow(url);
    })
  );
});
