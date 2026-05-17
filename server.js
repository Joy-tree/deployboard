'use strict';

// ===== DEPLOYBOARD UPDATE MARKER (VISIBLE) ====
// UPDATED BY AGENT: 2026-04-29T00:00:00Z
// DNS behavior in this version:
// - supports Cloudflare specific-record UPSERT (create/update)
// - supports CF_FORCE_SPECIFIC_RECORDS=true to force per-subdomain records
// ===============================================

const express  = require('express');
const http     = require('http');
const { Server: SocketIO } = require('socket.io');
const path     = require('path');
const fs       = require('fs');
const mongoose = require('mongoose');
const crypto   = require('crypto');
require('dotenv').config();

const app    = express();
const server = http.createServer(app);
const io     = new SocketIO(server, { cors: { origin: '*', methods: ['GET','POST'] } });

const PORT        = process.env.PORT        || 3001;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/deployboard';
const SITES_DIR   = process.env.SITES_DIR   || '/var/www/user-sites';
const TMP_DIR     = process.env.TMP_DIR     || '/tmp/deployboard-builds';
const GITHUB_TOKEN= process.env.GITHUB_TOKEN|| '';
const BASE_DOMAIN = normalizeBaseDomain(process.env.BASE_DOMAIN || 'localhost');
const CF_API_TOKEN  = process.env.CF_API_TOKEN  || '';
const CF_ZONE_ID    = process.env.CF_ZONE_ID    || '';
const CF_TUNNEL_ID  = process.env.CF_TUNNEL_ID  || '';
const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID || '';
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID || process.env.GITHUB_OAUTH_CLIENT_ID || process.env.GH_CLIENT_ID || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || process.env.GITHUB_OAUTH_CLIENT_SECRET || process.env.GH_CLIENT_SECRET || '';
const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY || '';
let GLOBAL_WEBHOOK_SECRET = process.env.GLOBAL_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET || '';
if (!GLOBAL_WEBHOOK_SECRET) GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
const INTERNAL_DEPLOY_KEY = process.env.INTERNAL_DEPLOY_KEY || crypto.randomBytes(24).toString('hex');
const SESSION_TTL_DAYS = Number(process.env.SESSION_TTL_DAYS || 30);
const AUTO_DEPLOY_POLL_INTERVAL_MS = Math.max(250, Number(process.env.AUTO_DEPLOY_POLL_INTERVAL_MS || 750) || 750);
const AUTO_DEPLOY_INITIAL_DELAY_MS = Math.max(250, Number(process.env.AUTO_DEPLOY_INITIAL_DELAY_MS || AUTO_DEPLOY_POLL_INTERVAL_MS) || AUTO_DEPLOY_POLL_INTERVAL_MS);

function normalizeBaseDomain(value) {
  return String(value || 'localhost')
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//, '')
    .split('/')[0]
    .replace(/:[0-9]+$/, '')
    .replace(/^\.+|\.+$/g, '');
}

function normalizeHostHeader(value) {
  return String(value || '')
    .toLowerCase()
    .split(',')[0]
    .trim()
    .replace(/^https?:\/\//, '')
    .split('/')[0]
    .replace(/:[0-9]+$/, '')
    .replace(/^\.+|\.+$/g, '');
}

// ── Ensure directories ────────────────────────────────────────────────────────
[SITES_DIR, TMP_DIR].forEach(d => { try { fs.mkdirSync(d, { recursive: true }); } catch(e) {} });

// ── Port registry ─────────────────────────────────────────────────────────────
// Maps subdomain → host port where the user's Docker container is listening
const PORTS_FILE = path.join(SITES_DIR, 'ports.json');
const PORT_START = 4000;
const PORT_END   = 4099;

let portRegistry = {};
try {
  if (fs.existsSync(PORTS_FILE)) {
    portRegistry = JSON.parse(fs.readFileSync(PORTS_FILE, 'utf8'));
    console.log(`[Ports] Loaded ${Object.keys(portRegistry).length} entries`);
  }
} catch(e) { console.warn('[Ports] Could not load ports.json:', e.message); }

function savePortRegistry() {
  try { fs.writeFileSync(PORTS_FILE, JSON.stringify(portRegistry, null, 2)); }
  catch(e) { console.warn('[Ports] Could not save:', e.message); }
}

function getOrAssignPort(subdomain) {
  if (portRegistry[subdomain]) return portRegistry[subdomain];
  const used = new Set(Object.values(portRegistry));
  for (let p = PORT_START; p <= PORT_END; p++) {
    if (!used.has(p)) {
      portRegistry[subdomain] = p;
      savePortRegistry();
      console.log(`[Ports] Assigned port ${p} to ${subdomain}`);
      return p;
    }
  }
  throw new Error('No free ports available (4000-4099 all in use)');
}

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json({
  verify: (req, _res, buf) => { req.rawBody = Buffer.from(buf || ''); }
}));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Static file helper ────────────────────────────────────────────────────────
function serveStatic(req, res, distDir) {
  const mw = express.static(distDir, { index: 'index.html' });
  mw(req, res, () => {
    const idx = path.join(distDir, 'index.html');
    if (fs.existsSync(idx)) return res.sendFile(idx);
    res.status(404).send('Not found');
  });
}

// ── Subdomain routing ─────────────────────────────────────────────────────────
// Routes *.BASE_DOMAIN requests to either:
//   - A user app Docker container (proxy)
//   - Static files from SITES_DIR/subdomain/dist/
// ── Custom domain routing ─────────────────────────────────────────────────────
// If the incoming host matches a saved custom domain, serve that project directly
app.use(async (req, res, next) => {
  const host = normalizeHostHeader(
    req.headers['x-forwarded-host'] ||
    req.headers.host ||
    ''
  );

  // Skip if it looks like the base domain or localhost
  if (!host || host === BASE_DOMAIN || host.endsWith('.' + BASE_DOMAIN) || host === 'localhost') {
    return next();
  }

  try {
    const cd = await CustomDomain.findOne({ domain: host }).lean().maxTimeMS(5000);
    if (!cd) return next();

    const subdomain = cd.subdomain;
    try {
      if (fs.existsSync(PORTS_FILE)) {
        portRegistry = JSON.parse(fs.readFileSync(PORTS_FILE, 'utf8'));
      }
    } catch(e) {}

    const appEntry = portRegistry[subdomain];
    const distDir  = path.join(SITES_DIR, subdomain, 'dist');

    if (appEntry) {
      let proxyHost, proxyPort;
      if (typeof appEntry === 'string' && appEntry.includes(':')) {
        [proxyHost, proxyPort] = appEntry.split(':');
        proxyPort = parseInt(proxyPort);
      } else {
        proxyHost = '127.0.0.1';
        proxyPort = parseInt(appEntry);
      }
      const hopByHop = ['connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailers','transfer-encoding','upgrade'];
      const fwdHeaders = { ...req.headers };
      hopByHop.forEach(h => delete fwdHeaders[h]);
      fwdHeaders['host'] = host;
      fwdHeaders['x-forwarded-for'] = req.ip || '';
      fwdHeaders['x-real-ip'] = req.ip || '';
      fwdHeaders['x-forwarded-proto'] = 'https';

      const proxyReq = require('http').request(
        { hostname: proxyHost, port: proxyPort, path: req.url, method: req.method, headers: fwdHeaders },
        (proxyRes) => {
          res.writeHead(proxyRes.statusCode, proxyRes.headers);
          proxyRes.pipe(res, { end: true });
          proxyRes.on('error', () => { if (!res.headersSent) res.end(); });
        }
      );
      proxyReq.setTimeout(30000, () => { proxyReq.destroy(); if (!res.headersSent) res.status(502).end(); });
      proxyReq.on('error', () => { if (!res.headersSent) res.status(502).end(); });
      req.pipe(proxyReq, { end: true });
      req.on('error', () => proxyReq.destroy());
      return;
    }

    if (fs.existsSync(distDir)) {
      const mw = require('express').static(distDir, { index: 'index.html' });
      return mw(req, res, () => {
        const idx = path.join(distDir, 'index.html');
        if (fs.existsSync(idx)) return res.sendFile(idx);
        res.status(404).end();
      });
    }

    return res.status(404).send(`<h2>${host}</h2><p>Project not found or not deployed yet.</p>`);
  } catch(e) {
    console.error('[CustomDomain] Error:', e.message);
    next();
  }
});

app.use((req, res, next) => {
  // Cloudflare Tunnel forwards the original hostname in X-Forwarded-Host.
  // Fall back to the Host header when running locally or without a tunnel.
  const host = normalizeHostHeader(
    req.headers['x-forwarded-host'] ||
    req.headers.host ||
    ''
  );

  const regex = new RegExp(`^([a-z0-9][a-z0-9-]{0,61}[a-z0-9]?)\\.${BASE_DOMAIN.replace(/\./g,'\\.')}$`);
  const match = host.match(regex);
  if (!match) return next();

  const subdomain = match[1];

  // Reload port registry from disk on every request so newly-deployed apps
  // are reachable immediately without restarting DeployBoard.
  try {
    if (fs.existsSync(PORTS_FILE)) {
      portRegistry = JSON.parse(fs.readFileSync(PORTS_FILE, 'utf8'));
    }
  } catch(e) {}

  const appEntry  = portRegistry[subdomain];
  const distDir   = path.join(SITES_DIR, subdomain, 'dist');

  // ── Server app: proxy to its isolated Docker container ──────────────────
  if (appEntry) {
    // appEntry can be "192.168.x.x:3000" (Docker network) or plain port number (legacy)
    let proxyHost, proxyPort;
    if (typeof appEntry === 'string' && appEntry.includes(':')) {
      [proxyHost, proxyPort] = appEntry.split(':');
      proxyPort = parseInt(proxyPort);
    } else {
      proxyHost = '127.0.0.1';
      proxyPort = parseInt(appEntry);
    }
    const appPort = proxyPort; // for display in error pages

    // Strip hop-by-hop headers before forwarding (prevents proxy errors)
    const hopByHop = ['connection','keep-alive','proxy-authenticate',
                      'proxy-authorization','te','trailers','transfer-encoding','upgrade'];
    const forwardHeaders = { ...req.headers };
    hopByHop.forEach(h => delete forwardHeaders[h]);
    forwardHeaders['host']            = req.headers.host;
    forwardHeaders['x-forwarded-for'] = req.ip || '';
    forwardHeaders['x-real-ip']       = req.ip || '';
    forwardHeaders['x-forwarded-proto'] = 'https';

    const proxyReq = http.request({
      hostname: proxyHost,
      port:     proxyPort,
      path:     req.url,
      method:   req.method,
      headers:  forwardHeaders
    }, (proxyRes) => {
      // Intercept framework 404 on root so it shows a nice page
      if (proxyRes.statusCode === 404 && req.url === '/') {
        let body = '';
        proxyRes.on('data', c => body += c);
        proxyRes.on('end', () => {
          if (body.includes('Cannot GET') || body.includes('Not Found')) {
            return res.status(200).send(noRootRoutePage(subdomain, appPort));
          }
          res.writeHead(proxyRes.statusCode, proxyRes.headers);
          res.end(body);
        });
        return;
      }
      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res, { end: true });
      proxyRes.on('error', () => { if (!res.headersSent) res.end(); });
    });

    proxyReq.setTimeout(30000, () => {
      proxyReq.destroy();
      if (!res.headersSent) res.status(502).send(errorPage('App Timed Out',
        `${subdomain} did not respond in 30 seconds. The app may still be starting — wait a moment and refresh.`));
    });

    proxyReq.on('error', () => {
      if (!res.headersSent) res.status(502).send(errorPage('App Not Running',
        `${subdomain} container is not responding on port ${appPort}. Redeploy it.`));
    });

    req.pipe(proxyReq, { end: true });
    req.on('error', () => proxyReq.destroy());
    return;
  }

  // ── Static site ───────────────────────────────────────────────────────────
  if (fs.existsSync(distDir)) return serveStatic(req, res, distDir);

  // ── Not deployed ──────────────────────────────────────────────────────────
  res.status(404).send(errorPage('Not Deployed',
    `${subdomain}.${BASE_DOMAIN} has not been deployed yet.`));
});

// Dashboard static serving is registered AFTER all API routes (see bottom of file)

// ── HTML helpers ─────────────────────────────────────────────────────────────
function errorPage(title, msg) {
  return `<!DOCTYPE html><html><head><title>${title}</title>
    <style>body{font-family:sans-serif;background:#060b14;color:#e2e8f0;display:flex;
    align-items:center;justify-content:center;height:100vh;margin:0}
    .b{text-align:center;padding:2rem}a{color:#10b981}
    code{background:#1e293b;padding:3px 8px;border-radius:4px}</style></head>
    <body><div class="b"><h1>${title}</h1><p>${msg}</p>
    <p><a href="https://${BASE_DOMAIN}">Back to DeployBoard</a></p>
    </div></body></html>`;
}

function noRootRoutePage(subdomain, port) {
  return `<!DOCTYPE html><html><head><title>${subdomain} is live</title>
    <style>body{font-family:sans-serif;background:#060b14;color:#e2e8f0;display:flex;
    align-items:center;justify-content:center;height:100vh;margin:0}
    .b{text-align:center;padding:2rem}
    .badge{background:#10b981;color:#fff;padding:4px 14px;border-radius:20px;
    font-size:.85rem;display:inline-block;margin-bottom:1rem}
    p{color:#94a3b8}code{background:#1e293b;padding:3px 8px;border-radius:4px}</style></head>
    <body><div class="b">
    <div class="badge">✓ App is Running</div>
    <h2>${subdomain}.${BASE_DOMAIN}</h2>
    <p>Your app is live in its own container on port <code>${port}</code>.</p>
    <p>This app has no <code>/</code> route — try a specific path like<br>
    <code>/api</code>, <code>/users</code>, <code>/messages</code>, etc.</p>
    </div></body></html>`;
}

// ── Cloudflare ────────────────────────────────────────────────────────────────
async function registerSubdomain(subdomain) {
  const fullDomain  = `${subdomain}.${BASE_DOMAIN}`;
  const forceSpecificRecords = process.env.CF_FORCE_SPECIFIC_RECORDS === 'true';
  const wildcardMode = process.env.CF_WILDCARD_MODE !== 'false';
  const shouldSkipRecordCreate = wildcardMode && !forceSpecificRecords;
  if (shouldSkipRecordCreate || !CF_API_TOKEN || !CF_ZONE_ID) {
    return { ok: true, url: `https://${fullDomain}` };
  }
  try {
    const target = CF_TUNNEL_ID ? `${CF_TUNNEL_ID}.cfargotunnel.com` : BASE_DOMAIN;
    const lookup = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records?name=${fullDomain}`, {
      headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` }
    });
    const lookupData = await lookup.json();
    const existing = lookupData?.result?.[0];

    const body = { type: 'CNAME', name: subdomain, content: target, proxied: true, ttl: 1 };
    const endpoint = existing
      ? `https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records/${existing.id}`
      : `https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records`;
    const method = existing ? 'PUT' : 'POST';
    const r = await fetch(endpoint, {
      method,
      headers: { 'Authorization': `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!d.success) {
      return { ok: false, reason: d.errors?.[0]?.message || 'DNS upsert error' };
    }
  } catch(e) { return { ok: false, reason: e.message }; }
  return { ok: true, url: `https://${fullDomain}` };
}

async function removeSubdomain(subdomain) {
  if (!CF_API_TOKEN || !CF_ZONE_ID) return;
  try {
    const r  = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records?name=${subdomain}.${BASE_DOMAIN}`,
      { headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` } });
    const d  = await r.json();
    if (d.result?.length) {
      await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records/${d.result[0].id}`,
        { method: 'DELETE', headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` } });
    }
  } catch(e) {}
}

// ── MongoDB models ────────────────────────────────────────────────────────────
// ── Custom Domain model ──────────────────────────────────────────────────────
const customDomainSchema = new mongoose.Schema({
  domain:    { type: String, required: true, unique: true }, // e.g. "mysite.com"
  subdomain: { type: String, required: true },               // which DeployBoard project
  verified:  { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now }
});
const CustomDomain = mongoose.model('CustomDomain', customDomainSchema);

const projectSchema = new mongoose.Schema({
  name:       { type: String, required: true },
  isDockerfileDeploy: { type: Boolean, default: false },
  isWorker:           { type: Boolean, default: false },
  dockerfilePath:     { type: String, default: 'Dockerfile' },
  exposedPort:        { type: Number, default: 3000 },
  subdomain:  { type: String, required: true, unique: true },
  repoUrl:    { type: String, required: true },
  branch:     { type: String, default: 'main' },
  installCmd: { type: String, default: 'npm install' },
  buildCmd:   { type: String, default: 'npm run build' },
  startCmd:   { type: String, default: '' },
  outputDir:  { type: String, default: 'dist' },
  nodeVer:    { type: String, default: '20' },
  siteType:   { type: String, default: 'static' },
  appPort:    { type: Number, default: 0 },
  envVars:    { type: Map, of: String, default: {} },
  liveUrl:    { type: String, default: '' },
  createdAt:  { type: Date,   default: Date.now },
  updatedAt:  { type: Date,   default: Date.now },
  ownerUserId: { type: String, default: '', index: true },
  autoDeployEnabled: { type: Boolean, default: false },
  autoDeploySecret: { type: String, default: '' },
  autoDeployMode: { type: String, default: 'polling' },
  autoDeployLastSha: { type: String, default: '' },
  autoDeployLastCheckedAt: { type: Date, default: null },
  autoDeployLastTriggeredAt: { type: Date, default: null },
  autoDeployLastCompletedAt: { type: Date, default: null },
  autoDeployLastError: { type: String, default: '' },
  autoDeployStatus: { type: String, default: 'idle' }
});

const deploymentSchema = new mongoose.Schema({
  projectId:   { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
  projectName: String,
  branch:      { type: String, default: 'main' },
  status:      { type: String, enum: ['pending','building','success','failed'], default: 'pending' },
  logs:        [String],
  duration:    Number,
  startedAt:   { type: Date, default: Date.now },
  endedAt:     Date,
  source:      { type: String, enum: ['manual','auto'], default: 'manual', index: true },
  triggerSha:  { type: String, default: '', index: true }
});

const userSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  passwordHash: { type: String, default: '' },
  passwordSalt: { type: String, default: '' },
  name: { type: String, default: '' },
  githubId: { type: String, default: '', index: true },
  githubUsername: { type: String, default: '' },
  githubAccessToken: { type: String, default: '' },
  githubAvatarUrl: { type: String, default: '' },
  firebaseUid: { type: String, default: '', index: true },
  workspace: {
    projects: { type: Array, default: [] },
    deployments: { type: Array, default: [] },
    envStore: { type: Object, default: {} },
    settings: { type: Object, default: {} }
  },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});
const sessionSchema = new mongoose.Schema({
  token: { type: String, required: true, unique: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  createdAt: { type: Date, default: Date.now },
  expiresAt: { type: Date, required: true }
});

const Project    = mongoose.model('Project',    projectSchema);
const Deployment = mongoose.model('Deployment', deploymentSchema);
const User = mongoose.model('User', userSchema);
const Session = mongoose.model('Session', sessionSchema);

const LOCAL_AUTH_FILE = path.join(SITES_DIR, 'local-auth.json');
let localAuth = { users: [], sessions: [] };
try {
  if (fs.existsSync(LOCAL_AUTH_FILE)) localAuth = JSON.parse(fs.readFileSync(LOCAL_AUTH_FILE, 'utf8'));
} catch {}
function saveLocalAuth() { try { fs.writeFileSync(LOCAL_AUTH_FILE, JSON.stringify(localAuth, null, 2)); } catch {} }
function isDbReady() { return mongoose.connection.readyState === 1; }
function createPasswordHash(password, salt = crypto.randomBytes(16).toString('hex')) {
  const hash = crypto.pbkdf2Sync(password, salt, 120000, 64, 'sha512').toString('hex');
  return { salt, hash };
}
function createSessionToken() {
  return crypto.randomBytes(36).toString('hex');
}
async function getAuthUser(req) {
  const auth = (req.headers.authorization || '').trim();
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
  if (!token) return null;
  if (isDbReady()) {
    const session = await Session.findOne({ token, expiresAt: { $gt: new Date() } }).lean();
    if (!session) return null;
    return User.findById(session.userId);
  }
  const session = localAuth.sessions.find(s => s.token === token && new Date(s.expiresAt) > new Date());
  if (!session) return null;
  return localAuth.users.find(u => u.id === session.userId) || null;
}

async function upsertFirebaseGitHubUser(githubAccessToken) {
  if (!FIREBASE_API_KEY || !githubAccessToken) return null;
  const r = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithIdp?key=${encodeURIComponent(FIREBASE_API_KEY)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      requestUri: 'https://localhost/firebase-auth',
      returnSecureToken: true,
      returnIdpCredential: true,
      postBody: `access_token=${encodeURIComponent(githubAccessToken)}&providerId=github.com`
    })
  });
  const d = await r.json().catch(()=>null);
  if (!r.ok) return null;
  return d;
}

async function verifyFirebaseIdToken(idToken) {
  if (!FIREBASE_API_KEY) throw new Error('FIREBASE_API_KEY is not configured on server');
  const r = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:lookup?key=${encodeURIComponent(FIREBASE_API_KEY)}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ idToken })
  });
  const d = await r.json();
  if (!r.ok || !Array.isArray(d.users) || !d.users[0]) throw new Error(d.error?.message || 'Invalid Firebase ID token');
  return d.users[0];
}

async function requireAuth(req, res, next) {
  try {
    const internalKey = String(req.headers['x-deployboard-internal-key'] || '');
    if (internalKey && timingSafeEqualString(internalKey, INTERNAL_DEPLOY_KEY)) {
      const ownerId = String(req.headers['x-deployboard-owner-id'] || '').trim();
      if (!ownerId) return res.status(401).json({ error: 'Unauthorized' });
      const owner = isDbReady()
        ? await User.findById(ownerId).catch(()=>null)
        : localAuth.users.find(u => String(u.id) === ownerId || String(u._id || '') === ownerId);
      if (!owner) return res.status(401).json({ error: 'Unauthorized' });
      req.user = owner;
      return next();
    }
    const user = await getAuthUser(req);
    if (!user) return res.status(401).json({ error: 'Unauthorized' });
    req.user = user;
    next();
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
}

async function attachAuthIfPresent(req, _res, next) {
  try {
    const user = await getAuthUser(req);
    if (user) req.user = user;
    next();
  } catch (_) {
    next();
  }
}

// Connect with retry so container startup timing doesn't break things
function connectMongo(retries=5, delay=3000) {
  mongoose.connect(MONGODB_URI, {
    serverSelectionTimeoutMS: 8000,
    socketTimeoutMS: 45000,
    connectTimeoutMS: 10000,
  })
  .then(() => console.log('[DB] MongoDB connected to:', MONGODB_URI.replace(/\/\/.*@/, '//***@')))
  .catch(e => {
    console.warn('[DB] MongoDB connection failed:', e.message);
    if (retries > 0) {
      console.log(`[DB] Retrying in ${delay/1000}s (${retries} attempts left)...`);
      setTimeout(() => connectMongo(retries-1, delay), delay);
    } else {
      console.error('[DB] All connection attempts failed. Custom domains and env vars will not persist.');
    }
  });
}
connectMongo();

// ── Build runner ──────────────────────────────────────────────────────────────
const { runBuild } = require('./buildRunner');

// ── API routes ────────────────────────────────────────────────────────────────
app.get('/api/health', async (req, res) => {
  let runningContainers = '—';
  let diskUsed = '—';
  try {
    const { execSync } = require('child_process');
    const containers = execSync("docker ps --filter 'name=db-' --format '{{.Names}}' 2>/dev/null || echo ''", { encoding:'utf8' }).trim();
    runningContainers = containers ? containers.split('\n').filter(Boolean).length : 0;
  } catch(e) {}
  try {
    const { execSync } = require('child_process');
    diskUsed = execSync('du -sh /var/www/user-sites 2>/dev/null || echo "N/A"', { encoding:'utf8' }).trim().split('\t')[0];
  } catch(e) {}
  res.json({
    ok: true, baseDomain: BASE_DOMAIN,
    db: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
    uptime: Math.round(process.uptime()) + 's',
    runningContainers, diskUsed
  });
});

app.post('/api/auth/firebase', async (req, res) => {
  try {
    const idToken = String(req.body.idToken || '');
    if (!idToken) return res.status(400).json({ error: 'Missing Firebase idToken' });
    const fbUser = await verifyFirebaseIdToken(idToken);
    const email = String(fbUser.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'Firebase user has no email' });
    let user = isDbReady() ? await User.findOne({ email }) : localAuth.users.find(u => u.email === email);
    if (!user) {
      if (isDbReady()) user = await User.create({ email, name: fbUser.displayName || '', firebaseUid: fbUser.localId || '' });
      else { user = { id: 'u_' + Date.now(), email, name: fbUser.displayName || '', firebaseUid: fbUser.localId || '', githubUsername: '', githubAccessToken: '' }; localAuth.users.push(user); saveLocalAuth(); }
    }
    user.firebaseUid = fbUser.localId || user.firebaseUid || '';
    user.name = user.name || fbUser.displayName || '';
    if (isDbReady()) await user.save(); else saveLocalAuth();
    const token = createSessionToken();
    const expiresAt = new Date(Date.now() + SESSION_TTL_DAYS * 86400000);
    if (isDbReady()) await Session.create({ token, userId: user._id, expiresAt });
    else { localAuth.sessions.push({ token, userId: user.id, expiresAt }); saveLocalAuth(); }
    res.json({ token, user: { id: user._id || user.id, email: user.email, name: user.name, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl || '', firebaseUid: user.firebaseUid } });
  } catch (e) { res.status(401).json({ error: e.message }); }
});

app.post('/api/auth/signup', async (req, res) => {
  try {
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');
    const name = String(req.body.name || '').trim();
    if (!email || !password || password.length < 6) return res.status(400).json({ error: 'Invalid signup payload' });
    let existing = null;
    if (isDbReady()) existing = await User.findOne({ email });
    else existing = localAuth.users.find(u => u.email === email);
    if (existing) return res.status(409).json({ error: 'Email already exists' });
    const { salt, hash } = createPasswordHash(password);
    let user;
    if (isDbReady()) user = await User.create({ email, name, passwordSalt: salt, passwordHash: hash });
    else {
      user = { id: 'u_' + Date.now(), email, name, passwordSalt: salt, passwordHash: hash, githubUsername: '', githubAccessToken: '' };
      localAuth.users.push(user); saveLocalAuth();
    }
    const token = createSessionToken();
    const expiresAt = new Date(Date.now() + SESSION_TTL_DAYS * 86400000);
    if (isDbReady()) await Session.create({ token, userId: user._id, expiresAt });
    else { localAuth.sessions.push({ token, userId: user.id, expiresAt }); saveLocalAuth(); }
    res.json({ token, user: { id: user._id || user.id, email: user.email, name: user.name, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl || '', firebaseUid: user.firebaseUid || '' } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');
    const user = isDbReady() ? await User.findOne({ email }) : localAuth.users.find(u => u.email === email);
    if (!user || !user.passwordSalt || !user.passwordHash) return res.status(401).json({ error: 'Invalid credentials' });
    const { hash } = createPasswordHash(password, user.passwordSalt);
    if (hash !== user.passwordHash) return res.status(401).json({ error: 'Invalid credentials' });
    const token = createSessionToken();
    const expiresAt = new Date(Date.now() + SESSION_TTL_DAYS * 86400000);
    if (isDbReady()) await Session.create({ token, userId: user._id, expiresAt });
    else { localAuth.sessions.push({ token, userId: user.id, expiresAt }); saveLocalAuth(); }
    res.json({ token, user: { id: user._id || user.id, email: user.email, name: user.name, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl || '', firebaseUid: user.firebaseUid || '' } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/auth/me', requireAuth, async (req, res) => {
  res.json({ user: { id: req.user._id || req.user.id, email: req.user.email, name: req.user.name, githubUsername: req.user.githubUsername, githubAvatarUrl: req.user.githubAvatarUrl || '', firebaseUid: req.user.firebaseUid || '' } });
});

function normalizeGitHubClientId(value) {
  return String(value || '').trim();
}

app.get('/api/auth/github/url', (req, res) => {
  if (!GITHUB_CLIENT_ID) return res.status(400).json({ error: 'GitHub OAuth client ID is not configured' });
  const origin = getPublicOrigin(req);
  const configuredRedirect = process.env.GITHUB_REDIRECT_URI || process.env.GITHUB_OAUTH_REDIRECT_URI || '';
  let redirectUri = `${origin}/`;
  try {
    if (configuredRedirect) {
      const normalizedRedirect = /^https?:\/\//i.test(configuredRedirect)
        ? configuredRedirect
        : `https://${configuredRedirect.replace(/^\/+/, '')}`;
      const parsed = new URL(normalizedRedirect);
      if (parsed.protocol === 'https:' || parsed.hostname === 'localhost') {
        redirectUri = normalizedRedirect;
      }
    }
  } catch (_) {}
  const normalizedClientId = normalizeGitHubClientId(GITHUB_CLIENT_ID);
  const url = new URL('https://github.com/login/oauth/authorize');
  url.searchParams.set('client_id', normalizedClientId);
  url.searchParams.set('redirect_uri', redirectUri);
  url.searchParams.set('scope', 'repo read:user user:email');
  url.searchParams.set('state', 'deployboard_github_auth');
  res.json({ url: url.toString(), redirectUri, origin, clientIdHint: normalizedClientId.slice(0,6) + '...' });
});

app.post('/api/auth/github/exchange', async (req, res) => {
  try {
    if (!GITHUB_CLIENT_ID || !GITHUB_CLIENT_SECRET) {
      return res.status(400).json({
        error: 'GitHub OAuth is not configured',
        details: {
          missingClientId: !GITHUB_CLIENT_ID,
          missingClientSecret: !GITHUB_CLIENT_SECRET,
          acceptedEnvVars: [
            'GITHUB_CLIENT_ID','GITHUB_OAUTH_CLIENT_ID','GH_CLIENT_ID',
            'GITHUB_CLIENT_SECRET','GITHUB_OAUTH_CLIENT_SECRET','GH_CLIENT_SECRET'
          ]
        }
      });
    }
    const code = String(req.body.code || '');
    if (!code) return res.status(400).json({ error: 'Missing code' });
    const tokenResp = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: normalizeGitHubClientId(GITHUB_CLIENT_ID), client_secret: GITHUB_CLIENT_SECRET, code })
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(400).json({ error: tokenData.error_description || 'GitHub token exchange failed' });
    const fbGh = await upsertFirebaseGitHubUser(tokenData.access_token);
    const firebaseLinked = !!(fbGh && fbGh.localId);
    const ghUserResp = await fetch('https://api.github.com/user', {
      headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'User-Agent': 'deployboard' }
    });
    const ghUser = await ghUserResp.json();
    const email = ghUser.email || fbGh?.email || `${ghUser.login}@github.local`;
    let user;
    if (isDbReady()) {
      user = await User.findOne({ githubId: String(ghUser.id) });
      if (!user) user = await User.findOne({ email });
      if (!user) user = new User({ email });
      user.githubId = String(ghUser.id);
      user.githubUsername = ghUser.login || '';
      user.githubAccessToken = tokenData.access_token;
      user.githubAvatarUrl = ghUser.avatar_url || user.githubAvatarUrl || '';
      user.name = user.name || ghUser.name || ghUser.login || '';
      user.firebaseUid = fbGh?.localId || user.firebaseUid || '';
      user.updatedAt = new Date();
      await user.save();
    } else {
      user = localAuth.users.find(u => u.githubId === String(ghUser.id)) || localAuth.users.find(u => u.email === email);
      if (!user) { user = { id: 'u_' + Date.now(), email, githubAvatarUrl: '' }; localAuth.users.push(user); }
      user.githubId = String(ghUser.id);
      user.githubUsername = ghUser.login || '';
      user.githubAccessToken = tokenData.access_token;
      user.githubAvatarUrl = ghUser.avatar_url || user.githubAvatarUrl || '';
      user.name = user.name || ghUser.name || ghUser.login || '';
      user.firebaseUid = fbGh?.localId || user.firebaseUid || '';
      saveLocalAuth();
    }
    const token = createSessionToken();
    if (isDbReady()) await Session.create({ token, userId: user._id, expiresAt: new Date(Date.now() + SESSION_TTL_DAYS * 86400000) });
    else { localAuth.sessions.push({ token, userId: user.id, expiresAt: new Date(Date.now() + SESSION_TTL_DAYS * 86400000) }); saveLocalAuth(); }
    res.json({ token, user: { id: user._id || user.id, email: user.email, name: user.name, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl || '', firebaseUid: user.firebaseUid || '' }, firebaseLinked });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/api/workspace', requireAuth, async (req, res) => {
  const ws = req.user.workspace || {};
  res.json({
    projects: Array.isArray(ws.projects) ? ws.projects : [],
    deployments: Array.isArray(ws.deployments) ? ws.deployments : [],
    envStore: ws.envStore && typeof ws.envStore === 'object' ? ws.envStore : {},
    settings: ws.settings && typeof ws.settings === 'object' ? ws.settings : {}
  });
});

app.post('/api/workspace', requireAuth, async (req, res) => {
  try {
    const payload = req.body || {};
    const workspace = {
      projects: Array.isArray(payload.projects) ? payload.projects : [],
      deployments: Array.isArray(payload.deployments) ? payload.deployments : [],
      envStore: payload.envStore && typeof payload.envStore === 'object' ? payload.envStore : {},
      settings: payload.settings && typeof payload.settings === 'object' ? payload.settings : {}
    };
    if (isDbReady()) {
      await User.updateOne({ _id: req.user._id }, { $set: { workspace, updatedAt: new Date() } });
    } else {
      const user = localAuth.users.find(u => String(u.id) === String(req.user.id));
      if (user) user.workspace = workspace;
      saveLocalAuth();
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.get('/api/github/repos', requireAuth, async (req, res) => {
  try {
    const token = req.user.githubAccessToken;
    if (!token) return res.status(400).json({ error: 'GitHub account not connected' });
    let all = [];
    for (let page = 1; page <= 3; page++) {
      const r = await fetch(`https://api.github.com/user/repos?per_page=100&page=${page}&sort=updated&visibility=all&affiliation=owner,collaborator,organization_member`, {
        headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'deployboard', 'Accept': 'application/vnd.github+json' }
      });
      const data = await r.json();
      if (!r.ok) {
        return res.status(r.status).json({
          error: data?.message || 'GitHub repos fetch failed',
          githubStatus: r.status,
          requiredScopeHint: 'repo',
          acceptedOauthScopes: r.headers.get('x-oauth-scopes') || ''
        });
      }
      if (!Array.isArray(data) || !data.length) break;
      all = all.concat(data);
      if (data.length < 100) break;
    }
    const repos = all.map((repo) => ({
      id: repo.id,
      name: repo.name,
      full_name: repo.full_name,
      private: repo.private,
      visibility: repo.visibility,
      description: repo.description || '',
      html_url: repo.html_url,
      homepage: repo.homepage || '',
      default_branch: repo.default_branch,
      updated_at: repo.updated_at,
      pushed_at: repo.pushed_at,
      created_at: repo.created_at,
      language: repo.language,
      languages: {},
      topics: Array.isArray(repo.topics) ? repo.topics : [],
      fork: !!repo.fork,
      archived: !!repo.archived,
      disabled: !!repo.disabled,
      stargazers_count: repo.stargazers_count || 0,
      forks_count: repo.forks_count || 0,
      open_issues_count: repo.open_issues_count || 0,
      watchers_count: repo.watchers_count || 0,
      size: repo.size || 0,
      license: repo.license ? { key: repo.license.key, name: repo.license.name, spdx_id: repo.license.spdx_id } : null,
      languages_url: repo.languages_url
    }));

    const languageHeaders = {
      'Authorization': `Bearer ${token}`,
      'User-Agent': 'deployboard',
      'Accept': 'application/vnd.github+json'
    };
    const languageEnrichmentLimit = Math.min(repos.length, 100);
    for (let i = 0; i < languageEnrichmentLimit; i += 20) {
      const batch = repos.slice(i, i + 20);
      await Promise.all(batch.map(async (repo) => {
        if (!repo.languages_url) return;
        try {
          const lr = await fetch(repo.languages_url, { headers: languageHeaders, signal: AbortSignal.timeout(2500) });
          if (!lr.ok) return;
          const languages = await lr.json();
          if (!languages || typeof languages !== 'object' || Array.isArray(languages)) return;
          repo.languages = languages;
          if (!repo.language) {
            const primary = Object.entries(languages).sort((a,b) => Number(b[1] || 0) - Number(a[1] || 0))[0];
            if (primary) repo.language = primary[0];
          }
        } catch (_) {}
      }));
    }

    res.json({ repos });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/github/branches', requireAuth, async (req, res) => {
  try {
    const token = req.user.githubAccessToken;
    const repo = String(req.query.repo || '').trim(); // owner/name
    if (!token) return res.status(400).json({ error: 'GitHub account not connected' });
    if (!repo || !repo.includes('/')) return res.status(400).json({ error: 'repo query is required as owner/name' });
    const r = await fetch(`https://api.github.com/repos/${repo}/branches?per_page=100`, {
      headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'deployboard' }
    });
    const data = await r.json();
    const branches = Array.isArray(data) ? data.map(b => b.name).filter(Boolean) : [];
    res.json({ branches });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/debug/host', (req, res) => {
  const token = (req.query.token || req.headers['x-debug-token'] || '').toString();
  const requiredToken = process.env.DEBUG_HOST_TOKEN || '';
  const isLocal = req.ip === '127.0.0.1' || req.ip === '::1' || req.ip === '::ffff:127.0.0.1';

  if (requiredToken && token !== requiredToken) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!requiredToken && !isLocal) {
    return res.status(403).json({ error: 'Set DEBUG_HOST_TOKEN to enable remote host debugging' });
  }

  const rawForwardedHost = req.headers['x-forwarded-host'] || '';
  const rawHost = req.headers.host || '';

  res.json({
    ok: true,
    now: new Date().toISOString(),
    baseDomain: BASE_DOMAIN,
    ip: req.ip,
    ips: req.ips || [],
    headers: {
      host: rawHost,
      xForwardedHost: rawForwardedHost,
      xForwardedProto: req.headers['x-forwarded-proto'] || '',
      cfRay: req.headers['cf-ray'] || '',
      cfConnectingIp: req.headers['cf-connecting-ip'] || ''
    },
    normalized: {
      host: normalizeHostHeader(rawHost),
      forwardedHost: normalizeHostHeader(rawForwardedHost),
      effectiveHost: normalizeHostHeader(rawForwardedHost || rawHost)
    }
  });
});

app.get('/api/projects', attachAuthIfPresent, async (req, res) => {
  try {
    const mineOnly = String(req.query.mine || '') === '1';
    if (mineOnly && !req.user) return res.status(401).json({ error: 'Unauthorized' });
    const filter = mineOnly ? { ownerUserId: String(req.user._id || req.user.id || '') } : {};
    const projects  = await Project.find(filter).sort({ createdAt: -1 });
    const enriched  = await Promise.all(projects.map(async p => {
      const last = await Deployment.findOne({ projectId: p._id }).sort({ startedAt: -1 }).select('status duration endedAt');
      const obj  = p.toObject();
      obj.id = String(p._id);
      obj.lastDeployStatus   = last?.status   || null;
      obj.lastDeployDuration = last?.duration || null;
      return obj;
    }));
    res.json(enriched);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/projects/:id', async (req, res) => {
  try {
    const p = await Project.findById(req.params.id);
    if (!p) return res.status(404).json({ error: 'Not found' });
    res.json(p);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/projects/:id', async (req, res) => {
  try {
    const p = await Project.findByIdAndDelete(req.params.id);
    if (p) {
      await Deployment.deleteMany({ projectId: req.params.id });
      // Remove site files
      try { fs.rmSync(path.join(SITES_DIR, p.subdomain), { recursive: true, force: true }); } catch(e) {}
      // Stop and remove user app Docker container
      try {
        const { execSync } = require('child_process');
        execSync(`docker rm -f db-${p.subdomain}`, { stdio: 'pipe' });
        console.log(`[Docker] Removed container db-${p.subdomain}`);
      } catch(e) {}
      // Remove from port registry
      delete portRegistry[p.subdomain];
      savePortRegistry();
      // Remove CF DNS
      await removeSubdomain(p.subdomain);
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/deployments', attachAuthIfPresent, async (req, res) => {
  try {
    const mineOnly = String(req.query.mine || '') === '1';
    if (mineOnly && !req.user) return res.status(401).json({ error: 'Unauthorized' });
    const filter = req.query.projectId ? { projectId: req.query.projectId } : {};
    if (mineOnly) {
      const owned = await Project.find({ ownerUserId: String(req.user._id || req.user.id || '') }).select('_id').lean().maxTimeMS(5000);
      filter.projectId = { $in: owned.map(p => p._id) };
    }
    const rows = await Deployment.find(filter).sort({ startedAt: -1 }).limit(100).lean();
    res.json(rows.map(d => ({ ...d, id: String(d._id), projectId: String(d.projectId || '') })));
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Custom Domain API ─────────────────────────────────────────────────────────
// In-memory fallback for domains when MongoDB is unavailable
const memDomains = [];

function isMongoReady() {
  return mongoose.connection.readyState === 1;
}

app.get('/api/domains', async (req, res) => {
  try {
    if (!isMongoReady()) return res.json(memDomains);
    res.json(await CustomDomain.find().sort({ createdAt: -1 }).lean().maxTimeMS(5000));
  } catch(e) { res.json(memDomains); }
});

app.post('/api/domains', async (req, res) => {
  const { domain, subdomain } = req.body;
  if (!domain || !subdomain) return res.status(400).json({ error: 'domain and subdomain are required' });
  const clean = normalizeHostHeader(domain);
  if (!clean) return res.status(400).json({ error: 'Invalid domain' });
  try {
    if (!isMongoReady()) {
      // In-memory fallback
      const exists = memDomains.find(d => d.domain === clean);
      if (exists) {
        if (exists.subdomain !== subdomain) return res.status(409).json({ error: 'Domain already registered to another project' });
        return res.json({ ok: true, domain: exists, existing: true, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
      }
      const entry = { domain: clean, subdomain, verified: false, createdAt: new Date() };
      memDomains.push(entry);
      addActivity('domain', 'Custom domain added (mem): ' + clean + ' → ' + subdomain);
      return res.json({ ok: true, domain: entry, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
    }
    const existing = await CustomDomain.findOne({ domain: clean }).lean().maxTimeMS(5000);
    if (existing) {
      if (existing.subdomain !== subdomain) return res.status(409).json({ error: 'Domain already registered to another project' });
      return res.json({ ok: true, domain: existing, existing: true });
    }
    const project = await Project.findOne({ subdomain }).lean().maxTimeMS(5000);
    if (!project) return res.status(404).json({ error: 'Project not found for subdomain: ' + subdomain });
    const cd = await new CustomDomain({ domain: clean, subdomain, verified: false }).save();
    console.log('[CustomDomain] Added: ' + clean + ' -> ' + subdomain);
    addActivity('domain', 'Custom domain added: ' + clean + ' → ' + subdomain);
    res.json({ ok: true, domain: cd });
  } catch(e) {
    console.error('[CustomDomain] save error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

function isCloudflareIpv4(ip) {
  const parts = String(ip || '').split('.').map(Number);
  if (parts.length !== 4 || parts.some(n => Number.isNaN(n) || n < 0 || n > 255)) return false;
  const n = (((parts[0] << 24) >>> 0) + (parts[1] << 16) + (parts[2] << 8) + parts[3]) >>> 0;
  const ranges = [
    ['173.245.48.0', 20], ['103.21.244.0', 22], ['103.22.200.0', 22], ['103.31.4.0', 22],
    ['141.101.64.0', 18], ['108.162.192.0', 18], ['190.93.240.0', 20], ['188.114.96.0', 20],
    ['197.234.240.0', 22], ['198.41.128.0', 17], ['162.158.0.0', 15], ['104.16.0.0', 13],
    ['104.24.0.0', 14], ['172.64.0.0', 13], ['131.0.72.0', 22]
  ];
  return ranges.some(([base, bits]) => {
    const b = base.split('.').map(Number);
    const m = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
    const bn = (((b[0] << 24) >>> 0) + (b[1] << 16) + (b[2] << 8) + b[3]) >>> 0;
    return (n & m) === (bn & m);
  });
}

function isCloudflareIpv6(ip) {
  const s = String(ip || '').toLowerCase();
  return s.startsWith('2400:cb00:') || s.startsWith('2606:4700:') || s.startsWith('2803:f800:') ||
         s.startsWith('2405:b500:') || s.startsWith('2405:8100:') || s.startsWith('2a06:98c0:') ||
         s.startsWith('2c0f:f248:');
}

async function resolveDnsRecords(domain) {
  const dns = require('dns');
  const resolvers = [dns.promises];
  const publicResolver = new dns.promises.Resolver();
  publicResolver.setServers(['1.1.1.1', '8.8.8.8']);
  resolvers.push(publicResolver);
  const records = { cname: [], a: [], aaaa: [] };
  for (const resolver of resolvers) {
    for (const [type, fn] of [['cname', 'resolveCname'], ['a', 'resolve4'], ['aaaa', 'resolve6']]) {
      try {
        const vals = await resolver[fn](domain);
        for (const v of vals || []) if (!records[type].includes(v)) records[type].push(v);
      } catch (_) {}
    }
  }
  return records;
}

async function verifyDomainDns(domain) {
  const clean = normalizeHostHeader(domain);
  const expectedTunnel = CF_TUNNEL_ID ? `${CF_TUNNEL_ID}.cfargotunnel.com` : '';
  const expected = [expectedTunnel, BASE_DOMAIN, 'cfargotunnel.com'].filter(Boolean).map(v => v.toLowerCase().replace(/\.$/, ''));
  const records = await resolveDnsRecords(clean);
  const cnames = records.cname.map(v => String(v).toLowerCase().replace(/\.$/, ''));
  const cnameMatch = cnames.some(c => expected.some(t => c === t || c.endsWith('.' + t) || c.includes(t)));
  const proxiedCloudflare = records.a.some(isCloudflareIpv4) || records.aaaa.some(isCloudflareIpv6);
  return {
    verified: cnameMatch || proxiedCloudflare,
    method: cnameMatch ? 'cname' : (proxiedCloudflare ? 'cloudflare-proxy' : 'pending'),
    records,
    expected: expectedTunnel || BASE_DOMAIN,
    reason: cnameMatch
      ? 'CNAME points to the expected DeployBoard target.'
      : (proxiedCloudflare
        ? 'Cloudflare proxy detected. Proxied records hide the CNAME publicly, so this is accepted.'
        : 'No matching CNAME or Cloudflare-proxied DNS record was detected yet.')
  };
}

app.post('/api/domains/:domain/verify', async (req, res) => {
  const domain = normalizeHostHeader(req.params.domain);
  if (!domain) return res.status(400).json({ error: 'Invalid domain' });
  try {
    const result = await verifyDomainDns(domain);
    if (!isMongoReady()) {
      const entry = memDomains.find(d => d.domain === domain);
      if (entry) entry.verified = result.verified;
    } else {
      await CustomDomain.findOneAndUpdate({ domain }, { verified: result.verified }).maxTimeMS(5000);
    }
    res.json({ ok: true, ...result });
  } catch(e) {
    console.error('[CustomDomain] verify error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/domains/:domain', async (req, res) => {
  try {
    await CustomDomain.findOneAndDelete({ domain: req.params.domain });
    console.log('[CustomDomain] Removed: ' + req.params.domain);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Proxy-test endpoint (used by cron job URL tester) ────────────────────────
app.post('/api/proxy-test', async (req, res) => {
  const { url, method = 'GET' } = req.body;
  if (!url || !url.startsWith('http')) return res.status(400).json({ error: 'Invalid URL' });
  try {
    const http  = require('http');
    const https = require('https');
    const lib   = url.startsWith('https') ? https : http;
    const start = Date.now();
    const r = await new Promise((resolve, reject) => {
      const req2 = lib.request(url, { method, timeout: 8000 }, resolve);
      req2.on('error', reject);
      req2.on('timeout', () => { req2.destroy(); reject(new Error('timeout')); });
      req2.end();
    });
    r.resume(); // drain
    res.json({ ok: r.statusCode < 400, status: r.statusCode, duration: Date.now() - start });
  } catch(e) { res.status(200).json({ ok: false, status: null, error: e.message }); }
});

// ── Env Variables API ────────────────────────────────────────────────────────
app.get('/api/projects/:id/env', async (req, res) => {
  try {
    const p = await Project.findById(req.params.id);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    const vars = p.envVars instanceof Map ? Object.fromEntries(p.envVars) : (p.envVars || {});
    res.json(vars);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/projects/:id/env', async (req, res) => {
  try {
    const { key, value } = req.body;
    if (!key) return res.status(400).json({ error: 'key required' });
    const p = await Project.findById(req.params.id);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    if (!p.envVars) p.envVars = {};
    p.envVars[key] = value || '';
    p.markModified('envVars');
    await p.save();
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/projects/:id/env/:key', async (req, res) => {
  try {
    const p = await Project.findById(req.params.id);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    if (p.envVars) { delete p.envVars[req.params.key]; p.markModified('envVars'); await p.save(); }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Activity API (server-side log) ────────────────────────────────────────────
const activityLog = [];
function addActivity(type, message) {
  activityLog.unshift({ type, message, time: new Date().toISOString() });
  if (activityLog.length > 500) activityLog.pop();
}

app.get('/api/activity', async (req, res) => {
  try {
    // Merge in-memory activity with recent deployments from DB
    const feed = [...activityLog];
    if (isMongoReady()) {
      const recent = await Deployment.find().sort({ startedAt: -1 }).limit(30).lean().maxTimeMS(5000);
      recent.forEach(d => {
        const icon = d.status === 'success' ? '✓' : d.status === 'failed' ? '✗' : '⏳';
        feed.push({
          type: 'deploy',
          message: icon + ' ' + (d.projectName || 'Unknown') + ' — ' + d.status + (d.duration ? ' in ' + d.duration + 's' : ''),
          time: d.startedAt || d.createdAt
        });
      });
    }
    feed.sort((a,b) => new Date(b.time) - new Date(a.time));
    res.json(feed.slice(0, 100));
  } catch(e) {
    res.json(activityLog.slice(0, 100));
  }
});

// ── Deploy endpoint ───────────────────────────────────────────────────────────
app.post('/api/deploy', requireAuth, async (req, res) => {
  const { name, subdomain, repoUrl, branch, installCmd, buildCmd,
          startCmd, outputDir, nodeVer, siteType, envVars,
          isDockerfileDeploy, isWorker, dockerfilePath, exposedPort } = req.body;
  const deploySource = (req.body?.source === 'auto' || req.body?.autoDeploy === true || req.headers['x-deployboard-deploy-source'] === 'auto') ? 'auto' : 'manual';
  const triggerSha = String(req.body?.triggerSha || req.headers['x-deployboard-trigger-sha'] || '').trim();

  if (!name || !subdomain || !repoUrl) {
    return res.status(400).json({ error: 'name, subdomain and repoUrl are required' });
  }

  const cleanSub = subdomain.toLowerCase()
    .replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');

  // Respect explicit site type first so static deployments are not forced into
  // server mode when UI/default data still contains a start command.
  const explicitType = String(siteType || '').trim().toLowerCase();
  const hasStartCmd = !!String(startCmd || '').trim();
  const isServerApp = explicitType === 'server' || (!explicitType && hasStartCmd);

  // Assign port for server apps
  let appPort = 0;
  if (isServerApp) {
    try { appPort = getOrAssignPort(cleanSub); }
    catch(e) { return res.status(500).json({ error: e.message }); }
  }

  // Upsert project
  let project;
  try {
    project = await Project.findOneAndUpdate(
      { subdomain: cleanSub },
      { name, subdomain: cleanSub, repoUrl,
        branch:     branch     || 'main',
        installCmd: installCmd || 'npm install',
        buildCmd:   buildCmd   || 'npm run build',
        startCmd:   startCmd   || '',
        outputDir:  outputDir  || 'dist',
        nodeVer:    nodeVer    || '20',
        siteType:   siteType   || 'static',
        appPort,
        envVars:            envVars             || {},
        isDockerfileDeploy: !!isDockerfileDeploy,
        isWorker:           !!isWorker,
        dockerfilePath:     dockerfilePath      || 'Dockerfile',
        exposedPort:        exposedPort         || 3000,
        ownerUserId: String(req.user?._id || req.user?.id || ''),
        updatedAt:  new Date() },
      { upsert: true, new: true }
    );
  } catch(e) {
    project = {
      _id: 'local_' + Date.now(), name, subdomain: cleanSub, repoUrl,
      branch: branch||'main', installCmd: installCmd||'npm install',
      buildCmd: buildCmd||'npm run build', startCmd: startCmd||'',
      outputDir: outputDir||'dist', nodeVer: nodeVer||'20',
      siteType: siteType||'static', appPort, envVars: envVars||{},
      save: async () => {}
    };
  }

  if (deploySource === 'auto') {
    try {
      const activeFilter = { projectId: project._id, status: { $in: ['pending','building'] } };
      if (triggerSha) activeFilter.triggerSha = triggerSha;
      let active = await Deployment.findOne(activeFilter).sort({ startedAt: -1 }).lean().maxTimeMS(3000);
      if (!active && triggerSha) {
        active = await Deployment.findOne({ projectId: project._id, status: { $in: ['pending','building'] } }).sort({ startedAt: -1 }).lean().maxTimeMS(3000);
      }
      if (active) {
        emitAutoDeployStatus(project._id, 'deploying', {
          branch: branch || project.branch || 'main',
          sha: triggerSha,
          deployId: String(active._id),
          subdomain: cleanSub,
          projectName: name
        });
        return res.json({
          ok: true,
          deployId: String(active._id),
          projectId: String(project._id),
          message: 'Automatic deployment already running for this push',
          deduped: true,
          source: deploySource,
          liveUrl: `https://${cleanSub}.${BASE_DOMAIN}`
        });
      }
    } catch (_) {}
  }

  // If redeploying as static, clean up old container
  if (!isServerApp) {
    try {
      const { execSync } = require('child_process');
      execSync(`docker rm -f db-${cleanSub}`, { stdio: 'pipe' });
    } catch(e) {}
    if (portRegistry[cleanSub]) {
      delete portRegistry[cleanSub];
      savePortRegistry();
    }
  }

  let deployment;
  try {
    deployment = await new Deployment({
      projectId: project._id, projectName: name,
      branch: branch||'main', status: 'pending', source: deploySource, triggerSha
    }).save();
  } catch(e) {
    deployment = {
      _id: 'local_' + Date.now(), projectId: project._id,
      projectName: name, branch: branch||'main',
      status: 'pending', source: deploySource, triggerSha, logs: [], startedAt: new Date(),
      save: async () => {}
    };
  }

  const deployId = deployment._id.toString();
  if (deploySource === 'auto') {
    emitAutoDeployStatus(project._id, 'deploying', {
      branch: branch || 'main',
      sha: triggerSha,
      deployId,
      subdomain: cleanSub,
      projectName: name
    });
  }
  res.json({ ok: true, deployId, projectId: String(project._id), message: 'Build started',
             source: deploySource, triggerSha, liveUrl: `https://${cleanSub}.${BASE_DOMAIN}` });

  // Async build
  const buildStart = Date.now();
  deployment.status = 'building';
  try { await deployment.save(); } catch(e) {}

  const emit = (event, data) => io.emit(event, { deployId, projectId: String(project._id), source: deploySource, triggerSha, ...data });

  try {
    emit('build:log', { line: `\x1b[36m[DeployBoard]\x1b[0m Building \x1b[1m${name}\x1b[0m` });
    emit('build:log', { line: `\x1b[90mRepo: ${repoUrl}  Branch: ${branch||'main'}\x1b[0m` });
    emit('build:log', { line: `\x1b[90mTarget: https://${cleanSub}.${BASE_DOMAIN}\x1b[0m` });
    emit('build:log', { line: '' });

    const deployGithubToken = req.user?.githubAccessToken || '';
    if (/github\.com/i.test(repoUrl) && !deployGithubToken) {
      throw new Error('GitHub account not connected for this user. Connect GitHub in your account to deploy private repositories.');
    }
    let saveTimer = null;
    const flushLogs = async () => {
      if (!deployment || !deployment.save) return;
      try { await deployment.save(); } catch (_) {}
    };
    await runBuild({
      deployId, project, sitesDir: SITES_DIR, tmpDir: TMP_DIR,
      githubToken: deployGithubToken, appPort, emit,
      isDockerfileDeploy: !!isDockerfileDeploy,
      isWorker:           !!isWorker,
      onLog: (line) => {
        deployment.logs = deployment.logs || [];
        deployment.logs.push(line);
        if (!saveTimer) {
          saveTimer = setTimeout(async () => { saveTimer = null; await flushLogs(); }, 1500);
        }
      }
    });
    if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
    await flushLogs();

    // Register CF subdomain
    emit('build:log', { line: `\x1b[36m[DeployBoard]\x1b[0m Registering subdomain…` });
    const cf = await registerSubdomain(cleanSub);
    if (cf.ok) {
      emit('build:log', { line: `\x1b[32m[CF]\x1b[0m Live at: \x1b[1m${cf.url}\x1b[0m` });
      try { await Project.findByIdAndUpdate(project._id, { liveUrl: cf.url }); } catch(e) {}
    }
    if (!isServerApp) {
      const live = cf?.url || `https://${cleanSub}.${BASE_DOMAIN}`;
      emit('build:log', { line: `\x1b[90m[static] OAuth note: add ${live} callback URL(s) in GitHub/Google OAuth settings.\x1b[0m` });
      emit('build:log', { line: `\x1b[90m[static] Turnstile note: add ${cleanSub}.${BASE_DOMAIN} to your widget domain allowlist.\x1b[0m` });
    }

    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'success'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    if (deploySource === 'auto') {
      try {
        await Project.findByIdAndUpdate(project._id, {
          autoDeployStatus: 'watching',
          autoDeployLastCompletedAt: deployment.endedAt,
          autoDeployLastError: '',
          ...(triggerSha ? { autoDeployLastSha: triggerSha } : {})
        });
      } catch (_) {}
      emitAutoDeployStatus(project._id, 'watching', { branch: branch || 'main', sha: triggerSha, completed: true, result: 'success' });
    }
    addActivity('deploy', (deploySource === 'auto' ? '✓ Automatic deployment succeeded: ' : '✓ Deployment succeeded: ') + name + ' in ' + duration + 's');
    emit('build:log', { line: `\n\x1b[32m✓ Deployed in ${duration}s\x1b[0m` });
    emit('build:done', { status: 'success', duration, liveUrl: cf?.url || null });

  } catch(buildErr) {
    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'failed'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    if (deploySource === 'auto') {
      const safeAutoErr = sanitizeSecrets(buildErr.message).slice(0, 240);
      try {
        await Project.findByIdAndUpdate(project._id, {
          autoDeployStatus: 'error',
          autoDeployLastCompletedAt: deployment.endedAt,
          autoDeployLastError: safeAutoErr,
          ...(triggerSha ? { autoDeployLastSha: triggerSha } : {})
        });
      } catch (_) {}
      emitAutoDeployStatus(project._id, 'error', { branch: branch || 'main', sha: triggerSha, completed: true, result: 'failed', error: safeAutoErr });
    }
    addActivity('deploy', (deploySource === 'auto' ? '✗ Automatic deployment failed: ' : '✗ Deployment failed: ') + name + ' — ' + buildErr.message.slice(0,80));
    const buildDir = path.join(TMP_DIR, deployId);
    try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
    const safeErr = sanitizeSecrets(buildErr.message);
    emit('build:log', { line: `\x1b[31m[DeployBoard]\x1b[0m Build failed: ${safeErr}` });
    emit('build:done', { status: 'failed', duration });
    console.error(`[Deploy] FAILED ${name}:`, sanitizeSecrets(buildErr.message));
  }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log('[Socket.io] Connected:', socket.id);
  socket.on('disconnect', () => console.log('[Socket.io] Disconnected:', socket.id));
});




async function findProjectByAnyId(id) {
  const sid = String(id || '').trim();
  if (!sid) return null;
  if (mongoose.Types.ObjectId.isValid(sid)) {
    const byOid = await Project.findById(sid);
    if (byOid) return byOid;
  }
  return await Project.findOne({ $or: [{ subdomain: sid }, { name: sid }] });
}

app.post('/api/projects/:id/autodeploy', requireAuth, async (req, res) => {
  try {
    const id = String(req.params.id || '');
    const enabled = !!req.body.enabled;
    if (!isDbReady()) {
      memAutoDeploy.set(id, { enabled: !!enabled, mode: 'polling-memory', updatedAt: Date.now() });
      return res.json({
        ok: true,
        enabled: !!enabled,
        mode: 'polling-memory',
        pollIntervalMs: AUTO_DEPLOY_POLL_INTERVAL_MS,
        note: 'Database is offline, so this auto-deploy setting is only stored in memory for this server process.'
      });
    }
    const project = await findProjectByAnyId(id);
    if (!project) return res.status(404).json({ error: 'Project not found' });
    const requestOwnerId = String(req.user._id || req.user.id || '');
    if (project.ownerUserId && String(project.ownerUserId) !== requestOwnerId) return res.status(403).json({ error: 'Forbidden' });
    if (!project.ownerUserId) project.ownerUserId = requestOwnerId;

    project.autoDeployEnabled = enabled;
    project.autoDeployMode = 'polling';
    project.autoDeployStatus = enabled ? 'watching' : 'idle';
    project.autoDeployLastError = '';
    project.autoDeployLastCheckedAt = new Date();

    let headSha = '';
    let baselineWarning = '';
    if (enabled) {
      try {
        headSha = await getProjectHeadSha(project, req.user.githubAccessToken);
        project.autoDeployLastSha = headSha || project.autoDeployLastSha || '';
      } catch (e) {
        baselineWarning = e.message || 'Could not read the current GitHub commit yet';
        project.autoDeployLastError = baselineWarning;
        project.autoDeployStatus = 'watching-with-warning';
      }
    }

    await project.save();
    res.json({
      ok: true,
      enabled: project.autoDeployEnabled,
      mode: 'polling',
      pollIntervalMs: AUTO_DEPLOY_POLL_INTERVAL_MS,
      lastSha: project.autoDeployLastSha || headSha || '',
      warning: baselineWarning,
      note: enabled
        ? 'DeployBoard will poll GitHub with this user OAuth token and trigger a deploy when the configured branch SHA changes. No repository webhook is required.'
        : 'Polling auto-deploy disabled for this project.'
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});



app.post('/api/projects/:id/autodeploy/check', requireAuth, async (req, res) => {
  try {
    if (!isDbReady()) {
      const project = normalizeWorkspaceAutoDeployProject(req.body?.project || {}, req.params.id);
      if (!project) return res.status(400).json({ error: 'Project details are required while MongoDB is offline. Refresh the page and try again.' });
      if (!project.autoDeployEnabled) return res.status(400).json({ error: 'Auto deploy is not enabled for this project' });
      const result = await checkWorkspaceProjectAutoDeploy(req.user, project, {
        manual: true,
        authorization: req.headers.authorization || ''
      });
      return res.json({ ok: true, ...result, project });
    }

    const project = await findProjectByAnyId(String(req.params.id || ''));
    if (!project) return res.status(404).json({ error: 'Project not found' });
    const requestOwnerId = String(req.user._id || req.user.id || '');
    if (project.ownerUserId && String(project.ownerUserId) !== requestOwnerId) return res.status(403).json({ error: 'Forbidden' });
    if (!project.ownerUserId) {
      project.ownerUserId = requestOwnerId;
      await project.save();
    }
    if (!project.autoDeployEnabled) return res.status(400).json({ error: 'Auto deploy is not enabled for this project' });
    const result = await checkProjectAutoDeploy(project, { manual: true });
    const latest = await Project.findById(project._id).lean().catch(() => null);
    res.json({ ok: true, ...result, project: latest || null });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.post('/api/github/webhook', async (req, res) => {
  try {
    if (!verifyGitHubWebhookSecret(req, GLOBAL_WEBHOOK_SECRET)) return res.status(401).json({ error: 'Invalid webhook secret' });
    const event = String(req.headers['x-github-event'] || '');
    if (event && event !== 'push') return res.json({ ok:true, ignored:true, reason:'event_not_push' });
    const repoFull = String(req.body?.repository?.full_name || '').toLowerCase();
    const ref = String(req.body?.ref || '');
    const branch = ref.startsWith('refs/heads/') ? ref.slice('refs/heads/'.length) : '';
    if (!repoFull) return res.status(400).json({ error: 'Missing repository.full_name' });
    const all = isDbReady() ? await Project.find({ autoDeployEnabled: true }).lean().maxTimeMS(6000).catch(()=>[]) : [];
    let matched = 0;
    let queued = 0;
    const skipped = [];
    for (const p of all) {
      const parsed = parseGitHubRepo(p.repoUrl || '');
      if (!parsed) continue;
      const full = `${parsed.owner}/${parsed.repo}`.toLowerCase();
      if (full !== repoFull) continue;
      matched++;
      const targetBranch = String(p.branch || 'main');
      if (branch && targetBranch !== branch) {
        skipped.push({ project: p.name, reason: 'branch_mismatch', expected: targetBranch, got: branch });
        continue;
      }
      const webhookSha = String(req.body?.after || req.body?.head_commit?.id || '').trim();
      try {
        const result = await queueProjectAutoDeployFromWebhook(p, branch || targetBranch, webhookSha);
        if (result.queued) queued++;
        else skipped.push({ project: p.name, reason: result.reason || 'not_queued' });
      } catch (e) {
        skipped.push({ project: p.name, reason: String(e?.message || 'deploy_trigger_failed') });
      }
    }
    res.json({ ok:true, matched, queued, skipped, repo: repoFull, branch: branch || null });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/github/webhook/:projectId', async (req, res) => {
  try {
    const project = await Project.findById(String(req.params.projectId || ''));
    if (!project || !project.autoDeployEnabled || !project.autoDeploySecret) return res.status(404).json({ error: 'Webhook not configured' });
    if (!verifyGitHubWebhookSecret(req, project.autoDeploySecret)) return res.status(401).json({ error: 'Invalid webhook secret' });
    const event = String(req.headers['x-github-event'] || '');
    if (event && event !== 'push') return res.json({ ok:true, ignored:true, reason:'event_not_push' });
    const ref = String(req.body?.ref || '');
    const branch = ref.startsWith('refs/heads/') ? ref.slice('refs/heads/'.length) : (project.branch || 'main');
    if (branch && String(project.branch || 'main') !== branch) return res.json({ ok:true, ignored:true, reason:'branch_mismatch', branch });
    const sha = String(req.body?.after || req.body?.head_commit?.id || '').trim();
    const result = await queueProjectAutoDeployFromWebhook(project, branch, sha);
    res.json({ ok:true, ...result, branch });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/api/webhook/global-secret', requireAuth, async (req, res) => {
  res.json({
    ok:true,
    secret: GLOBAL_WEBHOOK_SECRET || '',
    webhookUrl: `${getPublicOrigin(req)}/api/github/webhook`,
    envConfigured: !!(process.env.GLOBAL_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET),
    note: 'Use this one GitHub webhook secret for every repository webhook. DeployBoard matches the pushed repository/branch to all enabled projects.'
  });
});

app.post('/api/webhook/global-secret/regenerate', requireAuth, async (req, res) => {
  GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
  res.json({ ok:true, secret: GLOBAL_WEBHOOK_SECRET, webhookUrl: `${getPublicOrigin(req)}/api/github/webhook`, envConfigured: false, note: 'Runtime value updated. Persist in .env as GLOBAL_WEBHOOK_SECRET after restart.' });
});

// ── Dashboard static serving — MUST be after all API routes ──────────────────
app.use(express.static(path.join(__dirname)));
app.get('*', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ── Start ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[DeployBoard] Running on http://localhost:${PORT}`);
  console.log(`[DeployBoard] Base domain: ${BASE_DOMAIN}`);
  console.log(`[DeployBoard] Sites dir:   ${SITES_DIR}`);
});

function sanitizeSecrets(text = '') {
  return String(text)
    .replace(/github_pat_[A-Za-z0-9_]+/g, 'github_pat_[REDACTED]')
    .replace(/ghp_[A-Za-z0-9]+/g, 'ghp_[REDACTED]')
    .replace(/x-access-token:[^@\s]+@github\.com/gi, 'x-access-token:[REDACTED]@github.com')
    .replace(/AUTHORIZATION:\s*bearer\s+[^\s"']+/gi, 'AUTHORIZATION: bearer [REDACTED]');
}

function parseGitHubRepo(repoUrl = '') {
  const m = String(repoUrl).match(/github\.com\/([^/]+)\/([^/#?]+)/i);
  if (!m) return null;
  return { owner: m[1], repo: m[2].replace(/\.git$/i, '') };
}

function getPublicOrigin(req) {
  const host = String(req.headers['x-forwarded-host'] || req.get('host') || '').trim();
  const xfProtoRaw = String(req.headers['x-forwarded-proto'] || '').trim();
  const xfProto = xfProtoRaw.split(',')[0].trim().toLowerCase();
  const hostLooksPublic = host && !/^localhost(?::\d+)?$/i.test(host) && !/^127(?:\.\d{1,3}){3}(?::\d+)?$/.test(host);
  const proto = xfProto || (req.secure ? 'https' : (hostLooksPublic ? 'https' : 'http'));
  return `${proto}://${host}`;
}


function timingSafeEqualString(a = '', b = '') {
  const left = Buffer.from(String(a));
  const right = Buffer.from(String(b));
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function verifyGitHubWebhookSecret(req, secret) {
  const expectedSecret = String(secret || '').trim();
  if (!expectedSecret) return false;

  // Legacy/manual mode: useful for curl tests or older DeployBoard webhook docs.
  const provided = String(req.headers['x-deployboard-secret'] || req.query.secret || '').trim();
  if (provided && timingSafeEqualString(provided, expectedSecret)) return true;

  // GitHub's real webhook "Secret" field is not sent as a plain header. GitHub
  // signs the raw request body and sends the HMAC in X-Hub-Signature-256.
  const signature = String(req.headers['x-hub-signature-256'] || '').trim();
  if (!signature.startsWith('sha256=')) return false;
  const rawBody = req.rawBody && req.rawBody.length ? req.rawBody : Buffer.from(JSON.stringify(req.body || {}));
  const digest = 'sha256=' + crypto.createHmac('sha256', expectedSecret).update(rawBody).digest('hex');
  return timingSafeEqualString(signature, digest);
}



async function getProjectHeadSha(project, githubToken) {
  const parsed = parseGitHubRepo(project.repoUrl || '');
  if (!parsed) throw new Error('project_repo_is_not_github');
  const token = String(githubToken || '').trim();
  if (!token) throw new Error('missing_github_oauth_token');
  const branch = String(project.branch || 'main');
  const r = await fetch(`https://api.github.com/repos/${parsed.owner}/${parsed.repo}/commits/${encodeURIComponent(branch)}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'User-Agent': 'DeployBoard AutoDeploy',
      'Accept': 'application/vnd.github+json'
    }
  });
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d?.message || `GitHub commit lookup failed (${r.status})`);
  if (!d?.sha) throw new Error('GitHub commit lookup did not return a SHA');
  return d.sha;
}


async function queueProjectAutoDeployFromWebhook(projectLike, branch, sha = '') {
  if (!isDbReady()) return { queued: false, reason: 'database_offline' };
  const id = String(projectLike?._id || projectLike?.id || '');
  if (!id) return { queued: false, reason: 'missing_project_id' };
  const triggerSha = String(sha || '').trim();
  const projectId = mongoose.Types.ObjectId.isValid(id) ? id : null;
  if (!projectId) return { queued: false, reason: 'invalid_project_id' };

  const active = await Deployment.findOne({ projectId, status: { $in: ['pending','building'] } })
    .lean().maxTimeMS(3000).catch(() => null);
  if (active) {
    await Project.updateOne({ _id: projectId }, {
      $set: { autoDeployStatus: 'waiting-for-active-build', autoDeployLastCheckedAt: new Date(), autoDeployLastCompletedAt: null, autoDeployLastError: '' }
    }).catch(() => {});
    emitAutoDeployStatus(projectId, 'deploying', { branch, sha: triggerSha, deployId: String(active._id), subdomain: projectLike.subdomain || '', projectName: projectLike.name || '' });
    return { queued: false, reason: 'active_deployment_running', deployId: String(active._id) };
  }

  const filter = { _id: projectId, autoDeployEnabled: true };
  if (triggerSha) filter.autoDeployLastSha = { $ne: triggerSha };
  const locked = await Project.findOneAndUpdate(filter, {
    $set: {
      autoDeployLastSha: triggerSha || String(projectLike.autoDeployLastSha || ''),
      autoDeployStatus: 'deploying',
      autoDeployLastCheckedAt: new Date(),
      autoDeployLastTriggeredAt: new Date(),
      autoDeployLastCompletedAt: null,
      autoDeployLastError: ''
    }
  }, { new: true }).maxTimeMS(5000).catch(() => null);

  if (!locked) return { queued: false, reason: triggerSha ? 'sha_already_handled' : 'not_enabled' };
  autoDeployState.set(String(projectId), { sha: triggerSha, pendingSha: triggerSha, deploying: true });
  emitAutoDeployStatus(projectId, 'deploying', { branch, sha: triggerSha, subdomain: locked.subdomain || projectLike.subdomain || '', projectName: locked.name || projectLike.name || '' });
  addActivity('deploy', `↻ GitHub webhook queued ${locked.name} (${branch || locked.branch || 'main'}) after push${triggerSha ? ' ' + triggerSha.slice(0, 7) : ''}`);

  try {
    const deploy = await triggerProjectDeploy(locked, branch || locked.branch || 'main', { source: 'auto', sha: triggerSha });
    autoDeployState.set(String(projectId), { sha: triggerSha, deploying: false });
    await Project.updateOne({ _id: projectId }, { $set: { autoDeployStatus: 'deploying', autoDeployLastCompletedAt: null, autoDeployLastError: '' } }).catch(() => {});
    emitAutoDeployStatus(projectId, 'deploying', { branch, sha: triggerSha, deployId: deploy?.deployId || '', subdomain: locked.subdomain || projectLike.subdomain || '', projectName: locked.name || projectLike.name || '' });
    return { queued: true, deployId: deploy?.deployId || '', sha: triggerSha };
  } catch (e) {
    const message = String(e?.message || 'deploy_trigger_failed').slice(0, 240);
    autoDeployState.set(String(projectId), { sha: triggerSha, deploying: false });
    await Project.updateOne({ _id: projectId }, { $set: { autoDeployStatus: 'error', autoDeployLastError: message, autoDeployLastCheckedAt: new Date() } }).catch(() => {});
    emitAutoDeployStatus(projectId, 'error', { branch, sha: triggerSha, error: message });
    return { queued: false, reason: 'deploy_trigger_failed', error: message, sha: triggerSha };
  }
}

async function triggerProjectDeploy(p, branchOverride = null, options = {}) {
  const ownerId = String(p.ownerUserId || '');
  if (!ownerId) throw new Error('missing_owner_user');
  const owner = isDbReady()
    ? await User.findById(ownerId).catch(()=>null)
    : localAuth.users.find(u => String(u.id) === ownerId || String(u._id || '') === ownerId);
  if (!owner) throw new Error('owner_not_found');
  const payload = {
    name: p.name, subdomain: p.subdomain, repoUrl: p.repoUrl, branch: branchOverride || p.branch || 'main',
    installCmd: p.installCmd || 'npm install', buildCmd: p.buildCmd || 'npm run build',
    startCmd: p.startCmd || '', outputDir: p.outputDir || 'dist', nodeVer: p.nodeVer || '20',
    siteType: p.siteType || 'static', envVars: p.envVars || {}, isDockerfileDeploy: !!p.isDockerfileDeploy,
    isWorker: !!p.isWorker, dockerfilePath: p.dockerfilePath || 'Dockerfile', exposedPort: p.exposedPort || 3000,
    source: options.source || 'auto', autoDeploy: true, triggerSha: String(options.sha || '')
  };
  const deployResp = await fetch(`http://127.0.0.1:${PORT}/api/deploy`, {
    method:'POST',
    headers:{
      'Content-Type':'application/json',
      'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY,
      'x-deployboard-owner-id': String(owner._id || ownerId),
      'x-deployboard-deploy-source': options.source || 'auto',
      'x-deployboard-trigger-sha': String(options.sha || '')
    },
    body: JSON.stringify(payload)
  }).catch(()=>null);
  if (!deployResp || !deployResp.ok) {
    const detail = deployResp ? await deployResp.text().catch(()=> '') : 'no_response';
    throw new Error('deploy_trigger_failed:' + (detail || deployResp?.status || 'unknown'));
  }
  return deployResp.json().catch(() => ({ ok: true }));
}


function normalizeWorkspaceAutoDeployProject(project, fallbackId = '') {
  const id = String(project?.id || project?._id || fallbackId || '').trim();
  const repoUrl = String(project?.repoUrl || '').trim();
  const subdomain = String(project?.subdomain || '').trim();
  const name = String(project?.name || subdomain || 'Project').trim();
  if (!id || !repoUrl || !subdomain) return null;
  return {
    ...project,
    id,
    _id: project?._id || id,
    name,
    subdomain,
    repoUrl,
    branch: project?.branch || 'main',
    installCmd: project?.installCmd || 'npm install',
    buildCmd: project?.buildCmd || 'npm run build',
    startCmd: project?.startCmd || '',
    outputDir: project?.outputDir || 'dist',
    nodeVer: project?.nodeVer || '20',
    siteType: project?.siteType || 'static',
    envVars: project?.envVars || {},
    isDockerfileDeploy: !!project?.isDockerfileDeploy,
    isWorker: !!project?.isWorker,
    dockerfilePath: project?.dockerfilePath || 'Dockerfile',
    exposedPort: project?.exposedPort || 3000,
    autoDeployEnabled: !!project?.autoDeployEnabled,
    autoDeployLastSha: String(project?.autoDeployLastSha || ''),
    autoDeployLastError: String(project?.autoDeployLastError || ''),
    autoDeployStatus: project?.autoDeployStatus || 'watching'
  };
}

function updateLocalWorkspaceProject(user, projectId, patch = {}) {
  if (!user || !projectId) return;
  user.workspace = user.workspace || {};
  user.workspace.projects = Array.isArray(user.workspace.projects) ? user.workspace.projects : [];
  const p = user.workspace.projects.find(x => String(x.id || x._id || '') === String(projectId) || (patch.subdomain && x.subdomain === patch.subdomain));
  if (p) Object.assign(p, patch);
  saveLocalAuth();
}

async function triggerWorkspaceProjectDeploy(user, project, authorization = '', options = {}) {
  const payload = {
    name: project.name, subdomain: project.subdomain, repoUrl: project.repoUrl, branch: project.branch || 'main',
    installCmd: project.installCmd || 'npm install', buildCmd: project.buildCmd || 'npm run build',
    startCmd: project.startCmd || '', outputDir: project.outputDir || 'dist', nodeVer: project.nodeVer || '20',
    siteType: project.siteType || 'static', envVars: project.envVars || {}, isDockerfileDeploy: !!project.isDockerfileDeploy,
    isWorker: !!project.isWorker, dockerfilePath: project.dockerfilePath || 'Dockerfile', exposedPort: project.exposedPort || 3000,
    source: options.source || 'auto', autoDeploy: true, triggerSha: String(options.sha || '')
  };
  const headers = { 'Content-Type': 'application/json' };
  if (authorization) headers.Authorization = authorization;
  else {
    headers['x-deployboard-internal-key'] = INTERNAL_DEPLOY_KEY;
    headers['x-deployboard-owner-id'] = String(user._id || user.id || '');
    headers['x-deployboard-deploy-source'] = options.source || 'auto';
    headers['x-deployboard-trigger-sha'] = String(options.sha || '');
  }
  const deployResp = await fetch(`http://127.0.0.1:${PORT}/api/deploy`, {
    method: 'POST', headers, body: JSON.stringify(payload)
  }).catch(() => null);
  if (!deployResp || !deployResp.ok) {
    const detail = deployResp ? await deployResp.text().catch(()=> '') : 'no_response';
    throw new Error('deploy_trigger_failed:' + (detail || deployResp?.status || 'unknown'));
  }
  return deployResp.json().catch(() => ({}));
}

async function checkWorkspaceProjectAutoDeploy(user, project, { manual = false, authorization = '' } = {}) {
  const id = String(project.id || project._id || project.subdomain || '');
  const state = autoDeployState.get(id) || {};
  if (state.deploying) return { checked: false, triggered: false, reason: 'already_deploying' };
  const branch = project.branch || 'main';
  const nowIso = new Date().toISOString();
  try {
    updateLocalWorkspaceProject(user, id, { subdomain: project.subdomain, autoDeployStatus: 'checking', autoDeployLastCheckedAt: nowIso, autoDeployLastError: '' });
    emitAutoDeployStatus(id, 'checking', { branch });
    const token = String(user?.githubAccessToken || '').trim();
    if (!token) throw new Error('missing_github_oauth_token');
    const sha = await getProjectHeadSha(project, token);
    const previousSha = String(project.autoDeployLastSha || '');

    if (!previousSha) {
      Object.assign(project, { autoDeployLastSha: sha, autoDeployStatus: 'watching', autoDeployLastCheckedAt: nowIso, autoDeployLastError: '' });
      updateLocalWorkspaceProject(user, id, project);
      autoDeployState.set(id, { sha, deploying: false });
      emitAutoDeployStatus(id, 'watching', { branch, sha });
      return { checked: true, triggered: false, changed: false, reason: 'baseline_created', previousSha: '', sha, branch };
    }

    if (previousSha === sha) {
      Object.assign(project, { autoDeployStatus: 'watching', autoDeployLastCheckedAt: nowIso, autoDeployLastError: '' });
      updateLocalWorkspaceProject(user, id, project);
      autoDeployState.set(id, { sha, deploying: false });
      emitAutoDeployStatus(id, 'watching', { branch, sha });
      return { checked: true, triggered: false, changed: false, reason: 'no_change', previousSha, sha, branch };
    }

    // Lock this SHA immediately so polling does not retrigger multiple deploys
    // for the same single GitHub push while the current deploy is still running.
    autoDeployState.set(id, { sha, pendingSha: sha, deploying: true });
    Object.assign(project, {
      autoDeployLastSha: sha,
      autoDeployStatus: 'deploying',
      autoDeployLastCheckedAt: nowIso,
      autoDeployLastCompletedAt: null,
      autoDeployLastError: ''
    });
    updateLocalWorkspaceProject(user, id, project);
    emitAutoDeployStatus(id, 'deploying', { branch, previousSha, sha });
    addActivity('deploy', `↻ OAuth workspace polling queued ${project.name} (${branch}) after GitHub SHA changed${manual ? ' (manual check)' : ''}`);

    try {
      await triggerWorkspaceProjectDeploy(user, project, authorization, { source: 'auto', sha });
      const patch = { ...project, autoDeployLastSha: sha, autoDeployLastTriggeredAt: nowIso, autoDeployStatus: 'deploying', autoDeployLastCompletedAt: '', autoDeployLastError: '' };
      Object.assign(project, patch);
      updateLocalWorkspaceProject(user, id, patch);
      autoDeployState.set(id, { sha, deploying: false });
      emitAutoDeployStatus(id, 'deploying', { branch, sha, subdomain: project.subdomain || '', projectName: project.name || '' });
      return { checked: true, triggered: true, changed: true, reason: 'deploy_queued', previousSha, sha, branch };
    } catch (e) {
      const message = String(e?.message || 'deploy_trigger_failed').slice(0, 240);
      Object.assign(project, {
        autoDeployLastSha: previousSha,
        autoDeployStatus: 'error',
        autoDeployLastError: message,
        autoDeployLastCheckedAt: new Date().toISOString()
      });
      updateLocalWorkspaceProject(user, id, project);
      autoDeployState.set(id, { sha: previousSha, deploying: false });
      emitAutoDeployStatus(id, 'error', { branch, error: message });
      return { checked: true, triggered: false, changed: true, reason: 'deploy_trigger_failed', error: message, previousSha, sha, branch };
    }
  } catch (e) {
    const message = String(e?.message || 'auto_deploy_check_failed').slice(0, 240);
    Object.assign(project, {
      autoDeployStatus: 'error',
      autoDeployLastCheckedAt: new Date().toISOString(),
      autoDeployLastError: message
    });
    updateLocalWorkspaceProject(user, id, project);
    autoDeployState.set(id, { sha: String(project.autoDeployLastSha || ''), deploying: false });
    emitAutoDeployStatus(id, 'error', { branch, error: message });
    return { checked: false, triggered: false, reason: 'check_failed', error: message, branch };
  }
}

const memAutoDeploy = new Map();
const autoDeployState = new Map();
function emitAutoDeployStatus(projectId, status, extra = {}) {
  try {
    io.emit('autodeploy:status', {
      projectId: String(projectId || ''),
      status: String(status || ''),
      at: new Date().toISOString(),
      ...extra
    });
  } catch (_) {}
}

async function checkProjectAutoDeploy(projectLike, { manual = false } = {}) {
  const id = String(projectLike?._id || projectLike?.id || projectLike || '');
  if (!id) throw new Error('missing_project_id');
  const state = autoDeployState.get(id) || {};
  if (state.deploying) return { checked: false, triggered: false, reason: 'already_deploying' };

  const project = projectLike?.save ? projectLike : await Project.findById(id);
  if (!project || !project.autoDeployEnabled) return { checked: false, triggered: false, reason: 'not_enabled' };
  const branch = project.branch || 'main';
  const now = new Date();

  try {
    await Project.updateOne({ _id: project._id }, { $set: { autoDeployStatus: 'checking', autoDeployLastCheckedAt: now, autoDeployLastError: '' } }).catch(()=>{});
    emitAutoDeployStatus(project._id, 'checking', { branch, subdomain: project.subdomain || '', projectName: project.name || '' });
    const owner = await User.findById(project.ownerUserId).lean().catch(()=>null);
    const token = String(owner?.githubAccessToken || '').trim();
    if (!token) throw new Error('missing_github_oauth_token');

    const sha = await getProjectHeadSha(project, token);
    const previousSha = String(project.autoDeployLastSha || '');

    if (!previousSha) {
      autoDeployState.set(id, { sha, deploying: false });
      await Project.updateOne({ _id: project._id }, { $set: { autoDeployLastSha: sha, autoDeployStatus: 'watching', autoDeployLastCheckedAt: now, autoDeployLastError: '' } }).catch(()=>{});
      emitAutoDeployStatus(project._id, 'watching', { branch, sha, subdomain: project.subdomain || '', projectName: project.name || '' });
      return { checked: true, triggered: false, changed: false, reason: 'baseline_created', previousSha: '', sha, branch };
    }

    const active = await Deployment.findOne({ projectId: project._id, status: { $in: ['pending','building'] } }).lean().maxTimeMS(3000).catch(()=>null);
    if (active) {
      const activeStatus = previousSha === sha ? 'deploying' : 'waiting-for-active-build';
      await Project.updateOne({ _id: project._id }, { $set: { autoDeployStatus: activeStatus, autoDeployLastCheckedAt: now, autoDeployLastError: '' } }).catch(()=>{});
      emitAutoDeployStatus(project._id, activeStatus, { branch, sha, deployId: String(active._id), subdomain: project.subdomain || '', projectName: project.name || '' });
      return { checked: true, triggered: false, changed: previousSha !== sha, reason: 'active_deployment_running', previousSha, sha, branch };
    }

    if (previousSha === sha) {
      autoDeployState.set(id, { sha, deploying: false });
      await Project.updateOne({ _id: project._id }, { $set: { autoDeployStatus: 'watching', autoDeployLastCheckedAt: now, autoDeployLastError: '' } }).catch(()=>{});
      emitAutoDeployStatus(project._id, 'watching', { branch, sha, subdomain: project.subdomain || '', projectName: project.name || '' });
      return { checked: true, triggered: false, changed: false, reason: 'no_change', previousSha, sha, branch };
    }

    // Lock this SHA immediately to avoid duplicate redeploy triggers from one push.
    autoDeployState.set(id, { sha, pendingSha: sha, deploying: true });
    await Project.updateOne(
      { _id: project._id },
      { $set: { autoDeployLastSha: sha, autoDeployStatus: 'deploying', autoDeployLastCheckedAt: now, autoDeployLastCompletedAt: null, autoDeployLastError: '' } }
    ).catch(()=>{});
    emitAutoDeployStatus(project._id, 'deploying', { branch, previousSha, sha, subdomain: project.subdomain || '', projectName: project.name || '' });
    addActivity('deploy', `↻ OAuth polling pipeline queued ${project.name} (${branch}) after GitHub SHA changed${manual ? ' (manual check)' : ''}`);

    try {
      await triggerProjectDeploy(project, branch, { source: 'auto', sha });
      await Project.updateOne({ _id: project._id }, { $set: { autoDeployLastSha: sha, autoDeployLastTriggeredAt: now, autoDeployStatus: 'deploying', autoDeployLastCompletedAt: null, autoDeployLastError: '' } }).catch(()=>{});
      emitAutoDeployStatus(project._id, 'deploying', { branch, sha, subdomain: project.subdomain || '', projectName: project.name || '' });
      autoDeployState.set(id, { sha, deploying: false });
      return { checked: true, triggered: true, changed: true, reason: 'deploy_queued', previousSha, sha, branch };
    } catch (e) {
      const message = String(e?.message || 'deploy_trigger_failed').slice(0, 240);
      await Project.updateOne({ _id: project._id }, { $set: { autoDeployStatus: 'error', autoDeployLastError: message, autoDeployLastCheckedAt: new Date() } }).catch(()=>{});
      emitAutoDeployStatus(project._id, 'error', { branch, error: message });
      autoDeployState.set(id, { sha: previousSha, deploying: false });
      return { checked: true, triggered: false, changed: true, reason: 'deploy_trigger_failed', error: message, previousSha, sha, branch };
    }
  } catch (e) {
    const message = String(e?.message || 'auto_deploy_check_failed').slice(0, 240);
    await Project.updateOne({ _id: project._id }, { $set: { autoDeployStatus: 'error', autoDeployLastCheckedAt: new Date(), autoDeployLastError: message } }).catch(()=>{});
    emitAutoDeployStatus(project._id, 'error', { branch, error: message });
    autoDeployState.set(id, { sha: String(project.autoDeployLastSha || ''), deploying: false });
    return { checked: false, triggered: false, reason: 'check_failed', error: message, branch };
  }
}

let autoDeployPollRunning = false;
async function checkAndAutoDeployProjects() {
  if (autoDeployPollRunning) return;
  autoDeployPollRunning = true;
  try {
  if (!isDbReady()) {
    for (const user of localAuth.users || []) {
      const workspaceProjects = Array.isArray(user?.workspace?.projects) ? user.workspace.projects : [];
      for (const raw of workspaceProjects) {
        const project = normalizeWorkspaceAutoDeployProject(raw, raw?.id || raw?._id || raw?.subdomain);
        if (!project || !project.autoDeployEnabled) continue;
        try { await checkWorkspaceProjectAutoDeploy(user, project); } catch (_) {}
      }
    }
    return;
  }
  let allProjects = [];
  try { allProjects = await Project.find({ autoDeployEnabled: true }).maxTimeMS(7000); } catch { return; }
  for (const p of allProjects) {
    try { await checkProjectAutoDeploy(p); } catch (_) {}
  }
  } finally {
    autoDeployPollRunning = false;
  }
}
setInterval(checkAndAutoDeployProjects, AUTO_DEPLOY_POLL_INTERVAL_MS);
setTimeout(checkAndAutoDeployProjects, AUTO_DEPLOY_INITIAL_DELAY_MS);
