'use strict';

const express  = require('express');
const http     = require('http');
const { Server: SocketIO } = require('socket.io');
const path     = require('path');
const fs       = require('fs');
const mongoose = require('mongoose');
require('dotenv').config();

const app    = express();
const server = http.createServer(app);
const io     = new SocketIO(server, { cors: { origin: '*', methods: ['GET','POST'] } });

const PORT        = process.env.PORT        || 3001;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/deployboard';
const SITES_DIR   = process.env.SITES_DIR   || '/var/www/user-sites';
const TMP_DIR     = process.env.TMP_DIR     || '/tmp/deployboard-builds';
const GITHUB_TOKEN= process.env.GITHUB_TOKEN|| '';
const BASE_DOMAIN = process.env.BASE_DOMAIN || 'localhost';
const CF_API_TOKEN  = process.env.CF_API_TOKEN  || '';
const CF_ZONE_ID    = process.env.CF_ZONE_ID    || '';
const CF_TUNNEL_ID  = process.env.CF_TUNNEL_ID  || '';
const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID || '';

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
app.use(express.json());
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
  const host = (
    req.headers['x-forwarded-host'] ||
    req.headers.host ||
    ''
  ).toLowerCase().split(',')[0].trim().replace(/:[0-9]+$/, '');

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
  const host = (
    req.headers['x-forwarded-host'] ||
    req.headers.host ||
    ''
  ).toLowerCase().split(',')[0].trim(); // x-forwarded-host can be a comma list

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
  const wildcardMode = process.env.CF_WILDCARD_MODE !== 'false';
  if (wildcardMode || !CF_API_TOKEN || !CF_ZONE_ID) {
    return { ok: true, url: `https://${fullDomain}` };
  }
  try {
    const target = CF_TUNNEL_ID ? `${CF_TUNNEL_ID}.cfargotunnel.com` : BASE_DOMAIN;
    const r = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: 'CNAME', name: subdomain, content: target, proxied: true, ttl: 1 })
    });
    const d = await r.json();
    if (!d.success && !d.errors?.[0]?.message?.toLowerCase().includes('already exists')) {
      return { ok: false, reason: d.errors?.[0]?.message || 'DNS error' };
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
  updatedAt:  { type: Date,   default: Date.now }
});

const deploymentSchema = new mongoose.Schema({
  projectId:   { type: mongoose.Schema.Types.ObjectId, ref: 'Project' },
  projectName: String,
  branch:      { type: String, default: 'main' },
  status:      { type: String, enum: ['pending','building','success','failed'], default: 'pending' },
  logs:        [String],
  duration:    Number,
  startedAt:   { type: Date, default: Date.now },
  endedAt:     Date
});

const Project    = mongoose.model('Project',    projectSchema);
const Deployment = mongoose.model('Deployment', deploymentSchema);

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

app.get('/api/projects', async (req, res) => {
  try {
    const projects  = await Project.find().sort({ createdAt: -1 });
    const enriched  = await Promise.all(projects.map(async p => {
      const last = await Deployment.findOne({ projectId: p._id }).sort({ startedAt: -1 }).select('status duration endedAt');
      const obj  = p.toObject();
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

app.get('/api/deployments', async (req, res) => {
  try {
    const filter = req.query.projectId ? { projectId: req.query.projectId } : {};
    res.json(await Deployment.find(filter).sort({ startedAt: -1 }).limit(100));
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
  const clean = domain.toLowerCase().trim().replace('https://', '').replace('http://', '').replace(/\/$/, '');
  try {
    if (!isMongoReady()) {
      // In-memory fallback
      const exists = memDomains.find(d => d.domain === clean);
      if (exists) return res.status(409).json({ error: 'Domain already registered' });
      const entry = { domain: clean, subdomain, verified: false, createdAt: new Date() };
      memDomains.push(entry);
      addActivity('domain', 'Custom domain added (mem): ' + clean + ' → ' + subdomain);
      return res.json({ ok: true, domain: entry, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
    }
    const existing = await CustomDomain.findOne({ domain: clean }).lean().maxTimeMS(5000);
    if (existing) return res.status(409).json({ error: 'Domain already registered' });
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

app.post('/api/domains/:domain/verify', async (req, res) => {
  const domain = req.params.domain;
  try {
    const { execSync } = require('child_process');
    let verified = false;
    try {
      let result = '';
      // Try dig first, fall back to nslookup, fall back to a simple DNS check
      const cmds = [
        'dig +short CNAME ' + domain,
        'nslookup ' + domain + ' | grep -i cname',
        'host ' + domain,
      ];
      for (const cmd of cmds) {
        try {
          result = execSync(cmd + ' 2>/dev/null', { encoding: 'utf8', timeout: 5000 }).trim();
          if (result) break;
        } catch(ex) {}
      }
      if (result.includes('cfargotunnel.com') || result.includes(BASE_DOMAIN)) {
        verified = true;
      }
      // If no DNS tools available, mark as pending (user can verify later)
    } catch(e) {}
    await CustomDomain.findOneAndUpdate({ domain }, { verified }).maxTimeMS(5000);
    res.json({ ok: true, verified });
  } catch(e) { res.status(500).json({ error: e.message }); }
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
app.post('/api/deploy', async (req, res) => {
  const { name, subdomain, repoUrl, branch, installCmd, buildCmd,
          startCmd, outputDir, nodeVer, siteType, envVars,
          isDockerfileDeploy, isWorker, dockerfilePath, exposedPort } = req.body;

  if (!name || !subdomain || !repoUrl) {
    return res.status(400).json({ error: 'name, subdomain and repoUrl are required' });
  }

  const cleanSub = subdomain.toLowerCase()
    .replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');

  const isServerApp = (siteType === 'server') || !!(startCmd || '').trim();

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
      branch: branch||'main', status: 'pending'
    }).save();
  } catch(e) {
    deployment = {
      _id: 'local_' + Date.now(), projectId: project._id,
      projectName: name, branch: branch||'main',
      status: 'pending', logs: [], startedAt: new Date(),
      save: async () => {}
    };
  }

  const deployId = deployment._id.toString();
  res.json({ ok: true, deployId, message: 'Build started',
             liveUrl: `https://${cleanSub}.${BASE_DOMAIN}` });

  // Async build
  const buildStart = Date.now();
  deployment.status = 'building';
  try { await deployment.save(); } catch(e) {}

  const emit = (event, data) => io.emit(event, { deployId, ...data });

  try {
    emit('build:log', { line: `\x1b[36m[DeployBoard]\x1b[0m Building \x1b[1m${name}\x1b[0m` });
    emit('build:log', { line: `\x1b[90mRepo: ${repoUrl}  Branch: ${branch||'main'}\x1b[0m` });
    emit('build:log', { line: `\x1b[90mTarget: https://${cleanSub}.${BASE_DOMAIN}\x1b[0m` });
    emit('build:log', { line: '' });

    await runBuild({
      deployId, project, sitesDir: SITES_DIR, tmpDir: TMP_DIR,
      githubToken: GITHUB_TOKEN, appPort, emit,
      isDockerfileDeploy: !!isDockerfileDeploy,
      isWorker:           !!isWorker,
      onLog: (line) => { deployment.logs = deployment.logs || []; deployment.logs.push(line); }
    });

    // Register CF subdomain
    emit('build:log', { line: `\x1b[36m[DeployBoard]\x1b[0m Registering subdomain…` });
    const cf = await registerSubdomain(cleanSub);
    if (cf.ok) {
      emit('build:log', { line: `\x1b[32m[CF]\x1b[0m Live at: \x1b[1m${cf.url}\x1b[0m` });
      try { await Project.findByIdAndUpdate(project._id, { liveUrl: cf.url }); } catch(e) {}
    }

    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'success'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    addActivity('deploy', '✓ Deployment succeeded: ' + name + ' in ' + duration + 's');
    emit('build:log', { line: `\n\x1b[32m✓ Deployed in ${duration}s\x1b[0m` });
    emit('build:done', { status: 'success', duration, liveUrl: cf?.url || null });

  } catch(buildErr) {
    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'failed'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    addActivity('deploy', '✗ Deployment failed: ' + name + ' — ' + buildErr.message.slice(0,80));
    const buildDir = path.join(TMP_DIR, deployId);
    try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
    emit('build:log', { line: `\x1b[31m[DeployBoard]\x1b[0m Build failed: ${buildErr.message}` });
    emit('build:done', { status: 'failed', duration });
    console.error(`[Deploy] FAILED ${name}:`, buildErr.message);
  }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log('[Socket.io] Connected:', socket.id);
  socket.on('disconnect', () => console.log('[Socket.io] Disconnected:', socket.id));
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
