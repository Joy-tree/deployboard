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
const FIREBASE_RTDB_URL = (process.env.FIREBASE_RTDB_URL || process.env.FIREBASE_DATABASE_URL || '').replace(/\/+$/, '');
const FIREBASE_RTDB_SECRET = process.env.FIREBASE_RTDB_SECRET || process.env.FIREBASE_DATABASE_SECRET || '';
const TURNSTILE_SITE_KEY = process.env.TURNSTILE_SITE_KEY || '';
const TURNSTILE_SECRET_KEY = process.env.TURNSTILE_SECRET_KEY || '';
let GLOBAL_WEBHOOK_SECRET = process.env.GLOBAL_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET || '';
if (!GLOBAL_WEBHOOK_SECRET) GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
const INTERNAL_DEPLOY_KEY = process.env.INTERNAL_DEPLOY_KEY || crypto.randomBytes(24).toString('hex');
const SESSION_TTL_DAYS = Number(process.env.SESSION_TTL_DAYS || 30);
const AUTO_DEPLOY_POLL_INTERVAL_MS = Math.max(250, Number(process.env.AUTO_DEPLOY_POLL_INTERVAL_MS || 750) || 750);
const AUTO_DEPLOY_INITIAL_DELAY_MS = Math.max(250, Number(process.env.AUTO_DEPLOY_INITIAL_DELAY_MS || AUTO_DEPLOY_POLL_INTERVAL_MS) || AUTO_DEPLOY_POLL_INTERVAL_MS);
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || '').trim();
const RESEND_FROM_EMAIL = String(process.env.RESEND_FROM_EMAIL || '').trim();
const RESEND_AUDIENCE_EMAIL = String(process.env.RESEND_AUDIENCE_EMAIL || '').trim();
const RESEND_REPLY_TO_EMAIL = String(process.env.RESEND_REPLY_TO_EMAIL || '').trim();
const RESEND_LOGO_URL = String(process.env.RESEND_LOGO_URL || '').trim();
const RESEND_HERO_IMAGE_URL = String(process.env.RESEND_HERO_IMAGE_URL || '').trim();
const RESEND_HERO_IMAGE_STARTED_URL = String(process.env.RESEND_HERO_IMAGE_STARTED_URL || '').trim();
const RESEND_HERO_IMAGE_SUCCESS_URL = String(process.env.RESEND_HERO_IMAGE_SUCCESS_URL || '').trim();
const RESEND_HERO_IMAGE_FAILED_URL = String(process.env.RESEND_HERO_IMAGE_FAILED_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_FREE_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_FREE_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_STARTER_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_STARTER_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_PRO_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_PRO_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_GROWTH_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_GROWTH_URL || '').trim();
const RESEND_BILLING_HERO_IMAGE_SCALE_URL = String(process.env.RESEND_BILLING_HERO_IMAGE_SCALE_URL || '').trim();
const RESEND_WELCOME_HERO_IMAGE_URL = String(process.env.RESEND_WELCOME_HERO_IMAGE_URL || '').trim();
const PAYSTACK_PUBLIC_KEY = String(process.env.PAYSTACK_PUBLIC_KEY || '').trim();
const PAYSTACK_SECRET_KEY = String(process.env.PAYSTACK_SECRET_KEY || '').trim();
const PAYSTACK_WEBHOOK_SECRET = String(process.env.PAYSTACK_WEBHOOK_SECRET || process.env.PAYSTACK_SECRET_KEY || '').trim();
const GROQ_API_KEY = String(process.env.GROQ_API_KEY || process.env.GROQ_API_KEY_1 || '').trim();
const GROQ_MODEL = String(process.env.GROQ_MODEL || 'llama-3.3-70b-versatile').trim();
// xAI (Grok) — secondary AI provider for fallback and Joy AI v2
const XAI_API_KEY  = String(process.env.XAI_API_KEY  || process.env.XAI_API_KEY_1  || '').trim();
const XAI_MODEL    = String(process.env.XAI_MODEL    || 'grok-3-mini').trim();
// Joytree API v3 — Cerebras (ultra-fast 1000+ tok/s cloud AI, OpenAI-compatible)
const JOYTREE_V3_ADMIN_EMAIL = String(process.env.JOYTREE_V3_ADMIN_EMAIL || 'projectvpn89@gmail.com').trim().toLowerCase();
const CEREBRAS_API_KEY = String(process.env.CEREBRAS_API_KEY || '').trim();
const CEREBRAS_MODEL = String(process.env.CEREBRAS_MODEL || 'llama-3.3-70b').trim();
const CEREBRAS_MODEL_FALLBACK = 'llama3.1-8b'; // fast fallback if 70b is unavailable
const CEREBRAS_FLOW_TIMEOUT_MS = Number(process.env.CEREBRAS_FLOW_TIMEOUT_MS || 120000);
const CEREBRAS_CHUNK_TIMEOUT_MS = Number(process.env.CEREBRAS_CHUNK_TIMEOUT_MS || 90000);
const CEREBRAS_FLOW_MAX_TOKENS = Number(process.env.CEREBRAS_FLOW_MAX_TOKENS || 8192);
const CEREBRAS_CHUNK_MAX_TOKENS = Number(process.env.CEREBRAS_CHUNK_MAX_TOKENS || 4096);
const JOYTREE_WEB_SEARCH_URL = String(process.env.JOYTREE_WEB_SEARCH_URL || '').trim().replace(/\/+$/, '');
const JOYTREE_WEB_SEARCH_ENABLED = String(process.env.JOYTREE_WEB_SEARCH_ENABLED || 'false').toLowerCase() !== 'false';

// Joytree API v4 — Groq + xAI cascade with alternating chunk providers

const authOtpStore = new Map();
const deployStopRequests = new Set();

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

async function sendDeploymentStatusEmail({
  userEmail = '',
  projectName = '',
  subdomain = '',
  branch = 'main',
  status = 'success',
  duration = 0,
  source = 'manual',
  liveUrl = '',
  sha = '',
  errorMessage = '',
  repoUrl = '',
  buildStatus = '',
  deployStatus = '',
  memoryLimit = '',
  cpuShares = '',
  totalDeployments = 0,
  deployedAt = null,
  phase = 'completed'
} = {}) {
  if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) return { ok: false, skipped: true, reason: 'resend_not_configured' };
  const recipient = RESEND_AUDIENCE_EMAIL || String(userEmail || '').trim();
  if (!recipient) return { ok: false, skipped: true, reason: 'missing_recipient' };
  const isStartedPhase = phase === 'started';
  const statusLabel = isStartedPhase ? 'Started' : (status === 'success' ? 'Successful' : 'Failed');
  const shortSha = String(sha || '').trim().slice(0, 7);
  const sourceLabel = source === 'auto' ? 'Automatic (GitHub push)' : 'Manual (Redeploy click)';
  const safeError = String(errorMessage || '').trim().slice(0, 500);
  const logoUrl = RESEND_LOGO_URL || `https://${BASE_DOMAIN}/logo_optimized.jpg`;
  const heroImageUrl = (
    isStartedPhase
      ? (RESEND_HERO_IMAGE_STARTED_URL || RESEND_HERO_IMAGE_URL)
      : (status === 'success' ? (RESEND_HERO_IMAGE_SUCCESS_URL || RESEND_HERO_IMAGE_URL) : (RESEND_HERO_IMAGE_FAILED_URL || RESEND_HERO_IMAGE_URL))
  ) || '';
  const deployedAtText = deployedAt ? new Date(deployedAt).toLocaleString('en-US', { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' }) + ' UTC' : '';
  const dashboardUrl = `https://${BASE_DOMAIN}`;
  const lines = [
    `JOYTREE deployment report`,
    `Project: ${projectName || subdomain}`,
    `Status: ${statusLabel}`,
    `Source: ${sourceLabel}`,
    `Branch: ${branch || 'main'}`,
    shortSha ? `Commit: ${shortSha}` : '',
    duration > 0 ? `Duration: ${duration}s` : '',
    liveUrl ? `Live URL: ${liveUrl}` : '',
    safeError ? `Error: ${safeError}` : '',
    '',
    'Sent by JOYTREE.'
  ].filter(Boolean);
  const statusColor = isStartedPhase ? '#1d4ed8' : (status === 'success' ? '#0f766e' : '#b91c1c');
  const statusBg = isStartedPhase ? '#eff6ff' : (status === 'success' ? '#ecfeff' : '#fef2f2');
  const html = `<!doctype html>
<html><body style="margin:0;padding:0;background:#f5f8fb;font-family:Inter,Segoe UI,Arial,sans-serif;color:#0f172a;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:28px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background:#ffffff;border:1px solid #e2e8f0;border-radius:18px;overflow:hidden;">
        <tr>
          <td style="padding:22px 26px;background:linear-gradient(120deg,#0f172a,#14532d);color:#fff;">
            <table role="presentation" width="100%"><tr>
              <td style="width:54px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JOYTREE" width="44" height="44" style="width:44px;height:44px;border-radius:10px;display:block;background:#fff;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;">
                <div style="font-size:12px;letter-spacing:.12em;opacity:.82;">DEPLOYMENT INTELLIGENCE</div>
                <div style="font-size:24px;font-weight:800;line-height:1.2;">JOYTREE</div>
              </td>
            </tr></table>
          </td>
        </tr>
        ${heroImageUrl ? `<tr><td><img src="${heroImageUrl}" alt="JOYTREE Deployment Banner" style="display:block;width:100%;max-height:280px;object-fit:cover;"></td></tr>` : ''}
        <tr><td style="padding:24px 26px;">
          <div style="display:inline-block;padding:8px 12px;border-radius:999px;background:${statusBg};color:${statusColor};font-size:12px;font-weight:700;letter-spacing:.03em;">
            ${statusLabel.toUpperCase()} DEPLOYMENT
          </div>
          <h2 style="margin:14px 0 8px;font-size:22px;line-height:1.3;">${projectName || subdomain} is now ${isStartedPhase ? 'being deployed' : (status === 'success' ? 'live' : 'reporting an issue')}.</h2>
          <p style="margin:0 0 16px;color:#334155;font-size:14px;">Here’s your professional deployment report from JOYTREE.</p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Project:</strong> ${projectName || subdomain}</td></tr>
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Source:</strong> ${sourceLabel}</td></tr>
            ${repoUrl ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Repository:</strong> <a href="${repoUrl}" style="color:#0ea5e9;text-decoration:none;">${repoUrl}</a></td></tr>` : ''}
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Branch:</strong> ${branch || 'main'}${shortSha ? ` <span style="color:#64748b;">(${shortSha})</span>` : ''}</td></tr>
            ${duration > 0 ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Duration:</strong> ${duration}s</td></tr>` : ''}
            ${buildStatus ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Build Status:</strong> ${buildStatus}</td></tr>` : ''}
            ${deployStatus ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Deployment Status:</strong> ${deployStatus}</td></tr>` : ''}
            ${memoryLimit || cpuShares ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Resources:</strong> RAM ${memoryLimit || '—'} • CPU ${cpuShares ? `${cpuShares} shares` : '—'}</td></tr>` : ''}
            ${deployedAtText ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Deployed At:</strong> ${deployedAtText}</td></tr>` : ''}
            ${totalDeployments > 0 ? `<tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Total Deployments:</strong> ${totalDeployments}</td></tr>` : ''}
            ${liveUrl ? `<tr><td style="padding:14px 16px;border-bottom:${safeError ? '1px solid #e2e8f0' : '0'};font-size:14px;"><strong>Live URL:</strong> <a href="${liveUrl}" style="color:#0ea5e9;text-decoration:none;">${liveUrl}</a></td></tr>` : ''}
            ${safeError ? `<tr><td style="padding:14px 16px;font-size:14px;color:#991b1b;"><strong>Error:</strong> ${safeError}</td></tr>` : ''}
          </table>
          <div style="margin-top:18px;">
            <a href="${liveUrl || dashboardUrl}" style="display:inline-block;background:linear-gradient(135deg,#16a34a,#15803d);color:#fff;text-decoration:none;padding:12px 22px;border-radius:10px;font-weight:700;">View Deployment</a>
          </div>
          <p style="margin:16px 0 0;color:#64748b;font-size:12px;">This message was sent by JOYTREE deployment notifications.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;
  const payload = {
    from: RESEND_FROM_EMAIL,
    to: [recipient],
    subject: `JOYTREE • ${statusLabel} deployment — ${projectName || subdomain}`,
    text: lines.join('\n'),
    html
  };
  if (RESEND_REPLY_TO_EMAIL) payload.reply_to = RESEND_REPLY_TO_EMAIL;
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!r.ok) {
    const detail = await r.text().catch(() => '');
    return { ok: false, skipped: false, reason: `resend_http_${r.status}`, detail: detail.slice(0, 300) };
  }
  return { ok: true };
}

// ── Ensure directories ────────────────────────────────────────────────────────
[SITES_DIR, TMP_DIR].forEach(d => { try { fs.mkdirSync(d, { recursive: true }); } catch(e) {} });

// ── Port registry ─────────────────────────────────────────────────────────────
// Maps subdomain → host port where the user's Docker container is listening
const PORTS_FILE = path.join(SITES_DIR, 'ports.json');
const PORT_START = Number(process.env.APP_PORT_START || 10000);
const PORT_END   = Number(process.env.APP_PORT_END || 20000);

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
  // Also persist to Firebase so ports survive a full VPS reinstall
  if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
    const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
    fetch(`${FIREBASE_RTDB_URL}/deployboard_ports.json${authQuery}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(portRegistry)
    }).catch(e => console.warn('[Ports] Firebase save failed:', e.message));
  }
}

function getOrAssignPort(subdomain) {
  const isPortAvailable = (port) => {
    try {
      execSync(`sh -lc "ss -ltn '( sport = :${Number(port)} )' | tail -n +2 | grep -q . && exit 1 || exit 0"`, { stdio: 'pipe' });
      return true;
    } catch {
      return false;
    }
  };

  const assigned = Number(portRegistry[subdomain] || 0);
  if (assigned >= PORT_START && assigned <= PORT_END && isPortAvailable(assigned)) return assigned;

  const used = new Set(
    Object.values(portRegistry)
      .map(v => Number(v))
      .filter(v => Number.isFinite(v))
  );
  if (assigned) used.delete(assigned);
  for (let p = PORT_START; p <= PORT_END; p++) {
    if (!used.has(p) && isPortAvailable(p)) {
      portRegistry[subdomain] = p;
      savePortRegistry();
      console.log(`[Ports] Assigned port ${p} to ${subdomain}`);
      return p;
    }
  }
  throw new Error(`No free ports available (${PORT_START}-${PORT_END} all in use)`);
}
function clearAssignedPort(subdomain) {
  if (!subdomain) return;
  if (Object.prototype.hasOwnProperty.call(portRegistry, subdomain)) {
    delete portRegistry[subdomain];
    savePortRegistry();
  }
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
  // are reachable immediately without restarting Joytree.
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
      if (!res.headersSent) res.status(504).send(errorPage('gateway_timeout', subdomain, BASE_DOMAIN));
    });

    proxyReq.on('error', () => {
      if (!res.headersSent) res.status(502).send(errorPage('bad_gateway', subdomain, BASE_DOMAIN));
    });

    req.pipe(proxyReq, { end: true });
    req.on('error', () => proxyReq.destroy());
    return;
  }

  // ── Static site ───────────────────────────────────────────────────────────
  if (fs.existsSync(distDir)) return serveStatic(req, res, distDir);

  // ── Not deployed ──────────────────────────────────────────────────────────
  res.status(503).send(errorPage('not_deployed', subdomain, BASE_DOMAIN));
});

// Dashboard static serving is registered AFTER all API routes (see bottom of file)

// ── HTML helpers ─────────────────────────────────────────────────────────────
function errorPage(type, subdomain, baseDomain) {
  const ts   = new Date().toISOString();
  const reqId = Math.random().toString(36).slice(2,10).toUpperCase() + '-' + Date.now().toString(36).toUpperCase();
  const host  = subdomain + '.' + baseDomain;

  const variants = {
    not_deployed: {
      code: '503',
      label: 'Service Unavailable',
      headline: 'Not Deployed',
      color: '#f59e0b',
      colorDim: 'rgba(245,158,11,0.12)',
      colorBorder: 'rgba(245,158,11,0.22)',
      dotColor: '#f59e0b',
      icon: `<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`,
      detail: 'This subdomain is registered but has no active deployment. Push a release from the JOYTREE dashboard to bring it online.',
      meta: [
        { k: 'STATUS',    v: '503 Service Unavailable' },
        { k: 'REASON',    v: 'No active deployment found' },
        { k: 'HOST',      v: host },
        { k: 'TIMESTAMP', v: ts },
        { k: 'REQUEST ID',v: reqId },
      ]
    },
    bad_gateway: {
      code: '502',
      label: 'Bad Gateway',
      headline: 'App Not Running',
      color: '#ef4444',
      colorDim: 'rgba(239,68,68,0.10)',
      colorBorder: 'rgba(239,68,68,0.22)',
      dotColor: '#ef4444',
      icon: `<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>`,
      detail: 'The container for this subdomain is not responding. It may have crashed or failed to start. Redeploy from the JOYTREE dashboard.',
      meta: [
        { k: 'STATUS',    v: '502 Bad Gateway' },
        { k: 'REASON',    v: 'Container not responding' },
        { k: 'HOST',      v: host },
        { k: 'TIMESTAMP', v: ts },
        { k: 'REQUEST ID',v: reqId },
      ]
    },
    gateway_timeout: {
      code: '504',
      label: 'Gateway Timeout',
      headline: 'App Timed Out',
      color: '#f97316',
      colorDim: 'rgba(249,115,22,0.10)',
      colorBorder: 'rgba(249,115,22,0.22)',
      dotColor: '#f97316',
      icon: `<svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
      detail: 'The app did not respond within 30 seconds. It may still be starting up — wait a moment and try again, or redeploy from the dashboard.',
      meta: [
        { k: 'STATUS',    v: '504 Gateway Timeout' },
        { k: 'REASON',    v: 'Upstream response timeout (30s)' },
        { k: 'HOST',      v: host },
        { k: 'TIMESTAMP', v: ts },
        { k: 'REQUEST ID',v: reqId },
      ]
    }
  };

  const v = variants[type] || variants['not_deployed'];

  const metaRows = v.meta.map(m =>
    `<div class="meta-row"><span class="meta-k">${m.k}</span><span class="meta-v">${m.v}</span></div>`
  ).join('');

  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${v.code} ${v.label} — ${host}</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
html{height:100%}
body{
  font-family:'Inter',system-ui,-apple-system,sans-serif;
  background:#09090b;
  color:#e2e8f0;
  min-height:100%;
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:center;
  padding:32px 20px;
  position:relative;
  overflow:hidden;
}

/* Subtle radial glow behind card */
body::before{
  content:'';
  position:fixed;
  top:50%;left:50%;
  transform:translate(-50%,-52%);
  width:660px;height:440px;
  background:radial-gradient(ellipse at center, ${v.colorDim} 0%, transparent 68%);
  pointer-events:none;
  z-index:0;
}

/* Dot grid background */
body::after{
  content:'';
  position:fixed;
  inset:0;
  background-image:radial-gradient(rgba(255,255,255,0.045) 1px, transparent 1px);
  background-size:28px 28px;
  pointer-events:none;
  z-index:0;
}

/* ── Top bar ── */
.topbar{
  position:fixed;top:0;left:0;right:0;
  height:44px;
  display:flex;align-items:center;justify-content:space-between;
  padding:0 24px;
  background:rgba(9,9,11,0.88);
  border-bottom:1px solid rgba(255,255,255,0.06);
  backdrop-filter:blur(12px);
  z-index:10;
  font-size:.72rem;
  letter-spacing:.08em;
}
.topbar-left{display:flex;align-items:center;gap:16px;color:#52525b}
.topbar-brand{display:flex;align-items:center;gap:8px;color:#71717a;font-weight:700;letter-spacing:.12em;text-transform:uppercase;font-size:.68rem}
.topbar-brand-dot{width:6px;height:6px;border-radius:50%;background:#10b981;box-shadow:0 0 6px rgba(16,185,129,.9)}
.topbar-right{display:flex;align-items:center;gap:8px;color:#3f3f46;font-family:'Courier New',monospace;font-size:.65rem}
.status-pill{
  display:inline-flex;align-items:center;gap:5px;
  padding:3px 10px;
  border-radius:999px;
  background:${v.colorDim};
  border:1px solid ${v.colorBorder};
  color:${v.color};
  font-size:.63rem;font-weight:800;letter-spacing:.1em;text-transform:uppercase;
}
.status-dot{width:5px;height:5px;border-radius:50%;background:${v.dotColor};flex-shrink:0}

/* ── Main card ── */
.card{
  position:relative;z-index:1;
  width:min(96vw,580px);
  background:#111113;
  border:1px solid rgba(255,255,255,0.08);
  border-radius:20px;
  overflow:hidden;
  box-shadow:
    0 0 0 1px rgba(255,255,255,0.03) inset,
    0 24px 80px rgba(0,0,0,0.6),
    0 0 0 1px rgba(0,0,0,0.4);
  margin-top:12px;
}

/* Colored top edge */
.card-edge{
  height:1px;
  background:linear-gradient(90deg, transparent 0%, ${v.color} 30%, ${v.color} 70%, transparent 100%);
  opacity:0.7;
}

/* ── Code block ── */
.code-block{
  padding:28px 28px 0;
  display:flex;
  align-items:flex-end;
  gap:16px;
  border-bottom:1px solid rgba(255,255,255,0.05);
  padding-bottom:24px;
}
.error-code{
  font-family:'Courier New',Courier,monospace;
  font-size:5.5rem;
  font-weight:900;
  line-height:1;
  color:${v.color};
  opacity:0.18;
  letter-spacing:-.04em;
  user-select:none;
  flex-shrink:0;
}
.code-right{flex:1;min-width:0;padding-bottom:6px}
.code-label{
  font-family:'Courier New',Courier,monospace;
  font-size:.65rem;
  font-weight:700;
  letter-spacing:.16em;
  color:#3f3f46;
  text-transform:uppercase;
  margin-bottom:8px;
}
.code-icon{
  color:${v.color};
  margin-bottom:10px;
  opacity:0.85;
}
.code-headline{
  font-size:1.55rem;
  font-weight:800;
  letter-spacing:-.03em;
  color:#f4f4f5;
  line-height:1.15;
  margin-bottom:6px;
}
.code-sub{
  font-size:.82rem;
  color:#71717a;
  line-height:1.6;
}

/* ── Meta table ── */
.meta{
  padding:20px 28px;
  border-bottom:1px solid rgba(255,255,255,0.05);
}
.meta-row{
  display:flex;
  align-items:baseline;
  justify-content:space-between;
  gap:12px;
  padding:6px 0;
  border-bottom:1px solid rgba(255,255,255,0.04);
}
.meta-row:last-child{border-bottom:none}
.meta-k{
  font-family:'Courier New',Courier,monospace;
  font-size:.63rem;
  font-weight:700;
  letter-spacing:.12em;
  color:#3f3f46;
  text-transform:uppercase;
  flex-shrink:0;
  white-space:nowrap;
}
.meta-v{
  font-family:'Courier New',Courier,monospace;
  font-size:.72rem;
  color:#52525b;
  text-align:right;
  word-break:break-all;
}
.meta-v.host{color:#71717a;}

/* ── Actions ── */
.actions{
  padding:20px 28px 24px;
  display:flex;
  gap:10px;
  flex-wrap:wrap;
}
.btn-primary{
  flex:1;
  min-height:44px;
  display:inline-flex;align-items:center;justify-content:center;gap:8px;
  padding:0 20px;
  border-radius:10px;
  background:#10b981;
  color:#fff;
  text-decoration:none;
  font-size:.85rem;
  font-weight:700;
  letter-spacing:.01em;
  border:none;
  cursor:pointer;
  transition:background .15s;
  white-space:nowrap;
}
.btn-primary:hover{background:#059669}
.btn-ghost{
  flex:1;
  min-height:44px;
  display:inline-flex;align-items:center;justify-content:center;gap:8px;
  padding:0 20px;
  border-radius:10px;
  background:transparent;
  color:#71717a;
  text-decoration:none;
  font-size:.85rem;
  font-weight:600;
  border:1px solid rgba(255,255,255,0.08);
  cursor:pointer;
  transition:background .15s,color .15s,border-color .15s;
  white-space:nowrap;
}
.btn-ghost:hover{background:rgba(255,255,255,0.05);color:#a1a1aa;border-color:rgba(255,255,255,0.14)}

/* ── Footer ── */
.footer{
  position:relative;z-index:1;
  margin-top:22px;
  text-align:center;
  font-size:.65rem;
  color:#27272a;
  letter-spacing:.06em;
  font-family:'Courier New',monospace;
}
.footer a{color:#3f3f46;text-decoration:none}
.footer a:hover{color:#71717a}

@media(max-width:480px){
  .error-code{font-size:3.8rem}
  .code-headline{font-size:1.25rem}
  .code-block{gap:10px;padding:20px 18px 20px}
  .meta{padding:16px 18px}
  .actions{padding:16px 18px 20px;gap:8px}
  .btn-primary,.btn-ghost{font-size:.8rem;min-height:42px}
  .topbar{padding:0 16px}
}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <div class="topbar-left">
    <div class="topbar-brand">
      <span class="topbar-brand-dot"></span>
      JOYTREE
    </div>
    <div class="status-pill">
      <span class="status-dot"></span>
      ${v.code} ${v.label}
    </div>
  </div>
  <div class="topbar-right">${ts.slice(0,19).replace('T',' ')} UTC</div>
</div>

<!-- Main card -->
<div class="card">
  <div class="card-edge"></div>

  <div class="code-block">
    <div class="error-code">${v.code}</div>
    <div class="code-right">
      <div class="code-label">JOYTREE / EDGE ROUTER</div>
      <div class="code-icon">${v.icon}</div>
      <div class="code-headline">${v.headline}</div>
      <div class="code-sub">${v.detail}</div>
    </div>
  </div>

  <div class="meta">
    ${metaRows}
  </div>

  <div class="actions">
    <a class="btn-primary" href="https://${baseDomain}">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round"><polyline points="15 18 9 12 15 6"/></svg>
      Back to Dashboard
    </a>
    <a class="btn-ghost" href="" onclick="location.reload();return false;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
      Retry
    </a>
  </div>
</div>

<div class="footer">
  JOYTREE EDGE &nbsp;·&nbsp; REQ <span style="color:#3f3f46">${reqId}</span> &nbsp;·&nbsp; <a href="https://${baseDomain}">joytree.site</a>
</div>

</body>
</html>`;
}

function noRootRoutePage(subdomain, port) {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${subdomain} is live — JOYTREE</title>
    <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:Inter,system-ui,sans-serif;background:#0a0a0a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:24px}
    .card{width:min(94vw,480px);background:#111;border:1px solid rgba(255,255,255,.1);border-radius:20px;overflow:hidden}
    .accent-bar{height:3px;background:#10b981}
    .inner{padding:32px 28px 36px;text-align:center}
    .badge{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:999px;background:#1a1a1a;border:1px solid rgba(16,185,129,.3);color:#86efac;font-size:.72rem;letter-spacing:.1em;font-weight:800;text-transform:uppercase;margin-bottom:18px}
    .badge::before{content:'';width:7px;height:7px;border-radius:50%;background:#10b981;box-shadow:0 0 8px rgba(16,185,129,.8)}
    h2{font-size:1.6rem;font-weight:900;letter-spacing:-.03em;color:#f8fafc;margin-bottom:12px}
    p{color:#94a3b8;line-height:1.65;font-size:.9rem;margin-bottom:8px}
    .divider{height:1px;background:rgba(255,255,255,.07);margin:20px 0}
    code{background:#1a1a1a;border:1px solid rgba(255,255,255,.08);padding:3px 8px;border-radius:6px;font-family:monospace;font-size:.85em;color:#86efac}
    a{color:#10b981;text-decoration:none;font-weight:700}
    a:hover{text-decoration:underline}
    </style></head>
    <body><div class="card">
    <div class="accent-bar"></div>
    <div class="inner">
      <div class="badge">✓ App is Running</div>
      <h2>${subdomain}.${BASE_DOMAIN}</h2>
      <p>Your app is live in its own container on port <code>${port}</code>.</p>
      <div class="divider"></div>
      <p>This app has no <code>/</code> route — try a specific path like<br><code>/api</code>, <code>/users</code>, <code>/messages</code>, etc.</p>
    </div>
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

// ── Database model ────────────────────────────────────────────────────────────
const databaseSchema = new mongoose.Schema({
  name:          { type: String, required: true },
  engine:        { type: String, enum: ['mongodb','postgres','postgresql','mysql','mariadb','redis'], required: true },
  image:         { type: String, required: true },
  user:          { type: String, default: '' },
  pass:          { type: String, default: '' },
  dbName:        { type: String, default: '' },
  memory:        { type: String, default: '512m' },
  volume:        { type: String, default: '' },
  connStr:       { type: String, default: '' },
  internalPort:  { type: Number, default: 0 },
  containerName: { type: String, default: '' },
  status:        { type: String, enum: ['provisioning','running','stopped','error'], default: 'provisioning' },
  errorMessage:  { type: String, default: '' },
  ownerUserId:   { type: String, default: '', index: true },
  linkProjectId: { type: String, default: '' },
  createdAt:     { type: Date, default: Date.now },
  updatedAt:     { type: Date, default: Date.now }
});
const Database = mongoose.model('Database', databaseSchema);

const LOCAL_AUTH_FILE = path.join(SITES_DIR, 'local-auth.json');
let localAuth = { users: [], sessions: [] };
try {
  if (fs.existsSync(LOCAL_AUTH_FILE)) localAuth = JSON.parse(fs.readFileSync(LOCAL_AUTH_FILE, 'utf8'));
} catch {}
function saveLocalAuth() { try { fs.writeFileSync(LOCAL_AUTH_FILE, JSON.stringify(localAuth, null, 2)); } catch {} }
function isDbReady() { return mongoose.connection.readyState === 1; }

// ── Local database file fallback (used when MongoDB is not ready) ─────────────
const LOCAL_DB_FILE = path.join(SITES_DIR, 'local-databases.json');
let localDatabases = [];
try {
  if (fs.existsSync(LOCAL_DB_FILE)) localDatabases = JSON.parse(fs.readFileSync(LOCAL_DB_FILE, 'utf8'));
} catch {}
function saveLocalDatabases() {
  try { fs.writeFileSync(LOCAL_DB_FILE, JSON.stringify(localDatabases, null, 2)); } catch {}
}
function upsertLocalDb(rec) {
  const id = String(rec.id || rec._id || '');
  if (!id) return;
  const idx = localDatabases.findIndex(d => d.id === id);
  if (idx === -1) localDatabases.push(rec); else localDatabases[idx] = rec;
  saveLocalDatabases();
}
function removeLocalDb(id) {
  localDatabases = localDatabases.filter(d => d.id !== String(id));
  saveLocalDatabases();
}
// Sync localDatabases from MongoDB whenever it is available
function syncLocalDbsFromMongo() {
  if (!isDbReady()) return;
  Database.find({}).lean().then(dbs => {
    for (const db of dbs) {
      const rec = { ...db, id: String(db._id), connectionString: db.connStr || db.connectionString || '' };
      upsertLocalDb(rec);
    }
  }).catch(() => {});
}
setInterval(syncLocalDbsFromMongo, 30000);

function firebaseWorkspaceKey(user = {}) {
  // STABILITY FIX: Always prefer email as the canonical Firebase RTDB key.
  // email is the only identifier that is consistent across VPS rebuilds, new
  // MongoDB _id values, new firebaseUid tokens, and local-auth.json resets.
  // firebaseUid changes if Firebase is re-linked; _id/id change every time
  // MongoDB creates a new document; so we NEVER use those as the primary key.
  const email = String(user.email || '').trim().toLowerCase();
  if (email) return email.replace(/[^a-z0-9_-]/g, '_');
  // Last-resort fallback (should never happen for a real authenticated user)
  const raw = String(user.firebaseUid || user.id || user._id || '').trim().toLowerCase();
  return raw ? raw.replace(/[^a-z0-9_-]/g, '_') : '';
}
function firebaseWorkspaceUrl(user) {
  const key = firebaseWorkspaceKey(user);
  if (!FIREBASE_RTDB_URL || !key) return '';
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  return `${FIREBASE_RTDB_URL}/deployboard_workspaces/${key}.json${authQuery}`;
}
async function readWorkspaceFromFirebase(user) {
  try {
    const url = firebaseWorkspaceUrl(user);
    if (!url) return null;
    const r = await fetch(url, { method: 'GET', headers: { 'Accept': 'application/json' } });
    if (!r.ok) return null;
    const data = await r.json();
    return (data && typeof data === 'object') ? data : null;
  } catch { return null; }
}
async function writeWorkspaceToFirebase(user, workspace) {
  try {
    const url = firebaseWorkspaceUrl(user);
    if (!url) return false;
    const payload = workspace && typeof workspace === 'object' ? workspace : {};
    const r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    return r.ok;
  } catch { return false; }
}

// ── Firebase RTDB helpers for databases ───────────────────────────────────────
// Databases are stored at: deployboard_databases/<userKey>/<dbId>
// This is the PRIMARY store — MongoDB is not required.
function firebaseDbBaseUrl(user) {
  const key = firebaseWorkspaceKey(user);
  if (!FIREBASE_RTDB_URL || !key) {
    if (!FIREBASE_RTDB_URL) console.warn('[db/firebase] FIREBASE_RTDB_URL missing; skipping write');
    else console.warn('[db/firebase] workspace key missing; skipping write', {
      userId: String(user?._id || user?.id || ''),
      hasEmail: !!String(user?.email || '').trim(),
      hasFirebaseUid: !!String(user?.firebaseUid || '').trim()
    });
    return '';
  }
  return `${FIREBASE_RTDB_URL}/deployboard_databases/${key}`;
}
async function readAllDbsFromFirebase(user) {
  try {
    const base = firebaseDbBaseUrl(user);
    if (!base) return null;
    const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${base}.json${authQuery}`, { headers: { Accept: 'application/json' } });
    if (!r.ok) return null;
    const data = await r.json();
    if (!data || typeof data !== 'object') return [];
    return Object.values(data).filter(Boolean);
  } catch { return null; }
}
async function writeDbToFirebase(user, db) {
  try {
    const base = firebaseDbBaseUrl(user);
    if (!base) return false;
    const id = String(db.id || db._id || '');
    if (!id) return false;
    const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${base}/${id}.json${authQuery}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...db, id })
    });
    return r.ok;
  } catch { return false; }
}
async function deleteDbFromFirebase(user, dbId) {
  try {
    const base = firebaseDbBaseUrl(user);
    if (!base) return false;
    const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${base}/${dbId}.json${authQuery}`, { method: 'DELETE' });
    return r.ok;
  } catch { return false; }
}
// Save db record to all available stores (Firebase + local file)
async function persistDb(user, db) {
  user = await enrichAuthUser(user);
  const rec = { ...db, id: String(db.id || db._id || ''), externalConnectionString: externalDbConnStr(db) };
  upsertLocalDb(rec);                        // always write local file
  await writeDbToFirebase(user, rec);        // write to Firebase RTDB
  if (isDbReady()) {                         // also sync to Mongo if available
    Database.updateOne({ _id: rec.id }, { $set: { status: rec.status, updatedAt: new Date() } }).catch(() => {});
  }
  writeDbGatewayConfig(localDatabases);
}
async function removeDb(user, dbId) {
  user = await enrichAuthUser(user);
  removeLocalDb(dbId);
  await deleteDbFromFirebase(user, dbId);
  if (isDbReady()) Database.deleteOne({ _id: dbId }).catch(() => {});
}

async function enrichAuthUser(user) {
  if (!user) return user;
  if (String(user.email || '').trim()) return user;
  const uid = String(user._id || user.id || '');
  if (!uid) return user;
  if (isDbReady()) {
    try {
      const found = await User.findById(uid).select('email').lean();
      if (found?.email) return { ...user, email: String(found.email) };
    } catch {}
  }
  const local = localAuth.users.find(u => String(u.id || u._id || '') === uid);
  if (local?.email) return { ...user, email: String(local.email) };
  return user;
}
// Load databases for a user — Firebase first, then local file, then Mongo
async function loadUserDatabases(user) {
  user = await enrichAuthUser(user);
  const userId = String(user._id || user.id);
  // 1. Try Firebase RTDB (primary)
  const fbDbs = await readAllDbsFromFirebase(user);
  if (fbDbs && fbDbs.length > 0) {
    // Sync to local file
    fbDbs.forEach(db => upsertLocalDb({ ...db, ownerUserId: userId }));
    return fbDbs.map(db => ({ ...db, id: String(db.id || db._id || ''), ownerUserId: userId, externalConnectionString: externalDbConnStr(db) }));
  }
  // 2. Fall back to local file cache
  const localDbs = localDatabases.filter(d => String(d.ownerUserId) === userId);
  if (localDbs.length > 0) return localDbs.map(d=>({ ...d, externalConnectionString: externalDbConnStr(d) }));
  // 3. Last resort: Mongo (if available)
  if (isDbReady()) {
    const mongoDbs = await Database.find({ ownerUserId: userId }).sort({ createdAt: -1 }).lean().catch(() => []);
    // Back-fill Firebase and local file from Mongo
    for (const db of mongoDbs) {
      const rec = { ...db, id: String(db._id), connectionString: db.connStr || db.connectionString || '' };
      await writeDbToFirebase(user, rec);
      upsertLocalDb(rec);
    }
    return mongoDbs.map(d => ({ ...d, id: String(d._id), connectionString: d.connStr || d.connectionString || '', externalConnectionString: externalDbConnStr(d) }));
  }
  return [];
}
function createPasswordHash(password, salt = crypto.randomBytes(16).toString('hex')) {
  const hash = crypto.pbkdf2Sync(password, salt, 120000, 64, 'sha512').toString('hex');
  return { salt, hash };
}
function createSessionToken() {
  return crypto.randomBytes(36).toString('hex');
}
function generateOtpCode() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

async function sendPaymentSuccessEmail({ userEmail = '', plan = '', amountKobo = 0, currency = 'GHS', reference = '', paidAt = null } = {}) {
  if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) return { ok: false, skipped: true, reason: 'resend_not_configured' };
  const recipient = RESEND_AUDIENCE_EMAIL || String(userEmail || '').trim().toLowerCase();
  if (!recipient) return { ok: false, skipped: true, reason: 'missing_recipient' };
  const logoUrl = RESEND_LOGO_URL || `https://${BASE_DOMAIN}/logo_optimized.jpg`;
  const planLabelMap = { free: 'Free Plan', starter: 'Starter', pro: 'Pro', growth: 'Growth', scale: 'Scale Max' };
  const safePlan = String(plan || '').trim().toLowerCase();
  const planLabel = planLabelMap[safePlan] || (safePlan ? safePlan.toUpperCase() : 'Paid Plan');
  const amountMajor = Number(amountKobo || 0) / 100;
  const paidText = paidAt
    ? new Date(paidAt).toLocaleString('en-US', { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' }) + ' UTC'
    : new Date().toLocaleString('en-US', { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' }) + ' UTC';

  const billingHeroMap = {
    free: RESEND_BILLING_HERO_IMAGE_FREE_URL,
    starter: RESEND_BILLING_HERO_IMAGE_STARTER_URL,
    pro: RESEND_BILLING_HERO_IMAGE_PRO_URL,
    growth: RESEND_BILLING_HERO_IMAGE_GROWTH_URL,
    scale: RESEND_BILLING_HERO_IMAGE_SCALE_URL
  };
  const heroImageUrl = billingHeroMap[safePlan] || RESEND_BILLING_HERO_IMAGE_URL || '';

  const subject = `JOYTREE • Subscription payment confirmed — ${planLabel}`;
  const text = [
    'JOYTREE subscription payment receipt',
    `Plan: ${planLabel}`,
    `Amount: ${currency} ${amountMajor.toFixed(2)}`,
    `Reference: ${reference || '-'}`,
    `Paid at: ${paidText}`,
    '',
    `Manage your workspace: https://${BASE_DOMAIN}/dashboard/usage`,
    '',
    'This is an automated billing confirmation from JOYTREE.'
  ].join('\n');

  const html = `<!doctype html>
<html><body style="margin:0;padding:0;background:#f5f8fb;font-family:Inter,Segoe UI,Arial,sans-serif;color:#0f172a;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:28px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;">
        <tr>
          <td style="padding:20px 24px;background:linear-gradient(120deg,#0f172a,#14532d);color:#fff;">
            <table role="presentation" width="100%"><tr>
              <td style="width:52px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JOYTREE" width="42" height="42" style="width:42px;height:42px;border-radius:10px;display:block;background:#fff;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;">
                <div style="font-size:12px;letter-spacing:.12em;opacity:.82;">BILLING CONFIRMATION</div>
                <div style="font-size:24px;font-weight:800;line-height:1.2;">JOYTREE</div>
              </td>
            </tr></table>
          </td>
        </tr>
        ${heroImageUrl ? `<tr><td><img src="${heroImageUrl}" alt="JOYTREE Billing Banner" style="display:block;width:100%;max-height:280px;object-fit:cover;"></td></tr>` : ''}
        <tr><td style="padding:24px;">
          <div style="display:inline-block;padding:8px 12px;border-radius:999px;background:#ecfeff;color:#0f766e;font-size:12px;font-weight:700;letter-spacing:.03em;">PAYMENT RECEIVED</div>
          <h2 style="margin:14px 0 8px;font-size:22px;line-height:1.3;color:#0f172a;">Your ${planLabel} subscription is active.</h2>
          <p style="margin:0 0 16px;color:#334155;font-size:14px;">Thank you for your payment. This receipt confirms your subscription activation on JOYTREE.</p>

          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Plan:</strong> ${planLabel}</td></tr>
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Amount:</strong> ${currency} ${amountMajor.toFixed(2)}</td></tr>
            <tr><td style="padding:14px 16px;border-bottom:1px solid #e2e8f0;font-size:14px;"><strong>Reference:</strong> <span style="font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#334155;">${reference || '-'}</span></td></tr>
            <tr><td style="padding:14px 16px;font-size:14px;"><strong>Paid At:</strong> ${paidText}</td></tr>
          </table>

          <div style="margin-top:18px;">
            <a href="https://${BASE_DOMAIN}/dashboard/usage" style="display:inline-block;background:linear-gradient(135deg,#16a34a,#15803d);color:#fff;text-decoration:none;padding:12px 22px;border-radius:10px;font-weight:700;">Open Billing & Usage</a>
          </div>

          <p style="margin:16px 0 0;color:#64748b;font-size:12px;">Need help with billing? Reply to this email and our team will assist you.</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>`;

  const payload = { from: RESEND_FROM_EMAIL, to: [recipient], subject, html, text };
  if (RESEND_REPLY_TO_EMAIL) payload.reply_to = RESEND_REPLY_TO_EMAIL;
  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!r.ok) {
    const detail = await r.text().catch(() => '');
    return { ok: false, skipped: false, reason: `resend_http_${r.status}`, detail: detail.slice(0, 300) };
  }
  return { ok: true };
}


async function sendWelcomeEmail({ userEmail = '', userName = '' } = {}) {
  if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) return { ok: false, skipped: true, reason: 'resend_not_configured' };
  const recipient = RESEND_AUDIENCE_EMAIL || String(userEmail || '').trim().toLowerCase();
  if (!recipient) return { ok: false, skipped: true, reason: 'missing_recipient' };

  const logoUrl = RESEND_LOGO_URL || `https://${BASE_DOMAIN}/logo_optimized.jpg`;
  const heroImageUrl = RESEND_WELCOME_HERO_IMAGE_URL || '';
  const firstName = String(userName || '').trim().split(/\s+/)[0] || 'there';
  const dashboardUrl = `https://${BASE_DOMAIN}/dashboard`;
  const deployUrl = `https://${BASE_DOMAIN}/dashboard/new-deploy`;
  const docsUrl = `https://${BASE_DOMAIN}/dashboard/docs`;
  const supportUrl = `https://${BASE_DOMAIN}/dashboard/support`;

  const subject = 'Welcome to JOYTREE — your professional deployment workspace is ready';

  const text = [
    `Welcome to JOYTREE, ${firstName}.`,
    '',
    'Your account verification is complete and your workspace is now ready.',
    'What you can do next:',
    '- Deploy from GitHub repositories',
    '- Monitor real-time build and runtime logs',
    '- Configure environment variables and domains',
    '- Manage billing, plans, and usage limits',
    '',
    `Open Dashboard: ${dashboardUrl}`,
    `Create First Deployment: ${deployUrl}`,
    `Read Documentation: ${docsUrl}`,
    `Contact Support: ${supportUrl}`,
    '',
    'JOYTREE Team'
  ].join('\n');

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Welcome to JOYTREE</title>
</head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:'Segoe UI',Arial,sans-serif;color:#0f172a;-webkit-font-smoothing:antialiased;">

<!-- Outer wrapper -->
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f0f4f8;padding:36px 12px 48px;">
<tr><td align="center">

<!-- Card -->
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="max-width:640px;">

  <!-- ══ HEADER ══ -->
  <tr>
    <td style="background:linear-gradient(135deg,#052e16 0%,#064e2c 50%,#0a3d20 100%);border-radius:18px 18px 0 0;padding:28px 32px 26px;">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
        <tr>
          <td style="vertical-align:middle;width:52px;">
            <img src="${logoUrl}" alt="JOYTREE" width="48" height="48" style="width:48px;height:48px;border-radius:12px;display:block;object-fit:cover;border:2px solid rgba(255,255,255,.12);">
          </td>
          <td style="vertical-align:middle;padding-left:14px;">
            <div style="font-size:11px;letter-spacing:.22em;color:rgba(134,239,172,.75);text-transform:uppercase;font-weight:600;">Ship Anything. Instantly.</div>
            <div style="font-size:22px;font-weight:800;color:#ffffff;margin-top:3px;letter-spacing:-.02em;">JOYTREE</div>
          </td>
          <td style="vertical-align:middle;text-align:right;">
            <span style="display:inline-block;background:rgba(52,211,153,.15);border:1px solid rgba(52,211,153,.3);color:#6ee7b7;font-size:11px;font-weight:700;letter-spacing:.1em;padding:5px 11px;border-radius:999px;text-transform:uppercase;">New Member</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ HERO IMAGE ══ -->
  ${heroImageUrl ? `
  <tr>
    <td style="background:#052e16;">
      <img src="${heroImageUrl}" alt="JOYTREE — Ship Anything Instantly" width="640" style="display:block;width:100%;max-height:280px;object-fit:cover;">
    </td>
  </tr>` : `
  <tr>
    <td style="background:linear-gradient(180deg,#064e2c,#0a0a0a);padding:32px 32px 36px;text-align:center;">
      <div style="font-size:13px;letter-spacing:.18em;color:#34d399;text-transform:uppercase;font-weight:600;margin-bottom:10px;">Your workspace is live</div>
      <div style="font-size:38px;font-weight:900;color:#ffffff;line-height:1.15;letter-spacing:-.03em;">Ship anything.<br>Instantly.</div>
      <div style="margin-top:14px;font-size:14px;color:rgba(255,255,255,.5);letter-spacing:.04em;">GitHub → Production in minutes</div>
    </td>
  </tr>`}

  <!-- ══ WHITE BODY ══ -->
  <tr>
    <td style="background:#ffffff;padding:40px 36px 0;">

      <!-- Greeting -->
      <p style="margin:0 0 6px;font-size:12px;color:#16a34a;font-weight:700;letter-spacing:.1em;text-transform:uppercase;">Account verified &amp; workspace ready</p>
      <h1 style="margin:0 0 20px;font-size:28px;font-weight:800;color:#0f172a;line-height:1.2;letter-spacing:-.02em;">Welcome to JOYTREE, ${firstName}.</h1>

      <!-- Intro paragraph -->
      <p style="margin:0 0 16px;font-size:15px;line-height:1.9;color:#334155;">
        Your account is fully verified and your JOYTREE workspace is now active. JOYTREE is a professional deployment platform that takes your code from a GitHub repository to a live, publicly accessible URL — in minutes, with zero DevOps configuration required.
      </p>
      <p style="margin:0 0 16px;font-size:15px;line-height:1.9;color:#334155;">
        Whether you're deploying a Node.js API, a static site, a full-stack web app, or any other server-based project, JOYTREE handles the entire release pipeline on your behalf: cloning your code, running your build, provisioning a subdomain, routing traffic, and keeping your deployment live 24/7.
      </p>
      <p style="margin:0 0 28px;font-size:15px;line-height:1.9;color:#334155;">
        Every project you deploy gets its own real-time log terminal, resource monitor, and deployment history. You stay in full control — redeploy, rollback, pause, or delete at any time from your dashboard. No CLI knowledge required. No server management. Just push your code and ship.
      </p>

      <!-- Divider -->
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 30px;">

      <!-- What JOYTREE does — prose block -->
      <p style="margin:0 0 8px;font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;font-weight:700;">How it works</p>
      <p style="margin:0 0 14px;font-size:14px;line-height:1.85;color:#475569;">
        You connect your GitHub account, select a repository, and JOYTREE takes over. It pulls your latest code, installs dependencies, runs your configured build command, and deploys the result to a live subdomain under <strong style="color:#0f172a;">joytree.site</strong>. The entire process is visible in real time through a streaming log terminal — every line of output, from install to boot.
      </p>
      <p style="margin:0 0 28px;font-size:14px;line-height:1.85;color:#475569;">
        Once live, JOYTREE monitors your deployment continuously. If you push new code to GitHub and have auto-deploy enabled, your project is automatically rebuilt and redeployed without any manual action. If something breaks, you can roll back to the last working deployment in a single click — no re-uploading, no reconfiguring.
      </p>

      <!-- Divider -->
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 30px;">

      <!-- Section heading -->
      <p style="margin:0 0 20px;font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;font-weight:700;">Everything included in your account</p>

      <!-- Feature grid: 2 columns -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
        <tr>
          <td style="width:50%;vertical-align:top;padding:0 10px 16px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">⚡</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">GitHub Deployments</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Connect any public or private GitHub repo. JOYTREE auto-detects your stack, runs your build, and deploys to a live URL — one click from your dashboard.</div>
              </td></tr>
            </table>
          </td>
          <td style="width:50%;vertical-align:top;padding:0 0 16px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">📡</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">Real-Time Log Terminal</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Watch every build and runtime event as it happens — from <code style="font-size:12px;background:#f1f5f9;padding:1px 5px;border-radius:4px;">git clone</code> to server boot — with full diagnostic output streamed live.</div>
              </td></tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="width:50%;vertical-align:top;padding:0 10px 16px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">🌐</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">Custom Domains</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Point your own domain to any deployment. JOYTREE handles DNS verification, CNAME routing, and HTTPS automatically — setup takes under two minutes.</div>
              </td></tr>
            </table>
          </td>
          <td style="width:50%;vertical-align:top;padding:0 0 16px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">🔄</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">Instant Rollbacks</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Every deployment is preserved in your history. Roll back to any previous version in one click — no re-uploading, no downtime, no configuration changes needed.</div>
              </td></tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="width:50%;vertical-align:top;padding:0 10px 0 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">🔐</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">Environment Variables</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Define secrets, API keys, and config per project. They are injected securely at build and runtime — never exposed in your repository or logs.</div>
              </td></tr>
            </table>
          </td>
          <td style="width:50%;vertical-align:top;padding:0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:18px;">
              <tr><td>
                <div style="font-size:22px;margin-bottom:8px;">💳</div>
                <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:6px;">Plans &amp; Billing</div>
                <div style="font-size:13px;color:#64748b;line-height:1.7;">Choose a plan that matches your usage. Upgrade, downgrade, or view payment history and invoices at any time from your billing settings.</div>
              </td></tr>
            </table>
          </td>
        </tr>
      </table>

    </td>
  </tr>

  <!-- ══ GETTING STARTED STEPS ══ -->
  <tr>
    <td style="background:#ffffff;padding:32px 36px 0;">
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 28px;">
      <p style="margin:0 0 20px;font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#94a3b8;font-weight:700;">Your first deployment — 3 steps</p>

      <!-- Step 1 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-bottom:20px;">
        <tr>
          <td style="width:36px;vertical-align:top;padding-top:2px;">
            <div style="width:28px;height:28px;background:#052e16;border-radius:50%;text-align:center;line-height:28px;font-size:13px;font-weight:800;color:#4ade80;">1</div>
          </td>
          <td style="vertical-align:top;padding-left:12px;">
            <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:5px;">Connect your GitHub repository</div>
            <div style="font-size:13px;color:#64748b;line-height:1.75;">Open your dashboard and click <strong style="color:#0f172a;">New Deployment</strong>. Authorize JOYTREE to access your GitHub account, then select any repository — public or private. JOYTREE reads your project structure and pre-fills recommended settings for common frameworks including Node.js, React, Next.js, and static sites.</div>
          </td>
        </tr>
      </table>

      <!-- Step 2 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-bottom:20px;">
        <tr>
          <td style="width:36px;vertical-align:top;padding-top:2px;">
            <div style="width:28px;height:28px;background:#052e16;border-radius:50%;text-align:center;line-height:28px;font-size:13px;font-weight:800;color:#4ade80;">2</div>
          </td>
          <td style="vertical-align:top;padding-left:12px;">
            <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:5px;">Configure, set environment variables, and deploy</div>
            <div style="font-size:13px;color:#64748b;line-height:1.75;">Review your build command and output settings, add any required environment variables, and click <strong style="color:#0f172a;">Deploy</strong>. The log terminal opens immediately — you will see every step in real time, from dependency installation through to your server coming online.</div>
          </td>
        </tr>
      </table>

      <!-- Step 3 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
        <tr>
          <td style="width:36px;vertical-align:top;padding-top:2px;">
            <div style="width:28px;height:28px;background:#052e16;border-radius:50%;text-align:center;line-height:28px;font-size:13px;font-weight:800;color:#4ade80;">3</div>
          </td>
          <td style="vertical-align:top;padding-left:12px;">
            <div style="font-size:14px;font-weight:700;color:#0f172a;margin-bottom:5px;">Go live, share your URL, and enable auto-deploy</div>
            <div style="font-size:13px;color:#64748b;line-height:1.75;">When your build succeeds, your project is immediately accessible at a JOYTREE subdomain. Share that link or attach your own custom domain — DNS configuration is guided step by step and takes under two minutes. Turn on auto-deploy in your project settings so that every future push to your selected branch triggers a new deployment automatically, with no manual action required.</div>
          </td>
        </tr>
      </table>

      <!-- Support note -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-top:24px;">
        <tr>
          <td style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:16px 20px;">
            <p style="margin:0;font-size:13px;color:#166534;line-height:1.75;">
              <strong>Need help getting started?</strong> Reply directly to this email and a member of our team will assist you with your first deployment. We also have step-by-step documentation available in your dashboard under the <strong>Docs</strong> section.
            </p>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ CTA BUTTONS ══ -->
  <tr>
    <td style="background:#ffffff;padding:28px 36px 0;">
      <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
        <tr>
          <td style="padding-bottom:12px;">
            <a href="${dashboardUrl}" style="display:block;background:linear-gradient(135deg,#16a34a,#15803d);color:#ffffff;text-decoration:none;padding:15px 24px;border-radius:12px;font-weight:800;font-size:15px;text-align:center;letter-spacing:.01em;">
              Open My Dashboard →
            </a>
          </td>
        </tr>
        <tr>
          <td>
            <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
              <tr>
                <td style="width:50%;padding-right:6px;">
                  <a href="${deployUrl}" style="display:block;background:#0f172a;color:#ffffff;text-decoration:none;padding:12px 16px;border-radius:10px;font-weight:700;font-size:13px;text-align:center;">
                    ⚡ New Deployment
                  </a>
                </td>
                <td style="width:50%;padding-left:6px;">
                  <a href="${docsUrl}" style="display:block;background:#f1f5f9;color:#334155;text-decoration:none;padding:12px 16px;border-radius:10px;font-weight:700;font-size:13px;text-align:center;border:1px solid #e2e8f0;">
                    📖 Documentation
                  </a>
                </td>
              </tr>
            </table>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ DIVIDER + QUICK LINKS ══ -->
  <tr>
    <td style="background:#ffffff;padding:24px 36px 0;">
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 18px;">
      <p style="margin:0;font-size:13px;color:#64748b;line-height:2;">
        <strong style="color:#334155;">Quick links:</strong>&nbsp;&nbsp;
        <a href="${dashboardUrl}" style="color:#16a34a;text-decoration:none;font-weight:600;">Dashboard</a> &nbsp;·&nbsp;
        <a href="${deployUrl}" style="color:#16a34a;text-decoration:none;font-weight:600;">Deploy</a> &nbsp;·&nbsp;
        <a href="${docsUrl}" style="color:#16a34a;text-decoration:none;font-weight:600;">Docs</a> &nbsp;·&nbsp;
        <a href="${supportUrl}" style="color:#16a34a;text-decoration:none;font-weight:600;">Support</a>
      </p>
    </td>
  </tr>

  <!-- ══ FOOTER LOGO REPEAT ══ -->
  <tr>
    <td style="background:#ffffff;padding:32px 36px 0;">
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 28px;">
      <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
        <tr>
          <td style="vertical-align:middle;width:44px;">
            <img src="${logoUrl}" alt="JOYTREE" width="40" height="40" style="width:40px;height:40px;border-radius:10px;display:block;object-fit:cover;">
          </td>
          <td style="vertical-align:middle;padding-left:12px;">
            <div style="font-size:15px;font-weight:800;color:#0f172a;letter-spacing:-.01em;">JOYTREE</div>
            <div style="font-size:12px;color:#94a3b8;margin-top:1px;">Ship Anything. Instantly.</div>
          </td>
          <td style="text-align:right;vertical-align:middle;">
            <a href="${dashboardUrl}" style="font-size:12px;color:#16a34a;text-decoration:none;font-weight:600;">joytree.site →</a>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ DARK FOOTER ══ -->
  <tr>
    <td style="background:#0a0f1a;border-radius:0 0 18px 18px;padding:24px 36px 28px;margin-top:24px;">
      <p style="margin:0 0 10px;font-size:12px;color:rgba(148,163,184,.6);line-height:1.75;text-align:center;">
        This message was sent to <strong style="color:rgba(148,163,184,.85);">${recipient}</strong> because you created a JOYTREE account.<br>
        If you need help with your first deployment, just reply — our team will get back to you.
      </p>
      <p style="margin:0;font-size:11px;color:rgba(148,163,184,.35);text-align:center;letter-spacing:.04em;">
        © ${new Date().getFullYear()} JOYTREE &nbsp;·&nbsp; joytree.site &nbsp;·&nbsp; All rights reserved
      </p>
    </td>
  </tr>

</table>
<!-- End card -->

</td></tr>
</table>
<!-- End outer wrapper -->

</body>
</html>`;

  const payload = { from: RESEND_FROM_EMAIL, to: [recipient], subject, html, text };
  if (RESEND_REPLY_TO_EMAIL) payload.reply_to = RESEND_REPLY_TO_EMAIL;

  const r = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!r.ok) {
    const detail = await r.text().catch(() => '');
    return { ok: false, skipped: false, reason: `resend_http_${r.status}`, detail: detail.slice(0, 300) };
  }
  return { ok: true };
}

async function sendVerificationCodeEmail(email = '', code = '') {
  if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) return;
  const logoUrl = RESEND_LOGO_URL || `https://${BASE_DOMAIN}/logo_optimized.jpg`;
  const dashboardUrl = `https://${BASE_DOMAIN}/dashboard`;
  const year = new Date().getFullYear();

  // Split the 6-digit code into individual characters for styled digit boxes
  const digits = String(code).padStart(6, '0').split('');
  const digitBoxStyle = 'display:inline-block;width:44px;height:54px;line-height:54px;text-align:center;font-size:26px;font-weight:900;color:#0f172a;background:#f8fafc;border:1.5px solid #e2e8f0;border-radius:10px;margin:0 4px;letter-spacing:0;font-family:monospace;';
  const digitBoxesHtml = digits.map(d => `<span style="${digitBoxStyle}">${d}</span>`).join('');

  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>JOYTREE — Verify your email</title>
</head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:'Segoe UI',Arial,sans-serif;color:#0f172a;-webkit-font-smoothing:antialiased;">

<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background:#f0f4f8;padding:36px 12px 48px;">
<tr><td align="center">

<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="max-width:600px;">

  <!-- ══ HEADER ══ -->
  <tr>
    <td style="background:linear-gradient(135deg,#052e16 0%,#064e2c 55%,#0a3d20 100%);border-radius:18px 18px 0 0;padding:26px 32px 24px;">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
        <tr>
          <td style="vertical-align:middle;width:50px;">
            <img src="${logoUrl}" alt="JOYTREE" width="44" height="44" style="width:44px;height:44px;border-radius:11px;display:block;object-fit:cover;border:2px solid rgba(255,255,255,.1);">
          </td>
          <td style="vertical-align:middle;padding-left:13px;">
            <div style="font-size:10px;letter-spacing:.22em;color:rgba(134,239,172,.7);text-transform:uppercase;font-weight:600;">Security alert</div>
            <div style="font-size:20px;font-weight:800;color:#ffffff;margin-top:2px;letter-spacing:-.01em;">JOYTREE</div>
          </td>
          <td style="text-align:right;vertical-align:middle;">
            <span style="display:inline-block;background:rgba(234,179,8,.12);border:1px solid rgba(234,179,8,.3);color:#fde047;font-size:11px;font-weight:700;letter-spacing:.08em;padding:5px 11px;border-radius:999px;text-transform:uppercase;">Action Required</span>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ LOCK ICON BANNER ══ -->
  <tr>
    <td style="background:#ffffff;padding:40px 36px 28px;text-align:center;border-bottom:1px solid #f1f5f9;">
      <!-- Lock icon as table -->
      <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" style="margin:0 auto 20px;">
        <tr>
          <td style="width:64px;height:64px;background:#f0fdf4;border:1.5px solid #bbf7d0;border-radius:16px;text-align:center;line-height:64px;font-size:30px;">
            🔐
          </td>
        </tr>
      </table>
      <h1 style="margin:0 0 10px;font-size:24px;font-weight:800;color:#0f172a;letter-spacing:-.02em;line-height:1.2;">Verify your email address</h1>
      <p style="margin:0;font-size:14px;color:#64748b;line-height:1.75;max-width:400px;display:inline-block;">
        Enter the 6-digit code below to confirm your identity and access your JOYTREE workspace. This code expires in <strong style="color:#0f172a;">10 minutes</strong>.
      </p>
    </td>
  </tr>

  <!-- ══ CODE BLOCK ══ -->
  <tr>
    <td style="background:#ffffff;padding:32px 36px;">

      <p style="margin:0 0 16px;font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;font-weight:700;text-align:center;">Your one-time verification code</p>

      <!-- Individual digit boxes -->
      <div style="text-align:center;margin-bottom:18px;">
        ${digitBoxesHtml}
      </div>

      <!-- Expiry note -->
      <table role="presentation" cellspacing="0" cellpadding="0" border="0" align="center" style="margin:0 auto;">
        <tr>
          <td style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 18px;">
            <p style="margin:0;font-size:12px;color:#92400e;text-align:center;">
              ⏱ &nbsp;This code expires in <strong>10 minutes</strong>. Do not share it with anyone.
            </p>
          </td>
        </tr>
      </table>

      <!-- Divider -->
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:28px 0 24px;">

      <!-- Security tips heading -->
      <p style="margin:0 0 14px;font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#94a3b8;font-weight:700;">🛡️ Security reminders</p>

      <!-- Tip 1 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-bottom:12px;">
        <tr>
          <td style="width:32px;vertical-align:top;padding-top:1px;">
            <div style="width:24px;height:24px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:50%;text-align:center;line-height:24px;font-size:12px;">✓</div>
          </td>
          <td style="vertical-align:top;padding-left:10px;">
            <div style="font-size:13px;font-weight:600;color:#0f172a;margin-bottom:2px;">Never share this code</div>
            <div style="font-size:12px;color:#64748b;line-height:1.6;">No JOYTREE staff will ever ask for your verification code — not by email, chat, or phone.</div>
          </td>
        </tr>
      </table>

      <!-- Tip 2 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="margin-bottom:12px;">
        <tr>
          <td style="width:32px;vertical-align:top;padding-top:1px;">
            <div style="width:24px;height:24px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:50%;text-align:center;line-height:24px;font-size:12px;">✓</div>
          </td>
          <td style="vertical-align:top;padding-left:10px;">
            <div style="font-size:13px;font-weight:600;color:#0f172a;margin-bottom:2px;">Only enter this on joytree.site</div>
            <div style="font-size:12px;color:#64748b;line-height:1.6;">Make sure you're on the official site. Bookmark it to avoid phishing pages.</div>
          </td>
        </tr>
      </table>

      <!-- Tip 3 -->
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
        <tr>
          <td style="width:32px;vertical-align:top;padding-top:1px;">
            <div style="width:24px;height:24px;background:#fff1f2;border:1px solid #fecdd3;border-radius:50%;text-align:center;line-height:24px;font-size:12px;color:#e11d48;">!</div>
          </td>
          <td style="vertical-align:top;padding-left:10px;">
            <div style="font-size:13px;font-weight:600;color:#0f172a;margin-bottom:2px;">Didn't request this?</div>
            <div style="font-size:12px;color:#64748b;line-height:1.6;">If you didn't try to sign in, you can safely ignore this email. No action is needed — your account is secure.</div>
          </td>
        </tr>
      </table>

    </td>
  </tr>

  <!-- ══ FOOTER LOGO ══ -->
  <tr>
    <td style="background:#ffffff;padding:0 36px 28px;">
      <hr style="border:none;border-top:1px solid #e2e8f0;margin:0 0 24px;">
      <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
        <tr>
          <td style="vertical-align:middle;width:40px;">
            <img src="${logoUrl}" alt="JOYTREE" width="36" height="36" style="width:36px;height:36px;border-radius:9px;display:block;object-fit:cover;">
          </td>
          <td style="vertical-align:middle;padding-left:10px;">
            <div style="font-size:14px;font-weight:800;color:#0f172a;">JOYTREE</div>
            <div style="font-size:11px;color:#94a3b8;margin-top:1px;">Ship Anything. Instantly.</div>
          </td>
          <td style="text-align:right;vertical-align:middle;">
            <a href="${dashboardUrl}" style="font-size:12px;color:#16a34a;text-decoration:none;font-weight:600;">joytree.site →</a>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- ══ DARK FOOTER ══ -->
  <tr>
    <td style="background:#0a0f1a;border-radius:0 0 18px 18px;padding:22px 36px 26px;">
      <p style="margin:0 0 8px;font-size:12px;color:rgba(148,163,184,.55);line-height:1.75;text-align:center;">
        This security code was sent to <strong style="color:rgba(148,163,184,.8);">${email}</strong> at your request.<br>
        Codes are single-use and expire automatically after 10 minutes.
      </p>
      <p style="margin:0;font-size:11px;color:rgba(148,163,184,.3);text-align:center;letter-spacing:.04em;">
        © ${year} JOYTREE &nbsp;·&nbsp; joytree.site &nbsp;·&nbsp; All rights reserved
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>

</body>
</html>`;

  await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: RESEND_FROM_EMAIL, to: [email], subject: 'Your JOYTREE verification code', html, text: `Your JOYTREE verification code is ${code}. It expires in 10 minutes. Never share this code with anyone — JOYTREE staff will never ask for it.` })
  }).catch(()=>{});
}
async function issueEmailVerification(user) {
  const pendingToken = createSessionToken();
  const code = generateOtpCode();
  authOtpStore.set(pendingToken, { userId: String(user._id || user.id), code, email: String(user.email || '').toLowerCase(), expiresAt: Date.now() + 10 * 60 * 1000 });
  await sendVerificationCodeEmail(String(user.email || '').toLowerCase(), code);
  return pendingToken;
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

async function verifyTurnstileToken(token, remoteIp='') {
  if (!TURNSTILE_SECRET_KEY) return { enforced: false, success: true, skipped: true };
  const t = String(token || '').trim();
  if (!t) return { enforced: true, success: false, error: 'missing_turnstile_token' };
  const body = new URLSearchParams();
  body.set('secret', TURNSTILE_SECRET_KEY);
  body.set('response', t);
  if (remoteIp) body.set('remoteip', remoteIp);
  const r = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', { method:'POST', body });
  const d = await r.json().catch(() => ({}));
  return { enforced: true, success: !!d.success, details: d };
}

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

app.get('/api/health/storage', requireAuth, async (req, res) => {
  try {
    const mongoConnected = isDbReady();
    const firebaseConfigured = !!FIREBASE_RTDB_URL;
    let firebaseReachable = false;
    if (!mongoConnected && firebaseConfigured) {
      const probe = await readWorkspaceFromFirebase(req.user);
      firebaseReachable = probe !== null;
    }
    res.json({
      ok: true,
      active: mongoConnected ? 'mongodb' : (firebaseConfigured ? 'firebase_rtdb_fallback' : 'local_file_fallback'),
      mongo: { connected: mongoConnected },
      firebase: { configured: firebaseConfigured, reachable: firebaseReachable },
      localFile: { enabled: true, path: LOCAL_AUTH_FILE }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/api/support/message', async (req, res) => {
  try {
    const name = String(req.body?.name || '').trim();
    const email = String(req.body?.email || '').trim().toLowerCase();
    const message = String(req.body?.message || '').trim();
    const page = String(req.body?.page || '').trim();
    if (!name || !email || !message) return res.status(400).json({ error: 'name, email, and message are required' });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return res.status(400).json({ error: 'invalid email' });

    const toEmail = process.env.SUPPORT_TO_EMAIL || 'projectvpn89@gmail.com';
    const smtpHost = String(process.env.SMTP_HOST || '').trim();
    const smtpUser = String(process.env.SMTP_USER || '').trim();
    const smtpPass = String(process.env.SMTP_PASS || '').trim();
    const smtpPort = Number(process.env.SMTP_PORT || 587);
    const smtpSecure = String(process.env.SMTP_SECURE || '').toLowerCase() === 'true' || smtpPort === 465;

    if (!smtpHost || !smtpUser || !smtpPass) {
      return res.status(500).json({ error: 'support messaging not configured on server (missing SMTP env vars)' });
    }
    let nodemailer;
    try { nodemailer = require('nodemailer'); }
    catch (_) { return res.status(500).json({ error: 'support messaging unavailable: nodemailer not installed' }); }

    const transporter = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: smtpSecure,
      auth: { user: smtpUser, pass: smtpPass }
    });

    await transporter.sendMail({
      from: process.env.SUPPORT_FROM_EMAIL || smtpUser,
      to: toEmail,
      replyTo: email,
      subject: `[JOYTREE] New support message from ${name}`,
      text: `From: ${name} <${email}>\nPage: ${page || 'unknown'}\nIP: ${req.ip || 'unknown'}\n\nMessage:\n${message}`
    });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message || 'Could not send message' });
  }
});

app.get('/api/auth/turnstile/config', (req, res) => {
  res.json({ enabled: !!TURNSTILE_SITE_KEY, siteKey: TURNSTILE_SITE_KEY || '' });
});

app.post('/api/auth/firebase', async (req, res) => {
  try {
    const idToken = String(req.body.idToken || '');
    const turnstileToken = String(req.body.turnstileToken || '');
    const verify = await verifyTurnstileToken(turnstileToken, req.ip || '');
    if (verify.enforced && !verify.success) return res.status(400).json({ error: 'Turnstile verification failed' });
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
    const pendingToken = await issueEmailVerification(user);
    res.json({ requiresVerification: true, pendingToken, user: { id: user._id || user.id, email: user.email, name: user.name } });
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
    const pendingToken = await issueEmailVerification(user);
    res.json({ requiresVerification: true, pendingToken, user: { id: user._id || user.id, email: user.email, name: user.name } });
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
    const pendingToken = await issueEmailVerification(user);
    res.json({ requiresVerification: true, pendingToken, user: { id: user._id || user.id, email: user.email, name: user.name } });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/auth/me', requireAuth, async (req, res) => {
  res.json({ user: { id: req.user._id || req.user.id, email: req.user.email, name: req.user.name, githubUsername: req.user.githubUsername, githubAvatarUrl: req.user.githubAvatarUrl || '', firebaseUid: req.user.firebaseUid || '' } });
});

function isRootEmailAdmin(user = {}) {
  const email = String(user?.email || '').trim().toLowerCase();
  return email === JOYTREE_V3_ADMIN_EMAIL;
}

function collectEmailsDeep(input, out = new Set()) {
  if (!input) return out;
  if (typeof input === 'string') {
    const v = input.trim().toLowerCase();
    if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v)) out.add(v);
    return out;
  }
  if (Array.isArray(input)) {
    for (const item of input) collectEmailsDeep(item, out);
    return out;
  }
  if (typeof input === 'object') {
    for (const [k, v] of Object.entries(input)) {
      if (k.toLowerCase().includes('email') && typeof v === 'string') collectEmailsDeep(v, out);
      else collectEmailsDeep(v, out);
    }
  }
  return out;
}

async function collectFirebaseIndexedEmails() {
  if (!FIREBASE_RTDB_URL) return [];
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  const paths = [
    'deployboard_user_emails',
    'deployboard_users',
    'users',
    'deployboard_workspaces'
  ];
  const found = new Set();
  for (const p of paths) {
    try {
      const r = await fetch(`${FIREBASE_RTDB_URL}/${p}.json${authQuery}`, { headers: { Accept: 'application/json' } });
      if (!r.ok) continue;
      const d = await r.json().catch(() => null);
      collectEmailsDeep(d, found);
    } catch (_) {}
  }
  return Array.from(found);
}

// Fetches ALL users from Firebase Authentication using the Identity Toolkit REST API.
// Uses batchGet (downloadAccount) with the FIREBASE_RTDB_SECRET as a legacy admin token,
// paging through results until all users are collected.
async function collectFirebaseAuthEmails() {
  if (!FIREBASE_API_KEY) return [];
  const emails = new Set();
  // We use the Google Identity Toolkit v3 downloadAccount endpoint which requires
  // a Google OAuth2 token. Since we only have the legacy RTDB secret / API key,
  // we use the v1 accounts:batchGet endpoint with the API key for the project.
  // The /v1/projects/{projectId}/accounts:batchGet endpoint requires a service-account
  // credential, which we don't have. Instead, we use the workaround: query
  // /identitytoolkit/v3/relyingparty/downloadAccount with the FIREBASE_RTDB_SECRET
  // as a Bearer token (legacy admin access).
  const base = 'https://www.googleapis.com/identitytoolkit/v3/relyingparty/downloadAccount';
  let nextPageToken = '';
  const maxResults = 500;
  try {
    do {
      const body = { maxResults };
      if (nextPageToken) body.nextPageToken = nextPageToken;
      const r = await fetch(`${base}?key=${encodeURIComponent(FIREBASE_API_KEY)}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(FIREBASE_RTDB_SECRET ? { 'Authorization': `Bearer ${FIREBASE_RTDB_SECRET}` } : {})
        },
        body: JSON.stringify(body)
      });
      if (!r.ok) break;
      const data = await r.json().catch(() => null);
      if (!data) break;
      const users = Array.isArray(data.users) ? data.users : [];
      for (const u of users) {
        const email = String(u?.email || '').trim().toLowerCase();
        if (email) emails.add(email);
        // Also collect emails from providerUserInfo (e.g. GitHub OAuth users)
        if (Array.isArray(u?.providerUserInfo)) {
          for (const p of u.providerUserInfo) {
            const pe = String(p?.email || '').trim().toLowerCase();
            if (pe) emails.add(pe);
          }
        }
      }
      nextPageToken = String(data.nextPageToken || '');
    } while (nextPageToken);
  } catch (e) {
    console.warn('[Firebase Auth] collectFirebaseAuthEmails error:', e.message);
  }
  return Array.from(emails);
}

app.post('/api/admin/emails/broadcast', requireAuth, async (req, res) => {
  try {
    if (!isRootEmailAdmin(req.user)) return res.status(403).json({ error: 'Forbidden' });
    if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) return res.status(500).json({ error: 'Resend is not configured on server' });

    const senderName = String(req.body?.senderName || '').trim();
    const subject = String(req.body?.subject || '').trim();
    const preheader = String(req.body?.preheader || '').trim();
    const intro = String(req.body?.intro || '').trim();
    const message = String(req.body?.message || '').trim();
    if (!senderName || !subject || !message) return res.status(400).json({ error: 'senderName, subject and message are required' });

    const records = isDbReady() ? await User.find({}).select('email').lean() : (localAuth.users || []);
    const dbEmails = records.map(u => String(u?.email || '').trim().toLowerCase()).filter(Boolean);
    // Fetch all users directly from Firebase Authentication (covers ALL signed-up users)
    const firebaseAuthEmails = await collectFirebaseAuthEmails();
    // Also keep RTDB-indexed emails as a fallback/supplement
    const firebaseRtdbEmails = await collectFirebaseIndexedEmails();
    const recipients = Array.from(new Set([...dbEmails, ...firebaseAuthEmails, ...firebaseRtdbEmails]));
    if (!recipients.length) return res.status(400).json({ error: 'No users found to receive email' });

    const logoUrl = 'https://raw.githubusercontent.com/joygood123/Url/refs/heads/main/favicon_256.png';
    const safeMessage = message.replace(/\n/g, '<br>');
    const html = `<!doctype html><html><body style="margin:0;padding:0;background:#f8fafc;font-family:Inter,Arial,sans-serif;color:#0f172a;">
<div style="max-width:640px;margin:24px auto;background:#ffffff;border:1px solid #e2e8f0;border-radius:16px;overflow:hidden;">
<div style="padding:20px 24px;border-bottom:1px solid #e2e8f0;background:#020617;color:#e2e8f0;">
<img src="${logoUrl}" alt="JOYTREE" width="36" height="36" style="border-radius:8px;vertical-align:middle;margin-right:10px;"><strong style="font-size:16px;vertical-align:middle;">JOYTREE</strong>
</div><div style="padding:24px;">
${intro ? `<p style="margin:0 0 14px;font-size:16px;">${intro}</p>` : ''}
<div style="font-size:15px;line-height:1.7;color:#1e293b;">${safeMessage}</div>
<p style="margin:22px 0 0;font-size:14px;color:#475569;">— ${senderName}<br>JOYTREE Team</p>
</div></div>
<div style="max-width:640px;margin:0 auto 20px;padding:0 10px;color:#64748b;font-size:12px;text-align:center;">${preheader || 'You received this update because you are a JOYTREE user.'}</div>
</body></html>`;

    let sent = 0; let failed = 0;
    for (const to of recipients) {
      const r = await fetch('https://api.resend.com/emails', {
        method: 'POST',
        headers: { Authorization: `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ from: RESEND_FROM_EMAIL, to: [to], subject, html, text: `${intro ? intro + '\n\n' : ''}${message}\n\n— ${senderName}\nJOYTREE Team` })
      });
      if (r.ok) sent += 1; else failed += 1;
    }
    res.json({ ok: true, sent, failed, total: recipients.length });
  } catch (e) {
    res.status(500).json({ error: e.message || 'Could not broadcast email' });
  }
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

      // ORPHAN RECOVERY FIX: Some projects may have been created with a temp
      // ownerUserId (like 'u_1234567890' from local-auth.json when MongoDB was
      // down). Now that MongoDB is up and we know the real _id, reassign any
      // projects that reference the old temp id or that have ownerUserId=''.
      // We match by subdomain cross-referenced with Firebase workspace data.
      try {
        const mongoId = String(user._id);
        // Reassign projects whose ownerUserId is blank or a temp u_<timestamp> id
        const orphanFilter = {
          $or: [
            { ownerUserId: '' },
            { ownerUserId: { $regex: /^u_\d+$/ } },
            // Also re-claim if sub-domain matches something in the user's Firebase workspace
          ]
        };
        // Attempt to read firebase workspace so we know which subdomains belong to this user
        const fbWs = await readWorkspaceFromFirebase(user).catch(() => null);
        const wsSubdomains = Array.isArray(fbWs?.projects)
          ? fbWs.projects.map(p => String(p.subdomain || '')).filter(Boolean)
          : [];
        if (wsSubdomains.length > 0) {
          await Project.updateMany(
            { subdomain: { $in: wsSubdomains }, ownerUserId: { $in: ['', ...Array.from({length:1}, () => null)] } },
            { $set: { ownerUserId: mongoId } }
          ).catch(() => {});
          // Also reclaim projects with old temp ids from this user's previous local-auth session
          await Project.updateMany(
            { subdomain: { $in: wsSubdomains }, ownerUserId: { $regex: /^u_\d+/ } },
            { $set: { ownerUserId: mongoId } }
          ).catch(() => {});
        }
      } catch (_) {}
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
    const pendingToken = await issueEmailVerification(user);
    res.json({ requiresVerification: true, pendingToken, user: { id: user._id || user.id, email: user.email, name: user.name, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl || '' }, firebaseLinked });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/auth/verify-email', async (req, res) => {
  try {
    const pendingToken = String(req.body.pendingToken || '');
    const code = String(req.body.code || '').trim();
    const rec = authOtpStore.get(pendingToken);
    if (!rec || rec.expiresAt < Date.now()) return res.status(400).json({ error: 'Verification expired. Request a new code.' });
    if (rec.code !== code) return res.status(400).json({ error: 'Invalid verification code' });
    authOtpStore.delete(pendingToken);
    const token = createSessionToken();
    const expiresAt = new Date(Date.now() + SESSION_TTL_DAYS * 86400000);
    if (isDbReady()) await Session.create({ token, userId: rec.userId, expiresAt });
    else { localAuth.sessions.push({ token, userId: rec.userId, expiresAt }); saveLocalAuth(); }
    const user = isDbReady() ? await User.findById(rec.userId) : localAuth.users.find(u => u.id === rec.userId);

    // STABILITY FIX: After login, check if workspace data exists under the stable
    // email key. If not, try the old firebaseUid key and migrate it automatically.
    if (user) {
      try {
        const existingWs = await readWorkspaceFromFirebase(user);
        if (!existingWs && user.firebaseUid) {
          const oldKey = String(user.firebaseUid).trim().toLowerCase().replace(/[^a-z0-9_-]/g, '_');
          if (oldKey && FIREBASE_RTDB_URL) {
            const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
            const oldUrl = `${FIREBASE_RTDB_URL}/deployboard_workspaces/${oldKey}.json${authQuery}`;
            const r = await fetch(oldUrl, { method: 'GET', headers: { 'Accept': 'application/json' } }).catch(() => null);
            if (r && r.ok) {
              const oldData = await r.json().catch(() => null);
              if (oldData && typeof oldData === 'object') {
                await writeWorkspaceToFirebase(user, oldData).catch(() => {});
                console.log('[Auth] Migrated workspace from old firebaseUid key to email key on login:', user.email);
              }
            }
          }
        }
      } catch (_) {}
    }

    const welcomeResult = await sendWelcomeEmail({ userEmail: user?.email || rec.email, userName: user?.name || '' }).catch((e) => ({ ok:false, reason:e.message || 'send_exception' }));
    if (!welcomeResult?.ok && !welcomeResult?.skipped) console.warn('[Auth] Welcome email not sent:', welcomeResult?.reason || 'unknown');

    res.json({
      token,
      user: {
        id: user?._id || user?.id,
        email: user?.email || rec.email,
        name: user?.name || '',
        githubUsername: user?.githubUsername || '',
        githubAvatarUrl: user?.githubAvatarUrl || '',
        firebaseUid: user?.firebaseUid || ''
      }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


app.get('/api/workspace', requireAuth, async (req, res) => {
  // Firebase is the canonical workspace store for dashboard data persistence
  // across VPS/container restarts.
  let ws = {};
  const fbWs = await readWorkspaceFromFirebase(req.user);
  if (fbWs && typeof fbWs === 'object') {
    ws = fbWs;
  } else {
    // DATA MIGRATION FIX: The user's data may have been written under their
    // firebaseUid key (old behaviour before this fix). Try reading from there
    // and if found, migrate it to the stable email key.
    if (req.user.firebaseUid) {
      const oldKeyUser = { firebaseUid: req.user.firebaseUid }; // forces old key lookup
      // Temporarily build the old-style key
      const oldKey = String(req.user.firebaseUid || '').trim().toLowerCase().replace(/[^a-z0-9_-]/g, '_');
      if (oldKey && FIREBASE_RTDB_URL) {
        try {
          const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
          const oldUrl = `${FIREBASE_RTDB_URL}/deployboard_workspaces/${oldKey}.json${authQuery}`;
          const r = await fetch(oldUrl, { method: 'GET', headers: { 'Accept': 'application/json' } });
          if (r.ok) {
            const oldData = await r.json().catch(() => null);
            if (oldData && typeof oldData === 'object') {
              ws = oldData;
              // Migrate: write under the new stable email key
              await writeWorkspaceToFirebase(req.user, ws).catch(() => {});
              console.log('[Workspace] Migrated workspace from old firebaseUid key to email key for:', req.user.email);
            }
          }
        } catch (_) {}
      }
    }
    // Final fallback to user.workspace from MongoDB
    if (!ws || Object.keys(ws).length === 0) {
      ws = req.user.workspace || {};
    }
  }
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
    // Always write workspace to Firebase so user data survives VPS restarts.
    // Primary write uses email as the key (stable across all rebuilds).
    const fbSaved = await writeWorkspaceToFirebase(req.user, workspace).catch(() => false);
    if (!fbSaved) console.warn('[Workspace] Firebase write failed for:', req.user.email, '— continuing with DB fallback');

    // Keep existing secondary stores as best-effort mirrors.
    if (isDbReady()) {
      await User.updateOne({ _id: req.user._id }, { $set: { workspace, updatedAt: new Date() } });
    } else {
      const user = localAuth.users.find(u => String(u.id) === String(req.user.id));
      if (user) user.workspace = workspace;
      saveLocalAuth();
    }

    res.json({ ok: true, firebaseSynced: fbSaved });
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

function parseGitHubRepoSlug(repoUrl='') {
  const m = String(repoUrl || '').match(/github\.com\/([^/]+)\/([^/#?]+)/i);
  if (!m) return '';
  const owner = String(m[1] || '').trim();
  const repo = String(m[2] || '').trim().replace(/\.git$/i, '');
  return (owner && repo) ? `${owner}/${repo}` : '';
}

async function syncRepoHomepageToLiveUrl(repoUrl='', liveUrl='', githubToken='') {
  const slug = parseGitHubRepoSlug(repoUrl);
  if (!slug || !liveUrl || !githubToken) return { ok:false, skipped:true };
  try {
    const r = await fetch(`https://api.github.com/repos/${slug}`, {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${githubToken}`,
        'User-Agent': 'deployboard',
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ homepage: liveUrl })
    });
    if (!r.ok) return { ok:false, skipped:false, status:r.status };
    return { ok:true, slug, homepage: liveUrl };
  } catch (_) {
    return { ok:false, skipped:false };
  }
}


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
    const mongoUserId = String(req.user?._id || req.user?.id || '');

    let filter = mineOnly ? { ownerUserId: mongoUserId } : {};
    if (!isDbReady()) {
      const ws = (await readWorkspaceFromFirebase(req.user || {})) || {};
      const projects = Array.isArray(ws.projects) ? ws.projects : [];
      return res.json(projects.map(p => ({ ...p, id: String(p.id || p._id || '') })));
    }
    let projects = await Project.find(filter).sort({ createdAt: -1 });

    // ORPHAN RECOVERY FIX: If the user has few or no projects in MongoDB under their current _id,
    // also search via Firebase workspace to find subdomains that belong to them,
    // then surface any MongoDB projects by those subdomains (even if ownerUserId doesn't match).
    if (mineOnly && projects.length === 0) {
      try {
        const fbWs = await readWorkspaceFromFirebase(req.user);
        const wsSubdomains = Array.isArray(fbWs?.projects)
          ? fbWs.projects.map(p => String(p.subdomain || '')).filter(Boolean)
          : [];
        if (wsSubdomains.length > 0) {
          const orphaned = await Project.find({ subdomain: { $in: wsSubdomains } }).sort({ createdAt: -1 });
          if (orphaned.length > 0) {
            // Reclaim these orphans by updating their ownerUserId
            await Project.updateMany(
              { subdomain: { $in: wsSubdomains } },
              { $set: { ownerUserId: mongoUserId } }
            ).catch(() => {});
            projects = orphaned;
          }
        }
      } catch (_) {}
    }

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

app.get('/api/projects/check-availability', async (req, res) => {
  try {
    const rawName = String(req.query.name || '').trim();
    const rawSubdomain = String(req.query.subdomain || '').trim();
    const rawProjectId = String(req.query.projectId || '').trim();
    const cleanSubdomain = rawSubdomain.toLowerCase()
      .replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');

    if (!rawName && !cleanSubdomain) {
      return res.status(400).json({ error: 'name or subdomain query is required' });
    }

    const query = [];
    if (rawName) query.push({ name: rawName });
    if (cleanSubdomain) query.push({ subdomain: cleanSubdomain });

    const existing = await Project.findOne({ $or: query }).select('_id name subdomain').lean().maxTimeMS(5000);
    const sameProject = !!(existing && rawProjectId && String(existing._id) === rawProjectId);
    res.json({
      name: rawName,
      subdomain: cleanSubdomain,
      nameAvailable: rawName ? (sameProject ? true : !(existing && existing.name === rawName)) : null,
      subdomainAvailable: cleanSubdomain ? (sameProject ? true : !(existing && existing.subdomain === cleanSubdomain)) : null,
      existing: existing || null
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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
async function resolveEnvProject(req) {
  const projectId = String(req.params.id || '').trim();
  if (!projectId) return { project: null, source: null };
  if (mongoose.Types.ObjectId.isValid(projectId)) {
    const project = await Project.findById(projectId);
    if (project) return { project, source: 'db' };
  }

  // Prefer live workspace from Firebase so env updates done by other endpoints are immediately visible
  const liveWs = (await readWorkspaceFromFirebase(req.user)) || {};
  const workspaceProjects = Array.isArray(liveWs.projects) ? liveWs.projects : (Array.isArray(req.user?.workspace?.projects) ? req.user.workspace.projects : []);
  const project = workspaceProjects.find(p =>
    String(p.id || p._id || '') === projectId ||
    String(p.subdomain || '') === projectId ||
    String(p.name || '') === projectId
  );
  if (project) return { project, source: 'workspace' };
  return { project: null, source: null };
}

app.get('/api/projects/:id/env', attachAuthIfPresent, async (req, res) => {
  try {
    const { project: p } = await resolveEnvProject(req);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    const vars = p.envVars instanceof Map ? Object.fromEntries(p.envVars) : (p.envVars || {});
    res.json(vars);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/projects/:id/env', requireAuth, async (req, res) => {
  try {
    const { key, value } = req.body;
    if (!key) return res.status(400).json({ error: 'key required' });
    const { project: p, source } = await resolveEnvProject(req);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    if (!p.envVars) p.envVars = {};
    p.envVars[key] = value || '';
    if (source === 'db') {
      p.markModified('envVars');
      p.updatedAt = new Date();
      await p.save();
    } else {
      updateLocalWorkspaceProject(req.user, String(p.id || p._id || req.params.id), { envVars: p.envVars });
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/projects/:id/env/:key', requireAuth, async (req, res) => {
  try {
    const { project: p, source } = await resolveEnvProject(req);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    if (p.envVars) {
      delete p.envVars[req.params.key];
      if (source === 'db') {
        p.markModified('envVars');
        p.updatedAt = new Date();
        await p.save();
      } else {
        updateLocalWorkspaceProject(req.user, String(p.id || p._id || req.params.id), { envVars: p.envVars });
      }
    }
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Bulk replace all env vars for a project (used by Project Detail editor) ──
app.put('/api/projects/:id/env', requireAuth, async (req, res) => {
  try {
    const vars = req.body && typeof req.body === 'object' ? req.body : {};
    const { project: p, source } = await resolveEnvProject(req);
    if (!p) return res.status(404).json({ error: 'Project not found' });
    // Replace entire envVars map
    p.envVars = vars;
    if (source === 'db') {
      p.markModified('envVars');
      p.updatedAt = new Date();
      await p.save();
    } else {
      updateLocalWorkspaceProject(req.user, String(p.id || p._id || req.params.id), { envVars: p.envVars });
    }
    res.json({ ok: true, count: Object.keys(vars).length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Activity API (server-side log) ────────────────────────────────────────────

const DB_PUBLIC_BASE_DOMAIN = process.env.DB_PUBLIC_BASE_DOMAIN || process.env.BASE_DOMAIN || '';
const DB_GATEWAY_CONFIG_PATH = process.env.DB_GATEWAY_CONFIG_PATH || '/etc/haproxy/deployboard-db-gateway.cfg';

function publicDbHost(db){
  const base = String(DB_PUBLIC_BASE_DOMAIN || '').trim();
  if (!base) return '';
  const id = String(db.id || db._id || '').slice(-12);
  return `db-${id}.${base}`;
}
function externalDbConnStr(db){
  const host = publicDbHost(db);
  if (!host) return '';
  const user = encodeURIComponent(String(db.user || (db.engine==='postgres'?'postgres':'root')));
  const pass = encodeURIComponent(String(db.pass || ''));
  const name = encodeURIComponent(String(db.dbName || 'mydb'));
  switch (db.engine){
    case 'postgres': return `postgresql://${user}:${pass}@${host}:5432/${name}`;
    case 'mysql': return `mysql://${user}:${pass}@${host}:3306/${name}`;
    case 'mariadb': return `mariadb://${user}:${pass}@${host}:3306/${name}`;
    case 'mongodb': return `mongodb://${user}:${pass}@${host}:27017/${name}`;
    case 'redis': return pass ? `redis://:${pass}@${host}:6379` : `redis://${host}:6379`;
    default: return '';
  }
}
function writeDbGatewayConfig(dbs = []){
  try {
    const lines = ['# Autogenerated by DeployBoard DB gateway'];
    for (const db of dbs){
      if (!db?.containerName || !db?.internalPort || !publicDbHost(db)) continue;
      const bindPort = Number(db.engine==='postgres'?5432:db.engine==='mongodb'?27017:db.engine==='redis'?6379:3306);
      const backendPort = Number(db.internalPort);
      const name = String(db.id || db._id || '').replace(/[^a-zA-Z0-9_-]/g,'');
      lines.push(`frontend ft_${name}`);
      lines.push(`  bind *:${bindPort}`);
      lines.push(`  mode tcp`);
      lines.push(`  use_backend bk_${name} if { req.ssl_sni -i ${publicDbHost(db)} }`);
      lines.push(`backend bk_${name}`);
      lines.push(`  mode tcp`);
      lines.push(`  server s1 127.0.0.1:${backendPort}`);
    }
    fs.writeFileSync(DB_GATEWAY_CONFIG_PATH, lines.join('\n') + '\n', 'utf8');
  } catch (e){ console.warn('[DB Gateway] could not write config:', e.message); }
}
const activityLog = [];
function addActivity(type, message) {
  activityLog.unshift({ type, message, time: new Date().toISOString() });
  if (activityLog.length > 500) activityLog.pop();
}

// ── LogiFlow: Visual Backend Simulator (developer prototype) ─────────────────
const FLOW_FILE = path.join(__dirname, 'database_storage.json');
const flowRegistry = new Map();     // flowId -> flowDefinition
const executionLogs = [];           // most-recent-first
const rateLimiterState = new Map(); // key(flowId:ip) -> { count, windowStart }
let virtualDatabase = {};           // { [flowId]: { [collection]: docs[] } }
const APIS_FILE = path.join(__dirname, 'api_catalog.json');
let apiCatalog = [];
try { if (fs.existsSync(APIS_FILE)) apiCatalog = JSON.parse(fs.readFileSync(APIS_FILE, 'utf8')) || []; } catch {}
function saveApiCatalog(){ try { fs.writeFileSync(APIS_FILE, JSON.stringify(apiCatalog, null, 2)); } catch {} }

// ── Restore flowRegistry from apiCatalog on startup ───────────────────────────
// Without this, every VPS restart wipes the in-memory flowRegistry and all
// /api/live/<flowId> calls return "Flow not found" until flows are re-deployed.
(function restoreFlowRegistryFromCatalog() {
  let restored = 0;
  for (const rec of apiCatalog) {
    if (!rec || !rec.flowId) continue;
    if (flowRegistry.has(rec.flowId)) continue; // already present (shouldn't happen at boot)
    // Reconstruct minimal nodes from stored metadata so the live endpoint keeps working
    const isQuiz = !!rec.quizSeeded;
    const aiTemplate = rec.responseTemplate || { ok: true, flowId: rec.flowId, message: 'API is live' };
    const httpMethod = String(rec.httpMethod || 'GET').toUpperCase();
    const routePath  = String(rec.routePath || '/api');
    const nodes = [
      { id:'n1', type:'INCOMING_REQUEST', config:{ method: httpMethod, routePath }, next:'n2' },
      { id:'n2', type:'DB_INSERT',  config:{ collection: isQuiz ? 'quiz_submissions' : 'requests', source:'req.body' }, next:'n3' },
      { id:'n3', type:'DB_FIND',   config:{ collection: isQuiz ? 'quiz_questions'   : 'requests', filters: [] }, next:'n4' },
      { id:'n4', type:'HTTP_RESPONSE', config:{ status:200, json: aiTemplate } }
    ];
    flowRegistry.set(rec.flowId, {
      flowId:   rec.flowId,
      owner:    rec.ownerUserId || '',
      nodes,
      createdAt: rec.createdAt || new Date().toISOString(),
      prompt:   rec.prompt || '',
      aiTemplate,
      aiDataSeed: Array.isArray(rec.dataSeed) ? rec.dataSeed : [],
      isQuiz
    });
    restored++;
  }
  if (restored > 0) console.log(`[FlowRegistry] Restored ${restored} flows from api_catalog.json`);
})();
function firebaseApisBaseUrl(user){
  const key = firebaseWorkspaceKey(user);
  if (!FIREBASE_RTDB_URL || !key) return '';
  return `${FIREBASE_RTDB_URL}/deployboard_apis/${key}`;
}
async function writeApiToFirebase(user, rec){
  try {
    const base = firebaseApisBaseUrl(user); if (!base) return false;
    const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${base}/${encodeURIComponent(String(rec.flowId||''))}.json${authQuery}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(rec) });
    return r.ok;
  } catch { return false; }
}
async function readApisFromFirebase(user){
  try {
    const base = firebaseApisBaseUrl(user); if (!base) return [];
    const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${base}.json${authQuery}`, { headers:{Accept:'application/json'} });
    if (!r.ok) return [];
    const d = await r.json();
    return d && typeof d === 'object' ? Object.values(d).filter(Boolean) : [];
  } catch { return []; }
}
try {
  if (fs.existsSync(FLOW_FILE)) {
    const raw = JSON.parse(fs.readFileSync(FLOW_FILE, 'utf8'));
    if (raw && typeof raw === 'object') virtualDatabase = raw;
  }
} catch {}
function saveVirtualDb() {
  try { fs.writeFileSync(FLOW_FILE, JSON.stringify(virtualDatabase, null, 2)); } catch {}
}
function getFlowDb(flowId) {
  if (!virtualDatabase[flowId]) virtualDatabase[flowId] = {};
  return virtualDatabase[flowId];
}
function getCollection(flowId, name) {
  const fdb = getFlowDb(flowId);
  if (!fdb[name]) fdb[name] = [];
  return fdb[name];
}
function logExec(entry) {
  executionLogs.unshift({ timestamp: new Date().toISOString(), ...entry });
  if (executionLogs.length > 2000) executionLogs.length = 2000;
}
function deepGet(obj, pathStr = '') {
  return String(pathStr).split('.').reduce((acc, k) => (acc && typeof acc === 'object') ? acc[k] : undefined, obj);
}
function resolveTemplate(str, scope) {
  return String(str || '').replace(/\{\{\s*([^}]+)\s*\}\}/g, (_, key) => {
    const v = deepGet(scope, key.trim());
    return v == null ? '' : String(v);
  });
}
function evalCond(actual, op, expected) {
  switch (op) {
    case '$eq': return String(actual) === String(expected);
    case '$gt': return Number(actual) > Number(expected);
    case '$lt': return Number(actual) < Number(expected);
    case '$contains': return String(actual || '').includes(String(expected || ''));
    default: return false;
  }
}
const AI_FLOW_INSTRUCTION = `You are a JSON API flow generator. Return ONLY valid JSON, no markdown, no explanation, no code fences.
Shape: {"routePath":"/api","method":"POST","responseTemplate":{"ok":true},"seedQuestions":[],"dataSeed":[]}
Rules:
- responseTemplate must directly satisfy the user's requested API behavior with realistic example data.
- For quiz/riddle/question APIs: include seedQuestions as [{question,answer}] with 3-5 real examples only (keep it short).
- For list/fact/data APIs: include dataSeed as array of 3-5 relevant example objects only (keep it short).
- For any other API: fill responseTemplate with meaningful example output matching the request.
- Keep dataSeed and seedQuestions concise — 3 to 5 items maximum. Do not generate large arrays.
- Return ONLY the JSON object, nothing else.`;

// Instruction for chunk-only generation (questions/riddles only — no wrapper JSON needed)
const AI_CHUNK_INSTRUCTION = `You are a question/riddle generator. Return ONLY a valid JSON array, no markdown, no explanation, no code fences.
Shape: [{"question":"...","answer":"..."},...]
Rules:
- Generate EXACTLY the number of items requested.
- Every item must have a non-empty "question" and "answer" field.
- Make questions unique — do not repeat any question already listed in the EXISTING list provided.
- Return ONLY the JSON array, nothing else. No wrapper object.`;

// Detect how many questions the user is asking for
function detectRequestedCount(prompt = '') {
  const m = String(prompt).match(/\b(\d{1,4})\s*(riddle|question|quiz|mcq|item|fact|joke)s?\b/i)
         || String(prompt).match(/\bgenerate\s+(\d{1,4})\b/i)
         || String(prompt).match(/\b(\d{1,4})\s+(?:unique|different|random|hard|easy|medium)\b/i);
  return m ? Math.min(Number(m[1]), 2000) : 0;
}


function wantsWebSearch(text = '') {
  return /\b(web\s*search|search\s+the\s+web|google|latest|today|current|news|recent|202[5-9]|up\s*to\s*date)\b/i.test(String(text || ''));
}

async function fetchWebSearchContext(query = '') {
  if (!JOYTREE_WEB_SEARCH_ENABLED) return '';
  const q = String(query || '').replace(/https?:\/\/[^\s)]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 500);
  if (!q || !wantsWebSearch(q)) return '';
  try {
    if (JOYTREE_WEB_SEARCH_URL) {
      const url = `${JOYTREE_WEB_SEARCH_URL}/search?q=${encodeURIComponent(q)}&format=json`;
      const r = await fetch(url, { headers: { Accept: 'application/json', 'User-Agent': 'JoytreeAPI/3.0' }, signal: AbortSignal.timeout(12000) });
      if (r.ok) {
        const d = await r.json().catch(() => ({}));
        const results = Array.isArray(d.results) ? d.results : [];
        const lines = results.slice(0, 6).map((item, i) => {
          const title = String(item.title || '').trim();
          const url = String(item.url || item.pretty_url || '').trim();
          const snippet = String(item.content || item.snippet || '').replace(/\s+/g, ' ').trim();
          return `${i + 1}. ${title}${url ? ` (${url})` : ''}${snippet ? ` — ${snippet}` : ''}`;
        }).filter(Boolean);
        if (lines.length) return `WEB_SEARCH_RESULTS for "${q}":\n${lines.join('\n')}`;
      }
    }
  } catch (e) {
    console.warn('[Joytree Web Search] SearXNG search failed:', e.message);
  }
  try {
    const r = await fetch(`https://api.duckduckgo.com/?q=${encodeURIComponent(q)}&format=json&no_html=1&skip_disambig=1`, { headers: { Accept: 'application/json', 'User-Agent': 'JoytreeAPI/3.0' }, signal: AbortSignal.timeout(12000) });
    if (!r.ok) return '';
    const d = await r.json().catch(() => ({}));
    const parts = [];
    if (d.AbstractText) parts.push(`Summary: ${String(d.AbstractText).slice(0, 1200)}`);
    if (Array.isArray(d.RelatedTopics)) {
      for (const t of d.RelatedTopics.slice(0, 6)) {
        if (t.Text) parts.push(`- ${String(t.Text).slice(0, 500)}${t.FirstURL ? ` (${t.FirstURL})` : ''}`);
      }
    }
    return parts.length ? `WEB_SEARCH_RESULTS for "${q}":\n${parts.join('\n')}` : '';
  } catch (e) {
    console.warn('[Joytree Web Search] DuckDuckGo fallback failed:', e.message);
    return '';
  }
}

async function callOpenAICompatibleChat({ apiUrl, apiKey = '', model, messages, maxTokens = 1200, timeoutMs = 120000, temperature = 0.3, tokenField = 'max_tokens' } = {}) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const headers = { 'Content-Type': 'application/json' };
    if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
    const body = { model, messages, temperature, top_p: 1, stream: false };
    body[tokenField] = maxTokens;
    const r = await fetch(apiUrl, { method: 'POST', signal: controller.signal, headers, body: JSON.stringify(body) });
    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      const errJson = (() => { try { return JSON.parse(errText); } catch { return null; } })();
      const msg = errJson?.error?.message || errText.slice(0, 220) || `HTTP ${r.status}`;
      throw new Error(`AI HTTP ${r.status}: ${msg}`);
    }
    const data = await r.json().catch(() => ({}));
    return String(data?.choices?.[0]?.message?.content || data?.content || '');
  } finally {
    clearTimeout(timeoutId);
  }
}

// Cerebras strict requirements:
//   - Do NOT send top_p alongside temperature (causes 400 error)
//   - Do NOT send stream: false explicitly (causes 400 error)
//   - temperature must be between 0 and 1.5 (not 0-2 like OpenAI)
async function callCerebrasChat({ messages, maxTokens = CEREBRAS_FLOW_MAX_TOKENS, timeoutMs = CEREBRAS_FLOW_TIMEOUT_MS, temperature = 0.3 } = {}) {
  if (!CEREBRAS_API_KEY) throw new Error('CEREBRAS_API_KEY is not configured.');
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const body = {
      model: CEREBRAS_MODEL,
      messages: messages,
      max_tokens: maxTokens,
      temperature: Math.min(1.5, Math.max(0, temperature))
    };
    const r = await fetch('https://api.cerebras.ai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + CEREBRAS_API_KEY
      },
      body: JSON.stringify(body)
    });
    if (!r.ok) {
      const errText = await r.text().catch(function(){ return ''; });
      const errJson = (function(){ try { return JSON.parse(errText); } catch(x){ return null; } })();
      const msg = (errJson && (errJson.error && errJson.error.message || errJson.message)) || errText.slice(0, 300) || ('HTTP ' + r.status);
      console.error('[Cerebras] API error', r.status, ':', msg);
      // If model not found, retry once with fallback model
      if ((r.status === 404 || r.status === 400) && body.model !== CEREBRAS_MODEL_FALLBACK) {
        console.warn('[Cerebras] Model', body.model, 'failed, retrying with', CEREBRAS_MODEL_FALLBACK);
        body.model = CEREBRAS_MODEL_FALLBACK;
        const r2 = await fetch('https://api.cerebras.ai/v1/chat/completions', {
          method: 'POST',
          signal: controller.signal,
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + CEREBRAS_API_KEY },
          body: JSON.stringify(body)
        });
        if (!r2.ok) {
          const e2 = await r2.text().catch(function(){ return ''; });
          const j2 = (function(){ try { return JSON.parse(e2); } catch { return null; } })();
          throw new Error('Cerebras HTTP ' + r2.status + ': ' + ((j2 && (j2.error && j2.error.message || j2.message)) || e2.slice(0, 200)));
        }
        const d2 = await r2.json().catch(function(){ return {}; });
        const c2 = String((d2 && d2.choices && d2.choices[0] && d2.choices[0].message && d2.choices[0].message.content) || '');
        if (!c2) throw new Error('Cerebras fallback model returned empty content.');
        return c2;
      }
      throw new Error('Cerebras HTTP ' + r.status + ': ' + msg);
    }
    const data = await r.json().catch(function(){ return {}; });
    const content = String((data && data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content) || '');
    if (!content) throw new Error('Cerebras returned empty content. Check model name and API key.');
    return content;
  } catch (e) {
    if (e.name === 'AbortError') throw new Error('Cerebras timed out after ' + Math.round(timeoutMs / 1000) + 's.');
    throw e;
  } finally {
    clearTimeout(timeoutId);
  }
}

// Robust JSON array extractor — handles markdown fences, trailing commas, truncated arrays
function extractChunkArray(text) {
  if (!text) return null;
  // Strip markdown code fences
  let t = text.replace(/```(?:json)?\s*/gi, '').replace(/```\s*/g, '').trim();
  // Find outermost [ ... ]
  const s = t.indexOf('[');
  let e = t.lastIndexOf(']');
  if (s === -1) return null;
  // If no closing bracket, try to close it ourselves (truncated response)
  if (e === -1 || e <= s) {
    t = t.slice(s);
    // Count unclosed objects and close them
    let depth = 0, inStr = false, esc = false;
    for (let i = 0; i < t.length; i++) {
      const c = t[i];
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (c === '"') { inStr = !inStr; continue; }
      if (inStr) continue;
      if (c === '{') depth++;
      if (c === '}') depth--;
    }
    // Close any unclosed last object then close array
    if (depth > 0) t = t + '}'.repeat(depth);
    // Remove trailing comma before closing
    t = t.replace(/,\s*$/, '') + ']';
    e = t.length - 1;
  } else {
    t = t.slice(s, e + 1);
  }
  // Remove trailing commas before ] or } (common AI mistake)
  t = t.replace(/,\s*([}\]])/g, '$1');
  try {
    const arr = JSON.parse(t);
    if (Array.isArray(arr)) return arr;
  } catch(e1) {
    // Last resort: try to extract individual objects with regex
    const matches = [];
    const objRe = /\{[^{}]*"question"[^{}]*"answer"[^{}]*\}/g;
    let m;
    while ((m = objRe.exec(text)) !== null) {
      try { matches.push(JSON.parse(m[0])); } catch {}
    }
    if (matches.length > 0) return matches;
  }
  return null;
}

// Call AI to generate one chunk of questions and return the array
async function generateQuestionChunk({ provider = 'groq', topic = '', count = 25, offset = 0, existing = [] } = {}) {
  const existingSample = existing.slice(-10).map(function(q){ return q.question; }).join('\n');
  const userMsg = 'Topic/context: ' + topic + '\n\nGenerate EXACTLY ' + count + ' items (items ' + (offset + 1) + ' to ' + (offset + count) + ').\n\n' +
    (existingSample ? 'EXISTING (do not repeat):\n' + existingSample + '\n' : '') +
    'Return ONLY a valid JSON array of {question, answer} objects. No explanation, no markdown, no code fences.';

  // Generic hardened fetch for Groq / xAI
  const callAI = async function(apiUrl, apiKey, model, maxTokens, timeoutMs) {
    const controller = new AbortController();
    const tid = setTimeout(function(){ controller.abort(); }, timeoutMs);
    try {
      const r = await fetch(apiUrl, {
        method: 'POST',
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + (apiKey || '') },
        body: JSON.stringify({
          model: model,
          messages: [
            { role: 'system', content: AI_CHUNK_INSTRUCTION },
            { role: 'user', content: userMsg }
          ],
          temperature: 0.7,
          max_tokens: maxTokens,
          top_p: 1,
          stream: false
        })
      });
      if (!r.ok) {
        const t = await r.text().catch(function(){ return ''; });
        const j = (function(){ try { return JSON.parse(t); } catch { return null; } })();
        const msg = (j && (j.error && j.error.message || j.message)) || t.slice(0, 200) || ('HTTP ' + r.status);
        console.error('[AI chunk] ' + model + ' HTTP ' + r.status + ':', msg);
        throw new Error('AI HTTP ' + r.status + ': ' + msg);
      }
      const data = await r.json().catch(function(){ return {}; });
      const text = String((data && data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content) || '');
      const arr = extractChunkArray(text);
      if (!arr || arr.length === 0) {
        console.error('[AI chunk] ' + model + ' returned unparseable response:', text.slice(0, 300));
        throw new Error('AI returned unparseable response for this chunk. Raw: ' + text.slice(0, 120));
      }
      return arr;
    } catch(e) {
      if (e.name === 'AbortError') throw new Error('AI timed out after ' + Math.round(timeoutMs / 1000) + 's on chunk.');
      throw e;
    } finally {
      clearTimeout(tid);
    }
  };

  if (provider === 'v4cascade') {
    // v4: use the Groq/xAI cascade pool with silent failover
    const result = await callV4Chunk([
      { role: 'system', content: AI_CHUNK_INSTRUCTION },
      { role: 'user', content: userMsg }
    ]);
    const arr = extractChunkArray(result.text);
    if (!arr || arr.length === 0) {
      console.error('[v4 chunk] unparseable response from', result.provider, ':', (result.text || '').slice(0, 300));
      throw new Error('v4 AI returned unparseable response. Raw: ' + (result.text || '').slice(0, 120));
    }
    return arr;
  }

  if (provider === 'cerebras') {
    let text;
    try {
      text = await callCerebrasChat({
        messages: [
          { role: 'system', content: AI_CHUNK_INSTRUCTION },
          { role: 'user', content: userMsg }
        ],
        temperature: 0.7,
        maxTokens: CEREBRAS_CHUNK_MAX_TOKENS,
        timeoutMs: CEREBRAS_CHUNK_TIMEOUT_MS
      });
    } catch(e) {
      console.error('[Cerebras chunk] call failed:', e.message);
      throw e;
    }
    const arr = extractChunkArray(text);
    if (!arr || arr.length === 0) {
      console.error('[Cerebras chunk] unparseable response:', (text || '').slice(0, 300));
      throw new Error('Cerebras returned unparseable response. Raw: ' + (text || '').slice(0, 120));
    }
    return arr;
  }
  if (provider === 'xai' && XAI_API_KEY) {
    return callAI('https://api.x.ai/v1/chat/completions', XAI_API_KEY, XAI_MODEL, 2000, 120000);
  }
  if (GROQ_API_KEY) {
    return callAI('https://api.groq.com/openai/v1/chat/completions', GROQ_API_KEY, GROQ_MODEL, 2000, 90000);
  }
  throw new Error('No AI provider available for chunked generation.');
}

// Main chunked orchestrator — splits large requests into CHUNK_SIZE batches, merges results
const CHUNK_SIZE = 25; // items per AI call for all providers

// 2-minute cooldown between every chunk for ALL providers (rate-limit recovery)
const INTER_CHUNK_DELAY_MS = 65000; // 65 seconds (60s rolling window + 5s buffer)

// Status messages cycled during the wait so users know it is still working
const CHUNK_WAIT_MESSAGES = [
  'Starting container...',
  'Writing API code...',
  'Building your API...',
  'Compiling questions...',
  'Organising responses...',
  'Calibrating AI outputs...',
  'Assembling data...',
  'Polishing answers...',
  'Packaging results...',
  'Almost there...',
  'Warming up next batch...',
  'Processing your request...',
  'Generating more items...',
  'Crafting unique questions...',
  'Checking for duplicates...'
];

// Max retries per chunk before skipping it
const CHUNK_MAX_RETRIES = 5;

async function buildQuestionsInChunks({ prompt = '', total = 0, aiVersion = 'v1', socketId = null } = {}) {
  const allQuestions = [];
  const chunkSize = CHUNK_SIZE;
  const totalChunks = Math.ceil(total / chunkSize);
  const topic = prompt;
  let waitMsgIdx = 0;

  const emitProgress = (done, chunkNum, message) => {
    if (!socketId) return;
    try {
      const pct = total > 0 ? Math.round((done / total) * 100) : 0;
      io.to(socketId).emit('ai:chunk:progress', {
        done, total, chunkNum, totalChunks, pct,
        message: message || (done >= total
          ? '\u2713 All ' + total + ' items generated!'
          : 'Generating batch ' + chunkNum + '/' + totalChunks + ' \u2014 ' + done + ' of ' + total + ' done...')
      });
    } catch (_) {}
  };

  // Wait gapMs while emitting live countdown messages every 5 seconds
  // Strip AI provider/model names before showing to user
  const sanitiseLabel = (s) => (s || '').replace(/\b(groq|xai|cerebras|grok|llama|openai|gsk-[\S]+|xai-[\S]+|csk-[\S]+)\b/gi, 'AI');

  const waitWithCountdown = async (gapMs, label) => {
    label = sanitiseLabel(label || '');
    const stepMs = 5000;
    const steps = Math.ceil(gapMs / stepMs);
    for (let step = 0; step < steps; step++) {
      const elapsed = step * stepMs;
      const remaining = Math.max(0, Math.round((gapMs - elapsed) / 1000));
      const waitMsg = CHUNK_WAIT_MESSAGES[waitMsgIdx % CHUNK_WAIT_MESSAGES.length];
      waitMsgIdx++;
      emitProgress(
        allQuestions.length,
        Math.max(1, Math.floor(allQuestions.length / chunkSize)),
        waitMsg + (label ? ' — ' + label : '') + ' (' + remaining + 's until next batch · ' + allQuestions.length + '/' + total + ' done)'
      );
      await new Promise(function(r){ setTimeout(r, Math.min(stepMs, gapMs - elapsed)); });
    }
  };

  // Emit a non-fatal warning hint to the socket (never kills the job)
  const emitHint = (errMsg) => {
    if (!socketId) return;
    try {
      const safe = sanitiseLabel(String(errMsg || 'Temporary error'));
      io.to(socketId).emit('ai:chunk:progress', {
        done: allQuestions.length, total, chunkNum: 0, totalChunks,
        message: '⚠️ ' + safe + ' — waiting and retrying automatically...'
      });
    } catch(_) {}
  };


  emitProgress(0, 0, 'Starting \u2014 generating ' + total + ' items in ' + totalChunks + ' batches...');

  for (let chunk = 0; chunk < totalChunks; chunk++) {
    const offset = chunk * chunkSize;
    const count = Math.min(chunkSize, total - offset);

    // v4 = Groq+xAI alternating cascade; v3 = Cerebras; v2 = Groq+xAI fallback; v1 = Groq only
    const providers = aiVersion === 'v4'
      ? ['v4cascade']
      : (aiVersion === 'v3'
        ? ['cerebras']
        : (aiVersion === 'v2' && XAI_API_KEY ? ['groq', 'xai'] : ['groq']));

    let chunkItems = null;
    let lastErr = null;
    let chunkSucceeded = false;

    // Try this chunk up to CHUNK_MAX_RETRIES times with 2-min waits between attempts
    for (let attempt = 0; attempt < CHUNK_MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        console.warn('[Chunker] chunk ' + (chunk + 1) + '/' + totalChunks + ' attempt ' + attempt + ' failed (' + (lastErr && lastErr.message) + '). Waiting 2 min before retry...');
        emitHint((lastErr && lastErr.message) || 'Batch failed, retrying...');
        await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'retrying batch ' + (chunk + 1) + ' (attempt ' + (attempt + 1) + '/' + CHUNK_MAX_RETRIES + ')');
      }

      for (let pi = 0; pi < providers.length; pi++) {
        const provider = providers[pi];
        try {
          emitProgress(allQuestions.length, chunk + 1, 'Generating batch ' + (chunk + 1) + '/' + totalChunks + ' \u2014 ' + allQuestions.length + '/' + total + ' done...');
          chunkItems = await generateQuestionChunk({ provider: provider, topic: topic, count: count, offset: offset, existing: allQuestions });
          if (Array.isArray(chunkItems) && chunkItems.length > 0) {
            chunkSucceeded = true;
            break;
          }
        } catch (e) {
          lastErr = e;
          emitHint(e.message);
          console.warn('[Chunker] chunk ' + (chunk + 1) + '/' + totalChunks + ' attempt ' + (attempt + 1) + ' failed on ' + provider + ':', e.message);
        }
      }

      if (chunkSucceeded) break;
    }

    if (!chunkSucceeded || !Array.isArray(chunkItems) || chunkItems.length === 0) {
      if (allQuestions.length > 0) {
        console.warn('[Chunker] chunk ' + (chunk + 1) + ' exhausted all retries. Collected so far: ' + allQuestions.length + '. Continuing to next chunk...');
        emitProgress(allQuestions.length, chunk + 1, 'Batch ' + (chunk + 1) + ' skipped after retries \u2014 continuing... (' + allQuestions.length + '/' + total + ' done)');
        if (chunk < totalChunks - 1) {
          await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'recovering tokens before batch ' + (chunk + 2));
        }
        continue;
      }
      throw new Error('Failed to generate any items after ' + CHUNK_MAX_RETRIES + ' attempts. Last error: ' + ((lastErr && lastErr.message) || 'unknown'));
    }

    // Normalise and dedup
    const seen = new Set(allQuestions.map(function(q){ return q.question.toLowerCase(); }));
    for (let i = 0; i < chunkItems.length; i++) {
      const item = chunkItems[i];
      const q = String((item && item.question) || '').trim();
      const a = String((item && item.answer) || '').trim();
      if (q && a && !seen.has(q.toLowerCase())) {
        allQuestions.push({ question: q, answer: a });
        seen.add(q.toLowerCase());
      }
    }

    emitProgress(allQuestions.length, chunk + 1);

    // After every chunk except the last, wait 2 full minutes for token recovery
    if (chunk < totalChunks - 1) {
      await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'recovering tokens before batch ' + (chunk + 2));
    }
  }

  emitProgress(allQuestions.length, totalChunks, '\u2713 All done \u2014 ' + allQuestions.length + ' items generated and deployed!');
  return allQuestions;
}

// ── Joytree API v4 helpers ────────────────────────────────────────────────────

function getConfiguredV4Providers() {
  const providers = [];
  if (GROQ_API_KEY) providers.push('groq');
  if (XAI_API_KEY) providers.push('xai');
  return providers;
}

async function callGroqChat({ messages, maxTokens = 4096, timeoutMs = 90000, temperature = 0.3 } = {}) {
  if (!GROQ_API_KEY) throw new Error('GROQ_API_KEY is not configured.');
  return callOpenAICompatibleChat({
    apiUrl: 'https://api.groq.com/openai/v1/chat/completions',
    apiKey: GROQ_API_KEY,
    model: GROQ_MODEL,
    messages,
    maxTokens,
    timeoutMs,
    temperature
  });
}

async function callXAIChat({ messages, maxTokens = 4096, timeoutMs = 120000, temperature = 0.3 } = {}) {
  if (!XAI_API_KEY) throw new Error('XAI_API_KEY is not configured.');
  return callOpenAICompatibleChat({
    apiUrl: 'https://api.x.ai/v1/chat/completions',
    apiKey: XAI_API_KEY,
    model: XAI_MODEL,
    messages,
    maxTokens,
    timeoutMs,
    temperature
  });
}

async function callV4ProviderChat(provider, options) {
  if (provider === 'groq') return callGroqChat(options);
  if (provider === 'xai') return callXAIChat(options);
  throw new Error('Unsupported Joytree API v4 provider: ' + provider);
}

function isLikelyTransientAIError(err) {
  const msg = String((err && err.message) || err || '').toLowerCase();
  return msg.includes('429') || msg.includes('quota') || msg.includes('rate') ||
    msg.includes('limit') || msg.includes('timeout') || msg.includes('timed out') ||
    msg.includes('503') || msg.includes('502') || msg.includes('504');
}

// v4 single-shot flow builder — uses the same deployed Groq/xAI credentials as v1/v2.
// Groq starts the flow, then xAI silently continues if Groq fails or returns non-JSON.
async function buildFlowWithV4({ prompt = '', sourceText = '', fileName = '' } = {}) {
  const providers = getConfiguredV4Providers();
  if (!providers.length) throw new Error('Joytree API v4 is not configured. Add GROQ_API_KEY or XAI_API_KEY to your server .env file and restart.');

  const userInput = 'PROMPT:\n' + String(prompt || '').slice(0, 20000) + '\n\nSOURCE_TEXT:\n' + String(sourceText || '').slice(0, 20000) + '\n\nFILE_NAME:\n' + String(fileName || '').slice(0, 200);
  const systemMsg = AI_FLOW_INSTRUCTION + '\nJoytree API v4 uses the platform Groq/xAI cascade. If WEB_SEARCH_RESULTS appear in SOURCE_TEXT, use them as current context.';
  const messages = [{ role: 'system', content: systemMsg }, { role: 'user', content: userInput }];
  let lastError = null;

  for (const provider of providers) {
    try {
      const text = await callV4ProviderChat(provider, { messages, temperature: 0.25, maxTokens: 8192, timeoutMs: provider === 'xai' ? 180000 : 120000 });
      const parsed = extractJsonFromText(text);
      if (parsed) return Object.assign({}, parsed, { _aiProvider: provider === 'xai' ? 'joytree_xai' : 'joytree_groq', _aiModel: 'Joytree API v4' });
      throw new Error(provider + ' returned non-JSON output.');
    } catch(e) {
      lastError = e;
      console.warn('[v4] ' + provider + ' flow build failed:', e.message, '— trying next configured provider');
    }
  }

  throw new Error('Joytree API v4 could not build a valid flow with the configured Groq/xAI providers. Last error: ' + ((lastError && lastError.message) || 'unknown'));
}

// v4 provider pool — TRUE alternation (Groq→xAI→Groq→…) with cooldown fallback
const _v4CooldownUntil = { groq: 0, xai: 0 };
const V4_PROVIDERS = ['groq', 'xai'];
let _v4ChunkCounter = 0; // increments each chunk to drive alternation

function getV4ProviderOrder(chunkIndex) {
  const configured = V4_PROVIDERS.filter(function(provider) {
    return provider === 'groq' ? !!GROQ_API_KEY : !!XAI_API_KEY;
  });
  if (configured.length <= 1) return configured;
  const preferred = V4_PROVIDERS[chunkIndex % V4_PROVIDERS.length];
  const fallback = V4_PROVIDERS[(chunkIndex + 1) % V4_PROVIDERS.length];
  return [preferred, fallback].filter(function(provider) { return configured.includes(provider); });
}

function markV4ProviderCooling(provider) {
  _v4CooldownUntil[provider] = Date.now() + INTER_CHUNK_DELAY_MS;
  console.warn('[v4] Provider', provider, 'is cooling for', Math.round(INTER_CHUNK_DELAY_MS / 1000) + 's; another configured provider will take over.');
}

// Call one chunk using v4 TRUE alternation — Groq↔xAI every chunk, failover on error
async function callV4Chunk(messages) {
  const chunkIndex = _v4ChunkCounter++;
  const order = getV4ProviderOrder(chunkIndex);
  if (!order.length) throw new Error('Joytree API v4 is not configured. Add GROQ_API_KEY or XAI_API_KEY to your server .env file and restart.');

  console.log('[v4] Chunk', chunkIndex, '— preferred provider:', order[0]);
  let lastError = null;

  for (const provider of order) {
    const now = Date.now();
    if (now < (_v4CooldownUntil[provider] || 0) && order.length > 1) {
      console.warn('[v4 chunk ' + chunkIndex + '] ' + provider + ' still cooling — trying another provider');
      continue;
    }
    try {
      const text = await callV4ProviderChat(provider, {
        messages: messages,
        temperature: 0.7,
        maxTokens: 4096,
        timeoutMs: provider === 'xai' ? 120000 : 90000
      });
      console.log('[v4] Chunk', chunkIndex, 'completed by', provider);
      return { text: text, provider: provider };
    } catch(e) {
      lastError = e;
      console.warn('[v4 chunk ' + chunkIndex + '] ' + provider + ' error: ' + e.message + ' — trying next configured provider');
      if (isLikelyTransientAIError(e)) markV4ProviderCooling(provider);
      await new Promise(function(r){ setTimeout(r, 500); });
    }
  }

  // If every provider was skipped because of cooldown, wait briefly for the earliest recovery and try once more.
  const retryable = order.filter(function(provider) { return Date.now() < (_v4CooldownUntil[provider] || 0); });
  if (retryable.length) {
    const waitMs = Math.max(500, Math.min.apply(null, retryable.map(function(provider) { return _v4CooldownUntil[provider] - Date.now(); })));
    await new Promise(function(r){ setTimeout(r, Math.min(waitMs, 5000)); });
    for (const provider of order) {
      try {
        const text = await callV4ProviderChat(provider, { messages: messages, temperature: 0.7, maxTokens: 4096, timeoutMs: provider === 'xai' ? 120000 : 90000 });
        console.log('[v4] Chunk', chunkIndex, 'completed by', provider, 'after cooldown wait');
        return { text: text, provider: provider };
      } catch(e) {
        lastError = e;
      }
    }
  }

  throw new Error('v4 chunk ' + chunkIndex + ': all configured Groq/xAI providers failed. Last error: ' + ((lastError && lastError.message) || 'unknown'));
}


function extractJsonFromText(text = '') {
  const s = text.indexOf('{');
  const e = text.lastIndexOf('}');
  if (s === -1 || e === -1 || e <= s) return null;
  try { return JSON.parse(text.slice(s, e + 1)); } catch { return null; }
}

async function buildFlowWithGroq({ prompt = '', sourceText = '', fileName = '' } = {}) {
  if (!GROQ_API_KEY) return null;
  const userInput = `PROMPT:\n${String(prompt || '').slice(0, 12000)}\n\nSOURCE_TEXT:\n${String(sourceText || '').slice(0, 8000)}\n\nFILE_NAME:\n${String(fileName || '').slice(0, 200)}`;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), Number(process.env.GROQ_FLOW_TIMEOUT_MS || 120000));
    let r;
    try {
      r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
        method: 'POST',
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_API_KEY}` },
        body: JSON.stringify({
          model: GROQ_MODEL,
          messages: [
            { role: 'system', content: AI_FLOW_INSTRUCTION },
            { role: 'user', content: userInput }
          ],
          temperature: 0.3,
          max_tokens: Number(process.env.GROQ_FLOW_MAX_TOKENS || 1200),
          top_p: 1,
          stream: false
        })
      });
    } finally {
      clearTimeout(timeoutId);
    }
    if (!r.ok) {
      const errText = await r.text().catch(() => '');
      const errJson = (() => { try { return JSON.parse(errText); } catch { return null; } })();
      const msg = errJson?.error?.message || errText.slice(0, 200) || `HTTP ${r.status}`;
      console.error('[Joytree AI] Groq HTTP error', r.status, msg);
      throw new Error(`Groq error: ${msg}`);
    }
    const data = await r.json().catch(() => ({}));
    const text = String(data?.choices?.[0]?.message?.content || '');
    const parsed = extractJsonFromText(text);
    if (!parsed) throw new Error('Groq returned non-JSON output. Try rephrasing your prompt.');
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (e) {
    if (e.name === 'AbortError') throw new Error('Groq timed out. Try a shorter prompt.');
    console.error('[Joytree AI] buildFlowWithGroq error:', e.message);
    throw e;
  }
}

async function buildFlowWithCerebras({ prompt = '', sourceText = '', fileName = '' } = {}) {
  if (!CEREBRAS_API_KEY) throw new Error('CEREBRAS_API_KEY is not configured. Add it to your .env file.');
  const userInput = `PROMPT:\n${String(prompt || '').slice(0, 20000)}\n\nSOURCE_TEXT:\n${String(sourceText || '').slice(0, 20000)}\n\nFILE_NAME:\n${String(fileName || '').slice(0, 200)}`;
  try {
    const text = await callCerebrasChat({
      messages: [
        { role: 'system', content: `${AI_FLOW_INSTRUCTION}\nJoytree API v3 runs on Cerebras ultra-fast AI. If WEB_SEARCH_RESULTS appear in SOURCE_TEXT, use them as current web context.` },
        { role: 'user', content: userInput }
      ],
      temperature: 0.25,
      maxTokens: CEREBRAS_FLOW_MAX_TOKENS,
      timeoutMs: CEREBRAS_FLOW_TIMEOUT_MS
    });
    const parsed = extractJsonFromText(text);
    if (!parsed) throw new Error('Cerebras returned non-JSON output. Try rephrasing your prompt.');
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch (e) {
    if (e.name === 'AbortError') throw new Error('Cerebras timed out. Try increasing CEREBRAS_FLOW_TIMEOUT_MS.');
    console.error('[Joytree API v3] buildFlowWithCerebras error:', e.message);
    throw e;
  }
}

async function buildFlowWithXAI({ prompt = '', sourceText = '', fileName = '' } = {}) {
  if (!XAI_API_KEY) throw new Error('xAI API key not configured. Add XAI_API_KEY to your server .env file.');
  const userInput = `PROMPT:\n${String(prompt || '').slice(0, 12000)}\n\nSOURCE_TEXT:\n${String(sourceText || '').slice(0, 8000)}\n\nFILE_NAME:\n${String(fileName || '').slice(0, 200)}`;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), Number(process.env.XAI_FLOW_TIMEOUT_MS || 180000));
  let r;
  try {
    r = await fetch('https://api.x.ai/v1/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${XAI_API_KEY}` },
      body: JSON.stringify({
        model: XAI_MODEL,
        messages: [
          { role: 'system', content: AI_FLOW_INSTRUCTION },
          { role: 'user', content: userInput }
        ],
        temperature: 0.3,
        max_tokens: Number(process.env.XAI_FLOW_MAX_TOKENS || 16000),
        top_p: 1,
        stream: false
      })
    });
  } finally {
    clearTimeout(timeoutId);
  }
  if (!r.ok) {
    const errText = await r.text().catch(() => '');
    const errJson = (() => { try { return JSON.parse(errText); } catch { return null; } })();
    const msg = errJson?.error?.message || errText.slice(0, 200) || `HTTP ${r.status}`;
    console.error('[Joytree AI] xAI HTTP error', r.status, msg);
    throw new Error(`xAI error: ${msg}`);
  }
  const data = await r.json().catch(() => ({}));
  const text = String(data?.choices?.[0]?.message?.content || '');
  const parsed = extractJsonFromText(text);
  if (!parsed) throw new Error('xAI returned non-JSON output. Try rephrasing your prompt.');
  return parsed && typeof parsed === 'object' ? parsed : null;
}

async function buildFlowWithAI({ prompt = '', sourceText = '', fileBase64 = '', fileMime = '', fileName = '', aiProvider = 'auto', aiVersion = 'v1' } = {}) {
  // Joytree API v4 — Groq + xAI cascade with web search context
  if (aiVersion === 'v4') {
    const result = await buildFlowWithV4({ prompt, sourceText, fileName });
    if (result) return result; // already has _aiProvider and _aiModel set
    throw new Error('Joytree API v4 returned an empty response.');
  }

  // Joytree API v3 — Cerebras ultra-fast cloud AI (admin only at route layer)
  if (aiVersion === 'v3') {
    const result = await buildFlowWithCerebras({ prompt, sourceText, fileName });
    if (result) return { ...result, _aiProvider: 'cerebras', _aiModel: 'Joytree API v3 (Cerebras)' };
    throw new Error('Joytree API v3 returned an empty response. Check CEREBRAS_API_KEY.');
  }
  // Joy AI v1 — Groq only (fails if Groq fails)
  if (aiVersion === 'v1') {
    if (!GROQ_API_KEY) throw new Error('Joytree AI is not configured. Add GROQ_API_KEY to your server .env file and restart.');
    const result = await buildFlowWithGroq({ prompt, sourceText, fileName });
    if (result) return { ...result, _aiProvider: 'joytree', _aiModel: 'Joy AI v1' };
    throw new Error('Joytree AI returned an empty response. Try rephrasing your prompt.');
  }
  // Joy AI v2 — Groq first, silent fallback to xAI if Groq fails or rate-limits
  if (aiVersion === 'v2') {
    let lastError = null;
    // Try Groq first
    if (GROQ_API_KEY) {
      try {
        const result = await buildFlowWithGroq({ prompt, sourceText, fileName });
        if (result) return { ...result, _aiProvider: 'joytree', _aiModel: 'Joy AI v2' };
        // Groq returned null/empty — treat as soft failure, try xAI if available
        lastError = new Error('Groq returned an empty response.');
        console.warn('[Joy AI v2] Groq returned empty, trying xAI fallback');
      } catch (e) {
        lastError = e;
        console.warn('[Joy AI v2] Groq failed, trying xAI fallback:', e.message);
      }
    }
    // Fallback to xAI silently if key is configured
    if (XAI_API_KEY) {
      try {
        const result = await buildFlowWithXAI({ prompt, sourceText, fileName });
        if (result) return { ...result, _aiProvider: 'joytree_xai', _aiModel: 'Joy AI v2' };
      } catch (e) {
        lastError = e;
        console.warn('[Joy AI v2] xAI fallback also failed:', e.message);
      }
    }
    // If we get here both failed (or xAI not configured)
    const baseMsg = lastError?.message || 'AI returned an empty response.';
    const hint = !XAI_API_KEY ? ' (Add XAI_API_KEY to .env to enable xAI fallback)' : '';
    throw new Error(baseMsg + hint);
  }
  // Default — same as v1 for unknown versions
  if (!GROQ_API_KEY) throw new Error('Joytree AI is not configured. Add GROQ_API_KEY to your server .env file and restart.');
  try {
    const result = await buildFlowWithGroq({ prompt, sourceText, fileName });
    if (result) return { ...result, _aiProvider: 'joytree', _aiModel: 'Joytree AI' };
    throw new Error('Joytree AI returned an empty response. Try rephrasing your prompt.');
  } catch (e) {
    throw new Error(String(e.message || e));
  }
}
async function fetchUrlContext(text = '') {
  const urls = Array.from(String(text || '').matchAll(/https?:\/\/[^\s)]+/g)).map(m => m[0]).slice(0, 3);
  if (!urls.length) return '';
  const parts = [];
  for (const u of urls) {
    try {
      const r = await fetch(u, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!r.ok) continue;
      const raw = await r.text();
      const cleaned = String(raw).replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
      parts.push(`URL: ${u}\nCONTENT: ${cleaned.slice(0, 5000)}`);
    } catch {}
  }
  return parts.join('\n\n');
}
function applyTransform(scope, cfg = {}) {
  const srcVal = deepGet(scope, cfg.source || 'req.body');
  const target = String(cfg.target || 'vars.value');
  let val = srcVal;
  if (cfg.op === 'uppercase') val = String(srcVal || '').toUpperCase();
  if (cfg.op === 'lowercase') val = String(srcVal || '').toLowerCase();
  if (cfg.op === 'sha256') val = crypto.createHash('sha256').update(String(srcVal || '')).digest('hex');
  if (cfg.op === 'calc_multiply') val = Number(deepGet(scope, cfg.left || 'req.body.price')) * Number(deepGet(scope, cfg.right || 'req.body.quantity'));
  const parts = target.split('.');
  let cur = scope;
  while (parts.length > 1) {
    const p = parts.shift();
    if (!cur[p] || typeof cur[p] !== 'object') cur[p] = {};
    cur = cur[p];
  }
  cur[parts[0]] = val;
}
function signToken(payload = {}) {
  const secret = process.env.LOGIFLOW_JWT_SECRET || 'logiflow-dev-secret';
  const body = { ...payload, exp: Date.now() + 3600_000 };
  const data = Buffer.from(JSON.stringify(body)).toString('base64url');
  const sig = crypto.createHmac('sha256', secret).update(data).digest('base64url');
  return `${data}.${sig}`;
}
function verifyToken(token = '') {
  const [data, sig] = String(token).split('.');
  if (!data || !sig) return { ok: false };
  const secret = process.env.LOGIFLOW_JWT_SECRET || 'logiflow-dev-secret';
  const expected = crypto.createHmac('sha256', secret).update(data).digest('base64url');
  if (sig !== expected) return { ok: false };
  try {
    const payload = JSON.parse(Buffer.from(data, 'base64url').toString('utf8'));
    if (!payload.exp || Date.now() > payload.exp) return { ok: false };
    return { ok: true, payload };
  } catch { return { ok: false }; }
}

app.post('/api/developer/flows/deploy', requireAuth, (req, res) => {
  const body = req.body || {};
  const flowId = String(body.flowId || `flow_${Date.now()}`);
  const nodes = Array.isArray(body.nodes) ? body.nodes : [];
  if (!nodes.length) return res.status(400).json({ error: 'nodes required' });
  flowRegistry.set(flowId, { flowId, owner: String(req.user?._id || req.user?.id || ''), nodes, createdAt: new Date().toISOString() });
  if (!virtualDatabase[flowId]) virtualDatabase[flowId] = {};
  saveVirtualDb();
  res.json({ ok: true, flowId, endpoint: `/api/live/${flowId}` });
});

app.get('/api/developer/database/state', requireAuth, (req, res) => res.json({ virtualDatabase }));
app.post('/api/developer/database/clear', requireAuth, (req, res) => {
  const { flowId, collection, seed } = req.body || {};
  if (!flowId) return res.status(400).json({ error: 'flowId required' });
  if (!virtualDatabase[flowId]) virtualDatabase[flowId] = {};
  if (collection) virtualDatabase[flowId][collection] = [];
  else virtualDatabase[flowId] = {};
  if (seed === 'students') {
    virtualDatabase[flowId].students = [
      { id: 1, name: 'Ava', grade: 'A', createdAt: new Date().toISOString() },
      { id: 2, name: 'Liam', grade: 'B', createdAt: new Date().toISOString() }
    ];
  }
  saveVirtualDb();
  res.json({ ok: true });
});
app.get('/api/developer/logs', requireAuth, (req, res) => res.json({ logs: executionLogs.slice(0, 500) }));

app.post('/api/developer/flows/from-text', requireAuth, (req, res) => {
  const initialAiVersion = String(req.body?.aiVersion || 'v1').toLowerCase();
  // Dynamic timeout: for chunked large requests we need enough time for all chunks + 2-min waits between them.
  // Formula: up to 2000 items = 80 chunks. Each chunk ~90s AI call + 120s wait = 210s.
  // So max realistic: 80 * 210 = 16800s. We cap at 3 hours (10800s) as a reasonable ceiling.
  const requestedCount = (() => {
    const body = req.body || {};
    const p = String(body.prompt || '') + ' ' + String(body.sourceText || '');
    const m = p.match(/(\d{1,4})\s*(riddle|question|quiz|mcq|item|fact|joke)s?/i)
           || p.match(/generate\s+(\d{1,4})/i);
    return m ? Math.min(Number(m[1]), 2000) : 0;
  })();
  const estimatedChunks = requestedCount > 25 ? Math.ceil(requestedCount / 25) : 1;
  // 90s per chunk AI call + 120s wait between chunks + 60s buffer
  const routeTimeoutMs = estimatedChunks > 1
    ? Math.min(10800000, estimatedChunks * 155000 + 60000) // 90s AI + 65s wait per chunk
    : 300000;
  const reqTimeout = setTimeout(() => {
    if (!res.headersSent) res.status(504).json({ error: `Request timed out after ${Math.round(routeTimeoutMs / 1000)}s. Try a shorter prompt or split into smaller batches.` });
  }, routeTimeoutMs);
  (async () => {
  const { prompt, routePath = '/quiz', method = 'POST', sourceText = '', fileBase64 = '', fileMime = '', fileName = '', aiProvider = 'auto', aiVersion = 'v1', socketId = '' } = req.body || {};
  const selectedAiVersion = String(aiVersion || 'v1').toLowerCase();
  if (selectedAiVersion === 'v3' && !isRootEmailAdmin(req.user)) {
    return res.status(403).json({ error: 'Joytree API v3 is reserved for the platform admin. Upgrade your plan to unlock high-volume AI API generation with longer local runs and web context.' });
  }
  if (selectedAiVersion === 'v4' && !isRootEmailAdmin(req.user)) {
    return res.status(403).json({ error: 'Joytree API v4 is reserved for the platform admin. Upgrade your plan to unlock multi-AI cascade generation.' });
  }
  const userPlanKey = await getUserPlanKey(req.user);
  const planLimits = PLAN_DB_API_LIMITS[userPlanKey] || PLAN_DB_API_LIMITS.free;
  const ownerUserId = String(req.user?._id || req.user?.id || '');
  const fbApis = await readApisFromFirebase(req.user).catch(()=>[]);
  const localApis = apiCatalog.filter(a => a.ownerUserId === ownerUserId);
  const uniqueApiCount = new Set([...fbApis, ...localApis].map(a => String(a.flowId || ''))).size;
  if (uniqueApiCount >= Number(planLimits.maxApis || 0)) return res.status(403).json({ error: `API builder limit reached for ${userPlanKey} plan (${planLimits.maxApis} max). Upgrade to create more APIs.` });
  const flowId = `flow_${Date.now()}`;
  if (selectedAiVersion === 'v4' && !GROQ_API_KEY && !XAI_API_KEY) return res.status(503).json({ error: 'Joytree API v4 is not configured. Add GROQ_API_KEY or XAI_API_KEY to your .env file.' });
  if (selectedAiVersion !== 'v3' && selectedAiVersion !== 'v4' && !GROQ_API_KEY) return res.status(503).json({ error: 'Joytree AI is not configured. Add GROQ_API_KEY to your server .env file and restart.' });
  if (selectedAiVersion === 'v3' && !CEREBRAS_API_KEY) return res.status(503).json({ error: 'Joytree API v3 is not configured. Add CEREBRAS_API_KEY to your server .env file and restart.' });
  const providerRequested = 'auto';
  const fileContextNote = (fileName && fileMime && !String(fileMime).startsWith('text/') && fileMime !== 'application/json')
    ? `\n[Uploaded file: ${fileName} (${fileMime}) — use this file context to generate API content]`
    : '';
  const scrapedContext = await fetchUrlContext(`${prompt}\n${sourceText}`).catch(() => '');
  // v4 always gets web search context before sending the prompt to Groq/xAI.
  const webSearchContext = selectedAiVersion === 'v4'
    ? await (async () => {
        // For v4 use DuckDuckGo directly (no SearXNG needed)
        try {
          const q = String(prompt || '').replace(/https?:\/\/[^\s)]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 500);
          if (!q) return '';
          const r = await fetch('https://api.duckduckgo.com/?q=' + encodeURIComponent(q) + '&format=json&no_html=1&skip_disambig=1', { headers: { Accept: 'application/json', 'User-Agent': 'JoytreeAPI/4.0' }, signal: AbortSignal.timeout(10000) });
          if (!r.ok) return '';
          const d = await r.json().catch(() => ({}));
          const parts = [];
          if (d.AbstractText) parts.push('Summary: ' + String(d.AbstractText).slice(0, 800));
          if (Array.isArray(d.RelatedTopics)) {
            for (const t of d.RelatedTopics.slice(0, 5)) {
              if (t.Text) parts.push('- ' + String(t.Text).slice(0, 400));
            }
          }
          return parts.length ? 'WEB_SEARCH_RESULTS for "' + q + '":\n' + parts.join('\n') : '';
        } catch(e) { return ''; }
      })()
    : await fetchWebSearchContext(`${prompt}\n${sourceText}`).catch(() => '');
  const combinedSourceText = `${sourceText}${fileContextNote}\n${scrapedContext}${webSearchContext ? `\n\n${webSearchContext}` : ''}`;

  // ── Detect if this is a large quiz/riddle request that needs chunking ──────
  const isQuiz = /quiz|question|mcq|exam|riddle/i.test(String(prompt||'') + ' ' + String(sourceText||''));
  const requestedCount = isQuiz ? detectRequestedCount(String(prompt||'')) : 0;
  const effectiveChunkSize = CHUNK_SIZE; // Same for all versions now
  const useChunking = isQuiz && requestedCount > effectiveChunkSize;

  let aiSpec = null;
  let aiError = null;
  let chunkedQuestions = [];

  if (useChunking) {
    // ── CHUNKED PATH: generate questions in batches ───────────────────────────
    console.log(`[Chunker] Large request detected: ${requestedCount} items. Splitting into chunks of ${effectiveChunkSize} (${initialAiVersion}).`);
    try {
      chunkedQuestions = await buildQuestionsInChunks({
        prompt: String(prompt||''),
        total: requestedCount,
        aiVersion: selectedAiVersion,
        socketId: String(socketId || '')
      });
    } catch (e) {
      aiError = e.message || String(e);
    }
    if (!chunkedQuestions.length) {
      return res.status(502).json({ error: aiError || 'Chunked generation returned no questions. Try rephrasing your prompt.' });
    }
    // Build a minimal aiSpec with a proper responseTemplate for a quiz API
    aiSpec = {
      routePath: '/quiz',
      method: 'GET',
      responseTemplate: {
        ok: true,
        total: chunkedQuestions.length,
        questions: chunkedQuestions.slice(0, 3).map(q => ({ question: q.question, answer: q.answer })),
        note: `Call GET /api/live/${flowId} to get all questions`
      },
      seedQuestions: chunkedQuestions
    };
  } else {
    // ── NORMAL PATH: single AI call ───────────────────────────────────────────
    try {
      aiSpec = await buildFlowWithAI({ prompt, sourceText: combinedSourceText, fileBase64, fileMime, fileName, aiProvider: providerRequested, aiVersion: selectedAiVersion });
    } catch (e) {
      aiError = e.message || String(e);
    }
    if (!aiSpec) return res.status(502).json({ error: aiError || 'AI did not return a valid response. Try rephrasing your prompt.' });
    if (!aiSpec.responseTemplate || typeof aiSpec.responseTemplate !== 'object') return res.status(502).json({ error: 'AI returned an invalid output structure. Try rephrasing your prompt.' });
  }

  const usedProvider = aiSpec?._aiProvider || 'joytree';
  const usedModel = aiSpec?._aiModel || (selectedAiVersion === 'v4' ? 'Joytree API v4' : (selectedAiVersion === 'v3' ? 'Joytree API v3 (Cerebras)' : (selectedAiVersion === 'v2' ? 'Joy AI v2' : 'Joy AI v1')));
  delete aiSpec._aiProvider; delete aiSpec._aiModel;
  const route = String(aiSpec?.routePath || routePath || '/api');
  const httpMethod = String(aiSpec?.method || method || 'GET').toUpperCase();
  const aiTemplate = aiSpec.responseTemplate;
  const nodes = [
    { id:'n1', type:'INCOMING_REQUEST', config:{ method: httpMethod, routePath: route }, next:'n2' },
    { id:'n2', type:'DB_INSERT', config:{ collection: isQuiz ? 'quiz_submissions' : 'requests', source:'req.body' }, next:'n3' },
    { id:'n3', type:'DB_FIND', config:{ collection: isQuiz ? 'quiz_questions' : 'requests', filters: [] }, next:'n4' },
    { id:'n4', type:'HTTP_RESPONSE', config:{ status:200, json: aiTemplate } }
  ];
  if (isQuiz) {
    let rawQuestions = useChunking ? chunkedQuestions : (Array.isArray(aiSpec?.seedQuestions) ? aiSpec.seedQuestions : []);

    // Fallback: AI sometimes embeds questions inside responseTemplate instead of seedQuestions.
    // Check common keys the model uses: questions, riddles, items, data, results, quiz, entries.
    if (!rawQuestions.length && !useChunking) {
      const rt = aiSpec?.responseTemplate || {};
      const fallbackArr = rt.questions || rt.riddles || rt.items || rt.data || rt.results || rt.quiz || rt.entries || [];
      if (Array.isArray(fallbackArr) && fallbackArr.length > 0) {
        console.warn('[Quiz] seedQuestions was empty — rescued', fallbackArr.length, 'items from responseTemplate');
        rawQuestions = fallbackArr;
      }
    }

    // Second fallback: scan ALL top-level responseTemplate values for any array of {question,answer} objects
    if (!rawQuestions.length && !useChunking) {
      const rt = aiSpec?.responseTemplate || {};
      for (const val of Object.values(rt)) {
        if (Array.isArray(val) && val.length > 0 && val[0]?.question && val[0]?.answer) {
          console.warn('[Quiz] seedQuestions rescued from responseTemplate key scan —', val.length, 'items found');
          rawQuestions = val;
          break;
        }
      }
    }

    const questions = rawQuestions.slice(0, 2000).map((q, i) => ({ id:i+1, question:String(q?.question||'').trim(), answer:String(q?.answer||'').trim() })).filter(q => q.question && q.answer);
    if (!questions.length) return res.status(502).json({ error: 'AI returned no quiz/riddle content. Refine your prompt.' });
    getCollection(flowId, 'quiz_questions').push(...questions);
    saveVirtualDb();
  }
  const aiDataSeed = Array.isArray(aiSpec?.dataSeed) ? aiSpec.dataSeed.slice(0, 1000) : [];
  if (aiDataSeed.length) {
    getCollection(flowId, 'ai_data').push(...aiDataSeed.map((d, i) => ({ id: i + 1, ...d })));
    saveVirtualDb();
  }
  flowRegistry.set(flowId, { flowId, owner: ownerUserId, nodes, createdAt: new Date().toISOString(), prompt: String(prompt||''), aiTemplate, aiDataSeed, isQuiz });
  const rec = {
    flowId, ownerUserId, prompt: String(prompt||''), sourceText: String(sourceText||'').slice(0, 120000),
    endpoint:`/api/live/${flowId}`, status:'active', createdAt:new Date().toISOString(), updatedAt:new Date().toISOString(),
    quizSeeded:isQuiz, dockerized:false, aiProvider: usedProvider, aiModel: usedModel,
    responseTemplate: aiTemplate, dataSeed: aiDataSeed, routePath: route, httpMethod,
    chunked: useChunking, totalGenerated: useChunking ? chunkedQuestions.length : undefined
  };
  apiCatalog = [rec, ...apiCatalog.filter(a => !(a.flowId === flowId && a.ownerUserId === ownerUserId))];
  saveApiCatalog();
  void writeApiToFirebase(req.user, rec);
  if (res.headersSent) return;
  res.json({ ok:true, flowId, endpoint:`/api/live/${flowId}`, nodesCount:nodes.length, quizSeeded:isQuiz, aiUsed: true, aiProvider: usedProvider, aiModel: usedModel, chunked: useChunking, totalGenerated: useChunking ? chunkedQuestions.length : undefined });
  })().catch(e => { if (!res.headersSent) res.status(500).json({ error: e.message }); })
    .finally(() => clearTimeout(reqTimeout));
});


app.post('/api/ai/db-query', requireAuth, async (req, res) => {
  const { prompt = '', engine = 'postgres' } = req.body || {};
  if (!String(prompt || '').trim()) return res.status(400).json({ error: 'prompt is required' });
  if (!GROQ_API_KEY) return res.status(503).json({ error: 'Joytree AI is not configured. Add GROQ_API_KEY to .env and restart.' });
  const safeEngine = String(engine || 'postgres').toLowerCase();
  const systemPrompt = `You are Joytree AI, a database query expert. Generate ONLY the ${safeEngine} query for the user's request. No explanation, no markdown, no code fences, only raw query text. For MongoDB return valid JSON command object. For Redis return a valid redis-cli command.`;
  try {
    const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_API_KEY}` },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: `Database engine: ${safeEngine}\n\nRequest: ${String(prompt).slice(0, 4000)}` }
        ],
        temperature: 0.2,
        max_tokens: 600
      }),
      signal: AbortSignal.timeout(25000)
    });
    if (!r.ok) {
      const t = await r.text().catch(()=>'');
      return res.status(502).json({ error: `Joytree AI request failed (${r.status}): ${t.slice(0,200)}` });
    }
    const d = await r.json().catch(()=>({}));
    const query = String(d?.choices?.[0]?.message?.content || '').trim().replace(/^```[a-z]*\n?/i,'').replace(/```$/,'').trim();
    if (!query) return res.status(502).json({ error: 'Joytree AI returned an empty query' });
    res.json({ ok: true, query, aiProvider: 'Joytree AI' });
  } catch (e) {
    res.status(500).json({ error: e.message || 'Joytree AI query generation failed' });
  }
});

app.get('/api/developer/ai/status', requireAuth, (req, res) => {
  res.json({
    ok: true,
    aiProvider: 'Joytree AI',
    aiConfigured: !!GROQ_API_KEY,
    aiAnalysisMode: 'server-side-ai',
    v3: {
      name: 'Joytree API v3 (Cerebras)',
      adminOnly: true,
      hasAccess: isRootEmailAdmin(req.user),
      cerebrasConfigured: !!CEREBRAS_API_KEY,
      webSearchEnabled: JOYTREE_WEB_SEARCH_ENABLED
    },
    v4: {
      name: 'Joytree API v4',
      adminOnly: true,
      hasAccess: isRootEmailAdmin(req.user),
      groqConfigured: !!GROQ_API_KEY,
      xaiConfigured: !!XAI_API_KEY,
      webSearchEnabled: true
    }
  });
});

// ── Groq-powered build log analysis ──────────────────────────────────────────
app.post('/api/ai/analyze-logs', requireAuth, async (req, res) => {
  const { errorText = '', deploymentId = '', projectName = '' } = req.body || {};
  if (!errorText) return res.status(400).json({ error: 'errorText is required' });
  if (!GROQ_API_KEY) return res.status(503).json({ error: 'GROQ_API_KEY not configured' });
  try {
    const systemPrompt = `You are an expert DevOps engineer and deployment troubleshooter for the Joytree platform (Node.js, Docker, Cloudflare, Firebase). Analyze build/deployment error logs and return a JSON object with these exact fields:
{
  "errorSummary": "One sentence plain-English summary of what went wrong",
  "errorCategory": "one of: build_failure | dependency_error | docker_error | port_conflict | env_missing | permission_error | timeout | network_error | config_error | runtime_error",
  "rootCause": "Technical explanation of the root cause (2-4 sentences)",
  "steps": ["Step 1 to fix", "Step 2", "Step 3", ...],
  "codeSnippet": "Optional: exact command or config change needed, or empty string",
  "preventionTip": "How to prevent this error in future deployments",
  "severity": "critical | high | medium | low",
  "estimatedFixTime": "e.g. 2 minutes | 10 minutes | 30 minutes"
}
Return ONLY valid JSON, no markdown, no explanation.`;
    const userMsg = `Project: ${projectName || 'unknown'}\nDeployment ID: ${deploymentId || 'unknown'}\n\nError logs:\n${String(errorText).slice(0, 8000)}`;
    let groqRes = null;
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const r = await fetch('https://api.groq.com/openai/v1/chat/completions', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${GROQ_API_KEY}` },
          body: JSON.stringify({ model: GROQ_MODEL, max_tokens: 1200, temperature: 0.2, messages: [{ role:'system', content: systemPrompt }, { role:'user', content: userMsg }] }),
          signal: AbortSignal.timeout(25000)
        });
        if (r.ok) { groqRes = await r.json(); break; }
      } catch (e) { if (attempt === 1) throw e; }
    }
    const raw = groqRes?.choices?.[0]?.message?.content || '{}';
    let parsed = {};
    try { parsed = JSON.parse(raw.replace(/```json|```/g, '').trim()); } catch { parsed = { errorSummary: raw.slice(0, 300), steps: [], rootCause: '', errorCategory: 'build_failure', severity: 'high', estimatedFixTime: 'unknown', preventionTip: '', codeSnippet: '' }; }
    res.json({ ok: true, analysis: parsed });
  } catch (e) {
    res.status(500).json({ error: e.message || 'AI analysis failed' });
  }
});

app.post('/api/developer/flows/:flowId/dockerize', requireAuth, async (req, res) => {
  try {
    const flowId = String(req.params.flowId || '');
    let flow = flowRegistry.get(flowId);
    if (!flow) {
      // Fallback: rebuild from apiCatalog so server restarts don't break dockerize
      const rec = apiCatalog.find(a => a.flowId === flowId);
      if (rec) {
        const aiTemplate = rec.responseTemplate || { ok: true, flowId: rec.flowId, message: 'API is live' };
        flow = {
          flowId:     rec.flowId,
          owner:      rec.ownerUserId || '',
          nodes:      [],
          aiTemplate,
          aiDataSeed: Array.isArray(rec.dataSeed) ? rec.dataSeed : [],
          isQuiz:     !!rec.quizSeeded,
          prompt:     rec.prompt || ''
        };
        flowRegistry.set(flowId, flow); // restore into memory for subsequent calls
      }
    }
    if (!flow) return res.status(404).json({ error: 'Flow not found — it may have been lost after server restart. Re-deploy the flow first.' });
    const owner = String(req.user?._id || req.user?.id || '');
    if (flow.owner && String(flow.owner) !== owner) return res.status(403).json({ error: 'Forbidden' });

    const subdomain = `api-${flowId.slice(-8).toLowerCase()}`.replace(/[^a-z0-9-]/g, '-');
    const appDir = path.join(SITES_DIR, `logiflow-${flowId}`);
    fs.mkdirSync(appDir, { recursive: true });

    // Use the actual AI-generated response template stored on the flow
    const aiTemplate = flow.aiTemplate || { ok: true, flowId, message: 'API is live' };
    const aiDataSeed = Array.isArray(flow.aiDataSeed) ? flow.aiDataSeed : [];
    const isQuiz = !!flow.isQuiz;
    const quizQuestions = isQuiz ? (getCollection(flowId, 'quiz_questions') || []) : [];

    // Build a real standalone Node.js server that actually returns the AI content
    const templateJson = JSON.stringify(aiTemplate);
    const seedJson = JSON.stringify(isQuiz ? quizQuestions : aiDataSeed);
    const appJs = `'use strict';
const http = require('http');
const FLOW_ID = ${JSON.stringify(flowId)};
const IS_QUIZ = ${isQuiz};
const AI_TEMPLATE = ${templateJson};
const SEED_DATA = ${seedJson};
let reqCount = 0;

function respond(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS' });
  res.end(body);
}

http.createServer((req, res) => {
  if (req.method === 'OPTIONS') return respond(res, 204, {});
  reqCount++;
  if (req.method === 'GET' && req.url === '/health') return respond(res, 200, { ok: true, flowId: FLOW_ID, requests: reqCount });

  if (IS_QUIZ && SEED_DATA.length > 0) {
    // Return a random question or all questions
    const url = req.url || '/';
    if (url.includes('all') || url.includes('list')) {
      return respond(res, 200, { ok: true, flowId: FLOW_ID, questions: SEED_DATA, count: SEED_DATA.length });
    }
    const q = SEED_DATA[Math.floor(Math.random() * SEED_DATA.length)];
    return respond(res, 200, { ok: true, flowId: FLOW_ID, ...q });
  }

  if (!IS_QUIZ && SEED_DATA.length > 0) {
    const item = SEED_DATA[Math.floor(Math.random() * SEED_DATA.length)];
    return respond(res, 200, { ok: true, flowId: FLOW_ID, data: item, all: SEED_DATA });
  }

  respond(res, 200, { ok: true, flowId: FLOW_ID, ...AI_TEMPLATE });
}).listen(3000, '0.0.0.0', () => console.log('Flow ' + FLOW_ID + ' running on :3000'));
`;
    fs.writeFileSync(path.join(appDir, 'server.js'), appJs, 'utf8');
    fs.writeFileSync(path.join(appDir, 'Dockerfile'), `FROM node:20-alpine\nWORKDIR /app\nCOPY server.js /app/server.js\nEXPOSE 3000\nCMD ["node","server.js"]\n`, 'utf8');

    const image = `logiflow-${flowId}`.toLowerCase();
    const cname = `db-${subdomain}`;
    runDocker(`docker rm -f ${cname} 2>/dev/null || true`);
    const build = runDocker(`docker build -t ${image} "${appDir}"`, 120000);
    if (!build.ok) return res.status(500).json({ error: build.stderr || 'docker build failed' });
    let run = null;
    let port = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      port = getOrAssignPort(subdomain);
      run = runDocker(`docker run -d --name ${cname} --restart unless-stopped -p 127.0.0.1:${port}:3000 ${image}`);
      if (run.ok) break;
      const err = String(run.stderr || '').toLowerCase();
      const portBusy = err.includes('port is already allocated') || err.includes('bind for');
      if (!portBusy) break;
      clearAssignedPort(subdomain);
      runDocker(`docker rm -f ${cname} 2>/dev/null || true`);
    }
    if (!run || !run.ok) return res.status(500).json({ error: run?.stderr || 'docker run failed' });
    await registerSubdomain(subdomain).catch(() => null);
    portRegistry[subdomain] = port;
    savePortRegistry();
    const ownerUserId = String(req.user?._id || req.user?.id || '');
    const idx = apiCatalog.findIndex(a => a.flowId === flowId && a.ownerUserId === ownerUserId);
    if (idx !== -1) {
      apiCatalog[idx] = { ...apiCatalog[idx], dockerized:true, dockerUrl:`https://${subdomain}.${BASE_DOMAIN}`, dockerContainer:cname, dockerPort:port, updatedAt:new Date().toISOString() };
      saveApiCatalog();
      void writeApiToFirebase(req.user, apiCatalog[idx]);
    }
    return res.json({ ok: true, subdomain: `${subdomain}.${BASE_DOMAIN}`, liveUrl: `https://${subdomain}.${BASE_DOMAIN}`, flowId, container: cname, port });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/developer/apis', requireAuth, async (req, res) => {
  const ownerUserId = String(req.user?._id || req.user?.id || '');
  const fb = await readApisFromFirebase(req.user);
  const merged = [...fb, ...apiCatalog.filter(a => a.ownerUserId === ownerUserId)].reduce((m, a) => {
    m.set(String(a.flowId), { ...a, ownerUserId }); return m;
  }, new Map());
  res.json({ apis: Array.from(merged.values()).sort((a,b)=>new Date(b.updatedAt||b.createdAt||0)-new Date(a.updatedAt||a.createdAt||0)) });
});

app.get('/api/developer/apis/:flowId', requireAuth, async (req, res) => {
  const flowId = String(req.params.flowId || '');
  const ownerUserId = String(req.user?._id || req.user?.id || '');

  let apiRec = apiCatalog.find(a => a.flowId === flowId && a.ownerUserId === ownerUserId) || null;

  // Fallback to Firebase so API details still load after VPS restart
  if (!apiRec) {
    const fb = await readApisFromFirebase(req.user);
    apiRec = fb.find(a => String(a.flowId || '') === flowId) || null;
    if (apiRec) {
      apiRec = { ...apiRec, ownerUserId };
      const existsLocal = apiCatalog.some(a => a.flowId === apiRec.flowId && a.ownerUserId === ownerUserId);
      if (!existsLocal) {
        apiCatalog.push(apiRec);
        saveApiCatalog();
      }
    }
  }

  if (!apiRec) return res.status(404).json({ error: 'API not found' });

  // Enrich with live data
  const quizData = apiRec.quizSeeded ? (getCollection(flowId, 'quiz_questions') || []) : [];
  const aiData = getCollection(flowId, 'ai_data') || [];
  const logs = executionLogs.filter(l => l.flowId === flowId).slice(0, 50);
  res.json({ ok: true, api: { ...apiRec, quizQuestions: quizData, aiData, recentLogs: logs } });
});

app.delete('/api/developer/apis/:flowId', requireAuth, async (req, res) => {
  const flowId = String(req.params.flowId || '');
  const ownerUserId = String(req.user?._id || req.user?.id || '');
  const idx = apiCatalog.findIndex(a => a.flowId === flowId && a.ownerUserId === ownerUserId);
  if (idx === -1) return res.status(404).json({ error: 'API not found' });
  const rec = apiCatalog[idx];
  // Stop docker container if exists
  if (rec.dockerContainer) {
    try { runDocker(`docker rm -f ${rec.dockerContainer} 2>/dev/null || true`); } catch {}
  }
  apiCatalog.splice(idx, 1);
  flowRegistry.delete(flowId);
  if (virtualDatabase[flowId]) delete virtualDatabase[flowId];
  saveApiCatalog();
  saveVirtualDb();
  // Delete from Firebase
  try {
    const base = firebaseApisBaseUrl(req.user);
    if (base) {
      const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
      await fetch(`${base}/${encodeURIComponent(flowId)}.json${authQuery}`, { method: 'DELETE' }).catch(()=>{});
    }
  } catch {}
  res.json({ ok: true, deleted: flowId });
});
app.patch('/api/developer/apis/:flowId', requireAuth, async (req, res) => {
  const flowId = String(req.params.flowId || '');
  const ownerUserId = String(req.user?._id || req.user?.id || '');
  const idx = apiCatalog.findIndex(a => a.flowId === flowId && a.ownerUserId === ownerUserId);
  if (idx === -1) return res.status(404).json({ error:'API not found' });
  const patch = req.body && typeof req.body === 'object' ? req.body : {};
  apiCatalog[idx] = {
    ...apiCatalog[idx],
    prompt: String((patch.prompt ?? apiCatalog[idx].prompt) || ''),
    status: String(patch.status || apiCatalog[idx].status || 'active'),
    updatedAt: new Date().toISOString()
  };
  saveApiCatalog();
  await writeApiToFirebase(req.user, apiCatalog[idx]);
  res.json({ ok:true, api: apiCatalog[idx] });
});

app.post('/api/developer/apis/:flowId/followup', requireAuth, async (req, res) => {
  try {
    const flowId = String(req.params.flowId || '');
    const followup = String(req.body?.followup || '').trim();
    if (!flowId) return res.status(400).json({ error: 'flowId required' });
    if (!followup) return res.status(400).json({ error: 'followup required' });

    const ownerUserId = String(req.user?._id || req.user?.id || '');
    const idx = apiCatalog.findIndex(a => a.flowId === flowId && a.ownerUserId === ownerUserId);
    if (idx === -1) return res.status(404).json({ error: 'API not found' });
    const current = apiCatalog[idx];

    const combinedPrompt = `${String(current.prompt || '').trim()}

Follow-up update request:
${followup}`;
    const aiSpec = await buildFlowWithAI({ prompt: combinedPrompt, sourceText: String(current.sourceText || '') });
    if (!aiSpec || !aiSpec.responseTemplate || typeof aiSpec.responseTemplate !== 'object') {
      return res.status(502).json({ error: 'AI returned invalid follow-up output' });
    }

    const route = String(aiSpec?.routePath || current.routePath || '/api');
    const httpMethod = String(aiSpec?.method || current.httpMethod || 'GET').toUpperCase();
    const aiTemplate = aiSpec.responseTemplate;
    const isQuiz = /quiz|question|mcq|exam|riddle/i.test(combinedPrompt + ' ' + String(current.sourceText||''));

    const nodes = [
      { id:'n1', type:'INCOMING_REQUEST', config:{ method: httpMethod, routePath: route }, next:'n2' },
      { id:'n2', type:'DB_INSERT', config:{ collection: isQuiz ? 'quiz_submissions' : 'requests', source:'req.body' }, next:'n3' },
      { id:'n3', type:'DB_FIND', config:{ collection: isQuiz ? 'quiz_questions' : 'requests', filters: [] }, next:'n4' },
      { id:'n4', type:'HTTP_RESPONSE', config:{ status:200, json: aiTemplate } }
    ];

    virtualDatabase[flowId] = virtualDatabase[flowId] || {};
    if (isQuiz) {
      let aiQuestions = Array.isArray(aiSpec?.seedQuestions) ? aiSpec.seedQuestions : [];

      // Fallback: AI may have embedded questions inside responseTemplate instead of seedQuestions
      if (!aiQuestions.length) {
        const rt = aiSpec?.responseTemplate || {};
        const fallbackArr = rt.questions || rt.riddles || rt.items || rt.data || rt.results || rt.quiz || rt.entries || [];
        if (Array.isArray(fallbackArr) && fallbackArr.length > 0) {
          console.warn('[Quiz/followup] seedQuestions was empty — rescued', fallbackArr.length, 'items from responseTemplate');
          aiQuestions = fallbackArr;
        }
      }

      // Second fallback: scan all responseTemplate values for any {question,answer} array
      if (!aiQuestions.length) {
        const rt = aiSpec?.responseTemplate || {};
        for (const val of Object.values(rt)) {
          if (Array.isArray(val) && val.length > 0 && val[0]?.question && val[0]?.answer) {
            console.warn('[Quiz/followup] rescued', val.length, 'items via responseTemplate key scan');
            aiQuestions = val;
            break;
          }
        }
      }

      const questions = aiQuestions.slice(0, 500).map((q, i) => ({ id:i+1, question:String(q?.question||'').trim(), answer:String(q?.answer||'').trim() })).filter(q => q.question && q.answer);
      if (questions.length) virtualDatabase[flowId].quiz_questions = questions;
    }
    const aiDataSeed = Array.isArray(aiSpec?.dataSeed) ? aiSpec.dataSeed.slice(0, 1000) : [];
    if (aiDataSeed.length) virtualDatabase[flowId].ai_data = aiDataSeed.map((d, i) => ({ id: i + 1, ...d }));
    saveVirtualDb();

    flowRegistry.set(flowId, { flowId, owner: ownerUserId, nodes, createdAt: current.createdAt || new Date().toISOString(), prompt: combinedPrompt, aiTemplate, aiDataSeed, isQuiz });
    apiCatalog[idx] = { ...current, prompt: combinedPrompt, responseTemplate: aiTemplate, dataSeed: aiDataSeed, routePath: route, httpMethod, quizSeeded: isQuiz, updatedAt: new Date().toISOString() };
    saveApiCatalog();
    await writeApiToFirebase(req.user, apiCatalog[idx]);

    res.json({ ok: true, flowId, endpoint: current.endpoint || `/api/live/${flowId}` });
  } catch (e) {
    res.status(500).json({ error: e.message || 'Follow-up update failed' });
  }
});

app.post('/api/developer/apis/:flowId/link-project', requireAuth, async (req, res) => {
  const flowId = String(req.params.flowId || '');
  const ownerUserId = String(req.user?._id || req.user?.id || '');
  const idx = apiCatalog.findIndex(a => a.flowId === flowId && a.ownerUserId === ownerUserId);
  if (idx === -1) return res.status(404).json({ error: 'API not found' });
  const projectId = String(req.body?.projectId || '').trim();
  if (!projectId) return res.status(400).json({ error: 'projectId is required' });
  const endpoint = `https://${BASE_DOMAIN}${apiCatalog[idx].endpoint || `/api/live/${flowId}`}`;
  const apiKey = crypto.randomBytes(16).toString('hex');
  await injectConnStrIntoProject(projectId, 'api', endpoint, req.user);
  try {
    const { project: p, source } = await resolveEnvProject({ ...req, params: { ...req.params, id: projectId } });
    if (p) {
      p.envVars = p.envVars && typeof p.envVars === 'object' ? p.envVars : {};
      p.envVars.API_ENDPOINT = endpoint;
      p.envVars.API_KEY = apiKey;
      if (source === 'db') { p.markModified('envVars'); p.updatedAt = new Date(); await p.save(); }
      else updateLocalWorkspaceProject(req.user, String(p.id || p._id || projectId), { envVars: p.envVars });
    }
  } catch {}
  apiCatalog[idx] = { ...apiCatalog[idx], linkedProjectId: projectId, apiKey, updatedAt: new Date().toISOString() };
  saveApiCatalog();
  await writeApiToFirebase(req.user, apiCatalog[idx]);
  res.json({ ok: true, linkedProjectId: projectId, endpoint, apiKey });
});


async function executeFlowRequest(req, res) {
  const flowId = String(req.params.flowId || '');
  const flow = flowRegistry.get(flowId);
  if (!flow) return res.status(404).json({ error: 'Flow not found' });
  const nodesById = new Map(flow.nodes.map(n => [String(n.id), n]));
  let currentId = String((flow.nodes.find(n => n.type === 'INCOMING_REQUEST') || flow.nodes[0]).id);
  const scope = { req: { body: req.body || {}, query: req.query || {}, params: req.params || {}, method: req.method, headers: req.headers }, vars: {}, db_result: null };
  const rollbackSnapshot = JSON.parse(JSON.stringify(getFlowDb(flowId)));
  const visited = new Set();
  try {
    while (currentId) {
      if (visited.has(currentId)) throw new Error('Flow loop detected');
      visited.add(currentId);
      const n = nodesById.get(currentId);
      if (!n) throw new Error(`Node missing: ${currentId}`);
      logExec({ flowId, nodeId: currentId, nodeType: n.type, payload: req.body || {}, vars: scope.vars });

      if (n.type === 'RATE_LIMITER') {
        const max = Number(n.config?.max || 60), winMs = Number(n.config?.windowMs || 60000);
        const key = `${flowId}:${req.ip}`;
        const now = Date.now();
        const r = rateLimiterState.get(key) || { count: 0, windowStart: now };
        if (now - r.windowStart > winMs) { r.count = 0; r.windowStart = now; }
        r.count++; rateLimiterState.set(key, r);
        if (r.count > max) {
          res.setHeader('Retry-After', String(Math.ceil((winMs - (now - r.windowStart)) / 1000)));
          return res.status(429).json({ error: 'Too Many Requests' });
        }
      } else if (n.type === 'AUTH_VERIFY_TOKEN') {
        const auth = String(req.headers.authorization || '');
        const token = auth.startsWith('Bearer ') ? auth.slice(7) : '';
        if (!verifyToken(token).ok) return res.status(401).json({ error: 'Unauthorized' });
      } else if (n.type === 'AUTH_GENERATE_TOKEN') {
        scope.vars.token = signToken({ uid: deepGet(scope, n.config?.source || 'req.body.userId') || 'user' });
      } else if (n.type === 'TRANSFORM_DATA') {
        applyTransform(scope, n.config || {});
      } else if (n.type === 'DB_INSERT') {
        const collection = String(n.config?.collection || 'default');
        const doc = JSON.parse(JSON.stringify(deepGet(scope, n.config?.source || 'req.body') || {}));
        const schema = Array.isArray(n.config?.schema) ? n.config.schema : [];
        for (const f of schema) {
          const v = doc[f.field];
          if (f.required && (v === undefined || v === null || v === '')) return res.status(422).json({ error: `Field ${f.field} required` });
          if (v != null && f.type === 'Number' && Number.isNaN(Number(v))) return res.status(422).json({ error: `Field ${f.field} must be Number` });
          if (v != null && f.type === 'Boolean' && !(v === true || v === false || v === 'true' || v === 'false')) return res.status(422).json({ error: `Field ${f.field} must be Boolean` });
        }
        doc._ts = new Date().toISOString();
        getCollection(flowId, collection).push(doc);
        saveVirtualDb();
      } else if (n.type === 'DB_FIND') {
        const collection = String(n.config?.collection || 'default');
        const filters = Array.isArray(n.config?.filters) ? n.config.filters : [];
        let rows = getCollection(flowId, collection).slice();
        for (const f of filters) {
          const actualPath = String(f.field || '');
          const expected = resolveTemplate(String(f.value || ''), scope);
          rows = rows.filter(r => evalCond(deepGet(r, actualPath), f.op || '$eq', expected));
        }
        scope.db_result = rows;
      } else if (n.type === 'EXT_API_CALL') {
        const method = String(n.config?.method || 'GET').toUpperCase();
        const timeoutMs = Math.min(5000, Number(n.config?.timeoutMs || 5000));
        const controller = new AbortController();
        const t = setTimeout(() => controller.abort(), timeoutMs);
        try {
          const r = await fetch(resolveTemplate(n.config?.url || '', scope), {
            method,
            headers: n.config?.headers || {},
            body: ['GET','HEAD'].includes(method) ? undefined : JSON.stringify(n.config?.body || scope.req.body),
            signal: controller.signal
          });
          scope.vars.ext = { status: r.status, ok: r.ok, body: await r.text() };
          clearTimeout(t);
          currentId = r.ok ? String(n.onSuccess || n.next || '') : String(n.onFailure || n.next || '');
          continue;
        } catch {
          clearTimeout(t);
          currentId = String(n.onFailure || n.next || '');
          continue;
        }
      } else if (n.type === 'CONDITION') {
        const actual = deepGet(scope, n.config?.left || 'req.body.value');
        const expected = resolveTemplate(String(n.config?.right || ''), scope);
        currentId = evalCond(actual, n.config?.op || '$eq', expected) ? String(n.onTrue || '') : String(n.onFalse || '');
        continue;
      } else if (n.type === 'HTTP_RESPONSE') {
        const status = Number(n.config?.status || 200);
        const payload = n.config?.json && typeof n.config.json === 'object' ? JSON.parse(resolveTemplate(JSON.stringify(n.config.json), scope)) : { ok: true };
        if (scope.vars.token) res.setHeader('x-logiflow-token', scope.vars.token);
        logExec({ flowId, nodeId: currentId, finalResponse: { status, payload } });
        return res.status(status).json(payload);
      }
      currentId = String(n.next || '');
    }
    return res.status(500).json({ error: 'Flow ended without HTTP_RESPONSE node' });
  } catch (e) {
    virtualDatabase[flowId] = rollbackSnapshot;
    saveVirtualDb();
    logExec({ flowId, error: e.message });
    return res.status(500).json({ error: e.message });
  }
}

app.all('/api/simulated/:flowId/*', executeFlowRequest);
app.all('/api/simulated/:flowId', executeFlowRequest);
app.all('/api/live/:flowId/*', executeFlowRequest);
app.all('/api/live/:flowId', executeFlowRequest);

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


const PLAN_RUNTIME_PROFILES = {
  free:    { memoryLimit: '800m',  cpuShares: 256,  memorySwap: '1g' },
  starter: { memoryLimit: '1g',    cpuShares: 384,  memorySwap: '1536m' },
  pro:     { memoryLimit: '2g',    cpuShares: 640,  memorySwap: '3g' },
  growth:  { memoryLimit: '5g',    cpuShares: 1024, memorySwap: '6g' },
  scale:   { memoryLimit: '6g',    cpuShares: 1536, memorySwap: '8g' }
};

// Disk quota per plan (bytes). Enforced before every build.
const PLAN_DISK_LIMITS = {
  free:    2  * 1024 * 1024 * 1024,   //  2 GB
  starter: 10 * 1024 * 1024 * 1024,   // 10 GB
  pro:     35 * 1024 * 1024 * 1024,   // 35 GB
  growth:  60 * 1024 * 1024 * 1024,   // 60 GB
  scale:   Infinity                    // unlimited
};

// Bandwidth quota per plan (bytes/month) — tracked via nginx logs externally;
// stored here for reference and API enforcement responses.
const PLAN_BANDWIDTH_LIMITS = {
  free:    100  * 1024 * 1024 * 1024,  // 100 GB
  starter: 250  * 1024 * 1024 * 1024,  // 250 GB
  pro:     500  * 1024 * 1024 * 1024,  // 500 GB
  growth:  1024 * 1024 * 1024 * 1024,  // 1 TB
  scale:   Infinity
};

function getDiskUsedBytesForUser(userId) {
  try {
    const siteDir = process.env.SITES_DIR || '/var/www/user-sites';
    const userDir = require('path').join(siteDir, String(userId || ''));
    const out = require('child_process').execSync(
      `du -sb "${userDir}" 2>/dev/null || echo 0`,
      { encoding: 'utf8', timeout: 10000 }
    ).trim().split(/\s+/)[0];
    return Number(out) || 0;
  } catch { return 0; }
}

function checkDiskQuota(userId, planKey) {
  const limit = PLAN_DISK_LIMITS[String(planKey || 'free').toLowerCase()] ?? PLAN_DISK_LIMITS.free;
  if (!Number.isFinite(limit)) return { ok: true };
  const used = getDiskUsedBytesForUser(userId);
  if (used >= limit) {
    const usedGb = (used / (1024**3)).toFixed(2);
    const limitGb = (limit / (1024**3)).toFixed(0);
    return { ok: false, usedGb, limitGb, error: `Disk quota exceeded: ${usedGb} GB used of your ${limitGb} GB ${planKey} plan limit. Delete unused projects or upgrade your plan.` };
  }
  return { ok: true };
}
function getRuntimeProfileForPlan(planKey='free') {
  const key = String(planKey || 'free').toLowerCase();
  return PLAN_RUNTIME_PROFILES[key] || PLAN_RUNTIME_PROFILES.free;
}


const PLAN_DB_API_LIMITS = {
  free:    { maxDatabases: 3,   maxDbMemoryBytes: 512  * 1024 * 1024,        maxApis: 5 },
  starter: { maxDatabases: 8,   maxDbMemoryBytes: 1    * 1024 * 1024 * 1024, maxApis: 20 },
  pro:     { maxDatabases: 20,  maxDbMemoryBytes: 2    * 1024 * 1024 * 1024, maxApis: 60 },
  growth:  { maxDatabases: 50,  maxDbMemoryBytes: 5    * 1024 * 1024 * 1024, maxApis: 150 },
  scale:   { maxDatabases: 200, maxDbMemoryBytes: 16   * 1024 * 1024 * 1024, maxApis: 500 }
};

function parseMemToBytes(v = '') {
  const s = String(v || '').trim().toLowerCase();
  const m = s.match(/^(\d+(?:\.\d+)?)([mg])$/);
  if (!m) return 0;
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return m[2] === 'g' ? Math.round(n * 1024 * 1024 * 1024) : Math.round(n * 1024 * 1024);
}
function bytesToDockerMem(bytes = 0) {
  const mb = Math.max(256, Math.floor(bytes / (1024 * 1024)));
  if (mb % 1024 === 0) return `${mb / 1024}g`;
  return `${mb}m`;
}

function getHostTotalMemoryBytes() {
  try {
    const m = fs.readFileSync('/proc/meminfo', 'utf8');
    const match = m.match(/^MemTotal:\s+(\d+)\s+kB$/m);
    if (match) return Number(match[1]) * 1024;
  } catch {}
  return 0;
}
function getDbMemoryBudgetBytes() {
  const total = getHostTotalMemoryBytes();
  const pct = Math.max(10, Math.min(90, Number(process.env.DB_MEMORY_BUDGET_PERCENT || 45) || 45));
  if (!total) return 1024 * 1024 * 1024; // fallback 1GB safety budget
  return Math.floor(total * (pct / 100));
}
function plannedDbMemoryBytes(db = {}) {
  const b = parseMemToBytes(db.memory || '0');
  return b > 0 ? b : (512 * 1024 * 1024);
}

async function getUserPlanKey(user) {
  const fallback = String(user?.billingPlan || user?.workspace?.settings?.billingPlan || 'free').toLowerCase();
  const ws = await readWorkspaceFromFirebase(user).catch(() => null);
  const wsPlan = String(ws?.settings?.billingPlan || '').toLowerCase();
  const key = wsPlan || fallback || 'free';
  return PLAN_DB_API_LIMITS[key] ? key : 'free';
}

// ── Deploy endpoint ───────────────────────────────────────────────────────────
app.post('/api/deploy', requireAuth, async (req, res) => {
  const { name, subdomain, repoUrl, branch, installCmd, buildCmd,
          startCmd, outputDir, nodeVer, siteType, envVars,
          isDockerfileDeploy, isWorker, dockerfilePath, exposedPort, billingPlan } = req.body;
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

  // Ensure project name/subdomain are available before creating/updating.
  // Allow redeploy updates for the same existing subdomain record.
  const existingBySub = await Project.findOne({ subdomain: cleanSub }).select('_id name subdomain').lean().maxTimeMS(5000).catch(() => null);
  const existingByName = await Project.findOne({ name }).select('_id name subdomain').lean().maxTimeMS(5000).catch(() => null);
  const existingBySubId = existingBySub?._id ? String(existingBySub._id) : '';
  const existingByNameId = existingByName?._id ? String(existingByName._id) : '';
  if (existingByName && existingBySubId && existingByNameId !== existingBySubId) {
    return res.status(409).json({ error: 'Project name is unavailable. Please choose another name.' });
  }
  if (existingBySub && existingByName && existingBySubId !== existingByNameId) {
    return res.status(409).json({ error: 'Subdomain is unavailable. Please choose another subdomain.' });
  }

  // Assign port for server apps
  let appPort = 0;
  if (isServerApp) {
    try { appPort = getOrAssignPort(cleanSub); }
    catch(e) { return res.status(500).json({ error: e.message }); }
  }

  const planKey = String(billingPlan || 'free').toLowerCase();
  const runtimeProfile = getRuntimeProfileForPlan(planKey);

  // ── Disk quota check ──────────────────────────────────────────────────────
  const ownerIdForDisk = String(req.user?._id || req.user?.id || '');
  const diskCheck = checkDiskQuota(ownerIdForDisk, planKey);
  if (!diskCheck.ok) return res.status(403).json({ error: diskCheck.error });

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
        billingPlan: planKey,
        memoryLimit: runtimeProfile.memoryLimit,
        cpuShares: runtimeProfile.cpuShares,
        memorySwap: runtimeProfile.memorySwap,
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
      siteType: siteType||'static', appPort, billingPlan: planKey, memoryLimit: runtimeProfile.memoryLimit, cpuShares: runtimeProfile.cpuShares, memorySwap: runtimeProfile.memorySwap, envVars: envVars||{},
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
  deployStopRequests.delete(deployId);
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
    if (deployStopRequests.has(deployId)) throw new Error('Deployment stopped by user');
    const ownerUser = project?.ownerUserId
      ? await User.findById(project.ownerUserId).select('email').lean().catch(() => null)
      : null;
    const notifyEmail = ownerUser?.email || req.user?.email || '';
    const totalDeploymentsAtStart = await Deployment.countDocuments({ projectId: project._id }).catch(() => 0);
    const notifyStart = await sendDeploymentStatusEmail({
      userEmail: notifyEmail,
      projectName: name,
      subdomain: cleanSub,
      branch: branch || 'main',
      status: 'success',
      duration: 0,
      source: deploySource,
      liveUrl: `https://${cleanSub}.${BASE_DOMAIN}`,
      sha: triggerSha,
      repoUrl,
      buildStatus: 'building',
      deployStatus: 'in_progress',
      memoryLimit: String(project?.memoryLimit || ''),
      cpuShares: String(project?.cpuShares || ''),
      totalDeployments: totalDeploymentsAtStart,
      deployedAt: deployment.startedAt || new Date(),
      phase: 'started'
    }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
    if (!notifyStart.ok && !notifyStart.skipped) {
      emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment start email could not be sent (${notifyStart.reason || 'unknown'}).` });
    }

    emit('build:log', { line: `\x1b[36m[Joytree]\x1b[0m Building \x1b[1m${name}\x1b[0m` });
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
    // Resolve envVars as a plain object from every possible source:
    // 1. envVars stored on the project record in DB (set via Env Manager page)
    // 2. envVars from the request body (deploy form / redeploy) — override DB vars
    // This ensures env vars saved via the Env Manager always travel into the build.
    const projectEnvVarsRaw = project?.envVars instanceof Map
      ? Object.fromEntries(project.envVars)
      : (project?.envVars && typeof project.envVars === 'object' ? { ...project.envVars } : {});
    const requestEnvVarsRaw = envVars && typeof envVars === 'object' ? envVars : {};
    const resolvedEnvVars = Object.assign({}, projectEnvVarsRaw, requestEnvVarsRaw);

    // Log env var count (keys only — never log values for security)
    const envKeyCount = Object.keys(resolvedEnvVars).length;
    if (envKeyCount > 0) {
      emit('build:log', { line: `\x1b[90m[env] Injecting ${envKeyCount} environment variable${envKeyCount > 1 ? 's' : ''}: ${Object.keys(resolvedEnvVars).join(', ')}\x1b[0m` });
    }

    await runBuild({
      deployId, project, sitesDir: SITES_DIR, tmpDir: TMP_DIR,
      githubToken: deployGithubToken, appPort, emit,
      envVars: resolvedEnvVars,
      isDockerfileDeploy: !!isDockerfileDeploy,
      isWorker:           !!isWorker,
      onLog: (line) => {
        if (deployStopRequests.has(deployId)) throw new Error('Deployment stopped by user');
        deployment.logs = deployment.logs || [];
        deployment.logs.push(line);
        if (!saveTimer) {
          saveTimer = setTimeout(async () => { saveTimer = null; await flushLogs(); }, 1500);
        }
      }
    });
    if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
    await flushLogs();
    if (deployStopRequests.has(deployId)) throw new Error('Deployment stopped by user');

    // Register CF subdomain
    emit('build:log', { line: `\x1b[36m[Joytree]\x1b[0m Registering subdomain…` });
    const cf = await registerSubdomain(cleanSub);
    if (cf.ok) {
      emit('build:log', { line: `\x1b[32m[CF]\x1b[0m Live at: \x1b[1m${cf.url}\x1b[0m` });
      try { await Project.findByIdAndUpdate(project._id, { liveUrl: cf.url }); } catch(e) {}
      const ghHomepageSync = await syncRepoHomepageToLiveUrl(repoUrl, cf.url, deployGithubToken);
      if (ghHomepageSync.ok) {
        emit('build:log', { line: `\x1b[32m[GitHub]\x1b[0m Repo homepage synced: \x1b[1m${cf.url}\x1b[0m` });
      } else if (!ghHomepageSync.skipped) {
        emit('build:log', { line: `\x1b[33m[GitHub]\x1b[0m Could not sync repository homepage automatically.` });
      }
    }
    if (!isServerApp) {
      const live = cf?.url || `https://${cleanSub}.${BASE_DOMAIN}`;
      emit('build:log', { line: `\x1b[90m[static] OAuth note: add ${live} callback URL(s) in GitHub/Google OAuth settings.\x1b[0m` });
      emit('build:log', { line: `\x1b[90m[static] Turnstile note: add ${cleanSub}.${BASE_DOMAIN} to your widget domain allowlist.\x1b[0m` });
    }

    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'success'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    const ownerUserSuccess = project?.ownerUserId
      ? await User.findById(project.ownerUserId).select('email').lean().catch(() => null)
      : null;
    const totalDeployments = await Deployment.countDocuments({ projectId: project._id }).catch(() => 0);
    const notifyEmailSuccess = ownerUserSuccess?.email || req.user?.email || '';
    const liveUrl = cf?.url || `https://${cleanSub}.${BASE_DOMAIN}`;
    const notifySuccess = await sendDeploymentStatusEmail({
      userEmail: notifyEmailSuccess,
      projectName: name,
      subdomain: cleanSub,
      branch: branch || 'main',
      status: 'success',
      duration,
      source: deploySource,
      liveUrl,
      sha: triggerSha,
      repoUrl,
      buildStatus: 'building',
      deployStatus: 'success',
      memoryLimit: String(project?.memoryLimit || ''),
      cpuShares: String(project?.cpuShares || ''),
      totalDeployments,
      deployedAt: deployment.endedAt
    }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
    if (!notifySuccess.ok && !notifySuccess.skipped) {
      emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment email could not be sent (${notifySuccess.reason || 'unknown'}).` });
    }
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
    const wasStopped = /stopped by user/i.test(String(buildErr.message || ''));
    addActivity('deploy', (wasStopped ? '⏹ Deployment stopped: ' : (deploySource === 'auto' ? '✗ Automatic deployment failed: ' : '✗ Deployment failed: ')) + name + ' — ' + buildErr.message.slice(0,80));
    const buildDir = path.join(TMP_DIR, deployId);
    try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
    const safeErr = sanitizeSecrets(buildErr.message);
    const ownerUserFailure = project?.ownerUserId
      ? await User.findById(project.ownerUserId).select('email').lean().catch(() => null)
      : null;
    const totalDeployments = await Deployment.countDocuments({ projectId: project._id }).catch(() => 0);
    const notifyEmailFailure = ownerUserFailure?.email || req.user?.email || '';
    const notifyFailure = await sendDeploymentStatusEmail({
      userEmail: notifyEmailFailure,
      projectName: name,
      subdomain: cleanSub,
      branch: branch || 'main',
      status: 'failed',
      duration,
      source: deploySource,
      liveUrl: `https://${cleanSub}.${BASE_DOMAIN}`,
      sha: triggerSha,
      errorMessage: safeErr,
      repoUrl,
      buildStatus: 'failed',
      deployStatus: 'failed',
      memoryLimit: String(project?.memoryLimit || ''),
      cpuShares: String(project?.cpuShares || ''),
      totalDeployments,
      deployedAt: deployment.endedAt
    }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
    if (!notifyFailure.ok && !notifyFailure.skipped) {
      emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment email could not be sent (${notifyFailure.reason || 'unknown'}).` });
    }
    emit('build:log', { line: `\x1b[31m[Joytree]\x1b[0m Build failed: ${safeErr}` });
    emit('build:done', { status: wasStopped ? 'canceled' : 'failed', duration });
    console.error(`[Deploy] FAILED ${name}:`, sanitizeSecrets(buildErr.message));
    deployStopRequests.delete(deployId);
  }
});

app.post('/api/deploy/:deployId/stop', requireAuth, async (req, res) => {
  try {
    const deployId = String(req.params.deployId || '').trim();
    if (!deployId) return res.status(400).json({ error: 'deployId required' });
    const dep = await Deployment.findById(deployId);
    if (!dep) return res.status(404).json({ error: 'Deployment not found' });
    const project = await findProjectByAnyId(dep.projectId);
    if (project?.ownerUserId && String(project.ownerUserId) !== String(req.user?._id || req.user?.id || '')) return res.status(403).json({ error: 'Forbidden' });
    deployStopRequests.add(deployId);
    dep.status = 'failed';
    dep.endedAt = new Date();
    dep.logs = dep.logs || [];
    dep.logs.push('[manual] Deployment stop requested by user.');
    await dep.save().catch(()=>{});
    io.emit('build:log', { deployId, projectId: String(dep.projectId || ''), line: '\x1b[33m[Joytree]\x1b[0m Stop requested by user. Attempting to halt build…' });
    io.emit('build:done', { deployId, projectId: String(dep.projectId || ''), status: 'canceled' });
    res.json({ ok: true, message: 'Stop requested' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log('[Socket.io] Connected:', socket.id);
  socket.on('disconnect', () => console.log('[Socket.io] Disconnected:', socket.id));
});




async function findProjectByAnyId(id) {
  if (!isDbReady()) return null;
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
        ? 'Joytree will poll GitHub with this user OAuth token and trigger a deploy when the configured branch SHA changes. No repository webhook is required.'
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




function paystackSignatureValid(req) {
  if (!PAYSTACK_WEBHOOK_SECRET) return false;
  const sig = String(req.headers['x-paystack-signature'] || '').trim().toLowerCase();
  if (!sig || !req.rawBody) return false;
  const expected = crypto.createHmac('sha512', PAYSTACK_WEBHOOK_SECRET).update(req.rawBody).digest('hex').toLowerCase();
  return timingSafeEqualString(sig, expected);
}

async function markPaystackReferenceProcessed(reference = '', payload = {}) {
  const ref = String(reference || '').trim();
  if (!ref || !FIREBASE_RTDB_URL) return { ok: false, duplicate: false };
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  const url = `${FIREBASE_RTDB_URL}/deployboard_paystack_events/${encodeURIComponent(ref)}.json${authQuery}`;
  const existingRes = await fetch(url, { method: 'GET', headers: { 'Accept': 'application/json' } }).catch(() => null);
  if (!existingRes || !existingRes.ok) return { ok: false, duplicate: false };
  const existing = await existingRes.json().catch(() => null);
  if (existing) return { ok: true, duplicate: true };
  const wr = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).catch(() => null);
  return { ok: !!(wr && wr.ok), duplicate: false };
}

app.get('/api/billing/paystack/config', requireAuth, async (_req, res) => {
  res.json({
    ok: true,
    publicKey: PAYSTACK_PUBLIC_KEY,
    configured: !!(PAYSTACK_PUBLIC_KEY && PAYSTACK_SECRET_KEY)
  });
});


app.post('/api/billing/paystack/initialize', requireAuth, async (req, res) => {
  try {
    if (!PAYSTACK_SECRET_KEY || !PAYSTACK_PUBLIC_KEY) return res.status(503).json({ error: 'Paystack is not configured on server' });
    const firstName = String(req.body?.firstName || '').trim();
    const lastName = String(req.body?.lastName || '').trim();
    const email = String(req.body?.email || req.user?.email || '').trim();
    const plan = String(req.body?.plan || '').trim().toLowerCase();
    const amountKobo = Number(req.body?.amountKobo || 0);
    const phone = String(req.body?.phone || '').trim();
    if (!firstName) return res.status(400).json({ error: 'firstName is required' });
    if (!lastName) return res.status(400).json({ error: 'lastName is required' });
    if (!email) return res.status(400).json({ error: 'email is required' });
    if (!Number.isFinite(amountKobo) || amountKobo < 100) return res.status(400).json({ error: 'amountKobo must be at least 100' });

    const r = await fetch('https://api.paystack.co/transaction/initialize', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${PAYSTACK_SECRET_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        email,
        first_name: firstName,
        last_name: lastName,
        amount: Math.round(amountKobo),
        currency: 'GHS',
        callback_url: `https://${BASE_DOMAIN}/dashboard/checkout`,
        channels: ['mobile_money'],
        metadata: {
          plan,
          custom_fields: [
            { display_name: 'Plan', variable_name: 'plan', value: plan },
            { display_name: 'Phone', variable_name: 'phone', value: phone },
            { display_name: 'First name', variable_name: 'first_name', value: firstName },
            { display_name: 'Last name', variable_name: 'last_name', value: lastName }
          ]
        }
      })
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d?.status !== true || !d?.data?.reference) {
      return res.status(400).json({ error: d?.message || 'Failed to initialize Paystack transaction' });
    }
    return res.json({
      ok: true,
      reference: String(d.data.reference || ''),
      accessCode: String(d.data.access_code || ''),
      authorizationUrl: String(d.data.authorization_url || '')
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/billing/paystack/verify', requireAuth, async (req, res) => {
  try {
    if (!PAYSTACK_SECRET_KEY) return res.status(503).json({ error: 'Paystack secret key is not configured' });
    const reference = String(req.body?.reference || '').trim();
    if (!reference) return res.status(400).json({ error: 'reference is required' });

    const r = await fetch(`https://api.paystack.co/transaction/verify/${encodeURIComponent(reference)}`, {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${PAYSTACK_SECRET_KEY}`,
        'Content-Type': 'application/json'
      }
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok || d?.status !== true) {
      return res.status(400).json({ error: d?.message || 'Paystack verification failed' });
    }

    const tx = d?.data || {};
    const amountKobo = Number(tx.amount || 0);
    const currency = String(tx.currency || '').toUpperCase();
    const paidAt = tx.paid_at || tx.paidAt || null;
    const customerEmail = String(tx.customer?.email || '').trim().toLowerCase();
    const requestEmail = String(req.user?.email || '').trim().toLowerCase();
    const metadataPlan = String(tx.metadata?.custom_fields?.find?.(f => String(f?.variable_name || '').toLowerCase() === 'plan')?.value || tx.metadata?.plan || '').toLowerCase();

    if (tx.status !== 'success') return res.status(400).json({ error: 'Payment not successful' });
    if (currency && currency !== 'GHS') return res.status(400).json({ error: `Unexpected currency: ${currency}` });
    if (requestEmail && customerEmail && customerEmail !== requestEmail) return res.status(403).json({ error: 'Payment email does not match signed-in user' });

    // Persist subscription directly on verify path (webhook-safe, restart-safe)
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.settings = ws.settings && typeof ws.settings === 'object' ? ws.settings : {};
    if (metadataPlan) ws.settings.billingPlan = metadataPlan;
    ws.settings.subscriptionStatus = 'active';
    ws.settings.subscriptionReference = reference;
    ws.settings.subscriptionVerifiedAt = new Date().toISOString();
    ws.settings.subscriptionActivatedAt = new Date().toISOString();
    ws.settings.subscriptionPaidAmountKobo = Number(amountKobo || 0);
    ws.settings.subscriptionCurrency = String(currency || 'GHS').toUpperCase();
    ws.settings.subscriptionEmail = customerEmail || requestEmail || '';
    let wsSaved = await writeWorkspaceToFirebase(req.user, ws);
    if (!wsSaved && (customerEmail || requestEmail)) {
      // Fallback write by email key for cases where auth user shape changed across restarts.
      wsSaved = await writeWorkspaceToFirebase({ email: customerEmail || requestEmail }, ws);
    }

    const mailResult = await sendPaymentSuccessEmail({ userEmail: customerEmail || requestEmail, plan: metadataPlan || null, amountKobo, currency: currency || 'GHS', reference, paidAt }).catch((e) => ({ ok:false, skipped:false, reason:e.message || 'send_exception' }));
    if (!wsSaved) console.warn('[Billing] Subscription not persisted to Firebase after verify:', reference);
    if (!mailResult?.ok) console.warn('[Billing] Payment success email not sent (verify):', mailResult?.reason || 'unknown');
    else console.log('[Billing] Payment success email sent (verify):', reference);

    return res.json({
      ok: true,
      reference,
      amountKobo,
      currency,
      paidAt,
      plan: metadataPlan || null,
      customerEmail: customerEmail || null,
      gatewayResponse: tx.gateway_response || '',
      warnings: { firebasePersisted: !!wsSaved, emailSent: !!mailResult?.ok }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


app.post('/api/paystack/webhook', async (req, res) => {
  try {
    if (!PAYSTACK_WEBHOOK_SECRET) return res.status(503).json({ error: 'Paystack webhook signing key not configured' });
    if (!paystackSignatureValid(req)) return res.status(401).json({ error: 'Invalid paystack signature' });

    const event = String(req.body?.event || '').trim();
    if (event !== 'charge.success') return res.json({ ok: true, ignored: true, event });

    const data = req.body?.data || {};
    const reference = String(data.reference || '').trim();
    const email = String(data.customer?.email || '').trim().toLowerCase();
    const plan = String(data.metadata?.plan || data.metadata?.custom_fields?.find?.(f => String(f?.variable_name || '').toLowerCase() === 'plan')?.value || '').toLowerCase();

    if (!reference || !email) return res.status(400).json({ error: 'Missing reference or customer email' });

    const idempotency = await markPaystackReferenceProcessed(reference, {
      reference,
      event,
      email,
      plan,
      paidAt: data.paid_at || data.paidAt || new Date().toISOString(),
      amount: Number(data.amount || 0),
      currency: String(data.currency || '').toUpperCase(),
      createdAt: new Date().toISOString()
    });
    if (idempotency.ok && idempotency.duplicate) return res.json({ ok: true, duplicate: true });

    const userLike = { email };
    const ws = (await readWorkspaceFromFirebase(userLike)) || {};
    ws.settings = ws.settings && typeof ws.settings === 'object' ? ws.settings : {};
    if (plan) ws.settings.billingPlan = plan;
    ws.settings.subscriptionStatus = 'active';
    ws.settings.subscriptionReference = reference;
    ws.settings.subscriptionVerifiedAt = new Date().toISOString();
    ws.settings.subscriptionActivatedAt = new Date().toISOString();
    ws.settings.subscriptionPaidAmountKobo = Number(data.amount || 0);
    ws.settings.subscriptionCurrency = String(data.currency || 'GHS').toUpperCase();
    ws.settings.subscriptionEmail = email;

    const saved = await writeWorkspaceToFirebase(userLike, ws);
    if (!saved) return res.status(503).json({ error: 'Failed to persist subscription update in Firebase' });

    const webhookMail = await sendPaymentSuccessEmail({ userEmail: email, plan: plan || null, amountKobo: Number(data.amount || 0), currency: String(data.currency || 'GHS').toUpperCase(), reference, paidAt: data.paid_at || data.paidAt || new Date().toISOString() }).catch((e) => ({ ok:false, skipped:false, reason:e.message || 'send_exception' }));
    if (!webhookMail?.ok) console.warn('[Billing] Payment success email not sent (webhook):', webhookMail?.reason || 'unknown');
    else console.log('[Billing] Payment success email sent (webhook):', reference);

    res.json({ ok: true, reference, plan: plan || null });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/webhook/global-secret', requireAuth, async (req, res) => {
  res.json({
    ok:true,
    secret: GLOBAL_WEBHOOK_SECRET || '',
    webhookUrl: `${getPublicOrigin(req)}/api/github/webhook`,
    envConfigured: !!(process.env.GLOBAL_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET),
    note: 'Use this one GitHub webhook secret for every repository webhook. Joytree matches the pushed repository/branch to all enabled projects.'
  });
});

app.post('/api/webhook/global-secret/regenerate', requireAuth, async (req, res) => {
  GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
  res.json({ ok:true, secret: GLOBAL_WEBHOOK_SECRET, webhookUrl: `${getPublicOrigin(req)}/api/github/webhook`, envConfigured: false, note: 'Runtime value updated. Persist in .env as GLOBAL_WEBHOOK_SECRET after restart.' });
});

// ── Dashboard static serving — MUST be after all API routes ──────────────────

// ══════════════════════════════════════════════════════════════════════════════
// DATABASE MANAGEMENT — Docker-backed managed databases
// ══════════════════════════════════════════════════════════════════════════════

const { execSync, exec: execAsync } = require('child_process');

const DB_ENGINE_CONFIG = {
  mongodb:  { image: 'mongo:7',     defaultPort: 27017, envVars: (u,p,d) => [`MONGO_INITDB_ROOT_USERNAME=${u}`, `MONGO_INITDB_ROOT_PASSWORD=${p}`, `MONGO_INITDB_DATABASE=${d}`] },
  postgres: { image: 'postgres:16', defaultPort: 5432,  envVars: (u,p,d) => [`POSTGRES_USER=${u}`, `POSTGRES_PASSWORD=${p}`, `POSTGRES_DB=${d}`] },
  mysql:    { image: 'mysql:8',     defaultPort: 3306,  envVars: (u,p,d) => {
    const vars = [`MYSQL_ROOT_PASSWORD=${p}`, `MYSQL_DATABASE=${d}`];
    if (u && u !== 'root') vars.push(`MYSQL_USER=${u}`, `MYSQL_PASSWORD=${p}`);
    return vars;
  } },
  mariadb:  { image: 'mariadb:11',  defaultPort: 3306,  envVars: (u,p,d) => {
    const vars = [`MARIADB_ROOT_PASSWORD=${p}`, `MARIADB_DATABASE=${d}`];
    if (u && u !== 'root') vars.push(`MARIADB_USER=${u}`, `MARIADB_PASSWORD=${p}`);
    return vars;
  } },
  redis:    { image: 'redis:7',     defaultPort: 6379,  envVars: (_u,p)  => p ? [`requirepass ${p}`] : [] }
};

// Find a free port in range 14000–15000 avoiding already-used ones
function findFreeDbPort() {
  try {
    const used = execSync("docker ps --format '{{.Ports}}' 2>/dev/null || echo ''", { encoding: 'utf8' });
    const usedPorts = new Set([...used.matchAll(/:(\d+)->/g)].map(m => Number(m[1])));
    for (let p = 14000; p <= 15000; p++) {
      if (!usedPorts.has(p)) return p;
    }
  } catch { /* fall through */ }
  return 14000 + Math.floor(Math.random() * 900);
}

// Run docker command and return { ok, stdout, stderr }
// timeoutMs defaults to 3 minutes — long enough for a cold image pull
function runDocker(cmd, timeoutMs = 180000) {
  try {
    const stdout = execSync(cmd, {
      encoding: 'utf8',
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: timeoutMs
    }).trim();
    return { ok: true, stdout };
  } catch (e) {
    const eStdout = String(e.stdout || '').trim();
    const eStderr = String(e.stderr || '').trim();
    // execSync throws with signal SIGTERM when it hits the timeout
    if (e.signal === 'SIGTERM' || e.killed) {
      return {
        ok: false, stdout: '',
        stderr: `Docker command timed out after ${Math.round(timeoutMs / 1000)}s. ` +
                `The image may still be pulling in the background — try again in a minute.`
      };
    }
    // With 2>&1 in the shell command, DB error text lands in e.stdout (not e.stderr).
    // Return both so the query route can surface the real DB error message.
    return { ok: false, stdout: eStdout, stderr: eStderr || String(e.message || '') };
  }
}

// Check if a container exists (running or stopped)
function containerExists(name) {
  // Hard 2-second cap so a slow Docker daemon never blocks the event loop
  const r = runDocker(`timeout 2 docker inspect --format '{{.State.Status}}' ${name} 2>/dev/null || echo ''`, 4000);
  return r.stdout && r.stdout !== '';
}

// Get container running state
// Uses `timeout 2` (Unix coreutils) so a busy Docker daemon can't stall the
// Node.js event loop longer than ~2 s per container — critical when GET
// /api/databases calls this for every DB and the client has an 8-second abort.
function containerStatus(name) {
  const r = runDocker(`timeout 2 docker inspect --format '{{.State.Status}}' ${name} 2>/dev/null || echo 'missing'`, 4000);
  const s = (r.stdout || 'missing').trim();
  if (s === 'running') return 'running';
  if (s === 'exited' || s === 'created' || s === 'paused') return 'stopped';
  return 'error';
}

// Provision a new Docker database container
async function provisionDbContainer(db) {
  const cfg = DB_ENGINE_CONFIG[db.engine];
  if (!cfg) throw new Error(`Unknown engine: ${db.engine}`);

  const containerName = `jt-db-${db.name}-${String(db._id).slice(-6)}`;
  const hostPort      = findFreeDbPort();
  const volumePath    = db.volume || `/var/joytree-data/${containerName}`;
  const imageToUse    = db.image || cfg.image;

  // Ensure volume dir exists
  try { execSync(`mkdir -p ${volumePath}`, { encoding: 'utf8' }); } catch {}

  // ── Pre-pull the image (3-minute timeout) ──────────────────────────────────
  // docker run would pull silently but hang with no feedback. Doing it explicitly
  // here means (a) we get a clear error if the registry is unreachable and (b)
  // the subsequent docker run completes in seconds because the layers are cached.
  console.log(`[DB] Pulling image ${imageToUse} for "${db.name}"…`);
  const pullResult = runDocker(`docker pull ${imageToUse}`, 180000);
  if (!pullResult.ok) {
    throw new Error(`Failed to pull Docker image "${imageToUse}": ${pullResult.stderr}`);
  }
  console.log(`[DB] Image ${imageToUse} ready.`);

  // Build env flags
  const envFlags = cfg.envVars(db.user, db.pass, db.dbName)
    .map(e => `-e "${e}"`)
    .join(' ');

  // Redis uses a config arg for password, not just env
  const redisArgs = db.engine === 'redis' && db.pass
    ? `--requirepass "${db.pass}"`
    : '';

  const volumeMount = db.engine === 'mongodb'  ? `/data/db`
                    : db.engine === 'postgres' ? `/var/lib/postgresql/data`
                    : db.engine === 'mysql'    ? `/var/lib/mysql`
                    : db.engine === 'redis'    ? `/data`
                    : `/data`;

  const cmd = [
    'docker run -d',
    `--name ${containerName}`,
    `--restart unless-stopped`,
    `-p 127.0.0.1:${hostPort}:${cfg.defaultPort}`,
    `-v "${volumePath}:${volumeMount}"`,
    `--memory="${db.memory || '128m'}"`,
    `--memory-swap="${db.memory || '128m'}"`,
    `--pids-limit=256`,
    `--cpus="${process.env.DB_CONTAINER_CPUS || '0.75'}"`,
    envFlags,
    imageToUse,
    redisArgs
  ].filter(Boolean).join(' ');

  // docker run itself (image already pulled) — 30-second timeout is plenty
  const result = runDocker(cmd, 30000);
  if (!result.ok) throw new Error(result.stderr || 'docker run failed');

  return { containerName, hostPort };
}

// Inject connection string(s) into a linked project's env vars.
// connType: 'internal' | 'external' | 'both' (default).
// internalConn  = Docker-internal / localhost connection string
// externalConn  = public-facing connection string (may equal internalConn if no gateway)
async function injectConnStrIntoProject(projectId, engine, internalConn, user = null, connType = 'both', externalConn = null) {
  if (!projectId || !internalConn) return;

  // Resolve external conn: use provided value, else derive from db object via externalDbConnStr
  const extConn = externalConn || internalConn;
  const type = String(connType || 'both').toLowerCase();

  // Key definitions per engine
  const envKeyDef = {
    mongodb:  { primary: 'MONGODB_URI',  secondary: 'DATABASE_URL' },
    postgres: { primary: 'DATABASE_URL', secondary: 'POSTGRES_URL' },
    mysql:    { primary: 'MYSQL_URL',    secondary: 'DATABASE_URL' },
    mariadb:  { primary: 'MARIADB_URL',  secondary: 'DATABASE_URL' },
    redis:    { primary: 'REDIS_URL',    secondary: null }
  };
  const keyDef = envKeyDef[engine] || { primary: 'DATABASE_URL', secondary: null };

  // Build the exact set of vars to write based on connType
  const patch = {};
  if (type === 'internal') {
    patch[keyDef.primary] = internalConn;
    if (keyDef.secondary) patch[keyDef.secondary] = internalConn;
  } else if (type === 'external') {
    patch[keyDef.primary] = extConn;
    if (keyDef.secondary) patch[keyDef.secondary] = extConn;
  } else {
    // 'both' — external as primary keys, internal as *_INTERNAL variant
    patch[keyDef.primary] = extConn;
    if (keyDef.secondary) patch[keyDef.secondary] = extConn;
    patch[`${keyDef.primary}_INTERNAL`] = internalConn;
  }

  try {
    // Primary: Mongo project doc (when available)
    if (isDbReady() && mongoose.Types.ObjectId.isValid(String(projectId))) {
      const updates = {};
      Object.entries(patch).forEach(([k, v]) => { updates[`envVars.${k}`] = v; });
      await Project.updateOne({ _id: projectId }, { $set: updates });
      if (user) {
        updateLocalWorkspaceProject(user, String(projectId), {
          envVars: {
            ...((user?.workspace?.projects||[]).find(p => String(p.id||p._id||'') === String(projectId))?.envVars || {}),
            ...patch
          }
        });
      }
      return;
    }

    // Fallback: Firebase workspace project list (Mongo-less mode)
    if (user) {
      const ws = (await readWorkspaceFromFirebase(user)) || {};
      ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
      const idx = ws.projects.findIndex(p =>
        String(p.id || p._id || '') === String(projectId) ||
        String(p.subdomain || '') === String(projectId) ||
        String(p.name || '') === String(projectId)
      );
      if (idx >= 0) {
        const p = ws.projects[idx] || {};
        const envVars = (p.envVars && typeof p.envVars === 'object') ? { ...p.envVars } : {};
        Object.entries(patch).forEach(([k, v]) => { envVars[k] = v; });
        ws.projects[idx] = { ...p, envVars, updatedAt: new Date().toISOString() };
        await writeWorkspaceToFirebase(user, ws);
      }
    }
  } catch (e) {
    console.warn('[DB] Failed to inject conn str into project:', e.message);
  }
}

// ── GET /api/databases — list all databases for the user ──────────────────────
app.get('/api/databases', requireAuth, async (req, res) => {
  try {
    // Firebase RTDB is the primary store — no MongoDB needed
    let dbs = await loadUserDatabases(req.user);
    // Sync live Docker status for each
    for (const db of dbs) {
      if (db.containerName) {
        const liveStatus = containerStatus(db.containerName);
        if (liveStatus !== db.status) {
          db.status = liveStatus;
          await persistDb(req.user, db);
        }
      }
    }
    res.json({ databases: dbs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

function normalizeDbCredentials(engine, dbUser, pass, dbName) {
  const safeUser = String(dbUser || '').trim();
  const safePass = String(pass || '').trim();
  const safeDbName = String(dbName || '').trim();
  const defaultUser = engine === 'postgres' ? 'postgres' : engine === 'mongodb' ? 'root' : engine === 'mysql' || engine === 'mariadb' ? 'root' : '';
  const user = safeUser || defaultUser;
  const password = safePass || crypto.randomBytes(12).toString('base64url');
  const databaseName = safeDbName || 'mydb';
  return { user, password, databaseName };
}

// ── POST /api/databases — provision a new database ───────────────────────────
app.post('/api/databases', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const { name, engine, image, user: dbUser, pass, dbName, memory, volume, linkProjectId, connStr } = req.body || {};

    const userPlanKey = await getUserPlanKey(req.user);
    const planLimits = PLAN_DB_API_LIMITS[userPlanKey] || PLAN_DB_API_LIMITS.free;

    if (!name)   return res.status(400).json({ error: 'name is required' });
    if (!engine) return res.status(400).json({ error: 'engine is required' });
    if (!DB_ENGINE_CONFIG[engine]) return res.status(400).json({ error: `Unknown engine: ${engine}` });

    // Sanitize name — alphanumeric and hyphens only
    const safeName = String(name).toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 40);
    if (!safeName) return res.status(400).json({ error: 'Invalid database name' });


    // Check for name collision across Firebase/local/Mongo-backed view
    const existingDbs = await loadUserDatabases(req.user);
    if (existingDbs.length >= Number(planLimits.maxDatabases || 0)) {
      return res.status(403).json({ error: `Database limit reached for ${userPlanKey} plan (${planLimits.maxDatabases} max). Upgrade to create more databases.` });
    }

    const requestedBytesRaw = parseMemToBytes(memory || '512m') || (512 * 1024 * 1024);
    const requestedBytesCappedPlan = Math.min(requestedBytesRaw, Number(planLimits.maxDbMemoryBytes || requestedBytesRaw));
    const hostBudgetBytes = getDbMemoryBudgetBytes();
    const existingBudgetUse = existingDbs.reduce((sum, db) => sum + plannedDbMemoryBytes(db), 0);
    if (existingBudgetUse + requestedBytesCappedPlan > hostBudgetBytes) {
      const toMb = b => Math.round(b / (1024 * 1024));
      return res.status(403).json({
        error: `Host memory budget exceeded for databases. Requested ${toMb(requestedBytesCappedPlan)}MB, current allocated ${toMb(existingBudgetUse)}MB, budget ${toMb(hostBudgetBytes)}MB. Reduce DB memory or upgrade VPS.`
      });
    }
    if (existingDbs.some(d => String(d.name || '').toLowerCase() === safeName)) {
      return res.status(409).json({ error: `A database named "${safeName}" already exists` });
    }

    // Normalize credentials server-side so every created DB has valid auth details
    const { user: normalizedUser, password: normalizedPass, databaseName: normalizedDbName } = normalizeDbCredentials(engine, dbUser, pass, dbName);

    // Create DB record first (provisioning state)
    let dbRecord;
    const cfg = DB_ENGINE_CONFIG[engine];
    const record = {
      name: safeName, engine,
      image: image || cfg.image,
      user: normalizedUser,
      pass: normalizedPass,
      dbName: normalizedDbName,
      memory: bytesToDockerMem(requestedBytesCappedPlan),
      volume: volume || '',
      connStr: connStr || '',
      status: 'provisioning',
      ownerUserId: userId,
      linkProjectId: linkProjectId || '',
      internalPort: cfg.defaultPort
    };

    if (isDbReady()) {
      dbRecord = await Database.create(record);
    } else {
      dbRecord = { ...record, _id: `local_${Date.now()}` };
    }

    // Run Docker provisioning asynchronously — don't block the HTTP response
    res.json({ ok: true, id: String(dbRecord._id), name: safeName, status: 'provisioning' });

    // Provision in background
    setImmediate(async () => {
      try {
        const { containerName, hostPort } = await provisionDbContainer({ ...record, _id: dbRecord._id });

        // Build the real connection string with normalized credentials
        const realConn = buildDbConnStr(engine, normalizedUser, normalizedPass, normalizedDbName, 'localhost', hostPort);

        if (isDbReady()) {
          await Database.updateOne({ _id: dbRecord._id }, {
            $set: { containerName, internalPort: hostPort, connStr: realConn, status: 'running', updatedAt: new Date() }
          });
        }

        // Inject conn str into linked project if set — default 'both' on creation
        if (linkProjectId) {
          const extConn = externalDbConnStr({ ...dbRecord, containerName, internalPort: hostPort, connStr: realConn });
          await injectConnStrIntoProject(linkProjectId, engine, realConn, req.user, 'both', extConn || realConn);
        }

        addActivity('database', `✅ Database "${safeName}" (${engine}) provisioned on port ${hostPort}`);
        console.log(`[DB] Provisioned ${engine} container "${containerName}" on port ${hostPort}`);

        // ── Notify all connected clients so the UI updates instantly ──────────
        const dbStatusPayload = {
          id:     String(dbRecord._id),
          name:   safeName,
          engine,
          status: 'running',
          containerName,
          hostPort,
          connStr: realConn
        };
        io.emit('db:status', dbStatusPayload);
        // Persist to Firebase (primary), local file, and Mongo — all three
        const newDbRec = {
          id: String(dbRecord._id),
          ownerUserId: String(dbRecord.ownerUserId),
          name: safeName, engine, status: 'running',
          containerName, internalPort: String(hostPort),
          connStr: realConn, connectionString: realConn,
          image: image || cfg.image, memory: dbRecord.memory || '512m',
          user: normalizedUser, pass: normalizedPass, dbName: normalizedDbName,
          createdAt: new Date().toISOString()
        };
        // req.user is captured in the setImmediate closure — use it directly
        // so Firebase key is derived from email (most reliable identifier)
        await persistDb(req.user, newDbRec);

      } catch (e) {
        console.error(`[DB] Provisioning failed for "${safeName}":`, e.message);

        // Always try to persist the error, even if isDbReady() flaps
        const errMsg = String(e.message).slice(0, 300);
        try {
          if (isDbReady()) {
            await Database.updateOne({ _id: dbRecord._id }, {
              $set: { status: 'error', errorMessage: errMsg, updatedAt: new Date() }
            });
          }
        } catch (dbErr) {
          console.error('[DB] Could not persist error status:', dbErr.message);
        }

        await persistDb(req.user, {
          id: String(dbRecord._id),
          ownerUserId: String(dbRecord.ownerUserId || userId),
          name: safeName,
          engine,
          status: 'error',
          errorMessage: errMsg,
          user: normalizedUser,
          pass: normalizedPass,
          dbName: normalizedDbName,
          updatedAt: new Date().toISOString()
        });

        // ── Notify all connected clients of the failure ────────────────────────
        io.emit('db:status', {
          id:     String(dbRecord._id),
          name:   safeName,
          engine,
          status: 'error',
          error:  errMsg
        });
      }
    });

  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Helper to build connection strings server-side
function buildDbConnStr(engine, user, pass, dbName, host, port) {
  const enc = s => encodeURIComponent(String(s || ''));
  switch (engine) {
    case 'mongodb':  return `mongodb://${enc(user||'root')}:${enc(pass)}@${host}:${port}/${dbName||'mydb'}`;
    case 'postgres': return `postgresql://${enc(user||'postgres')}:${enc(pass)}@${host}:${port}/${dbName||'mydb'}`;
    case 'mysql':    return `mysql://${enc(user||'root')}:${enc(pass)}@${host}:${port}/${dbName||'mydb'}`;
    case 'mariadb':  return `mariadb://${enc(user||'root')}:${enc(pass)}@${host}:${port}/${dbName||'mydb'}`;
    case 'redis':    return pass ? `redis://:${enc(pass)}@${host}:${port}` : `redis://${host}:${port}`;
    default:         return '';
  }
}

// ── GET /api/databases/:id — get a single database ───────────────────────────
app.get('/api/databases/:id', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });
    if (db.containerName) db.status = containerStatus(db.containerName);
    res.json({ ...db, id: String(db.id || db._id || '') });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/start ────────────────────────────────────────────
app.post('/api/databases/:id/start', requireAuth, async (req, res) => {
  try {
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => d.id === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not provisioned yet' });
    const result = runDocker(`docker start ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker start failed' });
    db.status = 'running'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    io.emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'running', connStr: db.connStr || db.connectionString || '' });
    addActivity('database', `▶ Database "${db.name}" started`);
    res.json({ ok: true, status: 'running' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/stop ─────────────────────────────────────────────
app.post('/api/databases/:id/stop', requireAuth, async (req, res) => {
  try {
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => d.id === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not provisioned yet' });
    const result = runDocker(`docker stop ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker stop failed' });
    db.status = 'stopped'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    io.emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'stopped' });
    addActivity('database', `⏹ Database "${db.name}" stopped`);
    res.json({ ok: true, status: 'stopped' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/restart ──────────────────────────────────────────
app.post('/api/databases/:id/restart', requireAuth, async (req, res) => {
  try {
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => d.id === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not provisioned yet' });
    const result = runDocker(`docker restart ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker restart failed' });
    db.status = 'running'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    io.emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'running', connStr: db.connStr || db.connectionString || '' });
    addActivity('database', `↺ Database "${db.name}" restarted`);
    res.json({ ok: true, status: 'running' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/delete ───────────────────────────────────────────
app.post('/api/databases/:id/delete', requireAuth, async (req, res) => {
  try {
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => d.id === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });

    // Stop and remove the container
    if (db.containerName) {
      runDocker(`docker stop ${db.containerName} 2>/dev/null || true`);
      runDocker(`docker rm -f ${db.containerName} 2>/dev/null || true`);
    }

    // Remove the volume directory
    if (db.volume && String(db.volume).startsWith('/var/joytree-data/')) {
      try { execSync(`rm -rf "${db.volume}"`, { encoding: 'utf8' }); } catch {}
    }

    // Remove from Firebase, local cache, and Mongo
    await removeDb(req.user, req.params.id);
    io.emit('db:deleted', { id: req.params.id });
    addActivity('database', `🗑 Database "${db.name}" deleted`);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/databases/:id/logs ───────────────────────────────────────────────
app.get('/api/databases/:id/logs', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });
    if (!db.containerName) return res.json({ logs: '' });

    const lines = Math.min(200, Number(req.query.lines || 100));
    const result = runDocker(`docker logs --tail ${lines} ${db.containerName} 2>&1`);
    res.json({ logs: result.stdout || result.stderr || '' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/link ─────────────────────────────────────────────
// Link a database to a project — injects conn string as env var
app.post('/api/databases/:id/link', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });

    const { projectId } = req.body || {};
    if (!projectId) return res.status(400).json({ error: 'projectId is required' });

    let project = null;
    if (isDbReady()) project = await findProjectByAnyId(projectId);
    if (!project) {
      const ws = (await readWorkspaceFromFirebase(req.user)) || {};
      const wsProjects = Array.isArray(ws.projects) ? ws.projects : [];
      project = wsProjects.find(p =>
        String(p.id || p._id || '') === String(projectId) ||
        String(p.subdomain || '') === String(projectId) ||
        String(p.name || '') === String(projectId)
      ) || null;
    }
    if (!project) return res.status(404).json({ error: 'Project not found' });
    const projectOwnerId = String(project.ownerUserId || userId);
    if (projectOwnerId !== userId) return res.status(403).json({ error: 'Forbidden' });

    const canonicalProjectId = String(project._id || project.id || projectId);
    const internalConn = db.connStr || db.connectionString || '';
    const externalConn = externalDbConnStr(db) || internalConn;

    // connType: 'internal' | 'external' (default) | 'both'
    const connType = String(req.body.connType || 'external').toLowerCase();

    // Delegate to the unified helper which builds the correct patch based on connType
    await injectConnStrIntoProject(canonicalProjectId, db.engine, internalConn, req.user, connType, externalConn);

    db.linkProjectId = canonicalProjectId;
    db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);

    // Build the same patch object to return to the frontend so it can update local state
    const envKeyDef = {
      mongodb:  { primary: 'MONGODB_URI',  secondary: 'DATABASE_URL' },
      postgres: { primary: 'DATABASE_URL', secondary: 'POSTGRES_URL' },
      mysql:    { primary: 'MYSQL_URL',    secondary: 'DATABASE_URL' },
      mariadb:  { primary: 'MARIADB_URL',  secondary: 'DATABASE_URL' },
      redis:    { primary: 'REDIS_URL',    secondary: null }
    };
    const keyDef = envKeyDef[db.engine] || { primary: 'DATABASE_URL', secondary: null };
    const type = connType;
    const injected = {};
    if (type === 'internal') {
      injected[keyDef.primary] = internalConn;
      if (keyDef.secondary) injected[keyDef.secondary] = internalConn;
    } else if (type === 'external') {
      injected[keyDef.primary] = externalConn;
      if (keyDef.secondary) injected[keyDef.secondary] = externalConn;
    } else {
      injected[keyDef.primary] = externalConn;
      if (keyDef.secondary) injected[keyDef.secondary] = externalConn;
      injected[`${keyDef.primary}_INTERNAL`] = internalConn;
    }

    addActivity('database', `🔗 Database "${db.name}" linked to project "${project.name}" (${connType})`);
    res.json({ ok: true, projectId: canonicalProjectId, envVars: injected });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/query — proxy a query to container ───────────────
// Executes SQL (pg/mysql) or mongo commands through docker exec on the container.
// Returns rows as JSON for the DB Editor.
app.post('/api/databases/:id/query', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not provisioned' });

    const liveStatus = containerStatus(db.containerName);
    if (liveStatus !== 'running') return res.status(400).json({ error: `Container is ${liveStatus}, not running` });

    const query = String(req.body?.query || '').trim().slice(0, 4000);
    if (!query) return res.status(400).json({ error: 'query is required' });

    let cmd = '';
    switch (db.engine) {
      case 'mongodb': {
        // Parse as mongo JSON command
        let mongoCmd;
        try { mongoCmd = JSON.parse(query); } catch { return res.status(400).json({ error: 'Invalid JSON for MongoDB command' }); }
        // Embed the command JSON safely inside a single-quoted shell string.
        // Single-quote escape: end the single-quoted string, insert a literal ', restart.
        // This prevents inner double-quotes from breaking a double-quoted --eval "..." shell arg.
        const mongoCmdJson = JSON.stringify(mongoCmd).replace(/'/g, "'\\''");
        const evalScript = `JSON.stringify(db.runCommand(${mongoCmdJson}))`;
        const authArgs = db.user ? `--username "${db.user}" --password "${db.pass}" --authenticationDatabase admin` : '';
        cmd = `docker exec ${db.containerName} mongosh --quiet ${authArgs} ${db.dbName||'mydb'} --eval '${evalScript}'`;
        break;
      }
      case 'postgres': {
        const safeQ = query.replace(/'/g, "'\\''").replace(/\n/g, ' ');
        // --set ON_ERROR_STOP=on makes psql exit non-zero on SQL errors so runDocker catches it.
        // 2>&1 merges stderr into stdout so error text is always captured in result.stdout.
        cmd = `docker exec ${db.containerName} psql -U "${db.user||'postgres'}" -d "${db.dbName||'mydb'}" -t -A -F'|' --set ON_ERROR_STOP=on -c '${safeQ}' 2>&1`;
        break;
      }
      case 'mysql':
      case 'mariadb': {
        const safeQ = query.replace(/'/g, "'\\''").replace(/\n/g, ' ');
        cmd = `docker exec ${db.containerName} mysql -u"${db.user||'root'}" -p"${db.pass}" "${db.dbName||'mydb'}" --batch --silent -e '${safeQ}' 2>&1`;
        break;
      }
      case 'redis': {
        const parts = query.trim().split(/\s+/);
        const redisArgs = parts.map(p => `"${p.replace(/"/g,'\\"')}"`).join(' ');
        cmd = `docker exec ${db.containerName} redis-cli ${db.pass ? `-a "${db.pass}"` : ''} ${redisArgs} 2>&1`;
        break;
      }
      default:
        return res.status(400).json({ error: `Unsupported engine: ${db.engine}` });
    }

    const result = runDocker(cmd);

    // If the command itself failed (non-zero exit), return the error immediately.
    // Prefer stdout since 2>&1 redirects DB error text there; fall back to stderr/message.
    if (!result.ok) {
      const errMsg = (result.stdout || result.stderr || 'Query execution failed').trim();
      return res.status(400).json({ ok: false, error: errMsg, engine: db.engine });
    }

    const rawOut = (result.stdout || '').trim();

    // Even when exit code is 0, some engines write error text to stdout.
    // Detect these and return a proper 400 so the frontend shows the error correctly.
    const isErrorOutput = (() => {
      if (!rawOut) return false;
      if (db.engine === 'postgres') {
        // psql with ON_ERROR_STOP=on exits non-zero, but guard just in case
        return /^ERROR:/m.test(rawOut) || /^FATAL:/m.test(rawOut);
      }
      if (db.engine === 'mysql' || db.engine === 'mariadb') {
        return /^ERROR\s+\d+/m.test(rawOut);
      }
      if (db.engine === 'redis') {
        // redis-cli errors start with "-ERR", "-WRONGTYPE", "(error)", etc.
        return /^\(error\)/m.test(rawOut) || /^-[A-Z]/m.test(rawOut);
      }
      if (db.engine === 'mongodb') {
        try {
          const p = JSON.parse(rawOut);
          if (p?.ok === 0 || p?.errmsg) return true;
        } catch {}
        return /MongoServerError|MongoError/i.test(rawOut);
      }
      return false;
    })();

    if (isErrorOutput) {
      return res.status(400).json({ ok: false, error: rawOut, engine: db.engine });
    }

    // Parse output into rows for tabular display
    let rows = [];
    if (db.engine === 'mongodb') {
      try { const parsed = JSON.parse(rawOut); rows = parsed.cursor?.firstBatch || (Array.isArray(parsed) ? parsed : [parsed]); }
      catch { rows = [{ result: rawOut }]; }
    } else if (db.engine === 'postgres') {
      rows = rawOut ? rawOut.split('\n').filter(Boolean).map(line => {
        const parts = line.split('|');
        return { _row: parts.join(' | ') };
      }) : [];
    } else {
      rows = rawOut ? rawOut.split('\n').filter(Boolean).map(line => ({ result: line })) : [];
    }

    addActivity('database', `🔍 Query on "${db.name}" (${db.engine})`);
    res.json({ ok: true, rows, raw: rawOut, engine: db.engine });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/ping — live connection test ──────────────────────
// Called by the SDK page "Run Connection Test" button.
// Sends the lightest possible native ping to each engine using docker exec,
// so it validates the actual in-container process, not just Docker health.
app.post('/api/databases/:id/ping', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db  = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Database not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not yet provisioned' });

    const live = containerStatus(db.containerName);
    if (live !== 'running') return res.status(400).json({ error: `Container is ${live}, not running` });

    const t0 = Date.now();
    let cmd = '';
    switch (db.engine) {
      case 'mongodb':
        cmd = `docker exec ${db.containerName} mongosh --quiet ${
          db.user ? `--username "${db.user}" --password "${db.pass}" --authenticationDatabase admin` : ''
        } --eval 'JSON.stringify(db.runCommand({ping:1}))' 2>&1`;
        break;
      case 'postgres':
        cmd = `docker exec ${db.containerName} psql -U "${db.user || 'postgres'}" -d "${db.dbName || 'mydb'}" -c "SELECT 1" -t -A 2>&1`;
        break;
      case 'mysql':
      case 'mariadb':
        cmd = `docker exec ${db.containerName} mysqladmin -u"${db.user || 'root'}" -p"${db.pass || ''}" ping 2>&1`;
        break;
      case 'redis':
        cmd = `docker exec ${db.containerName} redis-cli ${db.pass ? `-a "${db.pass}"` : ''} PING 2>&1`;
        break;
      default:
        return res.status(400).json({ error: `Unsupported engine: ${db.engine}` });
    }

    const result = runDocker(cmd, 8000);
    const out    = (result.stdout || result.stderr || '').trim();
    const ms     = Date.now() - t0;

    // Determine success by engine-specific output signals
    let ok = false;
    switch (db.engine) {
      case 'redis':    ok = out.toUpperCase().includes('PONG'); break;
      case 'mysql':
      case 'mariadb':  ok = out.toLowerCase().includes('is alive') || out.toLowerCase().includes('mysqld is alive'); break;
      case 'postgres': ok = result.ok && !out.toLowerCase().includes('error'); break;
      case 'mongodb': {
        try {
          // Find the last line that looks like JSON (mongosh may print warnings before it)
          const jsonLine = out.split('\n').reverse().find(l => l.trim().startsWith('{'));
          const parsed = jsonLine ? JSON.parse(jsonLine) : null;
          ok = parsed?.ok === 1;
        } catch { ok = out.includes('"ok":1') || out.includes('"ok" : 1') || out.includes('"ok":1.0'); }
        break;
      }
      default:         ok = result.ok;
    }

    if (!ok) {
      return res.status(502).json({ error: `Ping failed: ${out.slice(0, 200)}`, raw: out });
    }

    res.json({ ok: true, ms, engine: db.engine, raw: out });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── GET /api/databases/:id/metrics — live container metrics ──────────────────
app.get('/api/databases/:id/metrics', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user._id || req.user.id);
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => String(d.id || d._id || '') === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (String(db.ownerUserId || userId) !== userId) return res.status(403).json({ error: 'Forbidden' });
    if (!db.containerName) return res.json({ status: 'no_container' });

    const status = containerStatus(db.containerName);
    let stats = {};
    if (status === 'running') {
      const r = runDocker(`docker stats --no-stream --format '{"cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}","net":"{{.NetIO}}","block":"{{.BlockIO}}"}' ${db.containerName} 2>/dev/null || echo '{}'`);
      try { stats = JSON.parse(r.stdout || '{}'); } catch {}
    }
    res.json({ ok: true, status, ...stats, containerName: db.containerName });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.use(express.static(path.join(__dirname)));

// ── SPA client-side routing: serve index.html for all /dashboard/* paths ──────
// This enables deep-links like joytree.site/dashboard/logs to work on reload
// or when shared, without hitting a 404 from the server.
const DASHBOARD_INDEX = path.join(__dirname, 'index.html');
const DASHBOARD_PAGES = [
  'dashboard', 'projects', 'deployments', 'repos', 'new-resource', 'new-deploy',
  'logs', 'ai-analysis', 'env-manager', 'domains', 'analytics', 'webhooks',
  'cronjobs', 'workers', 'streaks', 'team', 'usage', 'activity', 'secrets',
  'settings', 'docs', 'faq', 'support', 'forum', 'domain-add', 'cron-create',
  'dockerfile-deploy', 'pricing', 'databases', 'db-create', 'db-editor', 'db-detail', 'checkout'
];

// /dashboard  (base — no trailing segment)
app.get('/dashboard', (req, res) => res.sendFile(DASHBOARD_INDEX));

// /dashboard/:page  (any valid sub-page)
app.get('/dashboard/:page', (req, res, next) => {
  // Only intercept known page slugs; let anything else fall through (future API routes etc.)
  if (DASHBOARD_PAGES.includes(req.params.page)) {
    return res.sendFile(DASHBOARD_INDEX);
  }
  next();
});

// Fallback catch-all (keeps existing behaviour for the root landing page)
app.get('*', (req, res) => res.sendFile(DASHBOARD_INDEX));

// ── Start ─────────────────────────────────────────────────────────────────────
// ── Auto-recover ports.json from running Docker containers on every startup ──
// This means you never have to manually recreate ports.json when moving to a
// new VPS or after a server restart — the registry is rebuilt automatically.
async function recoverPortRegistryFromDocker() {
  try {
    const { execSync: exec } = require('child_process');
    // Get all running db-api-* containers with their port mappings
    const raw = exec(
      "docker ps --filter 'name=db-api-' --format '{{.Names}}|{{.Ports}}' 2>/dev/null || echo ''",
      { encoding: 'utf8', stdio: ['pipe','pipe','pipe'] }
    ).trim();
    let recovered = 0;
    if (raw) {
      for (const line of raw.split('\n')) {
        const [namePart, portsPart] = line.split('|');
        if (!namePart || !portsPart) continue;
        const subdomain = namePart.trim().replace(/^db-/, '');
        const portMatch = portsPart.match(/0\.0\.0\.0:(\d+)->3000/);
        if (!portMatch) continue;
        const port = parseInt(portMatch[1]);
        if (!portRegistry[subdomain] || portRegistry[subdomain] !== port) {
          portRegistry[subdomain] = port;
          recovered++;
          console.log(`[Ports] Recovered ${subdomain} -> port ${port}`);
        }
      }
      if (recovered > 0) {
        savePortRegistry();
        console.log(`[Ports] Recovered ${recovered} entries from running Docker containers`);
      } else {
        console.log('[Ports] Port registry already up to date');
      }
    }
    // Also pull from Firebase to cover cases where ports.json was lost on reinstall
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const fr = await fetch(`${FIREBASE_RTDB_URL}/deployboard_ports.json${authQuery}`, { headers:{Accept:'application/json'} });
      if (fr.ok) {
        const fbPorts = await fr.json();
        if (fbPorts && typeof fbPorts === 'object') {
          let fromFb = 0;
          for (const [sub, port] of Object.entries(fbPorts)) {
            if (!portRegistry[sub]) { portRegistry[sub] = port; fromFb++; }
          }
          if (fromFb > 0) {
            fs.writeFileSync(PORTS_FILE, JSON.stringify(portRegistry, null, 2));
            console.log(`[Ports] Restored ${fromFb} entries from Firebase`);
          }
        }
      }
    }
  } catch (e) {
    console.warn('[Ports] Could not recover from Docker:', e.message);
  }
}

// ── Restore flows from Firebase for ALL known users on startup ─────────────────
// This fills any gaps where a flow is in Firebase but not in the local api_catalog.json
async function restoreFlowRegistryFromFirebase() {
  if (!FIREBASE_RTDB_URL || !FIREBASE_RTDB_SECRET) return;
  try {
    const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
    const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_apis.json${authQuery}`, { headers:{Accept:'application/json'} });
    if (!r.ok) return;
    const allUsers = await r.json();
    if (!allUsers || typeof allUsers !== 'object') return;
    let restoredFb = 0;
    for (const userKey of Object.keys(allUsers)) {
      const userApis = allUsers[userKey];
      if (!userApis || typeof userApis !== 'object') continue;
      for (const rec of Object.values(userApis)) {
        if (!rec || !rec.flowId) continue;
        // Merge into local apiCatalog if missing
        const existsLocal = apiCatalog.some(a => a.flowId === rec.flowId && a.ownerUserId === rec.ownerUserId);
        if (!existsLocal) {
          apiCatalog.push(rec);
        }
        // Restore flowRegistry if missing
        if (!flowRegistry.has(rec.flowId)) {
          const isQuiz = !!rec.quizSeeded;
          const aiTemplate = rec.responseTemplate || { ok: true, flowId: rec.flowId, message: 'API is live' };
          const nodes = [
            { id:'n1', type:'INCOMING_REQUEST', config:{ method: String(rec.httpMethod||'GET').toUpperCase(), routePath: String(rec.routePath||'/api') }, next:'n2' },
            { id:'n2', type:'DB_INSERT',  config:{ collection: isQuiz ? 'quiz_submissions':'requests', source:'req.body' }, next:'n3' },
            { id:'n3', type:'DB_FIND',   config:{ collection: isQuiz ? 'quiz_questions':'requests', filters:[] }, next:'n4' },
            { id:'n4', type:'HTTP_RESPONSE', config:{ status:200, json: aiTemplate } }
          ];
          flowRegistry.set(rec.flowId, { flowId:rec.flowId, owner:rec.ownerUserId||'', nodes, createdAt:rec.createdAt||new Date().toISOString(), prompt:rec.prompt||'', aiTemplate, aiDataSeed:Array.isArray(rec.dataSeed)?rec.dataSeed:[], isQuiz });
          restoredFb++;
        }
      }
    }
    if (restoredFb > 0) {
      saveApiCatalog(); // persist anything pulled from Firebase
      console.log(`[FlowRegistry] Restored ${restoredFb} additional flows from Firebase`);
    }
  } catch (e) {
    console.warn('[FlowRegistry] Firebase restore failed:', e.message);
  }
}

server.listen(PORT, () => {
  console.log(`[Joytree] Running on http://localhost:${PORT}`);
  console.log(`[Joytree] Base domain: ${BASE_DOMAIN}`);
  console.log(`[Joytree] Sites dir:   ${SITES_DIR}`);
  // Rebuild ports.json from any already-running containers automatically
  recoverPortRegistryFromDocker().catch(() => {});
  // Restore flows from Firebase (fills gaps not covered by local api_catalog.json)
  restoreFlowRegistryFromFirebase().catch(() => {});
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

  // Legacy/manual mode: useful for curl tests or older Joytree webhook docs.
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
  void writeWorkspaceToFirebase(user, user.workspace || {});
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
