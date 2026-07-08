'use strict';

// ===== CRASH GUARD =====
// [FIX] This used to log-and-continue forever on ANY uncaught error. That's
// an anti-pattern Node's own docs warn against: after an uncaughtException,
// internal state (an HTTP server's listener, a socket, a half-finished
// proxy request) can be left corrupted in ways that don't throw again but
// also stop serving real traffic correctly. Because the process never
// actually exits in that scenario, Docker's restart policy — confirmed set
// to `unless-stopped` on this container — never gets a chance to kick in,
// since that policy only reacts to the container's process exiting, not to
// it being silently wedged. That mismatch (alive-but-broken, with no exit
// to trigger a restart) is what produced "everything 502s/Error-1033s for
// every subdomain until I manually run docker compose up --build."
// Fix: log the error, give in-flight requests ~1s to flush, then exit —
// Docker brings the process back automatically and quickly (typically
// 1-2s), instead of limping along in an unknown state until a human notices.
let _crashExitScheduled = false;
function _crashGuardExit(kind, errOrReason) {
  console.error(`[CRASH GUARD] ${kind} — restarting process in 1s (Docker restart:unless-stopped will bring it back):`,
    (errOrReason && errOrReason.stack) || errOrReason);
  if (_crashExitScheduled) return;
  _crashExitScheduled = true;
  setTimeout(() => process.exit(1), 1000);
}
process.on('uncaughtException', (err) => _crashGuardExit('uncaughtException', err));
process.on('unhandledRejection', (reason) => _crashGuardExit('unhandledRejection', reason));
// ========================================================================

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
const archiver = require('archiver');
const os       = require('os');
const mongoose = require('mongoose');
const crypto   = require('crypto');
const { exec: _execAsync } = require('child_process');
const { promisify: _promisify } = require('util');
// [FIX] The AI agent code below (cloneUserRepo, execute_command tool, git
// checkout/diff/push, tmpDir cleanup) calls `execAsync(...)` and expects a
// promise that resolves to {stdout, stderr} — the same contract as Node's
// util.promisify(exec), which is what groq-code-cli (the project this agent
// logic was ported from) actually uses. That definition never got carried
// over during the port, so every reference to `execAsync` in this file was
// hitting an undefined identifier — a ReferenceError on the very first git
// operation, every single run.
const execAsync = _promisify(_execAsync);
require('dotenv').config();

// [FIX] Async replacement for execSync() inside HTTP request handlers.
// execSync() blocks the ENTIRE Node event loop — including the reverse
// proxy for every deployed app — until the shell command returns. Commands
// like `docker stats`, `docker inspect`, and `du -sh` can take anywhere from
// tens of milliseconds to several seconds depending on host load, and during
// that window EVERY tenant's site is unresponsive, which Cloudflare reports
// as a 502/521/522. execP() runs the same command without blocking the loop.
// Returns stdout (trimmed) on success, or '' on error/timeout (matching the
// "|| echo ''" fallback pattern used throughout this file).
function execP(cmd, opts = {}) {
  return new Promise((resolve) => {
    _execAsync(cmd, { encoding: 'utf8', timeout: 8000, ...opts }, (err, stdout) => {
      resolve(err ? '' : String(stdout).trim());
    });
  });
}

const app    = express();

// [FIX] Cheap, dependency-free heartbeat for the in-process watchdog below.
// Registered before any other middleware so it never waits on Mongo,
// Firebase, the custom-domain cache, or anything else that could itself be
// the thing that's stuck — this route's only job is "is Express + the event
// loop still actually able to respond right now."
app.get('/__internal_heartbeat', (req, res) => res.status(200).end('ok'));
const server = http.createServer(app);
// [FIX] Default Socket.IO ping settings (pingInterval 25s, pingTimeout 20s)
// are too tight for this app: while a heavy `npm install`/`npm run build`
// is streaming hundreds/thousands of log lines per second through this
// process, the event loop can fall behind on ping/pong handling, causing
// the client to see "Disconnected from build server" (ping timeout) and
// reconnect mid-build — even though the build itself is fine. Raising
// pingInterval/pingTimeout gives the event loop more headroom during these
// bursts without meaningfully delaying detection of genuinely dead clients.
const io = new SocketIO(server, {
  cors: { origin: '*', methods: ['GET','POST'] },
  pingInterval: 25000,
  pingTimeout: 60000
});

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
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || '';
const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY || '';
const FIREBASE_RTDB_URL = (process.env.FIREBASE_RTDB_URL || process.env.FIREBASE_DATABASE_URL || '').replace(/\/+$/, '');
const FIREBASE_RTDB_SECRET = process.env.FIREBASE_RTDB_SECRET || process.env.FIREBASE_DATABASE_SECRET || '';
const TURNSTILE_SITE_KEY = process.env.TURNSTILE_SITE_KEY || '';
const TURNSTILE_SECRET_KEY = process.env.TURNSTILE_SECRET_KEY || '';
let GLOBAL_WEBHOOK_SECRET = process.env.GLOBAL_WEBHOOK_SECRET || process.env.WEBHOOK_SECRET || '';
if (!GLOBAL_WEBHOOK_SECRET) GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
const INTERNAL_DEPLOY_KEY = process.env.INTERNAL_DEPLOY_KEY || crypto.randomBytes(24).toString('hex');
const SESSION_TTL_DAYS = Number(process.env.SESSION_TTL_DAYS || 30);
// [FIX] Raised floor from 250ms to 5000ms — see comment block below
const AUTO_DEPLOY_POLL_INTERVAL_MS = Math.max(5000, Number(process.env.AUTO_DEPLOY_POLL_INTERVAL_MS || 5000) || 5000);
const AUTO_DEPLOY_INITIAL_DELAY_MS = Math.max(5000, Number(process.env.AUTO_DEPLOY_INITIAL_DELAY_MS || AUTO_DEPLOY_POLL_INTERVAL_MS) || AUTO_DEPLOY_POLL_INTERVAL_MS);
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

// OpenAI (GPT) — OpenAI-compatible coding agent provider
const OPENAI_API_KEY = String(process.env.OPENAI_API_KEY || '').trim();
const OPENAI_MODEL   = String(process.env.OPENAI_MODEL   || 'gpt-4o-mini').trim();
// Anthropic (Claude) — premium coding agent provider
// Add ANTHROPIC_API_KEY to your .env to enable Claude in the JoyTree AI Agent
const ANTHROPIC_API_KEY = String(process.env.ANTHROPIC_API_KEY || '').trim();
const ANTHROPIC_MODEL   = String(process.env.ANTHROPIC_MODEL   || 'claude-sonnet-4-6').trim();
// Joytree API v3 — DeepSeek (OpenAI-compatible high-reasoning AI)
const JOYTREE_V3_ADMIN_EMAIL = String(process.env.JOYTREE_V3_ADMIN_EMAIL || 'projectvpn89@gmail.com').trim().toLowerCase();
const DEEPSEEK_API_KEY = String(process.env.DEEPSEEK_API_KEY || '').trim();
const DEEPSEEK_MODEL = String(process.env.DEEPSEEK_MODEL || 'deepseek-v4-pro').trim();
const DEEPSEEK_MODEL_FALLBACK = String(process.env.DEEPSEEK_MODEL_FALLBACK || 'deepseek-v4-flash').trim();
const DEEPSEEK_FLOW_TIMEOUT_MS = Number(process.env.DEEPSEEK_FLOW_TIMEOUT_MS || 180000);
const DEEPSEEK_CHUNK_TIMEOUT_MS = Number(process.env.DEEPSEEK_CHUNK_TIMEOUT_MS || 120000);
const DEEPSEEK_FLOW_MAX_TOKENS = Number(process.env.DEEPSEEK_FLOW_MAX_TOKENS || 8192);
const DEEPSEEK_CHUNK_MAX_TOKENS = Number(process.env.DEEPSEEK_CHUNK_MAX_TOKENS || 4096);
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
  const sourceLabel = source === 'auto' ? 'Automatic (GitHub push)' : source === 'upload' ? 'Upload (file archive)' : 'Manual (Redeploy click)';
  const safeError = String(errorMessage || '').trim().slice(0, 500);
  const logoUrl = RESEND_LOGO_URL || `https://${BASE_DOMAIN}/logo_optimized.jpg`;
  const deployedAtText = deployedAt ? new Date(deployedAt).toLocaleString('en-US', { timeZone: 'UTC', dateStyle: 'medium', timeStyle: 'short' }) + ' UTC' : '';
  const dashboardUrl = `https://${BASE_DOMAIN}`;
  // Clean, flat status banner -- a colored strip + simple line-icon + label,
  // same register as Vercel/GitHub deployment emails. Not an illustration,
  // just a clear at-a-glance status signal before you even read the body.
  const bannerBg = isStartedPhase ? '#eff6ff' : (status === 'success' ? '#10b981' : '#fef2f2');
  const bannerFg = isStartedPhase ? '#1d4ed8' : (status === 'success' ? '#ffffff' : '#b91c1c');
  const bannerIcon = isStartedPhase
    ? '<circle cx="12" cy="12" r="9"/><polyline points="12 7 12 12 15 14"/>'
    : (status === 'success'
        ? '<path d="M20 6 9 17l-5-5"/>'
        : '<circle cx="12" cy="12" r="9"/><line x1="12" y1="8" x2="12" y2="13"/><line x1="12" y1="16" x2="12.01" y2="16"/>');
  // These images are real, user-configured assets (RESEND_HERO_IMAGE_*_URL
  // env vars) -- not something to drop just because the old full-width
  // banner treatment was too much. Used here as a small icon inside the
  // clean status strip instead: a tasteful accent, not a giant illustration.
  // Falls back to the plain line-icon above if no image is configured for
  // this particular status.
  const heroImageUrl = (
    isStartedPhase
      ? (RESEND_HERO_IMAGE_STARTED_URL || RESEND_HERO_IMAGE_URL)
      : (status === 'success' ? (RESEND_HERO_IMAGE_SUCCESS_URL || RESEND_HERO_IMAGE_URL) : (RESEND_HERO_IMAGE_FAILED_URL || RESEND_HERO_IMAGE_URL))
  ) || '';
  const bannerIconHtml = heroImageUrl
    ? `<img src="${heroImageUrl}" alt="" width="104" height="104" style="width:104px;height:104px;border-radius:14px;display:block;object-fit:cover;">`
    : `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="${bannerFg}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${bannerIcon}</svg>`;
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
  const html = `<!doctype html>
<html><body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif;color:#202124;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:32px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:560px;background:#ffffff;">
        <tr>
          <td style="background:${bannerBg};padding:24px 32px;">
            <table role="presentation" width="100%"><tr>
              <td style="width:104px;vertical-align:middle;">
                ${bannerIconHtml}
              </td>
              <td style="vertical-align:middle;padding-left:18px;">
                <span style="font-size:19px;font-weight:700;color:${bannerFg};letter-spacing:0.02em;">${isStartedPhase ? 'DEPLOYMENT STARTED' : (status === 'success' ? 'DEPLOYMENT SUCCESSFUL' : 'DEPLOYMENT FAILED')}</span>
              </td>
            </tr></table>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 32px 0;">
            <table role="presentation" width="100%"><tr>
              <td style="width:32px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JOYTREE" width="28" height="28" style="width:28px;height:28px;border-radius:6px;display:block;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;padding-left:10px;">
                <span style="font-size:15px;font-weight:600;color:#202124;">JoyTree</span>
              </td>
            </tr></table>
          </td>
        </tr>
        <tr><td style="padding:24px 32px 4px;">
          <h2 style="margin:0 0 4px;font-size:19px;font-weight:500;color:#202124;">${projectName || subdomain} ${isStartedPhase ? 'deployment started' : (status === 'success' ? 'is live' : 'deployment failed')}</h2>
          <p style="margin:0 0 20px;color:#5f6368;font-size:14px;line-height:1.6;">
            ${isStartedPhase ? 'Your deployment has started. We\'ll let you know when it finishes.' : (status === 'success' ? 'Your project deployed successfully and is now live.' : 'Your deployment ran into a problem. Details are below.')}
          </p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            <tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Project</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${projectName || subdomain}</td></tr>
            <tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Source</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${sourceLabel}</td></tr>
            ${repoUrl ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Repository</td><td style="padding:10px 0;font-size:13px;text-align:right;">${source === 'upload' ? `<span style="color:#5f6368;">Uploaded archive — ${projectName || subdomain}</span>` : `<a href="${repoUrl}" style="color:#0d9488;text-decoration:none;">${repoUrl}</a>`}</td></tr>` : ''}
            <tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Branch</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${branch || 'main'}${shortSha ? ` (${shortSha})` : ''}</td></tr>
            ${duration > 0 ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Duration</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${duration}s</td></tr>` : ''}
            ${buildStatus ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Build status</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${buildStatus}</td></tr>` : ''}
            ${deployStatus ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Deployment status</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${deployStatus}</td></tr>` : ''}
            ${(memoryLimit || cpuShares) ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Resources</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${memoryLimit ? `RAM ${memoryLimit}` : ''}${memoryLimit && cpuShares ? ' · ' : ''}${cpuShares ? `CPU ${cpuShares} shares` : ''}</td></tr>` : ''}
            ${deployedAtText ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Deployed at</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${deployedAtText}</td></tr>` : ''}
            ${totalDeployments > 0 ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Total deployments</td><td style="padding:10px 0;font-size:13px;color:#202124;text-align:right;">${totalDeployments}</td></tr>` : ''}
            ${liveUrl ? `<tr><td style="padding:10px 0;font-size:13px;color:#5f6368;">Live URL</td><td style="padding:10px 0;font-size:13px;text-align:right;"><a href="${liveUrl}" style="color:#0d9488;text-decoration:none;">${liveUrl}</a></td></tr>` : ''}
            ${safeError ? `<tr><td colspan="2" style="padding:10px 0;font-size:13px;color:#c5221f;">${safeError}</td></tr>` : ''}
          </table>
          <div style="margin-top:24px;">
            <a href="${liveUrl || dashboardUrl}" style="display:inline-block;background:#10b981;color:#ffffff;text-decoration:none;padding:9px 20px;border-radius:4px;font-weight:500;font-size:14px;">View deployment</a>
          </div>
        </td></tr>
        <tr><td style="padding:24px 32px 24px;">
          <hr style="border:none;border-top:1px solid #e8eaed;margin:0 0 16px;">
          <p style="margin:0;color:#80868b;font-size:12px;line-height:1.6;">This is an automated deployment notification from JoyTree. You're receiving it because you have an account at ${BASE_DOMAIN}.</p>
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
const PORT_START = Number(process.env.APP_PORT_START || 3000);
const PORT_END   = Number(process.env.APP_PORT_END || 3999);
// [FIX] Default was 300 s — holding 300 concurrent slow/hung requests ties up 300 TCP
// sockets + 300 async stack frames for 5 full minutes each, rapidly eating RAM.
// 60 s is still generous for any real web request. Override via APP_PROXY_TIMEOUT_MS
// if you have legitimate long-polling routes that need more time.
const APP_PROXY_TIMEOUT_MS = Math.max(30000, Number(process.env.APP_PROXY_TIMEOUT_MS || 60000));

// ── Build concurrency limiter ────────────────────────────────────────────
// [FIX] Each deploy spins up one or more ephemeral `docker run node:20 ...`
// containers for install/build (see runBuildCommandInContainer in
// buildRunner.js). With no limit, multiple simultaneous deploys (or a queue
// of auto-deploys from pushes) can spawn many heavy node containers at once,
// exhausting host RAM/CPU. This causes: deploys disconnecting mid-stream
// ("Disconnected from build server"), the proxy/dashboard becoming
// unresponsive (502s via cloudflared), and requiring a container restart to
// recover.
//
// MAX_CONCURRENT_BUILDS caps how many runBuild() calls execute at once;
// additional deploy requests queue and wait for a free slot, with a
// build:log line so the user sees why their deploy hasn't started yet.
const MAX_CONCURRENT_BUILDS = Math.max(1, Number(process.env.MAX_CONCURRENT_BUILDS || 2));
let _activeBuildCount = 0;
const _buildWaitQueue = [];

function acquireBuildSlot() {
  if (_activeBuildCount < MAX_CONCURRENT_BUILDS) {
    _activeBuildCount++;
    return Promise.resolve();
  }
  return new Promise((resolve) => { _buildWaitQueue.push(resolve); });
}

function releaseBuildSlot() {
  if (_buildWaitQueue.length > 0) {
    const next = _buildWaitQueue.shift();
    next();
  } else {
    _activeBuildCount = Math.max(0, _activeBuildCount - 1);
  }
}

let portRegistry = {};
try {
  if (fs.existsSync(PORTS_FILE)) {
    portRegistry = JSON.parse(fs.readFileSync(PORTS_FILE, 'utf8'));
    console.log(`[Ports] Loaded ${Object.keys(portRegistry).length} entries`);
  }
} catch(e) { console.warn('[Ports] Could not load ports.json:', e.message); }

// [FIX] reloadPortRegistryFromDisk() used to be called synchronously on
// EVERY proxied request (every subdomain, every visitor) — fs.existsSync +
// fs.readFileSync + JSON.parse on the hot path. Under load, or whenever disk
// I/O briefly stalls (e.g. during a heavy build writing to the same disk),
// this blocks the single Node event loop for ALL tenants at once. Cloudflare
// Tunnel then sees the origin as unresponsive and every visitor gets a
// Cloudflare 502/521/522 simultaneously — which "fixes itself" on reload once
// the blocking call finishes.
//
// buildRunner.js writes ports.json directly via fs.writeFileSync from a
// separate read/modify/write of its own registry copy (it does NOT go through
// this process's `portRegistry` object), so this process still needs to pick
// up those changes. Instead of reading on every request, watch the file and
// reload asynchronously+debounced only when it actually changes.
let _portsReloadTimer = null;
function _reloadPortRegistryFromDiskAsync() {
  if (_portsReloadTimer) return;
  _portsReloadTimer = setTimeout(() => {
    _portsReloadTimer = null;
    fs.readFile(PORTS_FILE, 'utf8', (err, data) => {
      if (err) {
        if (err.code !== 'ENOENT') console.warn('[Ports] Could not reload ports.json:', err.message);
        return;
      }
      try {
        portRegistry = JSON.parse(data);
      } catch (e) {
        console.warn('[Ports] Could not parse ports.json:', e.message);
      }
    });
  }, 100); // small debounce so rapid successive writes only trigger one reload
}
try {
  fs.watch(path.dirname(PORTS_FILE), { persistent: false }, (eventType, filename) => {
    // filename can be null on some platforms/filesystems (e.g. certain Docker
    // volume mounts) — if so, just reload on any change in the directory
    // rather than risk missing ports.json updates.
    if (!filename || filename === path.basename(PORTS_FILE)) _reloadPortRegistryFromDiskAsync();
  });
} catch (e) {
  console.warn('[Ports] Could not watch ports.json for changes:', e.message);
  // Fallback: periodic async reload if watch isn't supported on this filesystem.
  setInterval(_reloadPortRegistryFromDiskAsync, 5000);
}

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

// Extract the numeric port from a registry entry. Entries can be either:
//   · a plain number / numeric string (legacy, or mid-deploy placeholder): 10018
//   · "containerName:port" Docker DNS format (current): "db-myapp:4000"
// Always returns a Number, or NaN if the entry is missing/invalid.
function _portFromEntry(entry) {
  if (entry == null) return NaN;
  if (typeof entry === 'number') return entry;
  const s = String(entry);
  if (s.includes(':')) {
    const parts = s.split(':');
    return Number(parts[parts.length - 1]);
  }
  return Number(s);
}

// Extract the Docker container name from a registry entry. Current entries use
// "containerName:port" so the proxy can route to Docker DNS; legacy numeric
// entries fall back to the conventional db-<subdomain> container name.
function _containerFromEntry(subdomain, entry) {
  const fallback = `db-${String(subdomain || '').toLowerCase().replace(/[^a-z0-9-]/g, '-')}`;
  if (entry == null || typeof entry === 'number') return fallback;
  const s = String(entry).trim();
  if (!s) return fallback;
  const name = s.includes(':') ? s.split(':')[0] : s;
  if (/^\d+$/.test(name)) return fallback;
  return name.replace(/[^a-zA-Z0-9_.-]/g, '') || fallback;
}

function getOrAssignPort(subdomain) {
  // [FIX] PRE-EXISTING BUG: `execSync` was used here without being imported
  // anywhere in this file's scope, so this call always threw ReferenceError,
  // was swallowed by the catch below, and isPortAvailable() ALWAYS returned
  // false — meaning the port-scan loop below never found a "free" port and
  // getOrAssignPort() always threw "No free ports available (3000-3999 all
  // in use)" for any deploy that needed a fresh port. Using the synchronous
  // execSync from 'child_process' here (this function is called from
  // synchronous deploy code paths, not the per-request proxy, so it's safe).
  const { execSync: _execSyncLocal } = require('child_process');
  const isPortAvailable = (port) => {
    try {
      _execSyncLocal(`sh -lc "ss -ltn '( sport = :${Number(port)} )' | tail -n +2 | grep -q . && exit 1 || exit 0"`, { stdio: 'pipe' });
      return true;
    } catch {
      return false;
    }
  };

  // Parse existing entry regardless of format ("db-myapp:4000" or plain 10018).
  // This is only used as the PORT env var passed into the new container
  // (process.env.PORT) — the actual proxy registry entry is always
  // "containerName:port" and is written by buildRunner after the build.
  const assigned = _portFromEntry(portRegistry[subdomain]);
  if (Number.isFinite(assigned) && assigned >= PORT_START && assigned <= PORT_END && isPortAvailable(assigned)) {
    return assigned;
  }

  const used = new Set(
    Object.values(portRegistry)
      .map(v => _portFromEntry(v))
      .filter(v => Number.isFinite(v))
  );
  if (Number.isFinite(assigned)) used.delete(assigned);
  for (let p = PORT_START; p <= PORT_END; p++) {
    if (!used.has(p) && isPortAvailable(p)) {
      // Store as plain number placeholder — buildRunner will overwrite with
      // "containerName:port" once the container is live and registered.
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

// [FIX] reloadPortRegistryFromDisk() removed — replaced by the fs.watch-based
// async reload above (_reloadPortRegistryFromDiskAsync). Calls below are now
// no-ops kept only for compatibility; portRegistry stays fresh automatically.
function reloadPortRegistryFromDisk() { /* no-op: see fs.watch above */ }

function parseProxyTarget(entry) {
  if (typeof entry !== 'string' || !entry.includes(':')) return null;
  const idx = entry.lastIndexOf(':');
  const host = entry.slice(0, idx).trim();
  const port = Number(entry.slice(idx + 1));
  if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) return null;
  return { host, port };
}

function requestPrefersJson(req) {
  const accept = String(req.headers.accept || '').toLowerCase();
  const contentType = String(req.headers['content-type'] || '').toLowerCase();
  return accept.includes('application/json') || contentType.includes('application/json') || req.path.startsWith('/api/');
}

function requestLooksProgrammatic(req) {
  const dest = String(req.headers['sec-fetch-dest'] || '').toLowerCase();
  const mode = String(req.headers['sec-fetch-mode'] || '').toLowerCase();
  return requestPrefersJson(req) || (dest === 'empty' && ['cors', 'same-origin', ''].includes(mode));
}

function proxyResponseOrJsonMismatch(req, res, proxyRes, subdomain) {
  const contentType = String(proxyRes.headers['content-type'] || '').toLowerCase();
  const declaredHtml = contentType.includes('text/html');

  // Block HTML responses for any request that is clearly not a browser page load.
  // This covers two cases:
  // 1. fetch()/XHR from the deployed app's own frontend hitting /api/* routes
  //    -> the proxy must not return Joytree's own index.html (causes "Unexpected token '<'" JSON parse error)
  // 2. Any request where Accept header explicitly excludes text/html
  const acceptHeader = String(req.headers['accept'] || '');
  const acceptsHtml = acceptHeader === '' || acceptHeader === '*/*' || acceptHeader.includes('text/html');
  const isProgrammatic = requestLooksProgrammatic(req);
  const clientWantsNoHtml = !acceptsHtml && declaredHtml;

  if (declaredHtml && (isProgrammatic || clientWantsNoHtml)) {
    proxyRes.resume();
    const statusCode = Number(proxyRes.statusCode) >= 400 ? Number(proxyRes.statusCode) : 502;
    return res.status(statusCode).json({
      error: 'The deployed app returned HTML for a non-HTML request.',
      detail: 'This usually means the request hit the frontend SPA fallback instead of a real API route. Verify your backend routes exist and all required environment variables are configured.',
      subdomain
    });
  }

  // [FIX] Some apps return an SPA's index.html for unmatched /api/* routes
  // WITHOUT a text/html content-type (missing, text/plain, or no content-type
  // at all). The check above only catches a declared text/html content-type,
  // so a programmatic request can still receive a raw "<!DOCTYPE html>..."
  // body that fails JSON.parse on the client ("Unexpected token '<'").
  // For programmatic requests where the upstream didn't declare JSON, sniff
  // the first bytes of the body for an HTML signature before piping through.
  const declaredJson = contentType.includes('application/json') || contentType.includes('+json');
  if (isProgrammatic && !declaredJson && !declaredHtml) {
    const chunks = [];
    let total = 0;
    const SNIFF_LIMIT = 512;
    let decided = false;

    // [FIX] If the client disconnects while we're still buffering the first
    // SNIFF_LIMIT bytes (e.g. browser tab closed, mobile network drop), the
    // `chunks` array would previously stay in memory until proxyRes emits
    // 'end' — which may never happen for a slow upstream. Listening to
    // req.on('close') lets us drain+discard immediately and free the buffer.
    req.on('close', () => {
      if (decided) return;
      decided = true;
      proxyRes.removeAllListeners('data');
      proxyRes.removeAllListeners('end');
      proxyRes.resume(); // drain and discard so the upstream socket closes cleanly
      chunks.length = 0;
    });

    const finishPassthrough = () => {
      if (decided) return;
      decided = true;
      if (!res.headersSent) res.writeHead(proxyRes.statusCode, proxyRes.headers);
      for (const c of chunks) res.write(c);
      proxyRes.on('data', (c) => res.write(c));
      proxyRes.on('end', () => { if (!res.writableEnded) res.end(); });
    };

    const finishHtmlMismatch = () => {
      if (decided) return;
      decided = true;
      proxyRes.removeAllListeners('data');
      proxyRes.resume();
      const statusCode = Number(proxyRes.statusCode) >= 400 ? Number(proxyRes.statusCode) : 502;
      return res.status(statusCode).json({
        error: 'The deployed app returned HTML for a non-HTML request.',
        detail: 'This usually means the request hit the frontend SPA fallback instead of a real API route. Verify your backend routes exist and all required environment variables are configured.',
        subdomain
      });
    };

    proxyRes.on('data', (chunk) => {
      if (decided) return;
      chunks.push(chunk);
      total += chunk.length;
      if (total >= SNIFF_LIMIT || total >= Number(proxyRes.headers['content-length'] || Infinity)) {
        const sniff = Buffer.concat(chunks).slice(0, SNIFF_LIMIT).toString('utf8').trimStart().toLowerCase();
        if (sniff.startsWith('<!doctype html') || sniff.startsWith('<html')) {
          finishHtmlMismatch();
        } else {
          finishPassthrough();
        }
      }
    });
    proxyRes.on('end', () => {
      if (decided) return;
      const sniff = Buffer.concat(chunks).slice(0, SNIFF_LIMIT).toString('utf8').trimStart().toLowerCase();
      if (sniff.startsWith('<!doctype html') || sniff.startsWith('<html')) {
        finishHtmlMismatch();
      } else {
        finishPassthrough();
        res.end();
      }
    });
    proxyRes.on('error', () => { if (!res.headersSent) res.end(); });
    return;
  }

  res.writeHead(proxyRes.statusCode, proxyRes.headers);
  proxyRes.pipe(res, { end: true });
  proxyRes.on('error', () => { if (!res.headersSent) res.end(); });
}

function sendProxyError(req, res, status, type, subdomain, baseDomain) {
  if (res.headersSent) return;
  if (requestLooksProgrammatic(req)) {
    const timeoutSeconds = Math.round(APP_PROXY_TIMEOUT_MS / 1000);
    const messages = {
      bad_gateway: 'The app container is not responding. It may have crashed or restarted.',
      gateway_timeout: `The app did not respond within ${timeoutSeconds}s. Long-running server work can continue, but the request exceeded the Joytree proxy timeout.`,
      not_deployed: 'This subdomain has no active deployment.'
    };
    return res.status(status).json({ error: messages[type] || messages.not_deployed });
  }
  return res.status(status).send(errorPage(type, subdomain, baseDomain));
}

// ── Middleware ────────────────────────────────────────────────────────────────
app.use((req, res, next) => {
  // Skip express.json for multipart upload routes — raw stream needed for parseMultipart
  if (req.path === '/api/upload-project') return next();
  express.json({ verify: (req, _res, buf) => { req.rawBody = Buffer.from(buf || ''); } })(req, res, next);
});
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, x-deployboard-internal-key, x-deployboard-owner-id');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Static file helper ────────────────────────────────────────────────────────
function serveStatic(req, res, distDir) {
  const mw = express.static(distDir, { index: 'index.html' });
  mw(req, res, () => {
    if (requestLooksProgrammatic(req)) {
      return res.status(404).json({
        error: 'No server/API route is active for this deployment.',
        detail: 'This request fell through to the static file handler. Deploy the project as a Server App or add the requested API route.',
        path: req.originalUrl || req.url
      });
    }
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
// In-memory cache of custom domain → subdomain mappings, populated from Firebase.
// This avoids a Firebase round-trip on every HTTP request while staying up to date.
const _cdCache = new Map(); // host → subdomain
let _cdCacheLastRefresh = 0;
let _cdCacheRefreshing = false; // [FIX] prevents concurrent Firebase stampede when cache is stale
const CD_CACHE_TTL_MS = 120 * 1000; // [FIX] raised 30s→120s — reduces Firebase stampede on every subdomain request

// ── External URL Proxy cache ──────────────────────────────────────────────
const _epCache = new Map();
let _epCacheLastRefresh = 0;
let _epCacheRefreshing = false;
const EP_CACHE_TTL_MS = 60_000;

async function refreshExternalProxyCache() {
  try {
    if (FIREBASE_RTDB_URL) {
      const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
      const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_external_proxies.json${authQuery}`);
      if (r.ok) {
        const data = await r.json().catch(() => ({}));
        _epCache.clear();
        if (data && typeof data === 'object') {
          for (const entry of Object.values(data)) {
            if (entry && entry.subdomain && entry.externalUrl) {
              _epCache.set(String(entry.subdomain).toLowerCase(), String(entry.externalUrl));
            }
          }
        }
      }
    }
    _epCacheLastRefresh = Date.now();
  } catch(e) {
    console.error('[ExternalProxy] cache refresh error:', e.message);
  }
}

async function refreshCustomDomainCache() {
  try {
    // 1. Firebase (primary store)
    if (FIREBASE_RTDB_URL) {
      const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
      const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_custom_domains.json${authQuery}`);
      if (r.ok) {
        const data = await r.json().catch(() => ({}));
        if (data && typeof data === 'object') {
          for (const entry of Object.values(data)) {
            if (entry && entry.domain && entry.subdomain) {
              _cdCache.set(String(entry.domain).toLowerCase(), String(entry.subdomain));
            }
          }
        }
      }
    }
    // 2. MongoDB (secondary — keeps old entries working if they exist)
    if (isMongoReady()) {
      const docs = await CustomDomain.find().lean().maxTimeMS(3000).catch(() => []);
      for (const doc of docs) {
        if (doc.domain && doc.subdomain) _cdCache.set(String(doc.domain).toLowerCase(), String(doc.subdomain));
      }
    }
    // 3. In-memory fallback store
    for (const entry of memDomains) {
      if (entry.domain && entry.subdomain) _cdCache.set(String(entry.domain).toLowerCase(), String(entry.subdomain));
    }
    _cdCacheLastRefresh = Date.now();
  } catch(e) {
    console.error('[CustomDomain] cache refresh error:', e.message);
  }
}

// Expose a function so the transfer endpoint can push new entries immediately
function upsertCustomDomainCache(domain, subdomain) {
  if (domain && subdomain) _cdCache.set(String(domain).toLowerCase(), String(subdomain));
}

// [FIX] Serve the DeployBoard dashboard's own static assets and SPA routes
// BEFORE the heavy custom-domain and subdomain middleware below.
// Without this, a page reload on joytree.site/dashboard/* goes through:
//   1. Custom-domain cache middleware → may trigger Firebase + MongoDB refresh
//   2. Subdomain router (regex match)
//   3. Many more API handlers...
//   4. express.static → index.html (only near the end of the file)
// If step 1 is momentarily slow, Cloudflare sees a timeout and shows a 502
// even though the server is healthy and the heartbeat is passing fine.
// This fix: for requests arriving on the BASE DOMAIN itself (joytree.site),
// serve static assets and the SPA shell IMMEDIATELY from disk — before Firebase
// or MongoDB are ever consulted. /api/* is exempted so API calls still flow
// through the normal pipeline.
app.use((req, res, next) => {
  const hostEarly = normalizeHostHeader(
    req.headers['x-forwarded-host'] || req.headers.host || ''
  );
  // Only intercept the base domain — subdomains must fall through to the
  // custom-domain and subdomain routers
  if (hostEarly !== BASE_DOMAIN && hostEarly !== 'localhost' && hostEarly !== '') return next();
  // API routes go through normal pipeline
  if (req.path.startsWith('/api/')) return next();
  // Serve static files (JS, CSS, images) that exist on disk
  if (req.path !== '/') {
    const staticPath = path.join(__dirname, req.path);
    if (fs.existsSync(staticPath) && fs.statSync(staticPath).isFile()) {
      return res.sendFile(staticPath);
    }
  }
  // SPA fallback: serve index.html for all other base-domain paths
  const dashIdx = path.join(__dirname, 'index.html');
  if (fs.existsSync(dashIdx)) return res.sendFile(dashIdx);
  next();
});
// ── END early base-domain static serving ─────────────────────────────────────

// If the incoming host matches a saved custom domain, serve that project directly
app.use(async (req, res, next) => {
  const host = normalizeHostHeader(
    req.headers['x-forwarded-host'] ||
    req.headers.host ||
    ''
  );

  // Skip bare base domain and localhost
  if (!host || host === BASE_DOMAIN || host === 'localhost') return next();

  try {
    // [FIX] Only one refresh at a time — without the guard, every concurrent
    // request hitting a stale cache fires its own Firebase fetch simultaneously,
    // creating a stampede that slows all tenant traffic.
    if (Date.now() - _cdCacheLastRefresh > CD_CACHE_TTL_MS && !_cdCacheRefreshing) {
      _cdCacheRefreshing = true;
      _cdCacheLastRefresh = Date.now(); // optimistic so further requests don't pile in
      refreshCustomDomainCache()
        .catch(() => {})
        .finally(() => { _cdCacheRefreshing = false; });
    }

    const cachedSubdomain = _cdCache.get(host);
    // If this host is a *.BASE_DOMAIN subdomain AND is NOT in the custom domain
    // cache, fall through to the normal subdomain router below.
    if (!cachedSubdomain) return next();
    // It IS in the cache — serve it as a custom domain even if it looks like
    // a *.BASE_DOMAIN subdomain (e.g. transfertest.joytree.site mapped to a
    // different project than its own subdomain name).
    
    const cd = { domain: host, subdomain: cachedSubdomain };

    const subdomain = cd.subdomain;
    // [FIX] portRegistry is kept fresh via fs.watch (see above) — no per-request reload.

    const appEntry = portRegistry[subdomain];
    const distDir  = path.join(SITES_DIR, subdomain, 'dist');

    if (appEntry) {
      const target = parseProxyTarget(appEntry);
      if (!target) {
        console.warn(`[Proxy] Ignoring non-stable port registry entry for ${subdomain}: ${appEntry}`);
        return sendProxyError(req, res, 503, 'not_deployed', subdomain, BASE_DOMAIN);
      } else {
        const { host: proxyHost, port: proxyPort } = target;
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
            proxyResponseOrJsonMismatch(req, res, proxyRes, subdomain);
          }
        );
        proxyReq.setTimeout(APP_PROXY_TIMEOUT_MS, () => { proxyReq.destroy(); sendProxyError(req, res, 504, 'gateway_timeout', subdomain, BASE_DOMAIN); });
        proxyReq.on('error', () => sendProxyError(req, res, 502, 'bad_gateway', subdomain, BASE_DOMAIN));
        // [FIX] If the global express.json() middleware above already consumed
        // this request's body (any request with Content-Type: application/json
        // — i.e. virtually every API POST/PUT/PATCH from a deployed app), `req`
        // is now an already-ended stream. req.pipe(proxyReq, {end:true}) never
        // fires proxyReq.end() on an already-ended source, so the proxied
        // request to the app container is never finished/sent — it just hangs
        // until Cloudflare gives up with a 524. Replay the buffered rawBody
        // instead so the request actually completes.
        if (req.rawBody !== undefined) {
          proxyReq.end(req.rawBody);
        } else {
          req.pipe(proxyReq, { end: true });
        }
        req.on('error', () => proxyReq.destroy());
        return;
      }
    }

    if (fs.existsSync(distDir)) {
      return serveStatic(req, res, distDir);
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

  // [FIX] portRegistry is kept fresh via fs.watch (see above), so newly
  // deployed apps are reachable without a blocking per-request disk read.

  const appEntry  = portRegistry[subdomain];
  const distDir   = path.join(SITES_DIR, subdomain, 'dist');

  // ── External URL proxy: mirror a remote site on this subdomain ───────────
  if (Date.now() - _epCacheLastRefresh > EP_CACHE_TTL_MS && !_epCacheRefreshing) {
    _epCacheRefreshing = true;
    _epCacheLastRefresh = Date.now();
    refreshExternalProxyCache().catch(() => {}).finally(() => { _epCacheRefreshing = false; });
  }
  const externalUrl = _epCache.get(subdomain);
  if (externalUrl) {
    try {
      const targetUrl = new URL(req.url === '/' ? externalUrl : externalUrl.replace(/\/$/, '') + req.url);
      const https = require('https');
      const http2 = require('http');
      const lib = targetUrl.protocol === 'https:' ? https : http2;
      const opts = {
        hostname: targetUrl.hostname,
        port: targetUrl.port || (targetUrl.protocol === 'https:' ? 443 : 80),
        path: targetUrl.pathname + targetUrl.search,
        method: req.method,
        headers: { ...req.headers, host: targetUrl.hostname, 'x-forwarded-host': req.headers.host || '' }
      };
      const extReq = lib.request(opts, (extRes) => {
        const resHeaders = { ...extRes.headers };
        if (resHeaders.location) {
          try {
            const loc = new URL(resHeaders.location);
            if (loc.hostname === targetUrl.hostname) {
              loc.hostname = subdomain + '.' + BASE_DOMAIN;
              loc.protocol = 'https:';
              resHeaders.location = loc.toString();
            }
          } catch(_) {}
        }
        delete resHeaders['x-frame-options'];
        delete resHeaders['content-security-policy'];
        res.writeHead(extRes.statusCode, resHeaders);
        extRes.pipe(res);
      });
      extReq.setTimeout(15000, () => { extReq.destroy(); res.status(504).send('External proxy timeout'); });
      extReq.on('error', (e) => { console.error('[ExternalProxy] error:', e.message); res.status(502).send('External proxy error'); });
      if (req.rawBody !== undefined) extReq.end(req.rawBody);
      else req.pipe(extReq, { end: true });
      req.on('error', () => extReq.destroy());
      return;
    } catch(e) {
      console.error('[ExternalProxy] routing error:', e.message);
    }
  }

  // ── Server app: proxy to its isolated Docker container ──────────────────
  if (appEntry) {
    const target = parseProxyTarget(appEntry);
    if (!target) {
      console.warn(`[Proxy] Ignoring non-stable port registry entry for ${subdomain}: ${appEntry}`);
      return sendProxyError(req, res, 503, 'not_deployed', subdomain, BASE_DOMAIN);
    } else {
      const { host: proxyHost, port: proxyPort } = target;
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
        proxyResponseOrJsonMismatch(req, res, proxyRes, subdomain);
      });

      proxyReq.setTimeout(APP_PROXY_TIMEOUT_MS, () => {
        proxyReq.destroy();
        sendProxyError(req, res, 504, 'gateway_timeout', subdomain, BASE_DOMAIN);
      });

      proxyReq.on('error', () => {
        sendProxyError(req, res, 502, 'bad_gateway', subdomain, BASE_DOMAIN);
      });

      // [FIX] Same issue as the custom-domain proxy above: the global
      // express.json() middleware already consumed `req`'s body for any
      // request with Content-Type: application/json (almost every API
      // POST/PUT/PATCH/DELETE-with-body from a deployed app's frontend).
      // Piping an already-ended `req` into proxyReq never calls
      // proxyReq.end(), so the request to the app container is never sent —
      // it just hangs until Cloudflare returns a 524 ~100s later. Replay the
      // buffered rawBody instead so the request actually completes.
      if (req.rawBody !== undefined) {
        proxyReq.end(req.rawBody);
      } else {
        req.pipe(proxyReq, { end: true });
      }
      req.on('error', () => proxyReq.destroy());
      return;
    }
  }

  // ── Static site ───────────────────────────────────────────────────────────
  if (fs.existsSync(distDir)) return serveStatic(req, res, distDir);

  // ── Not deployed ──────────────────────────────────────────────────────────
  sendProxyError(req, res, 503, 'not_deployed', subdomain, BASE_DOMAIN);
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
      detail: 'The app did not respond before the Joytree proxy timeout. It may still be processing a long-running server request — wait a moment and try again, or redeploy from the dashboard if it keeps timing out.',
      meta: [
        { k: 'STATUS',    v: '504 Gateway Timeout' },
        { k: 'REASON',    v: `Upstream response timeout (${Math.round(APP_PROXY_TIMEOUT_MS / 1000)}s)` },
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

// [FIX] applyMemoryLimitsToExistingDbContainers — runs at startup to patch
// any DB containers created before the memory-limiting startArgs were added.
// For MongoDB: uses `docker exec mongosh` to set WiredTiger cache size live
// without restarting the container (zero downtime for existing databases).
// For all engines: updates Docker memory limits via `docker update`.
async function applyMemoryLimitsToExistingDbContainers() {
  try {
    const out = await execP('docker ps --format "{{.Names}}\t{{.Image}}" --filter "name=jt-db-"');
    if (!out) return;
    const containers = out.split('\n').filter(Boolean).map(line => {
      const [name, image] = line.split('\t');
      return { name: (name||'').trim(), image: (image||'').trim() };
    }).filter(c => c.name.startsWith('jt-db-'));

    for (const { name, image } of containers) {
      try {
        // Get current Docker memory limit
        const inspectOut = await execP(`docker inspect ${name} --format "{{.HostConfig.Memory}}"`);
        const currentBytes = Number(inspectOut.trim()) || 0;
        const memStr = currentBytes > 0 ? bytesToDockerMem(currentBytes) : '256m';
        const memBytes = parseMemToBytes(memStr);

        // Update docker memory cap if it's 0 (unlimited) or too low
        if (currentBytes === 0 || currentBytes < 128 * 1024 * 1024) {
          const newMem = process.env.DB_DEFAULT_MEMORY || '256m';
          const newSwap = bytesToDockerMem(parseMemToBytes(newMem) * 2);
          await execP(`docker update --memory=${newMem} --memory-swap=${newSwap} --pids-limit=300 ${name}`);
          console.log(`[DB Mem] Updated ${name} memory cap to ${newMem}`);
        }

        // For MongoDB: set WiredTiger cache size live via mongosh (no restart needed)
        if (image.includes('mongo')) {
          const cacheGb = Math.max(0.1, Math.round((memBytes * 0.6) / (1024**3) * 10) / 10);
          const envOut = await execP(`docker inspect ${name} --format "{{range .Config.Env}}{{println .}}{{end}}"`);
          const envMap = {};
          envOut.split('\n').forEach(l => { const e = l.indexOf('='); if (e > 0) envMap[l.slice(0,e)] = l.slice(e+1); });
          const user = envMap['MONGO_INITDB_ROOT_USERNAME'] || 'root';
          const pass = envMap['MONGO_INITDB_ROOT_PASSWORD'] || '';
          const mongoshCmd = `docker exec ${name} mongosh --quiet --username ${user} --password "${pass}" --authenticationDatabase admin --eval "db.adminCommand({setParameter:1, wiredTigerEngineRuntimeConfig:'cache_size=${cacheGb}G'})" admin`;
          const result = await execP(mongoshCmd);
          if (result.includes('"ok" : 1') || result.includes('"ok":1') || result.includes('ok: 1')) {
            console.log(`[DB Mem] Set MongoDB WiredTiger cache to ${cacheGb}GB on ${name}`);
          } else {
            console.warn(`[DB Mem] WiredTiger set may have failed on ${name}:`, result.slice(0,200));
          }
        }
      } catch (e) {
        console.warn(`[DB Mem] Could not update ${name}:`, e.message?.slice(0,100));
      }
    }
  } catch (e) {
    console.warn('[DB Mem] applyMemoryLimitsToExistingDbContainers error:', e.message);
  }
}

// [FIX] autoRepairDbDnsRecords — called at startup and after VPS IP detection.
// Finds all databases in the system, checks if their Cloudflare DNS record is
// proxied (orange-cloud), and flips it to proxied:false (DNS-only A record).
// This is what makes external apps able to connect — Cloudflare's HTTP proxy
// blocks raw TCP on non-standard ports, so every DB subdomain must bypass it.
async function autoRepairDbDnsRecords() {
  if (!CF_API_TOKEN || !CF_ZONE_ID) return;
  const ip = getVpsIp();
  if (!ip) return;
  try {
    const h = { 'Authorization': `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' };
    // Fetch all DNS records for the zone
    const r = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records?per_page=100`, { headers: h }).then(r => r.json());
    const allRecords = r.result || [];
    // Find all db-* subdomains that are still proxied
    const broken = allRecords.filter(rec => rec.name.startsWith('db-') && rec.proxied);
    if (broken.length === 0) {
      console.log('[DB DNS] All db-* records are already DNS-only — nothing to fix');
      return;
    }
    console.log(`[DB DNS] Found ${broken.length} proxied db-* record(s) — fixing now...`);
    for (const rec of broken) {
      const subdomain = rec.name.split('.')[0];
      const body = { type: 'A', name: subdomain, content: ip, proxied: false, ttl: 60 };
      const u = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records/${rec.id}`, {
        method: 'PUT', headers: h, body: JSON.stringify(body)
      }).then(r => r.json());
      if (u.success) console.log(`[DB DNS] Fixed ${rec.name} → A ${ip} (DNS-only)`);
      else console.warn(`[DB DNS] Failed to fix ${rec.name}:`, u.errors?.[0]?.message);
    }
    // Also create records for any DB containers that have no DNS entry at all
    // (e.g. created while the wildcard was the only record)
    try {
      const mongoose = require('mongoose');
      if (mongoose.connection.readyState === 1) {
        const dbs = await Database.find({ status: 'running' }).lean().catch(() => []);
        for (const db of dbs) {
          const host = publicDbHost(db);
          if (!host) continue;
          const subdomain = host.split('.')[0];
          const alreadyExists = allRecords.some(rec => rec.name === host || rec.name === subdomain);
          if (!alreadyExists) {
            const body = { type: 'A', name: subdomain, content: ip, proxied: false, ttl: 60 };
            const u = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records`, {
              method: 'POST', headers: h, body: JSON.stringify(body)
            }).then(r => r.json());
            if (u.success) console.log(`[DB DNS] Created missing record: ${host} → A ${ip} (DNS-only)`);
          }
        }
      }
    } catch (_) {}
  } catch (e) {
    console.warn('[DB DNS] autoRepairDbDnsRecords error:', e.message);
  }
}

// ── Cloudflare ────────────────────────────────────────────────────────────────
// [FIX] Database subdomains (db-*) MUST be created with proxied:false (DNS-only /
// grey cloud). Cloudflare's HTTP proxy only forwards ports 80 and 443 — raw TCP
// connections on the random DB host-ports (14000-15000 range) are silently dropped
// before they ever reach the VPS, making every user-created database unreachable
// from external apps. App subdomains stay proxied:true so they keep HTTPS via the
// Cloudflare Tunnel. DB subdomains need a direct A record pointing at the VPS IP
// instead of a CNAME to the tunnel, because Cloudflare won't proxy TCP anyway.
async function registerSubdomain(subdomain, { isDatabase = false } = {}) {
  const fullDomain  = `${subdomain}.${BASE_DOMAIN}`;
  const forceSpecificRecords = process.env.CF_FORCE_SPECIFIC_RECORDS === 'true';
  const wildcardMode = process.env.CF_WILDCARD_MODE !== 'false';
  const shouldSkipRecordCreate = wildcardMode && !forceSpecificRecords;
  if (shouldSkipRecordCreate || !CF_API_TOKEN || !CF_ZONE_ID) {
    return { ok: true, url: `https://${fullDomain}` };
  }
  try {
    const lookup = await fetch(`https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records?name=${fullDomain}`, {
      headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` }
    });
    const lookupData = await lookup.json();
    const existing = lookupData?.result?.[0];

    let body;
    if (isDatabase) {
      // DB subdomains: DNS-only A record pointing directly at the VPS public IP.
      // proxied:false means Cloudflare just resolves the IP — raw TCP reaches the VPS.
      const vpsIp = getVpsIp();
      if (!vpsIp || !vpsIp.includes('.')) {
        // VPS_IP not set — fall back to DNS-only CNAME (still grey-clouds it)
        const target = CF_TUNNEL_ID ? `${CF_TUNNEL_ID}.cfargotunnel.com` : BASE_DOMAIN;
        body = { type: 'CNAME', name: subdomain, content: target, proxied: false, ttl: 60 };
      } else {
        body = { type: 'A', name: subdomain, content: vpsIp, proxied: false, ttl: 60 };
      }
    } else {
      // App subdomains: CNAME to Cloudflare Tunnel, proxied (orange cloud) for HTTPS
      const target = CF_TUNNEL_ID ? `${CF_TUNNEL_ID}.cfargotunnel.com` : BASE_DOMAIN;
      body = { type: 'CNAME', name: subdomain, content: target, proxied: true, ttl: 1 };
    }

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
  return { ok: true, url: isDatabase ? fullDomain : `https://${fullDomain}` };
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
  workingDir: { type: String, default: '' },
  nodeVer:    { type: String, default: '20' },
  siteType:   { type: String, default: 'static' },
  appPort:    { type: Number, default: 0 },
  billingPlan: { type: String, default: 'free' },
  memoryLimit: { type: String, default: '' },
  cpuShares:  { type: Number, default: 0 },
  memorySwap: { type: String, default: '' },
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
  googleId: { type: String, default: '', index: true },
  googleAvatarUrl: { type: String, default: '' },
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

// [FIX] Many projects here use a custom string id rather than a real Mongo
// ObjectId -- subdomain-based uploads ("e-commerce-test"), or "local_..."
// ids for git-deployed projects. Project.findById()/findOne({_id}) throws a
// CastError for any of these instead of just returning null, which was
// surfacing as opaque 500s ("Cast to ObjectId failed for value ...") on
// several endpoints (get project, runtime logs, deployment history) for
// exactly this class of project. This is the single place that logic lives
// now -- every endpoint below calls this instead of Project.findById
// directly, so a project not existing looks like a normal 404 regardless of
// which id format it has.
function looksLikeObjectId(id) {
  return mongoose.Types.ObjectId.isValid(id) && String(new mongoose.Types.ObjectId(id)) === id;
}
async function findProjectByAnyId(id) {
  if (looksLikeObjectId(id)) {
    const byObjectId = await Project.findById(id);
    if (byObjectId) return byObjectId;
  }
  return Project.findOne({ $or: [{ subdomain: id }, { name: id }, { id: id }] });
}

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
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    let r;
    try { r = await fetch(url, { method: 'GET', headers: { 'Accept': 'application/json' }, signal: controller.signal }); }
    finally { clearTimeout(timer); }
    if (!r.ok) return null;
    const data = await r.json();
    return (data && typeof data === 'object') ? data : null;
  } catch { return null; }
}
async function writeWorkspaceToFirebase(user, workspace) {
  try {
    const url = firebaseWorkspaceUrl(user);
    if (!url) {
      console.warn('[Firebase] writeWorkspaceToFirebase: empty URL — FIREBASE_RTDB_URL=' + (FIREBASE_RTDB_URL||'MISSING') + ' key=' + (firebaseWorkspaceKey(user)||'MISSING'));
      return false;
    }
    const payload = workspace && typeof workspace === 'object' ? workspace : {};
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 8000);
    let r;
    try { r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), signal: controller.signal }); }
    finally { clearTimeout(timer); }
    if (!r.ok) {
      const body = await r.text().catch(()=>'');
      console.warn('[Firebase] writeWorkspaceToFirebase HTTP', r.status, body.slice(0,200));
    } else {
      // [FIX] Firebase is the source of truth, but localAuth.users[].workspace
      // is a separate in-memory (+ on-disk) cache that other code paths --
      // most notably updateLocalWorkspaceProject(), used by the autodeploy
      // poller on every interval tick -- read AND write wholesale, entirely
      // independent of this function. Before this fix, only a handful of the
      // ~30 call sites of writeWorkspaceToFirebase remembered to also update
      // that cache. Every one that didn't was a ticking time bomb: any write
      // made only to Firebase could be silently clobbered the next time a
      // stale-cache writer (like the autodeploy poller) did its own
      // read-modify-write of the whole workspace object. This is exactly
      // what made deleted projects reappear and made newly-created ones
      // vanish before they could even be queried. Syncing the cache here,
      // once, for every successful write, protects every current and future
      // caller instead of relying on each one to remember to do it.
      try {
        const uid = String(user?._id || user?.id || '');
        const uemail = String(user?.email || '').trim().toLowerCase();
        const localUser = localAuth.users.find(u =>
          (uid && String(u.id || u._id || '') === uid) ||
          (uemail && String(u.email || '').trim().toLowerCase() === uemail)
        );
        if (localUser) {
          localUser.workspace = payload;
          saveLocalAuth();
        }
      } catch (sy) {
        console.warn('[Firebase] writeWorkspaceToFirebase: localAuth cache sync failed (non-fatal):', sy.message);
      }
    }
    return r.ok;
  } catch(e) {
    console.warn('[Firebase] writeWorkspaceToFirebase exception:', e.message);
    return false;
  }
}


async function syncDeploymentProjectToFirebase(user, { project, deployment, status, liveUrl = '', envVars = null }) {
  try {
    const userForFirebase = await enrichAuthUser(user);
    if (!userForFirebase?.email || !project) return false;

    const nowIso = new Date().toISOString();
    const projectId = String(project._id || project.id || '');
    const deployId = deployment ? String(deployment._id || deployment.id || '') : '';
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
    ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];

    const pIdx = ws.projects.findIndex(p =>
      String(p.id || p._id || '') === projectId ||
      String(p.subdomain || '') === String(project.subdomain || '')
    );
    const existingProject = pIdx >= 0 ? (ws.projects[pIdx] || {}) : {};
    const projectSnapshot = {
      ...existingProject,
      id: projectId || existingProject.id,
      _id: projectId || existingProject._id,
      name: project.name || existingProject.name || '',
      subdomain: project.subdomain || existingProject.subdomain || '',
      repoUrl: project.repoUrl || existingProject.repoUrl || '',
      branch: project.branch || existingProject.branch || 'main',
      installCmd: project.installCmd || existingProject.installCmd || 'npm install',
      buildCmd: project.buildCmd || existingProject.buildCmd || 'npm run build',
      startCmd: project.startCmd || existingProject.startCmd || '',
      outputDir: project.outputDir || existingProject.outputDir || 'dist',
      workingDir: project.workingDir || '',
      nodeVer: project.nodeVer || existingProject.nodeVer || '20',
      siteType: project.siteType || existingProject.siteType || 'static',
      billingPlan: project.billingPlan || existingProject.billingPlan || 'free',
      memoryLimit: project.memoryLimit || existingProject.memoryLimit || '',
      cpuShares: project.cpuShares || existingProject.cpuShares || 0,
      memorySwap: project.memorySwap || existingProject.memorySwap || '',
      status: status || existingProject.status || 'building',
      liveUrl: liveUrl || project.liveUrl || existingProject.liveUrl || '',
      updatedAt: nowIso
    };
    if (envVars && typeof envVars === 'object') {
      projectSnapshot.envVars = { ...(existingProject.envVars || {}), ...envVars };
    }
    if (pIdx >= 0) ws.projects[pIdx] = projectSnapshot;
    else ws.projects.unshift({ ...projectSnapshot, createdAt: nowIso });

    if (deployment && deployId) {
      const dIdx = ws.deployments.findIndex(d => String(d.id || d._id || '') === deployId);
      const existingDeployment = dIdx >= 0 ? (ws.deployments[dIdx] || {}) : {};
      const deploymentSnapshot = {
        ...existingDeployment,
        id: deployId,
        _id: deployId,
        projectId: projectId || existingDeployment.projectId,
        projectName: project.name || existingDeployment.projectName || '',
        subdomain: project.subdomain || existingDeployment.subdomain || '',
        branch: project.branch || existingDeployment.branch || 'main',
        status: status || existingDeployment.status || 'building',
        source: deployment.source || existingDeployment.source || 'manual',
        triggerSha: deployment.triggerSha || existingDeployment.triggerSha || '',
        duration: deployment.duration ?? existingDeployment.duration,
        startedAt: deployment.startedAt ? new Date(deployment.startedAt).toISOString() : (existingDeployment.startedAt || nowIso),
        endedAt: deployment.endedAt ? new Date(deployment.endedAt).toISOString() : existingDeployment.endedAt
      };
      if (dIdx >= 0) ws.deployments[dIdx] = deploymentSnapshot;
      else ws.deployments.unshift(deploymentSnapshot);
      if (ws.deployments.length > 100) ws.deployments = ws.deployments.slice(0, 100);
    }

    await writeWorkspaceToFirebase(userForFirebase, ws);
    refreshApiKeySnapshot(userForFirebase, ws).catch(() => {});
    return true;
  } catch (e) {
    console.warn('[Firebase] Failed to sync deployment project:', e.message);
    return false;
  }
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
<html><body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif;color:#202124;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:32px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:560px;background:#ffffff;">
        <tr>
          <td style="padding:24px 32px 0;">
            <table role="presentation" width="100%"><tr>
              <td style="width:32px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JoyTree" width="28" height="28" style="width:28px;height:28px;border-radius:6px;display:block;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;padding-left:10px;">
                <span style="font-size:15px;font-weight:600;color:#202124;">JoyTree</span>
              </td>
            </tr></table>
          </td>
        </tr>
        <tr><td style="padding:24px 32px 4px;">
          <h2 style="margin:0 0 4px;font-size:19px;font-weight:500;color:#202124;">Payment received</h2>
          <p style="margin:0 0 20px;color:#5f6368;font-size:14px;line-height:1.6;">
            Thanks for your payment. Your ${planLabel} subscription is now active.
          </p>
          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-top:1px solid #e8eaed;">
            <tr><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#5f6368;">Plan</td><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#202124;text-align:right;">${planLabel}</td></tr>
            <tr><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#5f6368;">Amount</td><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#202124;text-align:right;">${currency} ${amountMajor.toFixed(2)}</td></tr>
            <tr><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#5f6368;">Reference</td><td style="padding:12px 0;border-bottom:1px solid #e8eaed;font-size:13px;color:#202124;text-align:right;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;">${reference || '-'}</td></tr>
            <tr><td style="padding:12px 0;font-size:13px;color:#5f6368;">Paid at</td><td style="padding:12px 0;font-size:13px;color:#202124;text-align:right;">${paidText}</td></tr>
          </table>
          <div style="margin-top:24px;">
            <a href="https://${BASE_DOMAIN}/dashboard/usage" style="display:inline-block;background:#10b981;color:#ffffff;text-decoration:none;padding:9px 20px;border-radius:4px;font-weight:500;font-size:14px;">View billing &amp; usage</a>
          </div>
        </td></tr>
        <tr><td style="padding:24px 32px 24px;">
          <hr style="border:none;border-top:1px solid #e8eaed;margin:0 0 16px;">
          <p style="margin:0;color:#80868b;font-size:12px;line-height:1.6;">This is an automated billing confirmation from JoyTree. Reply to this email if you have a billing question.</p>
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
  const firstName = String(userName || '').trim().split(/\s+/)[0] || 'there';
  const dashboardUrl = `https://${BASE_DOMAIN}/dashboard`;
  const deployUrl = `https://${BASE_DOMAIN}/dashboard/new-deploy`;
  const docsUrl = `https://${BASE_DOMAIN}/dashboard/docs`;
  const supportUrl = `https://${BASE_DOMAIN}/dashboard/support`;

  const subject = 'Welcome to JoyTree';

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
<html><body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif;color:#202124;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:32px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:560px;background:#ffffff;">
        <tr>
          <td style="padding:24px 32px 0;">
            <table role="presentation" width="100%"><tr>
              <td style="width:32px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JoyTree" width="28" height="28" style="width:28px;height:28px;border-radius:6px;display:block;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;padding-left:10px;">
                <span style="font-size:15px;font-weight:600;color:#202124;">JoyTree</span>
              </td>
            </tr></table>
          </td>
        </tr>
        <tr><td style="padding:24px 32px 4px;">
          <h2 style="margin:0 0 4px;font-size:19px;font-weight:500;color:#202124;">Welcome to JoyTree, ${firstName}</h2>
          <p style="margin:0 0 16px;color:#5f6368;font-size:14px;line-height:1.6;">
            Your account is verified and your workspace is ready. JoyTree is a deployment platform that takes your code from a GitHub repository — or a plain file upload — to a live, publicly accessible URL, handling the build, hosting, and routing for you.
          </p>
          <p style="margin:0 0 20px;color:#5f6368;font-size:14px;line-height:1.6;">
            Connect a repository and JoyTree clones your code, installs dependencies, runs your build, and deploys the result to a subdomain under joytree.site. If you enable auto-deploy, future pushes to your selected branch are rebuilt and redeployed automatically, with no manual steps.
          </p>

          <p style="margin:20px 0 8px;color:#202124;font-size:13px;font-weight:600;">What's included in your account</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">GitHub deployments</strong> — connect any public or private repo; JoyTree auto-detects your stack and build settings.</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">Real-time logs</strong> — every build and runtime event streamed live, from clone to server boot.</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">Custom domains</strong> — point your own domain at any deployment; DNS and HTTPS are handled for you.</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">Instant rollback</strong> — every deployment is kept in history; revert to a previous version in one click.</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">Environment variables</strong> — store secrets and config per project, injected securely at build and runtime.</p>
          <p style="margin:0 0 20px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">Plans &amp; billing</strong> — view usage, upgrade or downgrade, and manage invoices from your dashboard at any time.</p>

          <p style="margin:20px 0 8px;color:#202124;font-size:13px;font-weight:600;">Your first deployment, in three steps</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">1.</strong> Open your dashboard and click New Deployment, then authorize GitHub and pick a repository.</p>
          <p style="margin:0 0 6px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">2.</strong> Review the auto-detected build settings, add any environment variables you need, and click Deploy.</p>
          <p style="margin:0 0 24px;color:#5f6368;font-size:13px;line-height:1.7;"><strong style="color:#202124;">3.</strong> Once the build succeeds your project is live at a joytree.site subdomain — share it, attach your own domain, or enable auto-deploy for future pushes.</p>

          <div style="margin-bottom:24px;">
            <a href="${dashboardUrl}" style="display:inline-block;background:#10b981;color:#ffffff;text-decoration:none;padding:9px 20px;border-radius:4px;font-weight:500;font-size:14px;">Open dashboard</a>
          </div>
          <p style="margin:0 0 4px;color:#5f6368;font-size:13px;line-height:1.8;">
            <a href="${deployUrl}" style="color:#1a73e8;text-decoration:none;">Create a deployment</a> &nbsp;·&nbsp;
            <a href="${docsUrl}" style="color:#1a73e8;text-decoration:none;">Documentation</a> &nbsp;·&nbsp;
            <a href="${supportUrl}" style="color:#1a73e8;text-decoration:none;">Support</a>
          </p>
        </td></tr>
        <tr><td style="padding:24px 32px 24px;">
          <hr style="border:none;border-top:1px solid #e8eaed;margin:0 0 16px;">
          <p style="margin:0;color:#80868b;font-size:12px;line-height:1.6;">You're receiving this email because you created an account at ${BASE_DOMAIN}. If this wasn't you, you can ignore this message.</p>
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
  <title>JoyTree — Verify your email</title>
</head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif;color:#202124;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="padding:32px 12px;">
    <tr><td align="center">
      <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:560px;background:#ffffff;">
        <tr>
          <td style="padding:24px 32px 0;">
            <table role="presentation" width="100%"><tr>
              <td style="width:32px;vertical-align:middle;">
                <img src="${logoUrl}" alt="JoyTree" width="28" height="28" style="width:28px;height:28px;border-radius:6px;display:block;object-fit:cover;">
              </td>
              <td style="vertical-align:middle;padding-left:10px;">
                <span style="font-size:15px;font-weight:600;color:#202124;">JoyTree</span>
              </td>
            </tr></table>
          </td>
        </tr>
        <tr><td style="padding:24px 32px 4px;">
          <h2 style="margin:0 0 4px;font-size:19px;font-weight:500;color:#202124;">Verify your email address</h2>
          <p style="margin:0 0 24px;color:#5f6368;font-size:14px;line-height:1.6;">
            Enter this code to confirm it's you. It expires in 10 minutes.
          </p>
          <div style="text-align:center;margin-bottom:24px;">
            ${digitBoxesHtml}
          </div>
          <p style="margin:0 0 4px;color:#5f6368;font-size:13px;line-height:1.8;">
            This code is single-use and expires automatically after 10 minutes. Don't share it with anyone — JoyTree staff will never ask you for it, and it should only ever be entered on joytree.site.
          </p>
          <p style="margin:12px 0 0;color:#5f6368;font-size:13px;line-height:1.8;">
            If you didn't request this code, you can safely ignore this email — no action is needed.
          </p>
        </td></tr>
        <tr><td style="padding:24px 32px 24px;">
          <hr style="border:none;border-top:1px solid #e8eaed;margin:0 0 16px;">
          <p style="margin:0;color:#80868b;font-size:12px;line-height:1.6;">This code was sent to ${email} at your request. &copy; ${year} JoyTree</p>
        </td></tr>
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

  // Personal API key (jtk_...) — used by the Joytree CLI. Checked first since
  // it has a distinct, unambiguous prefix and lets attachAuthIfPresent-based
  // routes (runtime-logs, deployments, etc.) work for CLI requests too.
  const rawApiKey = auth.startsWith('Bearer jtk_') ? auth.slice(7)
                   : (req.query && String(req.query.api_key || '').startsWith('jtk_') ? String(req.query.api_key) : null);
  if (rawApiKey) {
    try {
      const keyHash  = crypto.createHash('sha256').update(rawApiKey).digest('hex');
      const indexUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
        (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
      const idxR = await fetch(indexUrl, { headers: { Accept: 'application/json' } }).catch(() => null);
      if (idxR && idxR.ok) {
        const idxData = await idxR.json().catch(() => null);
        const emailKey = idxData && idxData.emailKey;
        if (emailKey) {
          const rec = await getApiKeyRecord(emailKey);
          if (rec && rec.key === rawApiKey && !rec.disabled) {
            let user = null;
            if (isDbReady()) {
              user = await User.findOne({ email: rec.email }).catch(() => null);
            } else {
              user = localAuth.users.find(u => String(u.email || '').toLowerCase() === rec.email) || null;
            }
            if (user) return user;
          }
        }
      }
    } catch (_) {}
    return null; // valid-looking jtk_ key but resolution failed — do not fall through to session check
  }

  // Also accept token from query string — needed for EventSource and browser APIs
  // that cannot set custom headers (e.g. ?token=xxx)
  const token = (auth.startsWith('Bearer ') ? auth.slice(7) : '') || (req.query && req.query.token ? String(req.query.token) : '');
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

// Link a Google OAuth access token into Firebase so the user appears in
// Firebase Auth under the google.com provider — same pattern as GitHub above.
async function upsertFirebaseGoogleUser(googleAccessToken) {
  if (!FIREBASE_API_KEY || !googleAccessToken) return null;
  const r = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithIdp?key=${encodeURIComponent(FIREBASE_API_KEY)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      requestUri: 'https://localhost/firebase-auth',
      returnSecureToken: true,
      returnIdpCredential: true,
      postBody: `access_token=${encodeURIComponent(googleAccessToken)}&providerId=google.com`
    })
  });
  const d = await r.json().catch(() => null);
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
    // 1. Internal deploy key (server-to-server calls)
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

    // 2. Personal API key (jtk_...) — used by the Joytree CLI
    const authHeader = String(req.headers.authorization || '').trim();
    const rawApiKey  = authHeader.startsWith('Bearer jtk_') ? authHeader.slice(7)
                     : String(req.query.api_key || '').startsWith('jtk_') ? String(req.query.api_key) : null;
    if (rawApiKey) {
      try {
        const keyHash = crypto.createHash('sha256').update(rawApiKey).digest('hex');
        const indexUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
          (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
        const idxR = await fetch(indexUrl, { headers: { Accept: 'application/json' } }).catch(() => null);
        let emailKey = null;
        if (idxR && idxR.ok) {
          const idxData = await idxR.json().catch(() => null);
          if (idxData && idxData.emailKey) emailKey = idxData.emailKey;
        }
        if (emailKey) {
          const rec = await getApiKeyRecord(emailKey);
          if (rec && rec.key === rawApiKey && !rec.disabled) {
            let user = null;
            if (isDbReady()) {
              user = await User.findOne({ email: rec.email }).catch(() => null);
            } else {
              user = localAuth.users.find(u => String(u.email || '').toLowerCase() === rec.email) || null;
            }
            if (user) {
              req.user = user;
              req.apiKeyEmailKey = emailKey;
              // Fire-and-forget last-used update
              fetch(`${FIREBASE_RTDB_URL}/deployboard_api_keys/${emailKey}/lastUsed.json` +
                (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : ''), {
                method: 'PUT', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(new Date().toISOString())
              }).catch(() => {});
              return next();
            }
          }
        }
      } catch (_) {}
      return res.status(401).json({ error: 'Unauthorized' });
    }

    // 3. Session token (browser login)
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
// Only attempt MongoDB when a real URI is explicitly provided via env var.
// If MONGODB_URI was not set, the value falls back to the localhost default
// which will always fail when the site runs on Firebase-only mode — causing
// long retry loops and slow server startup. Skip it entirely in that case.
if (process.env.MONGODB_URI) {
  connectMongo();
} else {
  console.log('[DB] MONGODB_URI not set — running in Firebase-only mode. MongoDB skipped.');
}

// ── Build runner ──────────────────────────────────────────────────────────────
const { runBuild, getPlanRuntimeProfile, normalizeMemoryLimit } = require('./buildRunner');

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
    // [FIX] async — execSync here blocked the event loop (and thus the
    // proxy for every deployed app) for as long as `docker ps` took.
    const containers = await execP("docker ps --filter 'name=db-' --format '{{.Names}}' 2>/dev/null || echo ''");
    runningContainers = containers ? containers.split('\n').filter(Boolean).length : 0;
  } catch(e) {}
  try {
    // [FIX] async — `du -sh` on /var/www/user-sites recursively scans every
    // deployed project's files and gets slower as more apps are deployed;
    // running it synchronously froze all tenant traffic for its duration.
    const du = await execP('du -sh /var/www/user-sites 2>/dev/null || echo "N/A"');
    diskUsed = du.split('\t')[0];
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
  res.json({ user: { id: req.user._id || req.user.id, email: req.user.email, name: req.user.name, githubUsername: req.user.githubUsername, githubAvatarUrl: req.user.githubAvatarUrl || '', googleAvatarUrl: req.user.googleAvatarUrl || '', firebaseUid: req.user.firebaseUid || '' } });
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

// Link GitHub to an already-logged-in email account (keeps existing session)
app.post('/api/auth/github/link-account', requireAuth, async (req, res) => {
  try {
    if (!GITHUB_CLIENT_ID || !GITHUB_CLIENT_SECRET) {
      return res.status(400).json({ error: 'GitHub OAuth is not configured on this server.' });
    }
    const code = String(req.body.code || '').trim();
    if (!code) return res.status(400).json({ error: 'Missing GitHub OAuth code' });

    // Exchange the code for an access token
    const tokenResp = await fetch('https://github.com/login/oauth/access_token', {
      method: 'POST',
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify({ client_id: normalizeGitHubClientId(GITHUB_CLIENT_ID), client_secret: GITHUB_CLIENT_SECRET, code })
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(400).json({ error: tokenData.error_description || 'GitHub token exchange failed' });

    // Fetch GitHub user info
    const ghUserResp = await fetch('https://api.github.com/user', {
      headers: { 'Authorization': `Bearer ${tokenData.access_token}`, 'User-Agent': 'deployboard' }
    });
    const ghUser = await ghUserResp.json();

    // Merge GitHub identity into the currently logged-in user account
    const userId = String(req.user?._id || req.user?.id || '');
    if (isDbReady()) {
      const user = await User.findById(userId);
      if (!user) return res.status(404).json({ error: 'User account not found' });
      user.githubId = String(ghUser.id);
      user.githubUsername = ghUser.login || '';
      user.githubAccessToken = tokenData.access_token;
      user.githubAvatarUrl = ghUser.avatar_url || user.githubAvatarUrl || '';
      user.name = user.name || ghUser.name || ghUser.login || '';
      user.updatedAt = new Date();
      await user.save();
      res.json({ ok: true, githubUsername: user.githubUsername, githubAvatarUrl: user.githubAvatarUrl });
    } else {
      const u = localAuth.users.find(x => String(x.id || x._id || '') === userId);
      if (!u) return res.status(404).json({ error: 'User account not found' });
      u.githubId = String(ghUser.id);
      u.githubUsername = ghUser.login || '';
      u.githubAccessToken = tokenData.access_token;
      u.githubAvatarUrl = ghUser.avatar_url || u.githubAvatarUrl || '';
      u.name = u.name || ghUser.name || ghUser.login || '';
      saveLocalAuth();
      res.json({ ok: true, githubUsername: u.githubUsername, githubAvatarUrl: u.githubAvatarUrl });
    }
  } catch (e) {
    res.status(500).json({ error: e.message || 'GitHub link failed' });
  }
});

// ── Google OAuth endpoints ────────────────────────────────────────────────────

// GET /api/auth/google/url — build the Google consent-screen URL
app.get('/api/auth/google/url', (req, res) => {
  if (!GOOGLE_CLIENT_ID) return res.status(400).json({ error: 'Google OAuth client ID is not configured' });
  const origin = getPublicOrigin(req);
  const redirectUri = `${origin}/`;
  const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
  url.searchParams.set('client_id', GOOGLE_CLIENT_ID);
  url.searchParams.set('redirect_uri', redirectUri);
  url.searchParams.set('response_type', 'code');
  url.searchParams.set('scope', 'openid email profile');
  url.searchParams.set('access_type', 'offline');
  url.searchParams.set('prompt', 'select_account');
  url.searchParams.set('state', 'deployboard_google_auth');
  res.json({ url: url.toString(), redirectUri });
});

// POST /api/auth/google/exchange — swap the ?code= for tokens, upsert user, issue session
app.post('/api/auth/google/exchange', async (req, res) => {
  try {
    if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
      return res.status(400).json({ error: 'Google OAuth is not configured', details: { missingClientId: !GOOGLE_CLIENT_ID, missingClientSecret: !GOOGLE_CLIENT_SECRET } });
    }
    const code = String(req.body.code || '').trim();
    if (!code) return res.status(400).json({ error: 'Missing code' });

    const origin = getPublicOrigin(req);
    const redirectUri = `${origin}/`;

    // Exchange code → access_token + id_token
    const tokenResp = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: GOOGLE_CLIENT_ID, client_secret: GOOGLE_CLIENT_SECRET, redirect_uri: redirectUri, grant_type: 'authorization_code' }).toString()
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(400).json({ error: tokenData.error_description || tokenData.error || 'Google token exchange failed' });

    // Fetch Google user info
    const profileResp = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
      headers: { 'Authorization': `Bearer ${tokenData.access_token}` }
    });
    const profile = await profileResp.json();
    const email = String(profile.email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'Google account has no email address' });

    // Link into Firebase Auth under google.com provider
    const fbGoogle = await upsertFirebaseGoogleUser(tokenData.access_token);
    const firebaseLinked = !!(fbGoogle && fbGoogle.localId);

    let user;
    if (isDbReady()) {
      user = await User.findOne({ googleId: String(profile.sub) });
      if (!user) user = await User.findOne({ email });
      if (!user) user = new User({ email });
      user.googleId      = String(profile.sub);
      user.googleAvatarUrl = profile.picture || user.googleAvatarUrl || '';
      user.name          = user.name || profile.name || profile.given_name || '';
      user.firebaseUid   = fbGoogle?.localId || user.firebaseUid || '';
      user.updatedAt     = new Date();
      await user.save();

      // Orphan-recovery: reclaim projects that were created before Google login
      try {
        const mongoId = String(user._id);
        const fbWs = await readWorkspaceFromFirebase(user).catch(() => null);
        const wsSubdomains = Array.isArray(fbWs?.projects) ? fbWs.projects.map(p => String(p.subdomain || '')).filter(Boolean) : [];
        if (wsSubdomains.length > 0) {
          await Project.updateMany(
            { subdomain: { $in: wsSubdomains }, ownerUserId: { $in: ['', null] } },
            { $set: { ownerUserId: mongoId } }
          ).catch(() => {});
          await Project.updateMany(
            { subdomain: { $in: wsSubdomains }, ownerUserId: { $regex: /^u_\d+/ } },
            { $set: { ownerUserId: mongoId } }
          ).catch(() => {});
        }
      } catch (_) {}
    } else {
      user = localAuth.users.find(u => u.googleId === String(profile.sub)) || localAuth.users.find(u => u.email === email);
      if (!user) { user = { id: 'u_' + Date.now(), email, googleAvatarUrl: '', githubUsername: '', githubAccessToken: '' }; localAuth.users.push(user); }
      user.googleId        = String(profile.sub);
      user.googleAvatarUrl = profile.picture || user.googleAvatarUrl || '';
      user.name            = user.name || profile.name || profile.given_name || '';
      user.firebaseUid     = fbGoogle?.localId || user.firebaseUid || '';
      saveLocalAuth();
    }

    const pendingToken = await issueEmailVerification(user);
    res.json({ requiresVerification: true, pendingToken, user: { id: user._id || user.id, email: user.email, name: user.name, googleAvatarUrl: user.googleAvatarUrl || '' }, firebaseLinked });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/auth/google/link-account — link Google to an already-logged-in account
app.post('/api/auth/google/link-account', requireAuth, async (req, res) => {
  try {
    if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) return res.status(400).json({ error: 'Google OAuth is not configured on this server.' });
    const code = String(req.body.code || '').trim();
    if (!code) return res.status(400).json({ error: 'Missing Google OAuth code' });

    const origin = getPublicOrigin(req);
    const redirectUri = `${origin}/`;

    const tokenResp = await fetch('https://oauth2.googleapis.com/token', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({ code, client_id: GOOGLE_CLIENT_ID, client_secret: GOOGLE_CLIENT_SECRET, redirect_uri: redirectUri, grant_type: 'authorization_code' }).toString()
    });
    const tokenData = await tokenResp.json();
    if (!tokenData.access_token) return res.status(400).json({ error: tokenData.error_description || 'Google token exchange failed' });

    const profileResp = await fetch('https://www.googleapis.com/oauth2/v3/userinfo', { headers: { 'Authorization': `Bearer ${tokenData.access_token}` } });
    const profile = await profileResp.json();

    const userId = String(req.user?._id || req.user?.id || '');
    if (isDbReady()) {
      const user = await User.findById(userId);
      if (!user) return res.status(404).json({ error: 'User account not found' });
      user.googleId        = String(profile.sub);
      user.googleAvatarUrl = profile.picture || user.googleAvatarUrl || '';
      user.name            = user.name || profile.name || '';
      user.updatedAt       = new Date();
      await user.save();
      res.json({ ok: true, googleAvatarUrl: user.googleAvatarUrl });
    } else {
      const u = localAuth.users.find(x => String(x.id || x._id || '') === userId);
      if (!u) return res.status(404).json({ error: 'User account not found' });
      u.googleId        = String(profile.sub);
      u.googleAvatarUrl = profile.picture || u.googleAvatarUrl || '';
      u.name            = u.name || profile.name || '';
      saveLocalAuth();
      res.json({ ok: true, googleAvatarUrl: u.googleAvatarUrl });
    }
  } catch (e) { res.status(500).json({ error: e.message || 'Google link failed' }); }
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
  // Merge server-side env values into settings so every client (including
  // self-hosters) always gets the correct tunnel ID and base domain for their
  // own server — these are server facts, never overridden by stored workspace data.
  const wsSettings = ws.settings && typeof ws.settings === 'object' ? ws.settings : {};
  const serverSettings = {};
  if (CF_TUNNEL_ID) serverSettings.cfTunnelId = CF_TUNNEL_ID;
  if (BASE_DOMAIN)  serverSettings.baseDomain  = BASE_DOMAIN;
  res.json({
    projects: Array.isArray(ws.projects) ? ws.projects : [],
    deployments: Array.isArray(ws.deployments) ? ws.deployments : [],
    envStore: ws.envStore && typeof ws.envStore === 'object' ? ws.envStore : {},
    settings: { ...wsSettings, ...serverSettings },
    uploadedProjects: Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : []
  });
});

app.post('/api/workspace', requireAuth, async (req, res) => {
  try {
    const payload = req.body || {};
    // Read existing workspace first to preserve uploadedProjects (and any other
    // fields the dashboard sync does not send). Without this, every auto-sync
    // call from the frontend wipes uploadedProjects from Firebase.
    let existingFbWs = {};
    try { existingFbWs = (await readWorkspaceFromFirebase(req.user)) || {}; } catch {}
    // Merge uploadedProjects: start from Firebase's list as source of truth, then
    // upsert any items the client sent (client may have newly uploaded projects).
    const existingUploads = Array.isArray(existingFbWs.uploadedProjects) ? existingFbWs.uploadedProjects : [];
    const incomingUploads = Array.isArray(payload.uploadedProjects) ? payload.uploadedProjects : [];
    const mergedUploads = [...existingUploads];
    for (const p of incomingUploads) {
      if (!p || !p.id) continue;
      const idx = mergedUploads.findIndex(x => x.id === p.id);
      if (idx >= 0) mergedUploads[idx] = { ...mergedUploads[idx], ...p };
      else mergedUploads.unshift(p);
    }
    const workspace = {
      ...existingFbWs,
      projects: Array.isArray(payload.projects) ? payload.projects : [],
      deployments: Array.isArray(payload.deployments) ? payload.deployments : [],
      envStore: payload.envStore && typeof payload.envStore === 'object' ? payload.envStore : {},
      settings: payload.settings && typeof payload.settings === 'object' ? payload.settings : {},
      uploadedProjects: mergedUploads.slice(0, 50)
    };
    const fbSaved = await writeWorkspaceToFirebase(req.user, workspace).catch(() => false);
    if (!fbSaved) console.warn('[Workspace] Firebase write failed for:', req.user.email, '— continuing with DB fallback');
    // Keep API key snapshot in sync so jtk_ key always reflects current projects
    refreshApiKeySnapshot(req.user, workspace).catch(() => {});

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

// ── AI session history (stored in Realtime Database, same as workspace) ──
// Kept under a separate path so the dashboard's frequent /api/workspace syncs
// never clobber it. Mirrors exactly how the app persists everything else.
function firebaseAiHistoryUrl(user) {
  const key = firebaseWorkspaceKey(user);
  if (!FIREBASE_RTDB_URL || !key) return '';
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  return `${FIREBASE_RTDB_URL}/deployboard_aihistory/${key}.json${authQuery}`;
}

app.get('/api/ai/history', requireAuth, async (req, res) => {
  try {
    const url = firebaseAiHistoryUrl(req.user);
    if (!url) return res.json({ items: [] });
    const r = await fetch(url, { headers: { Accept: 'application/json' } }).catch(() => null);
    const data = r && r.ok ? await r.json().catch(() => null) : null;
    let items = Array.isArray(data) ? data : (data && typeof data === 'object' ? Object.values(data) : []);
    items = items.filter(Boolean).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)).slice(0, 30);
    res.json({ items });
  } catch (e) { res.json({ items: [] }); }
});

app.post('/api/ai/history', requireAuth, async (req, res) => {
  try {
    const entry = req.body && typeof req.body === 'object' ? req.body : {};
    if (!entry || !entry.createdAt) entry.createdAt = Date.now();
    const url = firebaseAiHistoryUrl(req.user);
    if (!url) return res.json({ ok: false, items: [] });
    // Read existing, prepend, cap at 30, write back (simple + consistent).
    const rGet = await fetch(url, { headers: { Accept: 'application/json' } }).catch(() => null);
    const cur  = rGet && rGet.ok ? await rGet.json().catch(() => null) : null;
    let items  = Array.isArray(cur) ? cur : (cur && typeof cur === 'object' ? Object.values(cur) : []);
    items = items.filter(Boolean);
    items.unshift(entry);
    // De-dupe by createdAt+repoSlug+status
    const seen = new Set();
    items = items.filter(it => { const k = (it.createdAt||0)+'|'+(it.repoSlug||'')+'|'+(it.status||''); if (seen.has(k)) return false; seen.add(k); return true; });
    items = items.sort((a,b)=>(b.createdAt||0)-(a.createdAt||0)).slice(0, 30);
    await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(items) }).catch(() => {});
    res.json({ ok: true, items });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/ai/history', requireAuth, async (req, res) => {
  try {
    const url = firebaseAiHistoryUrl(req.user);
    if (url) await fetch(url, { method: 'DELETE' }).catch(() => {});
    res.json({ ok: true });
  } catch (e) { res.json({ ok: true }); }
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


app.get('/api/projects/:id/runtime-logs', attachAuthIfPresent, async (req, res) => {
  // [FIX] Complete rewrite of runtime log streaming:
  // Old behaviour: spawn docker logs once → if container not ready or restarts → stream dies forever.
  // New behaviour:
  //   1. Wait up to 120s for the container to exist (post-deploy race window fix)
  //   2. Stream logs with --follow
  //   3. If docker logs exits (container restart/redeploy), retry automatically after 3s
  //   4. SSE retry header tells browser to reconnect in 2s if connection drops at network level
  //   5. keepAlive ping every 20s prevents Cloudflare/nginx from killing idle SSE connections
  let child = null;
  let closed = false;
  let keepAlive = null;

  const send = (event, data) => {
    if (res.writableEnded || closed) return;
    try {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    } catch (_) {}
  };

  const killChild = () => {
    if (child && !child.killed) {
      try { child.kill('SIGTERM'); } catch (_) {}
    }
    child = null;
  };

  try {
    const projectId = String(req.params.id || '');
    let project = null;
    if (isDbReady() && mongoose.Types.ObjectId.isValid(projectId)) {
      project = await Project.findById(projectId).lean().maxTimeMS(5000).catch(() => null);
    }
    if (!project && isDbReady()) {
      project = await Project.findOne({ subdomain: projectId }).lean().maxTimeMS(5000).catch(() => null);
    }
    if (!project && req.user) {
      const ws = (await readWorkspaceFromFirebase(req.user).catch(() => null)) || {};
      project = (Array.isArray(ws.projects) ? ws.projects : []).find(p =>
        String(p.id || p._id || '') === projectId || String(p.subdomain || '') === projectId
      ) || null;
    }
    if (!project) return res.status(404).json({ error: 'Project not found' });

    const subdomain = String(project.subdomain || '').toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/^-|-$/g, '');
    if (!subdomain) return res.status(400).json({ error: 'Project has no subdomain/container mapping' });

    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache, no-transform');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    // Tell the browser to reconnect after 2s if the SSE connection drops
    res.setHeader('X-SSE-Retry', '2000');
    if (typeof res.flushHeaders === 'function') res.flushHeaders();

    // Write SSE retry directive as first line so browser auto-reconnects
    res.write('retry: 2000\n\n');

    req.on('close', () => {
      closed = true;
      clearInterval(keepAlive);
      killChild();
    });

    keepAlive = setInterval(() => send('ping', { t: Date.now() }), 20000);

    // Wait for the container to be available (post-deploy race window)
    // The container is renamed from candidate name after promotion, so it may
    // not exist in portRegistry for a few seconds after the build completes.
    const waitForContainer = async (name, maxWaitMs = 120000) => {
      const start = Date.now();
      while (Date.now() - start < maxWaitMs) {
        if (closed) return false;
        try {
          const result = require('child_process').spawnSync(
            'docker', ['inspect', '--format={{.State.Status}}', name],
            { timeout: 3000 }
          );
          const status = (result.stdout || '').toString().trim();
          if (status === 'running') return true;
          if (status && status !== 'created' && status !== 'restarting') {
            // Container exists but not running yet — keep waiting
          }
        } catch (_) {}
        await new Promise(r => setTimeout(r, 2000));
        // Reload port registry in case it was just written by a fresh deploy
        reloadPortRegistryFromDisk();
      }
      return false;
    };

    const streamLoop = async () => {
      let firstRun = true;
      let tailLines = '200';

      while (!closed) {
        // Always re-read portRegistry to pick up renames after redeploy
        reloadPortRegistryFromDisk();
        const containerName = _containerFromEntry(subdomain, portRegistry[subdomain]);

        if (firstRun) {
          send('meta', { projectId, subdomain, containerName, startedAt: new Date().toISOString() });
          firstRun = false;
        }

        // Check if container is running; if not, wait for it
        const isRunning = await waitForContainer(containerName);
        if (closed) break;
        if (!isRunning) {
          send('status', { message: 'Container not available. Waiting for deployment to complete…' });
          await new Promise(r => setTimeout(r, 5000));
          continue;
        }

        send('status', { message: `Streaming live logs from container: ${containerName}` });

        await new Promise((resolve) => {
          child = require('child_process').spawn(
            'docker', ['logs', '--tail', tailLines, '--follow', '--timestamps', containerName],
            { stdio: ['ignore', 'pipe', 'pipe'] }
          );

          const forward = (stream, source) => {
            let buffered = '';
            stream.on('data', (chunk) => {
              if (closed) return;
              buffered += chunk.toString('utf8');
              const lines = buffered.split(/\r?\n/);
              buffered = lines.pop() || '';
              lines.filter(Boolean).forEach(line => send('log', { source, line }));
            });
            stream.on('end', () => {
              if (buffered.trim()) send('log', { source, line: buffered });
            });
          };

          forward(child.stdout, 'stdout');
          forward(child.stderr, 'stderr');

          child.on('error', (e) => {
            send('status', { message: `Log stream error: ${e.message} — reconnecting in 3s…` });
            resolve();
          });

          child.on('close', (code, signal) => {
            if (!closed) {
              send('status', { message: `Log stream ended (${signal || code}) — reconnecting in 3s…` });
            }
            resolve();
          });
        });

        child = null;
        // After first full stream, only tail last 0 lines on reconnect (avoid duplicate logs)
        tailLines = '0';
        if (!closed) await new Promise(r => setTimeout(r, 3000));
      }

      clearInterval(keepAlive);
      if (!res.writableEnded) res.end();
    };

    streamLoop().catch((e) => {
      send('error', { error: e.message });
      clearInterval(keepAlive);
      if (!res.writableEnded) res.end();
    });

  } catch(e) {
    if (!res.headersSent) return res.status(500).json({ error: e.message });
    send('error', { error: e.message });
    killChild();
    clearInterval(keepAlive);
    if (!res.writableEnded) res.end();
  }
});

// [FIX] This previously called findProjectByAnyId(), which is Mongo-only and
// always returns null now that Mongo has been fully removed from this
// platform -- this endpoint has been silently 404'ing on every call since
// then. Confirmed via search that neither the dashboard frontend nor the
// CLI/MCP (which use the separate, already-correct /api/v1/projects/:id)
// actually call this route, so it was safe to fix properly rather than
// leave it broken: also added requireAuth and scoped the lookup to the
// caller's own workspace. Without that scoping, "fixing" the lookup alone
// would have turned a harmless dead endpoint into a real vulnerability --
// it returns the full project record, envVars included, and this route has
// no auth check at all, so anyone who guessed a project id/subdomain would
// have been able to read any user's secrets.
app.get('/api/projects/:id', requireAuth, async (req, res) => {
  try {
    const reqId = String(req.params.id || '').trim();
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const p = (ws.projects || []).find(pr => pr.id === reqId || pr.subdomain === reqId || pr.name === reqId);
    if (!p) return res.status(404).json({ error: 'Not found' });
    res.json(p);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Remix feature: publish toggle ────────────────────────────────────────────
// Flips a project's `published` flag in the owner's own Firebase workspace.
// This never exposes anything by itself -- it's just metadata on the owner's
// own record. What actually gets shown publicly is controlled entirely by
// GET /api/remix/projects below, which explicitly whitelists safe fields.
app.post('/api/projects/:id/publish', requireAuth, async (req, res) => {
  try {
    const projectId = String(req.params.id || '').trim();
    const published = !!req.body?.published;
    const description = String(req.body?.description || '').slice(0, 500);
    const tags = Array.isArray(req.body?.tags) ? req.body.tags.map(t => String(t).slice(0, 30)).slice(0, 8) : undefined;

    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
    const idx = ws.projects.findIndex(p => p.id === projectId || p.subdomain === projectId || p.name === projectId);
    if (idx < 0) return res.status(404).json({ error: 'Project not found' });

    ws.projects[idx] = {
      ...ws.projects[idx],
      published,
      publishedAt: published ? (ws.projects[idx].publishedAt || new Date().toISOString()) : ws.projects[idx].publishedAt,
      description: description || ws.projects[idx].description || '',
      ...(tags ? { tags } : {}),
      viewCount: ws.projects[idx].viewCount || 0,
      likeCount: ws.projects[idx].likeCount || 0,
      remixCount: ws.projects[idx].remixCount || 0,
    };

    const ok = await writeWorkspaceToFirebase(req.user, ws);
    if (!ok) return res.status(502).json({ error: 'Failed to save publish state' });
    res.json({ ok: true, published, project: ws.projects[idx] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// [SECURITY] This whitelist is the ONLY thing standing between "safe public
// gallery" and "leaking every user's env vars, internal file paths, and
// upload storage locations to the entire internet". Every field returned by
// the endpoints below must be listed here explicitly -- never spread a raw
// project object into a public response, no matter how convenient.
function toPublicRemixFields(p, ownerWs, ownerKey) {
  return {
    id: p.id || p.subdomain,
    name: p.name,
    subdomain: p.subdomain,
    liveUrl: p.liveUrl || (p.subdomain ? `https://${p.subdomain}.${BASE_DOMAIN}` : ''),
    description: p.description || '',
    tags: Array.isArray(p.tags) ? p.tags : [],
    siteType: p.siteType || '',
    framework: p.framework || '',
    repoUrl: (p.repoUrl && p.repoUrl.startsWith('http')) ? p.repoUrl : '',
    source: p.repoUrl && p.repoUrl.startsWith('http') ? 'github' : 'upload',
    publishedAt: p.publishedAt || null,
    viewCount: p.viewCount || 0,
    likeCount: p.likeCount || 0,
    remixCount: p.remixCount || 0,
    owner: {
      name: ownerWs?.name || ownerWs?.githubUsername || (ownerWs?.email ? ownerWs.email.split('@')[0] : 'anonymous'),
      avatarUrl: ownerWs?.githubAvatarUrl || ownerWs?.googleAvatarUrl || '',
    },
    _ownerKey: ownerKey, // internal only, stripped before sending -- see callers
  };
}

async function fetchAllWorkspaces() {
  if (!FIREBASE_RTDB_URL) return {};
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_workspaces.json${authQuery}`);
  return (await r.json().catch(() => ({}))) || {};
}

// [FIX] The remix endpoints below scan across ALL users' workspaces (they
// have to -- a public gallery isn't scoped to one account) and need to write
// back to a SPECIFIC workspace found during that scan. writeWorkspaceToFirebase
// derives its write target by re-sanitizing user.email -- calling it with a
// synthetic {email: <already-sanitized-key>} would re-sanitize an
// already-sanitized string (harmless) but would also break its localAuth
// cache-sync step, which matches by real email and would never find this
// user, silently reintroducing the exact stale-cache bug fixed earlier
// tonight (ba49ee6 / f6dae72). Writing directly to the already-known-correct
// key avoids any re-derivation risk entirely, and does the same cache sync
// keyed off the real key instead of a fabricated email.
async function writeWorkspaceByKnownKey(key, workspace) {
  if (!FIREBASE_RTDB_URL || !key) return false;
  const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  const url = `${FIREBASE_RTDB_URL}/deployboard_workspaces/${key}.json${authQuery}`;
  try {
    const r = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(workspace) });
    if (!r.ok) return false;
    const localUser = localAuth.users.find(u => firebaseWorkspaceKey(u) === key);
    if (localUser) { localUser.workspace = workspace; saveLocalAuth(); }
    return true;
  } catch (_) { return false; }
}

// ── GET /api/remix/projects — public gallery listing ─────────────────────────
// No auth required. Only ever returns projects with published === true, and
// only the whitelisted fields above -- never envVars, uploadFilesDir,
// appPort, memoryLimit, or anything else from the raw project record.
app.get('/api/remix/projects', async (req, res) => {
  try {
    const allWs = await fetchAllWorkspaces();
    const out = [];
    for (const [key, ws] of Object.entries(allWs || {})) {
      const projects = Array.isArray(ws?.projects) ? ws.projects : [];
      for (const p of projects) {
        if (p.published) out.push(toPublicRemixFields(p, ws, key));
      }
    }
    out.sort((a, b) => new Date(b.publishedAt || 0) - new Date(a.publishedAt || 0));
    out.forEach(p => delete p._ownerKey);
    res.json({ ok: true, projects: out, count: out.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── GET /api/remix/projects/:id — single public project detail ───────────────
async function findPublishedProject(id) {
  const allWs = await fetchAllWorkspaces();
  for (const [key, ws] of Object.entries(allWs || {})) {
    const projects = Array.isArray(ws?.projects) ? ws.projects : [];
    const idx = projects.findIndex(p => (p.id === id || p.subdomain === id) && p.published);
    if (idx >= 0) return { ws, key, idx, project: projects[idx] };
  }
  return null;
}

app.get('/api/remix/projects/:id', async (req, res) => {
  try {
    const found = await findPublishedProject(req.params.id);
    if (!found) return res.status(404).json({ error: 'Not found or not published' });
    const pub = toPublicRemixFields(found.project, found.ws, found.key);
    delete pub._ownerKey;
    res.json({ ok: true, project: pub });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// In-memory view-dedup so refreshing doesn't inflate counts -- one view per
// (IP, project) per 30 minutes. Not persisted; resets on restart, which is
// fine for a soft engagement metric like this.
const remixViewDedup = new Map();
app.post('/api/remix/projects/:id/view', async (req, res) => {
  try {
    const found = await findPublishedProject(req.params.id);
    if (!found) return res.status(404).json({ error: 'Not found or not published' });
    const dedupKey = `${req.ip || 'unknown'}:${found.project.id || found.project.subdomain}`;
    const last = remixViewDedup.get(dedupKey);
    if (last && Date.now() - last < 30 * 60 * 1000) {
      return res.json({ ok: true, counted: false, viewCount: found.project.viewCount || 0 });
    }
    remixViewDedup.set(dedupKey, Date.now());
    found.ws.projects[found.idx].viewCount = (found.project.viewCount || 0) + 1;
    await writeWorkspaceByKnownKey(found.key, found.ws);
    res.json({ ok: true, counted: true, viewCount: found.ws.projects[found.idx].viewCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/remix/projects/:id/like', requireAuth, async (req, res) => {
  try {
    const found = await findPublishedProject(req.params.id);
    if (!found) return res.status(404).json({ error: 'Not found or not published' });
    const likerId = String(req.user?._id || req.user?.id || req.user?.email || '');
    found.ws.projects[found.idx].likedBy = Array.isArray(found.ws.projects[found.idx].likedBy) ? found.ws.projects[found.idx].likedBy : [];
    const already = found.ws.projects[found.idx].likedBy.includes(likerId);
    if (already) {
      found.ws.projects[found.idx].likedBy = found.ws.projects[found.idx].likedBy.filter(id => id !== likerId);
    } else {
      found.ws.projects[found.idx].likedBy.push(likerId);
    }
    found.ws.projects[found.idx].likeCount = found.ws.projects[found.idx].likedBy.length;
    await writeWorkspaceByKnownKey(found.key, found.ws);
    res.json({ ok: true, liked: !already, likeCount: found.ws.projects[found.idx].likeCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/remix/projects/:id/remix — the actual remix action ─────────────
// GitHub-sourced projects: forks the repo into the requester's own connected
// GitHub account (their existing OAuth token already has the `repo` scope
// needed -- no re-auth required). Upload-sourced projects (no repo to fork):
// builds a zip of the stored files on the fly, stripping common secret-file
// patterns as a safety net in case the original uploader included one.
app.post('/api/remix/projects/:id/remix', requireAuth, async (req, res) => {
  try {
    const found = await findPublishedProject(req.params.id);
    if (!found) return res.status(404).json({ error: 'Not found or not published' });
    const p = found.project;

    found.ws.projects[found.idx].remixCount = (p.remixCount || 0) + 1;
    await writeWorkspaceByKnownKey(found.key, found.ws);

    if (p.repoUrl && p.repoUrl.startsWith('http')) {
      const token = req.user?.githubAccessToken;
      if (!token) {
        return res.status(400).json({ error: 'Connect your GitHub account first to remix this project (Settings -> Connect GitHub).' });
      }
      const m = p.repoUrl.match(/github\.com\/([^/]+)\/([^/.]+)/i);
      if (!m) return res.status(400).json({ error: 'Could not parse repository from this project.' });
      const [, owner, repo] = m;
      const fr = await fetch(`https://api.github.com/repos/${owner}/${repo}/forks`, {
        method: 'POST',
        headers: { 'Authorization': `token ${token}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'joytree-remix' }
      });
      const fd = await fr.json().catch(() => ({}));
      if (!fr.ok) return res.status(400).json({ error: fd?.message || 'GitHub fork failed' });
      return res.json({ ok: true, type: 'fork', repoUrl: fd.html_url, cloneUrl: fd.clone_url });
    }

    // Upload-sourced: zip the stored files, excluding common secret patterns.
    const cleanSub = p.subdomain;
    const uploadFilesDir = p.uploadFilesDir || null;
    if (!uploadFilesDir || !fs.existsSync(uploadFilesDir)) {
      return res.status(404).json({ error: 'Original project files are no longer available to remix.' });
    }
    const SECRET_PATTERNS = [/^\.env(\..+)?$/i, /\.pem$/i, /\.key$/i, /credentials\.json$/i, /service-account.*\.json$/i];
    const remixZipDir = path.join(TMP_DIR, 'remix-zips');
    fs.mkdirSync(remixZipDir, { recursive: true });
    const zipName = `remix-${cleanSub}-${Date.now()}.zip`;
    const zipPath = path.join(remixZipDir, zipName);

    await new Promise((resolve, reject) => {
      const output = fs.createWriteStream(zipPath);
      const archive = archiver('zip', { zlib: { level: 9 } });
      output.on('close', resolve);
      archive.on('error', reject);
      archive.pipe(output);
      const walk = (dir, rel = '') => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
          if (entry.name === 'node_modules' || entry.name === '.git') continue;
          if (SECRET_PATTERNS.some(rx => rx.test(entry.name))) continue;
          const full = path.join(dir, entry.name);
          const relPath = path.join(rel, entry.name);
          if (entry.isDirectory()) walk(full, relPath);
          else archive.file(full, { name: relPath });
        }
      };
      walk(uploadFilesDir);
      archive.finalize();
    });

    res.json({ ok: true, type: 'zip', downloadUrl: `/api/remix/download/${zipName}` });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/remix/download/:zipName', (req, res) => {
  const zipName = String(req.params.zipName || '');
  if (!/^remix-[a-z0-9-]+-\d+\.zip$/i.test(zipName)) return res.status(400).json({ error: 'Invalid file name' });
  const zipPath = path.join(TMP_DIR, 'remix-zips', zipName);
  if (!fs.existsSync(zipPath)) return res.status(404).json({ error: 'This download link has expired.' });
  res.download(zipPath, zipName, (err) => {
    // Best-effort cleanup a few minutes after download, whether or not it succeeded.
    setTimeout(() => { try { fs.unlinkSync(zipPath); } catch (_) {} }, 5 * 60 * 1000);
  });
});

app.delete('/api/projects/:id', requireAuth, async (req, res) => {
  try {
    const reqId = req.params.id;
    const userId = String(req.user?._id || req.user?.id || '');

    // [FIX] This route never had requireAuth attached at all -- req.user was
    // always undefined, so every call (API, MCP, anything not going through
    // the dashboard's own session) unconditionally hit the 401 branch below.
    // Added requireAuth (same middleware every other authenticated route
    // uses; accepts session cookie, jtk_ API key, or internal deploy key).
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required to resolve which workspace to delete from.' });
    }

    // [FIX] This used to look the project up via Mongoose (Project.findById /
    // findOneAndDelete), which only ever works if MongoDB is both running
    // and actually the source of truth -- it isn't. This account (and
    // apparently this whole platform) runs on Firebase as its real
    // database; Mongo being unreachable is normal, not a fault condition.
    // That mismatch is *why* deletes kept silently failing all night: the
    // lookup was always hitting a store that doesn't hold the real data.
    // Firebase is now the primary and only required lookup; Mongo cleanup
    // below is best-effort only, for any legacy records that might still
    // exist from before the platform moved to Firebase.
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const p = (ws.projects || []).find(pr => pr.id === reqId || pr.subdomain === reqId || pr.name === reqId);

    if (!p) {
      console.warn(`[Delete] No project found in Firebase workspace matching id "${reqId}" -- nothing to clean up`);
      return res.status(404).json({ error: 'Project not found' });
    }

    console.log(`[Delete] Removing project "${p.name}" (subdomain: ${p.subdomain})`);

    // Remove from the Firebase workspace -- this is the real deletion.
    ws.projects = (ws.projects || []).filter(pr => pr.id !== p.id);
    if (Array.isArray(ws.deployments)) {
      ws.deployments = ws.deployments.filter(d => d.projectId !== p.id);
    }
    const wroteOk = await writeWorkspaceToFirebase(req.user, ws);
    if (!wroteOk) {
      console.error(`[Delete] Failed to write updated workspace to Firebase for ${p.subdomain} -- aborting before touching files/containers, since the project would still show as existing`);
      return res.status(502).json({ error: 'Failed to update Firebase workspace; nothing was deleted.' });
    }
    // Note: writeWorkspaceToFirebase() now keeps localAuth.users[].workspace
    // in sync automatically on every successful write -- see its definition
    // for why that matters here specifically.

    // Best-effort legacy Mongo cleanup, in case this project also has an
    // old Mongo-side record from before the Firebase migration. Not
    // required to succeed -- Firebase above is what actually matters now.
    if (isDbReady()) {
      await Project.deleteOne({ $or: [{ subdomain: p.subdomain }, { name: p.name }, { id: p.id }] }).catch(e =>
        console.warn(`[Delete] Legacy Mongo project cleanup skipped/failed for ${p.subdomain} (expected if Mongo isn't the real store):`, e.message));
      await Deployment.deleteMany({ projectId: p.id }).catch(e =>
        console.warn(`[Delete] Legacy Mongo deployment cleanup skipped/failed for ${p.subdomain}:`, e.message));
    }

    // Remove site files -- logs if this fails instead of silently leaving
    // orphaned files behind with no trace of why.
    try {
      fs.rmSync(path.join(SITES_DIR, p.subdomain), { recursive: true, force: true });
      console.log(`[Delete] Removed site files for ${p.subdomain}`);
    } catch(e) {
      console.error(`[Delete] Failed to remove site files for ${p.subdomain}:`, e.message);
    }

    // Stop and remove user app Docker container
    try {
      await execP(`docker rm -f db-${p.subdomain}`);
      console.log(`[Docker] Removed container db-${p.subdomain}`);
    } catch(e) {
      console.error(`[Delete] Failed to remove container db-${p.subdomain}:`, e.message);
    }

    delete portRegistry[p.subdomain];
    savePortRegistry();

    await removeSubdomain(p.subdomain).catch(e =>
      console.error(`[Delete] Failed to remove DNS/tunnel route for ${p.subdomain}:`, e.message));

    res.json({ ok: true, removedProjectId: p.id, subdomain: p.subdomain });
  } catch(e) {
    console.error('[Delete] Unexpected error deleting project:', e.message);
    res.status(500).json({ error: e.message });
  }
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

// Unified domain list — Firebase first, MongoDB fallback, memDomains last resort
async function getAllCustomDomains() {
  const map = new Map(); // domain -> entry (dedup)
  // 1. Firebase
  if (FIREBASE_RTDB_URL) {
    try {
      const authQuery = FIREBASE_RTDB_SECRET ? '?auth=' + encodeURIComponent(FIREBASE_RTDB_SECRET) : '';
      const r = await fetch(FIREBASE_RTDB_URL + '/deployboard_custom_domains.json' + authQuery);
      if (r.ok) {
        const data = await r.json().catch(() => ({}));
        if (data && typeof data === 'object') {
          for (const entry of Object.values(data)) {
            if (entry && entry.domain) map.set(String(entry.domain).toLowerCase(), entry);
          }
        }
      }
    } catch(_) {}
  }
  // 2. MongoDB
  if (isMongoReady()) {
    try {
      const docs = await CustomDomain.find().sort({ createdAt: -1 }).lean().maxTimeMS(5000);
      for (const d of docs) if (d.domain && !map.has(String(d.domain).toLowerCase())) map.set(String(d.domain).toLowerCase(), d);
    } catch(_) {}
  }
  // 3. In-memory fallback
  for (const d of memDomains) if (d.domain && !map.has(String(d.domain).toLowerCase())) map.set(String(d.domain).toLowerCase(), d);
  return Array.from(map.values());
}

app.get('/api/domains', async (req, res) => {
  try { res.json(await getAllCustomDomains()); }
  catch(e) { res.json(memDomains); }
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
        if (exists.subdomain !== subdomain) {
          // Reassign: overwrite old entry instead of blocking
          exists.subdomain = subdomain;
          exists.verified = false;
          _cdCache.delete(clean);
          addActivity('domain', 'Custom domain reassigned (mem): ' + clean + ' → ' + subdomain);
          return res.json({ ok: true, domain: exists, reassigned: true, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
        }
        return res.json({ ok: true, domain: exists, existing: true, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
      }
      const entry = { domain: clean, subdomain, verified: false, createdAt: new Date() };
      memDomains.push(entry);
      addActivity('domain', 'Custom domain added (mem): ' + clean + ' → ' + subdomain);
      return res.json({ ok: true, domain: entry, warning: 'MongoDB unavailable — domain saved in memory only and will reset on restart' });
    }
    const existing = await CustomDomain.findOne({ domain: clean }).lean().maxTimeMS(5000);
    if (existing) {
      if (existing.subdomain !== subdomain) {
        // Reassign: update to new project instead of blocking with 409
        await CustomDomain.findOneAndUpdate({ domain: clean }, { subdomain, verified: false }, { new: true }).maxTimeMS(5000);
        _cdCache.delete(clean);
        const mi = memDomains.findIndex(d => d.domain === clean);
        if (mi >= 0) { memDomains[mi].subdomain = subdomain; memDomains[mi].verified = false; }
        addActivity('domain', 'Custom domain reassigned: ' + clean + ' → ' + subdomain);
        return res.json({ ok: true, reassigned: true });
      }
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

app.delete('/api/domains/:domain', requireAuth, async (req, res) => {
  try {
    const domain = String(req.params.domain || '').toLowerCase();
    // Remove from Firebase
    if (FIREBASE_RTDB_URL) {
      try {
        const authQuery = FIREBASE_RTDB_SECRET ? '?auth=' + encodeURIComponent(FIREBASE_RTDB_SECRET) : '';
        const domainKey = domain.replace(/[^a-z0-9_-]/g, '_');
        await fetch(FIREBASE_RTDB_URL + '/deployboard_custom_domains/' + domainKey + '.json' + authQuery, { method: 'DELETE' });
      } catch(_) {}
    }
    // Remove from MongoDB
    if (isMongoReady()) await CustomDomain.findOneAndDelete({ domain }).catch(() => {});
    // Remove from memory cache and memDomains
    _cdCache.delete(domain);
    const mi = memDomains.findIndex(d => d.domain === domain);
    if (mi >= 0) memDomains.splice(mi, 1);
    console.log('[CustomDomain] Removed: ' + domain);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Real domain transfer with SSE progress logs ───────────────────
app.get('/api/domains/transfer', async (req, res) => {
  const domain    = normalizeHostHeader(String(req.query.domain || '').trim());
  const subdomain = String(req.query.subdomain || '').trim();

  if (!domain)    { res.status(400).json({ error: 'domain required' }); return; }
  if (!subdomain) { res.status(400).json({ error: 'subdomain required' }); return; }

  // SSE headers
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const startTime = Date.now();

  function send(type, message, extra = {}) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const payload = JSON.stringify({ type, message, elapsed: parseFloat(elapsed), ...extra });
    res.write(`data: ${payload}\n\n`);
    if (res.flush) res.flush();
  }

  function done(ok, message, extra = {}) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const payload = JSON.stringify({ type: ok ? 'done' : 'error', message, elapsed: parseFloat(elapsed), ...extra });
    res.write(`data: ${payload}\n\n`);
    if (res.flush) res.flush();
    res.end();
  }

  try {
    send('log', `Starting transfer of ${domain} → project [${subdomain}]`);
    await new Promise(r => setTimeout(r, 200));

    // 1. Validate project exists — Firebase only
    send('log', 'Checking project exists…');
    let project = null;
    let projectName = subdomain;

    if (FIREBASE_RTDB_URL) {
      try {
        const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
        const fbR = await fetch(`${FIREBASE_RTDB_URL}/deployboard_workspaces.json${authQuery}`);
        if (fbR.ok) {
          const allWs = await fbR.json().catch(() => ({}));
          for (const ws of Object.values(allWs || {})) {
            const fbProjects = Array.isArray(ws && ws.projects) ? ws.projects : [];
            const fbFound = fbProjects.find(p => String(p.subdomain || '') === subdomain);
            if (fbFound) { project = fbFound; projectName = fbFound.name || fbFound.subdomain || subdomain; break; }
          }
        }
      } catch (fbErr) {
        send('warn', `⚠ Firebase lookup failed: ${fbErr.message} — proceeding anyway`);
      }
    }

    if (!project) {
      const subdomainValid = /^[a-z0-9][a-z0-9-]{0,62}$/.test(subdomain);
      if (!subdomainValid) { done(false, `Invalid subdomain "${subdomain}".`); return; }
      send('warn', `⚠ Project not found in Firebase — proceeding anyway (may still be deploying)`);
      projectName = subdomain;
    } else {
      send('step', `✓ Project "${projectName}" found`);
    }
    await new Promise(r => setTimeout(r, 150));

    // 2. Check domain isn't already in use by a different project — Firebase only
    send('log', 'Checking for existing domain conflicts…');
    let existingEntry = null;
    if (FIREBASE_RTDB_URL) {
      try {
        const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
        const cdR = await fetch(`${FIREBASE_RTDB_URL}/deployboard_custom_domains.json${authQuery}`);
        if (cdR.ok) {
          const allCd = await cdR.json().catch(() => ({}));
          if (allCd && typeof allCd === 'object') {
            const entry = Object.values(allCd).find(e => e && e.domain === domain);
            if (entry) existingEntry = entry;
          }
        }
      } catch (_) {}
    }
    // Also check in-memory fallback
    if (!existingEntry) existingEntry = memDomains.find(d => d.domain === domain) || null;

    if (existingEntry && existingEntry.subdomain !== subdomain) {
      // Domain is assigned to a different project — auto-remove the old entry
      // so the transfer (reassign) can proceed without a manual DELETE step.
      send('log', `Removing old routing: ${domain} → ${existingEntry.subdomain}…`);
      try {
        if (FIREBASE_RTDB_URL) {
          const authQuery = FIREBASE_RTDB_SECRET ? '?auth=' + encodeURIComponent(FIREBASE_RTDB_SECRET) : '';
          const domainKey = domain.replace(/[^a-z0-9_-]/g, '_');
          await fetch(FIREBASE_RTDB_URL + '/deployboard_custom_domains/' + domainKey + '.json' + authQuery, { method: 'DELETE' }).catch(() => {});
        }
        if (isMongoReady()) await CustomDomain.findOneAndDelete({ domain }).catch(() => {});
        _cdCache.delete(domain);
        const mi = memDomains.findIndex(d => d.domain === domain);
        if (mi >= 0) memDomains.splice(mi, 1);
        existingEntry = null;
        send('step', `✓ Old routing removed — reassigning to "${subdomain}"`);
      } catch (removeErr) {
        send('warn', `Could not remove old entry: ${removeErr.message} — proceeding anyway`);
        existingEntry = null;
      }
    } else if (existingEntry && existingEntry.subdomain === subdomain) {
      send('step', '⚠ Domain already registered to this project — re-verifying DNS…');
    } else {
      send('step', '✓ No conflicts — domain is available');
    }
    await new Promise(r => setTimeout(r, 200));

    // 3. DNS resolution
    send('log', 'Resolving DNS records (checking CNAME / Cloudflare proxy)…');
    let dnsResult = null;
    try {
      dnsResult = await Promise.race([
        verifyDomainDns(domain),
        new Promise((_, reject) => setTimeout(() => reject(new Error('DNS timeout')), 8000))
      ]);
    } catch (dnsErr) {
      dnsResult = { verified: false, method: 'pending', reason: dnsErr.message, records: {} };
    }

    if (dnsResult.verified) {
      send('step', `✓ DNS verified via ${dnsResult.method} — records pointing correctly`);
    } else {
      send('warn', `⚠ DNS not detected yet (${dnsResult.reason || 'pending propagation'}) — proceeding to save anyway`);
    }
    await new Promise(r => setTimeout(r, 300));

    // 4. Save/upsert the domain mapping — Firebase only
    send('log', 'Saving domain → project mapping…');
    const domainEntry = { domain, subdomain, verified: dnsResult.verified, updatedAt: new Date().toISOString() };
    let savedToFirebase = false;
    if (FIREBASE_RTDB_URL) {
      try {
        const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
        // Use domain as the key (sanitised)
        const domainKey = domain.replace(/[^a-z0-9_-]/g, '_');
        const writeUrl = `${FIREBASE_RTDB_URL}/deployboard_custom_domains/${domainKey}.json${authQuery}`;
        const wr = await fetch(writeUrl, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(domainEntry)
        });
        if (wr.ok) {
          savedToFirebase = true;
        } else {
          send('warn', `⚠ Firebase write returned ${wr.status} — falling back to memory`);
        }
      } catch (fbWriteErr) {
        send('warn', `⚠ Firebase write error: ${fbWriteErr.message} — falling back to memory`);
      }
    }
    if (!savedToFirebase) {
      // In-memory fallback
      const memIdx = memDomains.findIndex(d => d.domain === domain);
      if (memIdx >= 0) memDomains[memIdx] = { ...memDomains[memIdx], ...domainEntry };
      else memDomains.push(domainEntry);
      send('warn', '⚠ Saved in memory only (resets on restart) — configure FIREBASE_RTDB_URL to persist');
    }
    // Also upsert into MongoDB if available (keeps old verifyCustomDomain endpoint working)
    if (isMongoReady()) {
      await CustomDomain.findOneAndUpdate(
        { domain },
        { domain, subdomain, verified: dnsResult.verified },
        { upsert: true, new: true, maxTimeMS: 5000 }
      ).catch(() => {});
    }
    // Push into routing cache immediately so traffic works without waiting for next refresh
    upsertCustomDomainCache(domain, subdomain);
    send('step', `✓ Mapping saved: ${domain} → ${subdomain}`);
    await new Promise(r => setTimeout(r, 200));

    // 5. Cloudflare tunnel route (if configured)
    if (CF_API_TOKEN && CF_ACCOUNT_ID && CF_TUNNEL_ID) {
      send('log', 'Registering tunnel ingress rule with Cloudflare…');
      try {
        const cfUrl = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/cfd_tunnel/${CF_TUNNEL_ID}/configurations`;
        const cfRes = await fetch(cfUrl, {
          headers: { 'Authorization': `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' }
        });
        const cfData = await cfRes.json().catch(() => ({}));
        const currentIngress = cfData?.result?.config?.ingress || [];
        // Remove any existing rule for this domain
        const filtered = currentIngress.filter(r => r.hostname !== domain && r.hostname !== '');
        // Add the new rule before the catch-all
        const catchAll = currentIngress.find(r => !r.hostname) || { service: 'http_status:404' };
        const newIngress = [
          ...filtered.filter(r => r.hostname),
          { hostname: domain, service: `http://localhost:80`, originRequest: {} },
          { service: catchAll.service }
        ];
        const putRes = await fetch(cfUrl, {
          method: 'PUT',
          headers: { 'Authorization': `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json' },
          body: JSON.stringify({ config: { ingress: newIngress } })
        });
        if (putRes.ok) {
          send('step', `✓ Cloudflare tunnel ingress rule created for ${domain}`);
        } else {
          send('warn', '⚠ Cloudflare ingress update returned non-200 — manual setup may be needed');
        }
      } catch (cfErr) {
        send('warn', `⚠ Cloudflare API error: ${cfErr.message} — domain saved but tunnel route may need manual setup`);
      }
    } else {
      send('log', 'Cloudflare API not configured — skipping tunnel route registration');
    }
    await new Promise(r => setTimeout(r, 200));

    // 6. Activity log
    addActivity('domain', `Custom domain transferred: ${domain} → ${subdomain}`);
    console.log(`[CustomDomain/transfer] ${domain} → ${subdomain} (dns:${dnsResult.verified})`);

    // 7. Done
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    send('step', `✓ Transfer complete in ${elapsed}s`);
    await new Promise(r => setTimeout(r, 100));

    done(true,
      dnsResult.verified
        ? `${domain} is live and pointing to your project!`
        : `${domain} saved — waiting for DNS propagation. Once your CNAME record propagates, the domain will go live automatically.`,
      { verified: dnsResult.verified, domain, subdomain, elapsed: parseFloat(elapsed) }
    );
  } catch (err) {
    console.error('[CustomDomain/transfer] error:', err.message);
    done(false, 'Unexpected error: ' + err.message);
  }
});

// ══════════════════════════════════════════════════════════════════
// DOMAIN STORE — NameSilo reseller API
// ══════════════════════════════════════════════════════════════════
const NAMESILO_API_KEY = process.env.NAMESILO_API_KEY || '';
const NAMESILO_BASE    = 'https://www.namesilo.com/api';

async function namesiloCall(operation, params = {}) {
  const qs = new URLSearchParams({ version: '1', type: 'json', key: NAMESILO_API_KEY, ...params });
  const url = `${NAMESILO_BASE}/${operation}?${qs}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 15000);
  try {
    const r = await fetch(url, { signal: controller.signal });
    const json = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(`NameSilo HTTP ${r.status}`);

    // NameSilo JSON is usually { reply: {...} }, but some responses are
    // double-wrapped as { reply: { reply: {...} } }. Always unwrap to the
    // payload that contains code/available/prices so availability is not
    // mistaken for taken when the API shape changes.
    let payload = json;
    while (payload?.reply && typeof payload.reply === 'object') payload = payload.reply;
    return payload || {};
  } finally {
    clearTimeout(timer);
  }
}

function namesiloCode(data) {
  return Number(data?.code || data?.status || 0);
}

function namesiloDomainsToSet(value) {
  const out = new Set();
  const normalize = item => String(item || '').trim().toLowerCase().replace(/^https?:\/\//, '').split('/')[0];
  const add = item => {
    if (!item) return;
    if (typeof item === 'string' || typeof item === 'number') {
      const domain = normalize(item);
      if (/^[a-z0-9-]+(\.[a-z0-9-]+)+$/.test(domain)) out.add(domain);
      return;
    }
    if (Array.isArray(item)) {
      item.forEach(add);
      return;
    }
    if (typeof item === 'object') {
      add(item.domain || item.name || item._ || item.$t);
      Object.values(item).forEach(add);
    }
  };
  add(value);
  return out;
}

function namesiloAvailabilitySets(data) {
  return {
    available: namesiloDomainsToSet(data?.available?.domain || data?.available || data?.available_domains),
    unavailable: namesiloDomainsToSet(data?.unavailable?.domain || data?.unavailable || data?.unavailable_domains)
  };
}

function namesiloAvailabilityStatus(data, domain) {
  const code = namesiloCode(data);
  if (code && code !== 300) {
    throw new Error(`NameSilo error ${code}: ${data?.detail || 'Availability check failed'}`);
  }

  const wanted = String(domain || '').toLowerCase();
  const { available, unavailable } = namesiloAvailabilitySets(data);

  if (available.has(wanted)) return 'available';
  if (unavailable.has(wanted)) return 'taken';

  // If NameSilo returns a successful response but omits both lists, report an
  // indeterminate state instead of falsely telling the user the domain is taken.
  return 'unknown';
}

function namesiloNormalizeTld(raw) {
  return String(raw || '').trim().toLowerCase().replace(/^\.+/, '');
}

function namesiloExtractPriceRows(priceData) {
  const rows = [];
  const containers = [priceData?.prices, priceData?.pricing, priceData];
  for (const container of containers) {
    if (!container || typeof container !== 'object' || Array.isArray(container)) continue;
    for (const [key, value] of Object.entries(container)) {
      const tld = namesiloNormalizeTld(value?.tld || value?.extension || key);
      if (!tld || ['code', 'detail'].includes(tld)) continue;
      const register = Number.parseFloat(value?.registration ?? value?.register ?? value?.registration_price ?? value?.price);
      const renew = Number.parseFloat(value?.renewal ?? value?.renew ?? value?.renewal_price ?? value?.price ?? register);
      if (Number.isFinite(register)) rows.push({ tld: `.${tld}`, register, renew: Number.isFinite(renew) ? renew : register });
    }
    if (rows.length) break;
  }
  return rows;
}

const DOMAIN_STORE_FALLBACK_TLDS = [
  { tld: '.com', register: 9.95, renew: 11.99 },
  { tld: '.net', register: 10.95, renew: 12.99 },
  { tld: '.org', register: 10.95, renew: 12.99 },
  { tld: '.io', register: 32.95, renew: 35.99 },
  { tld: '.xyz', register: 2.99, renew: 9.99 },
  { tld: '.tech', register: 7.99, renew: 34.99 },
  { tld: '.app', register: 14.99, renew: 16.99 },
  { tld: '.dev', register: 13.99, renew: 15.99 },
  { tld: '.co', register: 25.99, renew: 27.99 },
  { tld: '.site', register: 2.99, renew: 22.99 },
];

// ── Return up to 100 live NameSilo TLD options + prices ───────────────
// ── Domain routing debug/test ───────────────────────────────
// Open in browser: https://joytree.site/api/domains/debug?domain=joytreehostingserver.dpdns.org
app.get('/api/domains/debug', async (req, res) => {
  const domain = String(req.query.domain || '').trim().toLowerCase();
  const cacheAge = _cdCacheLastRefresh ? Math.round((Date.now() - _cdCacheLastRefresh) / 1000) + 's ago' : 'never';
  const cacheHitBefore = domain ? (_cdCache.get(domain) || null) : null;

  // Force refresh so result reflects latest Firebase data
  await refreshCustomDomainCache().catch(() => {});
  const cacheHitAfter = domain ? (_cdCache.get(domain) || null) : null;
  const cacheAllAfter = {};
  for (const [k, v] of _cdCache.entries()) cacheAllAfter[k] = v;

  // Firebase raw read
  let firebaseEntry = null;
  let firebaseError = null;
  if (FIREBASE_RTDB_URL) {
    try {
      const authQuery = FIREBASE_RTDB_SECRET ? '?auth=' + encodeURIComponent(FIREBASE_RTDB_SECRET) : '';
      const r = await fetch(FIREBASE_RTDB_URL + '/deployboard_custom_domains.json' + authQuery);
      if (r.ok) {
        const all = await r.json().catch(() => ({}));
        if (all && typeof all === 'object') {
          for (const entry of Object.values(all)) {
            if (entry && entry.domain && String(entry.domain).toLowerCase() === domain) { firebaseEntry = entry; break; }
          }
        }
      } else { firebaseError = 'HTTP ' + r.status; }
    } catch(e) { firebaseError = e.message; }
  } else { firebaseError = 'FIREBASE_RTDB_URL not configured'; }

  // DNS check
  let dnsResult = null;
  if (domain) {
    try {
      dnsResult = await Promise.race([
        verifyDomainDns(domain),
        new Promise((_, rej) => setTimeout(() => rej(new Error('timeout 6s')), 6000))
      ]);
    } catch(e) { dnsResult = { verified: false, reason: e.message }; }
  }

  res.json({
    ok: true,
    tested_domain: domain || '(none — pass ?domain=yourdomain.com)',
    routing: { working: !!cacheHitAfter, routes_to_project: cacheHitAfter, was_in_cache_before: !!cacheHitBefore },
    cache: { total_entries: _cdCache.size, last_refreshed: cacheAge, all_entries: cacheAllAfter },
    firebase: { configured: !!FIREBASE_RTDB_URL, entry_found: firebaseEntry, error: firebaseError },
    dns: dnsResult,
  });
});

app.get('/api/domains/tlds', requireAuth, async (req, res) => {
  try {
    const limit = Math.min(Math.max(Number.parseInt(req.query.limit, 10) || 100, 1), 100);

    if (!NAMESILO_API_KEY) {
      return res.json({ tlds: DOMAIN_STORE_FALLBACK_TLDS.slice(0, limit), demo: true });
    }

    const priceData = await namesiloCall('getPrices', {});
    const rows = namesiloExtractPriceRows(priceData)
      .sort((a, b) => {
        const popular = ['.com', '.net', '.org', '.io', '.xyz', '.tech', '.app', '.dev', '.co', '.site'];
        const ai = popular.indexOf(a.tld);
        const bi = popular.indexOf(b.tld);
        if (ai !== -1 || bi !== -1) return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
        return a.tld.localeCompare(b.tld);
      })
      .slice(0, limit);

    res.json({ tlds: rows.length ? rows : DOMAIN_STORE_FALLBACK_TLDS.slice(0, limit), demo: !rows.length });
  } catch(e) {
    console.warn('[DomainStore] tlds failed:', e.message);
    res.status(502).json({ error: e.message, tlds: DOMAIN_STORE_FALLBACK_TLDS });
  }
});

// ── Check domain availability + return live prices ───────────────
app.get('/api/domains/check', requireAuth, async (req, res) => {
  try {
    const domainsParam = String(req.query.domains || '').trim();
    if (!domainsParam) return res.status(400).json({ error: 'domains param required' });
    const domainList = domainsParam.split(',').map(d => d.trim().toLowerCase()).filter(Boolean).slice(0, 10);

    if (!NAMESILO_API_KEY) {
      // Demo mode — return mock availability + mock prices so UI still works
      const mock = {}; const mockPrices = {};
      domainList.forEach(d => {
        mock[d] = Math.random() > 0.4 ? 'available' : 'taken';
        const tld = '.' + d.split('.').slice(1).join('.');
        const fallback = DOMAIN_STORE_FALLBACK_TLDS.find(x => x.tld === tld) || DOMAIN_STORE_FALLBACK_TLDS[0];
        mockPrices[d] = { register: fallback.register, renew: fallback.renew };
      });
      return res.json({ availability: mock, prices: mockPrices, demo: true });
    }

    // Check the whole 10-domain batch in one provider request and wait for
    // the full response before returning anything to the UI. NameSilo's
    // availability API may only list available domains, so when that payload is
    // present, domains absent from the available list are confirmed as taken.
    const [availability, priceResults] = await Promise.all([
      (async () => {
        const statuses = Object.fromEntries(domainList.map(domain => [domain, 'unknown']));
        try {
          const data = await namesiloCall('checkRegisterAvailability', { domains: domainList.join(',') });
          const { available, unavailable } = namesiloAvailabilitySets(data);
          const hasAvailabilityPayload = Boolean(data?.available || data?.available_domains || data?.unavailable || data?.unavailable_domains);
          domainList.forEach(domain => {
            if (available.has(domain)) statuses[domain] = 'available';
            else if (unavailable.has(domain)) statuses[domain] = 'taken';
            else if (hasAvailabilityPayload) statuses[domain] = 'taken';
          });
        } catch(e) {
          console.warn('[DomainStore] availability batch failed:', e.message);
        }
        return statuses;
      })(),
      // Live pricing — NameSilo getPrices returns per-TLD wholesale prices
      (async () => {
        try {
          const priceData = await namesiloCall('getPrices', {});
          const priceRows = namesiloExtractPriceRows(priceData);
          const byTld = new Map(priceRows.map(row => [row.tld.replace(/^\./, ''), row]));
          const priceMap = {};
          domainList.forEach(domain => {
            const tld = domain.split('.').slice(1).join('.');
            const row = byTld.get(tld);
            if (row) priceMap[domain] = { register: row.register, renew: row.renew };
          });
          return priceMap;
        } catch(e) {
          console.warn('[DomainStore] getPrices failed:', e.message);
          return {};
        }
      })()
    ]);

    res.json({ availability, prices: priceResults });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Register a domain (called after Paystack payment confirmed) ──
app.post('/api/domains/register', requireAuth, async (req, res) => {
  try {
    const { domain, paystackRef, projectId } = req.body || {};
    if (!domain || !paystackRef) return res.status(400).json({ error: 'domain and paystackRef required' });

    const userId = String(req.user?._id || req.user?.id || req.user?.uid || '');
    const email  = String(req.user?.email || '');

    // ── Idempotency: if this reference was already processed, return the saved result ──
    const ws0 = (await readWorkspaceFromFirebase(req.user)) || {};
    const already = (ws0.registeredDomains || []).find(d => d.paystackRef === paystackRef);
    if (already) {
      console.log(`[DomainStore] Duplicate register attempt for ref ${paystackRef} — returning cached result`);
      return res.json({ ok: true, domain: already.domain, duplicate: true });
    }

    // ── Verify Paystack payment ──────────────────────────────────────────────
    const verifyR = await fetch(`https://api.paystack.co/transaction/verify/${encodeURIComponent(paystackRef)}`, {
      headers: { Authorization: `Bearer ${process.env.PAYSTACK_SECRET_KEY}` }
    });
    const verifyData = await verifyR.json().catch(() => ({}));
    if (!verifyData?.data || verifyData.data.status !== 'success') {
      return res.status(402).json({ error: 'Payment not verified — no charge was made or it is still pending.' });
    }

    // ── Save a pending record immediately so we can recover if NameSilo call fails ──
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.registeredDomains = Array.isArray(ws.registeredDomains) ? ws.registeredDomains : [];
    const pendingRec = {
      domain, status: 'pending', projectId: projectId || null,
      registeredAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 365*24*3600*1000).toISOString(),
      paystackRef, userId, namesiloCode: null
    };
    ws.registeredDomains.unshift(pendingRec);
    await writeWorkspaceToFirebase(req.user, ws);

    if (!NAMESILO_API_KEY) {
      // Demo mode — mark active without calling NameSilo
      pendingRec.status = 'active';
      pendingRec.namesiloCode = 300;
      await writeWorkspaceToFirebase(req.user, ws);
      return res.json({ ok: true, domain, demo: true });
    }

    // ── Contact NameSilo with up to 2 retries ────────────────────────────────
    const contactRes = await namesiloCall('contactList', {});
    const contactId  = contactRes?.contact?.[0]?.contact_id || '';

    let regData, namesiloErr;
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        regData = await namesiloCall('registerDomain', {
          domain, years: 1, private: 1, auto_renew: 0, contact_id: contactId
        });
        const c = Number(regData?.code || 0);
        if (c === 300 || c === 302) { namesiloErr = null; break; }
        namesiloErr = `NameSilo error ${c}: ${regData?.detail || 'Registration failed'}`;
        if (attempt < 2) await new Promise(r => setTimeout(r, 3000)); // wait 3s before retry
      } catch(e) {
        namesiloErr = e.message;
        if (attempt < 2) await new Promise(r => setTimeout(r, 3000));
      }
    }

    if (namesiloErr) {
      // ── Payment was taken but NameSilo failed — mark as failed_namesilo so admin can recover ──
      pendingRec.status = 'failed_namesilo';
      pendingRec.failReason = namesiloErr;
      pendingRec.failedAt = new Date().toISOString();
      await writeWorkspaceToFirebase(req.user, ws);
      console.error(`[DomainStore] PAYMENT TAKEN but NameSilo failed for ${domain} ref=${paystackRef}: ${namesiloErr}`);
      // Return a specific error so frontend can show the right message
      return res.status(502).json({
        error: namesiloErr,
        paymentTaken: true,
        domain,
        paystackRef,
        message: 'Your payment was received but domain registration encountered an issue. We have recorded your payment — please contact support with your reference: ' + paystackRef
      });
    }

    // ── Success: update the pending record to active ─────────────────────────
    const code = Number(regData?.code || 0);
    pendingRec.status = 'active';
    pendingRec.namesiloCode = code;
    await writeWorkspaceToFirebase(req.user, ws);

    console.log(`[DomainStore] Registered ${domain} for ${email} (ref=${paystackRef})`);
    res.json({ ok: true, domain });
  } catch(e) {
    console.error('[DomainStore] Register error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Recovery endpoint: retry a failed_namesilo registration ──────────────
app.post('/api/domains/register/retry', requireAuth, async (req, res) => {
  try {
    const { paystackRef } = req.body || {};
    if (!paystackRef) return res.status(400).json({ error: 'paystackRef required' });

    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const rec = (ws.registeredDomains || []).find(d => d.paystackRef === paystackRef);
    if (!rec) return res.status(404).json({ error: 'No record found for this payment reference' });
    if (rec.status === 'active') return res.json({ ok: true, domain: rec.domain, alreadyActive: true });
    if (rec.status !== 'failed_namesilo' && rec.status !== 'pending') {
      return res.status(400).json({ error: `Cannot retry a domain with status: ${rec.status}` });
    }

    if (!NAMESILO_API_KEY) return res.status(503).json({ error: 'NameSilo not configured' });

    const contactRes = await namesiloCall('contactList', {});
    const contactId  = contactRes?.contact?.[0]?.contact_id || '';
    const regData    = await namesiloCall('registerDomain', {
      domain: rec.domain, years: 1, private: 1, auto_renew: 0, contact_id: contactId
    });
    const code = Number(regData?.code || 0);
    if (code !== 300 && code !== 302) {
      throw new Error(`NameSilo error ${code}: ${regData?.detail || 'Retry failed'}`);
    }
    rec.status = 'active'; rec.namesiloCode = code; rec.retriedAt = new Date().toISOString();
    delete rec.failReason; delete rec.failedAt;
    await writeWorkspaceToFirebase(req.user, ws);
    console.log(`[DomainStore] Retry succeeded for ${rec.domain} ref=${paystackRef}`);
    res.json({ ok: true, domain: rec.domain });
  } catch(e) {
    console.error('[DomainStore] Retry error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Admin: list all failed/pending domain registrations ──────────────────
// Requires ADMIN_SECRET env var to be set on the server.
// Usage: curl -H "x-admin-secret: YOUR_SECRET" https://joytree.site/api/admin/domains/failed
app.get('/api/admin/domains/failed', async (req, res) => {
  try {
    const secret = process.env.ADMIN_SECRET || '';
    if (!secret || req.headers['x-admin-secret'] !== secret) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    // Scan all workspaces in Firebase for failed/pending domain records
    if (!FIREBASE_RTDB_URL) return res.status(503).json({ error: 'Firebase not configured' });
    const authQuery = process.env.FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(process.env.FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_workspaces.json${authQuery}`);
    const allWs = await r.json().catch(() => ({}));
    const failed = [];
    for (const [key, ws] of Object.entries(allWs || {})) {
      const domains = Array.isArray(ws?.registeredDomains) ? ws.registeredDomains : [];
      domains.forEach(d => {
        if (d.status === 'failed_namesilo' || d.status === 'pending') {
          failed.push({
            workspaceKey: key,
            userEmail: ws.email || ws.userEmail || key,
            domain: d.domain,
            status: d.status,
            paystackRef: d.paystackRef,
            failReason: d.failReason || null,
            registeredAt: d.registeredAt,
            failedAt: d.failedAt || null
          });
        }
      });
    }
    res.json({ ok: true, count: failed.length, failed });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Admin: manually mark a domain as active (after registering on NameSilo manually) ──
// Usage:
//   curl -X POST https://joytree.site/api/admin/domains/recover \
//     -H "Content-Type: application/json" \
//     -H "x-admin-secret: YOUR_SECRET" \
//     -d '{"paystackRef":"ref_here"}'
app.post('/api/admin/domains/recover', async (req, res) => {
  try {
    const secret = process.env.ADMIN_SECRET || '';
    if (!secret || req.headers['x-admin-secret'] !== secret) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
    const { paystackRef } = req.body || {};
    if (!paystackRef) return res.status(400).json({ error: 'paystackRef required' });

    if (!FIREBASE_RTDB_URL) return res.status(503).json({ error: 'Firebase not configured' });
    const authQuery = process.env.FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(process.env.FIREBASE_RTDB_SECRET)}` : '';
    const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_workspaces.json${authQuery}`);
    const allWs = await r.json().catch(() => ({}));

    let found = false;
    for (const [key, ws] of Object.entries(allWs || {})) {
      const domains = Array.isArray(ws?.registeredDomains) ? ws.registeredDomains : [];
      const rec = domains.find(d => d.paystackRef === paystackRef);
      if (rec) {
        rec.status = 'active';
        rec.recoveredAt = new Date().toISOString();
        rec.recoveredBy = 'admin';
        delete rec.failReason;
        delete rec.failedAt;
        // Write back to Firebase
        const writeUrl = `${FIREBASE_RTDB_URL}/deployboard_workspaces/${encodeURIComponent(key)}.json${authQuery}`;
        const wr = await fetch(writeUrl, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(ws)
        });
        if (!wr.ok) throw new Error('Firebase write failed: ' + wr.status);
        console.log(`[Admin] Recovered domain ${rec.domain} for workspace ${key} ref=${paystackRef}`);
        found = true;
        return res.json({ ok: true, domain: rec.domain, workspaceKey: key });
      }
    }
    if (!found) return res.status(404).json({ error: 'No domain found with that Paystack reference' });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// ── List user's registered domains ───────────────────────────────
app.get('/api/domains/mine', requireAuth, async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const domains = Array.isArray(ws.registeredDomains) ? ws.registeredDomains : [];
    // Attach project names
    const projectsMap = {};
    (Array.isArray(ws.projects) ? ws.projects : []).forEach(p => { projectsMap[p.id || p._id] = p.name; });
    const enriched = domains.map(d => ({
      ...d,
      projectName: d.projectId ? (projectsMap[d.projectId] || d.projectId) : null
    }));
    res.json({ ok: true, domains: enriched });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Attach domain to a project (set DNS CNAME → project subdomain) ─
app.post('/api/domains/attach', requireAuth, async (req, res) => {
  try {
    const { domain, projectId } = req.body || {};
    if (!domain || !projectId) return res.status(400).json({ error: 'domain and projectId required' });

    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => String(p.id || p._id) === String(projectId));
    if (!project) return res.status(404).json({ error: 'Project not found' });
    const subdomain = String(project.subdomain || '');
    const targetHost = subdomain ? `${subdomain}.${BASE_DOMAIN}` : null;
    if (!targetHost) return res.status(400).json({ error: 'Project has no subdomain' });

    // Update in Firebase
    ws.registeredDomains = Array.isArray(ws.registeredDomains) ? ws.registeredDomains : [];
    const rec = ws.registeredDomains.find(d => d.domain === domain);
    if (rec) { rec.projectId = projectId; rec.attachedAt = new Date().toISOString(); }
    await writeWorkspaceToFirebase(req.user, ws);

    // Add CNAME via NameSilo if API key available
    if (NAMESILO_API_KEY) {
      await namesiloCall('dnsAddRecord', {
        domain, rrtype: 'CNAME', rrhost: '@', rrvalue: targetHost, rrttl: 3600
      }).catch(e => console.warn('[DomainStore] CNAME add warn:', e.message));
    }

    res.json({ ok: true, domain, projectId, targetHost });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Get DNS records for a domain ─────────────────────────────────
app.get('/api/domains/dns', requireAuth, async (req, res) => {
  try {
    const domain = String(req.query.domain || '').trim();
    if (!domain) return res.status(400).json({ error: 'domain required' });

    if (!NAMESILO_API_KEY) {
      // Demo records
      return res.json({ ok: true, records: [
        { id: '1', type: 'A',     host: '@',   value: '76.76.21.21',           ttl: 3600 },
        { id: '2', type: 'CNAME', host: 'www', value: domain,                   ttl: 3600 },
        { id: '3', type: 'MX',   host: '@',   value: 'mail.namesilo.com',       ttl: 3600 },
        { id: '4', type: 'TXT',  host: '@',   value: 'v=spf1 include:namesilo.com ~all', ttl: 3600 },
      ]});
    }

    const data = await namesiloCall('dnsListRecords', { domain });
    const raw  = data?.resource_record || [];
    const records = (Array.isArray(raw) ? raw : [raw]).map(r => ({
      id:    String(r.record_id || r.id || ''),
      type:  String(r.type || r.rrtype || ''),
      host:  String(r.host || r.rrhost || ''),
      value: String(r.value || r.rrvalue || ''),
      ttl:   Number(r.ttl || r.rrttl || 3600)
    }));
    res.json({ ok: true, records });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Add DNS record ────────────────────────────────────────────────
app.post('/api/domains/dns/add', requireAuth, async (req, res) => {
  try {
    const { domain, type, host, value, ttl = 3600 } = req.body || {};
    if (!domain || !type || !host || !value) return res.status(400).json({ error: 'domain, type, host, value required' });

    if (!NAMESILO_API_KEY) return res.json({ ok: true, demo: true });

    const data = await namesiloCall('dnsAddRecord', {
      domain, rrtype: type, rrhost: host, rrvalue: value, rrttl: ttl
    });
    const code = Number(data?.code || 0);
    if (code !== 300) throw new Error(`NameSilo error ${code}: ${data?.detail || 'Add failed'}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Delete DNS record ─────────────────────────────────────────────
app.post('/api/domains/dns/delete', requireAuth, async (req, res) => {
  try {
    const { domain, recordId } = req.body || {};
    if (!domain || !recordId) return res.status(400).json({ error: 'domain and recordId required' });

    if (!NAMESILO_API_KEY) return res.json({ ok: true, demo: true });

    const data = await namesiloCall('dnsDeleteRecord', { domain, rrid: recordId });
    const code = Number(data?.code || 0);
    if (code !== 300) throw new Error(`NameSilo error ${code}: ${data?.detail || 'Delete failed'}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Update nameservers ────────────────────────────────────────────
// GET /api/domains/proxy-imports — list all external proxy mappings
app.get('/api/domains/proxy-imports', async (req, res) => {
  try {
    const list = Array.from(_epCache.entries()).map(([subdomain, externalUrl]) => ({ subdomain, externalUrl }));
    res.json({ ok: true, proxies: list });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/domains/proxy-import — save a new external proxy mapping
app.post('/api/domains/proxy-import', async (req, res) => {
  try {
    let { subdomain, externalUrl } = req.body || {};
    if (!subdomain || !externalUrl) return res.status(400).json({ error: 'subdomain and externalUrl are required' });
    subdomain = String(subdomain).trim().toLowerCase();
    if (!/^https?:\/\//.test(externalUrl)) externalUrl = 'https://' + externalUrl;
    try { new URL(externalUrl); } catch(_) { return res.status(400).json({ error: 'Invalid external URL' }); }
    try {
      const ctrl = new AbortController();
      const tmo = setTimeout(() => ctrl.abort(), 8000);
      const chk = await fetch(externalUrl, { method: 'HEAD', signal: ctrl.signal, redirect: 'follow' }).catch(() =>
        fetch(externalUrl, { method: 'GET', signal: ctrl.signal, redirect: 'follow' })
      );
      clearTimeout(tmo);
      if (!chk.ok && chk.status >= 500) return res.status(400).json({ error: `External URL returned ${chk.status} — check the address` });
    } catch(e) {
      return res.status(400).json({ error: 'Could not reach external URL: ' + e.message });
    }
    const entry = { subdomain, externalUrl, createdAt: new Date().toISOString() };
    if (FIREBASE_RTDB_URL) {
      const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
      const key = subdomain.replace(/[.#$[\]]/g, '_');
      await fetch(`${FIREBASE_RTDB_URL}/deployboard_external_proxies/${key}.json${authQuery}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(entry)
      });
    }
    _epCache.set(subdomain, externalUrl);
    if (typeof addActivity === 'function') addActivity('domain', `External proxy set: ${subdomain} → ${externalUrl}`);
    res.json({ ok: true, entry });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// DELETE /api/domains/proxy-import/:subdomain — remove an external proxy
app.delete('/api/domains/proxy-import/:subdomain', requireAuth, async (req, res) => {
  try {
    const subdomain = String(req.params.subdomain).trim().toLowerCase();
    _epCache.delete(subdomain);
    if (FIREBASE_RTDB_URL) {
      const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
      const key = subdomain.replace(/[.#$[\]]/g, '_');
      await fetch(`${FIREBASE_RTDB_URL}/deployboard_external_proxies/${key}.json${authQuery}`, { method: 'DELETE' });
    }
    if (typeof addActivity === 'function') addActivity('domain', `External proxy removed: ${subdomain}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/domains/nameservers', requireAuth, async (req, res) => {
  try {
    const { domain, nameservers } = req.body || {};
    if (!domain || !Array.isArray(nameservers) || !nameservers.length)
      return res.status(400).json({ error: 'domain and nameservers[] required' });

    if (!NAMESILO_API_KEY) return res.json({ ok: true, demo: true });

    const params = { domain };
    nameservers.forEach((ns, i) => { params[`ns${i+1}`] = ns; });
    const data = await namesiloCall('changeNameServers', params);
    const code = Number(data?.code || 0);
    if (code !== 300) throw new Error(`NameSilo error ${code}: ${data?.detail || 'NS update failed'}`);
    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Paystack webhook: handle domain registration payment ──────────
// (hook into existing webhook — domain type is detected by metadata)
// This is handled inside the existing /api/paystack/webhook route
// via metadata.type === 'domain_registration'

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

    // Start with project.envVars (MongoDB or Firebase project record)
    const vars = p.envVars instanceof Map
      ? Object.fromEntries(p.envVars)
      : (p.envVars && typeof p.envVars === 'object' ? { ...p.envVars } : {});

    // ALSO merge from ws.envStore — this is where vars set during
    // deployment form are stored. They are used at deploy time but
    // were previously invisible on the project detail page.
    try {
      if (req.user) {
        const liveWs = await readWorkspaceFromFirebase(req.user).catch(() => null);
        if (liveWs && liveWs.envStore && typeof liveWs.envStore === 'object') {
          const projectId = String(req.params.id || '');
          const subdomain = String(p.subdomain || '');
          const storeVars = liveWs.envStore[projectId] || liveWs.envStore[subdomain]
            || liveWs.envStore[String(p.id || p._id || '')] || {};
          if (typeof storeVars === 'object') Object.assign(vars, storeVars);
        }
      }
    } catch (_) {}

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

    // ALSO write to ws.envStore so the var is picked up by the deployment
    // pipeline which reads envStore as its primary source
    try {
      const liveWs = await readWorkspaceFromFirebase(req.user).catch(() => null);
      if (liveWs) {
        liveWs.envStore = liveWs.envStore || {};
        const pid = String(p.id || p._id || req.params.id);
        liveWs.envStore[pid] = liveWs.envStore[pid] || {};
        liveWs.envStore[pid][key] = value || '';
        // Also key by subdomain
        if (p.subdomain) {
          liveWs.envStore[p.subdomain] = liveWs.envStore[p.subdomain] || {};
          liveWs.envStore[p.subdomain][key] = value || '';
        }
        await writeWorkspaceToFirebase(req.user, liveWs).catch(() => {});
      }
    } catch (_) {}

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

    // Also remove from ws.envStore
    try {
      const liveWs = await readWorkspaceFromFirebase(req.user).catch(() => null);
      if (liveWs && liveWs.envStore) {
        const pid = String(p.id || p._id || req.params.id);
        if (liveWs.envStore[pid]) { delete liveWs.envStore[pid][req.params.key]; }
        if (p.subdomain && liveWs.envStore[p.subdomain]) { delete liveWs.envStore[p.subdomain][req.params.key]; }
        await writeWorkspaceToFirebase(req.user, liveWs).catch(() => {});
      }
    } catch (_) {}

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
// VPS_IP can be set explicitly; if not, we fall back to the BASE_DOMAIN hostname which resolves to the VPS.
const VPS_PUBLIC_HOST = (process.env.VPS_IP || process.env.PUBLIC_IP || '').trim();

// [FIX] Auto-detect the VPS public IP at startup if VPS_IP / PUBLIC_IP is not
// set in .env. This is needed so DB subdomains get a correct A record in
// Cloudflare (proxied:false) pointing straight at this machine. Without it,
// the wildcard *.joytree.site (orange-cloud) intercepts all DB traffic and
// Cloudflare drops raw TCP connections (MongoDB, MySQL, Redis, etc.) before
// they ever reach the VPS. We try three well-known metadata endpoints in
// parallel and take the first that returns a valid IPv4 address.
let _resolvedVpsIp = VPS_PUBLIC_HOST; // use .env value immediately if present
if (!_resolvedVpsIp) {
  (async () => {
    const sources = [
      'https://api.ipify.org',
      'https://checkip.amazonaws.com',
      'https://icanhazip.com',
    ];
    for (const url of sources) {
      try {
        const ip = (await fetch(url, { signal: AbortSignal.timeout(4000) }).then(r => r.text())).trim();
        if (/^\d{1,3}(\.\d{1,3}){3}$/.test(ip)) {
          _resolvedVpsIp = ip;
          console.log(`[VPS IP] Auto-detected public IP: ${ip} (set VPS_IP=${ip} in .env to skip this)`);
          // Immediately repair any DB DNS records that are still orange-cloud
          autoRepairDbDnsRecords().catch(() => {});
          break;
        }
      } catch (_) {}
    }
    if (!_resolvedVpsIp) console.warn('[VPS IP] Could not auto-detect public IP — set VPS_IP in .env');
  })();
}
function getVpsIp() { return _resolvedVpsIp || VPS_PUBLIC_HOST || ''; }

function publicDbHost(db){
  // Prefer a dedicated db-xxx subdomain if DB_PUBLIC_BASE_DOMAIN is configured
  const base = String(DB_PUBLIC_BASE_DOMAIN || '').trim();
  if (base) {
    const id = String(db.id || db._id || '').slice(-12);
    return `db-${id}.${base}`;
  }
  // Fall back to bare VPS IP or the main BASE_DOMAIN — users connect via host:port
  return VPS_PUBLIC_HOST || BASE_DOMAIN || '';
}
function externalDbConnStr(db){
  // Use VPS IP directly in connection strings so user app containers (which
  // cannot resolve db-xxx.joytree.site from inside Docker) can connect.
  // The pretty subdomain hostname is still shown in the dashboard via publicDbHost().
  const displayHost = publicDbHost(db);
  if (!displayHost) return '';
  const host = getVpsIp() || displayHost;
  const user = encodeURIComponent(String(db.user || (db.engine==='postgres'?'postgres':'root')));
  const pass = encodeURIComponent(String(db.pass || ''));
  const name = encodeURIComponent(String(db.dbName || 'mydb'));
  const port = db.internalPort || db.hostPort;
  switch (db.engine){
    case 'postgres': return `postgresql://${user}:${pass}@${host}:${port || 5432}/${name}`;
    case 'mysql':    return `mysql://${user}:${pass}@${host}:${port || 3306}/${name}`;
    case 'mariadb':  return `mariadb://${user}:${pass}@${host}:${port || 3306}/${name}`;
    case 'mongodb':  return `mongodb://${user}:${pass}@${host}:${port || 27017}/${name}?authSource=admin`;
    case 'redis':    return pass ? `redis://:${pass}@${host}:${port || 6379}` : `redis://${host}:${port || 6379}`;
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

// DeepSeek is OpenAI-compatible, but v3 sends the high-reasoning options
// requested by the platform owner. Do not hardcode keys here; use DEEPSEEK_API_KEY.
function getDeepSeekAttemptProfiles() {
  const profiles = [
    { model: DEEPSEEK_MODEL, thinkingEnabled: true, label: 'primary-thinking' },
    { model: DEEPSEEK_MODEL, thinkingEnabled: false, label: 'primary-json-safe' }
  ];
  if (DEEPSEEK_MODEL_FALLBACK && DEEPSEEK_MODEL_FALLBACK !== DEEPSEEK_MODEL) {
    profiles.push(
      { model: DEEPSEEK_MODEL_FALLBACK, thinkingEnabled: true, label: 'fallback-thinking' },
      { model: DEEPSEEK_MODEL_FALLBACK, thinkingEnabled: false, label: 'fallback-json-safe' }
    );
  }
  return profiles;
}

async function callDeepSeekChat({ messages, maxTokens = DEEPSEEK_FLOW_MAX_TOKENS, timeoutMs = DEEPSEEK_FLOW_TIMEOUT_MS, temperature = 0.3, model = DEEPSEEK_MODEL, thinkingEnabled = true, reasoningEffort = 'high' } = {}) {
  if (!DEEPSEEK_API_KEY) throw new Error('DEEPSEEK_API_KEY is not configured.');
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const body = {
      model: model || DEEPSEEK_MODEL,
      messages: messages,
      max_tokens: maxTokens,
      thinking: { type: thinkingEnabled ? 'enabled' : 'disabled' },
      stream: false
    };
    if (thinkingEnabled) {
      body.reasoning_effort = reasoningEffort || 'high';
      // Official DeepSeek docs say thinking mode ignores temperature/top_p.
      // Omitting them keeps v3 closer to DeepSeek's expected thinking-mode shape.
    } else {
      body.temperature = Math.min(2, Math.max(0, temperature));
    }
    const r = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + DEEPSEEK_API_KEY
      },
      body: JSON.stringify(body)
    });
    if (!r.ok) {
      const errText = await r.text().catch(function(){ return ''; });
      const errJson = (function(){ try { return JSON.parse(errText); } catch(x){ return null; } })();
      const msg = (errJson && (errJson.error && errJson.error.message || errJson.message)) || errText.slice(0, 300) || ('HTTP ' + r.status);
      console.error('[DeepSeek] API error', r.status, '(' + body.model + ', thinking=' + body.thinking.type + '):', msg);
      throw new Error('DeepSeek HTTP ' + r.status + ': ' + msg);
    }
    const data = await r.json().catch(function(){ return {}; });
    const msg = data && data.choices && data.choices[0] && data.choices[0].message;
    const content = String((msg && msg.content) || data.content || '');
    if (!content) throw new Error('DeepSeek returned empty content. Check model name and API key.');
    return content;
  } catch (e) {
    if (e.name === 'AbortError') throw new Error('DeepSeek timed out after ' + Math.round(timeoutMs / 1000) + 's.');
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
async function generateQuestionChunk({ provider = 'groq', topic = '', count = 25, offset = 0, existing = [], maxTokens = 2000 } = {}) {
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

  if (provider === 'deepseek') {
    let lastErr = null;
    const messages = [
      { role: 'system', content: AI_CHUNK_INSTRUCTION },
      { role: 'user', content: userMsg }
    ];
    for (const profile of getDeepSeekAttemptProfiles()) {
      try {
        const text = await callDeepSeekChat({
          messages: messages,
          temperature: 0.7,
          maxTokens: Math.max(maxTokens, DEEPSEEK_CHUNK_MAX_TOKENS),
          timeoutMs: DEEPSEEK_CHUNK_TIMEOUT_MS,
          model: profile.model,
          thinkingEnabled: profile.thinkingEnabled
        });
        const arr = extractChunkArray(text);
        if (arr && arr.length > 0) return arr;
        lastErr = new Error('DeepSeek returned unparseable response. Raw: ' + (text || '').slice(0, 120));
        console.warn('[DeepSeek chunk] unparseable response from ' + profile.label + ':', (text || '').slice(0, 300));
      } catch(e) {
        lastErr = e;
        console.warn('[DeepSeek chunk] ' + profile.label + ' failed:', e.message);
      }
    }
    throw lastErr || new Error('DeepSeek chunk generation failed.');
  }

  if (provider === 'xai' && XAI_API_KEY) {
    return callAI('https://api.x.ai/v1/chat/completions', XAI_API_KEY, XAI_MODEL, maxTokens, 120000);
  }
  if (GROQ_API_KEY) {
    return callAI('https://api.groq.com/openai/v1/chat/completions', GROQ_API_KEY, GROQ_MODEL, maxTokens, 90000);
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
  const sanitiseLabel = (s) => (s || '').replace(/\b(groq|xai|deepseek|grok|llama|openai|gsk-[\S]+|xai-[\S]+|csk-[\S]+|sk-[\S]+)\b/gi, 'AI');

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


  emitProgress(0, 0, 'Starting — generating ' + total + ' items in ' + totalChunks + ' batches...');

  const getProvidersForChunk = (chunkIndex) => {
    // v4 uses the SAME working chunk providers as v1/v2, but alternates the
    // preferred provider each batch: Groq -> xAI -> Groq -> xAI, with the other
    // provider as fallback so the next AI can continue from the prior chunks.
    if (aiVersion === 'v4') {
      const v4Providers = getV4ProviderOrder(chunkIndex);
      return v4Providers.length ? v4Providers : ['groq'];
    }
    if (aiVersion === 'v3') {
      const v3Providers = ['deepseek'];
      // DeepSeek stays primary for v3, but Groq/xAI are safety fallbacks so a
      // DeepSeek outage/bad JSON response does not turn into a deploy failure.
      if (GROQ_API_KEY) v3Providers.push('groq');
      if (XAI_API_KEY) v3Providers.push('xai');
      return v3Providers;
    }
    return aiVersion === 'v2' && XAI_API_KEY ? ['groq', 'xai'] : ['groq'];
  };

  const shouldWaitBeforeRetry = () => !(aiVersion === 'v4' && getConfiguredV4Providers().length > 1);

  const shouldWaitAfterChunk = (chunkIndex, hasMoreWork) => {
    if (!hasMoreWork) return false;
    const v4ProviderCount = aiVersion === 'v4' ? getConfiguredV4Providers().length : 0;
    // v4 should NOT wait between the first Groq chunk and the second xAI chunk,
    // because the second provider is still fresh. Wait only after each provider
    // in the pair has taken a turn (before chunk 3, 5, 7, ...).
    if (v4ProviderCount > 1) return (chunkIndex + 1) % v4ProviderCount === 0;
    return true;
  };

  let chunk = 0;
  const maxChunkRuns = Math.max(totalChunks + 8, Math.ceil(total / Math.max(1, Math.floor(chunkSize / 2))) + 8);
  while (allQuestions.length < total && chunk < maxChunkRuns) {
    const offset = allQuestions.length;
    const count = Math.min(chunkSize, total - allQuestions.length);
    const displayTotalChunks = Math.max(totalChunks, Math.ceil(total / chunkSize), chunk + 1);
    const providers = getProvidersForChunk(chunk);

    let chunkItems = null;
    let lastErr = null;
    let chunkSucceeded = false;

    // Try this chunk up to CHUNK_MAX_RETRIES times with waits between attempts
    for (let attempt = 0; attempt < CHUNK_MAX_RETRIES; attempt++) {
      if (attempt > 0) {
        console.warn('[Chunker] chunk ' + (chunk + 1) + '/' + displayTotalChunks + ' attempt ' + attempt + ' failed (' + (lastErr && lastErr.message) + '). ' + (shouldWaitBeforeRetry() ? 'Waiting before retry...' : 'Retrying immediately for v4 dual-AI mode...'));
        emitHint((lastErr && lastErr.message) || 'Batch failed, retrying...');
        if (shouldWaitBeforeRetry()) {
          await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'retrying batch ' + (chunk + 1) + ' (attempt ' + (attempt + 1) + '/' + CHUNK_MAX_RETRIES + ')');
        } else {
          emitProgress(allQuestions.length, chunk + 1, 'Retrying immediately with the alternate AI provider — ' + allQuestions.length + '/' + total + ' done...');
        }
      }

      for (let pi = 0; pi < providers.length; pi++) {
        const provider = providers[pi];
        try {
          emitProgress(allQuestions.length, chunk + 1, 'Generating batch ' + (chunk + 1) + '/' + displayTotalChunks + ' — ' + allQuestions.length + '/' + total + ' done...');
          chunkItems = await generateQuestionChunk({
            provider: provider,
            topic: topic,
            count: count,
            offset: offset,
            existing: allQuestions,
            maxTokens: aiVersion === 'v4' ? 4096 : 2000
          });
          if (Array.isArray(chunkItems) && chunkItems.length > 0) {
            chunkSucceeded = true;
            break;
          }
        } catch (e) {
          lastErr = e;
          emitHint(e.message);
          console.warn('[Chunker] chunk ' + (chunk + 1) + '/' + displayTotalChunks + ' attempt ' + (attempt + 1) + ' failed on ' + provider + ':', e.message);
        }
      }

      if (chunkSucceeded) break;
    }

    if (!chunkSucceeded || !Array.isArray(chunkItems) || chunkItems.length === 0) {
      if (allQuestions.length > 0) {
        console.warn('[Chunker] chunk ' + (chunk + 1) + ' exhausted all retries. Collected so far: ' + allQuestions.length + '. Continuing to next chunk...');
        emitProgress(allQuestions.length, chunk + 1, 'Batch ' + (chunk + 1) + ' skipped after retries — continuing... (' + allQuestions.length + '/' + total + ' done)');
        if (shouldWaitAfterChunk(chunk, allQuestions.length < total)) {
          await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'recovering tokens before batch ' + (chunk + 2));
        }
        chunk++;
        continue;
      }
      throw new Error('Failed to generate any items after ' + CHUNK_MAX_RETRIES + ' attempts. Last error: ' + ((lastErr && lastErr.message) || 'unknown'));
    }

    // Normalise and dedup. Accept at most this chunk's requested count so
    // batch 1 cannot become 26/50 and throw off the v4 alternation/wait logic.
    const beforeCount = allQuestions.length;
    const maxNewForChunk = Math.min(count, total - beforeCount);
    let addedThisChunk = 0;
    const seen = new Set(allQuestions.map(function(q){ return q.question.toLowerCase(); }));
    for (let i = 0; i < chunkItems.length && allQuestions.length < total && addedThisChunk < maxNewForChunk; i++) {
      const item = chunkItems[i];
      const q = String((item && item.question) || '').trim();
      const a = String((item && item.answer) || '').trim();
      if (q && a && !seen.has(q.toLowerCase())) {
        allQuestions.push({ question: q, answer: a });
        seen.add(q.toLowerCase());
        addedThisChunk++;
      }
    }

    if (allQuestions.length === beforeCount) {
      console.warn('[Chunker] chunk ' + (chunk + 1) + ' produced only duplicates/invalid items. Retrying/top-up will continue if needed.');
    }

    emitProgress(allQuestions.length, chunk + 1);

    const hasMoreWork = allQuestions.length < total;
    if (shouldWaitAfterChunk(chunk, hasMoreWork)) {
      await waitWithCountdown(INTER_CHUNK_DELAY_MS, 'recovering tokens before batch ' + (chunk + 2));
    } else if (hasMoreWork && aiVersion === 'v4' && getConfiguredV4Providers().length > 1) {
      emitProgress(allQuestions.length, chunk + 1, 'Switching to the next fresh AI provider immediately — ' + allQuestions.length + '/' + total + ' done...');
    }

    chunk++;
  }

  if (allQuestions.length < total) {
    throw new Error('Only generated ' + allQuestions.length + ' of ' + total + ' requested items after automatic v4 top-up attempts. Try a smaller batch or retry.');
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

// v4 single-shot flow builder — intentionally reuses the same proven v1/v2
// functions. Groq starts the flow, then xAI continues if Groq fails or returns
// invalid JSON. Web-search context is already merged into sourceText upstream.
async function buildFlowWithV4({ prompt = '', sourceText = '', fileName = '' } = {}) {
  const providers = getConfiguredV4Providers();
  if (!providers.length) throw new Error('Joytree API v4 is not configured. Add GROQ_API_KEY or XAI_API_KEY to your server .env file and restart.');

  let lastError = null;
  for (const provider of providers) {
    try {
      const result = provider === 'xai'
        ? await buildFlowWithXAI({ prompt, sourceText, fileName })
        : await buildFlowWithGroq({ prompt, sourceText, fileName });
      if (result) return Object.assign({}, result, { _aiProvider: provider === 'xai' ? 'joytree_xai' : 'joytree_groq', _aiModel: 'Joytree API v4' });
      throw new Error(provider + ' returned an empty response.');
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

async function buildFlowWithDeepSeek({ prompt = '', sourceText = '', fileName = '' } = {}) {
  if (!DEEPSEEK_API_KEY) throw new Error('DEEPSEEK_API_KEY is not configured. Add it to your .env file.');
  const userInput = `PROMPT:
${String(prompt || '').slice(0, 20000)}

SOURCE_TEXT:
${String(sourceText || '').slice(0, 20000)}

FILE_NAME:
${String(fileName || '').slice(0, 200)}`;
  const messages = [
    { role: 'system', content: `${AI_FLOW_INSTRUCTION}
Joytree API v3 runs on DeepSeek high-reasoning AI. If WEB_SEARCH_RESULTS appear in SOURCE_TEXT, use them as current web context. Return only valid JSON for the requested flow.` },
    { role: 'user', content: userInput }
  ];
  let lastErr = null;
  for (const profile of getDeepSeekAttemptProfiles()) {
    try {
      const text = await callDeepSeekChat({
        messages: messages,
        temperature: 0.25,
        maxTokens: DEEPSEEK_FLOW_MAX_TOKENS,
        timeoutMs: DEEPSEEK_FLOW_TIMEOUT_MS,
        model: profile.model,
        thinkingEnabled: profile.thinkingEnabled
      });
      const parsed = extractJsonFromText(text);
      if (parsed && typeof parsed === 'object') return parsed;
      lastErr = new Error('DeepSeek returned non-JSON output. Raw: ' + (text || '').slice(0, 160));
      console.warn('[Joytree API v3] DeepSeek ' + profile.label + ' returned non-JSON:', (text || '').slice(0, 300));
    } catch (e) {
      lastErr = e;
      console.warn('[Joytree API v3] DeepSeek ' + profile.label + ' failed:', e.message);
    }
  }
  if (lastErr && lastErr.name === 'AbortError') throw new Error('DeepSeek timed out. Try increasing DEEPSEEK_FLOW_TIMEOUT_MS.');
  console.error('[Joytree API v3] buildFlowWithDeepSeek failed:', lastErr && lastErr.message);
  throw lastErr || new Error('DeepSeek returned no valid JSON flow.');
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
  // Joytree API v4 — use the same proven Groq/xAI flow builders as v1/v2,
  // with Groq first and xAI continuing if Groq fails or returns invalid JSON.
  if (aiVersion === 'v4') {
    const result = await buildFlowWithV4({ prompt, sourceText, fileName });
    if (result) return result; // already has _aiProvider and _aiModel set
    throw new Error('Joytree API v4 returned an empty response.');
  }

  // Joytree API v3 — DeepSeek high-reasoning cloud AI (admin only at route layer).
  // If DeepSeek is temporarily failing, silently fall back to the same stable
  // Groq/xAI builders so users do not see a deploy failure.
  if (aiVersion === 'v3') {
    let lastError = null;
    try {
      const result = await buildFlowWithDeepSeek({ prompt, sourceText, fileName });
      if (result) return { ...result, _aiProvider: 'deepseek', _aiModel: 'Joytree API v3 (DeepSeek)' };
      lastError = new Error('DeepSeek returned an empty response.');
    } catch (e) {
      lastError = e;
      console.warn('[Joytree API v3] DeepSeek failed, trying stable fallback:', e.message);
    }
    if (GROQ_API_KEY) {
      try {
        const result = await buildFlowWithGroq({ prompt, sourceText, fileName });
        if (result) return { ...result, _aiProvider: 'deepseek_groq_fallback', _aiModel: 'Joytree API v3 (DeepSeek fallback)' };
      } catch (e) {
        lastError = e;
        console.warn('[Joytree API v3] Groq fallback failed:', e.message);
      }
    }
    if (XAI_API_KEY) {
      try {
        const result = await buildFlowWithXAI({ prompt, sourceText, fileName });
        if (result) return { ...result, _aiProvider: 'deepseek_xai_fallback', _aiModel: 'Joytree API v3 (DeepSeek fallback)' };
      } catch (e) {
        lastError = e;
        console.warn('[Joytree API v3] xAI fallback failed:', e.message);
      }
    }
    throw new Error('Joytree API v3 returned an empty response. Last error: ' + ((lastError && lastError.message) || 'Check DEEPSEEK_API_KEY.'));
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
  if (selectedAiVersion === 'v3' && !DEEPSEEK_API_KEY) return res.status(503).json({ error: 'Joytree API v3 is not configured. Add DEEPSEEK_API_KEY to your server .env file and restart.' });
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
  const usedModel = aiSpec?._aiModel || (selectedAiVersion === 'v4' ? 'Joytree API v4' : (selectedAiVersion === 'v3' ? 'Joytree API v3 (DeepSeek)' : (selectedAiVersion === 'v2' ? 'Joy AI v2' : 'Joy AI v1')));
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
      name: 'Joytree API v3 (DeepSeek)',
      adminOnly: true,
      hasAccess: isRootEmailAdmin(req.user),
      deepseekConfigured: !!DEEPSEEK_API_KEY,
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

// ══════════════════════════════════════════════════════════════════
// JOYTREE AI AGENT — Full autonomous coding agent
// Providers: Groq (Llama) · xAI (Grok) · Anthropic (Claude)
// Logic ported from groq-code-cli (MIT). Claude added natively.
// Tools: read_file, create_file, edit_file, delete_file,
//        list_files, search_files, execute_command,
//        create_tasks, update_tasks  (all 9 from groq-code-cli)
// ══════════════════════════════════════════════════════════════════

// ── In-memory session store ───────────────────────────────────────
const agentSessions = new Map();

// [FIX] Prevent agentSessions from growing forever — sessions older than 2h
// are removed. Without this, every agent run permanently occupies memory
// including the full event array (file diffs, logs, etc.) until restart.
setInterval(() => {
  const TWO_HOURS = 2 * 60 * 60 * 1000;
  const now = Date.now();
  for (const [id, s] of agentSessions) {
    if (s.startedAt && now - new Date(s.startedAt).getTime() > TWO_HOURS) {
      // Clean up the AI-fix zip on disk too, if any.
      if (s.zipPath) { try { fs.existsSync(s.zipPath) && fs.unlinkSync(s.zipPath); } catch (_) {} }
      agentSessions.delete(id);
    }
  }
}, 15 * 60 * 1000); // run every 15 minutes

// ── SSE helper ────────────────────────────────────────────────────
function sseWrite(res, event, data) {
  if (res.writableEnded) return;
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

// ── Provider detection ────────────────────────────────────────────
// Returns the best available agent provider in priority order:
// claude > groq > xai  (Claude is best at coding tasks)
function getAgentProvider(preferredProvider, userKeys = {}) {
  const has = (id, sharedKey) => !!((userKeys && userKeys[id]) || sharedKey);
  if (preferredProvider === 'claude'  && has('claude', ANTHROPIC_API_KEY)) return 'claude';
  if (preferredProvider === 'groq'    && has('groq',   GROQ_API_KEY))      return 'groq';
  if (preferredProvider === 'xai'     && has('xai',    XAI_API_KEY))       return 'xai';
  if (preferredProvider === 'openai'  && has('openai', OPENAI_API_KEY))    return 'openai';
  // Auto-select when no explicit choice: prefer the FREE/cheap providers first
  // so the default never silently burns paid credit. Groq (Llama) is free.
  if (has('groq',   GROQ_API_KEY))      return 'groq';
  if (has('xai',    XAI_API_KEY))       return 'xai';
  if (has('openai', OPENAI_API_KEY))    return 'openai';
  if (has('claude', ANTHROPIC_API_KEY)) return 'claude';
  return null;
}

function getAgentProviderLabel(provider) {
  if (provider === 'claude') return `Claude (${ANTHROPIC_MODEL})`;
  if (provider === 'xai')    return `Grok (${XAI_MODEL})`;
  if (provider === 'openai') return `GPT (${OPENAI_MODEL})`;
  return `Llama (${GROQ_MODEL})`;
}

// ── Tool SCHEMAS (exact copy from groq-code-cli tool-schemas.ts) ──
// These are sent to the model as function definitions.
// Claude uses the same tool_use format when called via its native API.
const GROQ_AGENT_TOOL_SCHEMAS = [
  {
    type: 'function',
    function: {
      name: 'read_file',
      description: 'Read file contents with optional line range. REQUIRED before edit_file. Use to check if files exist and examine current code before making changes.',
      parameters: {
        type: 'object',
        properties: {
          file_path:  { type: 'string',  description: 'Absolute path to file in the cloned repo.' },
          start_line: { type: 'integer', description: 'Starting line number (1-indexed, optional)', minimum: 1 },
          end_line:   { type: 'integer', description: 'Ending line number (1-indexed, optional)',  minimum: 1 }
        },
        required: ['file_path']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'create_file',
      description: 'Create NEW files or directories. CRITICAL: Always check if file exists first with list_files or read_file. If file exists, use edit_file instead. Set overwrite=true only if explicitly replacing content.',
      parameters: {
        type: 'object',
        properties: {
          file_path: { type: 'string',  description: 'Absolute path for new file/directory.' },
          content:   { type: 'string',  description: 'File content (use empty string for directories)' },
          file_type: { type: 'string',  enum: ['file','directory'], description: 'Create file or directory', default: 'file' },
          overwrite: { type: 'boolean', description: 'Overwrite existing file', default: false }
        },
        required: ['file_path','content']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'edit_file',
      description: 'Modify EXISTING files by exact text replacement. MANDATORY: read_file first. Text must match exactly including whitespace.',
      parameters: {
        type: 'object',
        properties: {
          file_path:   { type: 'string',  description: 'Absolute path to file to edit.' },
          old_text:    { type: 'string',  description: 'Exact text to replace (must match perfectly including spaces/newlines)' },
          new_text:    { type: 'string',  description: 'Replacement text' },
          replace_all: { type: 'boolean', description: 'Replace all occurrences (default: false)', default: false }
        },
        required: ['file_path','old_text','new_text']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'delete_file',
      description: 'Remove files or directories. Use with caution.',
      parameters: {
        type: 'object',
        properties: {
          file_path: { type: 'string',  description: 'Absolute path to file/directory to delete.' },
          recursive: { type: 'boolean', description: 'Delete directories and their contents recursively', default: false }
        },
        required: ['file_path']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'list_files',
      description: 'Browse directory contents and file structure. Use to explore project layout and check if files exist.',
      parameters: {
        type: 'object',
        properties: {
          directory:   { type: 'string',  description: 'Absolute directory path to list.' },
          pattern:     { type: 'string',  description: 'File pattern filter (*.py, *.js, etc.)', default: '*' },
          recursive:   { type: 'boolean', description: 'List subdirectories recursively', default: false },
          show_hidden: { type: 'boolean', description: 'Include hidden files (.gitignore, .env, etc.)', default: false }
        },
        required: []
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'search_files',
      description: 'Find text patterns in files across the codebase. Perfect for locating functions, classes, or specific code.',
      parameters: {
        type: 'object',
        properties: {
          pattern:       { type: 'string',  description: 'Text or regex to search for' },
          file_pattern:  { type: 'string',  description: 'File filter (*.py, *.js)', default: '*' },
          directory:     { type: 'string',  description: 'Absolute directory path to search in.' },
          case_sensitive:{ type: 'boolean', description: 'Case-sensitive search', default: false },
          pattern_type:  { type: 'string',  enum: ['substring','regex','exact','fuzzy'], default: 'substring' },
          file_types:    { type: 'array',   items: { type: 'string' }, description: 'File extensions to include (["py","js","ts"])' },
          exclude_dirs:  { type: 'array',   items: { type: 'string' }, description: 'Directories to skip' },
          exclude_files: { type: 'array',   items: { type: 'string' }, description: 'File patterns to skip' },
          max_results:   { type: 'integer', description: 'Maximum results', default: 100, minimum: 1, maximum: 1000 },
          context_lines: { type: 'integer', description: 'Lines of context around each match (0-10)', default: 0, minimum: 0, maximum: 10 },
          group_by_file: { type: 'boolean', description: 'Group results by filename', default: false }
        },
        required: ['pattern']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'execute_command',
      description: 'Run shell commands. SAFETY: Only for commands that EXIT (npm test, ls, git diff, node -e). NEVER for servers (npm start, flask run, nodemon).',
      parameters: {
        type: 'object',
        properties: {
          command:           { type: 'string', description: 'Shell command to execute. Must exit automatically.' },
          command_type:      { type: 'string', enum: ['bash','python','setup','run'], description: 'bash=shell, python=script, setup=auto-run, run=needs approval' },
          working_directory: { type: 'string', description: 'Absolute directory to run command in (optional)' },
          timeout:           { type: 'integer', description: 'Max execution time in ms', minimum: 1, maximum: 30000 }
        },
        required: ['command','command_type']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'create_tasks',
      description: 'Break down complex fix requests into organized task lists. Use at the START of multi-step fixes.',
      parameters: {
        type: 'object',
        properties: {
          user_query: { type: 'string', description: 'Original user request being broken down' },
          tasks: {
            type: 'array',
            description: 'List of actionable subtasks',
            items: {
              type: 'object',
              properties: {
                id:          { type: 'string', description: 'Unique task identifier (e.g. "1","2","3")' },
                description: { type: 'string', description: 'Clear, actionable task description' },
                status:      { type: 'string', enum: ['pending','in_progress','completed'], default: 'pending' }
              },
              required: ['id','description']
            }
          }
        },
        required: ['user_query','tasks']
      }
    }
  },
  {
    type: 'function',
    function: {
      name: 'update_tasks',
      description: 'Update task progress and status as you complete each step.',
      parameters: {
        type: 'object',
        properties: {
          task_updates: {
            type: 'array',
            items: {
              type: 'object',
              properties: {
                id:     { type: 'string', description: 'ID of task to update' },
                status: { type: 'string', enum: ['pending','in_progress','completed'] },
                notes:  { type: 'string', description: 'Optional progress notes' }
              },
              required: ['id','status']
            }
          }
        },
        required: ['task_updates']
      }
    }
  }
];

// ── Claude native tool format converter ───────────────────────────
// Anthropic uses { name, description, input_schema } instead of OpenAI's format
function toAnthropicTools(schemas) {
  return schemas.map(s => ({
    name:         s.function.name,
    description:  s.function.description,
    input_schema: s.function.parameters
  }));
}

// ── Single unified LLM call — handles Groq/xAI (OpenAI-compat) and Claude (native) ──
async function callAgentLLM({ provider, messages, tools, temperature = 0.2, maxTokens = 8000, userKeys = {} }) {

  // ── Claude via Anthropic native API ──────────────────────────
  if (provider === 'claude') {
    const anthropicKey = (userKeys && userKeys.claude) ? userKeys.claude : ANTHROPIC_API_KEY;
    if (!anthropicKey) throw new Error('ANTHROPIC_API_KEY not configured');

    // Convert OpenAI message format → Anthropic format
    // Anthropic: system is a top-level param, not a message role
    // Tool results use role:'user' with content type 'tool_result'
    const systemMsg = messages.filter(m => m.role === 'system').map(m => m.content).join('\n\n');
    const anthropicMessages = [];
    for (const m of messages) {
      if (m.role === 'system') continue;

      if (m.role === 'assistant') {
        const content = [];
        if (m.content) content.push({ type: 'text', text: m.content });
        if (m.tool_calls) {
          for (const tc of m.tool_calls) {
            let inp = {};
            try { inp = JSON.parse(tc.function?.arguments || '{}'); } catch (_) {}
            content.push({ type: 'tool_use', id: tc.id, name: tc.function.name, input: inp });
          }
        }
        anthropicMessages.push({ role: 'assistant', content });
        continue;
      }

      if (m.role === 'tool') {
        // Tool results must be wrapped in a user message
        // Group consecutive tool results together
        const last = anthropicMessages[anthropicMessages.length - 1];
        const toolResultBlock = {
          type: 'tool_result',
          tool_use_id: m.tool_call_id,
          content: String(m.content || '')
        };
        if (last && last.role === 'user' && Array.isArray(last.content)) {
          last.content.push(toolResultBlock);
        } else {
          anthropicMessages.push({ role: 'user', content: [toolResultBlock] });
        }
        continue;
      }

      // user
      anthropicMessages.push({ role: 'user', content: m.content });
    }

    // ── Prompt caching (cuts cost on repeated tokens) ──
    // The agent re-sends the SAME large system prompt + tool schemas on every
    // loop iteration. Marking them with cache_control means Anthropic bills the
    // repeated read at ~10% of the input price after the first call. We also
    // cache the conversation prefix (last content block) so the growing message
    // history is reused cheaply across iterations. Up to 4 breakpoints allowed.
    const systemBlocks = systemMsg
      ? [{ type: 'text', text: systemMsg, cache_control: { type: 'ephemeral' } }]
      : undefined;

    const cachedTools = toAnthropicTools(tools);
    if (Array.isArray(cachedTools) && cachedTools.length) {
      // Cache the whole tool set by marking the last tool (cache covers all prior).
      cachedTools[cachedTools.length - 1] = Object.assign({}, cachedTools[cachedTools.length - 1], { cache_control: { type: 'ephemeral' } });
    }

    // Cache the conversation prefix: mark the last block of the last message so
    // everything before it is reused on the next turn.
    if (anthropicMessages.length) {
      const lastMsg = anthropicMessages[anthropicMessages.length - 1];
      if (typeof lastMsg.content === 'string') {
        lastMsg.content = [{ type: 'text', text: lastMsg.content, cache_control: { type: 'ephemeral' } }];
      } else if (Array.isArray(lastMsg.content) && lastMsg.content.length) {
        const lb = lastMsg.content[lastMsg.content.length - 1];
        lastMsg.content[lastMsg.content.length - 1] = Object.assign({}, lb, { cache_control: { type: 'ephemeral' } });
      }
    }

    const body = {
      model:      ANTHROPIC_MODEL,
      max_tokens: maxTokens,
      temperature,
      system:     systemBlocks,
      tools:      cachedTools,
      messages:   anthropicMessages
    };

    let r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         anthropicKey,
        'anthropic-version': '2023-06-01',
        // Enables prompt caching.
        'anthropic-beta':    'prompt-caching-2024-07-31'
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(120000) // 120s — complex edits can take 90s+
    });

    // [FIX] Anthropic also returns 429 when rate-limited. Honor its
    // 'retry-after' header (seconds) when present, otherwise exponential
    // backoff (2s, 4s, 8s). Retry up to 3 times so transient throttling
    // doesn't kill the whole agent run.
    let _claudeRetry = 0;
    while (!r.ok && r.status === 429 && _claudeRetry < 3) {
      _claudeRetry++;
      const retryAfter = parseFloat(r.headers.get('retry-after') || '');
      let waitMs = (!isNaN(retryAfter) && retryAfter > 0) ? Math.ceil(retryAfter * 1000) : Math.pow(2, _claudeRetry) * 1000;
      waitMs = Math.min(waitMs + 350, 15000);
      await new Promise(res => setTimeout(res, waitMs));
      r = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'Content-Type':      'application/json',
          'x-api-key':         anthropicKey,
          'anthropic-version': '2023-06-01',
          'anthropic-beta':    'prompt-caching-2024-07-31'
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(120000)
      });
    }

    if (!r.ok) {
      const retryAfterHdr = parseFloat(r.headers.get('retry-after') || '');
      const e = await r.json().catch(() => ({}));
      if (r.status === 401) throw new Error('Anthropic API Error (401): Invalid API key. Check ANTHROPIC_API_KEY.');
      if (r.status === 429) {
        // Same clean, JoyTree-branded rate-limit message as the other providers.
        // Anthropic returns the reset window in the 'retry-after' header (seconds).
        const secs = (!isNaN(retryAfterHdr) && retryAfterHdr > 0) ? Math.ceil(retryAfterHdr) : 60;
        const label = _secondsToLabel(secs);
        const windowWord = secs >= 3600 ? 'hourly' : secs >= 120 ? '' : 'per-minute';
        const winTxt = windowWord ? `${windowWord} ` : '';
        throw new Error(`__JAI_RATE_LIMIT__:${secs}:JoyTree AI has hit its ${winTxt}usage limit for now. Please try again in about ${label}.`);
      }
      // [FIX] Anthropic 400 "credit balance too low" — surface a clean message
      // rather than the raw billing error.
      const msg400 = String(e?.error?.message || '');
      if (r.status === 400 && /credit balance|too low|billing/i.test(msg400)) {
        throw new Error('__JAI_NO_CREDITS__:JoyTree AI (Claude) is temporarily unavailable — the API account needs credits topped up. Try the Llama model instead, which is free.');
      }
      throw new Error(`Anthropic API error ${r.status}: ${e?.error?.message || r.statusText}`);
    }

    const data = await r.json();

    // [CACHE] Log cache effectiveness so you can confirm prompt caching is
    // working. cache_read_input_tokens are billed at ~10% of normal input.
    if (data.usage) {
      const u = data.usage;
      const cw = u.cache_creation_input_tokens || 0;
      const cr = u.cache_read_input_tokens || 0;
      if (cw || cr) console.log(`[claude-cache] in=${u.input_tokens||0} cache_write=${cw} cache_read=${cr} out=${u.output_tokens||0}`);
    }
    // [FIX] Handle max_tokens stop reason — if Claude hit the token limit,
    // it stopped mid-tool-call. Push a warning so the loop doesn't silently
    // treat a truncated response as a completed turn.
    if (data.stop_reason === 'max_tokens') {
      return {
        choices: [{ message: { role: 'assistant', content: '[Token limit reached — response was truncated. Continuing…]', tool_calls: undefined, _stop_reason: 'max_tokens' }, finish_reason: 'length' }],
        usage: data.usage
      };
    }

    const textBlocks  = data.content.filter(b => b.type === 'text');
    const toolBlocks  = data.content.filter(b => b.type === 'tool_use');
    const textContent = textBlocks.map(b => b.text).join('\n') || '';

    const normalised = {
      role:    'assistant',
      content: textContent || null,
      // Convert tool_use blocks → OpenAI tool_calls format
      tool_calls: toolBlocks.length ? toolBlocks.map(b => ({
        id: b.id,
        type: 'function',
        function: { name: b.name, arguments: JSON.stringify(b.input || {}) }
      })) : undefined,
      // Anthropic stop reasons
      _stop_reason: data.stop_reason  // 'end_turn' | 'tool_use' | 'max_tokens'
    };

    return {
      choices: [{ message: normalised, finish_reason: data.stop_reason === 'tool_use' ? 'tool_calls' : 'stop' }],
      usage: data.usage
    };
  }

  // ── Groq / xAI / OpenAI — OpenAI-compatible ───────────────────────────
  const apiUrl = provider === 'xai'
    ? 'https://api.x.ai/v1/chat/completions'
    : provider === 'openai'
    ? 'https://api.openai.com/v1/chat/completions'
    : 'https://api.groq.com/openai/v1/chat/completions';
  // BYOK: prefer the user's own key for this provider, fall back to the shared key.
  const apiKey = provider === 'xai'
    ? ((userKeys && userKeys.xai) || XAI_API_KEY)
    : provider === 'openai'
    ? ((userKeys && userKeys.openai) || OPENAI_API_KEY)
    : ((userKeys && userKeys.groq) || GROQ_API_KEY);
  const model  = provider === 'xai' ? XAI_MODEL : provider === 'openai' ? OPENAI_MODEL : GROQ_MODEL;

  if (!apiKey) throw new Error(`${provider.toUpperCase()}_API_KEY not configured`);

  const doCall = (tokens) => fetch(apiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ model, messages, tools, tool_choice: 'auto', temperature, max_tokens: tokens, stream: false }),
    signal: AbortSignal.timeout(provider === 'groq' ? 45000 : 90000)
  });

  let r = await doCall(maxTokens);

  // [FIX] 429 = transient rate limit (per-minute TPM/RPM throttling), NOT a
  // structural "too large" error. Unlike 413, the SAME request will succeed
  // if we simply wait a moment — Groq/xAI even tell us how long in the body
  // ("Please try again in 2.1675s"). Retry up to 3 times with backoff: honor
  // the server's suggested wait when present, otherwise exponential (2s, 4s,
  // 8s). This keeps the agent alive through free-tier throttling instead of
  // aborting a whole run on a wait that's usually just a second or two.
  let _retry429 = 0;
  while (!r.ok && r.status === 429 && _retry429 < 3) {
    _retry429++;
    const bodyText429 = await r.text().catch(() => '');
    // Parse "try again in 2.1675s" (seconds) from the error body if present
    const waitMatch = bodyText429.match(/try again in ([\d.]+)\s*s/i);
    let waitMs = waitMatch ? Math.ceil(parseFloat(waitMatch[1]) * 1000) : 0;
    // Fall back to exponential backoff, and always add a small buffer + cap
    if (!waitMs || isNaN(waitMs)) waitMs = Math.pow(2, _retry429) * 1000; // 2s, 4s, 8s
    waitMs = Math.min(waitMs + 350, 12000); // +350ms safety buffer, cap at 12s
    await new Promise(res => setTimeout(res, waitMs));
    r = await doCall(maxTokens);
  }

  if (!r.ok && r.status === 413) {
    // [FIX] "Request too large... on tokens per minute (TPM): Limit 8000,
    // Requested 9845" means prompt_tokens + max_tokens exceeded the model's
    // per-request TPM cap on this tier — a hard structural limit, not
    // transient throttling, so retrying the SAME max_tokens would just fail
    // again every time. Parse the two numbers out of Groq's error body and
    // retry once with a max_tokens that actually leaves room for the prompt
    // (Requested - originalMaxTokens ≈ prompt size), rather than aborting
    // the whole agent run on what's otherwise a recoverable situation.
    const bodyText = await r.text();
    const limitMatch     = bodyText.match(/Limit (\d+)/i);
    const requestedMatch = bodyText.match(/Requested (\d+)/i);
    if (limitMatch && requestedMatch) {
      const limit         = parseInt(limitMatch[1], 10);
      const requested     = parseInt(requestedMatch[1], 10);
      const promptTokens  = Math.max(0, requested - maxTokens);
      const retryTokens   = Math.max(512, limit - promptTokens - 256); // 256-token safety margin
      if (retryTokens < maxTokens) {
        r = await doCall(retryTokens);
      }
    }
  }

  if (!r.ok) {
    const e = await r.text();
    if (r.status === 401) throw new Error(`API Error (401): Invalid ${provider.toUpperCase()} API key.`);
    if (r.status === 429) {
      // Translate the raw provider rate-limit error into a clean, JoyTree-branded
      // message. Parse whatever reset window the provider returned ("try again in
      // 2.5s", "1m30s", "in 1h") and tell the user when JoyTree AI will be ready
      // again — without leaking Groq/xAI/TPM jargon.
      const wait = _parseRateLimitWait(e);
      const when = wait.text ? ` Please try again in about ${wait.text}.` : ' Please wait a moment and try again.';
      throw new Error(`__JAI_RATE_LIMIT__:${wait.seconds}:JoyTree AI has hit its ${wait.window} usage limit for now.${when}`);
    }
    if (r.status === 413) {
      // A 413 mentioning "tokens per minute (TPM)" means the conversation grew
      // too large for the per-minute window — treat it like a usage limit and
      // tell the user to wait, with the same clean JoyTree-branded message
      // (no raw Groq/TPM jargon). Non-TPM 413s are genuinely structural.
      if (/tokens per minute|TPM|per minute/i.test(e)) {
        const wait = _parseRateLimitWait(e);
        const secs = wait.seconds || 60;
        throw new Error(`__JAI_RATE_LIMIT__:${secs}:JoyTree AI's request grew too large for its current ${wait.window || 'per-minute'} limit. Please wait about ${_secondsToLabel(secs)} and continue — it'll pick up where it left off.`);
      }
      throw new Error(`__JAI_RATE_LIMIT__:60:JoyTree AI's request grew too large to process in one step. Please wait a moment and continue — it'll pick up where it left off.`);
    }
    throw new Error(`${provider} API error ${r.status}: ${e.slice(0, 300)}`);
  }
  return r.json();
}

// Convert seconds → human label e.g. "45 seconds", "3 minutes", "1.5 hours".
function _secondsToLabel(seconds) {
  const s = Math.ceil(seconds || 0);
  if (s < 60) return `${s} second${s !== 1 ? 's' : ''}`;
  if (s < 3600) { const m = Math.ceil(s / 60); return `${m} minute${m !== 1 ? 's' : ''}`; }
  const h = Math.round(s / 3600 * 10) / 10; return `${h} hour${h !== 1 ? 's' : ''}`;
}

// Parse a provider rate-limit error body into a friendly wait time + window label.
// Handles Groq formats like "try again in 2.5s", "try again in 1m30.5s",
// "try again in 1h12m", and infers whether it's a per-minute/hour/day window.
function _parseRateLimitWait(body) {
  const text = String(body || '');
  let seconds = 0;
  // Combined h/m/s e.g. "1h2m3.5s" or "1m30s" or "2.16s"
  const combo = text.match(/try again in\s+(?:(\d+)h)?(?:(\d+)m)?(?:([\d.]+)s)?/i);
  if (combo && (combo[1] || combo[2] || combo[3])) {
    seconds = (parseInt(combo[1] || 0, 10) * 3600)
            + (parseInt(combo[2] || 0, 10) * 60)
            + (parseFloat(combo[3] || 0));
  }
  // Determine which window was hit (Groq says "per day"/"per hour"/"tokens per minute (TPM)" etc.)
  let window = 'rate';
  if (/per day|requests per day|RPD|tokens per day|TPD/i.test(text)) window = 'daily';
  else if (/per hour|requests per hour|RPH|tokens per hour|TPH/i.test(text)) window = 'hourly';
  else if (/per minute|TPM|RPM/i.test(text)) window = 'minute';
  // If no explicit wait but we know the window, give a sensible default
  if (!seconds) {
    if (window === 'daily') seconds = 3600;        // unknown daily reset — suggest ~1h
    else if (window === 'hourly') seconds = 600;   // ~10m
    else seconds = 60;                              // minute window — ~1m
  }
  const s = Math.ceil(seconds);
  const windowLabel = window === 'daily' ? 'daily' : window === 'hourly' ? 'hourly' : window === 'minute' ? 'per-minute' : '';
  return { seconds: s, text: _secondsToLabel(s), window: windowLabel };
}

// ── Tool executor (full port of groq-code-cli tools.ts) ──────────
async function executeAgentTool(toolName, toolArgs, sandboxDir, readFilesTracker, taskState) {

  function guardPath(p) {
    const resolved = path.resolve(p);
    if (!resolved.startsWith(sandboxDir)) throw new Error(`Path "${resolved}" is outside the repo sandbox.`);
    return resolved;
  }

  function isBinary(name) {
    return /\.(exe|dll|so|dylib|bin|obj|o|a|lib|jpg|jpeg|png|gif|bmp|ico|webp|mp3|mp4|avi|mov|zip|tar|gz|bz2|rar|7z|pdf|doc|docx|xls|xlsx|ppt|pptx)$/i.test(name);
  }

  function matchesPattern(filename, pattern) {
    if (!pattern || pattern === '*') return true;
    const rx = pattern.replace(/\./g,'\\.').replace(/\*/g,'.*').replace(/\?/g,'.');
    return new RegExp(`^${rx}$`,'i').test(filename);
  }

  async function collectFiles(dir, filePattern, fileTypes, excludeDirs, excludeFiles) {
    const EX_DIRS  = ['.git','node_modules','.next','dist','build','.cache','coverage',...(excludeDirs||[])];
    const EX_FILES = ['*.log','*.tmp','*.cache','*.lock','package-lock.json','yarn.lock',...(excludeFiles||[])];
    const files = [];
    async function walk(d) {
      let entries; try { entries = await fs.promises.readdir(d,{withFileTypes:true}); } catch { return; }
      for (const e of entries) {
        const full = path.join(d, e.name);
        if (e.isDirectory()) {
          if (EX_DIRS.some(p => matchesPattern(e.name,p))) continue;
          if (e.name.startsWith('.')) continue;
          await walk(full);
        } else if (e.isFile()) {
          if (fileTypes?.length) { const ext=path.extname(e.name).slice(1); if(!fileTypes.includes(ext)) continue; }
          if (!matchesPattern(e.name, filePattern||'*')) continue;
          if (EX_FILES.some(p => matchesPattern(e.name,p))) continue;
          if (isBinary(e.name)) continue;
          files.push(full);
        }
      }
    }
    await walk(dir);
    return files;
  }

  async function displayTree(dirPath, pattern, recursive, showHidden) {
    const items = await fs.promises.readdir(dirPath,{withFileTypes:true}).catch(()=>[]);
    const visible = items
      .filter(i => showHidden||!i.name.startsWith('.'))
      .filter(i => i.isDirectory()||matchesPattern(i.name,pattern||'*'))
      .sort((a,b)=>{
        if(a.isDirectory()&&!b.isDirectory()) return -1;
        if(!a.isDirectory()&&b.isDirectory()) return 1;
        return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
      });
    let out = path.basename(dirPath)+'/\n';
    for (let i=0;i<visible.length;i++) {
      const item=visible[i]; const isLast=i===visible.length-1;
      const prefix=isLast?'└── ':'├── ';
      out += prefix+item.name+(item.isDirectory()?'/':'')+'\n';
      if (item.isDirectory()&&recursive) {
        const sub=await displayTree(path.join(dirPath,item.name),pattern,recursive,showHidden);
        const indent=isLast?'    ':'│   ';
        out += sub.split('\n').slice(1).map(l=>indent+l).join('\n')+'\n';
      }
    }
    return out.trimEnd();
  }

  try {
    // ── read_file ──────────────────────────────────────────────
    if (toolName==='read_file') {
      const fp=guardPath(toolArgs.file_path);
      try{await fs.promises.access(fp);}catch{return{success:false,error:'Error: File not found'};}
      const stat=await fs.promises.stat(fp);
      if(!stat.isFile()) return{success:false,error:'Error: Path is not a file'};
      if(stat.size>50*1024*1024) return{success:false,error:'Error: File too large (max 50MB)'};
      const raw=await fs.promises.readFile(fp,'utf-8');
      const lines=raw.split('\n');
      let content=raw, msg=`Read ${lines.length} lines from ${toolArgs.file_path}`;
      if(toolArgs.start_line!==undefined){
        const s=Math.max(0,toolArgs.start_line-1);
        const e=toolArgs.end_line!==undefined?Math.min(lines.length,toolArgs.end_line):lines.length;
        if(s>=lines.length) return{success:false,error:'Error: Start line exceeds file length'};
        content=lines.slice(s,e).join('\n');
        msg=`Read lines ${toolArgs.start_line}-${e} from ${toolArgs.file_path}`;
      }
      readFilesTracker.add(fp);
      return{success:true,content,message:msg};
    }

    // ── create_file ────────────────────────────────────────────
    if (toolName==='create_file') {
      const fp=guardPath(toolArgs.file_path);
      const exists=await fs.promises.access(fp).then(()=>true).catch(()=>false);
      if(exists&&!toolArgs.overwrite) return{success:false,error:'Error: File already exists. Use overwrite=true or edit_file instead.'};
      if(toolArgs.file_type==='directory'){
        await fs.promises.mkdir(fp,{recursive:true});
        return{success:true,message:`Directory created: ${toolArgs.file_path}`};
      }
      await fs.promises.mkdir(path.dirname(fp),{recursive:true});
      await fs.promises.writeFile(fp,toolArgs.content||'','utf-8');
      return{success:true,message:`File created: ${toolArgs.file_path}`};
    }

    // ── edit_file ──────────────────────────────────────────────
    if (toolName==='edit_file') {
      const fp=guardPath(toolArgs.file_path);
      if(!readFilesTracker.has(fp))
        return{success:false,error:`File must be read before editing. Call read_file first on: ${toolArgs.file_path}`};
      const original=await fs.promises.readFile(fp,'utf-8').catch(()=>null);
      if(original===null) return{success:false,error:'Error: File not found'};
      if(!original.includes(toolArgs.old_text))
        return{success:false,error:'Error: old_text not found exactly in file — must match character-for-character including whitespace. Re-read the file first.'};
      const updated=toolArgs.replace_all
        ?original.split(toolArgs.old_text).join(toolArgs.new_text)
        :original.replace(toolArgs.old_text,toolArgs.new_text);
      await fs.promises.writeFile(fp,updated,'utf-8');
      const count=toolArgs.replace_all?original.split(toolArgs.old_text).length-1:1;
      return{success:true,message:`Replaced ${count} occurrence(s) in ${toolArgs.file_path}`};
    }

    // ── delete_file ────────────────────────────────────────────
    if (toolName==='delete_file') {
      const fp=guardPath(toolArgs.file_path);
      if(fp===sandboxDir) return{success:false,error:'Error: Cannot delete the repo root.'};
      const exists=await fs.promises.access(fp).then(()=>true).catch(()=>false);
      if(!exists) return{success:false,error:'Error: Path not found'};
      const stat=await fs.promises.stat(fp);
      if(stat.isDirectory()){
        const items=await fs.promises.readdir(fp);
        if(items.length&&!toolArgs.recursive) return{success:false,error:'Error: Directory not empty, use recursive=true'};
        await fs.promises.rm(fp,{recursive:true,force:true});
      } else { await fs.promises.unlink(fp); }
      return{success:true,message:`Deleted: ${toolArgs.file_path}`};
    }

    // ── list_files ─────────────────────────────────────────────
    if (toolName==='list_files') {
      const dir=guardPath(toolArgs.directory||sandboxDir);
      const exists=await fs.promises.access(dir).then(()=>true).catch(()=>false);
      if(!exists) return{success:false,error:'Error: Directory not found'};
      const stat=await fs.promises.stat(dir);
      if(!stat.isDirectory()) return{success:false,error:'Error: Path is not a directory'};
      const tree=await displayTree(dir,toolArgs.pattern||'*',!!toolArgs.recursive,!!toolArgs.show_hidden);
      return{success:true,content:tree,message:`Listed ${toolArgs.directory||'.'}`};
    }

    // ── search_files ───────────────────────────────────────────
    if (toolName==='search_files') {
      const searchDir=guardPath(toolArgs.directory||sandboxDir);
      const exists=await fs.promises.access(searchDir).then(()=>true).catch(()=>false);
      if(!exists) return{success:false,error:'Error: Directory not found'};
      const caseSensitive=!!toolArgs.case_sensitive;
      const patternType=toolArgs.pattern_type||'substring';
      const maxResults=Math.min(toolArgs.max_results||100,1000);
      const contextLines=Math.min(Math.max(toolArgs.context_lines||0,0),10);
      const groupByFile=!!toolArgs.group_by_file;
      const rawPat=String(toolArgs.pattern||'');
      function escRx(s){return s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');}
      let searchRegex;
      try {
        const flags=caseSensitive?'g':'gi';
        if(patternType==='regex')      searchRegex=new RegExp(rawPat,flags);
        else if(patternType==='exact') searchRegex=new RegExp(escRx(rawPat),flags);
        else if(patternType==='fuzzy') searchRegex=new RegExp(rawPat.split('').map(escRx).join('.*'),flags);
        else                           searchRegex=new RegExp(escRx(rawPat),flags);
      } catch { return{success:false,error:'Error: Invalid regex pattern'}; }
      const filesToSearch=await collectFiles(searchDir,toolArgs.file_pattern||'*',toolArgs.file_types,toolArgs.exclude_dirs,toolArgs.exclude_files);
      const results=[]; let totalMatches=0;
      for(const fp of filesToSearch){
        if(totalMatches>=maxResults) break;
        try{
          const text=await fs.promises.readFile(fp,'utf-8');
          const lineArr=text.split('\n');
          const fileMatches=[];
          for(let i=0;i<lineArr.length&&totalMatches<maxResults;i++){
            searchRegex.lastIndex=0;
            const ms=[...lineArr[i].matchAll(searchRegex)];
            if(!ms.length) continue;
            const cs=Math.max(0,i-contextLines),ce=Math.min(lineArr.length-1,i+contextLines);
            fileMatches.push({lineNumber:i+1,lineContent:lineArr[i],contextLines:contextLines>0?lineArr.slice(cs,ce+1):undefined,matchPositions:ms.map(m=>({start:m.index||0,end:(m.index||0)+m[0].length,text:m[0]}))});
            totalMatches++;
          }
          if(fileMatches.length) results.push({filePath:path.relative(sandboxDir,fp),matches:fileMatches,totalMatches:fileMatches.length});
        } catch{continue;}
      }
      const formatted=groupByFile?results:results.flatMap(r=>r.matches.map(m=>({filePath:r.filePath,lineNumber:m.lineNumber,lineContent:m.lineContent,contextLines:m.contextLines,matchPositions:m.matchPositions})));
      return{success:true,content:formatted,message:`Found ${totalMatches} match(es) in ${results.length} file(s)`};
    }

    // ── execute_command ────────────────────────────────────────
    if (toolName==='execute_command') {
      const cmd=String(toolArgs.command||'').trim();

      // [SECURITY FIX] Allowlist approach — only permit safe, read-only or
      // build-tool commands. Everything else is blocked by default.
      // This prevents the agent from exfiltrating data, accessing VPS secrets,
      // spawning servers, or abusing the Docker socket mount.

      // Commands that are explicitly allowed (must start with one of these)
      const ALLOWED_PREFIXES = [
        'npm test','npm run test','npm run lint','npm run build','npm run check',
        'npx tsc','npx eslint','npx prettier',
        'node -e ','node --eval ',
        'python -m pytest','python -m unittest','python -c ',
        'pytest','jest','mocha',
        'git diff','git log','git status','git show','git branch',
        'ls ','ls','cat ','head ','tail ','grep ','find ','wc ',
        'echo ','pwd','which ','type ',
      ];

      // Commands that are always blocked regardless of allowlist
      const BLOCKED_PATTERNS = [
        // Network exfiltration
        'curl ','wget ','fetch ','nc ','netcat','ncat','ssh ','scp ','sftp ',
        'ftp ','telnet ','nmap ','ping ','dig ','nslookup ',
        // Secrets / sensitive files
        '/etc/','~/.ssh','~/.aws','~/.env','/.env','/proc/','id_rsa',
        'cat /','less /','more /','head /','tail /',
        // Docker abuse
        'docker ','kubectl ','helm ',
        // Destructive
        'rm -','rmdir','shred','mkfs','dd if','truncate',
        // Privilege escalation
        'sudo ','su ','chmod ','chown ','chgrp ',
        // Server spawning
        'npm start','node server','nodemon','forever ','pm2 ',
        'python -m http','flask run','rails s','uvicorn','gunicorn',
        // Shell tricks
        '$(','`','&&','||',';','|','>>','2>','>',
        'eval ','exec ','source ','bash ','sh ','zsh ','fish ',
        // Env / credential access
        'printenv','env ','export ','set |',
        'process.env','dotenv',
      ];

      const cmdLower = cmd.toLowerCase();

      // Check blocked patterns first — these override everything
      const blockedMatch = BLOCKED_PATTERNS.find(b => cmdLower.includes(b.toLowerCase()));
      if (blockedMatch) {
        return { success: false, error: `Command blocked for security reasons. Use file tools (read_file, edit_file, create_file) to make changes instead.` };
      }

      // Check allowlist — command must start with an allowed prefix
      const allowed = ALLOWED_PREFIXES.some(p => cmdLower.startsWith(p.toLowerCase()));
      if (!allowed) {
        return { success: false, error: `Command not in allowlist: "${cmd.slice(0,80)}". Only test runners, linters, git diff/log/status, and safe read commands are permitted.` };
      }

      // Additional safety: ensure cwd is within sandboxDir
      const cwd = toolArgs.working_directory ? guardPath(toolArgs.working_directory) : sandboxDir;

      // Cap timeout at 30s
      const timeout = Math.min(toolArgs.timeout || 15000, 30000);

      try {
        const { stdout, stderr } = await execAsync(cmd, {
          cwd, timeout,
          // [SECURITY] Strip sensitive env vars before passing to child process
          env: {
            PATH: process.env.PATH,
            HOME: sandboxDir,
            NODE_ENV: 'test',
            CI: 'true',
          }
        });
        return { success: true, content: `stdout: ${stdout.slice(0,3000)}\nstderr: ${stderr.slice(0,500)}`, message: 'Command executed successfully' };
      } catch(e) {
        if (e.killed || e.signal === 'SIGTERM') return { success: false, error: 'Error: Command timed out' };
        return { success: false, error: `Command failed: ${String(e.stderr||e.message||e).slice(0,500)}` };
      }
    }

    // ── create_tasks ───────────────────────────────────────────
    if (toolName==='create_tasks') {
      const tasks=(toolArgs.tasks||[]).map((t,i)=>{
        if(!t.id||!t.description) throw new Error(`Task ${i} missing required fields`);
        return{id:String(t.id),description:t.description,status:t.status||'pending',notes:t.notes||''};
      });
      taskState.taskList={user_query:toolArgs.user_query||'',tasks,created_at:new Date().toISOString()};
      return{success:true,content:JSON.parse(JSON.stringify(taskState.taskList)),message:`Created task list with ${tasks.length} tasks`};
    }

    // ── update_tasks ───────────────────────────────────────────
    if (toolName==='update_tasks') {
      if(!taskState.taskList) return{success:false,error:'Error: No task list. Call create_tasks first.'};
      for(const upd of(toolArgs.task_updates||[])){
        const task=taskState.taskList.tasks.find(t=>t.id===String(upd.id));
        if(!task) return{success:false,error:`Error: Task '${upd.id}' not found`};
        task.status=upd.status; if(upd.notes) task.notes=upd.notes;
        task.updated_at=new Date().toISOString();
      }
      return{success:true,content:JSON.parse(JSON.stringify(taskState.taskList)),message:`Updated ${toolArgs.task_updates.length} task(s)`};
    }

    return{success:false,error:`Unknown tool: ${toolName}`};
  } catch(e) {
    return{success:false,error:`Tool execution error: ${e.message}`};
  }
}

// ── GitHub helpers ────────────────────────────────────────────────

// [FIX] Normalize a repo reference to a bare "owner/repo" slug. The frontend
// may send a full URL ("https://github.com/owner/repo", optionally with .git
// or a trailing slash) OR an already-clean slug. Feeding a full URL into the
// clone/push/PR builders produced a malformed remote like
// "github.com/https://github.com/owner/repo.git" (note the doubled host) which
// made git fail. Strip protocol, host, .git suffix, and any stray whitespace
// so every downstream consumer gets a consistent "owner/repo".
function normalizeRepoSlug(ref) {
  if (!ref) return '';
  let s = String(ref).trim();
  // Strip a full GitHub URL down to its path (handles http/https, with or without www)
  s = s.replace(/^https?:\/\/(www\.)?github\.com\//i, '');
  // Strip any other leading protocol://host/ just in case
  s = s.replace(/^[a-z]+:\/\/[^/]+\//i, '');
  // Strip git@github.com: SSH-style prefix
  s = s.replace(/^git@github\.com:/i, '');
  // Drop trailing .git and any trailing slashes
  s = s.replace(/\.git$/i, '').replace(/\/+$/,'');
  // Collapse to just owner/repo (ignore extra path segments like /tree/main)
  const parts = s.split('/').filter(Boolean);
  if (parts.length >= 2) return `${parts[0]}/${parts[1]}`;
  return s;
}

async function cloneUserRepo(repoSlug, githubToken, tmpDir) {
  const slug = normalizeRepoSlug(repoSlug);
  const cloneUrl = `https://x-access-token:${githubToken}@github.com/${slug}.git`;
  await execAsync(`git clone --depth=1 "${cloneUrl}" "${tmpDir}" 2>&1`, { timeout: 90000 });
}

async function createGithubPR({ repoSlug, branchName, githubToken, title, body }) {
  const headers = { Authorization: `Bearer ${githubToken}`, 'Content-Type': 'application/json', 'User-Agent': 'JoyTree-AI-Agent/1.0' };
  for (const base of ['main','master','develop']) {
    const r = await fetch(`https://api.github.com/repos/${repoSlug}/pulls`, {
      method: 'POST', headers,
      body: JSON.stringify({ title, body, head: branchName, base, draft: false }),
      signal: AbortSignal.timeout(20000)
    });
    if (r.ok) return r.json();
    const err = await r.json().catch(() => ({}));
    if (!String(err?.message||'').toLowerCase().includes('base')) break;
  }
  throw new Error('GitHub PR creation failed — check repo permissions and branch name.');
}

// ── Agent system prompt ───────────────────────────────────────────
function buildAgentSystemPrompt(provider, repoSlug, projectName, deploymentId, tmpDir) {
  const modelLabel = getAgentProviderLabel(provider);
  return `You are JoyTree AI, an expert autonomous coding agent powered by ${modelLabel}, deployed on the Joytree platform (joytree.site).

You have been given access to a cloned GitHub repository at: ${tmpDir}
Repository: ${repoSlug}
Project: ${projectName || 'unknown'}
Deployment ID: ${deploymentId || 'unknown'}

CRITICAL RULES:
- For ANY fix request, you MUST use tools to read and edit actual files. NEVER provide text-only responses.
- Start with list_files to understand the project, then search_files to locate the error.
- Use create_tasks at the start to plan your work. Update tasks as you progress.

FILE OPERATION RULES:
- ALWAYS read_file BEFORE edit_file — the validator will reject edits on unread files.
- Check if a file exists with list_files or read_file before creating it.
- For existing files: read_file → edit_file. For new files: list_files check → create_file.

COMMAND SAFETY:
- Only execute_command for things that exit quickly (npm test, ls, git diff, node -e "...", etc.)
- NEVER: npm start, node server.js, python app.py, flask run, nodemon — these run forever.
- Use command_type: "bash" for shell commands.

SEARCH STRATEGY:
- search_files with context_lines: 3 to see surrounding code context.
- search_files with pattern_type: "regex" for complex patterns.

WORKFLOW for fixing a deployment error:
1. create_tasks — plan the fix steps
2. list_files — understand the project structure
3. search_files — locate where the error originates
4. read_file — read affected files fully
5. edit_file or create_file — apply precise, minimal fixes
6. execute_command — run tests if available (npm test, python -m pytest, etc.)
7. update_tasks — mark each step complete
8. Summarize exactly what you changed and why

All file paths must be absolute starting with: ${tmpDir}`;
}

// ── Main agent runner ─────────────────────────────────────────────
// ── Shared finalizer: commit+PR (GitHub) or zip-for-download (upload) ──
// Used by both the initial run and a resumed run so the logic stays identical.
async function finalizeAgentRun(session, push, { tmpDir, provider, isUpload, ctx, finalSummary, branchName }) {
  const { repoSlug, projectName, deploymentId, githubToken, errorText, uploadProjectId, userId } = ctx;
  const sessionId = [...agentSessions.entries()].find(([,s]) => s === session)?.[0] || session._id || '';

  if (isUpload) {
    push('status', { text: 'Packaging your fixed project…', phase: 'commit' });
    const origDir = session._uploadFilesDir || path.join(UPLOADS_DIR, userId, uploadProjectId, 'files');
    let changedCount = 0;
    try {
      const { stdout: diffOut } = await execAsync(`diff -rq "${origDir}" "${tmpDir}" 2>/dev/null | head -200`, { timeout: 30000 }).catch(() => ({ stdout: '' }));
      changedCount = diffOut.split('\n').filter(Boolean).length;
    } catch (_) {}
    const hasChanges = changedCount > 0 || (Array.isArray(session._fileChanges) && session._fileChanges.length > 0);
    if (hasChanges) {
      const zipDir = path.join(UPLOADS_DIR, userId, '_ai_fixes');
      fs.mkdirSync(zipDir, { recursive: true });
      const zipName = `joytree-ai-fix-${(projectName || uploadProjectId || 'project').replace(/[^a-zA-Z0-9_-]/g,'_')}-${String(sessionId).slice(-6)}.zip`;
      const zipPath = path.join(zipDir, zipName);
      await execAsync(`cd "${tmpDir}" && zip -r -q "${zipPath}" . -x '*.git*' -x '*/node_modules/*' -x '*/dist/*' 2>&1`, { timeout: 120000 });
      session.zipPath = zipPath; session.zipName = zipName; session.status = 'done';
      push('fix_zip_ready', {
        downloadUrl: `/api/ai/agent/download/${sessionId}`, zipName,
        summary: finalSummary, subdomain: session.uploadProjectId || '', projectName: projectName || '',
        files: Array.isArray(session._fileChanges) ? session._fileChanges : [],
        provider, providerLabel: getAgentProviderLabel(provider)
      });
      push('status', { text: 'Done! Your fixed project is ready to download ✓', phase: 'done' });
    } else {
      session.status = 'done_no_changes';
      push('no_changes', { summary: finalSummary });
      push('status', { text: 'Analysis complete — no file changes needed.', phase: 'done' });
    }
  } else {
    push('status', { text: 'Checking for file changes…', phase: 'commit' });
    const { stdout: diffStat }  = await execAsync(`cd "${tmpDir}" && git diff --stat HEAD 2>&1`).catch(() => ({ stdout: '' }));
    const { stdout: statusOut } = await execAsync(`cd "${tmpDir}" && git status --short 2>&1`).catch(() => ({ stdout: '' }));
    const hasChanges = (diffStat.trim() || statusOut.trim()).length > 0;
    if (hasChanges) {
      push('status', { text: 'Committing fixes…', phase: 'commit' });
      const commitMsg = `fix: JoyTree AI (${getAgentProviderLabel(provider)}) [${deploymentId ? deploymentId.slice(-8) : 'manual'}] - ${finalSummary.slice(0, 200).replace(/"/g, "'").replace(/\n/g, ' ')}`;
      await execAsync(`cd "${tmpDir}" && git add -A && git commit -m "${commitMsg}" 2>&1`, { timeout: 20000 });
      push('status', { text: 'Pushing branch to GitHub…', phase: 'push' });
      const cloneUrl = `https://x-access-token:${githubToken}@github.com/${repoSlug}.git`;
      await execAsync(`cd "${tmpDir}" && git remote set-url origin "${cloneUrl}" && git push origin "${branchName}" 2>&1`, { timeout: 30000 });
      push('status', { text: 'Creating Pull Request…', phase: 'pr' });
      const prData = await createGithubPR({
        repoSlug, branchName, githubToken,
        title: `🤖 JoyTree AI: Auto-fix${projectName ? ' for ' + projectName : ''}`,
        body: [
          '## JoyTree AI Agent Auto-Fix',
          `> Powered by **${getAgentProviderLabel(provider)}** via [JoyTree](https://joytree.site)`,
          '', '### Error', '```', (errorText || 'See deployment logs').slice(0, 1500), '```',
          '', '### What Changed', finalSummary.slice(0, 2000),
          '', '### Diff Stats', '```', diffStat.trim(), '```',
          '', `---\n*Generated by JoyTree AI Agent · ${getAgentProviderLabel(provider)}*`
        ].join('\n')
      });
      session.prUrl = prData.html_url; session.prNumber = prData.number; session.status = 'done';
      push('pr_created', { url: prData.html_url, branch: branchName, number: prData.number, diffStat: diffStat.trim(), title: prData.title, provider, providerLabel: getAgentProviderLabel(provider) });
      push('status', { text: 'Done! Pull Request created ✓', phase: 'done' });
    } else {
      session.status = 'done_no_changes';
      push('no_changes', { summary: finalSummary });
      push('status', { text: 'Analysis complete — no file changes needed.', phase: 'done' });
    }
  }
}

async function runAgentSession(sessionId, { repoSlug, githubToken, errorText, deploymentId, projectName, userPrompt, mode, preferredProvider, analysisContext, sourceType, uploadProjectId, uploadSubdomain, userId, userKeys }) {
  const isUpload = sourceType === 'upload';
  // [FIX] Normalize once up front — the frontend may pass a full GitHub URL.
  // (Skipped for uploads, which have no GitHub slug.)
  if (!isUpload) repoSlug = normalizeRepoSlug(repoSlug);

  const session = agentSessions.get(sessionId);
  const push = (type, payload) => session.events.push({ type, payload, ts: Date.now() });

  const readFilesTracker = new Set();
  const taskState = { taskList: null };
  // Persist for resume after a paused (rate-limited) run.
  session._readFilesTracker = readFilesTracker;
  session._taskState = taskState;
  let tmpDir = null;

  // Resolve provider
  const provider = getAgentProvider(preferredProvider, userKeys);
  if (!provider) {
    push('error', { message: 'No AI provider configured. Add ANTHROPIC_API_KEY, GROQ_API_KEY, or XAI_API_KEY to your .env file.' });
    session.status = 'error';
    return;
  }

  session.provider = provider;
  push('provider', { provider, label: getAgentProviderLabel(provider) });

  try {
    const branchName = session.branchName;
    tmpDir = path.join(os.tmpdir(), `joytree-agent-${sessionId}`);
    session.tmpDir = tmpDir;
    fs.mkdirSync(tmpDir, { recursive: true });

    if (isUpload) {
      // ── Upload project: copy stored files into the temp workspace ──
      push('status', { text: 'Loading your uploaded files…', phase: 'clone' });
      let projDir  = path.join(UPLOADS_DIR, userId, uploadProjectId);
      let filesDir = path.join(projDir, 'files');

      // [FIX] The stored files are keyed by the upload-time storage id, which may
      // differ from what the client sent (older projects passed the subdomain).
      // If the direct path misses, search this user's upload dirs for a match.
      if (!fs.existsSync(filesDir) && !fs.existsSync(projDir)) {
        try {
          const userRoot = path.join(UPLOADS_DIR, userId);
          if (fs.existsSync(userRoot)) {
            const candidates = fs.readdirSync(userRoot)
              .filter(d => d !== '_ai_fixes')
              .map(d => ({ id: d, dir: path.join(userRoot, d), files: path.join(userRoot, d, 'files') }))
              .filter(c => { try { return fs.statSync(c.dir).isDirectory(); } catch (_) { return false; } });
            // Prefer an exact id/subdomain match, then any dir that actually has files/.
            let match = candidates.find(c => c.id === uploadProjectId || c.id === uploadSubdomain);
            if (!match) match = candidates.find(c => fs.existsSync(c.files));
            // If multiple, pick the most recently modified.
            if (!match && candidates.length) {
              match = candidates.sort((a, b) => {
                let am = 0, bm = 0; try { am = fs.statSync(a.dir).mtimeMs; } catch(_){} try { bm = fs.statSync(b.dir).mtimeMs; } catch(_){}
                return bm - am;
              })[0];
            }
            if (match) { projDir = match.dir; filesDir = match.files; }
          }
        } catch (_) {}
      }

      // If still no files/ but an archive exists, extract it now.
      if (!fs.existsSync(filesDir) && fs.existsSync(projDir)) {
        let archivePath = null;
        try {
          for (const f of fs.readdirSync(projDir)) {
            if (/\.(zip|tar\.gz|tgz)$/i.test(f)) { archivePath = path.join(projDir, f); break; }
          }
        } catch (_) {}
        if (archivePath) {
          push('status', { text: 'Extracting your project archive…', phase: 'clone' });
          try { await extractUploadedArchive(archivePath, filesDir); } catch (ex) {
            push('error', { message: 'Could not extract your uploaded archive. Try re-uploading the project, then run the fix again.' });
            session.status = 'error'; return;
          }
        }
      }
      if (!fs.existsSync(filesDir)) {
        push('error', { message: 'Could not find your uploaded project files on the server. Re-deploy this project via Upload once, then run the fix again.' });
        session.status = 'error';
        return;
      }
      // Remember the resolved dir for the finalizer (change-detection + zip).
      session._uploadFilesDir = filesDir;
      // Copy the stored files into the sandbox.
      await execAsync(`cp -a "${filesDir}/." "${tmpDir}/" 2>&1`, { timeout: 60000 });
      session.repoCloned = true;
      push('status', { text: `Files loaded. Starting ${getAgentProviderLabel(provider)} agent…`, phase: 'analyze' });
    } else {
      // ── GitHub project: clone the repo ──
      push('status', { text: 'Cloning repository…', phase: 'clone' });
      await cloneUserRepo(repoSlug, githubToken, tmpDir);
      session.repoCloned = true;
      await execAsync(`cd "${tmpDir}" && git checkout -b "${branchName}" 2>&1`, { timeout: 15000 });
      await execAsync(`cd "${tmpDir}" && git config user.email "ai@joytree.site" && git config user.name "JoyTree AI Agent"`, { timeout: 10000 });
      push('status', { text: `Repository cloned. Starting ${getAgentProviderLabel(provider)} agent…`, phase: 'analyze' });
    }

    // Initial file tree for context
    const { stdout: rawTree } = await execAsync(
      `find "${tmpDir}" -not -path '*/node_modules/*' -not -path '*/.git/*' -not -path '*/dist/*' -not -path '*/.next/*' -not -path '*/coverage/*' | head -120 2>&1`,
      { timeout: 10000 }
    ).catch(() => ({ stdout: '' }));
    const fileTree = rawTree.split('\n').map(l => l.replace(tmpDir + '/', '')).filter(Boolean).join('\n');
    push('file_tree', { tree: fileTree });

    // Build messages (common format — callAgentLLM converts for Claude internally)
    const systemPrompt = buildAgentSystemPrompt(provider, repoSlug, projectName, deploymentId, tmpDir);
    const messages = [
      { role: 'system', content: systemPrompt },
      { role: 'system', content: `Repository file tree:\n${fileTree.slice(0, 8000)}` },
      // [FIX] If this session was triggered from the AI Analysis page, inject the
      // full Groq pre-analysis so Claude doesn't duplicate work and starts with
      // the root cause, affected files, and suggested fix steps already known.
      ...(analysisContext ? [{
        role: 'system',
        content: `== Pre-Analysis from diagnostic scan ==\nThe following analysis was already performed on the error logs before this session started. Use it as your starting context — do not re-derive what is already known here:\n\n${String(analysisContext).slice(0, 4000)}`
      }] : []),
      {
        role: 'user',
        content: userPrompt
          || (errorText
            ? `Fix this deployment error:\n\n${String(errorText).slice(0, 6000)}`
            : `Analyze the repository at ${tmpDir} for deployment issues and fix them.`)
      }
    ];
    // Persist the conversation + context on the session so a paused run
    // (rate limit / 413) can RESUME from exactly here instead of restarting.
    session.messages = messages;
    session.runCtx = { provider, repoSlug, projectName, deploymentId, githubToken, errorText, sourceType, uploadProjectId, userId, branchName: session.branchName, userKeys };
    session.iteration = 0;

    push('status', { text: `${getAgentProviderLabel(provider)} is thinking…`, phase: 'think' });

    // ── Agentic loop (50 iterations — same as groq-code-cli agent.ts) ──
    const MAX_ITERATIONS = 50;
    const MAX_WALL_MS    = 10 * 60 * 1000; // 10 minute hard wall-clock limit
    const loopStart      = Date.now();
    let iteration = 0;
    let finalSummary = '';

    while (iteration < MAX_ITERATIONS) {
      if (Date.now() - loopStart > MAX_WALL_MS) {
        push('error', { message: 'Agent timed out after 10 minutes. Partial changes (if any) will still be committed.' });
        break;
      }
      iteration++;
      session.iteration = iteration;

      let groqData;
      try {
        groqData = await callAgentLLM({
          provider,
          messages,
          tools: GROQ_AGENT_TOOL_SCHEMAS,
          temperature: 0.2,
          maxTokens: 8000,
          userKeys
        });
      } catch (llmErr) {
        const m = llmErr.message || String(llmErr);
        // Rate limit / oversized request → PAUSE the run, keep the workspace +
        // conversation alive, and let the user resume from this exact point.
        if (m.startsWith('__JAI_RATE_LIMIT__:')) {
          const parts = m.split(':');
          const retrySeconds = parseInt(parts[1], 10) || 60;
          const friendly = parts.slice(2).join(':');
          session.status = 'paused';
          session.paused = true;
          push('rate_limit', { message: friendly, retryAfterSeconds: retrySeconds, resumable: true });
          return; // keep tmpDir + session.messages for resume
        }
        if (m.startsWith('__JAI_NO_CREDITS__:')) {
          session.status = 'error';
          push('error', { message: m.slice('__JAI_NO_CREDITS__:'.length) });
          return;
        }
        throw llmErr;
      }

      const msg = groqData.choices?.[0]?.message;
      if (!msg) throw new Error('Empty response from AI provider');

      // Extract reasoning (DeepSeek/QwQ thinking models)
      const reasoning = msg.reasoning;
      if (msg.content && msg.tool_calls) push('thinking', { text: msg.content, reasoning: reasoning || null });

      // Add to history
      messages.push(msg);

      // No tool calls = agent is done
      if (!msg.tool_calls || msg.tool_calls.length === 0) {
        finalSummary = msg.content || 'Agent completed.';
        push('agent_message', { text: finalSummary });
        break;
      }

      // Execute each tool call
      for (const tc of msg.tool_calls) {
        // Strip hallucinated prefix (groq-code-cli does this too)
        let toolName = (tc.function?.name || '').replace(/^repo_browser\./, '');
        let toolArgs = {};
        try { toolArgs = JSON.parse(tc.function?.arguments || '{}'); }
        catch (e) {
          messages.push({ role: 'tool', tool_call_id: tc.id, content: JSON.stringify({ success: false, error: `Malformed arguments: ${e}. Use shorter content.` }) });
          continue;
        }

        push('tool_call', { tool: toolName, args: toolArgs });

        const result = await executeAgentTool(toolName, toolArgs, tmpDir, readFilesTracker, taskState);

        push('tool_result', { tool: toolName, success: result.success, message: result.message || '', error: result.error || '' });

        // Rich events for UI panels
        if (result.success && (toolName === 'create_file' || toolName === 'edit_file')) {
          const fcEntry = {
            action: toolName === 'create_file' ? 'created' : 'edited',
            file: String(toolArgs.file_path || '').replace(tmpDir + '/', ''),
            before: toolName === 'edit_file' ? String(toolArgs.old_text || '').slice(0, 500) : '',
            after:  toolName === 'edit_file' ? String(toolArgs.new_text || '').slice(0, 500) : String(toolArgs.content || '').slice(0, 500)
          };
          if (!session._fileChanges) session._fileChanges = [];
          session._fileChanges.push({ action: fcEntry.action, file: fcEntry.file });
          push('file_change', fcEntry);
        }
        if (result.success && toolName === 'delete_file') {
          if (!session._fileChanges) session._fileChanges = [];
          session._fileChanges.push({ action: 'deleted', file: String(toolArgs.file_path || '').replace(tmpDir + '/', '') });
          push('file_change', { action: 'deleted', file: String(toolArgs.file_path || '').replace(tmpDir + '/', ''), before: '', after: '' });
        }
        if (result.success && (toolName === 'create_tasks' || toolName === 'update_tasks')) {
          push('task_update', { taskList: result.content });
        }

        messages.push({ role: 'tool', tool_call_id: tc.id, content: JSON.stringify(result) });
      }
    }

    await finalizeAgentRun(session, push, {
      tmpDir, provider, isUpload,
      ctx: { repoSlug, projectName, deploymentId, githubToken, errorText, uploadProjectId, userId },
      finalSummary, branchName
    });

  } catch (e) {
    session.status = 'error';
    const raw = e.message || String(e);
    // Rate-limit errors carry a marker "__JAI_RATE_LIMIT__:<seconds>:<message>"
    // built in callAgentLLM — translate it into a dedicated, user-friendly event
    // so the UI can show a clean "JoyTree AI usage limit" notice + wait timer,
    // instead of dumping raw Groq/TPM text.
    if (raw.startsWith('__JAI_RATE_LIMIT__:')) {
      const parts = raw.split(':');
      const retrySeconds = parseInt(parts[1], 10) || 60;
      const friendly = parts.slice(2).join(':');
      push('rate_limit', { message: friendly, retryAfterSeconds: retrySeconds });
    } else if (raw.startsWith('__JAI_NO_CREDITS__:')) {
      const friendly = raw.slice('__JAI_NO_CREDITS__:'.length);
      push('error', { message: friendly });
    } else {
      push('error', { message: raw });
    }
  } finally {
    // [SECURITY] Only delete the tmp dir if it's actually inside os.tmpdir()
    // and the path contains our known prefix — prevents path traversal if
    // sessionId were somehow manipulated.
    // [RESUME] Keep the workspace alive while a run is PAUSED (rate limit) so
    // the user can continue from where it stopped. It's cleaned up on the next
    // successful finish, or by the 2-hour session sweeper.
    // [FOLLOW-UP] Keep the workspace alive after success/pause so the user can
    // send follow-up prompts (e.g. "improve the UI") that continue on the SAME
    // files + conversation instead of re-cloning. Only delete on hard error.
    // The 2-hour session sweeper cleans up abandoned workspaces.
    if (session.status === 'error' && tmpDir && tmpDir.startsWith(path.join(os.tmpdir(), 'joytree-agent-'))) {
      execAsync(`rm -rf "${tmpDir}"`).catch(() => {});
    }
  }
}

// ── Resume a paused agent run from where it left off ──────────────
async function resumeAgentSession(sessionId) {
  const session = agentSessions.get(sessionId);
  if (!session) throw new Error('Session not found or expired.');
  // Works for BOTH a paused (rate-limited) resume AND a follow-up turn — both
  // need an existing conversation, run context, and live workspace.
  if (!session.messages || !session.runCtx) throw new Error('This session cannot be continued.');
  if (!session.tmpDir || !fs.existsSync(session.tmpDir)) throw new Error('The workspace for this session is no longer available — please start a new fix.');

  session.paused = false;
  session.status = 'running';
  const push = (type, payload) => session.events.push({ type, payload, ts: Date.now() });
  const ctx = session.runCtx;
  const tmpDir = session.tmpDir;
  const provider = ctx.provider;
  const isUpload = ctx.sourceType === 'upload';
  const messages = session.messages;
  const branchName = ctx.branchName;

  try {
    push('status', { text: `Resuming ${getAgentProviderLabel(provider)}…`, phase: 'think' });

    const MAX_ITERATIONS = 50;
    const MAX_WALL_MS = 10 * 60 * 1000;
    const loopStart = Date.now();
    let iteration = session.iteration || 0;
    let finalSummary = '';

    while (iteration < MAX_ITERATIONS) {
      if (Date.now() - loopStart > MAX_WALL_MS) { push('error', { message: 'Agent timed out after 10 minutes.' }); break; }
      iteration++; session.iteration = iteration;

      let groqData;
      try {
        groqData = await callAgentLLM({ provider, messages, tools: GROQ_AGENT_TOOL_SCHEMAS, temperature: 0.2, maxTokens: 8000, userKeys: (ctx && ctx.userKeys) || {} });
      } catch (llmErr) {
        const m = llmErr.message || String(llmErr);
        if (m.startsWith('__JAI_RATE_LIMIT__:')) {
          const parts = m.split(':');
          session.status = 'paused'; session.paused = true;
          push('rate_limit', { message: parts.slice(2).join(':'), retryAfterSeconds: parseInt(parts[1],10)||60, resumable: true });
          return;
        }
        if (m.startsWith('__JAI_NO_CREDITS__:')) { session.status = 'error'; push('error', { message: m.slice('__JAI_NO_CREDITS__:'.length) }); return; }
        throw llmErr;
      }

      const msg = groqData.choices?.[0]?.message;
      if (!msg) throw new Error('Empty response from AI provider');
      const reasoning = msg.reasoning;
      if (msg.content && msg.tool_calls) push('thinking', { text: msg.content, reasoning: reasoning || null });
      messages.push(msg);

      if (!msg.tool_calls || msg.tool_calls.length === 0) {
        finalSummary = msg.content || 'Done.';
        break;
      }
      push('agent_message', { text: msg.content || '' });

      for (const tc of msg.tool_calls) {
        let toolArgs;
        try { toolArgs = JSON.parse(tc.function.arguments || '{}'); }
        catch (e) { messages.push({ role: 'tool', tool_call_id: tc.id, content: JSON.stringify({ success: false, error: `Malformed arguments: ${e}. Use shorter content.` }) }); continue; }
        const toolName = tc.function.name;
        push('tool_call', { tool: toolName, args: toolArgs });
        const result = await executeAgentTool(toolName, toolArgs, tmpDir, session._readFilesTracker || new Set(), session._taskState || { taskList: null });
        if (result.success && (toolName === 'create_file' || toolName === 'edit_file')) {
          const fc = { action: toolName === 'create_file' ? 'created' : 'edited', file: String(toolArgs.file_path||'').replace(tmpDir+'/',''), before: toolName==='edit_file'?String(toolArgs.old_text||'').slice(0,500):'', after: toolName==='edit_file'?String(toolArgs.new_text||'').slice(0,500):String(toolArgs.content||'').slice(0,500) };
          if (!session._fileChanges) session._fileChanges = [];
          session._fileChanges.push({ action: fc.action, file: fc.file });
          push('file_change', fc);
        }
        if (result.success && toolName === 'delete_file') {
          if (!session._fileChanges) session._fileChanges = [];
          session._fileChanges.push({ action: 'deleted', file: String(toolArgs.file_path||'').replace(tmpDir+'/','') });
          push('file_change', { action: 'deleted', file: String(toolArgs.file_path||'').replace(tmpDir+'/',''), before:'', after:'' });
        }
        messages.push({ role: 'tool', tool_call_id: tc.id, content: JSON.stringify(result) });
      }
    }

    // Finalize — same commit/PR or zip logic via the shared finalizer
    await finalizeAgentRun(session, push, { tmpDir, provider, isUpload, ctx, finalSummary, branchName });
  } catch (e) {
    session.status = 'error';
    const raw = e.message || String(e);
    if (raw.startsWith('__JAI_RATE_LIMIT__:')) { const p = raw.split(':'); session.paused = true; session.status='paused'; push('rate_limit', { message: p.slice(2).join(':'), retryAfterSeconds: parseInt(p[1],10)||60, resumable: true }); return; }
    push('error', { message: raw });
  } finally {
    // [FOLLOW-UP] Keep the workspace alive after success/pause so the user can
    // send follow-up prompts (e.g. "improve the UI") that continue on the SAME
    // files + conversation instead of re-cloning. Only delete on hard error.
    // The 2-hour session sweeper cleans up abandoned workspaces.
    if (session.status === 'error' && tmpDir && tmpDir.startsWith(path.join(os.tmpdir(), 'joytree-agent-'))) {
      execAsync(`rm -rf "${tmpDir}"`).catch(() => {});
    }
  }
}

// ── API Routes ────────────────────────────────────────────────────

// Read a user's own (BYOK) API keys from their workspace settings.
async function getUserApiKeys(user) {
  try {
    const ws = await readWorkspaceFromFirebase(user).catch(() => null);
    const k = ws && ws.settings && ws.settings.aiKeys;
    if (k && typeof k === 'object') {
      return {
        claude: String(k.claude || '').trim(),
        groq:   String(k.groq   || '').trim(),
        xai:    String(k.xai    || '').trim(),
        openai: String(k.openai || '').trim(),
      };
    }
  } catch (_) {}
  return { claude: '', groq: '', xai: '', openai: '' };
}

// GET /api/ai/agent/providers — which providers are available
app.get('/api/ai/agent/providers', requireAuth, async (req, res) => {
  const uk = await getUserApiKeys(req.user).catch(() => ({}));
  const avail = (id, sharedKey) => !!((uk && uk[id]) || sharedKey);
  res.json({
    providers: [
      { id: 'groq',   label: `Llama (${GROQ_MODEL})`,       available: avail('groq', GROQ_API_KEY),      best: true,  byok: !!(uk && uk.groq),   description: 'Free & fast — great for most fixes. Recommended default.' },
      { id: 'openai', label: `GPT (${OPENAI_MODEL})`,       available: avail('openai', OPENAI_API_KEY),  best: false, byok: !!(uk && uk.openai), description: 'OpenAI GPT — solid all-round coding model.' },
      { id: 'claude', label: `Claude (${ANTHROPIC_MODEL})`, available: avail('claude', ANTHROPIC_API_KEY), best: false, byok: !!(uk && uk.claude), description: 'Premium — best for complex fixes.' },
      { id: 'xai',    label: `Grok (${XAI_MODEL})`,         available: avail('xai', XAI_API_KEY),        best: false, byok: !!(uk && uk.xai),    description: 'xAI Grok — strong reasoning.' }
    ],
    recommended: getAgentProvider(null, uk)
  });
});

// GET /api/ai/keys — return masked status of the user's saved BYOK keys
app.get('/api/ai/keys', requireAuth, async (req, res) => {
  const uk = await getUserApiKeys(req.user).catch(() => ({}));
  const mask = (v) => v ? (String(v).slice(0, 4) + '••••' + String(v).slice(-4)) : '';
  res.json({
    keys: {
      claude: { set: !!uk.claude, masked: mask(uk.claude) },
      groq:   { set: !!uk.groq,   masked: mask(uk.groq) },
      xai:    { set: !!uk.xai,    masked: mask(uk.xai) },
      openai: { set: !!uk.openai, masked: mask(uk.openai) },
    }
  });
});

// POST /api/ai/keys — save/update/remove the user's own API keys
// Body: { provider: 'claude'|'groq'|'xai'|'openai', apiKey: '...'|'' }
// Stored in the workspace settings under aiKeys, persisted to Realtime DB.
app.post('/api/ai/keys', requireAuth, async (req, res) => {
  try {
    const { provider, apiKey } = req.body || {};
    const allowed = ['claude', 'groq', 'xai', 'openai'];
    if (!allowed.includes(provider)) return res.status(400).json({ error: 'Invalid provider.' });
    const key = String(apiKey || '').trim();
    // Light sanity check — not strict, just to catch obvious mistakes.
    if (key && key.length < 12) return res.status(400).json({ error: 'That doesn\'t look like a valid API key.' });

    const ws = await readWorkspaceFromFirebase(req.user).catch(() => null) || {};
    ws.settings = ws.settings && typeof ws.settings === 'object' ? ws.settings : {};
    ws.settings.aiKeys = ws.settings.aiKeys && typeof ws.settings.aiKeys === 'object' ? ws.settings.aiKeys : {};
    if (key) ws.settings.aiKeys[provider] = key;
    else delete ws.settings.aiKeys[provider];
    await writeWorkspaceToFirebase(req.user, ws).catch(() => {});
    res.json({ ok: true, provider, set: !!key });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ai/agent/start
app.post('/api/ai/agent/start', requireAuth, async (req, res) => {
  const { repoSlug, errorText, deploymentId, projectName, userPrompt, mode, preferredProvider, analysisContext,
          sourceType, uploadProjectId, uploadSubdomain } = req.body || {};
  const githubToken = req.user?.githubAccessToken || '';
  const isUpload = sourceType === 'upload';

  // BYOK: load the user's own API keys (if any) so they can use providers/models
  // with their own higher rate limits and billing.
  const userKeys = await getUserApiKeys(req.user).catch(() => ({}));

  const provider = getAgentProvider(preferredProvider, userKeys);
  if (!provider)    return res.status(503).json({ error: 'No AI provider available. Add your own API key in Settings, or ask the platform to configure one.' });

  // [PLAN GATE] Real-time Claude fixing on the SHARED key is a premium feature
  // on the Scale Max plan. BUT if the user brings their OWN Anthropic key, they
  // pay for it themselves — so allow it on any plan.
  if (provider === 'claude' && !(userKeys && userKeys.claude)) {
    const planKey = await getUserPlanKey(req.user).catch(() => 'free');
    if (planKey !== 'scale') {
      return res.status(403).json({ error: 'claude_locked', message: 'Real-time Claude fixing is included with the Scale Max plan. Upgrade to unlock it, add your own Anthropic API key in Settings, or use the free Llama model.' });
    }
  }

  // GitHub token only required for GitHub repos — upload fixes work on stored files.
  if (!isUpload && !githubToken) return res.status(400).json({ error: 'No GitHub token — please sign in with GitHub to use the AI Agent.' });
  if (!isUpload && !repoSlug)    return res.status(400).json({ error: 'repoSlug is required (e.g. username/my-app)' });
  if (isUpload && !uploadProjectId && !uploadSubdomain) return res.status(400).json({ error: 'uploadProjectId or uploadSubdomain is required for an upload fix.' });

  const userId = String(req.user?._id || req.user?.id || 'anon');
  if (mode === 'trial') {
    if (req.user.agentTrialUsed) return res.status(403).json({ error: 'trial_used', message: 'Your free trial has been used.' });
    try {
      req.user.agentTrialUsed = true;
      if (typeof localAuth !== 'undefined' && localAuth.users) {
        const u = localAuth.users.find(x => String(x.id||x._id||'') === userId);
        if (u) { u.agentTrialUsed = true; if (typeof saveLocalAuth === 'function') saveLocalAuth(); }
      }
    } catch (_) {}
  }

  const sessionId  = 'agt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const branchName = `joytree-ai-fix-${sessionId.slice(-10)}`;

  agentSessions.set(sessionId, {
    status: 'starting', provider,
    repoSlug, branchName, deploymentId, projectName,
    sourceType: isUpload ? 'upload' : 'github',
    uploadProjectId: uploadProjectId || uploadSubdomain || '',
    zipPath: null,
    userKeys,   // BYOK keys for this run (used by callAgentLLM)
    events: [], prUrl: null, prNumber: null,
    tmpDir: null, repoCloned: false,
    startedAt: new Date().toISOString()
  });

  res.json({ ok: true, sessionId, provider, providerLabel: getAgentProviderLabel(provider) });

  runAgentSession(sessionId, { repoSlug, githubToken, errorText, deploymentId, projectName, userPrompt, mode, preferredProvider, analysisContext,
                               sourceType: isUpload ? 'upload' : 'github', uploadProjectId: uploadProjectId || uploadSubdomain || '', uploadSubdomain: uploadSubdomain || '', userId, userKeys }).catch(e => {
    const s = agentSessions.get(sessionId);
    if (s) { s.status = 'error'; s.events.push({ type: 'error', payload: { message: e.message }, ts: Date.now() }); }
  });
});

// POST /api/ai/agent/resume/:sessionId — continue a paused (rate-limited) run
app.post('/api/ai/agent/resume/:sessionId', requireAuth, async (req, res) => {
  const session = agentSessions.get(req.params.sessionId);
  if (!session) return res.status(404).json({ error: 'Session expired — please start a new fix.' });
  if (!session.paused) return res.status(409).json({ error: 'This session is not paused.' });
  res.json({ ok: true, sessionId: req.params.sessionId });
  resumeAgentSession(req.params.sessionId).catch(e => {
    const s = agentSessions.get(req.params.sessionId);
    if (s) { s.status = 'error'; s.events.push({ type: 'error', payload: { message: e.message }, ts: Date.now() }); }
  });
});

// POST /api/ai/agent/followup/:sessionId — continue the SAME session with a new
// instruction (e.g. "improve the UI"), reusing the existing workspace + history
// so the agent builds on prior work instead of re-cloning from scratch.
app.post('/api/ai/agent/followup/:sessionId', requireAuth, async (req, res) => {
  const session = agentSessions.get(req.params.sessionId);
  const { userPrompt } = req.body || {};
  if (!session) return res.status(404).json({ error: 'expired' });          // tell client to start fresh
  if (!session.messages || !session.runCtx) return res.status(409).json({ error: 'not_followable' });
  if (!session.tmpDir || !fs.existsSync(session.tmpDir)) return res.status(410).json({ error: 'workspace_gone' });
  if (!userPrompt || !userPrompt.trim()) return res.status(400).json({ error: 'userPrompt required' });

  // Append the new instruction to the existing conversation.
  session.messages.push({ role: 'user', content: userPrompt });
  // Reset per-run state for the new turn (keep workspace + conversation).
  session.paused = false;
  session._fileChanges = [];
  session.iteration = 0;   // fresh iteration budget for this follow-up turn
  session.prUrl = null; session.prNumber = null; session.zipPath = null;
  session.status = 'running';

  res.json({ ok: true, sessionId: req.params.sessionId, eventIndex: session.events.length });

  resumeAgentSession(req.params.sessionId).catch(e => {
    const s = agentSessions.get(req.params.sessionId);
    if (s) { s.status = 'error'; s.events.push({ type: 'error', payload: { message: e.message }, ts: Date.now() }); }
  });
});

// GET /api/ai/agent/stream/:sessionId
app.get('/api/ai/agent/stream/:sessionId', requireAuth, (req, res) => {
  const session = agentSessions.get(req.params.sessionId);
  if (!session) return res.status(404).json({ error: 'Session not found' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  if (typeof res.flushHeaders === 'function') res.flushHeaders();

  let sentIdx = parseInt(req.query.from, 10) || 0;  // resume stream from this event index (avoids replaying on reconnect)
  if (sentIdx > session.events.length) sentIdx = session.events.length;
  let lastByteAt = Date.now();
  function flush() {
    while (sentIdx < session.events.length) {
      const ev = session.events[sentIdx++];
      sseWrite(res, ev.type, ev.payload);
      lastByteAt = Date.now();
    }
    if (['done','done_no_changes','error'].includes(session.status)) {
      sseWrite(res, 'done', { status: session.status, prUrl: session.prUrl || null });
      clearInterval(timer);
      res.end();
      return;
    }
    // [RESUME] When the run pauses on a rate limit, close the stream cleanly
    // (the rate_limit event has already been sent). The client shows the
    // countdown + Continue button; hitting Continue calls /resume which
    // re-runs the agent and the client re-opens this stream.
    if (session.status === 'paused') {
      sseWrite(res, 'paused', { status: 'paused' });
      clearInterval(timer);
      res.end();
      return;
    }
    // [FIX] Long agent steps (a single LLM call can take 45-90s) previously
    // left this connection completely silent in between events. Cloudflare's
    // idle-connection handling (and most reverse proxies/load balancers in
    // front of Node) can tear down a connection that's gone quiet for that
    // long, which surfaces to the client as a 520/524 even though the origin
    // process is still alive and working. A no-op SSE comment every few
    // seconds keeps real bytes flowing so nothing in between decides the
    // connection is dead.
    if (Date.now() - lastByteAt > 8000 && !res.writableEnded) {
      res.write(': ping\n\n');
      lastByteAt = Date.now();
    }
  }
  flush();
  const timer = setInterval(flush, 250);
  req.on('close', () => clearInterval(timer));
});

// GET /api/ai/agent/status/:sessionId
app.get('/api/ai/agent/status/:sessionId', requireAuth, (req, res) => {
  const session = agentSessions.get(req.params.sessionId);
  if (!session) return res.status(404).json({ error: 'not found' });
  res.json({ status: session.status, prUrl: session.prUrl || null, prNumber: session.prNumber || null, provider: session.provider, eventCount: session.events.length });
});

// GET /api/ai/agent/trial-status
app.get('/api/ai/agent/trial-status', requireAuth, (req, res) => {
  res.json({ trialUsed: !!req.user?.agentTrialUsed });
});

// GET /api/ai/agent/download/:sessionId — download the AI-fixed project zip (upload fixes)
app.get('/api/ai/agent/download/:sessionId', requireAuth, (req, res) => {
  const session = agentSessions.get(req.params.sessionId);
  if (!session || !session.zipPath) return res.status(404).json({ error: 'Fixed archive not found or expired. Re-run the fix to regenerate it.' });
  if (!fs.existsSync(session.zipPath)) return res.status(404).json({ error: 'Fixed archive no longer available. Re-run the fix to regenerate it.' });
  const fname = session.zipName || 'joytree-ai-fix.zip';
  res.download(session.zipPath, fname, (err) => {
    if (err && !res.headersSent) res.status(500).json({ error: 'Download failed.' });
  });
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
    await runDocker(`docker rm -f ${cname} 2>/dev/null || true`);
    const build = await runDocker(`docker build -t ${image} "${appDir}"`, 120000);
    if (!build.ok) return res.status(500).json({ error: build.stderr || 'docker build failed' });
    let run = null;
    let port = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      port = getOrAssignPort(subdomain);
      run = await runDocker(`docker run -d --name ${cname} --restart unless-stopped -p 127.0.0.1:${port}:3000 ${image}`);
      if (run.ok) break;
      const err = String(run.stderr || '').toLowerCase();
      const portBusy = err.includes('port is already allocated') || err.includes('bind for');
      if (!portBusy) break;
      clearAssignedPort(subdomain);
      await runDocker(`docker rm -f ${cname} 2>/dev/null || true`);
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
    try { await runDocker(`docker rm -f ${rec.dockerContainer} 2>/dev/null || true`); } catch {}
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
  free:    { memoryLimit: '870m',  cpuShares: 384,  memorySwap: '1g' },
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

// [FIX] async — du -sb recursively scans a user's entire site directory and
// previously ran via execSync on every deploy, blocking the proxy/event loop
// (and thus every tenant's traffic) for the duration of the scan.
async function getDiskUsedBytesForUser(userId) {
  try {
    const siteDir = process.env.SITES_DIR || '/var/www/user-sites';
    const userDir = require('path').join(siteDir, String(userId || ''));
    const out = (await execP(`du -sb "${userDir}" 2>/dev/null || echo 0`, { timeout: 10000 })).split(/\s+/)[0];
    return Number(out) || 0;
  } catch { return 0; }
}

async function checkDiskQuota(userId, planKey) {
  const limit = PLAN_DISK_LIMITS[String(planKey || 'free').toLowerCase()] ?? PLAN_DISK_LIMITS.free;
  if (!Number.isFinite(limit)) return { ok: true };
  const used = await getDiskUsedBytesForUser(userId);
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
  free:    { maxDatabases: 3,   maxDbMemoryBytes: 512  * 1024 * 1024,        maxApis: 5,   maxProjects: 5,   monthlyBuildSeconds: 300  },
  starter: { maxDatabases: 8,   maxDbMemoryBytes: 1    * 1024 * 1024 * 1024, maxApis: 20,  maxProjects: 10,  monthlyBuildSeconds: 1800 },
  pro:     { maxDatabases: 20,  maxDbMemoryBytes: 2    * 1024 * 1024 * 1024, maxApis: 60,  maxProjects: 20,  monthlyBuildSeconds: 5000 },
  growth:  { maxDatabases: 50,  maxDbMemoryBytes: 5    * 1024 * 1024 * 1024, maxApis: 150, maxProjects: 35,  monthlyBuildSeconds: 10000 },
  scale:   { maxDatabases: 200, maxDbMemoryBytes: 16   * 1024 * 1024 * 1024, maxApis: 500, maxProjects: 120, monthlyBuildSeconds: 60000 }
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
          startCmd, outputDir, workingDir, nodeVer, siteType, envVars,
          isDockerfileDeploy, isWorker, dockerfilePath, exposedPort } = req.body;
  const deploySource = (req.body?.source === 'auto' || req.body?.autoDeploy === true || req.headers['x-deployboard-deploy-source'] === 'auto') ? 'auto' : 'manual';
  const triggerSha = String(req.body?.triggerSha || req.headers['x-deployboard-trigger-sha'] || '').trim();

  if (!name || !subdomain || !repoUrl) {
    return res.status(400).json({ error: 'name, subdomain and repoUrl are required' });
  }

  const cleanSub = subdomain.toLowerCase()
    .replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  const cleanWorkingDir = sanitizeWorkingDir(workingDir);
  if (cleanWorkingDir === null) {
    return res.status(400).json({ error: 'Working directory must be a relative path inside the repository.' });
  }

  // Respect explicit site type first so static deployments are not forced into
  // server mode when UI/default data still contains a start command.
  const explicitType = String(siteType || '').trim().toLowerCase();
  const hasStartCmd = !!String(startCmd || '').trim();
  let isServerApp = explicitType === 'server' || (!explicitType && hasStartCmd);

  // [FIX] These were Project.findOne / Project.exists / Project.countDocuments
  // calls -- all Mongoose, querying a database that isn't actually this
  // platform's real datastore (Firebase is). That meant every deploy was
  // checked against an effectively empty/stale collection: existing-project
  // lookups would come back null even for a genuine redeploy of the same
  // subdomain (spawning a brand new project record instead of updating the
  // real one -- this is very likely why repeated deploys to the same "docs"
  // subdomain kept generating new project ids all night instead of updating
  // one project), and the project-count plan limit was being checked against
  // the wrong number entirely. Firebase is the real source of truth here, so
  // read from the same workspace data every other working endpoint uses.
  const deployWs = (await readWorkspaceFromFirebase(req.user)) || {};
  const deployWsProjects = Array.isArray(deployWs.projects) ? deployWs.projects : [];
  const existingBySub = deployWsProjects.find(p => p.subdomain === cleanSub) || null;
  const existingByName = deployWsProjects.find(p => p.name === name) || null;
  const existingBySubId = existingBySub?.id || '';
  const existingByNameId = existingByName?.id || '';
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

  const planKey = await getUserPlanKey(req.user);
  const runtimeProfile = getRuntimeProfileForPlan(planKey);

  // ── Plan project-count limit check ───────────────────────────────────────
  // Skip for redeploying an existing project (subdomain already exists).
  const githubPlanLimits = PLAN_DB_API_LIMITS[planKey] || PLAN_DB_API_LIMITS.free;
  const isExistingGithubProject = !!existingBySub;
  if (!isExistingGithubProject) {
    const ownedProjectCount = deployWsProjects.length; // already scoped to this user's own workspace
    if (Number.isFinite(githubPlanLimits.maxProjects) && ownedProjectCount >= githubPlanLimits.maxProjects) {
      return res.status(403).json({
        error: `Project limit reached for ${planKey} plan (${githubPlanLimits.maxProjects} max). Upgrade your plan to deploy more projects.`,
        limitReached: true,
        plan: planKey,
        maxProjects: githubPlanLimits.maxProjects,
        currentCount: ownedProjectCount
      });
    }
  }

  // ── Monthly build-time quota check ───────────────────────────────────────
  if (Number.isFinite(githubPlanLimits.monthlyBuildSeconds) && githubPlanLimits.monthlyBuildSeconds > 0) {
    const monthStart = new Date(); monthStart.setDate(1); monthStart.setHours(0,0,1,0);
    // [FIX] Same issue as above -- Deployment.aggregate() against Mongo,
    // silently returning 0 on failure/timeout, meaning this quota never
    // actually blocked anyone. Sum from the real Firebase deployment history
    // instead.
    const deployWsDeployments = Array.isArray(deployWs.deployments) ? deployWs.deployments : [];
    const usedSeconds = deployWsDeployments
      .filter(d => d.status === 'success' && d.startedAt && new Date(d.startedAt) >= monthStart)
      .reduce((sum, d) => sum + (Number(d.durationSeconds) || 0), 0);
    if (usedSeconds >= githubPlanLimits.monthlyBuildSeconds) {
      return res.status(403).json({
        error: `Monthly build-time quota reached (${usedSeconds}s / ${githubPlanLimits.monthlyBuildSeconds}s). Resets at the start of next month.`,
        buildQuotaReached: true
      });
    }
  }

  // ── Disk quota check ──────────────────────────────────────────────────────
  const ownerIdForDisk = String(req.user?._id || req.user?.id || '');
  const diskCheck = await checkDiskQuota(ownerIdForDisk, planKey);
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
        workingDir: cleanWorkingDir,
        nodeVer:    nodeVer    || '20',
        // [FIX] Was `siteType || 'static'` -- this is the actual Project
        // record used to build with (via runBuild -> _runBuildDispatch in
        // buildRunner.js), completely independent of the v1 API layer fix
        // made earlier tonight. That fix passed blank siteType through
        // correctly at the API boundary, but this line re-applied its own
        // 'static' default right here, silently undoing it every time --
        // forcing every deploy without an explicit siteType into the
        // static-file build path (expecting a "dist" output folder) instead
        // of letting _runBuildDispatch's real post-clone detection run.
        siteType:   siteType   || '',
        appPort,
        billingPlan: planKey,
        memoryLimit: runtimeProfile.memoryLimit,
        cpuShares: runtimeProfile.cpuShares,
        memorySwap: runtimeProfile.memorySwap,
        // [FIX] Only write envVars when the request body actually contains them.
        // An empty/missing envVars in the redeploy request must NOT erase vars
        // the user saved via the Env Manager — those are stored in the DB and
        // Firebase and should survive a redeploy that doesn't re-supply them.
        ...(envVars && typeof envVars === 'object' && Object.keys(envVars).length > 0
          ? { envVars }
          : {}),
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
      outputDir: outputDir||'dist', workingDir: cleanWorkingDir, nodeVer: nodeVer||'20',
      // [FIX] Same as the primary upsert above -- pass blank through instead
      // of forcing 'static', so _runBuildDispatch's real detection runs.
      // This fallback branch is the one actually used whenever Mongo isn't
      // configured (this account's normal Firebase-only setup), so this was
      // the line actually responsible for every auto-detected GitHub deploy
      // landing as a static site tonight.
      siteType: siteType||'', appPort, billingPlan: planKey, memoryLimit: runtimeProfile.memoryLimit, cpuShares: runtimeProfile.cpuShares, memorySwap: runtimeProfile.memorySwap, envVars: envVars||{},
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
    // [FIX] async — avoids blocking the proxy/event loop during redeploy.
    try { await execP(`docker rm -f db-${cleanSub}`); } catch(e) {}
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
  await syncDeploymentProjectToFirebase(req.user, {
    project,
    deployment,
    status: 'building',
    liveUrl: `https://${cleanSub}.${BASE_DOMAIN}`,
    envVars: envVars && typeof envVars === 'object' ? envVars : null
  });
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

  const _ownerRoomId = String(project?.ownerUserId || req.user?._id || req.user?.id || '');
  const _emitTarget = _ownerRoomId ? io.to('user:' + _ownerRoomId) : io;
  // Pass ownerRoomId into autodeploy status emits so they are also scoped
  const _boundEmitAutoDeployStatus = (pid, st, ex={}) => emitAutoDeployStatus(pid, st, ex, _ownerRoomId);
  const emit = (event, data) => _emitTarget.emit(event, { deployId, projectId: String(project._id), source: deploySource, triggerSha, ...data });

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
    // 1. envVars stored on the project record in MongoDB (set via Env Manager page)
    // 2. envVars stored in Firebase ws.projects (primary store when MongoDB is not the source of truth)
    // 3. envVars from the request body (deploy form / redeploy) — always wins over saved vars
    // [FIX] Firebase is the PRIMARY workspace store. Always re-read it here so that
    // env vars saved via the Env Manager (which writes to Firebase ws.projects) are
    // picked up even when the MongoDB upsert above had no envVars in the request body.
    const projectEnvVarsRaw = project?.envVars instanceof Map
      ? Object.fromEntries(project.envVars)
      : (project?.envVars && typeof project.envVars === 'object' ? { ...project.envVars } : {});
    const requestEnvVarsRaw = envVars && typeof envVars === 'object' ? envVars : {};

    let firebaseEnvVars = {};
    try {
      const userForFirebase = await enrichAuthUser(req.user);
      const ws = await readWorkspaceFromFirebase(userForFirebase);
      if (ws && Array.isArray(ws.projects)) {
        const fbProj = ws.projects.find(p =>
          p.subdomain === cleanSub || p.id === cleanSub || String(p._id || '') === cleanSub
        );
        if (fbProj?.envVars && typeof fbProj.envVars === 'object') {
          firebaseEnvVars = { ...fbProj.envVars };
        }
        // Also check ws.envStore which is used by buildProjectSnapshot
        if (ws.envStore && typeof ws.envStore === 'object') {
          const storeVars = ws.envStore[cleanSub] || ws.envStore[String(project._id || '')] || {};
          if (typeof storeVars === 'object') {
            firebaseEnvVars = { ...firebaseEnvVars, ...storeVars };
          }
        }
      }
    } catch (_) {}

    // Merge order: MongoDB vars → Firebase vars → request body vars
    // Firebase wins over MongoDB (it's the primary store), request body wins over both.
    const resolvedEnvVars = Object.assign({}, projectEnvVarsRaw, firebaseEnvVars, requestEnvVarsRaw);

    // Log env var count (keys only — never log values for security)
    const envKeyCount = Object.keys(resolvedEnvVars).length;
    if (envKeyCount > 0) {
      emit('build:log', { line: `\x1b[90m[env] Injecting ${envKeyCount} environment variable${envKeyCount > 1 ? 's' : ''}: ${Object.keys(resolvedEnvVars).join(', ')}\x1b[0m` });
    } else {
      emit('build:log', { line: `\x1b[33m[env] ⚠ No environment variables found for this project. If your app needs DATABASE_URL, API keys etc., add them via the Env Manager before deploying.\x1b[0m` });
    }

    // ── Auto-skip missing build / start scripts + Node.js version detection ─
    // If build or start command is not 'echo skip' but the corresponding npm
    // script doesn't exist in the repo's package.json, automatically skip it
    // and warn in the log — so the user gets a clear message instead of an error.
    // Also detects the required Node.js version from package.json engines field
    // and auto-corrects if the user selected the wrong version.
    const isSkipCmd = (cmd) => !cmd || /^(echo\s+skip|skip)$/i.test(String(cmd).trim());
    const projectBuildCmd  = String(project.buildCmd  || '').trim();
    const projectStartCmd  = String(project.startCmd  || '').trim();
    const looksLikeNpmBuild = /^npm\s+run\s+\S+/.test(projectBuildCmd);
    const looksLikeNpmStart = /^npm\s+(run\s+)?(start)/.test(projectStartCmd);

    // Try to read the cloned repo's package.json to check scripts (best-effort)
    // The tmp build dir follows the pattern: TMP_DIR/<cleanSub>
    const tmpBuildDir = path.join(TMP_DIR, cleanSub);
    let pkgScripts = null;
    let pkgEnginesNode = null;
    try {
      const pkgPath = path.join(tmpBuildDir, 'package.json');
      if (fs.existsSync(pkgPath)) {
        const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
        pkgScripts = pkg.scripts || {};
        pkgEnginesNode = (pkg.engines && pkg.engines.node) ? String(pkg.engines.node).trim() : null;
      }
    } catch (_) {}

    // ── Auto-correct Node.js version if engines field specifies a different one ─
    if (pkgEnginesNode) {
      const m = pkgEnginesNode.match(/(\d+)/);
      if (m) {
        const requiredMajor = Number(m[1]);
        const configuredMajor = Number(String(project.nodeVer || '20').match(/\d+/)?.[0] || 20);
        if (Number.isInteger(requiredMajor) && requiredMajor >= 14 && requiredMajor !== configuredMajor) {
          // Map to supported image version
          const autoNodeVer = requiredMajor >= 22 ? '22' : requiredMajor >= 20 ? '20' : requiredMajor >= 18 ? '18' : '16';
          emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m Node.js version mismatch detected:` });
          emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m  → You selected Node.js ${project.nodeVer} but package.json engines requires: Node.js ${requiredMajor}` });
          emit('build:log', { line: `\x1b[32m[Joytree]\x1b[0m  ✓ Automatically switching to Node.js ${autoNodeVer} for this deployment` });
          project = { ...project, nodeVer: autoNodeVer };
        } else if (Number.isInteger(requiredMajor)) {
          emit('build:log', { line: `\x1b[90m[detect] Node.js version confirmed: ${project.nodeVer} (matches package.json engines: "${pkgEnginesNode}")\x1b[0m` });
        }
      }
    }

    // Check build script
    if (!isSkipCmd(projectBuildCmd) && looksLikeNpmBuild && pkgScripts !== null) {
      const scriptName = (projectBuildCmd.match(/^npm\s+run\s+(\S+)/) || [])[1];
      if (scriptName && !pkgScripts[scriptName]) {
        emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m No "${scriptName}" script found in package.json — automatically skipping build step.` });
        emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m ℹ Tip: add a "${scriptName}" script to package.json, or set build command to "echo skip".` });
        // Also fix output directory: if no build runs, dist/ won't exist — serve root instead
        const configuredOutputDir = String(project.outputDir || 'dist').trim();
        if (configuredOutputDir !== '.') {
          emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m Output directory automatically changed from "${configuredOutputDir}" → "." (serving project root since no build was run)` });
          project = { ...project, buildCmd: 'echo skip', outputDir: '.' };
        } else {
          project = { ...project, buildCmd: 'echo skip' };
        }
      }
    }

    // Check start script — only relevant for server apps
    if (isServerApp && !isSkipCmd(projectStartCmd) && looksLikeNpmStart && pkgScripts !== null) {
      const startScript = (projectStartCmd.match(/^npm\s+(?:run\s+)?(\S+)/) || [])[1];
      if (startScript && !pkgScripts[startScript]) {
        emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m No "${startScript}" script found in package.json — automatically skipping start command (will try node index.js / server.js as fallback).` });
        emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m ℹ Tip: add a "${startScript}" script to package.json, or set your start command explicitly (e.g. "node server.js").` });
        project = { ...project, startCmd: '' };
      }
    }
    // ────────────────────────────────────────────────────────────────────────

    // [FIX] Acquire a build slot before spawning docker-build containers.
    // If the host is already running MAX_CONCURRENT_BUILDS builds, this
    // deploy waits here (queued) instead of piling another heavy node:20
    // container on top of an already-loaded host.
    if (_activeBuildCount >= MAX_CONCURRENT_BUILDS) {
      emit('build:log', { line: `\x1b[33m[Joytree]\x1b[0m Waiting for a free build slot (${_activeBuildCount}/${MAX_CONCURRENT_BUILDS} builds currently running)...` });
    }
    await acquireBuildSlot();
    let buildResult;
    try {
      buildResult = await runBuild({
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
    } finally {
      releaseBuildSlot();
    }
    // [FIX] runBuild() now reports back which type it actually resolved to
    // (see buildRunner.js's runServerBuild/runStaticBuild return values).
    // Without this, a correctly auto-detected server app's persisted
    // siteType stayed whatever it started as (blank, defaulting to
    // 'static' downstream) even though it deployed and ran as a server --
    // e.g. samz-demo-v2 showing siteType:"static" in the dashboard/API
    // despite being a working Express app with no build step at all.
    if (buildResult && buildResult.siteType && buildResult.siteType !== project.siteType) {
      project = { ...project, siteType: buildResult.siteType };
      isServerApp = buildResult.siteType === 'server';
      try { await Project.findByIdAndUpdate(project._id, { siteType: buildResult.siteType }); } catch (_) {}
    }
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
    await syncDeploymentProjectToFirebase(req.user, {
      project,
      deployment,
      status: 'success',
      liveUrl: cf?.url || `https://${cleanSub}.${BASE_DOMAIN}`,
      envVars: resolvedEnvVars
    });

    // ── Sync final status + duration to Firebase (source of truth on reload) ──
    try {
      const userForFirebase = await enrichAuthUser(req.user);
      if (userForFirebase?.email) {
        const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
        ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
        const idx = ws.deployments.findIndex(d => d.id === deployId || d.id === String(deployment._id));
        const updated = {
          id: deployId, _id: deployId,
          projectId: String(project._id),
          projectName: name,
          subdomain: cleanSub,
          branch: branch || 'main',
          status: 'success',
          source: deploySource,
          triggerSha,
          duration,
          startedAt: (deployment.startedAt || new Date()).toISOString(),
          endedAt: (deployment.endedAt || new Date()).toISOString(),
        };
        if (idx >= 0) ws.deployments[idx] = updated; else ws.deployments.unshift(updated);
        if (ws.deployments.length > 100) ws.deployments = ws.deployments.slice(0, 100);
        ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
        const pIdx = ws.projects.findIndex(p => p.subdomain === cleanSub);
        if (pIdx >= 0) ws.projects[pIdx] = { ...ws.projects[pIdx], status: 'success', updatedAt: updated.endedAt };
        await writeWorkspaceToFirebase(userForFirebase, ws);
      }
    } catch (_fbErr) { /* non-fatal — Firebase was already synced by syncDeploymentProjectToFirebase */ }

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
    // [FIX] Tell the frontend the container is live and it can open the runtime log stream now.
    // Without this the UI doesn't know when to switch from build logs to runtime logs.
    emit('runtime:ready', { subdomain: cleanSub, projectId: String(project._id), liveUrl: cf?.url || `https://${cleanSub}.${BASE_DOMAIN}` });

  } catch(buildErr) {
    const duration = Math.round((Date.now() - buildStart) / 1000);
    deployment.status = 'failed'; deployment.duration = duration; deployment.endedAt = new Date();
    try { await deployment.save(); } catch(e) {}
    await syncDeploymentProjectToFirebase(req.user, {
      project,
      deployment,
      status: 'failed',
      liveUrl: `https://${cleanSub}.${BASE_DOMAIN}`,
      envVars: envVars && typeof envVars === 'object' ? envVars : null
    });

    // ── Sync final status + duration to Firebase (source of truth on reload) ──
    try {
      const userForFirebase = await enrichAuthUser(req.user);
      if (userForFirebase?.email) {
        const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
        ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
        const idx = ws.deployments.findIndex(d => d.id === deployId || d.id === String(deployment._id));
        const updated = {
          id: deployId, _id: deployId,
          projectId: String(project._id),
          projectName: name,
          subdomain: cleanSub,
          branch: branch || 'main',
          status: 'failed',
          source: deploySource,
          triggerSha,
          duration,
          startedAt: (deployment.startedAt || new Date()).toISOString(),
          endedAt: (deployment.endedAt || new Date()).toISOString(),
        };
        if (idx >= 0) ws.deployments[idx] = updated; else ws.deployments.unshift(updated);
        if (ws.deployments.length > 100) ws.deployments = ws.deployments.slice(0, 100);
        ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
        const pIdx = ws.projects.findIndex(p => p.subdomain === cleanSub);
        if (pIdx >= 0) ws.projects[pIdx] = { ...ws.projects[pIdx], status: 'failed', updatedAt: updated.endedAt };
        await writeWorkspaceToFirebase(userForFirebase, ws);
      }
    } catch (_fbErr) { /* non-fatal — Firebase was already synced by syncDeploymentProjectToFirebase */ }

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
    // [FIX] Register the stop signal FIRST, unconditionally. This Set is what
    // the actual build loop polls to know it should abort (see the
    // deployStopRequests.has(deployId) checks in the git-deploy build flow
    // above) — it doesn't touch Mongo at all. Previously this only happened
    // AFTER Deployment.findById(deployId) succeeded, so for any deployment
    // using a non-ObjectId id (e.g. "local_..." ids used by git-deployed/IDE
    // projects, which are never saved as real Mongo documents) that lookup
    // threw a CastError before the stop signal was ever added — meaning
    // Stop silently failed to stop anything AND surfaced an opaque 500
    // ("Cast to ObjectId failed...") to the user.
    deployStopRequests.add(deployId);

    // [FIX] Only query Mongo when the id is actually a valid ObjectId.
    // Deployments with synthetic ids (local_..., subdomain-based, etc.)
    // simply don't have a Mongo document to update — that's expected, not
    // an error condition, so we skip straight to emitting the stop events.
    let dep = null;
    if (looksLikeObjectId(deployId)) {
      dep = await Deployment.findById(deployId).catch(() => null);
    }

    let projectId = '';
    let ownerUserId = '';
    if (dep) {
      const project = await findProjectByAnyId(dep.projectId);
      if (project?.ownerUserId && String(project.ownerUserId) !== String(req.user?._id || req.user?.id || '')) {
        return res.status(403).json({ error: 'Forbidden' });
      }
      projectId = String(dep.projectId || '');
      ownerUserId = String(project?.ownerUserId || '');
      dep.status = 'failed';
      dep.endedAt = new Date();
      dep.logs = dep.logs || [];
      dep.logs.push('[manual] Deployment stop requested by user.');
      await dep.save().catch(() => {});
    }

    const _stopOwner = ownerUserId || String(req.user?._id || req.user?.id || '');
    const _stopTarget = _stopOwner ? io.to('user:' + _stopOwner) : io;
    _stopTarget.emit('build:log', { deployId, projectId, line: '\x1b[33m[Joytree]\x1b[0m Stop requested by user. Attempting to halt build\u2026' });
    _stopTarget.emit('build:done', { deployId, projectId, status: 'canceled' });
    res.json({ ok: true, message: 'Stop requested' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Socket.io ─────────────────────────────────────────────────────────────────
io.on('connection', socket => {
  console.log('[Socket.io] Connected:', socket.id);
  // Client sends its userId so we can route events to the right user only
  socket.on('auth:join', ({ userId } = {}) => {
    if (userId) {
      socket.join('user:' + String(userId));
      socket._joytreeUserId = String(userId);
    }
  });
  socket.on('disconnect', () => console.log('[Socket.io] Disconnected:', socket.id));
});

// ─────────────────────────────────────────────────────────────────────────────
// CODE UPLOAD / IDE — Endpoints
// Storage: SITES_DIR/uploads/<userId>/<projectId>/  (permanent)
// ─────────────────────────────────────────────────────────────────────────────

const UPLOADS_DIR = process.env.UPLOADS_DIR || path.join(SITES_DIR, 'uploads');
try { fs.mkdirSync(UPLOADS_DIR, { recursive: true }); } catch(e) {}

// ── Pure Node.js multipart parser (zero external deps — no busboy needed) ────
// Reads the entire request body into memory as a Buffer, then scans for the
// multipart boundary to extract fields and the file.  Works on any Node ≥ 14.
function parseMultipart(req) {
  return new Promise((resolve, reject) => {
    const ct = req.headers['content-type'] || '';
    const boundaryMatch = ct.match(/boundary=("?)([^";,\s]+)\1/i);
    if (!boundaryMatch) return reject(new Error('No multipart boundary in Content-Type header'));
    const boundary = boundaryMatch[2];

    const MAX = 260 * 1024 * 1024; // 260 MB
    const chunks = [];
    let total = 0;
    req.on('data', chunk => {
      total += chunk.length;
      if (total > MAX) { req.destroy(); return reject(new Error('Upload exceeds 260 MB limit')); }
      chunks.push(chunk);
    });
    req.on('error', reject);
    req.on('end', () => {
      try {
        const body = Buffer.concat(chunks);
        const fields = {};
        let fileBuffer = null;
        let fileName = 'archive.zip';

        // Split on --boundary
        const BND = Buffer.from('--' + boundary);
        const CRLF = Buffer.from('\r\n');
        const CRLFCRLF = Buffer.from('\r\n\r\n');

        let pos = 0;
        while (pos < body.length) {
          // Find next boundary
          const bStart = indexOf(body, BND, pos);
          if (bStart === -1) break;
          pos = bStart + BND.length;

          // Check for final --
          if (body[pos] === 0x2D && body[pos+1] === 0x2D) break;

          // Skip the CRLF after boundary
          if (body[pos] === 0x0D && body[pos+1] === 0x0A) pos += 2;

          // Find end of headers (double CRLF)
          const headerEnd = indexOf(body, CRLFCRLF, pos);
          if (headerEnd === -1) break;
          const headerBuf = body.slice(pos, headerEnd).toString('utf8');
          pos = headerEnd + 4; // skip \r\n\r\n

          // Find start of next boundary to get this part's data end
          const nextBnd = indexOf(body, BND, pos);
          const dataEnd = nextBnd === -1 ? body.length : nextBnd - 2; // -2 to strip trailing \r\n
          const data = body.slice(pos, dataEnd);
          pos = nextBnd === -1 ? body.length : nextBnd;

          // Parse Content-Disposition
          const dispMatch = headerBuf.match(/Content-Disposition:[^\r\n]*name="([^"]+)"/i);
          if (!dispMatch) continue;
          const partName = dispMatch[1];
          const fileNameMatch = headerBuf.match(/filename="([^"]*)"/i);

          if (fileNameMatch) {
            // This is the file part
            fileName = fileNameMatch[1] || 'archive.zip';
            fileBuffer = data;
          } else {
            // Text field
            fields[partName] = data.toString('utf8');
          }
        }

        resolve({ fields, fileBuffer, fileName });
      } catch(e) { reject(e); }
    });
  });
}

// Buffer indexOf helper (Buffer.indexOf exists in Node ≥ 6, this is a safe wrapper)
function indexOf(buf, search, from) {
  return buf.indexOf(search, from || 0);
}

// ── Multipart upload ─────────────────────────────────────────────────────────
app.post('/api/upload-project', requireAuth, async (req, res) => {
  const userId = String(req.user?._id || req.user?.id || 'anon');
  const ct = req.headers['content-type'] || '';
  if (!ct.includes('multipart/form-data')) {
    return res.status(400).json({ error: 'multipart/form-data required' });
  }

  let parsed;
  try {
    parsed = await parseMultipart(req);
  } catch(e) {
    return res.status(400).json({ error: 'Failed to parse upload: ' + e.message });
  }

  const { fields, fileBuffer, fileName } = parsed;
  let projectId   = String(fields.projectId   || '').trim() || ('upload_' + Date.now());
  const projectName = String(fields.projectName || '').trim().slice(0, 80) || 'project';

  if (!fileBuffer || fileBuffer.length === 0) {
    return res.status(400).json({ error: 'No file received — make sure you selected a .zip, .tar.gz, or .tgz file' });
  }

  const origFilename = path.basename(fileName).replace(/[^a-zA-Z0-9._-]/g, '_') || 'archive.zip';
  const userUploadDir = path.join(UPLOADS_DIR, userId, projectId);
  try { fs.mkdirSync(userUploadDir, { recursive: true }); } catch(e) {
    return res.status(500).json({ error: 'Could not create upload directory: ' + e.message });
  }

  const savedArchivePath = path.join(userUploadDir, origFilename);
  try { fs.writeFileSync(savedArchivePath, fileBuffer); } catch(e) {
    return res.status(500).json({ error: 'File write failed: ' + e.message });
  }

  if (fs.statSync(savedArchivePath).size === 0) {
    fs.unlinkSync(savedArchivePath);
    return res.status(400).json({ error: 'Uploaded file is empty' });
  }

  const isSingleHtml = fields.singleHtml === '1' || origFilename.endsWith('.html') || origFilename.endsWith('.htm');

  try {
    // ── Single HTML file: no extraction needed — serve it directly ──────────
    let result;
    if (isSingleHtml) {
      const destDir = path.join(UPLOADS_DIR, userId, projectId, 'files');
      try { fs.mkdirSync(destDir, { recursive: true }); } catch(e) {}
      const safeFileName = origFilename.endsWith('.htm') ? origFilename.slice(0, -4) + '.html' : origFilename;
      const destPath = path.join(destDir, safeFileName);
      fs.copyFileSync(savedArchivePath, destPath);
      let content = '';
      try { content = fs.readFileSync(destPath, 'utf8'); } catch(e) { content = '[unreadable]'; }
      result = {
        lang: 'html',
        fileCount: 1,
        files: { [safeFileName]: content },
        unzippedSize: fileBuffer.length
      };
    } else {
      result = await extractUploadedArchive(savedArchivePath, path.join(UPLOADS_DIR, userId, projectId, 'files'));
    }

    // ── Sync to Firebase BEFORE responding ───────────────────────────────────
    let firebaseSynced = false;
    let fbDebugMsg = '';
    try {
      const userForFirebase = await enrichAuthUser(req.user);
      fbDebugMsg += `email=${userForFirebase?.email||'MISSING'} `;
      if (!userForFirebase?.email) throw new Error('No email on user — cannot build Firebase key');
      const meta = {
        id: projectId, name: projectName, fileName: origFilename,
        lang: result.lang, fileCount: result.fileCount,
        size: result.unzippedSize, uploadedAt: new Date().toISOString(),
        extracted: true,
        serverPath: path.join(UPLOADS_DIR, userId, projectId, 'files')
      };
      let ws = null;
      try { ws = await readWorkspaceFromFirebase(userForFirebase); fbDebugMsg += 'fbRead=ok '; }
      catch(re) { fbDebugMsg += `fbRead=ERR(${re.message}) `; ws = null; }
      ws = (ws && typeof ws === 'object') ? ws : {};
      ws.uploadedProjects = Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : [];
      const existingIdx = ws.uploadedProjects.findIndex(p => p.id === projectId);
      if (existingIdx >= 0) ws.uploadedProjects[existingIdx] = meta;
      else ws.uploadedProjects.unshift(meta);
      if (ws.uploadedProjects.length > 50) ws.uploadedProjects = ws.uploadedProjects.slice(0, 50);
      firebaseSynced = !!(await writeWorkspaceToFirebase(userForFirebase, ws));
      fbDebugMsg += `fbWrite=${firebaseSynced?'ok':'FAILED'} `;
      if (firebaseSynced) {
        const localUser = localAuth.users.find(u => String(u.id || u._id || '') === userId || u.email === userForFirebase.email);
        if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
      }
    } catch(fbErr) {
      fbDebugMsg += `exception=${fbErr.message}`;
    }
    console.log(`[upload-project] Firebase sync: ${fbDebugMsg}`);

    res.json({
      ok: true, projectId,
      lang: result.lang, fileCount: result.fileCount,
      files: result.files, unzippedSize: result.unzippedSize,
      serverPath: path.join(UPLOADS_DIR, userId, projectId, 'files'),
      firebaseSynced, fbDebug: fbDebugMsg
    });
  } catch(e) {
    return res.status(500).json({ error: 'Extraction failed: ' + e.message });
  }
});

// ── Re-extract already uploaded archive ─────────────────────────────────────
// ── Re-extract already uploaded archive ─────────────────────────────────────
app.get('/api/upload-projects', requireAuth, async (req, res) => {
  try {
    const userForFirebase = await enrichAuthUser(req.user);
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    const uploads = Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : [];
    return res.json({ ok: true, projects: uploads });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
});

// Sync a client-side project list to Firebase (used when Firebase is source of truth
// but the client has local-only entries it wants to persist to the account)
app.post('/api/upload-projects-sync', requireAuth, async (req, res) => {
  try {
    const userId = String(req.user?._id || req.user?.id || 'anon');
    const incoming = Array.isArray(req.body.projects) ? req.body.projects : [];
    if (incoming.length === 0) return res.json({ ok: true });
    const userForFirebase = await enrichAuthUser(req.user);
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    ws.uploadedProjects = Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : [];
    // Merge: incoming entries upsert into Firebase list
    for (const p of incoming) {
      const idx = ws.uploadedProjects.findIndex(x => x.id === p.id);
      if (idx >= 0) ws.uploadedProjects[idx] = { ...ws.uploadedProjects[idx], ...p };
      else ws.uploadedProjects.unshift(p);
    }
    if (ws.uploadedProjects.length > 50) ws.uploadedProjects = ws.uploadedProjects.slice(0, 50);
    await writeWorkspaceToFirebase(userForFirebase, ws);
    const localUser = localAuth.users.find(u => String(u.id || u._id || '') === userId || u.email === userForFirebase.email);
    if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
    return res.json({ ok: true });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
});

app.delete('/api/upload-projects/:projectId', requireAuth, async (req, res) => {
  try {
    const pId = String(req.params.projectId || '').trim();
    if (!pId) return res.status(400).json({ error: 'projectId required' });
    const userId = String(req.user?._id || req.user?.id || 'anon');
    const userForFirebase = await enrichAuthUser(req.user);
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    ws.uploadedProjects = (Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : []).filter(p => p.id !== pId);
    await writeWorkspaceToFirebase(userForFirebase, ws);
    const localUser = localAuth.users.find(u => String(u.id || u._id || '') === userId || u.email === userForFirebase.email);
    if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
    // Also delete files from disk
    try { const d = path.join(UPLOADS_DIR, userId, pId); if (fs.existsSync(d)) fs.rmSync(d, { recursive: true, force: true }); } catch(_e) {}
    return res.json({ ok: true });
  } catch(e) {
    return res.status(500).json({ error: e.message });
  }
});

app.post('/api/upload-extract', requireAuth, async (req, res) => {
  const userId = String(req.user?._id || req.user?.id || 'anon');
  const { projectId } = req.body;
  if (!projectId) return res.status(400).json({ error: 'projectId required' });

  const projectDir = path.join(UPLOADS_DIR, userId, projectId);
  if (!fs.existsSync(projectDir)) return res.status(404).json({ error: 'Upload not found' });

  // Find the archive
  let archivePath = null;
  for (const f of fs.readdirSync(projectDir)) {
    if (/\.(zip|tar\.gz|tgz)$/.test(f)) { archivePath = path.join(projectDir, f); break; }
  }
  if (!archivePath) return res.status(404).json({ error: 'Archive file not found' });

  try {
    const result = await extractUploadedArchive(archivePath, path.join(projectDir, 'files'));
    return res.json({ ok: true, ...result });
  } catch(e) {
    return res.status(500).json({ error: 'Extraction failed: ' + e.message });
  }
});

// ── Read all files for a project from disk (used after reload when client has no file content) ──
app.get('/api/upload-files/:projectId', requireAuth, async (req, res) => {
  const userId     = String(req.user?._id || req.user?.id || 'anon');
  const projectId  = String(req.params.projectId || '').trim();
  if (!projectId) return res.status(400).json({ error: 'projectId required' });

  const filesDir = path.join(UPLOADS_DIR, userId, projectId, 'files');
  if (!fs.existsSync(filesDir)) {
    return res.status(404).json({ error: 'Project files not found on disk. Please re-upload.' });
  }

  // Walk the files directory and collect text file contents (same logic as extractUploadedArchive)
  const SKIP_DIRS  = new Set(['node_modules','.git','.svn','__pycache__','.cache','.next','.nuxt','dist','build','.turbo']);
  const BINARY_EXTS = new Set(['png','jpg','jpeg','gif','webp','ico','woff','woff2','ttf','eot','pdf','zip','tar','gz','mp4','mp3','wav','bin','exe','so','dll','pyc','class','o','a']);
  const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB — large enough for big single-file HTML apps
  const MAX_FILES = 2000;
  const files = {};

  function walk(dir, prefix) {
    if (Object.keys(files).length >= MAX_FILES) return;
    let entries;
    try { entries = fs.readdirSync(dir); } catch(e) { return; }
    for (const e of entries) {
      if (e.startsWith('.') && e !== '.env' && e !== '.env.example' && e !== '.eslintrc' && e !== '.prettierrc') continue;
      const full = path.join(dir, e);
      const rel  = prefix ? prefix + '/' + e : e;
      try {
        const stat = fs.lstatSync(full);
        if (stat.isDirectory()) {
          if (!SKIP_DIRS.has(e)) walk(full, rel);
        } else if (stat.isFile()) {
          const ext = e.split('.').pop().toLowerCase();
          if (!BINARY_EXTS.has(ext) && stat.size <= MAX_FILE_SIZE) {
            try { files[rel] = fs.readFileSync(full, 'utf8'); } catch(_) { files[rel] = '[Binary or unreadable file]'; }
          }
        }
      } catch(_) {}
    }
  }
  walk(filesDir, '');

  return res.json({ ok: true, files });
});

// ── Save edited file ─────────────────────────────────────────────────────────
app.post('/api/upload-save-file', requireAuth, async (req, res) => {
  const userId = String(req.user?._id || req.user?.id || 'anon');
  const { projectId, filePath, content } = req.body;
  if (!projectId || !filePath) return res.status(400).json({ error: 'projectId and filePath required' });

  // Security: prevent path traversal
  const baseDir = path.join(UPLOADS_DIR, userId, projectId, 'files');
  const target = path.resolve(baseDir, filePath);
  if (!target.startsWith(baseDir)) return res.status(403).json({ error: 'Invalid path' });

  try {
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, String(content || ''), 'utf8');
    return res.json({ ok: true });
  } catch(e) {
    return res.status(500).json({ error: 'Save failed: ' + e.message });
  }
});

// ── Deploy from upload ───────────────────────────────────────────────────────
app.post('/api/upload-deploy', requireAuth, async (req, res) => {
  const userId = String(req.user?._id || req.user?.id || 'anon');
  const { projectId, name, subdomain, siteType, buildCmd, installCmd, startCmd, outputDir, nodeVer, envVars } = req.body;

  if (!projectId || !name || !subdomain) {
    return res.status(400).json({ error: 'projectId, name, and subdomain are required' });
  }

  const cleanSub = subdomain.toLowerCase().replace(/[^a-z0-9-]/g,'-').replace(/-+/g,'-').replace(/^-|-$/g,'');
  const filesDir = path.join(UPLOADS_DIR, userId, projectId, 'files');
  if (!fs.existsSync(filesDir)) {
    return res.status(404).json({ error: 'Project files not found. Please extract the archive first.' });
  }

  const explicitType = String(siteType || '').trim().toLowerCase();
  const hasStartCmd = !!String(startCmd || '').trim();
  let isServerApp = explicitType === 'server' || (!explicitType && hasStartCmd);

  let appPort = 0;
  if (isServerApp) {
    try { appPort = getOrAssignPort(cleanSub); } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  const planKey = await getUserPlanKey(req.user);
  const runtimeProfile = getRuntimeProfileForPlan(planKey);

  // ── Plan project-count limit check (same gate as GitHub /api/deploy) ────────
  // Only applies to NEW projects (subdomain not yet in the workspace).
  //
  // [FIX] This used to check Project.exists()/Project.countDocuments() --
  // straight MongoDB queries. This account (and any Firebase-only setup,
  // which is the normal/default configuration per MONGODB_URI not being
  // set) has no Mongo data at all, so isExistingUploadProject was always
  // null and ownedProjectCount was always 0 (from the .catch(() => 0)
  // fallback, since the query itself errors with no DB connection) --
  // meaning `0 >= maxProjects` was always false and this check silently
  // never fired. Reproduced live: deployed a 6th project on a 5-project
  // free plan with zero pushback, via both /api/upload-deploy directly and
  // the new /api/v1/deploy-from-zip, which both go through this same
  // handler. The GitHub deploy path (/api/deploy) never had this problem
  // because it already counts from the Firebase workspace directly
  // (deployWsProjects.length) -- switched this check to the same source.
  const uploadPlanLimits = PLAN_DB_API_LIMITS[planKey] || PLAN_DB_API_LIMITS.free;
  const uploadDeployWs = (await readWorkspaceFromFirebase(req.user)) || {};
  const uploadDeployWsProjects = Array.isArray(uploadDeployWs.projects) ? uploadDeployWs.projects : [];
  const isExistingUploadProject = uploadDeployWsProjects.some(p => p.subdomain === cleanSub);
  if (!isExistingUploadProject) {
    const ownedProjectCount = uploadDeployWsProjects.length; // already scoped to this user's own workspace
    if (Number.isFinite(uploadPlanLimits.maxProjects) && ownedProjectCount >= uploadPlanLimits.maxProjects) {
      return res.status(403).json({
        error: `Project limit reached for ${planKey} plan (${uploadPlanLimits.maxProjects} max). Upgrade your plan to deploy more projects.`,
        limitReached: true,
        plan: planKey,
        maxProjects: uploadPlanLimits.maxProjects,
        currentCount: ownedProjectCount
      });
    }
  }

  // ── Monthly build-time quota check ────────────────────────────────────────
  if (Number.isFinite(uploadPlanLimits.monthlyBuildSeconds) && uploadPlanLimits.monthlyBuildSeconds > 0) {
    const ownerId = String(req.user?._id || req.user?.id || '');
    const monthStart = new Date(); monthStart.setDate(1); monthStart.setHours(0,0,1,0);
    const usedSeconds = await Deployment.aggregate([
      { $match: { ownerUserId: ownerId, startedAt: { $gte: monthStart }, status: 'success' } },
      { $group: { _id: null, total: { $sum: '$durationSeconds' } } }
    ]).then(r => r[0]?.total || 0).catch(() => 0);
    if (usedSeconds >= uploadPlanLimits.monthlyBuildSeconds) {
      return res.status(403).json({
        error: `Monthly build-time quota reached (${usedSeconds}s / ${uploadPlanLimits.monthlyBuildSeconds}s). Resets at the start of next month.`,
        buildQuotaReached: true
      });
    }
  }

  const uploadRepoUrl = `upload://${cleanSub}`;
  const now = new Date().toISOString();

  // Build the project object — compatible with workspace.projects format
  const projectRecord = {
    id: cleanSub,
    _id: cleanSub,
    name,
    subdomain: cleanSub,
    repoUrl: uploadRepoUrl,
    branch: 'upload',
    installCmd: installCmd || 'npm install',
    buildCmd: buildCmd || '',
    startCmd: startCmd || '',
    outputDir: outputDir || 'dist',
    nodeVer: nodeVer || '20',
    // [FIX] Was `siteType || 'static'`. runUploadBuild's own dispatcher
    // (buildRunner.js) only runs its real auto-detect (autoDetectUploadServerApp)
    // when siteType is neither 'server' nor 'static' -- forcing it to
    // 'static' here short-circuited that check before it ever ran, exactly
    // like the two GitHub-deploy defaults fixed earlier tonight (bf9bca5).
    // Reproduced live: samz-demo-zip deployed via joytree_deploy_from_zip
    // with no siteType given, "succeeded", but the actual Express app was
    // never detected or started -- it silently ran as a static site instead.
    siteType: siteType || '',
    appPort,
    billingPlan: planKey,
    memoryLimit: runtimeProfile.memoryLimit,
    cpuShares: runtimeProfile.cpuShares,
    memorySwap: runtimeProfile.memorySwap,
    envVars: envVars || {},
    isDockerfileDeploy: false,
    isWorker: false,
    ownerUserId: userId,
    uploadProjectId: projectId,
    uploadFilesDir: filesDir,
    source: 'upload',
    status: 'building',
    createdAt: now,
    updatedAt: now
  };

  // Build deployment record
  const deployId = 'upload_dep_' + Date.now();
  const deploymentRecord = {
    id: deployId,
    _id: deployId,
    projectId: cleanSub,
    projectName: name,
    branch: 'upload',
    status: 'pending',
    source: 'upload',
    triggerSha: '',
    logs: [],
    startedAt: now
  };

  // Save project + deployment to Firebase workspace (primary) and localAuth (fallback)
  const userForFirebase = await enrichAuthUser(req.user);
  try {
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
    ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
    // Upsert project
    const existingProjIdx = ws.projects.findIndex(p => p.subdomain === cleanSub || p.id === cleanSub);
    if (existingProjIdx >= 0) ws.projects[existingProjIdx] = { ...ws.projects[existingProjIdx], ...projectRecord };
    else ws.projects.push(projectRecord);
    // Add deployment
    ws.deployments.unshift(deploymentRecord);
    if (ws.deployments.length > 100) ws.deployments = ws.deployments.slice(0, 100);
    await writeWorkspaceToFirebase(userForFirebase, ws);
    // Also mirror to localAuth
    const localUser = localAuth.users.find(u => String(u.id || u._id || '') === userId || u.email === userForFirebase.email);
    if (localUser) {
      localUser.workspace = ws;
      saveLocalAuth();
    }
  } catch(e) {
    console.warn('[upload-deploy] Firebase workspace save failed:', e.message);
  }

  // Also try MongoDB as secondary store (non-fatal if it fails)
  let mongoProject = { _id: cleanSub, save: async()=>{} };
  let mongoDeployment = { _id: deployId, status: 'pending', logs: [], save: async()=>{} };
  try {
    mongoProject = await Project.findOneAndUpdate(
      { subdomain: cleanSub },
      { ...projectRecord, updatedAt: new Date() },
      { upsert: true, new: true }
    );
  } catch(_e) { /* MongoDB offline — Firebase is primary */ }
  try {
    mongoDeployment = await new Deployment({
      projectId: mongoProject._id || cleanSub, projectName: name,
      branch: 'upload', status: 'pending', source: 'upload', triggerSha: ''
    }).save();
  } catch(_e) { /* MongoDB offline — use local deploy object */ }

  const finalDeployId = (mongoDeployment?._id || deployId).toString();
  deployStopRequests.delete(finalDeployId);

  res.json({ ok: true, deployId: finalDeployId, projectId: cleanSub,
             message: 'Upload deployment started', source: 'upload',
             liveUrl: `https://${cleanSub}.${BASE_DOMAIN}` });

  // Async build
  const buildStart = Date.now();
  const _uploadOwnerRoomId = String(req.user?._id || req.user?.id || '');
  const _uploadEmitTarget = _uploadOwnerRoomId ? io.to('user:' + _uploadOwnerRoomId) : io;
  const emit = (event, data) => _uploadEmitTarget.emit(event, { deployId: finalDeployId, projectId: cleanSub, source: 'upload', ...data });

  // Helper to persist deployment status to Firebase
  async function saveDeployStatus(status, extra = {}, projectPatch = {}) {
    try {
      const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
      ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
      const idx = ws.deployments.findIndex(d => d.id === deployId || d.id === finalDeployId);
      const updated = { ...deploymentRecord, ...extra, status, id: finalDeployId, _id: finalDeployId };
      if (idx >= 0) ws.deployments[idx] = updated; else ws.deployments.unshift(updated);
      // Update project status too
      ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
      const pIdx = ws.projects.findIndex(p => p.subdomain === cleanSub);
      // [FIX] projectPatch lets callers correct fields determined only after
      // the build actually ran -- specifically siteType, which the pre-build
      // record could only guess at for auto-detect deploys. Without this,
      // a correctly auto-detected server app kept showing siteType:"static"
      // in the dashboard/API forever, since nothing ever wrote the real
      // resolved type back after a successful build.
      if (pIdx >= 0) ws.projects[pIdx] = { ...ws.projects[pIdx], ...projectPatch, status, updatedAt: new Date().toISOString() };
      await writeWorkspaceToFirebase(userForFirebase, ws);
      const localUser = localAuth.users.find(u => String(u.id || u._id || '') === userId || u.email === userForFirebase.email);
      if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
    } catch(_e) {}
    // MongoDB secondary
    try { if (mongoDeployment?.save) { mongoDeployment.status = status; await mongoDeployment.save(); } } catch(_e) {}
  }

  await saveDeployStatus('building');

  (async () => {
    // ── Resolve the user's email for notifications ──────────────────────────
    const uploadNotifyEmail = req.user?.email || '';
    const uploadLiveUrl = `https://${cleanSub}.${BASE_DOMAIN}`;
    // Source label used throughout upload-deploy emails
    const uploadSourceLabel = 'upload';

    try {
      emit('build:log', { line: `\x1b[36m[Joytree]\x1b[0m Building \x1b[1m${name}\x1b[0m (from upload)` });
      emit('build:log', { line: `\x1b[90mSource: uploaded archive  Project: ${cleanSub}\x1b[0m` });
      emit('build:log', { line: `\x1b[90mTarget: ${uploadLiveUrl}\x1b[0m` });
      emit('build:log', { line: '' });

      // ── Deployment-started email ──────────────────────────────────────────
      const notifyUploadStart = await sendDeploymentStatusEmail({
        userEmail: uploadNotifyEmail,
        projectName: name,
        subdomain: cleanSub,
        branch: 'upload',
        status: 'success',
        duration: 0,
        source: uploadSourceLabel,
        liveUrl: uploadLiveUrl,
        repoUrl: `Uploaded archive — ${cleanSub}`,
        buildStatus: 'building',
        deployStatus: 'in_progress',
        memoryLimit: String(projectRecord.memoryLimit || ''),
        cpuShares: String(projectRecord.cpuShares || ''),
        deployedAt: now,
        phase: 'started'
      }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
      if (!notifyUploadStart.ok && !notifyUploadStart.skipped) {
        emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment start email could not be sent (${notifyUploadStart.reason || 'unknown'}).` });
      }

      const { runUploadBuild } = require('./buildRunner');
      const buildResult = await runUploadBuild({
        deployId: finalDeployId, project: projectRecord, sitesDir: SITES_DIR, tmpDir: TMP_DIR,
        appPort, emit, uploadFilesDir: filesDir,
        onLog: line => { deploymentRecord.logs = deploymentRecord.logs || []; deploymentRecord.logs.push(line); }
      });
      if (buildResult && buildResult.siteType && buildResult.siteType !== projectRecord.siteType) {
        projectRecord.siteType = buildResult.siteType;
      }

      const duration = Math.round((Date.now() - buildStart) / 1000);
      const cf = await registerSubdomain(cleanSub).catch(() => null);
      const finalLiveUrl = cf?.url || uploadLiveUrl;
      if (cf?.url) emit('build:log', { line: `\x1b[32m[CF]\x1b[0m Live at: \x1b[1m${cf.url}\x1b[0m` });
      emit('build:log', { line: `\n\x1b[32m✓ Deployed in ${duration}s\x1b[0m` });
      const endedAt = new Date().toISOString();
      await saveDeployStatus('success', { duration, endedAt, liveUrl: finalLiveUrl },
        buildResult && buildResult.siteType ? { siteType: buildResult.siteType } : {});

      // ── Deployment-success email ──────────────────────────────────────────
      const notifyUploadSuccess = await sendDeploymentStatusEmail({
        userEmail: uploadNotifyEmail,
        projectName: name,
        subdomain: cleanSub,
        branch: 'upload',
        status: 'success',
        duration,
        source: uploadSourceLabel,
        liveUrl: finalLiveUrl,
        repoUrl: `Uploaded archive — ${cleanSub}`,
        buildStatus: 'success',
        deployStatus: 'success',
        memoryLimit: String(projectRecord.memoryLimit || ''),
        cpuShares: String(projectRecord.cpuShares || ''),
        deployedAt: endedAt
      }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
      if (!notifyUploadSuccess.ok && !notifyUploadSuccess.skipped) {
        emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment success email could not be sent (${notifyUploadSuccess.reason || 'unknown'}).` });
      }

      emit('build:done', { status: 'success', duration, liveUrl: finalLiveUrl });
      emit('runtime:ready', { subdomain: cleanSub, liveUrl: finalLiveUrl });
    } catch(e) {
      const safeErr = String(e?.message || 'unknown error').slice(0, 400);
      const duration = Math.round((Date.now() - buildStart) / 1000);
      const endedAt = new Date().toISOString();
      emit('build:log', { line: `\x1b[31m[Joytree]\x1b[0m Build failed: ${safeErr}` });
      await saveDeployStatus('failed', { duration, endedAt, error: safeErr });

      // ── Deployment-failure email ──────────────────────────────────────────
      const notifyUploadFailure = await sendDeploymentStatusEmail({
        userEmail: uploadNotifyEmail,
        projectName: name,
        subdomain: cleanSub,
        branch: 'upload',
        status: 'failed',
        duration,
        source: uploadSourceLabel,
        liveUrl: uploadLiveUrl,
        repoUrl: `Uploaded archive — ${cleanSub}`,
        errorMessage: safeErr,
        buildStatus: 'failed',
        deployStatus: 'failed',
        memoryLimit: String(projectRecord.memoryLimit || ''),
        cpuShares: String(projectRecord.cpuShares || ''),
        deployedAt: endedAt
      }).catch(() => ({ ok: false, skipped: false, reason: 'email_send_exception' }));
      if (!notifyUploadFailure.ok && !notifyUploadFailure.skipped) {
        emit('build:log', { line: `\x1b[33m[Resend]\x1b[0m Deployment failure email could not be sent (${notifyUploadFailure.reason || 'unknown'}).` });
      }

      emit('build:done', { status: 'failed', duration });
    }
  })();
});

// ── Helper: extract archive (zip or tar.gz) ──────────────────────────────────
async function extractUploadedArchive(archivePath, destDir) {
  try { fs.mkdirSync(destDir, { recursive: true }); } catch(e) {}

  const filename = path.basename(archivePath).toLowerCase();
  const isZip = filename.endsWith('.zip');
  const isTar = filename.endsWith('.tar.gz') || filename.endsWith('.tgz');

  if (!isZip && !isTar) throw new Error('Unsupported archive format. Only .zip and .tar.gz supported.');

  // Try to clean dest first
  try { fs.rmSync(destDir, { recursive: true, force: true }); } catch(e) {}
  try { fs.mkdirSync(destDir, { recursive: true }); } catch(e) {}

  await new Promise((resolve, reject) => {
    const { spawn } = require('child_process');
    let cmd, args;
    if (isZip) {
      cmd = 'unzip'; args = ['-o', '-q', archivePath, '-d', destDir];
    } else {
      cmd = 'tar'; args = ['xzf', archivePath, '-C', destDir, '--strip-components=0'];
    }
    const child = spawn(cmd, args, { stdio: 'pipe' });
    let stderr = '';
    child.stderr.on('data', d => stderr += d.toString());
    child.on('close', code => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} failed (exit ${code}): ${stderr.slice(0,200)}`));
    });
    child.on('error', e => reject(new Error(`Cannot run ${cmd}: ${e.message}. Is it installed?`)));
    setTimeout(() => { child.kill('SIGTERM'); reject(new Error('Extraction timed out after 60s')); }, 60000);
  });

  // Strip single top-level wrapper directory (like GitHub zip exports)
  const entries = fs.readdirSync(destDir);
  if (entries.length === 1) {
    const single = path.join(destDir, entries[0]);
    if (fs.statSync(single).isDirectory()) {
      // Move contents up
      const tmpMove = destDir + '_mv_' + Date.now();
      fs.renameSync(single, tmpMove);
      for (const e of fs.readdirSync(tmpMove)) {
        fs.renameSync(path.join(tmpMove, e), path.join(destDir, e));
      }
      try { fs.rmdirSync(tmpMove); } catch(e) {}
    }
  }

  // Walk and collect files (skip node_modules, .git, binaries)
  const SKIP_DIRS = new Set(['node_modules','.git','.svn','__pycache__','.cache','.next','.nuxt','dist','build','.turbo']);
  const BINARY_EXTS_EXTRACT = new Set(['png','jpg','jpeg','gif','webp','ico','woff','woff2','ttf','eot','pdf','zip','tar','gz','mp4','mp3','wav','bin','exe','so','dll','pyc','class','o','a']);
  const MAX_FILE_SIZE = 5 * 1024 * 1024; // 5MB per file — handles large single-file HTML apps
  const MAX_FILES = 2000;

  const files = {};
  let unzippedSize = 0;

  function walk(dir, prefix='') {
    if (Object.keys(files).length >= MAX_FILES) return;
    let entries;
    try { entries = fs.readdirSync(dir); } catch(e) { return; }
    for (const e of entries) {
      if (e.startsWith('.') && e !== '.env' && e !== '.env.example' && e !== '.eslintrc' && e !== '.prettierrc') continue;
      const full = path.join(dir, e);
      const rel = prefix ? prefix + '/' + e : e;
      try {
        const stat = fs.lstatSync(full);
        if (stat.isDirectory()) {
          if (!SKIP_DIRS.has(e)) walk(full, rel);
        } else if (stat.isFile()) {
          unzippedSize += stat.size;
          const ext = e.split('.').pop().toLowerCase();
          if (!BINARY_EXTS_EXTRACT.has(ext) && stat.size <= MAX_FILE_SIZE) {
            try { files[rel] = fs.readFileSync(full, 'utf8'); } catch(e) { files[rel] = '[Binary or unreadable file]'; }
          }
        }
      } catch(e) {}
    }
  }
  walk(destDir);

  const fileCount = Object.keys(files).length;
  // Detect dominant language
  const counts = {};
  for (const p of Object.keys(files)) {
    const ext = p.split('.').pop().toLowerCase();
    counts[ext] = (counts[ext]||0)+1;
  }
  const langMap = { js:'JavaScript', ts:'TypeScript', py:'Python', html:'HTML', css:'CSS', go:'Go', rs:'Rust', php:'PHP', rb:'Ruby', java:'Java', cpp:'C++', vue:'Vue', svelte:'Svelte', jsx:'React', tsx:'TypeScript' };
  const topExt = Object.entries(counts).filter(([k])=>langMap[k]).sort((a,b)=>b[1]-a[1])[0];
  const lang = topExt ? topExt[0] : 'zip';

  return { files, fileCount, lang, unzippedSize };
}




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

// ── Re-deploy an upload-source project using its stored files ───────────────
// Called by the "New Version" button for upload projects. Re-runs the build
// from the already-extracted files without requiring the user to re-upload.
app.post('/api/projects/:id/redeploy-upload', requireAuth, async (req, res) => {
  const userId = String(req.user?._id || req.user?.id || 'anon');
  const projectId = String(req.params.id || '').trim();

  const userForFirebase = await enrichAuthUser(req.user);
  let project = null;
  try {
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    project = (ws.projects || []).find(p => p.id === projectId || p.subdomain === projectId || String(p._id||'') === projectId);
  } catch(_) {}
  if (!project) {
    const localUser = localAuth.users.find(u => String(u.id||u._id||'') === userId);
    project = ((localUser?.workspace?.projects) || []).find(p => p.id === projectId || p.subdomain === projectId);
  }
  if (!project) return res.status(404).json({ error: 'Project not found' });
  if (project.source !== 'upload' && !String(project.repoUrl||'').startsWith('upload://')) {
    return res.status(400).json({ error: 'This endpoint is only for upload-source projects.' });
  }

  const cleanSub = project.subdomain || projectId;
  const storedProjectId = project.uploadProjectId || cleanSub;
  const filesDir = project.uploadFilesDir || path.join(UPLOADS_DIR, userId, storedProjectId, 'files');

  if (!fs.existsSync(filesDir)) {
    return res.status(404).json({
      error: 'Stored project files not found. Please upload a new archive.',
      filesNotFound: true
    });
  }

  const isServerApp = project.siteType === 'server' || !!String(project.startCmd||'').trim();
  let appPort = 0;
  if (isServerApp) {
    try { appPort = getOrAssignPort(cleanSub); } catch(e) { return res.status(500).json({ error: e.message }); }
  }

  const planKey = await getUserPlanKey(req.user);
  const runtimeProfile = getRuntimeProfileForPlan(planKey);
  const now = new Date().toISOString();
  const deployId = 'upload_dep_' + Date.now();

  const projectRecord = {
    ...project, appPort, billingPlan: planKey,
    memoryLimit: runtimeProfile.memoryLimit,
    cpuShares: runtimeProfile.cpuShares,
    memorySwap: runtimeProfile.memorySwap,
    // [FIX] filesDir (resolved a few lines up, with a fallback path for
    // records that never had this field set at all) was used to run the
    // actual build but never saved back here -- projectRecord only ever
    // spread the OLD project object, so if uploadFilesDir was missing
    // before, it stayed missing after every subsequent redeploy too,
    // forever. Confirmed live: a project redeployed today still failed
    // Remix with "Original project files are no longer available" because
    // its persisted record had never actually carried this field.
    uploadFilesDir: filesDir,
    status: 'building', updatedAt: now
  };

  const deploymentRecord = {
    id: deployId, _id: deployId,
    projectId: cleanSub, projectName: project.name,
    branch: 'upload', status: 'pending', source: 'upload',
    triggerSha: '', logs: [], startedAt: now
  };

  try {
    const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
    ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
    ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
    const pIdx = ws.projects.findIndex(p => p.subdomain === cleanSub || p.id === cleanSub);
    if (pIdx >= 0) ws.projects[pIdx] = { ...ws.projects[pIdx], ...projectRecord };
    else ws.projects.push(projectRecord);
    ws.deployments.unshift(deploymentRecord);
    if (ws.deployments.length > 100) ws.deployments = ws.deployments.slice(0, 100);
    await writeWorkspaceToFirebase(userForFirebase, ws);
    const localUser = localAuth.users.find(u => String(u.id||u._id||'') === userId || u.email === userForFirebase.email);
    if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
  } catch(_) {}

  let mongoDeployment = { _id: deployId, status: 'pending', logs: [], save: async()=>{} };
  try {
    mongoDeployment = await new Deployment({
      projectId: cleanSub, projectName: project.name,
      branch: 'upload', status: 'pending', source: 'upload', triggerSha: ''
    }).save();
  } catch(_) {}

  const finalDeployId = String(mongoDeployment?._id || deployId);
  deployStopRequests.delete(finalDeployId);

  res.json({ ok: true, deployId: finalDeployId, projectId: cleanSub,
             message: 'Redeployment started from stored files', source: 'upload',
             liveUrl: `https://${cleanSub}.${BASE_DOMAIN}` });

  const buildStart = Date.now();
  const _redeployOwnerRoomId = String(req.user?._id || req.user?.id || '');
  const _redeployTarget = _redeployOwnerRoomId ? io.to('user:' + _redeployOwnerRoomId) : io;
  const emit = (event, data) => _redeployTarget.emit(event, { deployId: finalDeployId, projectId: cleanSub, source: 'upload', ...data });

  async function saveRedeployStatus(status, extra = {}, projectPatch = {}) {
    try {
      const ws = (await readWorkspaceFromFirebase(userForFirebase)) || {};
      ws.deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
      const idx = ws.deployments.findIndex(d => d.id === deployId || d.id === finalDeployId);
      const updated = { ...deploymentRecord, ...extra, status, id: finalDeployId, _id: finalDeployId };
      if (idx >= 0) ws.deployments[idx] = updated; else ws.deployments.unshift(updated);
      ws.projects = Array.isArray(ws.projects) ? ws.projects : [];
      const pIdx2 = ws.projects.findIndex(p => p.subdomain === cleanSub);
      if (pIdx2 >= 0) ws.projects[pIdx2] = { ...ws.projects[pIdx2], ...projectPatch, status, updatedAt: new Date().toISOString() };
      await writeWorkspaceToFirebase(userForFirebase, ws);
      const localUser = localAuth.users.find(u => String(u.id||u._id||'') === userId || u.email === userForFirebase.email);
      if (localUser) { localUser.workspace = ws; saveLocalAuth(); }
    } catch(_) {}
    try { if (mongoDeployment?.save) { mongoDeployment.status = status; await mongoDeployment.save(); } } catch(_) {}
  }

  await saveRedeployStatus('building');

  (async () => {
    const liveUrl = `https://${cleanSub}.${BASE_DOMAIN}`;
    try {
      emit('build:log', { line: `\x1b[36m[Joytree]\x1b[0m Redeploying \x1b[1m${project.name}\x1b[0m (stored upload files)` });
      emit('build:log', { line: `\x1b[90mTarget: ${liveUrl}\x1b[0m` });
      emit('build:log', { line: '' });
      const { runUploadBuild } = require('./buildRunner');
      const buildResult = await runUploadBuild({
        deployId: finalDeployId, project: projectRecord, sitesDir: SITES_DIR, tmpDir: TMP_DIR,
        appPort, emit, uploadFilesDir: filesDir,
        onLog: line => { deploymentRecord.logs = deploymentRecord.logs || []; deploymentRecord.logs.push(line); }
      });
      if (buildResult && buildResult.siteType && buildResult.siteType !== projectRecord.siteType) {
        projectRecord.siteType = buildResult.siteType;
      }
      const duration = Math.round((Date.now() - buildStart) / 1000);
      const cf = await registerSubdomain(cleanSub).catch(() => null);
      const finalLiveUrl = cf?.url || liveUrl;
      emit('build:log', { line: `\n\x1b[32m✓ Redeployed in ${duration}s\x1b[0m` });
      await saveRedeployStatus('success', { duration, endedAt: new Date().toISOString(), liveUrl: finalLiveUrl },
        buildResult && buildResult.siteType ? { siteType: buildResult.siteType } : {});
      emit('build:done', { status: 'success', duration, liveUrl: finalLiveUrl });
      emit('runtime:ready', { subdomain: cleanSub, liveUrl: finalLiveUrl });
    } catch(e) {
      const safeErr = String(e?.message || 'unknown error').slice(0, 400);
      const duration = Math.round((Date.now() - buildStart) / 1000);
      emit('build:log', { line: `\x1b[31m[Joytree]\x1b[0m Build failed: ${safeErr}` });
      await saveRedeployStatus('failed', { duration, endedAt: new Date().toISOString(), error: safeErr });
      emit('build:done', { status: 'failed', duration });
    }
  })();
});


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
    // FIX: extract meta fields sent by domain store checkout
    const meta = req.body?.meta && typeof req.body.meta === 'object' ? req.body.meta : {};
    const metaType = String(meta.type || '').trim().toLowerCase();
    const metaDomain = String(meta.domain || '').trim().toLowerCase();
    const metaProjectId = String(meta.projectId || '').trim();
    if (!firstName) return res.status(400).json({ error: 'firstName is required' });
    if (!lastName) return res.status(400).json({ error: 'lastName is required' });
    if (!email) return res.status(400).json({ error: 'email is required' });
    if (!Number.isFinite(amountKobo) || amountKobo < 100) return res.status(400).json({ error: 'amountKobo must be at least 100' });

    // Detect Ghana MoMo provider from number prefix
    const mtnPrefixes = ['024','054','055','059','025'];
    const telecelPrefixes = ['020','050'];
    const atPrefixes = ['027','057','026','056'];
    const phonePrefix = phone.slice(0,3);
    const momoProvider = mtnPrefixes.includes(phonePrefix) ? 'mtn'
      : telecelPrefixes.includes(phonePrefix) ? 'vod'
      : atPrefixes.includes(phonePrefix) ? 'tgo'
      : 'mtn'; // default to mtn

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
        mobile_money: { phone, provider: momoProvider },
        metadata: {
          plan,
          // FIX: pass type/domain/projectId so webhook can distinguish domain vs subscription payments
          ...(metaType     ? { type: metaType }           : {}),
          ...(metaDomain   ? { domain: metaDomain }       : {}),
          ...(metaProjectId? { projectId: metaProjectId } : {}),
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

    // [FIX] Apply the new plan's resource limits to the user's already-running
    // app containers right away — see applyPlanUpgradeToRunningApps() for why
    // this can't just wait for the next deploy. Best-effort; never blocks the
    // billing response.
    if (metadataPlan) {
      applyPlanUpgradeToRunningApps(customerEmail || requestEmail, metadataPlan)
        .catch(e => console.warn('[Billing] applyPlanUpgradeToRunningApps (verify) failed:', e.message));
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
    const metaType = String(data.metadata?.type || '').toLowerCase();
    const metaDomain = String(data.metadata?.domain || '').toLowerCase().trim();

    if (!reference || !email) return res.status(400).json({ error: 'Missing reference or customer email' });

    // ── Domain registration payment ──────────────────────────────
    // FIX: also catch domain payments where plan='domain_registration' even if metaType wasn't stored
    const isDomainPayment = (metaType === 'domain_registration' && metaDomain) ||
                            (plan === 'domain_registration');
    if (isDomainPayment) {
      // Only attempt domain registration when we actually have the domain name
      if (metaDomain) {
        const userLike = { email };
        const ws = (await readWorkspaceFromFirebase(userLike)) || {};
        ws.registeredDomains = Array.isArray(ws.registeredDomains) ? ws.registeredDomains : [];
        const alreadyDone = ws.registeredDomains.find(d => d.domain === metaDomain && d.paystackRef === reference);
        if (!alreadyDone) {
          // Try NameSilo registration
          let namesiloCode = null;
          if (NAMESILO_API_KEY) {
            try {
              const contactRes = await namesiloCall('contactList', {});
              const contactId  = contactRes?.contact?.[0]?.contact_id || '';
              const regData = await namesiloCall('registerDomain', { domain: metaDomain, years: 1, private: 1, auto_renew: 0, contact_id: contactId });
              namesiloCode = Number(regData?.code || 0);
            } catch(e) { console.warn('[DomainStore/webhook] NameSilo error:', e.message); }
          }
          ws.registeredDomains.unshift({
            domain: metaDomain, status: 'active',
            projectId: data.metadata?.projectId || null,
            registeredAt: new Date().toISOString(),
            expiresAt: new Date(Date.now() + 365*24*3600*1000).toISOString(),
            paystackRef: reference, namesiloCode
          });
          await writeWorkspaceToFirebase(userLike, ws);
        }
      }
      // FIX: always return here — never let domain payments fall through to subscription handler
      return res.json({ ok: true, type: 'domain_registration', domain: metaDomain || null });
    }

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

    // [FIX] Same as the verify path — apply the new plan's resource limits to
    // already-running containers immediately rather than waiting for the
    // user's next deploy. Best-effort; never blocks the webhook response
    // (Paystack retries on non-2xx, and the subscription is already saved).
    if (plan) {
      applyPlanUpgradeToRunningApps(email, plan)
        .catch(e => console.warn('[Billing] applyPlanUpgradeToRunningApps (webhook) failed:', e.message));
    }

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

// [FIX] execSync removed — runDocker() now uses the shared async execP-style
// helper (_execAsync) so Docker operations never block the event loop.

// [FIX] Memory unit helpers used by DB engine startArgs and applyMemoryLimits.
function parseMemToBytes(str) {
  if (!str) return 256 * 1024 * 1024;
  const m = String(str).toLowerCase().match(/^([0-9.]+)([kmg]?)b?$/);
  if (!m) return 256 * 1024 * 1024;
  const n = parseFloat(m[1]);
  const u = m[2];
  if (u === 'g') return Math.round(n * 1024**3);
  if (u === 'm') return Math.round(n * 1024**2);
  if (u === 'k') return Math.round(n * 1024);
  return Math.round(n);
}
function bytesToDockerMem(bytes) {
  if (bytes >= 1024**3) return (bytes / 1024**3).toFixed(1).replace(/\.0$/, '') + 'g';
  return Math.round(bytes / 1024**2) + 'm';
}

// [FIX] runDocker()/exec() runs commands through a real shell (`/bin/sh -c`),
// so any user-supplied value interpolated into a command string with only a
// bare `"..."` wrapper is NOT actually safe — an embedded `$`, backtick, or
// `"` is still interpreted by the shell. For DB passwords specifically this
// silently corrupts what actually gets set inside the container vs. what's
// stored in the DB record and used to build the connection string, causing
// exactly the kind of "everything looks fine but auth/connect fails" bug we
// hit with the migration-test container. For db.image/db.volume (both
// directly user-suppliable at creation) it's a straightforward command
// injection vector. Wrapping in single quotes and escaping any embedded
// single quotes (the standard POSIX-safe technique) neutralizes all shell
// metacharacters regardless of content.
function shellQuote(str) {
  return `'${String(str).replace(/'/g, `'\\''`)}'`;
}

const DB_ENGINE_CONFIG = {
  // [FIX] Each engine now includes startArgs() which passes memory-limiting
  // flags directly to the DB process inside the container. Without these,
  // even with Docker --memory limits, the DB's internal cache (WiredTiger
  // for MongoDB, InnoDB for MySQL/MariaDB) grabs a large % of total system
  // RAM — triggering OOM kills or heavy swap on your VPS.
  mongodb:  { image: 'mongo:7',     defaultPort: 27017,
    envVars: (u,p,d) => [`MONGO_INITDB_ROOT_USERNAME=${u}`, `MONGO_INITDB_ROOT_PASSWORD=${p}`, `MONGO_INITDB_DATABASE=${d}`],
    startArgs: (memStr) => {
      const bytes = parseMemToBytes(memStr || '256m');
      // [FIX] MongoDB hard-rejects any --wiredTigerCacheSizeGB below 0.25 at
      // startup ("must be greater than or equal to 0.25"), but this floor was
      // previously 0.1 — below Mongo's own minimum. At the default 256m
      // container memory (0.6 * 256m ≈ 0.15GB → rounds to 0.2), mongod was
      // computing a value that passed OUR floor but failed MONGO's real one,
      // so it crashed on every single boot. Docker's --restart unless-stopped
      // then endlessly relaunched it, and containerStatus() maps 'restarting'
      // to 'running' (intentionally, for MySQL's normal first-boot restarts),
      // so the dashboard showed "RUNNING" the whole time even though mongod
      // itself never once came up. Any TCP client (like the migration engine)
      // connecting during a restart cycle got an instantly-closed empty
      // socket, not a real handshake -- hence "Input must be at least 5
      // bytes, got 0 bytes" from the driver, at every attempt.
      const cacheGb = Math.max(0.25, Math.round((bytes * 0.6) / (1024**3) * 10) / 10);
      return `--wiredTigerCacheSizeGB ${cacheGb}`;
    }
  },
  postgres: { image: 'postgres:16', defaultPort: 5432,
    envVars: (u,p,d) => [`POSTGRES_USER=${u}`, `POSTGRES_PASSWORD=${p}`, `POSTGRES_DB=${d}`,
      `POSTGRES_INITDB_ARGS=--encoding=UTF8`],
    startArgs: (memStr) => {
      const bytes = parseMemToBytes(memStr || '256m');
      const sharedBufs = Math.max(16, Math.floor(bytes * 0.25 / (1024**2))) + 'MB';
      return `-c shared_buffers=${sharedBufs} -c max_connections=50 -c effective_cache_size=${Math.floor(bytes*0.5/(1024**2))}MB`;
    }
  },
  mysql:    { image: 'mysql:8',     defaultPort: 3306,
    startArgs: (memStr) => {
      const bytes = parseMemToBytes(memStr || '256m');
      const bufPool = Math.max(32, Math.floor(bytes * 0.4 / (1024**2))) + 'M';
      return `--innodb-buffer-pool-size=${bufPool} --max-connections=50`;
    },
    envVars: (u,p,d) => {
      const vars = [`MYSQL_ROOT_PASSWORD=${p}`, `MYSQL_DATABASE=${d}`];
      if (u && u !== 'root') vars.push(`MYSQL_USER=${u}`, `MYSQL_PASSWORD=${p}`);
      return vars;
    }
  },
  mariadb:  { image: 'mariadb:11',  defaultPort: 3306,
    startArgs: (memStr) => {
      const bytes = parseMemToBytes(memStr || '256m');
      const bufPool = Math.max(32, Math.floor(bytes * 0.4 / (1024**2))) + 'M';
      return `--innodb-buffer-pool-size=${bufPool} --max-connections=50`;
    },
    envVars: (u,p,d) => {
      const vars = [`MARIADB_ROOT_PASSWORD=${p}`, `MARIADB_DATABASE=${d}`];
      if (u && u !== 'root') vars.push(`MARIADB_USER=${u}`, `MARIADB_PASSWORD=${p}`);
      return vars;
    }
  },
  redis:    { image: 'redis:7',     defaultPort: 6379,
    envVars: (_u,p) => [],  // password handled via startArgs
    startArgs: (memStr, _u, p) => {
      const bytes = parseMemToBytes(memStr || '128m');
      const maxMem = Math.floor(bytes * 0.8 / (1024**2)) + 'mb';
      return (p ? `--requirepass ${shellQuote(p)} ` : '') + `--maxmemory ${maxMem} --maxmemory-policy allkeys-lru`;
    }
  }
};

// Find a free port in range 14000–15000 avoiding already-used ones
// [FIX] async — execSync('docker ps ...') blocked the event loop.
async function findFreeDbPort(preferredPort) {
  try {
    const used = await execP("docker ps --format '{{.Ports}}' 2>/dev/null || echo ''");
    const usedPorts = new Set([...used.matchAll(/:(\d+)->/g)].map(m => Number(m[1])));
    // [FIX] Recreate passes the database's previous port here so a rebuild
    // preserves its connection string. But that port isn't guaranteed to
    // still be free -- another database can grab it in the window between
    // the old container being removed and the new one starting (exactly
    // what happened when a freshly-created Postgres database took over
    // migration-test's old port 14000 mid-recreate). Validate it against
    // what's actually in use right now rather than trusting it blindly.
    if (preferredPort && !usedPorts.has(Number(preferredPort))) return Number(preferredPort);
    for (let p = 14000; p <= 15000; p++) {
      if (!usedPorts.has(p)) return p;
    }
  } catch { /* fall through */ }
  return 14000 + Math.floor(Math.random() * 900);
}

// ── Apply plan upgrade to already-running app containers ────────────────────
// [FIX] Previously, a successful subscription payment only updated
// ws.settings.billingPlan in Firebase. The new memory/CPU allocation
// (PLAN_RUNTIME_PROFILES in buildRunner.js) only took effect the NEXT time
// each project was deployed, because `-m`/`--cpu-shares` are set once at
// `docker run` time and Docker never changes them on its own afterwards. A
// user could pay for "pro" and their already-running server app (e.g.
// Uptime Kuma) would silently stay capped at the free tier's 870MB/384
// shares until they happened to redeploy — which for an always-on server
// app could be weeks.
//
// This applies the new plan's memory/CPU/swap limits to every running app
// container for the user IMMEDIATELY via `docker update`, and persists the
// new values onto each Project record so the next deploy starts from the
// right baseline too. Best-effort and non-blocking: the subscription record
// in Firebase is the source of truth regardless, so failures here are only
// logged — a project that misses the live update just catches up on its
// next deploy.
async function applyPlanUpgradeToRunningApps(email, planKey) {
  const plan = String(planKey || '').trim().toLowerCase();
  if (!plan) return;
  try {
    if (!isDbReady()) return;
    const user = await User.findOne({ email: String(email || '').trim().toLowerCase() }).select('_id').lean();
    if (!user) return;
    const ownerUserId = String(user._id);

    const profile = getPlanRuntimeProfile(plan);
    const memory = normalizeMemoryLimit(profile.memoryLimit);
    const cpuShares = Number(profile.cpuShares) || 0;
    const memorySwap = String(profile.memorySwap || '').trim();

    const projects = await Project.find({ ownerUserId }).select('_id subdomain').lean();
    for (const project of projects) {
      const containerName = `db-${String(project.subdomain || '').toLowerCase().replace(/[^a-z0-9-]/g, '')}`;
      if (containerName === 'db-') continue;

      // Persist the new plan-tied resource values so the NEXT deploy (and
      // getRuntimeConfig() in buildRunner) start from the upgraded baseline,
      // even if the live `docker update` below fails or the app isn't
      // currently running.
      try {
        await Project.updateOne({ _id: project._id }, { $set: {
          billingPlan: plan, memoryLimit: memory, cpuShares, memorySwap
        }});
      } catch (e) {
        console.warn(`[Billing] Failed to persist plan upgrade on project ${project.subdomain}:`, e.message);
      }

      // Apply to the live container right now, without waiting for a
      // redeploy. -m and --memory-swap are passed together so Docker can
      // order the underlying cgroup writes correctly when both increase
      // (as they always do on an upgrade).
      const memArgs = memorySwap ? `-m ${memory} --memory-swap ${memorySwap}` : `-m ${memory}`;
      const result = await runDocker(`docker update --cpu-shares ${cpuShares} ${memArgs} ${containerName}`, 15000);
      if (result.ok) {
        console.log(`[Billing] Applied '${plan}' plan resources to running container ${containerName} (${memory} RAM / ${cpuShares} CPU shares)`);
      } else if (!/No such container/i.test(result.stderr || '')) {
        // "No such container" just means the app isn't currently running —
        // not an error; it'll get the new limits whenever it next deploys.
        console.warn(`[Billing] docker update failed for ${containerName}:`, result.stderr);
      }
    }
  } catch (e) {
    console.warn('[Billing] applyPlanUpgradeToRunningApps failed:', e.message);
  }
}

// Run docker command and return { ok, stdout, stderr }
// timeoutMs defaults to 3 minutes — long enough for a cold image pull
// [FIX] async — this was the core primitive behind 26 call sites across the
// database-management routes. execSync() here blocked the entire event loop
// (and thus the reverse proxy for every deployed app) for up to `timeoutMs`,
// even with a shell-level `timeout N` wrapper, because execSync itself is
// synchronous regardless of what the child command does. Now uses exec()
// (async) so Docker calls — pulls, starts, stops, stats, logs — never freeze
// other tenants' traffic.
function runDocker(cmd, timeoutMs = 180000) {
  return new Promise((resolve) => {
    _execAsync(cmd, { encoding: 'utf8', timeout: timeoutMs, maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (!err) return resolve({ ok: true, stdout: String(stdout).trim() });
      const eStdout = String(stdout || '').trim();
      const eStderr = String(stderr || '').trim();
      // exec() also sets err.killed / err.signal === 'SIGTERM' on timeout.
      if (err.signal === 'SIGTERM' || err.killed) {
        return resolve({
          ok: false, stdout: '',
          stderr: `Docker command timed out after ${Math.round(timeoutMs / 1000)}s. ` +
                  `The image may still be pulling in the background — try again in a minute.`
        });
      }
      // With 2>&1 in the shell command, DB error text lands in stdout (not stderr).
      // Return both so the query route can surface the real DB error message.
      resolve({ ok: false, stdout: eStdout, stderr: eStderr || String(err.message || '') });
    });
  });
}

// Check if a container exists (running or stopped)
// [FIX] async — see runDocker.
async function containerExists(name) {
  // Hard 2-second cap so a slow Docker daemon never blocks the event loop
  const r = await runDocker(`timeout 2 docker inspect --format '{{.State.Status}}' ${name} 2>/dev/null || echo ''`, 4000);
  return r.stdout && r.stdout !== '';
}

// Get container running state
// Uses `timeout 2` (Unix coreutils) so a busy Docker daemon can't stall
// things longer than ~2 s per container — critical when GET /api/databases
// calls this for every DB and the client has an 8-second abort.
// [FIX] async — see runDocker.
async function containerStatus(name) {
  const r = await runDocker(`timeout 2 docker inspect --format '{{.State.Status}}' ${name} 2>/dev/null || echo 'missing'`, 4000);
  const s = (r.stdout || 'missing').trim();
  // 'restarting' is normal for MySQL 8 during first-boot init — Docker restarts
  // the process several times before settling. Treat it as running so queries
  // are not rejected with "Container is error" during that window.
  if (s === 'running' || s === 'restarting') return 'running';
  if (s === 'exited' || s === 'created' || s === 'paused' || s === 'dead') return 'stopped';
  if (s === 'missing' || s === '') return 'error';
  return 'stopped';
}

// Provision a new Docker database container
async function provisionDbContainer(db, opts = {}) {
  const cfg = DB_ENGINE_CONFIG[db.engine];
  if (!cfg) throw new Error(`Unknown engine: ${db.engine}`);

  const containerName = `jt-db-${db.name}-${String(db._id).slice(-6)}`;
  // [FIX] findFreeDbPort() now validates opts.hostPort against what's
  // actually in use before reusing it, instead of trusting it blindly --
  // see findFreeDbPort() for why that mattered in practice.
  const hostPort      = await findFreeDbPort(opts.hostPort);
  const volumePath    = db.volume || `/var/joytree-data/${containerName}`;
  const imageToUse    = db.image || cfg.image;

  // Ensure volume dir exists
  // [FIX] async fs.mkdir instead of execSync('mkdir -p ...').
  try { await fs.promises.mkdir(volumePath, { recursive: true }); } catch {}

  // ── Pre-pull the image (3-minute timeout) ──────────────────────────────────
  // docker run would pull silently but hang with no feedback. Doing it explicitly
  // here means (a) we get a clear error if the registry is unreachable and (b)
  // the subsequent docker run completes in seconds because the layers are cached.
  console.log(`[DB] Pulling image ${imageToUse} for "${db.name}"…`);
  const pullResult = await runDocker(`docker pull ${shellQuote(imageToUse)}`, 180000);
  if (!pullResult.ok) {
    throw new Error(`Failed to pull Docker image "${imageToUse}": ${pullResult.stderr}`);
  }
  console.log(`[DB] Image ${imageToUse} ready.`);

  // Build env flags
  // [FIX] Was `-e "${e}"` (naive double-quote wrap) — shellQuote() properly
  // escapes the whole KEY=VALUE pair so any special character in a
  // user-supplied password/username/db name survives the shell intact.
  const envFlags = cfg.envVars(db.user, db.pass, db.dbName)
    .map(e => `-e ${shellQuote(e)}`)
    .join(' ');

  // Redis uses a config arg for password, not just env
  // [FIX] Password + memory limits now handled via cfg.startArgs() below.
  const redisArgs = '';

  const volumeMount = db.engine === 'mongodb'  ? `/data/db`
                    : db.engine === 'postgres' ? `/var/lib/postgresql/data`
                    : db.engine === 'mysql'    ? `/var/lib/mysql`
                    : db.engine === 'redis'    ? `/data`
                    : `/data`;

  // [FIX] Memory defaults raised to 256m (was 128m — too low for MongoDB/MySQL
  // whose internal caches need headroom on top of the base process).
  // --memory-swap is now 2x memory (was equal to memory = swap entirely disabled,
  // causing OOM kills on any transient spike). Engine startArgs tell the DB
  // process itself to stay within the container's memory allocation.
  // MySQL 8 requires at least 512m to initialize without OOM crashing.
  // 256m causes a restart loop: Docker keeps killing and restarting the process.
  // [FIX] MongoDB now gets the same 512m floor as MySQL. Mongo's own hard
  // minimum for --wiredTigerCacheSizeGB is 0.25GB (256MB) -- at a 256m total
  // container limit, that leaves ~0 headroom for the mongod process itself
  // above the cache, so it would pass Mongo's config validation only to be
  // OOM-killed moments later by Docker's --memory limit, restart, repeat.
  // 512m gives the cache calc (~0.3GB at 60% of 512m) real room to breathe.
  const engineMinMem = (db.engine === 'mysql' || db.engine === 'mongodb') ? '512m' : '256m';
  const memAlloc = (() => {
    const requested = db.memory || process.env.DB_DEFAULT_MEMORY || engineMinMem;
    const reqBytes  = parseMemToBytes(requested);
    const minBytes  = parseMemToBytes(engineMinMem);
    return reqBytes >= minBytes ? requested : engineMinMem;
  })();
  const memBytes = parseMemToBytes(memAlloc);
  const swapAlloc = bytesToDockerMem(memBytes * 2);
  const engineStartArgs = cfg.startArgs ? cfg.startArgs(memAlloc, db.user, db.pass) : '';

  const cmd = [
    'docker run -d',
    `--name ${containerName}`,
    `--restart unless-stopped`,
    `-p 0.0.0.0:${hostPort}:${cfg.defaultPort}`,
    `-v ${shellQuote(volumePath + ':' + volumeMount)}`,
    `--memory="${memAlloc}"`,
    `--memory-swap="${swapAlloc}"`,
    `--pids-limit=300`,
    `--cpus="${process.env.DB_CONTAINER_CPUS || '0.75'}"`,
    `--ulimit nofile=65536:65536`,
    envFlags,
    shellQuote(imageToUse),
    engineStartArgs
  ].filter(Boolean).join(' ');

  // docker run itself (image already pulled) — 30-second timeout is plenty
  const result = await runDocker(cmd, 30000);
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
        const liveStatus = await containerStatus(db.containerName);
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

        // [FIX] Register the db-XXX subdomain as DNS-only (proxied:false) so
        // external apps can reach the raw TCP port. Without this, Cloudflare's
        // proxy silently drops all non-HTTP connections to the DB port.
        const dbSubdomain = publicDbHost({ ...dbRecord, id: String(dbRecord._id), containerName, internalPort: String(hostPort), connStr: realConn }).split('.')[0];
        if (dbSubdomain && CF_API_TOKEN && CF_ZONE_ID) {
          registerSubdomain(dbSubdomain, { isDatabase: true }).catch(e =>
            console.warn('[DB] Could not register DNS record for', dbSubdomain, e.message)
          );
        }
        // Also run a full repair pass in case other DBs are still broken
        autoRepairDbDnsRecords().catch(() => {});

        // ── Notify ONLY the owner's socket so status updates stay private ──────
        const dbStatusPayload = {
          id:     String(dbRecord._id),
          name:   safeName,
          engine,
          status: 'running',
          containerName,
          hostPort,
          connStr: realConn
        };
        // Use the user-specific room (joined on auth:join) — never broadcast globally
        io.to('user:' + String(userId)).emit('db:status', dbStatusPayload);
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

        // ── Notify ONLY the owner of the failure — not all connected users ────
        io.to('user:' + String(userId)).emit('db:status', {
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


// ── POST /api/databases/repair-dns ──────────────────────────────────────────
// [FIX] One-time repair: iterates all running databases and flips their
// Cloudflare DNS records from proxied:true to proxied:false (DNS-only),
// so external apps can reach the raw TCP ports. Safe to run multiple times.
app.post('/api/databases/repair-dns', requireAuth, async (req, res) => {
  if (!CF_API_TOKEN || !CF_ZONE_ID) {
    return res.json({ ok: false, reason: 'Cloudflare API not configured (CF_API_TOKEN / CF_ZONE_ID missing)' });
  }
  try {
    const dbs = await loadUserDatabases(req.user);
    const results = [];
    for (const db of dbs) {
      const host = publicDbHost(db);
      if (!host) { results.push({ name: db.name, skipped: true, reason: 'no_public_host' }); continue; }
      const subdomain = host.split('.')[0];
      const r = await registerSubdomain(subdomain, { isDatabase: true }).catch(e => ({ ok: false, reason: e.message }));
      results.push({ name: db.name, subdomain, host, ...r });
      console.log(`[DB DNS Repair] ${db.name} (${subdomain}): ${r.ok ? 'fixed → DNS-only' : 'failed: ' + r.reason}`);
    }
    const fixed   = results.filter(r => r.ok).length;
    const failed  = results.filter(r => !r.ok && !r.skipped).length;
    const skipped = results.filter(r => r.skipped).length;
    res.json({ ok: true, summary: { fixed, failed, skipped, total: results.length }, results });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Helper to build connection strings server-side
function buildDbConnStr(engine, user, pass, dbName, host, port) {
  const enc = s => encodeURIComponent(String(s || ''));
  switch (engine) {
    case 'mongodb':  return `mongodb://${enc(user||'root')}:${enc(pass)}@${host}:${port}/${dbName||'mydb'}?authSource=admin`;
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
    if (db.containerName) db.status = await containerStatus(db.containerName);
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
    const result = await runDocker(`docker start ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker start failed' });
    db.status = 'running'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    const _dbStartUserId = String(req.user?._id || req.user?.id || '');
    (_dbStartUserId ? io.to('user:' + _dbStartUserId) : io).emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'running', connStr: db.connStr || db.connectionString || '' });
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
    const result = await runDocker(`docker stop ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker stop failed' });
    db.status = 'stopped'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    const _dbStopUserId = String(req.user?._id || req.user?.id || '');
    (_dbStopUserId ? io.to('user:' + _dbStopUserId) : io).emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'stopped' });
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
    const result = await runDocker(`docker restart ${db.containerName}`);
    if (!result.ok) return res.status(500).json({ error: result.stderr || 'docker restart failed' });
    db.status = 'running'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    const _dbRestartUserId = String(req.user?._id || req.user?.id || '');
    (_dbRestartUserId ? io.to('user:' + _dbRestartUserId) : io).emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'running', connStr: db.connStr || db.connectionString || '' });
    addActivity('database', `↺ Database "${db.name}" restarted`);
    res.json({ ok: true, status: 'running' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── POST /api/databases/:id/recreate ─────────────────────────────────────────
// [FIX] docker restart/start/stop only ever touch an EXISTING container —
// they cannot change flags baked in at `docker run` time (memory limits,
// --wiredTigerCacheSizeGB, etc). A database provisioned under an old/buggy
// DB_ENGINE_CONFIG (like the MongoDB cache-size bug fixed alongside this
// endpoint) stays broken forever no matter how many times it's restarted --
// the only way to pick up a corrected config is a fresh `docker run`. Until
// now that meant delete-then-recreate-from-scratch in the dashboard, which
// loses the database's identity (new id, has to be re-linked to projects,
// any saved external connection strings go stale).
//
// This does the equivalent of delete+recreate under the hood, but keeps the
// same DB record (id, name, credentials) and reuses the same container name
// and host port, so:
//   - the connection string shown in the dashboard doesn't change
//   - any project already linked to this database keeps working without
//     re-linking
//   - it picks up whatever the current DB_ENGINE_CONFIG says (memory floor,
//     startArgs, etc) since it goes through the same provisionDbContainer()
//     used for brand-new databases
//
// Body: { wipeData?: boolean }  (default true)
//   true  — also deletes the volume directory first. Needed whenever the
//           existing container never finished a healthy first boot (like a
//           crash-looping Mongo container): MongoDB/MySQL/etc only apply
//           MONGO_INITDB_ROOT_USERNAME/PASSWORD (or equivalent) on a
//           completely empty data directory, so reusing a half-initialized
//           volume with new credentials/config can leave things in a worse,
//           inconsistent state.
//   false — preserve the existing volume/data. Only safe if the database
//           previously had a genuinely healthy boot and you just want to
//           apply a new memory/config tier going forward.
app.post('/api/databases/:id/recreate', requireAuth, async (req, res) => {
  try {
    const dbs = await loadUserDatabases(req.user);
    const db = dbs.find(d => d.id === req.params.id);
    if (!db) return res.status(404).json({ error: 'Not found' });
    if (!db.containerName) return res.status(400).json({ error: 'Container not provisioned yet — nothing to recreate' });

    const wipeData = req.body?.wipeData !== false; // default true
    const oldHostPort = Number(db.internalPort) || null;
    const volumePath = db.volume || '';

    addActivity('database', `↻ Recreating database "${db.name}" (${db.engine})…`);

    // Stop + remove the old container (best-effort — it may already be
    // dead/crash-looping, so failures here are not fatal to the recreate).
    await runDocker(`docker stop ${db.containerName} 2>/dev/null || true`);
    await runDocker(`docker rm -f ${db.containerName} 2>/dev/null || true`);

    if (wipeData && volumePath && volumePath.startsWith('/var/joytree-data/')) {
      try { await fs.promises.rm(volumePath, { recursive: true, force: true }); } catch {}
    }

    db.status = 'provisioning'; db.updatedAt = new Date().toISOString();
    await persistDb(req.user, db);
    const _dbRecreateUserId = String(req.user?._id || req.user?.id || '');
    (_dbRecreateUserId ? io.to('user:' + _dbRecreateUserId) : io).emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'provisioning' });

    res.json({ ok: true, status: 'provisioning' });

    // Provision in background, same pattern as initial creation.
    setImmediate(async () => {
      try {
        const { containerName, hostPort } = await provisionDbContainer(
          { ...db, _id: db.id },
          { hostPort: oldHostPort }
        );

        const realConn = buildDbConnStr(db.engine, db.user, db.pass, db.dbName, 'localhost', hostPort);
        const updated = { ...db, containerName, internalPort: hostPort, connStr: realConn, status: 'running', updatedAt: new Date().toISOString() };
        await persistDb(req.user, updated);

        const extConn = externalDbConnStr(updated);
        (_dbRecreateUserId ? io.to('user:' + _dbRecreateUserId) : io).emit('db:status', {
          id: db.id, name: db.name, engine: db.engine, status: 'running', connStr: extConn || realConn
        });
        // [FIX] hostPort can legitimately differ from oldHostPort now that
        // findFreeDbPort() validates availability instead of trusting the
        // old value blindly (see findFreeDbPort()) -- surface that clearly
        // since it means the connection string just changed.
        if (oldHostPort && hostPort !== oldHostPort) {
          addActivity('database', `⚠ Database "${db.name}" recreated on a NEW port ${hostPort} (was ${oldHostPort} — that port was taken by another database). Connection string updated.`);
        } else {
          addActivity('database', `✅ Database "${db.name}" (${db.engine}) recreated on port ${hostPort}`);
        }
        console.log(`[DB] Recreated ${db.engine} container "${containerName}" on port ${hostPort}`);
      } catch (e) {
        const updated = { ...db, status: 'error', updatedAt: new Date().toISOString() };
        await persistDb(req.user, updated).catch(() => {});
        (_dbRecreateUserId ? io.to('user:' + _dbRecreateUserId) : io).emit('db:status', { id: db.id, name: db.name, engine: db.engine, status: 'error' });
        addActivity('database', `❌ Failed to recreate database "${db.name}": ${e.message}`);
        console.error(`[DB] Recreate failed for "${db.name}":`, e.message);
      }
    });
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
      await runDocker(`docker stop ${db.containerName} 2>/dev/null || true`);
      await runDocker(`docker rm -f ${db.containerName} 2>/dev/null || true`);
    }

    // Remove the volume directory
    // [FIX] async fs.rm instead of execSync('rm -rf ...').
    if (db.volume && String(db.volume).startsWith('/var/joytree-data/')) {
      try { await fs.promises.rm(db.volume, { recursive: true, force: true }); } catch {}
    }

    // Remove from Firebase, local cache, and Mongo
    await removeDb(req.user, req.params.id);
    const _dbDelUserId = String(req.user?._id || req.user?.id || '');
    (_dbDelUserId ? io.to('user:' + _dbDelUserId) : io).emit('db:deleted', { id: req.params.id });
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
    const result = await runDocker(`docker logs --tail ${lines} ${db.containerName} 2>&1`);
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

    const liveStatus = await containerStatus(db.containerName);
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
        // [FIX] Write query to a host tmpfile, docker cp into container, run via stdin redirect.
        // This approach:
        //   1. Supports multi-statement SQL (CREATE TABLE + INSERT + ALTER all in one go)
        //   2. Is /bin/sh compatible — no bash-only <<< here-strings
        //   3. Handles single quotes, newlines, and special chars safely (no shell escaping needed)
        //   4. Cleans up the tmpfile after execution
        const mysqlBin = db.engine === 'mariadb' ? 'mariadb' : 'mysql';
        const tmpFile  = path.join(os.tmpdir(), `jt_query_${Date.now()}_${Math.random().toString(36).slice(2)}.sql`);
        fs.writeFileSync(tmpFile, query, 'utf8');
        try {
          // Copy the .sql file into the container's /tmp, then pipe it into the mariadb CLI
          const copyCmd = `docker cp ${tmpFile} ${db.containerName}:/tmp/jt_query.sql`;
          const copyResult = await runDocker(copyCmd, 10000);
          if (!copyResult.ok) {
            fs.unlinkSync(tmpFile);
            return res.status(400).json({ ok: false, error: 'Failed to copy query to container: ' + (copyResult.stderr || copyResult.stdout), engine: db.engine });
          }
          cmd = `docker exec ${db.containerName} sh -c '${mysqlBin} -u"${db.user||'root'}" -p"${db.pass}" "${db.dbName||'mydb'}" --batch --silent < /tmp/jt_query.sql && rm -f /tmp/jt_query.sql' 2>&1`;
        } finally {
          // Always remove the host tmpfile
          try { fs.unlinkSync(tmpFile); } catch (_) {}
        }
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

    const result = await runDocker(cmd);

    // [FIX] Previously, a non-zero exit code from the underlying `docker
    // exec` command was treated as an unconditional failure, and whatever
    // the command printed to stdout was shoved into the `error` field --
    // even when that stdout was perfectly valid, complete data. The
    // frontend's Data Browser then dumped that raw JSON/row text into its
    // red error box, making successful queries look like failures (and,
    // confusingly, showing the person their own real, correct data
    // formatted as an error message).
    //
    // A non-zero exit doesn't necessarily mean the command failed to
    // produce good output -- e.g. mongosh/psql can print a trailing
    // deprecation warning or exit with a nonstandard code in some
    // versions/configurations while still having written a complete,
    // correct result to stdout first. So now: if the exit was non-zero BUT
    // the stdout content doesn't actually look like an error for this
    // engine (checked the same way the isErrorOutput heuristic below
    // already does for the exit-0 case), we proceed to parse and return it
    // as a normal successful result instead of discarding it.
    const _rawOutOnFail = (result.stdout || '').trim();
    const _looksLikeRealErrorOnFail = (() => {
      if (!result.ok && !_rawOutOnFail) return true; // no output at all -- genuinely nothing to salvage
      if (db.engine === 'postgres') return /^ERROR:/m.test(_rawOutOnFail) || /^FATAL:/m.test(_rawOutOnFail);
      if (db.engine === 'mysql' || db.engine === 'mariadb') return /^ERROR\s+\d+/m.test(_rawOutOnFail);
      if (db.engine === 'redis') return /^\(error\)/m.test(_rawOutOnFail) || /^-[A-Z]/m.test(_rawOutOnFail);
      if (db.engine === 'mongodb') {
        try { const p = JSON.parse(_rawOutOnFail); return p?.ok === 0 || !!p?.errmsg; } catch { return true; } // can't parse -- treat as real error
      }
      return true;
    })();

    if (!result.ok && _looksLikeRealErrorOnFail) {
      const errMsg = (result.stdout || result.stderr || 'Query execution failed').trim();
      // Docker rejects exec on a restarting container with this specific message.
      // Give the user a clear actionable message instead of the raw daemon error.
      if (/is restarting|wait until the container is running/i.test(errMsg)) {
        return res.status(400).json({ ok: false, error: 'Database is still starting up — please wait 30-60 seconds and try again.', engine: db.engine });
      }
      return res.status(400).json({ ok: false, error: errMsg, engine: db.engine });
    }

    // Strip the mysql password CLI warning — it's noise that appears in every result
    const _rawOutFull = (result.stdout || '').trim();
    const rawOut = (db.engine === 'mysql' || db.engine === 'mariadb')
      ? _rawOutFull.split('\n')
          .filter(l => !l.includes('[Warning] Using a password on the command line'))
          .join('\n').trim()
      : _rawOutFull;

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

    const live = await containerStatus(db.containerName);
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
      case 'mariadb': {
        // mysqladmin ping uses Unix socket by default and fails in Docker containers.
        // Use SQL client with SELECT 1 instead — connects via TCP, works reliably.
        const mysqlPingBin = db.engine === 'mariadb' ? 'mariadb' : 'mysql';
        cmd = `docker exec ${db.containerName} ${mysqlPingBin} -u"${db.user || 'root'}" -p"${db.pass || ''}" --batch --silent -e "SELECT 1" 2>&1`;
        break;
      }
      case 'redis':
        cmd = `docker exec ${db.containerName} redis-cli ${db.pass ? `-a "${db.pass}"` : ''} PING 2>&1`;
        break;
      default:
        return res.status(400).json({ error: `Unsupported engine: ${db.engine}` });
    }

    const result = await runDocker(cmd, 8000);
    // Catch Docker daemon error when container is mid-restart (MySQL 8 init loop)
    const rawPingOut = (result.stdout || result.stderr || '').trim();
    if (/is restarting|wait until the container is running/i.test(rawPingOut)) {
      return res.json({ ok: false, ms: Date.now() - t0, engine: db.engine,
        error: 'Database is still starting — please wait 30-60 seconds and try again.' });
    }
    const out = rawPingOut;
    const ms  = Date.now() - t0;

    // Determine success by engine-specific output signals
    let ok = false;
    switch (db.engine) {
      case 'redis':    ok = out.toUpperCase().includes('PONG'); break;
      case 'mysql':
      case 'mariadb':  ok = result.ok && (out.trim() === '1' || !out.toLowerCase().includes('error')); break;  // SELECT 1 returns '1' on success
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

    const status = await containerStatus(db.containerName);
    let stats = {};
    if (status === 'running') {
      const r = await runDocker(`docker stats --no-stream --format '{"cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}","net":"{{.NetIO}}","block":"{{.BlockIO}}"}' ${db.containerName} 2>/dev/null || echo '{}'`);
      try { stats = JSON.parse(r.stdout || '{}'); } catch {}
    }
    res.json({ ok: true, status, ...stats, containerName: db.containerName });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Mount /api/v1 router HERE — before static middleware so the catch-all
// can never swallow API routes like /api/v1/transfer ──────────────────────────
app.options('/api/v1/transfer', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.sendStatus(204);
});
// NOTE: the full /api/v1/transfer GET handler and app.use('/api/v1', v1) are
// defined later in this file but we re-register the router mount here so it
// runs before express.static. The duplicate app.use below is harmless — Express
// will hit this one first.
app.use('/api/v1', (req, res, next) => {
  // Forward to v1 router — defined further down; we attach it lazily so all
  // route registrations in the file still work regardless of source order.
  setImmediate(() => v1(req, res, next));
});

// Guard: never serve static files for /api/* paths
app.use((req, res, next) => {
  if (req.path.startsWith('/api/')) return next();
  express.static(path.join(__dirname))(req, res, next);
});

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
// ── Screenshot proxy ──────────────────────────────────────────────────────────
// Fetches screenshots server-side via APIFlash so the browser never hits
// third-party APIs directly (avoids CORS issues).
// Usage: GET /api/screenshot?url=https://mysite.joytree.site[&_force=1]
//   _force=1  → tells APIFlash fresh=true  (new deploy, burn a quota slot)
//   (absent)  → fresh=false (card re-render, use APIFlash's own cache)
app.get('/api/screenshot', requireAuth, async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl || !/^https?:\/\//i.test(targetUrl)) {
    return res.status(400).json({ error: 'Missing or invalid url parameter' });
  }

  // _force=1 is sent only by the deploy-success overlay (new/redeployment).
  // Card re-renders omit it so APIFlash returns its cached copy cheaply.
  const forceFresh = req.query._force === '1';

  const https = require('https');

  // Collect full response body and headers from an HTTPS GET
  const proxyFetch = (url) => new Promise((resolve, reject) => {
    const r = https.get(url, { headers: { 'User-Agent': 'JoytreeBot/1.0' } }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ statusCode: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    r.on('error', reject);
    r.setTimeout(35000, () => { r.destroy(); reject(new Error('timeout')); });
  });

  // APIFlash — primary and only provider
  try {
    const APIFLASH_KEY = process.env.APIFLASH_ACCESS_KEY || 'ae7530bb50b05fad2b86b19e4361ec39';
    const apiflashUrl = 'https://api.apiflash.com/v1/urltoimage'
      + '?access_key=' + APIFLASH_KEY
      + '&url=' + encodeURIComponent(targetUrl)
      + '&width=1280&height=800&format=jpeg&quality=90'
      + '&delay=5&fresh=' + (forceFresh ? 'true' : 'false') + '&no_ads=true&scroll_page=false'
      + '&response_type=image';

    console.log('[Screenshot] Requesting APIFlash for:', targetUrl, '| fresh:', forceFresh);
    const result = await proxyFetch(apiflashUrl);

    if (result.statusCode === 200) {
      const contentType = result.headers['content-type'] || '';
      // Reject non-image responses (e.g. APIFlash error JSON returned as 200)
      if (!contentType.includes('image')) {
        const errText = result.body.toString('utf8').slice(0, 300);
        console.error('[Screenshot] APIFlash 200 but non-image content-type:', contentType, '—', errText);
        return res.status(502).json({ error: 'APIFlash returned non-image response', detail: errText });
      }
      // Reject suspiciously tiny images (error placeholders are often <2 KB)
      if (result.body.length < 2000) {
        console.error('[Screenshot] APIFlash image too small (' + result.body.length + ' bytes) — likely an error placeholder');
        return res.status(502).json({ error: 'APIFlash returned invalid image (too small)' });
      }
      console.log('[Screenshot] APIFlash success — ' + result.body.length + ' bytes');
      res.set('Content-Type', contentType);
      // Cache aggressively on the server side; the client always uses the stored
      // base64 data URL so this cache is only hit on explicit card reloads.
      res.set('Cache-Control', 'public, max-age=3600');
      return res.end(result.body);
    }

    // Non-200: log the response body for diagnosis
    const errBody = result.body.toString('utf8').slice(0, 300);
    console.error('[Screenshot] APIFlash returned HTTP', result.statusCode, '—', errBody);
    return res.status(502).json({ error: 'APIFlash error ' + result.statusCode, detail: errBody });

  } catch (e) {
    console.error('[Screenshot] APIFlash request failed:', e.message);
    return res.status(502).json({ error: 'Screenshot capture failed', detail: e.message });
  }
});

app.get('/dashboard', (req, res) => res.sendFile(DASHBOARD_INDEX));

// /dashboard/:page  (any valid sub-page)
app.get('/dashboard/:page', (req, res, next) => {
  // Only intercept known page slugs; let anything else fall through (future API routes etc.)
  if (DASHBOARD_PAGES.includes(req.params.page)) {
    return res.sendFile(DASHBOARD_INDEX);
  }
  next();
});

// Fallback catch-all — serves index.html for SPA routes but NEVER for /api/* paths
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api/')) return next();
  res.sendFile(DASHBOARD_INDEX);
});

// ── Start ─────────────────────────────────────────────────────────────────────
// ── Auto-recover ports.json from running Docker containers on every startup ──
// This means you never have to manually recreate ports.json when moving to a
// new VPS or after a server restart — the registry is rebuilt automatically.
async function recoverPortRegistryFromDocker() {
  try {
    // [FIX] async — was execSync at server startup; converted for consistency
    // (runs once before the server starts accepting traffic, so low risk, but
    // avoids blocking any in-flight startup work like Mongo connection retries).
    const raw = await execP("docker ps --filter 'name=db-api-' --format '{{.Names}}|{{.Ports}}' 2>/dev/null || echo ''");
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

/* ═══════════════════════════════════════════════════════════════════════════
   JOYTREE PERSONAL REST API  — /api/v1/*
   Auth:  Authorization: Bearer jtk_<hex>   OR   ?api_key=jtk_<hex>
   Keys stored in Firebase RTDB at: deployboard_api_keys/<emailKey>/key
   All routes read/write the user's live Firebase workspace in real time.
═══════════════════════════════════════════════════════════════════════════ */

// ── Firebase RTDB helpers for personal API keys ──────────────────────────────
function apiKeyFbUrl(emailKey, suffix = '') {
  if (!FIREBASE_RTDB_URL || !emailKey) return '';
  const auth = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
  return `${FIREBASE_RTDB_URL}/deployboard_api_keys/${emailKey}${suffix}.json${auth}`;
}

async function getApiKeyRecord(emailKey) {
  const url = apiKeyFbUrl(emailKey);
  if (!url) return null;
  const r = await fetch(url, { headers: { Accept: 'application/json' } }).catch(() => null);
  if (!r || !r.ok) return null;
  return r.json().catch(() => null);
}

async function setApiKeyRecord(emailKey, record) {
  const url = apiKeyFbUrl(emailKey);
  if (!url) return false;
  const r = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(record)
  }).catch(() => null);
  return r && r.ok;
}

function generatePersonalApiKey() {
  return 'jtk_' + crypto.randomBytes(24).toString('hex');
}

// ── Build a compact project manifest for a user's workspace ──────────────────
// Strips large fields (logs, raw deploy output) so the snapshot stays small.
function buildProjectSnapshot(ws, envStore = {}) {
  const projects = Array.isArray(ws.projects) ? ws.projects : [];
  return projects.map(p => {
    const env = envStore[p.id] || envStore[p.subdomain] || p.envVars || {};
    // Mask env values — receiver needs to supply their own secrets
    const maskedEnv = Object.fromEntries(
      Object.entries(env).map(([k, v]) => [k, String(v).length > 0 ? '••••' : ''])
    );
    return {
      id:                p.id,
      name:              p.name || p.subdomain || '',
      subdomain:         p.subdomain || '',
      repoUrl:           p.repoUrl || '',
      branch:            p.branch || 'main',
      buildCommand:      p.buildCommand || '',
      startCommand:      p.startCommand || '',
      nodeVersion:       p.nodeVersion || '20',
      isStatic:          !!p.isStatic,
      isDockerfileDeploy:!!p.isDockerfileDeploy,
      autoDeploy:        !!p.autoDeploy,
      status:            p.status || 'unknown',
      liveUrl:           p.liveUrl || '',
      createdAt:         p.createdAt || null,
      updatedAt:         p.updatedAt || null,
      envKeys:           Object.keys(env),      // key names only (no values)
      envMasked:         maskedEnv,
    };
  });
}

// ── Write snapshot into the key record (called after workspace save) ──────────
async function refreshApiKeySnapshot(user, ws) {
  try {
    const emailKey = firebaseWorkspaceKey(user);
    if (!emailKey) return;
    const rec = await getApiKeyRecord(emailKey);
    if (!rec || !rec.key) return; // no key yet — skip
    const envStore = ws.envStore && typeof ws.envStore === 'object' ? ws.envStore : {};
    const snapshot = {
      projects: buildProjectSnapshot(ws, envStore),
      projectCount: Array.isArray(ws.projects) ? ws.projects.length : 0,
      snapshotAt: new Date().toISOString(),
    };
    const url = apiKeyFbUrl(emailKey, '/snapshot');
    // [FIX] Explicit inner try/catch — a transient Firebase error here must
    // never surface as an unhandled rejection; snapshot writes are best-effort.
    try {
      await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(snapshot)
      });
    } catch (_) {}
  } catch (_) {}
}

// ── Middleware: authenticate via personal API key (jtk_xxx) ──────────────────
async function requirePersonalApiKey(req, res, next) {
  try {
    // Accept key from Authorization header OR ?api_key query param
    const auth = String(req.headers.authorization || '').trim();
    const raw = auth.startsWith('Bearer ') ? auth.slice(7) : String(req.query.api_key || '').trim();
    if (!raw || !raw.startsWith('jtk_')) {
      return res.status(401).json({ ok: false, error: 'Missing or invalid API key. Pass Authorization: Bearer jtk_... or ?api_key=jtk_...' });
    }

    // Find which user owns this key by scanning Firebase
    // We store keys at deployboard_api_keys/<emailKey>/key for O(1) lookup via emailKey
    // But we also need reverse lookup — we store a global index at deployboard_api_keys_index/<hashedKey>
    const keyHash = crypto.createHash('sha256').update(raw).digest('hex');
    const indexUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
      (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
    const idxR = await fetch(indexUrl, { headers: { Accept: 'application/json' } }).catch(() => null);
    let emailKey = null;
    if (idxR && idxR.ok) {
      const idxData = await idxR.json().catch(() => null);
      if (idxData && idxData.emailKey) emailKey = idxData.emailKey;
    }

    if (!emailKey) {
      return res.status(401).json({ ok: false, error: 'API key not found or revoked.' });
    }

    // Verify the key matches
    const rec = await getApiKeyRecord(emailKey);
    if (!rec || !rec.key || rec.key !== raw) {
      return res.status(401).json({ ok: false, error: 'API key mismatch.' });
    }
    if (rec.disabled) {
      return res.status(403).json({ ok: false, error: 'API key is disabled.' });
    }

    // Load user
    let user = null;
    if (isDbReady()) {
      user = await User.findOne({ email: rec.email }).catch(() => null);
    } else {
      user = localAuth.users.find(u => String(u.email || '').toLowerCase() === rec.email) || null;
    }
    if (!user) return res.status(401).json({ ok: false, error: 'User account not found.' });

    // Update last-used timestamp async (fire-and-forget)
    fetch(apiKeyFbUrl(emailKey, '/lastUsed'), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(new Date().toISOString())
    }).catch(() => {});

    req.user = user;
    req.apiKeyEmailKey = emailKey;
    next();
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
}

// ── GET  /api/account/api-key  — fetch or auto-create key for logged-in user ─
app.get('/api/account/api-key', requireAuth, async (req, res) => {
  try {
    const emailKey = firebaseWorkspaceKey(req.user);
    let rec = await getApiKeyRecord(emailKey);
    if (!rec || !rec.key) {
      // Auto-generate on first load
      const newKey = generatePersonalApiKey();
      const keyHash = crypto.createHash('sha256').update(newKey).digest('hex');
      rec = { key: newKey, email: req.user.email, createdAt: new Date().toISOString(), lastUsed: null, disabled: false };
      await setApiKeyRecord(emailKey, rec);
      // Write reverse-lookup index
      const idxUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
        (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
      await fetch(idxUrl, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ emailKey, email: req.user.email }) }).catch(() => {});
      // [FIX] Firebase has a read-your-own-write race: getApiKeyRecord() called
      // immediately after setApiKeyRecord() can return null because the write
      // hasn't propagated yet. This caused snapshotAt to always be null on first
      // load, making the settings page show "Error loading key" — only fixed by
      // rotating the key (the second write always succeeded). Wait 400 ms.
      await new Promise(r => setTimeout(r, 400));
      rec = await getApiKeyRecord(emailKey) || rec;
    }
    // If no snapshot exists yet, seed it now — retry up to 3 times so a slow
    // Firebase write on first-ever load doesn't permanently leave snapshotAt null.
    if (!rec.snapshot || !rec.snapshot.snapshotAt) {
      const ws = (await readWorkspaceFromFirebase(req.user)) || {};
      for (let attempt = 0; attempt < 3; attempt++) {
        await refreshApiKeySnapshot(req.user, ws);
        rec = await getApiKeyRecord(emailKey) || rec;
        if (rec.snapshot && rec.snapshot.snapshotAt) break;
        if (attempt < 2) await new Promise(r => setTimeout(r, 500));
      }
    }
    const snap = rec.snapshot || {};
    res.json({
      ok: true,
      key: rec.key,
      createdAt: rec.createdAt,
      lastUsed: rec.lastUsed || null,
      disabled: !!rec.disabled,
      projectCount: snap.projectCount || 0,
      snapshotAt: snap.snapshotAt || null,
      transferUrl: `https://${BASE_DOMAIN}/api/v1/transfer?api_key=${rec.key}`,
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/account/api-key/rotate — generate a new key, revoke old ────────
app.post('/api/account/api-key/rotate', requireAuth, async (req, res) => {
  try {
    const emailKey = firebaseWorkspaceKey(req.user);
    const oldRec = await getApiKeyRecord(emailKey);

    // Revoke old key from index
    if (oldRec && oldRec.key) {
      const oldHash = crypto.createHash('sha256').update(oldRec.key).digest('hex');
      const oldIdxUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${oldHash}.json` +
        (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
      await fetch(oldIdxUrl, { method: 'DELETE' }).catch(() => {});
    }

    const newKey = generatePersonalApiKey();
    const keyHash = crypto.createHash('sha256').update(newKey).digest('hex');
    const rec = { key: newKey, email: req.user.email, createdAt: new Date().toISOString(), lastUsed: null, disabled: false };
    await setApiKeyRecord(emailKey, rec);
    const idxUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
      (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
    await fetch(idxUrl, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ emailKey, email: req.user.email }) }).catch(() => {});

    // Seed snapshot for new key immediately
    const wsForSnap = (await readWorkspaceFromFirebase(req.user)) || {};
    await refreshApiKeySnapshot(req.user, wsForSnap);
    const snap = (await getApiKeyRecord(emailKey))?.snapshot || {};
    res.json({
      ok: true,
      key: newKey,
      createdAt: rec.createdAt,
      projectCount: snap.projectCount || 0,
      snapshotAt: snap.snapshotAt || null,
      transferUrl: `https://${BASE_DOMAIN}/api/v1/transfer?api_key=${newKey}`,
      message: 'Old key revoked. Save your new key — it will not be shown again in full.',
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════════════════════
// JOYTREE PUBLIC REST API v1  — all routes use jtk_ personal key auth
// Base: /api/v1
// ═══════════════════════════════════════════════════════════════════════════════
const v1 = express.Router();

// [FIX] Auth + parsing middleware MUST be registered before any v1 routes,
// including the migrations routes below — Express routers run middleware and
// routes in strict registration order. These were previously declared further
// down the file, after the /migrations routes, which meant every migrations
// request ran with req.user still undefined (requirePersonalApiKey is the only
// thing on this router that sets it), causing "Cannot read properties of
// undefined (reading '_id')" inside loadUserDatabases(). Moved here, unchanged
// otherwise, so they run first for every route on this router.
// [FIX] Default express.json() body limit is 100kb -- far too small for the
// new /deploy-from-zip endpoint below, which accepts a base64-encoded zip
// archive in the JSON body (for MCP/API clients that don't have a git repo
// to point at, e.g. an AI agent that just generated a project's files).
// 300mb comfortably covers the ~33% base64 size inflation on top of the
// existing 260MB raw multipart upload limit used by the dashboard's own
// upload flow (parseMultipart's MAX). A larger limit is harmless for every
// other v1 route, which send/receive tiny JSON payloads.
v1.use(express.json({ limit: '300mb' }));
v1.use(requirePersonalApiKey);

// Rate limiting per API key (simple in-memory, 120 req/min)
const v1RateMap = new Map();
v1.use((req, res, next) => {
  const key = req.apiKeyEmailKey || 'anon';
  const now = Date.now();
  const bucket = v1RateMap.get(key) || { count: 0, reset: now + 60000 };
  if (now > bucket.reset) { bucket.count = 0; bucket.reset = now + 60000; }
  bucket.count++;
  v1RateMap.set(key, bucket);
  res.setHeader('X-RateLimit-Limit', '120');
  res.setHeader('X-RateLimit-Remaining', String(Math.max(0, 120 - bucket.count)));
  res.setHeader('X-RateLimit-Reset', String(Math.floor(bucket.reset / 1000)));
  if (bucket.count > 120) return res.status(429).json({ ok: false, error: 'Rate limit exceeded. Max 120 requests/minute.' });
  next();
});

const { runMigration } = require('./migration-engine');
const migrationJobs = new Map(); // in-memory job registry for this process; persisted summary lives in Firebase (below) so history survives restarts

async function resolveJoyTreeDbAsEndpoint(user, databaseId) {
  const dbs = await loadUserDatabases(user);
  const db = dbs.find(d => String(d.id || d._id || '') === String(databaseId));
  if (!db) throw new Error(`JoyTree database not found: ${databaseId}`);
  const connectionString = db.externalConnectionString || externalDbConnStr(db);
  if (!connectionString) throw new Error(`Could not build a connection string for database ${databaseId}`);
  if (db.engine === 'redis') return { kind: 'redis', connectionString };
  if (db.engine === 'mongodb') return { kind: 'mongo', connectionString };
  return { kind: 'sql', engine: db.engine, connectionString };
}

async function resolveMigrationSource(user, source) {
  if (source.kind === 'joytree') return resolveJoyTreeDbAsEndpoint(user, source.databaseId);
  if (source.kind === 'mongo') return { kind: 'mongo', connectionString: source.connectionString };
  if (source.kind === 'firebase') return { kind: 'firebase', databaseUrl: source.databaseUrl, authSecret: source.authSecret || null };
  // [FIX] readFromSql/readFromRedis in migration-engine.js already supported
  // arbitrary external connection strings -- only the frontend source-type
  // picker and this resolver were missing, so external MySQL/PostgreSQL/
  // MariaDB/Redis sources (as opposed to JoyTree-provisioned ones) had no
  // way to reach the engine at all.
  if (source.kind === 'sql') {
    if (!['mysql', 'postgres', 'mariadb'].includes(source.engine)) throw new Error(`Unsupported SQL engine: ${source.engine}`);
    return { kind: 'sql', engine: source.engine, connectionString: source.connectionString };
  }
  if (source.kind === 'redis') return { kind: 'redis', connectionString: source.connectionString };
  throw new Error(`Unsupported source kind: ${source.kind}`);
}

// ── POST /api/migrations -- start a migration ───────────────────────────────
// [FIX] Moved off the /api/v1 router. That router is the PUBLIC REST API,
// gated exclusively by requirePersonalApiKey (jtk_... keys) for external/CLI
// consumers. The dashboard's browser session sends its regular session
// authToken via apiFetch(), which requirePersonalApiKey rejects outright
// ("Missing or invalid API key"). requireAuth (used below, same as
// /api/databases which already works from this dashboard) accepts session
// tokens, jtk_ keys, and internal server keys, so it's the correct gate for
// a dashboard-facing feature like this one.
// [FIX] Extracted into named handlers (rather than inline route callbacks)
// so the exact same logic can be mounted on BOTH routers: `app` here with
// requireAuth (session cookie -- what the dashboard's browser uses) and
// `v1` further down with requirePersonalApiKey (jtk_... key -- what the
// CLI and MCP server use). Migrations previously only existed on `app`,
// which meant there was no way for the CLI or MCP tools to trigger or
// inspect a migration at all -- they only ever talk to the API via a
// personal API key, never a browser session.
async function migrationsCreateHandler(req, res) {
  try {
    const { source, destination } = req.body || {};
    if (!source || !source.kind) return res.status(400).json({ ok: false, error: 'source.kind is required (joytree | mongo | firebase | sql | redis)' });
    if (!destination || !destination.databaseId) return res.status(400).json({ ok: false, error: 'destination.databaseId is required (must be one of your JoyTree databases)' });

    const resolvedSource = await resolveMigrationSource(req.user, source);
    const resolvedDest = await resolveJoyTreeDbAsEndpoint(req.user, destination.databaseId);

    const jobId = 'mig_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
    const logs = [];
    const sqlEngineLabels = { mysql: 'MySQL', postgres: 'PostgreSQL', mariadb: 'MariaDB' };
    const sourceLabel =
      source.kind === 'joytree'  ? `JoyTree DB (${source.databaseId})` :
      source.kind === 'mongo'    ? 'External MongoDB' :
      source.kind === 'firebase' ? 'Firebase Realtime Database' :
      source.kind === 'sql'      ? `External ${sqlEngineLabels[source.engine] || source.engine}` :
      source.kind === 'redis'    ? 'External Redis' :
      source.kind;
    const job = {
      id: jobId,
      status: 'running',
      ownerUserId: String(req.user._id || req.user.id),
      sourceKind: source.kind,
      sourceLabel,
      destinationLabel: `JoyTree DB (${destination.databaseId})`,
      startedAt: new Date().toISOString(),
      completedAt: null,
      result: null,
      error: null,
      logs,
    };
    migrationJobs.set(jobId, job);

    // Runs in the background; the caller polls GET .../migrations/:id for progress.
    runMigration(resolvedSource, resolvedDest, (line) => {
      logs.push({ t: new Date().toISOString(), line });
    }).then(async (result) => {
      job.status = 'success';
      job.result = result;
      job.completedAt = new Date().toISOString();
      await persistMigrationJobSummary(req.user, job).catch(() => {});
    }).catch(async (err) => {
      job.status = 'failed';
      job.error = err.message;
      job.completedAt = new Date().toISOString();
      logs.push({ t: new Date().toISOString(), line: `ERROR: ${err.message}` });
      await persistMigrationJobSummary(req.user, job).catch(() => {});
    });

    res.json({ ok: true, jobId, message: 'Migration started. Poll GET .../migrations/:id for progress.' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
}

async function migrationsListHandler(req, res) {
  try {
    const userId = String(req.user._id || req.user.id);
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const persisted = Array.isArray(ws.migrations) ? ws.migrations : [];
    const live = [...migrationJobs.values()]
      .filter(j => String(j.ownerUserId) === userId)
      .map(j => ({ ...j, logs: undefined }));
    res.json({ ok: true, jobs: [...live, ...persisted].sort((a, b) => new Date(b.startedAt) - new Date(a.startedAt)) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
}

async function migrationsGetHandler(req, res) {
  const userId = String(req.user._id || req.user.id);
  const job = migrationJobs.get(req.params.id);
  if (job && String(job.ownerUserId) === userId) return res.json({ ok: true, job });
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const persisted = (ws.migrations || []).find(j => j.id === req.params.id);
    if (persisted) return res.json({ ok: true, job: persisted });
  } catch (_) {}
  res.status(404).json({ ok: false, error: 'Migration job not found' });
}

async function migrationsDeleteOneHandler(req, res) {
  try {
    const userId = String(req.user._id || req.user.id);
    const jobId = req.params.id;
    const job = migrationJobs.get(jobId);
    const ownJob = job && String(job.ownerUserId) === userId ? job : null;
    if (ownJob && ownJob.status === 'running') {
      return res.status(409).json({ ok: false, error: 'This migration is still running — wait for it to finish before deleting it.' });
    }
    if (ownJob) migrationJobs.delete(jobId);

    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const before = Array.isArray(ws.migrations) ? ws.migrations.length : 0;
    ws.migrations = (ws.migrations || []).filter(j => j.id !== jobId);
    if (ws.migrations.length === before && !ownJob) {
      return res.status(404).json({ ok: false, error: 'Migration job not found' });
    }
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
}

async function migrationsClearAllHandler(req, res) {
  try {
    const userId = String(req.user._id || req.user.id);
    for (const [jobId, job] of migrationJobs.entries()) {
      if (job.status !== 'running' && String(job.ownerUserId) === userId) {
        migrationJobs.delete(jobId);
      }
    }
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.migrations = [];
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
}

app.post('/api/migrations', requireAuth, migrationsCreateHandler);
app.get('/api/migrations', requireAuth, migrationsListHandler);
app.get('/api/migrations/:id', requireAuth, migrationsGetHandler);
app.delete('/api/migrations/:id', requireAuth, migrationsDeleteOneHandler);
app.delete('/api/migrations', requireAuth, migrationsClearAllHandler);

async function persistMigrationJobSummary(user, job) {
  const ws = (await readWorkspaceFromFirebase(user)) || {};
  ws.migrations = Array.isArray(ws.migrations) ? ws.migrations : [];
  ws.migrations.unshift({ ...job, logs: job.logs.slice(-200) }); // cap stored logs per job
  ws.migrations = ws.migrations.slice(0, 50); // cap total history
  await writeWorkspaceToFirebase(user, ws);
}

// ── GET /api/v1/account ───────────────────────────────────────────────────────
v1.get('/account', async (req, res) => {
  try {
    const u = req.user;
    const emailKey = firebaseWorkspaceKey(u);
    const rec = await getApiKeyRecord(emailKey);
    res.json({
      ok: true,
      account: {
        id: String(u._id || u.id),
        email: u.email,
        name: u.name || '',
        githubUsername: u.githubUsername || '',
        githubAvatarUrl: u.githubAvatarUrl || '',
        googleAvatarUrl: u.googleAvatarUrl || '',
        firebaseUid: u.firebaseUid || '',
        createdAt: u.createdAt || null,
        apiKey: { createdAt: rec?.createdAt || null, lastUsed: rec?.lastUsed || null }
      }
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/workspace ─────────────────────────────────────────────────────
v1.get('/workspace', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    res.json({
      ok: true,
      projects: Array.isArray(ws.projects) ? ws.projects : [],
      deployments: Array.isArray(ws.deployments) ? ws.deployments : [],
      envStore: ws.envStore || {},
      settings: ws.settings || {},
      uploadedProjects: Array.isArray(ws.uploadedProjects) ? ws.uploadedProjects : []
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/projects ──────────────────────────────────────────────────────
v1.get('/projects', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const projects = Array.isArray(ws.projects) ? ws.projects : [];
    const { status, subdomain, limit = 100 } = req.query;
    let result = projects;
    if (status) result = result.filter(p => p.status === status);
    if (subdomain) result = result.filter(p => p.subdomain === subdomain);
    res.json({ ok: true, count: result.length, projects: result.slice(0, Number(limit) || 100) });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/projects/:id ──────────────────────────────────────────────────
v1.get('/projects/:id', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const projects = Array.isArray(ws.projects) ? ws.projects : [];
    const project = projects.find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    res.json({ ok: true, project });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── PATCH /api/v1/projects/:id ─────────────────────────────────────────────
v1.patch('/projects/:id', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const projects = Array.isArray(ws.projects) ? ws.projects : [];
    const idx = projects.findIndex(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (idx < 0) return res.status(404).json({ ok: false, error: 'Project not found.' });
    const allowed = ['name', 'description', 'subdomain', 'branch', 'envVars', 'buildCommand', 'startCommand', 'nodeVersion', 'autoDeploy'];
    const patch = {};
    for (const k of allowed) { if (req.body[k] !== undefined) patch[k] = req.body[k]; }
    projects[idx] = { ...projects[idx], ...patch, updatedAt: new Date().toISOString() };
    ws.projects = projects;
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, project: projects[idx] });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── DELETE /api/v1/projects/:id ───────────────────────────────────────────────
v1.delete('/projects/:id', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const p = (ws.projects || []).find(pr => pr.id === req.params.id || pr.subdomain === req.params.id);
    if (!p) return res.status(404).json({ ok: false, error: 'Project not found.' });

    ws.projects = (ws.projects || []).filter(pr => pr.id !== p.id);
    if (Array.isArray(ws.deployments)) {
      ws.deployments = ws.deployments.filter(d => d.projectId !== p.id);
    }
    const wroteOk = await writeWorkspaceToFirebase(req.user, ws);
    if (!wroteOk) return res.status(502).json({ ok: false, error: 'Failed to update Firebase workspace; nothing was deleted.' });

    // [FIX] This used to only remove the workspace record, leaving site
    // files, the Docker container, and the DNS/tunnel route all still in
    // place -- the exact bug behind tonight's docs.joytree.site mess. Now
    // does the same full cleanup as /api/projects/:id.
    try { fs.rmSync(path.join(SITES_DIR, p.subdomain), { recursive: true, force: true }); }
    catch(e) { console.error(`[v1 Delete] Failed to remove site files for ${p.subdomain}:`, e.message); }

    try { await execP(`docker rm -f db-${p.subdomain}`); }
    catch(e) { console.error(`[v1 Delete] Failed to remove container db-${p.subdomain}:`, e.message); }

    delete portRegistry[p.subdomain];
    savePortRegistry();

    await removeSubdomain(p.subdomain).catch(e =>
      console.error(`[v1 Delete] Failed to remove DNS/tunnel route for ${p.subdomain}:`, e.message));

    res.json({ ok: true, message: 'Project fully removed: workspace record, site files, container, and DNS route.' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/v1/projects/:id/redeploy ───────────────────────────────────────
// ── POST /api/v1/deploy — full deploy from CLI (create or update + build) ────
v1.post('/deploy', async (req, res) => {
  try {
    const { name, subdomain, repoUrl, branch, buildCmd, startCmd, installCmd,
            siteType, nodeVer, outputDir, workingDir, source } = req.body || {};

    if (!name || !repoUrl) {
      return res.status(400).json({ ok: false, error: 'name and repoUrl are required' });
    }

    const r = await fetch(`http://localhost:${PORT}/api/deploy`, {
      method: 'POST',
      headers: {
        'Content-Type':               'application/json',
        'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY,
        'x-deployboard-owner-id':     String(req.user._id || req.user.id),
      },
      body: JSON.stringify({
        name,
        subdomain:  subdomain || name,
        repoUrl,
        branch:     branch     || 'main',
        buildCmd:   buildCmd   || '',
        startCmd:   startCmd   || '',
        installCmd: installCmd || '',
        // [FIX] Was `siteType || 'static'` — forcing every deploy through this
        // endpoint that didn't explicitly pass siteType:'server' into the
        // static-file build path, before it ever reached the real dispatch
        // logic in buildRunner.js (_runBuildDispatch: blank siteType tries
        // runServerBuild, which auto-detects the runtime from the cloned repo
        // and falls back to static internally only if no server entry point
        // is found). This silently broke auto-detect for every server app
        // deployed via the CLI or MCP without an explicit siteType override.
        siteType:   siteType   || '',
        nodeVer:    nodeVer    || '20',
        outputDir:  outputDir  || '',
        workingDir: workingDir || '',
        source:     source     || 'cli',
      }),
    });

    const data = await r.json().catch(() => ({}));
    if (!r.ok) return res.status(r.status).json({ ok: false, error: data.error || `HTTP ${r.status}` });
    res.json({ ok: true, deployId: data.deployId || null, subdomain: subdomain || name, message: 'Deploy triggered.' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/v1/deploy-from-zip ──────────────────────────────────────────────
// Deploy from a base64-encoded zip archive instead of a git repo. Exists for
// API/MCP clients that don't have a repo to point at -- most notably an AI
// agent that generated a project's files locally and needs to ship them
// without ever pushing to GitHub first. The CLI intentionally does NOT expose
// this (zipping/uploading a local directory via a terminal command is a much
// worse experience than just deploying from the repo you already have), but
// there's no such friction for an MCP tool call, which can carry the archive
// bytes directly in the request.
//
// Reuses the exact same extraction (extractUploadedArchive) and build
// (runUploadBuild via /api/upload-deploy) pipeline the dashboard's own
// "Upload files" flow already uses and that we spent a long night confirming
// works correctly -- this endpoint's only job is turning a base64 string into
// the same on-disk layout /api/upload-project produces, then handing off to
// the proven /api/upload-deploy endpoint (which accepts the same jtk_ Bearer
// token this request already carried, via requireAuth's branch 2).
v1.post('/deploy-from-zip', async (req, res) => {
  try {
    const { name, subdomain, zipBase64, zipUrl, branch, buildCmd, startCmd, installCmd,
            outputDir, siteType, nodeVer, envVars } = req.body || {};

    if (!name || (!zipBase64 && !zipUrl)) {
      return res.status(400).json({ ok: false, error: 'name and one of zipBase64 or zipUrl are required' });
    }

    const MAX_ZIP_BYTES = 260 * 1024 * 1024; // matches parseMultipart's own limit
    let archiveBuffer;

    if (zipUrl) {
      // [FEATURE] Pull the archive server-side instead of requiring the
      // caller to inline potentially hundreds of KB of base64 in the
      // request body. Particularly useful for MCP/chat-based clients where
      // round-tripping a large base64 blob through a conversation transcript
      // is unreliable — passing a URL (e.g. a GitHub archive/release asset
      // link, or any other directly-downloadable zip URL) avoids that
      // entirely.
      let parsed;
      try { parsed = new URL(String(zipUrl)); } catch (_) {
        return res.status(400).json({ ok: false, error: 'zipUrl is not a valid URL' });
      }
      if (parsed.protocol !== 'https:') {
        return res.status(400).json({ ok: false, error: 'zipUrl must be an https:// URL' });
      }
      // Basic SSRF guard -- reject obviously internal/loopback hosts. Not
      // exhaustive (doesn't resolve DNS to check the actual IP), but blocks
      // the trivial cases; this endpoint also requires a valid jtk_ API key.
      const host = parsed.hostname.toLowerCase();
      if (host === 'localhost' || host === '127.0.0.1' || host === '::1' ||
          host.endsWith('.local') || host.startsWith('169.254.') ||
          /^10\.\d+\.\d+\.\d+$/.test(host) || /^192\.168\.\d+\.\d+$/.test(host) ||
          /^172\.(1[6-9]|2\d|3[01])\.\d+\.\d+$/.test(host)) {
        return res.status(400).json({ ok: false, error: 'zipUrl may not point to an internal/loopback address' });
      }

      let zr;
      try { zr = await fetch(parsed.toString(), { redirect: 'follow' }); }
      catch (e) { return res.status(400).json({ ok: false, error: 'Failed to fetch zipUrl: ' + e.message }); }
      if (!zr.ok) {
        return res.status(400).json({ ok: false, error: `zipUrl fetch failed: HTTP ${zr.status}` });
      }
      const lenHeader = Number(zr.headers.get('content-length') || 0);
      if (lenHeader && lenHeader > MAX_ZIP_BYTES) {
        return res.status(413).json({ ok: false, error: `Archive exceeds ${MAX_ZIP_BYTES / (1024*1024)}MB limit (per Content-Length)` });
      }
      try {
        const ab = await zr.arrayBuffer();
        archiveBuffer = Buffer.from(ab);
      } catch (e) {
        return res.status(400).json({ ok: false, error: 'Failed to read zipUrl response body: ' + e.message });
      }
    } else {
      try {
        archiveBuffer = Buffer.from(String(zipBase64), 'base64');
      } catch (_) {
        return res.status(400).json({ ok: false, error: 'zipBase64 is not valid base64' });
      }
    }

    if (!archiveBuffer.length) {
      return res.status(400).json({ ok: false, error: 'Decoded archive is empty -- check the base64 encoding or zipUrl response' });
    }
    if (archiveBuffer.length > MAX_ZIP_BYTES) {
      return res.status(413).json({ ok: false, error: `Archive exceeds ${MAX_ZIP_BYTES / (1024*1024)}MB limit` });
    }

    const userId = String(req.user?._id || req.user?.id || 'anon');
    const projectId = 'zip_' + Date.now();
    const userUploadDir = path.join(UPLOADS_DIR, userId, projectId);
    try { fs.mkdirSync(userUploadDir, { recursive: true }); }
    catch (e) { return res.status(500).json({ ok: false, error: 'Could not create upload directory: ' + e.message }); }

    const archivePath = path.join(userUploadDir, 'archive.zip');
    try { fs.writeFileSync(archivePath, archiveBuffer); }
    catch (e) { return res.status(500).json({ ok: false, error: 'Failed to write archive: ' + e.message }); }

    const filesDir = path.join(userUploadDir, 'files');
    try {
      await extractUploadedArchive(archivePath, filesDir);
    } catch (e) {
      return res.status(400).json({ ok: false, error: 'Failed to extract archive: ' + e.message });
    }

    const cleanSubdomain = String(subdomain || name).toLowerCase()
      .replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');

    // Hand off to the existing, already-proven upload-deploy pipeline. Forward
    // the same Authorization header this request came in with -- requireAuth
    // accepts jtk_ Bearer tokens directly (see branch 2), so no separate
    // internal-key auth path is needed here.
    const r = await fetch(`http://localhost:${PORT}/api/upload-deploy`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': req.headers.authorization || '',
      },
      body: JSON.stringify({
        projectId, name, subdomain: cleanSubdomain,
        siteType:   siteType   || '',
        buildCmd:   buildCmd   || '',
        startCmd:   startCmd   || '',
        installCmd: installCmd || '',
        outputDir:  outputDir  || '',
        nodeVer:    nodeVer    || '20',
        envVars:    envVars    || {},
      }),
    });

    const data = await r.json().catch(() => ({}));
    if (!r.ok) return res.status(r.status).json({ ok: false, error: data.error || `HTTP ${r.status}` });
    res.json({
      ok: true,
      deployId: data.deployId || null,
      subdomain: cleanSubdomain,
      liveUrl: data.liveUrl || null,
      message: data.message || 'Deploy from zip triggered.',
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/projects/:id/redeploy', async (req, res) => {
  try {
    // Proxy to internal deploy endpoint — requires INTERNAL_DEPLOY_KEY
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    const r = await fetch(`http://localhost:${PORT}/api/deploy`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY,
        'x-deployboard-owner-id': String(req.user._id || req.user.id)
      },
      body: JSON.stringify({ projectId: project.id, subdomain: project.subdomain, source: 'api' })
    }).catch(e => ({ ok: false, _err: e.message }));
    const data = r.json ? await r.json().catch(() => ({})) : r;
    res.json({ ok: !!data.ok, deployId: data.deployId || null, message: data.error || data.message || 'Deploy triggered.' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/projects/:id/logs ─────────────────────────────────────────────
v1.get('/projects/:id/logs', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    const deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
    const projectDeploys = deployments
      .filter(d => d.projectId === project.id || d.subdomain === project.subdomain)
      .sort((a, b) => new Date(b.startedAt || 0) - new Date(a.startedAt || 0))
      .slice(0, Number(req.query.limit) || 20);
    res.json({ ok: true, project: project.subdomain, logs: projectDeploys });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/projects/:id/env ──────────────────────────────────────────────
v1.get('/projects/:id/env', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    const envStore = ws.envStore || {};
    const projectEnv = envStore[project.id] || envStore[project.subdomain] || project.envVars || {};
    // Mask values by default; pass ?reveal=1 to show (key owner only)
    const reveal = req.query.reveal === '1';
    const masked = {};
    for (const [k, v] of Object.entries(projectEnv)) {
      masked[k] = reveal ? v : (String(v).length > 4 ? String(v).slice(0, 2) + '••••' + String(v).slice(-2) : '••••');
    }
    res.json({ ok: true, projectId: project.id, subdomain: project.subdomain, env: masked, count: Object.keys(masked).length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── PUT /api/v1/projects/:id/env ──────────────────────────────────────────────
v1.put('/projects/:id/env', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    if (!req.body || typeof req.body !== 'object') return res.status(400).json({ ok: false, error: 'Body must be a JSON object of key-value pairs.' });
    const envStore = ws.envStore || {};
    envStore[project.id] = { ...(envStore[project.id] || {}), ...req.body };
    ws.envStore = envStore;
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, message: 'Environment variables updated.', count: Object.keys(envStore[project.id]).length });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/deployments ───────────────────────────────────────────────────
v1.get('/deployments', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    let deploys = Array.isArray(ws.deployments) ? ws.deployments : [];
    const { projectId, status, limit = 50 } = req.query;
    if (projectId) deploys = deploys.filter(d => d.projectId === projectId || d.subdomain === projectId);
    if (status) deploys = deploys.filter(d => d.status === status);
    deploys = deploys.sort((a, b) => new Date(b.startedAt || 0) - new Date(a.startedAt || 0)).slice(0, Number(limit) || 50);
    res.json({ ok: true, count: deploys.length, deployments: deploys });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/deployments/:id ───────────────────────────────────────────────
v1.get('/deployments/:id', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const deploy = (ws.deployments || []).find(d => d.id === req.params.id || d.deployId === req.params.id);
    if (!deploy) return res.status(404).json({ ok: false, error: 'Deployment not found.' });
    res.json({ ok: true, deployment: deploy });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/databases ─────────────────────────────────────────────────────
v1.get('/databases', async (req, res) => {
  try {
    const user = req.user;
    let dbs = [];
    if (isDbReady()) {
      const ownerMatch = { $or: [{ ownerEmail: user.email }, { ownerUserId: String(user._id || user.id) }] };
      dbs = await (require('mongoose').model('Database') || (() => null))?.find(ownerMatch).lean().catch(() => []) || [];
    } else {
      // [FIX] This called readDbsFromFirebase(user), a function that was
      // never defined anywhere in this file -- guaranteed crash any time
      // MongoDB isn't ready. There's no evidence databases are tracked in
      // Firebase at all (no writes to a .databases field anywhere in this
      // codebase, unlike projects/deployments which clearly are), so rather
      // than invent a Firebase read that might silently return wrong data,
      // this degrades to an empty list with a clear signal in the response
      // and logs, rather than crashing the request outright.
      console.warn('[v1/databases] MongoDB not ready and no Firebase-backed database store exists -- returning empty list');
      dbs = [];
    }
    res.json({ ok: true, count: dbs.length, databases: dbs.map(d => ({
      id: d._id || d.id, name: d.name, engine: d.engine, status: d.status || 'unknown',
      host: d.host || '', port: d.port || null, createdAt: d.createdAt || null,
      internalUrl: d.internalUrl || '', externalUrl: d.externalUrl || ''
    })), degraded: !isDbReady() });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── /api/v1/migrations -- same handlers as the dashboard's /api/migrations,
// mounted here so the CLI and MCP server (which authenticate via jtk_...
// personal API keys, never a browser session) can trigger and inspect
// migrations too. req.user is already set correctly by this router's own
// v1.use(requirePersonalApiKey) applied earlier -- see migrationsCreateHandler
// and friends above for the shared logic.
v1.post('/migrations', migrationsCreateHandler);
v1.get('/migrations', migrationsListHandler);
v1.get('/migrations/:id', migrationsGetHandler);
v1.delete('/migrations/:id', migrationsDeleteOneHandler);
v1.delete('/migrations', migrationsClearAllHandler);

// ── GET /api/v1/usage ─────────────────────────────────────────────────────────
v1.get('/usage', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const projects = Array.isArray(ws.projects) ? ws.projects : [];
    const deployments = Array.isArray(ws.deployments) ? ws.deployments : [];
    const settings = ws.settings || {};
    const planKey = settings.planKey || 'free';
    const planInfo = { free: { maxProjects: 5, buildSeconds: 300, bandwidthGb: 100 }, starter: { maxProjects: 15, buildSeconds: 1800, bandwidthGb: 500 }, pro: { maxProjects: 50, buildSeconds: 7200, bandwidthGb: 2000 } };
    const plan = planInfo[planKey] || planInfo.free;
    const totalBuildSeconds = deployments.filter(d => d.status === 'success' && d.duration).reduce((s, d) => s + (Number(d.duration) || 0), 0);
    res.json({
      ok: true,
      plan: planKey,
      usage: {
        projects: { used: projects.length, limit: plan.maxProjects },
        buildSeconds: { used: totalBuildSeconds, limit: plan.buildSeconds },
        bandwidthGb: { used: settings.bandwidthUsed || 0, limit: plan.bandwidthGb },
        deployments: { total: deployments.length, successful: deployments.filter(d => d.status === 'success').length, failed: deployments.filter(d => d.status === 'failed').length }
      }
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/settings ──────────────────────────────────────────────────────
v1.get('/settings', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    res.json({ ok: true, settings: ws.settings || {} });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── PATCH /api/v1/settings ────────────────────────────────────────────────────
v1.patch('/settings', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const safe = ['timezone', 'prefNotifySuccess', 'prefNotifyFail', 'prefNotifyCron', 'prefCompactLog', 'prefAutoScroll', 'name'];
    const patch = {};
    for (const k of safe) { if (req.body[k] !== undefined) patch[k] = req.body[k]; }
    ws.settings = { ...(ws.settings || {}), ...patch };
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, settings: ws.settings });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/v1/projects/:id/stop ────────────────────────────────────────────
v1.post('/projects/:id/stop', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    // Add to stop set
    deployStopRequests.add(project.id);
    res.json({ ok: true, message: `Stop signal sent to project ${project.subdomain}.` });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/v1/projects/:id/restart ────────────────────────────────────────
v1.post('/projects/:id/restart', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    // [FIX] async — execSync('docker restart ...') here blocked the event
    // loop (and the proxy for every tenant) for up to 30s on a slow restart.
    // [FIX] Container name prefix was 'joytree-<subdomain>', but every
    // container this platform actually spawns (buildRunner.js, all six
    // deploy paths) uses 'db-<subdomain>'. This endpoint has been silently
    // failing on every call — 'docker restart' on a name that never existed.
    const containerName = `db-${project.subdomain}`;
    const r = await execP(`docker restart ${containerName}`, { timeout: 30000 });
    if (r !== '') {
      res.json({ ok: true, message: `Container ${containerName} restarted.` });
    } else {
      res.json({ ok: false, error: `Docker restart failed for ${containerName}.` });
    }
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/projects/:id/status ──────────────────────────────────────────
v1.get('/projects/:id/status', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    // [FIX] async — both execSync calls here blocked the event loop (and the
    // proxy for every tenant) for up to 5s/8s on a slow Docker daemon, and
    // this endpoint is polled frequently by users checking project status.
    // [FIX] Same wrong container-name prefix as the restart endpoint above —
    // 'joytree-<subdomain>' never matched any real container, so this always
    // reported 'unknown' status and empty stats regardless of the app's
    // actual state.
    const containerName = `db-${project.subdomain}`;
    let containerStatusResult = 'unknown';
    let stats = {};
    try {
      const out = await execP(`docker inspect --format '{{.State.Status}}' ${containerName} 2>/dev/null`, { timeout: 5000 });
      containerStatusResult = out || 'unknown';
      if (containerStatusResult === 'running') {
        const s = await execP(`docker stats --no-stream --format '{"cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}","netIn":"{{.NetIO}}","block":"{{.BlockIO}}"}' ${containerName} 2>/dev/null`, { timeout: 8000 });
        try { stats = JSON.parse(s); } catch (_) {}
      }
    } catch (_) {}
    res.json({ ok: true, project: project.subdomain, projectStatus: project.status || 'unknown', container: containerStatusResult, stats, liveUrl: project.liveUrl || `https://${project.subdomain}.${BASE_DOMAIN}` });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/export/:id ─── Export project config for Render / pxxl etc ───
v1.get('/export/:id', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === req.params.id || p.subdomain === req.params.id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found.' });
    const envStore = ws.envStore || {};
    const envVars = envStore[project.id] || envStore[project.subdomain] || project.envVars || {};
    const format = String(req.query.format || 'joytree').toLowerCase();

    if (format === 'render') {
      // render.yaml format
      const envList = Object.entries(envVars).map(([k, v]) => `      - key: ${k}\n        value: ${v}`).join('\n');
      const yaml = `services:\n  - type: web\n    name: ${project.name || project.subdomain}\n    env: node\n    region: oregon\n    plan: free\n    branch: ${project.branch || 'main'}\n    buildCommand: ${project.buildCommand || 'npm install && npm run build'}\n    startCommand: ${project.startCommand || 'npm start'}\n    envVars:\n${envList || '      []'}\n`;
      res.setHeader('Content-Type', 'text/yaml');
      res.setHeader('Content-Disposition', `attachment; filename="${project.subdomain}-render.yaml"`);
      return res.send(yaml);
    }

    if (format === 'dockerfile') {
      const df = `FROM node:${project.nodeVersion || '20'}-alpine\nWORKDIR /app\nCOPY package*.json ./\nRUN npm ci --only=production\nCOPY . .\n${project.buildCommand ? `RUN ${project.buildCommand}\n` : ''}EXPOSE 3000\nCMD ${JSON.stringify((project.startCommand || 'node index.js').split(' '))}\n`;
      res.setHeader('Content-Type', 'text/plain');
      res.setHeader('Content-Disposition', `attachment; filename="Dockerfile"`);
      return res.send(df);
    }

    if (format === 'dotenv') {
      const env = Object.entries(envVars).map(([k, v]) => `${k}=${v}`).join('\n');
      res.setHeader('Content-Type', 'text/plain');
      res.setHeader('Content-Disposition', `attachment; filename=".env"`);
      return res.send(env);
    }

    // Default: full JSON export (Joytree format)
    res.json({
      ok: true,
      exportFormat: 'joytree',
      exportedAt: new Date().toISOString(),
      project: {
        id: project.id,
        name: project.name || project.subdomain,
        subdomain: project.subdomain,
        repoUrl: project.repoUrl || '',
        branch: project.branch || 'main',
        buildCommand: project.buildCommand || '',
        startCommand: project.startCommand || '',
        nodeVersion: project.nodeVersion || '20',
        isStatic: project.isStatic || false,
        isDockerfileDeploy: project.isDockerfileDeploy || false,
        autoDeploy: project.autoDeploy || false,
        liveUrl: project.liveUrl || `https://${project.subdomain}.${BASE_DOMAIN}`,
        status: project.status || 'unknown',
        createdAt: project.createdAt || null,
        updatedAt: project.updatedAt || null,
        envVars: Object.fromEntries(Object.entries(envVars).map(([k]) => [k, '••••'])) // masked
      },
      importHint: 'POST this payload to your target platform. Use PUT /api/v1/projects/:id/env to restore env vars after importing.'
    });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── POST /api/v1/import ─── Import a project from another platform ──────────
v1.post('/import', async (req, res) => {
  try {
    const { project: p, envVars, source = 'external' } = req.body || {};
    if (!p || !p.subdomain) return res.status(400).json({ ok: false, error: 'project.subdomain is required.' });
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.projects = ws.projects || [];
    const existing = ws.projects.find(x => x.id === p.id || x.subdomain === p.subdomain);
    if (existing) return res.status(409).json({ ok: false, error: 'A project with this subdomain already exists. Use PATCH /api/v1/projects/:id to update it.' });
    const newProject = {
      id: p.id || ('p_api_' + Date.now()),
      name: p.name || p.subdomain,
      subdomain: p.subdomain,
      repoUrl: p.repoUrl || '',
      branch: p.branch || 'main',
      buildCommand: p.buildCommand || '',
      startCommand: p.startCommand || '',
      nodeVersion: p.nodeVersion || '20',
      isStatic: !!p.isStatic,
      isDockerfileDeploy: !!p.isDockerfileDeploy,
      autoDeploy: !!p.autoDeploy,
      status: 'imported',
      importSource: source,
      importedAt: new Date().toISOString(),
      createdAt: p.createdAt || new Date().toISOString()
    };
    ws.projects.push(newProject);
    if (envVars && typeof envVars === 'object') {
      ws.envStore = ws.envStore || {};
      ws.envStore[newProject.id] = envVars;
    }
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, message: 'Project imported successfully. Deploy it to make it live.', project: newProject });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/activity ─── Recent activity log from deployments ─────────────
v1.get('/activity', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const deploys = Array.isArray(ws.deployments) ? ws.deployments : [];
    const limit = Math.min(Number(req.query.limit) || 25, 100);
    const activity = deploys
      .sort((a, b) => new Date(b.startedAt || b.createdAt || 0) - new Date(a.startedAt || a.createdAt || 0))
      .slice(0, limit)
      .map(d => ({
        type: 'deployment',
        id: d.id || d.deployId,
        projectId: d.projectId,
        subdomain: d.subdomain,
        status: d.status,
        source: d.source || 'manual',
        branch: d.branch || 'main',
        commit: d.sha ? d.sha.slice(0, 7) : null,
        duration: d.duration || null,
        at: d.startedAt || d.createdAt || null,
        liveUrl: d.liveUrl || null
      }));
    res.json({ ok: true, count: activity.length, activity });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GET /api/v1/ping ─── Health check for API key validity ───────────────────
v1.get('/ping', (req, res) => {
  res.json({ ok: true, message: 'Joytree API v1 is alive.', user: req.user.email, timestamp: new Date().toISOString(), baseDomain: BASE_DOMAIN });
});

// ── GET /api/v1/docs ─── Machine-readable API spec ───────────────────────────
v1.get('/docs', (req, res) => {
  res.json({
    ok: true,
    version: '1.0',
    baseUrl: `https://${BASE_DOMAIN}/api/v1`,
    auth: 'Authorization: Bearer <your_jtk_key>  OR  ?api_key=<your_jtk_key>',
    rateLimit: '120 requests / minute',
    endpoints: [
      { method: 'GET',    path: '/ping',                        description: 'Health check & API key validation' },
      { method: 'GET',    path: '/account',                     description: 'Your account profile' },
      { method: 'GET',    path: '/workspace',                   description: 'Full workspace snapshot' },
      { method: 'GET',    path: '/projects',                    description: 'List all projects. Query: status, subdomain, limit' },
      { method: 'GET',    path: '/projects/:id',               description: 'Get one project by id or subdomain' },
      { method: 'PATCH',  path: '/projects/:id',               description: 'Update project fields (name, branch, buildCommand, envVars…)' },
      { method: 'DELETE', path: '/projects/:id',               description: 'Remove project from workspace' },
      { method: 'POST',   path: '/projects/:id/redeploy',      description: 'Trigger a redeploy' },
      { method: 'POST',   path: '/projects/:id/stop',          description: 'Send stop signal to active deploy' },
      { method: 'POST',   path: '/projects/:id/restart',       description: 'Restart the running Docker container' },
      { method: 'GET',    path: '/projects/:id/status',        description: 'Live container status + CPU/RAM stats' },
      { method: 'GET',    path: '/projects/:id/logs',          description: 'Deployment history for project. Query: limit' },
      { method: 'GET',    path: '/projects/:id/env',           description: 'Get env vars (masked). Query: reveal=1 to unmask' },
      { method: 'PUT',    path: '/projects/:id/env',           description: 'Set / merge env vars (body: {KEY: VALUE})' },
      { method: 'GET',    path: '/deployments',                description: 'All deployments. Query: projectId, status, limit' },
      { method: 'GET',    path: '/deployments/:id',            description: 'Single deployment by id' },
      { method: 'GET',    path: '/databases',                  description: 'List managed databases' },
      { method: 'GET',    path: '/usage',                      description: 'Plan usage — projects, build time, bandwidth' },
      { method: 'GET',    path: '/settings',                   description: 'Your workspace settings' },
      { method: 'PATCH',  path: '/settings',                   description: 'Update safe settings (timezone, notifications…)' },
      { method: 'GET',    path: '/activity',                   description: 'Recent activity log. Query: limit (max 100)' },
      { method: 'GET',    path: '/export/:id',                 description: 'Export project. Query: format=joytree|render|dockerfile|dotenv' },
      { method: 'POST',   path: '/import',                     description: 'Import a project from another platform' },
      { method: 'GET',    path: '/docs',                       description: 'This machine-readable API reference' },
    ]
  });
});

// ── GET /api/v1/transfer ─── PUBLIC endpoint: paste jtk_ key → get all projects
// This is what pxxl / Render / Railway import pages call.
// Auth: ?api_key=jtk_xxx   (no session header needed — designed for cross-platform use)
// CORS: open — any origin can call this so import tools work from their domain
app.options('/api/v1/transfer', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.sendStatus(204);
});
app.get('/api/v1/transfer', async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  try {
    // Accept key from query string OR Authorization header
    const auth = String(req.headers.authorization || '').trim();
    const raw = auth.startsWith('Bearer ') ? auth.slice(7) : String(req.query.api_key || '').trim();
    if (!raw || !raw.startsWith('jtk_')) {
      return res.status(401).json({ ok: false, error: 'Provide your Joytree API key via ?api_key=jtk_... or Authorization: Bearer jtk_...' });
    }

    // Reverse-lookup the key owner
    const keyHash = crypto.createHash('sha256').update(raw).digest('hex');
    const indexUrl = `${FIREBASE_RTDB_URL}/deployboard_api_keys_index/${keyHash}.json` +
      (FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '');
    const idxR = await fetch(indexUrl, { headers: { Accept: 'application/json' } }).catch(() => null);
    let emailKey = null;
    if (idxR && idxR.ok) {
      const idxData = await idxR.json().catch(() => null);
      if (idxData && idxData.emailKey) emailKey = idxData.emailKey;
    }
    if (!emailKey) return res.status(401).json({ ok: false, error: 'API key not found or revoked.' });

    const rec = await getApiKeyRecord(emailKey);
    if (!rec || !rec.key || rec.key !== raw) return res.status(401).json({ ok: false, error: 'API key mismatch.' });
    if (rec.disabled) return res.status(403).json({ ok: false, error: 'API key is disabled.' });

    // Find the user
    let user = null;
    if (isDbReady()) {
      user = await User.findOne({ email: rec.email }).catch(() => null);
    } else {
      user = localAuth.users.find(u => String(u.email || '').toLowerCase() === rec.email) || null;
    }
    if (!user) return res.status(401).json({ ok: false, error: 'User account not found.' });

    // Load live workspace from Firebase
    const ws = (await readWorkspaceFromFirebase(user)) || {};
    const envStore = ws.envStore && typeof ws.envStore === 'object' ? ws.envStore : {};
    const projects = Array.isArray(ws.projects) ? ws.projects : [];

    // Build the full transfer payload — each project gets its full config + env keys
    const transferProjects = projects.map(p => {
      const env = envStore[p.id] || envStore[p.subdomain] || p.envVars || {};
      return {
        // Identity
        id:                  p.id,
        name:                p.name || p.subdomain || '',
        subdomain:           p.subdomain || '',
        // Source
        repoUrl:             p.repoUrl || '',
        branch:              p.branch || 'main',
        // Build
        buildCommand:        p.buildCommand || '',
        startCommand:        p.startCommand || '',
        nodeVersion:         p.nodeVersion || '20',
        isStatic:            !!p.isStatic,
        isDockerfileDeploy:  !!p.isDockerfileDeploy,
        // Runtime
        autoDeploy:          !!p.autoDeploy,
        // Current state
        status:              p.status || 'unknown',
        liveUrl:             p.liveUrl || `https://${p.subdomain}.${BASE_DOMAIN}`,
        createdAt:           p.createdAt || null,
        updatedAt:           p.updatedAt || null,
        // Environment — keys visible, values masked (user must copy secrets manually)
        envKeys:             Object.keys(env),
        envMasked:           Object.fromEntries(Object.entries(env).map(([k, v]) => [k, String(v).length > 0 ? '••••' : ''])),
        // Platform-specific export hints
        _renderYaml: [
          'services:',
          '  - type: web',
          `    name: ${p.name || p.subdomain}`,
          '    env: node',
          '    region: oregon',
          '    plan: free',
          `    branch: ${p.branch || 'main'}`,
          `    buildCommand: ${p.buildCommand || 'npm install && npm run build'}`,
          `    startCommand: ${p.startCommand || 'npm start'}`,
          '    envVars:',
          ...Object.keys(env).map(k => `      - key: ${k}
        sync: false`),
        ].join('\n'),
        _dockerfile: [
          `FROM node:${p.nodeVersion || '20'}-alpine`,
          'WORKDIR /app',
          'COPY package*.json ./',
          'RUN npm ci --only=production',
          'COPY . .',
          p.buildCommand ? `RUN ${p.buildCommand}` : null,
          'EXPOSE 3000',
          `CMD ${JSON.stringify((p.startCommand || 'node index.js').split(' '))}`,
        ].filter(Boolean).join('\n'),
      };
    });

    // Update last-used async
    fetch(apiKeyFbUrl(emailKey, '/lastUsed'), {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(new Date().toISOString())
    }).catch(() => {});

    // Refresh snapshot async
    refreshApiKeySnapshot(user, ws).catch(() => {});

    res.json({
      ok: true,
      source: 'joytree',
      sourceDomain: BASE_DOMAIN,
      owner: {
        email: rec.email,
        name: user.name || '',
        githubUsername: user.githubUsername || '',
      },
      projectCount: transferProjects.length,
      exportedAt: new Date().toISOString(),
      projects: transferProjects,
      // Instructions for the receiving platform
      _importGuide: {
        note: 'Env var values are masked. Ask the user to paste their secrets after import.',
        redeployEndpoint: `https://${BASE_DOMAIN}/api/v1/projects/:id/redeploy`,
        authHeader: `Authorization: Bearer ${raw}`,
        renderDocs: 'Use _renderYaml field per project to create render.yaml',
        dockerDocs: 'Use _dockerfile field per project to create a Dockerfile',
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// CLI v1 EXTENSION ROUTES — all accept Authorization: Bearer jtk_... API key
// These mirror existing session-only endpoints so the Joytree CLI can access
// every feature without needing a browser session token.
// ══════════════════════════════════════════════════════════════════════════════

// ── SSH Keys ──────────────────────────────────────────────────────────────────

v1.get('/ssh-keys', async (req, res) => {
  try {
    let keys = [];
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const userKey   = firebaseWorkspaceKey(req.user);
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const url = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}.json${authQuery}`;
      const r = await fetch(url).catch(() => null);
      if (r && r.ok) {
        const data = await r.json().catch(() => null);
        if (data && typeof data === 'object') {
          keys = Object.values(data).filter(Boolean)
            .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        }
      }
    }
    res.json({ ok: true, keys });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/ssh-keys', async (req, res) => {
  try {
    const { label, projectId, projectName } = req.body || {};
    const safeLabel = String(label || req.user.email || 'joytree-key')
      .replace(/[^a-zA-Z0-9@._-]/g, '-').slice(0, 64);
    const { generateKeyPairSync, createHash } = require('crypto');
    const { privateKey: privKeyObj, publicKey: pubKeyObj } = generateKeyPairSync('ed25519');
    const privateKeyPem = privKeyObj.export({ type: 'pkcs8', format: 'pem' });
    const pubDer  = pubKeyObj.export({ type: 'spki', format: 'der' });
    const keyType = Buffer.from('ssh-ed25519');
    const typeLen = Buffer.alloc(4); typeLen.writeUInt32BE(keyType.length);
    const rawKey  = pubDer.slice(-32);
    const keyLen  = Buffer.alloc(4); keyLen.writeUInt32BE(rawKey.length);
    const wireKey = Buffer.concat([typeLen, keyType, keyLen, rawKey]);
    const publicKey   = `ssh-ed25519 ${wireKey.toString('base64')} ${safeLabel}`;
    const fpHash      = createHash('sha256').update(wireKey).digest('base64').replace(/=+$/, '');
    const fingerprint = `SHA256:${fpHash}`;
    const sshDir   = path.join(process.env.HOME || '/root', '.ssh');
    const authKeys = path.join(sshDir, 'authorized_keys');
    fs.mkdirSync(sshDir, { recursive: true, mode: 0o700 });
    let existing = '';
    try { existing = fs.readFileSync(authKeys, 'utf8'); } catch (_) {}
    const keyBody = publicKey.split(' ')[1] || '';
    if (keyBody && !existing.includes(keyBody)) {
      fs.appendFileSync(authKeys, '\n' + publicKey + '\n', 'utf8');
      fs.chmodSync(authKeys, 0o600);
    }
    const keyRecord = {
      id: `sshkey_${Date.now()}`, label: safeLabel, publicKey, fingerprint,
      projectId: String(projectId || ''), projectName: String(projectName || ''),
      createdAt: new Date().toISOString(), createdBy: String(req.user.email || ''),
      host: BASE_DOMAIN, port: 22, user: process.env.USER || 'root',
    };
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const userKey   = firebaseWorkspaceKey(req.user);
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const url = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}/${keyRecord.id}.json${authQuery}`;
      await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(keyRecord) }).catch(() => {});
    }
    res.json({ ok: true, ...keyRecord, privateKey: privateKeyPem });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.delete('/ssh-keys/:keyId', async (req, res) => {
  try {
    const keyId   = String(req.params.keyId || '');
    const userKey = firebaseWorkspaceKey(req.user);
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const getUrl = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}/${keyId}.json${authQuery}`;
      const gr = await fetch(getUrl).catch(() => null);
      if (gr && gr.ok) {
        const rec = await gr.json().catch(() => null);
        if (rec && rec.publicKey) {
          const authKeysPath = path.join(process.env.HOME || '/root', '.ssh', 'authorized_keys');
          try {
            const content = fs.readFileSync(authKeysPath, 'utf8');
            const kb = rec.publicKey.split(' ')[1] || '';
            if (kb) fs.writeFileSync(authKeysPath, content.split('\n').filter(l => !l.includes(kb)).join('\n'), 'utf8');
          } catch (_) {}
        }
      }
      await fetch(getUrl, { method: 'DELETE' }).catch(() => {});
    }
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── GitHub ────────────────────────────────────────────────────────────────────

v1.get('/github/repos', async (req, res) => {
  try {
    const token = req.user.githubAccessToken;
    if (!token) return res.status(400).json({ ok: false, error: 'GitHub account not connected. Link it from the dashboard.' });
    let all = [];
    for (let page = 1; page <= 3; page++) {
      const r = await fetch(
        `https://api.github.com/user/repos?per_page=100&page=${page}&sort=updated&visibility=all&affiliation=owner,collaborator,organization_member`,
        { headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'joytree', 'Accept': 'application/vnd.github+json' } }
      );
      const data = await r.json();
      if (!r.ok) return res.status(r.status).json({ ok: false, error: data && data.message ? data.message : 'GitHub fetch failed' });
      if (!Array.isArray(data) || !data.length) break;
      all = all.concat(data);
      if (data.length < 100) break;
    }
    const repos = all.map(repo => ({
      id: repo.id, name: repo.name, full_name: repo.full_name,
      private: repo.private, description: repo.description || '',
      html_url: repo.html_url, default_branch: repo.default_branch,
      updated_at: repo.updated_at, language: repo.language,
    }));
    res.json({ ok: true, repos });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.get('/github/branches', async (req, res) => {
  try {
    const token   = req.user.githubAccessToken;
    const repoUrl = String(req.query.repoUrl || req.query.repo || '').trim();
    if (!token)   return res.status(400).json({ ok: false, error: 'GitHub account not connected.' });
    if (!repoUrl) return res.status(400).json({ ok: false, error: 'repoUrl query param is required.' });
    const m = repoUrl.match(/github\.com\/([^/]+)\/([^/#?]+)/i);
    if (!m) return res.status(400).json({ ok: false, error: 'Invalid GitHub repo URL.' });
    const slug = `${m[1]}/${m[2].replace(/\.git$/i, '')}`;
    const r = await fetch(`https://api.github.com/repos/${slug}/branches?per_page=100`, {
      headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'joytree' }
    });
    const data = await r.json();
    const branches = Array.isArray(data) ? data.map(b => ({ name: b.name, default: false })) : [];
    res.json({ ok: true, branches });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Domains ───────────────────────────────────────────────────────────────────

v1.get('/domains', async (req, res) => {
  try {
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    const domains = Array.isArray(ws.customDomains) ? ws.customDomains : [];
    res.json({ ok: true, domains });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/domains/attach', async (req, res) => {
  try {
    const { domain, projectId } = req.body || {};
    if (!domain)    return res.status(400).json({ ok: false, error: 'domain is required' });
    if (!projectId) return res.status(400).json({ ok: false, error: 'projectId is required' });
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.customDomains = ws.customDomains || [];
    const existing = ws.customDomains.find(d => d.domain === domain);
    if (existing) { existing.projectId = projectId; existing.updatedAt = new Date().toISOString(); }
    else ws.customDomains.push({ domain, projectId, verified: false, createdAt: new Date().toISOString() });
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, message: `Domain ${domain} attached to ${projectId}. Add a CNAME DNS record pointing to ${BASE_DOMAIN}.` });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/domains/:domain/verify', async (req, res) => {
  try {
    const domain = String(req.params.domain || '');
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.customDomains = ws.customDomains || [];
    const entry = ws.customDomains.find(d => d.domain === domain);
    if (!entry) return res.status(404).json({ ok: false, error: 'Domain not found' });
    const dns = require('dns').promises;
    let verified = false;
    try { const records = await dns.resolveCname(domain); verified = records.some(r => r.includes(BASE_DOMAIN)); } catch (_) {}
    entry.verified  = verified;
    entry.checkedAt = new Date().toISOString();
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, verified, domain, message: verified ? 'Domain verified!' : 'DNS not yet propagated. Check your CNAME record.' });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.delete('/domains/:domain', async (req, res) => {
  try {
    const domain = String(req.params.domain || '');
    const ws = (await readWorkspaceFromFirebase(req.user)) || {};
    ws.customDomains = (ws.customDomains || []).filter(d => d.domain !== domain);
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, message: `Domain ${domain} removed.` });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.get('/domains/check', async (req, res) => {
  try {
    const domain = String(req.query.domain || '').trim().toLowerCase();
    if (!domain) return res.status(400).json({ ok: false, error: 'domain query param is required' });
    const dns = require('dns').promises;
    let available = false;
    try { await dns.resolve(domain); available = false; } catch (e) { available = e.code === 'ENOTFOUND'; }
    res.json({ ok: true, domain, available });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Webhooks ──────────────────────────────────────────────────────────────────

v1.get('/webhook/secret', async (req, res) => {
  res.json({ ok: true, secret: GLOBAL_WEBHOOK_SECRET || '', webhookUrl: `https://${BASE_DOMAIN}/api/github/webhook`, note: 'Use this secret for all GitHub repository webhooks.' });
});

v1.post('/webhook/rotate', async (req, res) => {
  GLOBAL_WEBHOOK_SECRET = crypto.randomBytes(24).toString('hex');
  res.json({ ok: true, secret: GLOBAL_WEBHOOK_SECRET, webhookUrl: `https://${BASE_DOMAIN}/api/github/webhook`, note: 'Secret rotated. Update your GitHub webhook settings and add GLOBAL_WEBHOOK_SECRET to your .env.' });
});

// ── Autodeploy ────────────────────────────────────────────────────────────────

v1.patch('/projects/:id/autodeploy', async (req, res) => {
  try {
    const id      = String(req.params.id || '');
    const enabled = !!req.body.enabled;
    const ws      = (await readWorkspaceFromFirebase(req.user)) || {};
    const project = (ws.projects || []).find(p => p.id === id || p.subdomain === id);
    if (!project) return res.status(404).json({ ok: false, error: 'Project not found' });
    project.autoDeploy = enabled;
    project.autoDeployEnabled = enabled;
    project.autoDeployUpdatedAt = new Date().toISOString();
    await writeWorkspaceToFirebase(req.user, ws);
    res.json({ ok: true, enabled, projectId: project.id, subdomain: project.subdomain });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── Databases extras ──────────────────────────────────────────────────────────

v1.post('/databases', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
      body: JSON.stringify(req.body),
    });
    const data = await r.json().catch(() => ({}));
    res.status(r.status).json(data);
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/databases/:id/start', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases/${req.params.id}/start`, {
      method: 'POST', headers: { 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/databases/:id/stop', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases/${req.params.id}/stop`, {
      method: 'POST', headers: { 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/databases/:id/restart', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases/${req.params.id}/restart`, {
      method: 'POST', headers: { 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.post('/databases/:id/delete', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases/${req.params.id}/delete`, {
      method: 'POST', headers: { 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

v1.get('/databases/:id/logs', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/databases/${req.params.id}/logs`, {
      headers: { 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ── API Key rotation ──────────────────────────────────────────────────────────

v1.post('/apikey/rotate', async (req, res) => {
  try {
    const r = await fetch(`http://localhost:${PORT}/api/account/api-key/rotate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-deployboard-internal-key': INTERNAL_DEPLOY_KEY, 'x-deployboard-owner-id': String(req.user._id || req.user.id) },
    });
    res.status(r.status).json(await r.json().catch(() => ({})));
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

app.use('/api/v1', v1);

/* ═══════════════════════════════════════════════════════════════════════════ */

// ── WebSocket upgrade proxy ───────────────────────────────────────────────────
// HTTP-level proxy (above) only handles regular requests. WebSocket connections
// require a separate 'upgrade' event handler on the raw HTTP server.
// Without this, any deployed app that uses socket.io, ws, or SSE over WebSocket
// (e.g. the Lovable clone AI streaming, Uptime Kuma live updates) will silently
// fail — the HTTP handshake succeeds but the WS upgrade is never forwarded to
// the Docker container, so the connection hangs or errors out.
server.on('upgrade', (req, socket, head) => {
  const net = require('net');
  try {
    const host = normalizeHostHeader(
      req.headers['x-forwarded-host'] ||
      req.headers['host'] ||
      ''
    );

    // Let Socket.IO handle its own websocket upgrade for the DASHBOARD itself
    // (build:log/build:step live log stream, etc.). Socket.IO registers its own
    // 'upgrade' listener, but Node calls ALL listeners — if we destroy() the
    // socket here first, Socket.IO never gets to upgrade it, breaking the
    // dashboard's realtime connection.
    //
    // IMPORTANT: this only applies when the request is for the dashboard's own
    // host (BASE_DOMAIN / localhost / no host). For *.BASE_DOMAIN subdomains
    // (deployed apps), a request to /socket.io/ is THAT APP's websocket
    // (e.g. Uptime Kuma's live status updates) and must be proxied to its
    // container below — NOT skipped, or the deployed app's socket never
    // connects and its frontend renders a blank page.
    const isDashboardHost = !host || host === BASE_DOMAIN || host === 'localhost';
    if (isDashboardHost && req.url && req.url.startsWith('/socket.io/')) {
      return;
    }

    // Match *.BASE_DOMAIN subdomains
    const regex = new RegExp(`^([a-z0-9][a-z0-9-]{0,61}[a-z0-9]?)\\.${BASE_DOMAIN.replace(/\./g,'\\.')}$`);
    const match = host.match(regex);

    // Also check custom domain cache
    let subdomain = match ? match[1] : (_cdCache.get(host) || null);

    if (!subdomain) {
      socket.destroy();
      return;
    }

    reloadPortRegistryFromDisk();
    const appEntry = portRegistry[subdomain];
    if (!appEntry) {
      socket.destroy();
      return;
    }

    const target = parseProxyTarget(appEntry);
    if (!target) {
      socket.destroy();
      return;
    }

    const proxySocket = net.connect(target.port, target.host, () => {
      // Reconstruct the upgrade request headers to forward to the container
      const headerLines = [`${req.method} ${req.url} HTTP/1.1`];
      for (const [k, v] of Object.entries(req.headers)) {
        // Skip hop-by-hop except connection/upgrade which are needed for WS
        if (['proxy-authenticate','proxy-authorization','te','trailers','transfer-encoding'].includes(k.toLowerCase())) continue;
        headerLines.push(`${k}: ${Array.isArray(v) ? v.join(', ') : v}`);
      }
      // Ensure the container knows the real origin
      if (!req.headers['x-forwarded-for']) headerLines.push(`x-forwarded-for: ${req.socket.remoteAddress || ''}`);
      if (!req.headers['x-forwarded-proto']) headerLines.push(`x-forwarded-proto: https`);
      headerLines.push('');
      headerLines.push('');
      proxySocket.write(headerLines.join('\r\n'));
      if (head && head.length) proxySocket.write(head);
      proxySocket.pipe(socket);
      socket.pipe(proxySocket);
    });

    proxySocket.on('error', () => { try { socket.destroy(); } catch (_) {} });
    socket.on('error', () => { try { proxySocket.destroy(); } catch (_) {} });
    socket.on('close', () => { try { proxySocket.destroy(); } catch (_) {} });
    proxySocket.on('close', () => { try { socket.destroy(); } catch (_) {} });
  } catch (e) {
    console.error('[WS Proxy] upgrade error:', e.message);
    try { socket.destroy(); } catch (_) {}
  }
});

// ── Recover builds orphaned by a mid-deploy server restart ──────────────────
// A deploy's candidate container only gets promoted (renamed to its stable
// db-<sub> name + written into ports.json) at the very end of the build
// function, inside buildRunner.js. That function runs detached from the
// browser/SSE connection — closing the dashboard tab does NOT kill it. But
// if the deployboard process ITSELF is restarted while a build is still
// running (e.g. redeploying deployboard to pick up a fix), the build
// function dies with it, mid-flight, before it ever reaches the promote
// step or the catch block that would normally write a final 'success' or
// 'failed' status. The candidate container is left running forever under
// its "-cand-" name — often perfectly healthy — while the dashboard shows
// the deploy frozen on "building"/"still waiting for app HTTP port"
// indefinitely, because nothing ever told it the job was gone.
//
// On every startup we look for leftover "-cand-" containers: if one is
// actually healthy and serving HTTP, we finish the promotion the
// interrupted build never got to do (so the site actually comes online
// with zero manual cleanup); if it isn't, we remove it. Either way, any
// workspace record still stuck on 'building' for that project gets
// resolved to a real terminal status so the dashboard reflects reality
// instead of hanging forever.
async function recoverOrphanedBuildsOnStartup() {
  try {
    const raw = await execP(`docker ps -a --format '{{.Names}}' | grep -- '-cand-' || true`);
    const names = raw ? raw.split('\n').map(s => s.trim()).filter(Boolean) : [];
    if (!names.length) return;

    console.log(`[Startup Recovery] Found ${names.length} leftover candidate container(s) from an interrupted build`);
    const { detectLivePort } = require('./buildRunner');

    for (const candidateName of names) {
      const m = candidateName.match(/^(db-.+?)-cand-/);
      if (!m) continue;
      const containerName = m[1];
      const cleanSub = containerName.replace(/^db-/, '');

      let state = '';
      try { state = await execP(`docker inspect --format='{{.State.Status}}' ${candidateName}`); } catch (_) {}

      let promoted = false;
      let promotedPort = null;
      if (state === 'running') {
        try {
          const livePort = await detectLivePort(candidateName, 3000, 20, (line) => {
            console.log(`[Startup Recovery] ${String(line).replace(/\x1b\[[0-9;]*m/g, '')}`);
          });
          if (livePort) {
            // Archive whatever is currently live under the stable name (don't
            // just delete it — same "archive, don't destroy" behavior the
            // normal promotion step uses), then promote the recovered candidate.
            try {
              const prevState = await execP(`docker inspect --format='{{.State.Status}}' ${containerName}`);
              if (prevState) {
                await execP(`docker stop -t 20 ${containerName}`);
                await execP(`docker rename ${containerName} ${containerName}-prev-${Date.now()}`);
              }
            } catch (_) {}
            await execP(`docker rename ${candidateName} ${containerName}`);
            let registry = {};
            try { registry = JSON.parse(fs.readFileSync(PORTS_FILE, 'utf8')); } catch (_) {}
            registry[cleanSub] = `${containerName}:${livePort}`;
            fs.writeFileSync(PORTS_FILE, JSON.stringify(registry, null, 2));
            portRegistry = registry;
            console.log(`[Startup Recovery] \u2713 Finished interrupted deploy: promoted ${candidateName} -> ${containerName}:${livePort}`);
            promoted = true;
            promotedPort = livePort;
          }
        } catch (_) {}
      }

      if (!promoted) {
        try { await execP(`docker rm -f ${candidateName}`); } catch (_) {}
        console.log(`[Startup Recovery] Removed broken/unreachable orphaned candidate: ${candidateName}`);
      }

      // Resolve any workspace record still stuck on 'building' for this project.
      if (FIREBASE_RTDB_URL) {
        try {
          const authQuery = FIREBASE_RTDB_SECRET ? `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}` : '';
          const r = await fetch(`${FIREBASE_RTDB_URL}/deployboard_workspaces.json${authQuery}`);
          const allWs = await r.json().catch(() => ({}));
          for (const [key, ws] of Object.entries(allWs || {})) {
            let changed = false;
            const projects = Array.isArray(ws?.projects) ? ws.projects : [];
            const pIdx = projects.findIndex(p => p.subdomain === cleanSub && p.status === 'building');
            if (pIdx >= 0) {
              projects[pIdx] = { ...projects[pIdx], status: promoted ? 'success' : 'failed', updatedAt: new Date().toISOString() };
              changed = true;
            }
            const deployments = Array.isArray(ws?.deployments) ? ws.deployments : [];
            const dIdx = deployments.findIndex(d => d.subdomain === cleanSub && d.status === 'building');
            if (dIdx >= 0) {
              deployments[dIdx] = {
                ...deployments[dIdx],
                status: promoted ? 'success' : 'failed',
                endedAt: new Date().toISOString(),
                liveUrl: promoted ? `https://${cleanSub}.${BASE_DOMAIN}` : deployments[dIdx].liveUrl,
                note: promoted
                  ? 'Recovered on server restart \u2014 the app had actually started successfully, it just never got promoted before the interruption.'
                  : 'Interrupted by a server restart before it could finish. Please redeploy.'
              };
              changed = true;
            }
            if (changed) {
              const writeUrl = `${FIREBASE_RTDB_URL}/deployboard_workspaces/${encodeURIComponent(key)}.json${authQuery}`;
              await fetch(writeUrl, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(ws) }).catch(() => {});
            }
          }
        } catch (e) {
          console.warn('[Startup Recovery] Could not reconcile workspace status:', e.message);
        }
      }
    }
  } catch (e) {
    console.warn('[Startup Recovery] Failed:', e.message);
  }
}

server.listen(PORT, () => {
  console.log(`[Joytree] Running on http://localhost:${PORT}`);
  console.log(`[Joytree] Base domain: ${BASE_DOMAIN}`);
  console.log(`[Joytree] Sites dir:   ${SITES_DIR}`);
  // Rebuild ports.json from any already-running containers automatically
  recoverPortRegistryFromDocker().catch(() => {});
  // Finish or clean up any build interrupted by a previous server restart —
  // see recoverOrphanedBuildsOnStartup() above for why this exists.
  recoverOrphanedBuildsOnStartup().catch(() => {});
  // Restore flows from Firebase (fills gaps not covered by local api_catalog.json)
  restoreFlowRegistryFromFirebase().catch(() => {});
  // [FIX] On every startup, auto-repair any DB DNS records that are still
  // orange-cloud (proxied:true). This runs automatically so you never need
  // to manually fix DNS after moving to a new VPS — just set VPS_IP in .env
  // and restart. If VPS_IP isn't set yet, the IP auto-detect above will
  // trigger this after resolving the public IP.
  if (getVpsIp()) {
    setTimeout(() => autoRepairDbDnsRecords().catch(() => {}), 5000);
  }
  // [FIX] Apply WiredTiger cache limit + Docker memory caps to any existing
  // DB containers that were created before these fixes. Runs once at startup,
  // non-blocking — won't restart or disrupt running databases.
  setTimeout(() => applyMemoryLimitsToExistingDbContainers().catch(() => {}), 8000);
  // Pre-populate custom domain routing cache from Firebase + MongoDB
  setTimeout(() => refreshCustomDomainCache().catch(() => {}), 2000);

  // ===== SELF-HEARTBEAT WATCHDOG =====
  // [FIX] Covers the failure mode the crash guard above can't see: the
  // process stays alive (no exception ever thrown) but the HTTP listener
  // or event loop gets wedged — e.g. by a long synchronous block, or a
  // socket left in a bad state — and stops actually serving traffic. Since
  // nothing ever throws or exits, Docker's restart:unless-stopped policy
  // (confirmed correctly set on this container) never has anything to
  // react to. This performs a real HTTP round trip to our own listener,
  // the same path real traffic takes, on a short interval; after enough
  // consecutive failures it force-exits so Docker restarts us automatically
  // — usually within 1-2s — instead of needing a manual
  // `docker compose up --build` once everything is already down.
  const HEARTBEAT_INTERVAL_MS  = Number(process.env.HEARTBEAT_INTERVAL_MS  || 15000);
  const HEARTBEAT_TIMEOUT_MS   = Number(process.env.HEARTBEAT_TIMEOUT_MS   || 8000);
  const HEARTBEAT_MAX_FAILURES = Number(process.env.HEARTBEAT_MAX_FAILURES || 3);
  let _heartbeatFailures = 0;
  let _heartbeatExitScheduled = false;

  function _onHeartbeatFailure(reason) {
    _heartbeatFailures++;
    console.error(`[Watchdog] heartbeat failed (${_heartbeatFailures}/${HEARTBEAT_MAX_FAILURES}): ${reason}`);
    if (_heartbeatFailures >= HEARTBEAT_MAX_FAILURES && !_heartbeatExitScheduled) {
      _heartbeatExitScheduled = true;
      console.error('[Watchdog] server unresponsive — restarting process now (Docker restart:unless-stopped will bring it back)');
      process.exit(1);
    }
  }

  setInterval(() => {
    if (_heartbeatExitScheduled) return;
    const hbReq = http.get(
      { hostname: '127.0.0.1', port: PORT, path: '/__internal_heartbeat', timeout: HEARTBEAT_TIMEOUT_MS },
      (hbRes) => {
        hbRes.resume();
        if (hbRes.statusCode === 200) { _heartbeatFailures = 0; }
        else { _onHeartbeatFailure(`unexpected status ${hbRes.statusCode}`); }
      }
    );
    hbReq.on('timeout', () => { hbReq.destroy(); _onHeartbeatFailure('timed out'); });
    hbReq.on('error', (e) => _onHeartbeatFailure(e.message));
  }, HEARTBEAT_INTERVAL_MS);
  // ========================================================================
});

function sanitizeWorkingDir(dir = '') {
  const raw = String(dir || '').trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
  if (!raw || raw === '.') return '';
  const parts = raw.split('/').filter(Boolean);
  if (parts.some(part => part === '.' || part === '..')) return null;
  return parts.join('/');
}

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
function emitAutoDeployStatus(projectId, status, extra = {}, ownerUserId = '') {
  try {
    const payload = {
      projectId: String(projectId || ''),
      status: String(status || ''),
      at: new Date().toISOString(),
      ...extra
    };
    // Scope to the owner's room so other users never see this event
    const uid = String(ownerUserId || extra.ownerUserId || '');
    if (uid) {
      io.to('user:' + uid).emit('autodeploy:status', payload);
    } else {
      // Fallback: include projectId in payload so client can filter
      io.emit('autodeploy:status', payload);
    }
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
      // [FIX] This used to read user.workspace.projects directly -- the
      // in-memory (and on-disk, via local-auth.json) cache. That cache is
      // only ever as fresh as whatever was loaded when THIS process
      // started. Firebase is the real source of truth and can have moved on
      // since then (e.g. a project deleted, or a new deploy created, while
      // a previous process instance was still running). Every restart's
      // first poll was reading that stale snapshot, then
      // checkWorkspaceProjectAutoDeploy -> updateLocalWorkspaceProject would
      // write that same stale snapshot's *entire* project list straight
      // back to Firebase -- silently undoing anything that had changed
      // there since this process's local-auth.json was last saved.
      // Refreshing from Firebase here, right before using it, closes that
      // gap: the poller (and everything it calls) now always operates on
      // live data instead of a potentially-stale disk snapshot.
      try {
        const freshWs = await readWorkspaceFromFirebase(user);
        if (freshWs && typeof freshWs === 'object') user.workspace = freshWs;
      } catch (_) {}
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

/* ═══════════════════════════════════════════════════════════════
   IDE TERMINAL  — Socket.IO + child_process.spawn (NO node-pty)
   node-pty is intentionally NOT used — it compiles native code
   and leaks memory on low-RAM VPS servers, causing crashes.
   This implementation uses plain pipe-based bash sessions which
   are lightweight and never crash the host Node process.

   Safety limits (all overridable via env vars):
     IDE_TERM_MAX_SESSIONS   – max concurrent shells  (default 5)
     IDE_TERM_MEM_LIMIT_MB   – per-child RSS cap MB   (default 128)
     IDE_TERM_IDLE_TIMEOUT_MS– kill idle shell after  (default 5 min)
     IDE_TERM_RATE_MAX_BYTES – max stdin bytes/sec    (default 32 KB)
═══════════════════════════════════════════════════════════════ */
const IDE_TERM_MEM_LIMIT_MB   = Number(process.env.IDE_TERM_MEM_LIMIT_MB   || 128);   // 128 MB per shell
const IDE_TERM_MAX_SESSIONS   = Number(process.env.IDE_TERM_MAX_SESSIONS   || 5);     // max 5 shells total
const IDE_TERM_RATE_WIN_MS    = Number(process.env.IDE_TERM_RATE_WIN_MS    || 1000);
const IDE_TERM_RATE_MAX_BYTES = Number(process.env.IDE_TERM_RATE_MAX_BYTES || 32768); // 32 KB/s stdin
const IDE_TERM_IDLE_TIMEOUT_MS= Number(process.env.IDE_TERM_IDLE_TIMEOUT_MS|| 300000);// 5 min idle kill

// node-pty intentionally NOT loaded — it causes VPS OOM crashes.
// All terminal sessions use plain child_process.spawn with pipes.

const ideTermSessions = new Map(); // sessionId → { proc, socket, userId, bytesThisSec, rateTimer, memTimer, idleTimer, lastActivity }

function ideTermGenId() { return crypto.randomBytes(12).toString('hex'); }

// Monitor total Node.js process RSS — if it climbs too high:
//   1. Kill oldest IDE terminal sessions to free memory (existing behaviour)
//   2. If RSS exceeds a hard ceiling, do a graceful self-exit so Docker's
//      --restart=unless-stopped brings us back cleanly instead of waiting for
//      the Linux OOM-killer to SIGKILL us mid-request (which kills every
//      proxied site simultaneously with no warning).
const IDE_HOST_MEM_WARN_MB = Number(process.env.IDE_HOST_MEM_WARN_MB || 400);
// Hard ceiling: default 800 MB. Override via OOM_RESTART_MB env var.
// When RSS exceeds this the process exits(1) and Docker restarts it.
// Set to 0 to disable the auto-restart (not recommended on low-RAM VPS).
const OOM_RESTART_MB = Number(process.env.OOM_RESTART_MB || 800);
let _oomRestartScheduled = false;
setInterval(() => {
  try {
    const rssBytes = process.memoryUsage().rss;
    const rssMB    = Math.round(rssBytes / 1048576);

    // Step 1: warn + kill oldest terminal session
    if (rssMB > IDE_HOST_MEM_WARN_MB && ideTermSessions.size > 0) {
      const [oldestId] = ideTermSessions.keys();
      const oldSess = ideTermSessions.get(oldestId);
      if (oldSess) {
        oldSess.socket.emit('ide:term:data', '\r\n\x1b[31m⚠ Server memory high — session auto-closed\x1b[0m\r\n');
        oldSess.socket.emit('ide:term:killed', { reason: 'host_memory' });
        ideTermCleanup(oldestId);
      }
    }

    // Step 2: graceful self-restart before the OS OOM-killer strikes
    if (OOM_RESTART_MB > 0 && rssMB > OOM_RESTART_MB && !_oomRestartScheduled) {
      _oomRestartScheduled = true;
      console.error(`[OOM Guard] RSS ${rssMB} MB exceeds limit ${OOM_RESTART_MB} MB — scheduling graceful restart in 3 s`);
      // Give in-flight requests 3 s to finish, then exit cleanly.
      // Docker's --restart=unless-stopped will bring the process back up.
      setTimeout(() => {
        console.error('[OOM Guard] Exiting now — Docker will restart the container');
        process.exit(1);
      }, 3000);
    }
  } catch (_) {}
}, 10000);

function ideTermSpawn(sessionId, socket, userId, projectId, userEmail) {
  if (ideTermSessions.size >= IDE_TERM_MAX_SESSIONS) {
    socket.emit('ide:term:killed', { reason: 'server_capacity' });
    socket.emit('ide:term:data', '\r\n\x1b[31m⚠ Max terminal sessions reached — please wait\x1b[0m\r\n');
    return;
  }

  const isAdmin = String(userEmail || '').trim().toLowerCase() === JOYTREE_V3_ADMIN_EMAIL;

  // ── ADMIN: full host bash shell ──────────────────────────────────────────
  // Admin gets a real bash session on the VPS host with full access.
  // Non-admin users are isolated to docker exec into their own container only.

  // ── NON-ADMIN: resolve the user's container from their project ───────────
  let containerName = null;
  if (!isAdmin) {
    if (!projectId) {
      socket.emit('ide:term:data', '\r\n\x1b[31m✗ No project selected. Pick a project to open its shell.\x1b[0m\r\n');
      socket.emit('ide:term:killed', { reason: 'no_project' });
      return;
    }
    // Look up the container name from the port registry (subdomain → containerName:port)
    const entry = portRegistry[String(projectId)];
    containerName = entry ? _containerFromEntry(String(projectId), entry) : null;
    if (!containerName) {
      socket.emit('ide:term:data', `\r\n\x1b[31m✗ Container not found for project "${projectId}". Make sure it is deployed and running.\x1b[0m\r\n`);
      socket.emit('ide:term:killed', { reason: 'no_container' });
      return;
    }
  }

  // ── Resolve working directory (admin only — docker exec sets its own cwd) ─
  let cwd = process.env.HOME || '/tmp';
  if (isAdmin) {
    if (projectId && userId) {
      const uploadsFiles = path.join(UPLOADS_DIR, String(userId), String(projectId), 'files');
      if (fs.existsSync(uploadsFiles)) { cwd = uploadsFiles; }
      else {
        const uploadsRoot = path.join(UPLOADS_DIR, String(userId), String(projectId));
        if (fs.existsSync(uploadsRoot)) { cwd = uploadsRoot; }
        else {
          const siteDir = path.join(SITES_DIR, String(projectId));
          if (fs.existsSync(siteDir)) { cwd = siteDir; }
        }
      }
    } else if (projectId) {
      const siteDir = path.join(SITES_DIR, String(projectId));
      if (fs.existsSync(siteDir)) { cwd = siteDir; }
    }
  }

  const shell = (() => {
    const candidates = ['/bin/bash', '/usr/bin/bash', '/usr/local/bin/bash', '/bin/sh', '/usr/bin/sh'];
    for (const s of candidates) { try { if (fs.existsSync(s)) return s; } catch(_){} }
    return '/bin/sh';
  })();

  const baseEnv = {
    HOME:  process.env.HOME  || '/root',
    USER:  process.env.USER  || 'root',
    PATH:  process.env.PATH  || '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
    TERM:  'xterm-256color',
    LANG:  'en_US.UTF-8',
    SHELL: shell,
  };

  // ── NON-ADMIN: docker exec isolated shell ────────────────────────────────
  // Spawn a persistent docker exec session into the user's container.
  // We use a wrapping bash -c that runs docker exec interactively so all
  // stdin/stdout flow directly through — the user is inside the container
  // and cannot escape to the host.
  if (!isAdmin) {
    const cp = require('child_process');
    // Try bash first, fall back to sh — whichever the container has
    const containerShell = '/bin/sh';
    const child = cp.spawn('docker', [
      'exec', '-i',
      '-e', 'TERM=xterm-256color',
      containerName, containerShell
    ], {
      stdio: ['pipe', 'pipe', 'pipe'],
      detached: false,
    });

    child.on('error', (err) => {
      socket.emit('ide:term:data', `\r\n\x1b[31m✗ Could not exec into container: ${err.message}\x1b[0m\r\n`);
      socket.emit('ide:term:killed', { reason: 'exec_error' });
      ideTermCleanup(sessionId);
    });

    child.on('exit', (code) => {
      try { socket.emit('ide:term:data', `\r\n\x1b[33mContainer session ended (exit ${code ?? '?'}).\x1b[0m\r\n`); } catch(_){}
      socket.emit('ide:term:killed', { reason: 'exit' });
      ideTermCleanup(sessionId);
    });

    child.stdout.on('data', (d) => {
      lastActivity = Date.now();
      try { socket.emit('ide:term:data', d.toString('utf8')); } catch(_){}
    });
    child.stderr.on('data', (d) => {
      lastActivity = Date.now();
      try { socket.emit('ide:term:data', d.toString('utf8')); } catch(_){}
    });

    let bytesThisSec = 0;
    let lastActivity  = Date.now();
    const rateTimer   = setInterval(() => { bytesThisSec = 0; }, IDE_TERM_RATE_WIN_MS);
    const memTimer    = setInterval(() => {}, 60000);
    const idleTimer   = setInterval(() => {
      if (Date.now() - lastActivity > IDE_TERM_IDLE_TIMEOUT_MS) {
        try { socket.emit('ide:term:data', '\r\n\x1b[33m⚠ Session idle timeout — closing\x1b[0m\r\n'); } catch(_){}
        socket.emit('ide:term:killed', { reason: 'idle_timeout' });
        ideTermCleanup(sessionId);
      }
    }, 30000);

    const proc = {
      write: (data) => { try { child.stdin.write(data); } catch(_){} },
      resize: () => {},
      pid: child.pid,
      stdin: child.stdin,
      stdout: { destroy: () => {} },
      stderr: { destroy: () => {} },
      kill: () => { try { child.kill('SIGKILL'); } catch(_){} },
    };

    // echoInput for isolated mode: pass raw bytes directly to container stdin
    const echoInput = (raw) => {
      lastActivity = Date.now();
      try { child.stdin.write(raw); } catch(_){}
    };

    ideTermSessions.set(sessionId, {
      proc, socket, userId, bytesThisSec, rateTimer, memTimer, idleTimer, echoInput,
      get lastActivity() { return lastActivity; },
      set lastActivity(v) { lastActivity = v; },
    });

    socket.emit('ide:term:ready', { sessionId, mode: 'container', containerName });
    // Send a newline to trigger the container's shell prompt immediately
    try { child.stdin.write('\n'); } catch(_){}
    return;
  }

  // ── ADMIN: full host bash shell (existing implementation) ─────────────────

  // ── Prompt helper ──────────────────────────────────────────────────────────
  // Builds a coloured "joytree:/path# " prompt string and sends it to client.
  function sendPrompt() {

    // Show path relative to HOME for brevity, or absolute if outside HOME
    const home = baseEnv.HOME;
    let display = cwd;
    if (cwd === home) display = '~';
    else if (cwd.startsWith(home + '/')) display = '~/' + cwd.slice(home.length + 1);
    const prompt = '\r\n\x1b[32mjoytree\x1b[0m:\x1b[34m' + display + '\x1b[0m# ';
    try { socket.emit('ide:term:data', prompt); } catch(_) {}
  }

  // ── Line buffer + echo ────────────────────────────────────────────────────
  // Accumulate keystrokes into a line; echo them back; run on Enter.
  let lineBuf = '';
  let currentProc = null; // the running child process, if any

  function echoInput(raw) {
    let out = '';
    for (const ch of raw) {
      const code = ch.charCodeAt(0);
      if (code === 13 || code === 10) {
        // Enter — echo newline, then run the buffered command
        out += '\r\n';
        const cmd = lineBuf.trim();
        lineBuf = '';
        if (out) { try { socket.emit('ide:term:data', out); } catch(_){} out = ''; }
        runCommand(cmd);
        return;
      } else if (code === 127 || code === 8) {
        // Backspace
        if (lineBuf.length > 0) {
          lineBuf = lineBuf.slice(0, -1);
          out += '\b \b';
        }
      } else if (code === 3) {
        // Ctrl-C — kill running process if any
        if (currentProc) {
          try { currentProc.kill('SIGINT'); } catch(_){}
          out += '^C\r\n';
          currentProc = null;
          if (out) { try { socket.emit('ide:term:data', out); } catch(_){} out = ''; }
          sendPrompt();
          return;
        }
        // Nothing running — just echo ^C and reprint prompt
        lineBuf = '';
        out += '^C';
        if (out) { try { socket.emit('ide:term:data', out); } catch(_){} out = ''; }
        sendPrompt();
        return;
      } else if (code >= 32) {
        lineBuf += ch;
        out += ch;
      }
    }
    if (out) { try { socket.emit('ide:term:data', out); } catch(_){} }
  }

  // ── Command runner ────────────────────────────────────────────────────────
  // Runs a full shell command line via `bash -c` (one-shot, no persistent shell).
  // stdout/stderr stream back in real-time; prompt is reprinted on exit.
  function runCommand(cmd) {
    lastActivity = Date.now();
    if (!cmd) { sendPrompt(); return; }

    // Built-in: cd
    const cdMatch = cmd.match(/^cd(?:\s+(.+))?$/);
    if (cdMatch) {
      const target = cdMatch[1] ? cdMatch[1].trim().replace(/^~/, baseEnv.HOME) : baseEnv.HOME;
      const resolved = path.resolve(cwd, target);
      if (fs.existsSync(resolved) && fs.statSync(resolved).isDirectory()) {
        cwd = resolved;
      } else {
        try { socket.emit('ide:term:data', '\x1b[31mbash: cd: ' + target + ': No such file or directory\x1b[0m\r\n'); } catch(_){}
      }
      sendPrompt();
      return;
    }

    // Built-in: exit / logout
    if (cmd === 'exit' || cmd === 'logout') {
      try { socket.emit('ide:term:data', '\x1b[33mSession closed.\x1b[0m\r\n'); } catch(_){}
      socket.emit('ide:term:killed', { reason: 'exit' });
      ideTermCleanup(sessionId);
      return;
    }

    // Built-in: clear
    if (cmd === 'clear') {
      try { socket.emit('ide:term:data', '\x1b[2J\x1b[H'); } catch(_){}
      sendPrompt();
      return;
    }

    // Use /usr/bin/script (full path) to allocate a pseudo-TTY so npm and
    // other tools stream output in real time instead of buffering.
    // Fall back to stdbuf -oL (line-buffered) if script is unavailable.
    const cp = require('child_process');
    let child;
    try {
      const scriptBin  = require('fs').existsSync('/usr/bin/script')  ? '/usr/bin/script'  : null;
      const stdbufBin  = require('fs').existsSync('/usr/bin/stdbuf')  ? '/usr/bin/stdbuf'  : null;
      const safeCmd    = cmd.replace(/"/g, '\\"');
      const childEnv   = { ...baseEnv, PWD: cwd, FORCE_COLOR: '1', NPM_CONFIG_PROGRESS: 'true', CI: '' };
      if (scriptBin) {
        child = cp.spawn(scriptBin, ['-q', '-c', safeCmd, '/dev/null'], {
          cwd, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'], detached: false,
        });
      } else if (stdbufBin) {
        child = cp.spawn(stdbufBin, ['-oL', '-eL', shell, '-c', cmd], {
          cwd, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'], detached: false,
        });
      } else {
        child = cp.spawn(shell, ['-c', cmd], {
          cwd, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'], detached: false,
        });
      }
    } catch (e) {
      try { socket.emit('ide:term:data', '\x1b[31m' + String(e.message) + '\x1b[0m\r\n'); } catch(_){}
      sendPrompt();
      return;
    }

    currentProc = child;

    child.stdout.on('data', (d) => {
      lastActivity = Date.now();
      try { socket.emit('ide:term:data', d.toString('utf8')); } catch(_){}
    });
    child.stderr.on('data', (d) => {
      lastActivity = Date.now();
      try { socket.emit('ide:term:data', d.toString('utf8')); } catch(_){}
    });
    child.on('error', (err) => {
      try { socket.emit('ide:term:data', '\x1b[31mError: ' + String(err.message) + '\x1b[0m\r\n'); } catch(_){}
    });
    child.on('exit', (code) => {
      currentProc = null;
      if (code !== 0 && code !== null) {
        try { socket.emit('ide:term:data', '\x1b[90m[exit ' + code + ']\x1b[0m\r\n'); } catch(_){}
      }
      sendPrompt();
    });
  }

  // ── No persistent shell process — all commands run via runCommand() ───────
  // We use a dummy stub so ideTermSessions has the same shape as before.
  const proc = {
    write: () => {},   // not used — input goes through echoInput → runCommand
    resize: () => {},
    pid: null,
    stdin: null,
    stdout: { destroy: () => {} },
    stderr: { destroy: () => {} },
    kill: () => { if (currentProc) { try { currentProc.kill('SIGKILL'); } catch(_){} } },
  };

  let bytesThisSec = 0;
  let lastActivity  = Date.now();

  const rateTimer = setInterval(() => { bytesThisSec = 0; }, IDE_TERM_RATE_WIN_MS);

  // Idle timeout
  const idleTimer = setInterval(() => {
    if (Date.now() - lastActivity > IDE_TERM_IDLE_TIMEOUT_MS) {
      try { socket.emit('ide:term:data', '\r\n\x1b[33m⚠ Session idle timeout — closing\x1b[0m\r\n'); } catch(_){}
      socket.emit('ide:term:killed', { reason: 'idle_timeout' });
      ideTermCleanup(sessionId);
    }
  }, 30000);

  // No per-child memory guard needed — no persistent child process
  const memTimer = setInterval(() => {}, 60000); // no-op, kept for session shape

  ideTermSessions.set(sessionId, {
    proc, socket, userId, bytesThisSec, rateTimer, memTimer, idleTimer, echoInput,
    get lastActivity() { return lastActivity; },
    set lastActivity(v) { lastActivity = v; },
  });

  // Print the first prompt immediately
  socket.emit('ide:term:ready', { sessionId });
  sendPrompt();
}

function ideTermCleanup(sessionId) {
  const sess = ideTermSessions.get(sessionId);
  if (!sess) return;
  clearInterval(sess.rateTimer);
  clearInterval(sess.memTimer);
  clearInterval(sess.idleTimer);
  try {
    if (sess.proc.stdin && !sess.proc.stdin.destroyed) sess.proc.stdin.destroy();
    sess.proc.stdout.destroy();
    sess.proc.stderr.destroy();
    sess.proc.kill('SIGKILL');
  } catch (_) {}
  ideTermSessions.delete(sessionId);
}

// Register Socket.IO handlers
io.on('connection', (socket) => {
  socket.on('ide:term:start', async (data) => {
    try {
      const sessionId = ideTermGenId();
      const userId    = String(data?.userId || '');
      const projectId = String(data?.projectId || '');

      // Authenticate via the token the client sends — same token used for HTTP requests.
      // This is the only way to safely identify the user on a socket connection.
      let userEmail = '';
      const token = String(data?.token || '').trim();
      if (token) {
        try {
          let user = null;
          if (isDbReady()) {
            const session = await Session.findOne({ token, expiresAt: { $gt: new Date() } }).lean().catch(() => null);
            if (session) user = await User.findById(session.userId).lean().catch(() => null);
          } else {
            const session = localAuth.sessions.find(s => s.token === token && new Date(s.expiresAt) > new Date());
            if (session) user = localAuth.users.find(u => u.id === session.userId) || null;
          }
          if (user) userEmail = String(user.email || '').trim().toLowerCase();
        } catch (_) {}
      }

      ideTermSpawn(sessionId, socket, userId, projectId, userEmail);
    } catch (err) {
      console.error('[ide:term:start] error:', err && err.message || err);
    }
  });

  socket.on('ide:term:input', (data) => {
    try {
      const sessionId = String(data?.sessionId || '');
      const sess = ideTermSessions.get(sessionId);
      if (!sess || sess.socket.id !== socket.id) return;
      const input = String(data?.data || '');
      sess.bytesThisSec += input.length;
      if (sess.bytesThisSec > IDE_TERM_RATE_MAX_BYTES) {
        socket.emit('ide:term:rate_warn');
        return;
      }
      sess.lastActivity = Date.now();
      // All input goes through echoInput which handles echo + line buffering + command dispatch
      sess.echoInput(input);
    } catch (_) {}
  });

  socket.on('ide:term:resize', (data) => {
    try {
      const sess = ideTermSessions.get(String(data?.sessionId || ''));
      if (!sess || sess.socket.id !== socket.id) return;
      // resize is a no-op without pty — silently accept so client doesn't break
    } catch (_) {}
  });

  socket.on('ide:term:kill', (data) => {
    try {
      const sess = ideTermSessions.get(String(data?.sessionId || ''));
      if (sess && sess.socket.id === socket.id) ideTermCleanup(String(data.sessionId));
    } catch (_) {}
  });

  socket.on('disconnect', () => {
    try {
      for (const [sid, sess] of ideTermSessions) {
        if (sess.socket.id === socket.id) ideTermCleanup(sid);
      }
    } catch (_) {}
  });
});

/* ═══════════════════════════════════════════════════════════════
   SSH KEY API  — /api/ssh-keys
   Admin-only: generate ed25519 keypairs, list and revoke keys.
   Non-admin users get read-only access to their own key info.
   Keys are installed into ~/.ssh/authorized_keys on the VPS host.
═══════════════════════════════════════════════════════════════ */

// POST /api/ssh-keys/generate — generate a new ed25519 keypair
// Returns the private key (one-time) and installs the public key on the host.
app.post('/api/ssh-keys/generate', requireAuth, express.json(), async (req, res) => {
  try {
    const { label, projectId, projectName } = req.body || {};
    const safeLabel = String(label || req.user.email || 'joytree-key').replace(/[^a-zA-Z0-9@._-]/g, '-').slice(0, 64);

    // Generate ed25519 keypair using Node's built-in crypto — no ssh-keygen binary needed
    const { generateKeyPairSync } = require('crypto');
    const { privateKey: privKeyObj, publicKey: pubKeyObj } = generateKeyPairSync('ed25519');

    // Export private key as OpenSSH PEM format
    const privateKeyPem = privKeyObj.export({ type: 'pkcs8', format: 'pem' });

    // Export public key in OpenSSH authorized_keys format: "ssh-ed25519 <base64> <comment>"
    const pubDer    = pubKeyObj.export({ type: 'spki', format: 'der' });
    // OpenSSH public key wire format: length-prefixed "ssh-ed25519" + key bytes
    const keyType   = Buffer.from('ssh-ed25519');
    const typeLen   = Buffer.alloc(4); typeLen.writeUInt32BE(keyType.length);
    // The DER for ed25519 spki has the 32-byte key at the end
    const rawKey    = pubDer.slice(-32);
    const keyLen    = Buffer.alloc(4); keyLen.writeUInt32BE(rawKey.length);
    const wireKey   = Buffer.concat([typeLen, keyType, keyLen, rawKey]);
    const publicKey = `ssh-ed25519 ${wireKey.toString('base64')} ${safeLabel}`;

    // Convert private key to OpenSSH format using openssh-compatible PEM
    // Node exports PKCS8; we need to wrap it in a recognisable format.
    // Simplest: store the PKCS8 PEM — most modern SSH clients (OpenSSH 6.5+, VS Code) accept it.
    const privateKey = privateKeyPem;

    // Derive a short fingerprint (SHA256 of the public key blob)
    const { createHash } = require('crypto');
    const fpHash = createHash('sha256').update(wireKey).digest('base64').replace(/=+$/, '');
    const fingerprint = `SHA256:${fpHash}`;

    // Install public key into ~/.ssh/authorized_keys on host
    const sshDir   = path.join(process.env.HOME || '/root', '.ssh');
    const authKeys = path.join(sshDir, 'authorized_keys');
    fs.mkdirSync(sshDir, { recursive: true, mode: 0o700 });
    let existing = '';
    try { existing = fs.readFileSync(authKeys, 'utf8'); } catch (_) {}
    const keyBody = publicKey.split(' ')[1] || '';
    if (keyBody && !existing.includes(keyBody)) {
      fs.appendFileSync(authKeys, '\n' + publicKey + '\n', 'utf8');
      fs.chmodSync(authKeys, 0o600);
    }

    const keyRecord = {
      id:          `sshkey_${Date.now()}`,
      label:       safeLabel,
      publicKey,
      fingerprint,
      projectId:   String(projectId   || ''),
      projectName: String(projectName || ''),
      createdAt:   new Date().toISOString(),
      createdBy:   String(req.user.email || ''),
      host:        BASE_DOMAIN,
      port:        22,
      user:        process.env.USER || 'root',
    };

    // Store in Firebase RTDB under deployboard_sshkeys/<userKey>/<keyId>
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const userKey   = firebaseWorkspaceKey(req.user);
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const url = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}/${keyRecord.id}.json${authQuery}`;
      await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(keyRecord) }).catch(() => {});
    }

    res.json({ ok: true, ...keyRecord, privateKey });
  } catch (e) {
    console.error('[SSH] generate error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /api/ssh-keys/list — list keys from Firebase for this user
app.get('/api/ssh-keys/list', requireAuth, async (req, res) => {
  try {
    let keys = [];
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const userKey   = firebaseWorkspaceKey(req.user);
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      const url = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}.json${authQuery}`;
      const r = await fetch(url).catch(() => null);
      if (r && r.ok) {
        const data = await r.json().catch(() => null);
        if (data && typeof data === 'object') {
          keys = Object.values(data).filter(Boolean).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        }
      }
    }
    res.json({ ok: true, keys });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// DELETE /api/ssh-keys/:keyId — revoke a key by its Firebase record ID
app.delete('/api/ssh-keys/:keyId', requireAuth, async (req, res) => {
  try {
    const keyId   = String(req.params.keyId || '');
    const userKey = firebaseWorkspaceKey(req.user);
    // Remove from Firebase
    if (FIREBASE_RTDB_URL && FIREBASE_RTDB_SECRET) {
      const authQuery = `?auth=${encodeURIComponent(FIREBASE_RTDB_SECRET)}`;
      // Read the key first to get publicKey for authorized_keys removal
      const getUrl = `${FIREBASE_RTDB_URL}/deployboard_sshkeys/${userKey}/${keyId}.json${authQuery}`;
      const gr = await fetch(getUrl).catch(() => null);
      if (gr && gr.ok) {
        const rec = await gr.json().catch(() => null);
        // Remove from authorized_keys on host
        if (rec?.publicKey) {
          const authKeys = path.join(process.env.HOME || '/root', '.ssh', 'authorized_keys');
          try {
            const existing = fs.readFileSync(authKeys, 'utf8');
            const keyBody  = rec.publicKey.split(' ')[1] || '';
            if (keyBody) {
              const filtered = existing.split('\n').filter(l => !l.includes(keyBody)).join('\n');
              fs.writeFileSync(authKeys, filtered, 'utf8');
            }
          } catch (_) {}
        }
      }
      await fetch(getUrl, { method: 'DELETE' }).catch(() => {});
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

/* ═══════════════════════════════════════════════════════════════
   IDE GIT API  — /api/ide-git
   Runs git commands in the project's working directory on the VPS.
   Supports: init, status, log, commit, push, pull, set-remote
═══════════════════════════════════════════════════════════════ */
app.post('/api/ide-git', express.json(), async (req, res) => {
  try {
    const { action, projectId, pat, remote, message } = req.body || {};
    if (!action) return res.json({ ok: false, error: 'missing_action' });

    const projDir = projectId ? path.join(SITES_DIR, String(projectId)) : null;
    if (!projDir || !fs.existsSync(projDir)) {
      return res.json({ ok: false, error: 'project_dir_not_found — deploy project first to create directory' });
    }

    const { execFile } = require('child_process');
    const util = require('util');
    const execFileAsync = util.promisify(execFile);

    const git = async (...args) => {
      const env = { ...process.env, GIT_TERMINAL_PROMPT: '0' };
      if (pat) env.GIT_ASKPASS = 'echo';
      const { stdout, stderr } = await execFileAsync('git', args, { cwd: projDir, env, timeout: 30000 }).catch(e => ({ stdout: '', stderr: String(e.stderr || e.message || '') }));
      return (stdout + stderr).trim();
    };

    if (action === 'init') {
      const out = await git('init');
      return res.json({ ok: true, output: out });
    }

    if (action === 'status') {
      // Ensure git is initialized
      const out = await git('status', '--porcelain');
      const lines = out.split('\n').filter(Boolean);
      const changed = lines.map(l => ({ status: (l[0] || 'M').trim().toLowerCase() || 'm', path: l.slice(3).trim() }));
      const full = await git('status');
      return res.json({ ok: true, output: full, changed });
    }

    if (action === 'log') {
      const out = await git('log', '--oneline', '-20');
      return res.json({ ok: true, output: out || '(no commits yet)' });
    }

    if (action === 'commit') {
      if (!message) return res.json({ ok: false, error: 'missing commit message' });
      await git('add', '-A');
      const out = await git('commit', '-m', message);
      return res.json({ ok: true, output: out });
    }

    if (action === 'push') {
      if (!pat)    return res.json({ ok: false, error: 'missing PAT' });
      if (!remote) return res.json({ ok: false, error: 'missing remote URL' });
      // Inject PAT into URL
      const authedRemote = remote.replace('https://', 'https://' + encodeURIComponent(pat) + '@');
      const out = await git('push', authedRemote, 'HEAD:main', '--force');
      return res.json({ ok: true, output: out });
    }

    if (action === 'pull') {
      const out = await git('pull');
      return res.json({ ok: true, output: out });
    }

    if (action === 'set-remote') {
      if (!remote) return res.json({ ok: false, error: 'missing remote URL' });
      // Remove existing origin then add new
      await git('remote', 'remove', 'origin').catch(()=>{});
      const out = await git('remote', 'add', 'origin', remote);
      return res.json({ ok: true, output: 'Remote set to ' + remote });
    }

    return res.json({ ok: false, error: 'unknown action: ' + action });
  } catch (e) {
    return res.json({ ok: false, error: String(e.message || e).slice(0, 200) });
  }
});
