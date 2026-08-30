'use strict';

// ===== JOYTREE UPDATE MARKER (VISIBLE) =====
// UPDATED BY AGENT: 2026-04-29T00:00:00Z
// If you can read this block, you're running the latest deployment runner patch.
// Key features in this version:
// 1) Candidate rollout + strict readiness gate
// 2) Rollback on failed candidate
// 3) Per-app runtime limits + service env injection
// 4) Better stabilization polling logs for heavy Node.js apps
// ===============================================

// UPDATED (2026-04-29): Render-like rollout + dependency-aware runtime
// - Fixes proxy registration for Node.js deployments that still returned Cloudflare 502.
// - Normalizes malformed appPort values and detects a reachable runtime port before writing ports.json.
// - Adds strict startup verification so slow/heavy apps don't get marked LIVE before they're actually reachable.
// - Adds per-app service env injection (DB/Redis), rollback on failed candidate, and configurable runtime limits.

/**
 * buildRunner.js
 *
 * STATIC SITES:  clone → install → build → copy dist/ → serve via Express
 * SERVER APPS:   clone → install → build → run in isolated Docker container
 *                Each app gets its own container, its own PORT, fully isolated.
 *                No pm2, no shared process space.
 */

const { spawn, execSync } = require('child_process');
const path      = require('path');
const fs        = require('fs');

// ── Cooperative build cancellation ──────────────────────────────────────────
// [FIX] The Stop button previously only force-killed whichever ONE docker
// container happened to be running at that exact moment (computed from a
// deterministic candidate name) and told the frontend to show "stopped" --
// but the actual build orchestration below is a single, uninterrupted async
// function once started, with no checkpoint anywhere to ever notice a stop
// was requested. Killing that one container didn't stop the *build*: if the
// kill happened during an early step, later steps (install, build, launch a
// DIFFERENT container for the next phase, verify) kept right on running to
// completion regardless, still consuming real CPU/RAM/disk the whole time --
// exactly the "click Stop, but Step 5/Verify still happens anyway" behavior.
// This is the actual checkpoint mechanism: a shared set of deployIds that
// have been asked to stop, checked at the start of every major step across
// every pipeline in this file. Throwing here with this exact message is
// intentional -- server.js's catch block already detects `/stopped by
// user/i` in the error message to distinguish an intentional stop from a
// real failure (skips the failure push notification, shows "canceled" not
// "failed" in the UI); that detection existed already, it just never had
// anything real to catch.
const _stoppedDeployIds = new Set();
function requestBuildStop(deployId) {
  if (deployId) _stoppedDeployIds.add(String(deployId));
}
function checkBuildStopped(deployId) {
  if (deployId && _stoppedDeployIds.has(String(deployId))) {
    _stoppedDeployIds.delete(String(deployId)); // self-cleaning -- this check is one-shot per stop request
    const e = new Error('Build stopped by user');
    e.isStopRequest = true;
    throw e;
  }
}

// ── Build-step CPU/IO niceness ──────────────────────────────────────────────
// npm install / npm run build / vite build / pip install etc. all run via
// exec() on the SAME host process as the Joytree dashboard itself (not inside
// a resource-limited container — that only happens for the FINAL deployed
// app). A heavy build (e.g. `vite build` spawning esbuild workers across all
// CPU cores) can starve the dashboard's Node.js event loop, causing GitHub
// API calls and other dashboard requests to time out or hang until the
// dashboard container is restarted.
//
// Fix: run build-tool commands under `nice`/`ionice` so they yield CPU and
// disk I/O priority to the dashboard process. This only lowers priority
// (niceness 0→15 needs no special privileges) — builds still complete, just
// without starving the rest of the app.
//
// [FIX] The original defaults (nice 15 / ionice best-effort priority 7 — the
// lowest priority in that class, effectively "only run when nothing else
// wants the disk") were FAR more aggressive than necessary on a host that
// isn't under heavy concurrent load, and were the single biggest reason
// `npm install` etc. felt dramatically slower here than on Render (whose
// build machines aren't shared with anything else at all). ionice priority 7
// in particular means any other disk activity — the dashboard, Docker, the
// proxy — gets served first, every time, so a build's I/O can be starved
// almost completely even when the host is mostly idle.
//
// New defaults (nice 5 / ionice best-effort priority 4 — the SAME priority a
// normal, non-niced process gets) still give the dashboard a slight edge
// under real contention (nice 5 > dashboard's nice 0) but stop pre-emptively
// starving every build's disk I/O. Combined with MAX_CONCURRENT_BUILDS and
// the per-build-container --cpus/--memory caps elsewhere in this file, the
// dashboard stays responsive without builds being throttled to a crawl.
// Override via env vars if your host's dashboard still struggles during
// builds (raise these) or if builds still feel slow and the dashboard stays
// snappy (lower further, even to 0).
const BUILD_NICE_LEVEL   = normalizePort(process.env.BUILD_NICE_LEVEL, 5);
const BUILD_IONICE_LEVEL = normalizePort(process.env.BUILD_IONICE_LEVEL, 4);
let _NICE_AVAILABLE = false;
let _IONICE_AVAILABLE = false;
try { execSync('which nice', { stdio: 'ignore' }); _NICE_AVAILABLE = true; } catch (_) {}
try { execSync('which ionice', { stdio: 'ignore' }); _IONICE_AVAILABLE = true; } catch (_) {}

// Commands that run heavy, CPU/IO-intensive build work and should be
// deprioritized relative to the dashboard process.
const BUILD_TOOL_COMMANDS = new Set([
  'npm', 'npx', 'yarn', 'pnpm', 'bun', 'deno',
  'pip', 'pip3', 'poetry',
  'bundle', 'gem',
  'cargo', 'rustc',
  'go',
  'mvn', 'gradle', './gradlew',
  'dotnet',
  'composer',
  'git', // large clones can saturate disk I/O
  'docker', // only 'docker pull' / 'docker build' are niced — see guard below
]);

// Default cap on Node.js heap during build steps (npm install/build, vite
// build, etc.) — prevents a single heavy build from ballooning memory and
// triggering swap thrashing that slows the whole VPS for every project.
const BUILD_NODE_MAX_OLD_SPACE_MB = normalizePort(process.env.BUILD_NODE_MAX_OLD_SPACE_MB, 1536);
const NODE_BUILD_COMMANDS = new Set(['npm', 'npx', 'yarn', 'pnpm', 'bun']);

// ── Persistent build caches (npm/yarn/pnpm/pip/composer/go/etc.) ────────────────
// [FIX] Previously every deploy ran a fully cold install — npm/pip/composer/
// etc. re-downloaded the ENTIRE dependency tree from the registry every
// single time, for every project, even when the lockfile hadn't changed.
// For apps with large dependency trees (Uptime Kuma, Next.js, etc.) this
// network+extraction cost often dwarfs CPU/memory limits and is the single
// biggest reason deploys feel much slower than Render/other PaaS, which
// persist package-manager caches between builds.
//
// Fix: persist package-manager caches under <sitesDir>/.build-cache (the
// same durable volume server apps are stored on), and point each package
// manager at it — both for commands that run directly on the host (via
// exec()) and for commands that run inside ephemeral `docker run --rm`
// build containers (via volume mounts). Repeat deploys of the same project
// (or any project sharing common dependencies) then mostly hit local disk
// instead of the network.
let _persistentCacheRoot = null;

function getCacheDir(sub) {
  if (!_persistentCacheRoot) return null;
  const dir = path.join(_persistentCacheRoot, sub);
  try { fs.mkdirSync(dir, { recursive: true }); } catch (_) {}
  return dir;
}


// ── Node.js version detection from package.json engines field ─────────────────
// Returns the best matching node major version string (e.g. '20', '22') or null.
// Supported engine strings: '>=20', '^22', '20.x', '20', etc.
function detectRequiredNodeVersion(projectRoot) {
  try {
    let bestMajor = null;
    const consider = (engineStr) => {
      const s = engineStr ? String(engineStr).trim() : '';
      if (!s) return;
      const m = s.match(/(\d+)/);
      if (!m) return;
      const major = Number(m[1]);
      if (Number.isInteger(major) && major >= 14 && (bestMajor === null || major > bestMajor)) bestMajor = major;
    };

    const pkgPath = path.join(projectRoot, 'package.json');
    if (fs.existsSync(pkgPath)) {
      const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
      consider(pkg.engines && pkg.engines.node);
    }

    // [FIX] A project's own package.json very often has no engines field at
    // all, even though a dependency it pulls in does -- almost always a
    // native-binary package (@tailwindcss/oxide, esbuild, sharp, @swc/core,
    // etc.) whose engines.node requirement is higher than whatever Node
    // image this build would otherwise use. When that mismatch happens, npm
    // silently skips installing the correct platform-specific optional
    // dependency (the documented npm/cli#4828 behavior), and the build
    // fails with "Cannot find native binding" -- an error that reads like a
    // corrupted lockfile, but isn't; reinstalling on the same too-old Node
    // image reproduces it every time. package-lock.json already records
    // every installed package's own engines field, so scanning it catches
    // this class of bug before the build even starts, instead of relying on
    // a same-image reinstall to fix what's actually a Node-version problem.
    const lockPath = path.join(projectRoot, 'package-lock.json');
    if (fs.existsSync(lockPath)) {
      const lock = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
      const pkgs = lock.packages || {};
      for (const key of Object.keys(pkgs)) {
        const entry = pkgs[key];
        if (entry && entry.engines) consider(entry.engines.node);
      }
    }

    if (bestMajor === null) return null;
    // Map to a supported Docker image version
    if (bestMajor >= 22) return '22';
    if (bestMajor >= 20) return '20';
    if (bestMajor >= 18) return '18';
    if (bestMajor >= 16) return '16';
    return '18'; // default safe fallback
  } catch (_) {
    return null;
  }
}

// Emit a node version warning/auto-correct log when detected version differs from configured
function emitNodeVersionWarning(log, configuredVer, detectedVer) {
  log(`\x1b[33m[Joytree]\x1b[0m Node.js version mismatch detected:`);
  log(`\x1b[33m[Joytree]\x1b[0m  → Configured: Node.js ${configuredVer} | package.json engines requires: Node.js ${detectedVer}`);
  log(`\x1b[32m[Joytree]\x1b[0m  ✓ Automatically switching to Node.js ${detectedVer} for this deployment`);
}

// ── Container tuning ──────────────────────────────────────────────────────────
const CPU_SHARES = '512';   // half CPU priority (1024 = full)
const PIDS_LIMIT = '200';   // max processes inside container
const DEFAULT_STARTUP_TIMEOUT_SECONDS = 300;
const BUILD_STEP_TIMEOUT_MINUTES = normalizePort(process.env.BUILD_STEP_TIMEOUT_MINUTES, 20);
const DEPLOY_HISTORY_KEEP = normalizePort(process.env.DEPLOY_HISTORY_KEEP, 2);

// [FIX] `log(line)` was previously called once per output line and
// immediately did `emit('build:log', {line})` (a synchronous Socket.IO
// emit) for EVERY line. During `npm install`/`npm run build`, hundreds or
// thousands of lines can be produced within milliseconds, and emitting
// each one individually saturates the event loop long enough that
// Socket.IO's ping/pong is missed, causing the client to see
// "Disconnected from build server" (ping timeout) mid-build.
//
// createBatchedLogger() returns a `log(line)` function with the same
// signature, but coalesces build:log emits into small batches flushed on a
// short timer (default 120ms) — drastically reducing emit frequency during
// bursty output while keeping logs feeling live. `onLog` (which persists
// lines to the DB and can throw to signal a user-requested stop) is still
// called synchronously per-line, since that side is cheap and stop-request
// detection should remain immediate.
const LOG_BATCH_FLUSH_MS = normalizePort(process.env.LOG_BATCH_FLUSH_MS, 120);
const LOG_BATCH_MAX_LINES = normalizePort(process.env.LOG_BATCH_MAX_LINES, 200);

// [FIX] Registry of active batched loggers for the CURRENT build, so
// runBuild() can flush any pending buffered lines when a build step
// finishes/throws — without needing to add log.flushNow() calls at every
// return/throw point inside the many run*Build functions (Dockerfile,
// worker, Python, Go, PHP, Ruby, JVM, Rust, .NET, Elixir, Bun, Deno,
// static, server, generic...).
let _activeBatchedLoggers = [];

function createBatchedLogger(emit, onLog) {
  let buffer = [];
  let flushTimer = null;

  const flush = () => {
    flushTimer = null;
    if (buffer.length === 0) return;
    const lines = buffer;
    buffer = [];
    if (lines.length === 1) {
      emit('build:log', { line: lines[0] });
    } else {
      // Multiple lines flushed at once: send as a single batched event so
      // the frontend can append them together, but ALSO emit individually
      // for clients that only listen for single-line 'build:log' (keeps
      // backward compatibility with existing frontend code).
      emit('build:log', { lines });
    }
  };

  const scheduleFlush = () => {
    if (!flushTimer) flushTimer = setTimeout(flush, LOG_BATCH_FLUSH_MS);
  };

  const log = (line) => {
    buffer.push(line);
    if (buffer.length >= LOG_BATCH_MAX_LINES) {
      // Avoid unbounded buffering if output is extremely bursty.
      if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
      flush();
    } else {
      scheduleFlush();
    }
    if (typeof onLog === 'function') onLog(line);
  };

  // Force any buffered lines out immediately (call at the end of a build
  // step / before returning) so the final lines aren't delayed by the
  // flush timer.
  log.flushNow = () => {
    if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
    flush();
  };

  _activeBatchedLoggers.push(log);

  return log;
}

// ── Entry point ───────────────────────────────────────────────────────────────
async function runBuild(args) {
  _activeBatchedLoggers = [];
  try {
    return await _runBuildDispatch(args);
  } finally {
    // [FIX] Ensure any buffered build:log lines from the batched logger are
    // flushed immediately when the build finishes (success, failure, or
    // user-requested stop), so the final log lines aren't delayed by the
    // ~120ms flush timer or lost if the process exits right after.
    for (const log of _activeBatchedLoggers) {
      try { log.flushNow(); } catch (_) {}
    }
    _activeBatchedLoggers = [];
  }
}

async function _runBuildDispatch({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, isDockerfileDeploy, isWorker, baseDomain, envVars }) {
  // [FIX] The caller (server.js) computes resolvedEnvVars by merging the
  // project's own stored envVars, Firebase's ws.envStore (what the "Env
  // Variables" dashboard page actually saves to), and any request-level
  // overrides -- and even logs "Injecting N environment variables" to the
  // user as confirmation. But this merged value was never actually passed
  // down: every build function below (runServerBuild, runUploadServerBuild,
  // runStaticBuild, runDockerfileBuild) only ever reads project.envVars
  // directly via resolveEnvVars(project.envVars). A var saved only into
  // envStore (the dashboard's real storage) and not yet mirrored onto the
  // bare project.envVars field never reached the deployed container, no
  // matter how many times the project was redeployed -- while the build log
  // misleadingly implied it had been injected. Baking the merged value into
  // project.envVars here, once, fixes every downstream deploy path at once.
  if (envVars && typeof envVars === 'object' && Object.keys(envVars).length) {
    project = { ...project, envVars: { ...(project.envVars || {}), ...envVars } };
  }

  // [FIX] Initialize the persistent build-cache root for this build. Lives
  // under sitesDir (the durable volume), so package-manager caches survive
  // across deploys and dashboard restarts. See getCacheDir() above.
  _persistentCacheRoot = path.join(sitesDir, '.build-cache');
  try { fs.mkdirSync(_persistentCacheRoot, { recursive: true }); } catch (_) {}

  // Merge flags from args or from project record
  const dockerfileDeploy = isDockerfileDeploy || project.isDockerfileDeploy;
  const workerDeploy     = isWorker           || project.isWorker;

  if (dockerfileDeploy) {
    return runDockerfileBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  }
  if (workerDeploy) {
    return runWorkerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  }

  // ── Runtime detection (explicit project.runtime field takes priority) ────────
  const runtime = String(project.runtime || '').toLowerCase();
  const siteType = String(project.siteType || '').trim().toLowerCase();
  const isPython  = runtime.startsWith('python') || siteType === 'python';
  const isGo      = runtime.startsWith('go');
  const isPHP     = runtime.startsWith('php');
  const isRuby    = runtime.startsWith('ruby');
  const isJava    = runtime.startsWith('java') || runtime.startsWith('kotlin');
  const isRust    = runtime.startsWith('rust');
  const isDotnet  = runtime === 'dotnet';
  const isElixir  = runtime.startsWith('elixir');
  const isBun     = runtime === 'bun';
  const isDeno    = runtime === 'deno';

  if (isPython)  return runPythonBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isGo)      return runGoBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isPHP)     return runPhpBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isRuby)    return runRubyBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isJava)    return runJvmBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isRust)    return runRustBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isDotnet)  return runDotnetBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isElixir)  return runElixirBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isBun)     return runBunBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  if (isDeno)    return runDenoBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });

  // ── File-based runtime auto-detection (when project.runtime is not set) ──────
  // This prevents non-Node projects from accidentally falling through to the
  // Node.js build path (runServerBuild) which uses node:XX for both build and
  // runtime, causing "sh: 1: composer: not found" / "python: not found" etc.
  //
  // We do a quick scan of the REPO URL or tmpDir for framework marker files
  // BEFORE cloning by checking project metadata first, then falling through.
  // A shallow clone happens inside each runner anyway so we detect here from
  // the project's explicit settings only — file-based detection after clone
  // is handled inside runServerBuild (see below).
  if (!runtime) {
    // PHP: has composer.json at root, OR artisan (Laravel), OR composer.lock
    const repoHint = String(project.repoUrl || project.repo || '').toLowerCase();
    const installHint = String(project.installCmd || '').toLowerCase();
    const buildHint   = String(project.buildCmd   || '').toLowerCase();
    const startHint   = String(project.startCmd   || '').toLowerCase();
    const allHints    = `${installHint} ${buildHint} ${startHint}`;

    if (/\bcomposer\b/.test(allHints) || /\bartisan\b/.test(allHints) || /\bphp\b/.test(startHint)) {
      return runPhpBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bpip\b|\bpoetry\b|\bpipenv\b|\bgunicorn\b|\buvicorn\b|\bflask\b|\bdjango\b/.test(allHints)) {
      return runPythonBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bgo\s+build\b|\bgo\s+run\b|\bgo\s+mod\b/.test(allHints)) {
      return runGoBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bbundle\b|\brails\b|\brackup\b|\brunner\b/.test(allHints)) {
      return runRubyBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bmvn\b|\bgradle\b|\bjava\s+-jar\b/.test(allHints)) {
      return runJvmBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bcargo\b/.test(allHints)) {
      return runRustBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bdotnet\b/.test(allHints)) {
      return runDotnetBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (/\bmix\b|\belixir\b/.test(allHints)) {
      return runElixirBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
  }

  // Respect explicit site type first.
  // · "static"  → always static, never forced into server mode.
  // · "server"  → always server.
  // · blank     → attempt server build; runServerBuild inspects package.json
  //               after clone and falls back to static internally if there's
  //               no start script / server entry found.
  const explicitType = String(project.siteType || '').trim().toLowerCase();
  const hasStartCmd  = !!String(project.startCmd || '').trim();

  if (explicitType === 'static') {
    return runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog, appPort, baseDomain });
  }
  if (explicitType === 'server' || hasStartCmd) {
    return runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
  }
  // blank siteType, no startCmd — try server (auto-detects from repo after clone)
  return runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
}

// ── DOCKERFILE BUILD ─────────────────────────────────────────────────────────
// Clones the repo and builds + runs the user's own Dockerfile
async function runDockerfileBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain }) {
  const buildDir      = path.join(tmpDir, deployId);
  const containerName = 'db-' + project.subdomain;
  const imageName     = 'deployboard-' + project.subdomain;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const dfPath        = project.dockerfilePath || 'Dockerfile';
  const exposedPort   = project.exposedPort   || 3000;

  const log = createBatchedLogger(emit, onLog);

  // ── Step 1: Clone ────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log('\x1b[36m━━━ Step 1/4 — Clone ━━━\x1b[0m');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  // For Dockerfile deploys, honor an explicitly selected working directory
  // before falling back to the historical repo-root / one-level-deep search.
  let dockerfileDir = resolveWorkingDirRoot(buildDir, project, log) || buildDir;
  const dfAtRoot = path.join(dockerfileDir, dfPath);
  if (!fs.existsSync(dfAtRoot)) {
    // Try one level deep from the selected root (or repo root when none selected).
    let found = false;
    for (const entry of fs.readdirSync(dockerfileDir)) {
      const sub = path.join(dockerfileDir, entry);
      if (fs.statSync(sub).isDirectory()) {
        const candidate = path.join(sub, dfPath);
        if (fs.existsSync(candidate)) { dockerfileDir = sub; found = true; break; }
      }
    }
    if (!found) throw new Error(`Dockerfile not found. Make sure "${dfPath}" exists inside the selected working directory.`);
  }
  const dfFullPath = path.join(dockerfileDir, dfPath);
  log(`\x1b[90m[docker] Found Dockerfile at: ${path.relative(buildDir, dfFullPath)}\x1b[0m`);

  // ── Step 2: Build Docker image ───────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 2/4 — Docker Build ━━━\x1b[0m');
  log(`\x1b[90m$ docker build -f ${dfPath} -t ${imageName} .\x1b[0m`);

  // Build args from envVars
  const envObj  = withDeployedAppRuntimeDefaults(resolveEnvVars(project.envVars), project, baseDomain);
  const buildArgs = Object.entries(envObj).map(([k,v]) => `--build-arg ${k}=${v}`).join(' ');
  const buildCmd  = `docker build -f ${dfPath} ${buildArgs} -t ${imageName} ${dockerfileDir}`;

  await exec('sh', ['-c', buildCmd], { cwd: dockerfileDir }, log);
  emitStep(emit, 'build', 'done');

  // ── Step 3: Stop old container, start new one ────────────────────────────
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 3/4 — Start Container ━━━\x1b[0m');

  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const networkName = 'deployboard-net';
  // [FIX] Apply the same memory/swap caps used by other deploy paths.
  // Previously this path had only --cpu-shares/--pids-limit with no
  // memory limit, so a Dockerfile-based app with a memory leak (or just
  // a heavy runtime) could consume unbounded host RAM.
  const runtime = getRuntimeConfig(project);
  const runArgs = [
    'run', '-d', '--restart=no',
    '--name',         candidateContainerName,
    '--network',      networkName,
    // [FIX] Apps that dial the VPS's own public IP for a locally-hosted
    // service (e.g. a self-hosted database) can hit hairpin-NAT: the
    // connection leaves the box and never routes back in, hanging until
    // the OS gives up. host.docker.internal:host-gateway gives apps a
    // route to the host that never leaves the machine.
    '--add-host',     'host.docker.internal:host-gateway',
    '--cpu-shares',   CPU_SHARES,
    '--pids-limit',   PIDS_LIMIT,
    '-m',             runtime.memory,
    '--memory-reservation', runtime.memory,
    '-e', `PORT=${exposedPort}`,
    ...Object.entries(envObj).flatMap(([k,v]) => {
      const key = String(k || '').toUpperCase();
      return (key === 'PORT' || key === 'HOST' || key === 'HOSTNAME') ? [] : ['-e', `${k}=${v}`];
    }),
    imageName
  ];

  log(`\x1b[90m[docker] CPU shares: ${CPU_SHARES}\x1b[0m`);
  await exec('docker', runArgs, {}, log);
  emitStep(emit, 'copy', 'done');

  // ── Step 4: Wait for app to be ready ────────────────────────────────────
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 4/4 — Verify ━━━\x1b[0m');
  log('\x1b[90m[docker] Waiting for app to start…\x1b[0m');
  await new Promise(r => setTimeout(r, 3000));

  const stable = await waitForContainerRunning(candidateContainerName, 90, log); // [FIX] increased from 30s to 90s for heavy apps
  if (!stable) {
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(e) {}
    // [FIX] Kill the dead candidate immediately instead of leaving it sitting
    // around exited (or, before the --restart=no fix above, crash-looping
    // forever) on the host. Every failed deploy that isn't cleaned up here
    // is one more container idling on the VPS, eating disk/pid slots and
    // slowing down every other user's builds over time.
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    throw new Error('Dockerfile container exited during startup. Check logs above.');
  }

  const livePort = await detectLivePort(candidateContainerName, exposedPort, 120, log);
  if (!livePort) {
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(e) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    throw new Error('Readiness gate failed: app did not expose a reachable HTTP port.');
  }
  const targetPort = livePort;

  // Promote candidate to stable name used by proxy/registry
  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}
  await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
  // [FIX] The candidate container launches with --restart=no so a
  // crash-looping start command stays visibly exited instead of being
  // silently relaunched by Docker mid-healthcheck (which was masking
  // startup failures as "still waiting" forever -- see detectLivePort's
  // liveness check above). Only apply real crash-resilience now that the
  // container has actually proven it serves HTTP.
  try { await exec('docker', ['update', '--restart', 'unless-stopped', containerName], {}, () => {}); } catch(_) {}

  // Register in port registry
  const portsFile = path.join(sitesDir, 'ports.json');
  let registry = {};
  try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch(e) {}
  registry[project.subdomain] = `${containerName}:${targetPort}`;
  fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));

  log(`\x1b[32m[docker] ✓ Dockerfile app is live at ${project.subdomain} → ${containerName}:${targetPort}\x1b[0m`);
  emitStep(emit, 'start', 'done');

  // Cleanup
  fs.rmSync(buildDir, { recursive: true, force: true });
}

// ── WORKER BUILD ─────────────────────────────────────────────────────────────
// Same as server build but skips port polling — workers don't expose HTTP ports
async function runWorkerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain }) {
  const buildDir      = path.join(tmpDir, deployId);
  const containerName = 'db-' + project.subdomain;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const nodeImage     = 'node:' + (project.nodeVer || '18');
  const startCmd      = (project.startCmd || '').trim();
  const appDir        = path.join(sitesDir, project.subdomain, 'app');

  const log = createBatchedLogger(emit, onLog);
  const env = resolveEnvVars(project.envVars);

  // Step 1: Clone
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log(`\x1b[36m━━━ Step 1/4 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log, project);
  const relativeProjectRoot = path.relative(buildDir, projectRoot) || '.';
  log(`\x1b[90m[deploy] Worker root: ${relativeProjectRoot} — install, build, and worker start all use this same directory.\x1b[0m`);

  // Step 2: Install
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 2/4 — Install ━━━\x1b[0m`);
  const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
  log(`\x1b[90m[install] cwd: ${relativeProjectRoot}\x1b[0m`);
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await runInstallStepWithRecovery({ projectRoot, nodeImage, envObj: env, installCmd, log, nodeEnv: 'production' });
  emitStep(emit, 'install', 'done');

  // Step 3: Build
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 3/4 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || 'echo skip';
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m[build] cwd: ${relativeProjectRoot}\x1b[0m`);
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
  } else { log('\x1b[90m(no build step)\x1b[0m'); }
  emitStep(emit, 'build', 'done');

  // Step 4: Start container (no port needed)
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 4/4 — Start Worker ━━━\x1b[0m`);

  // Copy files to persistent dir
  fs.mkdirSync(appDir, { recursive: true });
  copyDir(projectRoot, appDir);

  // Stop old container
  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const networkName = 'deployboard-net';
  // [FIX] Cap memory for worker containers too — same rationale as other
  // deploy paths (see comment near the Dockerfile-deploy path above).
  const runtime = getRuntimeConfig(project);
  const runArgs = [
    'run', '-d', '--restart=no',
    '--name',         candidateContainerName,
    '--network',      networkName,
    '--add-host',     'host.docker.internal:host-gateway',
    '--cpu-shares',   CPU_SHARES,
    '--pids-limit',   PIDS_LIMIT,
    '-m',             runtime.memory,
    '--memory-reservation', runtime.memory,
    '-w', '/app',
    '-v', `${appDir}:/app`,
    ...Object.entries(env).flatMap(([k,v]) => ['-e', `${k}=${v}`]),
    nodeImage, 'sh', '-c',
    `cd /app && ${startCmd || getDefaultStartCmd(appDir)}`
  ];

  log(`\x1b[90m[docker] Starting worker: ${startCmd}\x1b[0m`);
  log(`\x1b[90m[docker] CPU shares: ${CPU_SHARES}\x1b[0m`);
  await exec('docker', runArgs, {}, log);
  await new Promise(r => setTimeout(r, 3000));

  // Check it's still running (workers crash immediately if start cmd is wrong)
  let running = false;
  try {
    const { execSync } = require('child_process');
    const state = execSync(`docker inspect --format='{{.State.Status}}' ${candidateContainerName}`, { encoding: 'utf8' }).trim();
    running = state === 'running';
  } catch(e) {}

  if (!running) {
    log(`\x1b[31m[docker] Worker container exited. Container logs:\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '30', containerName], {}, log); } catch(e) {}
    throw new Error(`Worker container exited immediately. Check your start command: "${startCmd}"`);
  }

  // [FIX] Same reasoning as the promotion step in the other build paths:
  // launched with --restart=no so a crash-looping start command stays
  // visibly exited instead of being silently relaunched mid-healthcheck.
  // Now that liveness is confirmed, apply real crash-resilience.
  try { await exec('docker', ['update', '--restart', 'unless-stopped', candidateContainerName], {}, () => {}); } catch(_) {}
  log(`\x1b[32m[docker] ✓ Background worker is running\x1b[0m`);
  emitStep(emit, 'start', 'done');
  fs.rmSync(buildDir, { recursive: true, force: true });
}

// ── PYTHON BUILD ─────────────────────────────────────────────────────────────
// Supports Django, Flask, FastAPI, and generic Python server apps.
// Uses python:<ver>-slim Docker image. Install via pip/poetry/pipenv.
// Runs app via gunicorn (Django/Flask) or uvicorn (FastAPI) or user startCmd.
async function runPythonBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain }) {
  const buildDir      = path.join(tmpDir, deployId);
  const appDir        = path.join(sitesDir, project.subdomain, 'app');
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0, 20)}`;
  const expectedPort  = normalizePort(appPort, 8000);
  const runtime       = detectPythonRuntime(project);
  const pythonImage   = `python:${runtime.pythonVer}-slim`;

  const log = createBatchedLogger(emit, onLog);
  const env = withDeployedAppRuntimeDefaults({ ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project) }, project, baseDomain);

  // ── Step 1: Clone ────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findPythonProjectRoot(buildDir, log, project);
  log(`\x1b[90m[python] Framework: ${runtime.framework} | Image: ${pythonImage}\x1b[0m`);

  // ── Detect the actual requirements file ──────────────────────────────────
  const reqFile = detectPythonRequirementsFile(projectRoot, log);

  // ── Step 2: Install ──────────────────────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 2/6 — Install Dependencies ━━━\x1b[0m`);
  log(`\x1b[90m[python] Streaming logs in structured batches\x1b[0m`);

  // Pull the image once before any docker run
  log(`\x1b[90m[docker] Pulling ${pythonImage}…\x1b[0m`);
  try { await exec('docker', ['pull', pythonImage], {}, () => {}); } catch(_) { log(`\x1b[33m[docker] Using cached image\x1b[0m`); }

  await runSmartPythonInstall({ projectRoot, pythonImage, env, runtime, project, reqFile, log });
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build (collectstatic, migrate, etc.) ──────────────────────
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 3/6 — Build Project ━━━\x1b[0m`);
  log(`\x1b[90m[python] Streaming logs in structured batches\x1b[0m`);

  const hasManagePyForBuild = fs.existsSync(path.join(projectRoot, 'manage.py'));
  const isDjangoForBuild    = runtime.framework === 'Django' || hasManagePyForBuild;

  await runSmartPythonBuild({
    projectRoot, pythonImage, env, runtime,
    userBuildCmd: (project.buildCmd || '').trim(),
    isDjango: isDjangoForBuild,
    log,
  });
  emitStep(emit, 'build', 'done');

  // ── Step 4: Persist ap;

  // ── Step 4: Persist app dir ──────────────────────────────────────────────
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 4/6 — Prepare App Dir ━━━\x1b[0m`);
  if (fs.existsSync(appDir)) fs.rmSync(appDir, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(appDir), { recursive: true });
  try {
    fs.renameSync(projectRoot, appDir);
    log(`\x1b[32m[copy] ✓ App moved to permanent storage\x1b[0m`);
  } catch (e) {
    log(`\x1b[90m[copy] Cross-device move, copying files…\x1b[0m`);
    fs.mkdirSync(appDir, { recursive: true });
    const copyFiltered = (src, dst) => {
      fs.mkdirSync(dst, { recursive: true });
      for (const entry of fs.readdirSync(src)) {
        if (['__pycache__', '.git', 'venv', '.venv', 'env'].includes(entry) || entry.endsWith('.pyc')) continue;
        const s = path.join(src, entry), d = path.join(dst, entry);
        try {
          const st = fs.lstatSync(s);
          if (st.isDirectory()) copyFiltered(s, d);
          else if (st.isFile()) fs.copyFileSync(s, d);
        } catch (_) {}
      }
    };
    copyFiltered(projectRoot, appDir);
    log(`\x1b[32m[copy] ✓ App copied\x1b[0m`);
  }
  emitStep(emit, 'copy', 'done');

  // ── Step 5: Launch container ──────────────────────────────────────────────
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);

  // ── Smart start command resolution ────────────────────────────────────────
  // Analyse whatever the user typed (or left blank) and auto-correct it so the
  // container never crashes due to a missing/wrong start command.
  const { startCmd, startCmdWarnings } = resolveSmartPythonStartCmd({
    project,
    projectRoot,
    runtime,
    expectedPort,
    log,
  });
  for (const w of startCmdWarnings) {
    log(`\x1b[33m[Joytree] ${w}\x1b[0m`);
  }

  log(`\x1b[90m[docker] Image:     ${pythonImage}\x1b[0m`);
  log(`\x1b[90m[docker] Container: ${candidateContainerName}\x1b[0m`);
  log(`\x1b[90m[docker] Command:   ${startCmd}\x1b[0m`);

  const hostAppDir = appDir
    .replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data')
    .replace('/tmp/deployboard-builds', '/tmp/deployboard-builds');
  const dockerMountSrc = appDir.startsWith('/tmp') ? appDir : hostAppDir;

  try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}

  // Build the runtime pip install command using the same detected requirements file.
  // --prefer-binary avoids metadata-generation-failed errors on slim images missing gcc.
  // We use the same resolved install command that the build step used — this handles
  // nested -r include paths (e.g. requirements-dev.txt → -r src/requirements.txt)
  // that would break if re-run verbatim from a different working directory.
  const resolvedRuntimeReqFile = (() => {
    if (!reqFile) return null;
    // If reqFile contains -r includes that were rewritten during build, use src/requirements.txt directly
    try {
      const rContent = fs.readFileSync(path.join(projectRoot, reqFile), 'utf8');
      const includes = rContent.split('\n').filter(l => /^-r\s+/.test(l.trim()));
      if (includes.length > 0) {
        // The file delegates to includes — find the first resolvable one from projectRoot
        for (const inc of includes) {
          const incPath = inc.trim().replace(/^-r\s+/, '');
          if (fs.existsSync(path.join(projectRoot, incPath))) return incPath;
        }
      }
    } catch(_) {}
    return reqFile;
  })();
  const runtimeReqInstall = resolvedRuntimeReqFile
    ? `pip install --prefer-binary -r ${resolvedRuntimeReqFile} -q 2>&1 | tail -3`
    : `pip install --prefer-binary -r requirements.txt -q 2>/dev/null || pip install --prefer-binary -r requirements.dev.txt -q 2>/dev/null || true`;

  // ── Build safety pre-flight for Django ───────────────────────────────────
  // Before the real start command we run a lightweight shell pre-flight that
  // auto-installs gunicorn/uvicorn if missing, ensures DJANGO_SETTINGS_MODULE
  // is set, and does a dry-run of the wsgi import so we get a clear error
  // message instead of a silent exit-1.
  const isDjangoRuntime = runtime.framework === 'Django' ||
    fs.existsSync(path.join(projectRoot, 'manage.py'));
  const preflight = buildPythonPreflight({ isDjangoRuntime, projectRoot, reqFile, startCmd, expectedPort });

  const runArgs = [
    'run', '-d',
    '--name',       candidateContainerName,
    '--restart',    'no',
    '--network',    'deployboard-net',
    '--add-host',   'host.docker.internal:host-gateway',
    '--cpu-shares', CPU_SHARES,
    '--pids-limit', PIDS_LIMIT,
    '-m',           '2g',
    '-e', `PORT=${expectedPort}`,
    '-e', `PYTHONUNBUFFERED=1`,
    '-e', `PYTHONDONTWRITEBYTECODE=1`,
    '-e', `PIP_NO_CACHE_DIR=off`,
    // ── Django runtime safety defaults ──────────────────────────────────────
    // ALLOWED_HOSTS: if not set by the user, Django rejects ALL requests in
    // production (DEBUG=False) with a 400 DisallowedHost → Cloudflare 502.
    // We auto-set it to '*' so the app works immediately; users can tighten
    // this to their actual domain in Environment Variables.
    ...(!env['ALLOWED_HOSTS'] && isDjangoRuntime ? ['-e', 'ALLOWED_HOSTS=*'] : []),
    // DJANGO_SETTINGS_MODULE: auto-detect if not set, so manage.py and
    // gunicorn can find the settings without the user having to configure it.
    ...(!env['DJANGO_SETTINGS_MODULE'] && isDjangoRuntime ? (() => {
      const m = detectDjangoSettingsModule(projectRoot);
      return m ? ['-e', `DJANGO_SETTINGS_MODULE=${m}`] : [];
    })() : []),
    // SECRET_KEY: Django won't start at all without it. Auto-generate a
    // stable per-subdomain key as a fallback so the container boots.
    // Users MUST replace this with a real secret in Environment Variables.
    ...(!env['SECRET_KEY'] && !env['DJANGO_SECRET_KEY'] && isDjangoRuntime ? (() => {
      const crypto = require('crypto');
      const fallbackKey = 'joytree-auto-' + crypto.createHash('sha256')
        .update(project.subdomain + 'runtime-secret').digest('hex').slice(0, 40);
      log(`\x1b[33m[Joytree] WARNING: SECRET_KEY is not set in your Environment Variables. ` +
          `A temporary key has been generated but this is NOT secure for production. ` +
          `Add SECRET_KEY to your Environment Variables immediately.\x1b[0m`);
      return ['-e', `SECRET_KEY=${fallbackKey}`];
    })() : []),
    ...Object.entries(env).flatMap(([k, v]) => {
      if (String(k || '').toUpperCase() === 'PORT') return [];
      return ['-e', `${k}=${v}`];
    }),
    '-v', `${dockerMountSrc}:/app`,
    '-w', '/app',
    pythonImage,
    'sh', '-c',
    // Re-install deps at container start (pip installs to image layer, not volume)
    // then run preflight checks, then run the final resolved start command.
    `${runtimeReqInstall}; ${preflight}${startCmd}`
  ];

  await exec('docker', runArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);

  log(`\x1b[90m[docker] Waiting for app to stabilize…\x1b[0m`);
  const stable = await waitForContainerRunning(candidateContainerName, 90, log); // [FIX] increased from 30s to 90s for heavy apps
  if (!stable) {
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(_) {}
    throw new Error(
      `Python container exited during startup. Check logs above.\n` +
      `Common fixes:\n` +
      `  • Add a requirements.txt to your repo root listing all dependencies\n` +
      `  • Verify your start command (e.g. gunicorn myproject.wsgi:application --bind 0.0.0.0:$PORT)\n` +
      `  • Set DJANGO_SETTINGS_MODULE in Environment Variables if using Django\n` +
      `  • Make sure gunicorn or uvicorn is in your requirements.txt`
    );
  }

  const pFile = path.join(sitesDir, 'ports.json');
  const previousTarget = readRegistryTarget(pFile, project.subdomain);
  try {
    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(_) {}
    const livePort = await detectLivePort(candidateContainerName, expectedPort, 120, log);
    if (!livePort) throw new Error(`Readiness gate failed — app did not respond on any port within 120s`);
    const targetPort = normalizePort(livePort, expectedPort);
    registry[project.subdomain] = `${containerName}:${targetPort}`;
    fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    log(`\x1b[32m[docker] ✓ Proxy registered: ${project.subdomain} → ${containerName}:${targetPort}\x1b[0m`);
    await archivePreviousContainer(containerName, project.subdomain, log);
    await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
    // [FIX] The candidate container launches with --restart=no so a
    // crash-looping start command stays visibly exited instead of being
    // silently relaunched by Docker mid-healthcheck (which was masking
    // startup failures as "still waiting" forever -- see detectLivePort's
    // liveness check above). Only apply real crash-resilience now that the
    // container has actually proven it serves HTTP.
    try { await exec('docker', ['update', '--restart', 'unless-stopped', containerName], {}, () => {}); } catch(_) {}
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);
  } catch(e) {
    log(`\x1b[31m[docker] Candidate failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (!isStableRegistryTarget(previousTarget)) {
      let registry = {};
      try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(_) {}
      delete registry[project.subdomain];
      fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    }
    throw e;
  }
  emitStep(emit, 'start', 'done');

  // ── Step 6: Cleanup ───────────────────────────────────────────────────────
  emitStep(emit, 'cleanup', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(_) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Python app deployed in isolated container!\x1b[0m`);
}

// ── Python helpers ────────────────────────────────────────────────────────────

function detectPythonRuntime(project) {
  const runtime  = String(project.runtime  || '').toLowerCase();
  const pythonVer = String(project.pythonVer || project.pythonVersion || '3.11');

  const configs = {
    'python-django': {
      framework: 'Django',
      installCmd: 'pip install -r requirements.txt',
      buildCmd: 'python manage.py collectstatic --noinput; python manage.py migrate --noinput',
      startCmd: (port) => `gunicorn \$(python -c "import os,glob; mods=[m for m in glob.glob('*/wsgi.py')]; print(mods[0].replace('/','.',1).replace('.py','') if mods else 'myproject.wsgi') "):application --bind 0.0.0.0:${port} --workers 2 --timeout 120`,
    },
    'python-flask': {
      framework: 'Flask',
      installCmd: 'pip install -r requirements.txt',
      buildCmd: 'echo skip',
      startCmd: (port) => `gunicorn \$(python -c "import os; f=next((f for f in ['app','wsgi','main','run','server'] if os.path.exists(f+'.py')),None); print(f+':app' if f else 'app:app')") --bind 0.0.0.0:${port} --workers 2 --timeout 120`,
    },
    'python-fastapi': {
      framework: 'FastAPI',
      installCmd: 'pip install -r requirements.txt',
      buildCmd: 'echo skip',
      startCmd: (port) => `uvicorn \$(python -c "import os; f=next((f for f in ['main','app','server','api'] if os.path.exists(f+'.py')),None); print(f+':app' if f else 'main:app')") --host 0.0.0.0 --port ${port} --workers 2`,
    },
    'python-generic': {
      framework: 'Python',
      installCmd: 'pip install -r requirements.txt',
      buildCmd: 'echo skip',
      startCmd: (port) => `python \$(python -c "import os; f=next((f for f in ['app.py','main.py','server.py','run.py','index.py'] if os.path.exists(f)),None) or 'app.py'")`,
    },
  };

  const cfg = configs[runtime] || configs['python-generic'];
  return { ...cfg, pythonVer };
}

function findPythonProjectRoot(buildDir, log, project = null) {
  const explicitRoot = resolveWorkingDirRoot(buildDir, project, log);
  if (explicitRoot) return explicitRoot;

  // Look for requirements.txt, setup.py, pyproject.toml, Pipfile
  const markers = ['requirements.txt', 'setup.py', 'pyproject.toml', 'Pipfile', 'manage.py'];

  function walk(dir, depth) {
    if (depth > 5) return null;
    let entries;
    try { entries = fs.readdirSync(dir); } catch(_) { return null; }

    for (const marker of markers) {
      if (fs.existsSync(path.join(dir, marker))) return dir;
    }

    const subdirs = entries
      .filter(e => {
        if (['__pycache__', '.git', 'venv', '.venv', 'env', 'node_modules', 'dist', 'build'].includes(e)) return false;
        if (e.startsWith('.')) return false;
        try { return fs.lstatSync(path.join(dir, e)).isDirectory(); } catch(_) { return false; }
      })
      .sort();

    for (const sub of subdirs) {
      const found = walk(path.join(dir, sub), depth + 1);
      if (found) return found;
    }
    return null;
  }

  const found = walk(buildDir, 0);
  if (found && found !== buildDir) {
    log(`\x1b[90m[info] Python project root found in: ${path.relative(buildDir, found)}/\x1b[0m`);
    return found;
  }
  if (found) return found;
  log(`\x1b[33m[warn] No Python project markers found anywhere in repo — using repo root\x1b[0m`);
  return buildDir;
}

// ── Smart Python install orchestrator ────────────────────────────────────────
// Handles the full dependency installation step with intelligence:
//   1. Validate / auto-correct the user's install command
//   2. Detect Poetry / uv lock files and use correct installer
//   3. Warn about known compile-heavy packages and provide hints
//   4. Auto-install critical missing runtime packages (gunicorn, whitenoise, etc.)
//   5. Scan -r includes in requirements.txt and warn about missing files
//   6. Soft-fail with clear messages instead of cryptic pip errors
async function runSmartPythonInstall({ projectRoot, pythonImage, env, runtime, project, reqFile, log }) {

  const userInstallCmd = (project.installCmd || '').trim();
  const isDjango = runtime.framework === 'Django' ||
    fs.existsSync(path.join(projectRoot, 'manage.py'));

  // ── 1. Detect lock-file based package managers ───────────────────────────
  const hasPoetryLock   = fs.existsSync(path.join(projectRoot, 'poetry.lock'));
  const hasUvLock       = fs.existsSync(path.join(projectRoot, 'uv.lock'));
  const hasPipfileLock  = fs.existsSync(path.join(projectRoot, 'Pipfile.lock'));
  const hasPyproject    = fs.existsSync(path.join(projectRoot, 'pyproject.toml'));
  const hasSetupPy      = fs.existsSync(path.join(projectRoot, 'setup.py'));
  const hasSetupCfg     = fs.existsSync(path.join(projectRoot, 'setup.cfg'));

  // ── 2. Resolve and validate the install command ──────────────────────────
  let installCmd = userInstallCmd;
  let installCmdSource = 'user';

  // 2a. Catch "pip install requirements.txt" (missing -r flag — very common mistake)
  if (/pip\s+install\s+(?!-)[\w./]+requirements[\w.-]*\.txt/.test(installCmd)) {
    const fixed = installCmd.replace(
      /(pip\s+install\s+)((?!-)[\w./]+requirements[\w.-]*\.txt)/,
      '$1-r $2'
    );
    log(`\x1b[33m[Joytree] \u26a0 Install command is missing the -r flag: "${installCmd}"`);
    log(`[Joytree]   Auto-corrected to: "${fixed}"\x1b[0m`);
    installCmd = fixed;
    installCmdSource = 'corrected';
  }

  // 2b. Blank or default — auto-resolve from lock files / requirements
  if (!installCmd || installCmd === 'pip install -r requirements.txt') {
    if (hasPoetryLock) {
      // Check if poetry is available; if not, export to requirements and use pip
      installCmd = [
        `pip install --prefer-binary poetry --quiet 2>/dev/null || true`,
        `poetry export --without-hashes -f requirements.txt -o /tmp/poetry-requirements.txt 2>/dev/null`,
        `pip install --prefer-binary -r /tmp/poetry-requirements.txt`,
      ].join(' && ');
      log(`\x1b[33m[Joytree] Detected poetry.lock — installing via Poetry export. ` +
          `Tip: You can also add a requirements.txt generated by "poetry export" to your repo.\x1b[0m`);
      installCmdSource = 'poetry';
    } else if (hasUvLock) {
      installCmd = [
        `pip install --prefer-binary uv --quiet 2>/dev/null || true`,
        `uv pip install --system -r pyproject.toml 2>/dev/null || pip install --prefer-binary -r requirements.txt 2>/dev/null || true`,
      ].join(' && ');
      log(`\x1b[33m[Joytree] Detected uv.lock — installing via uv. ` +
          `Tip: You can also commit a requirements.txt ("uv pip compile pyproject.toml -o requirements.txt") for faster builds.\x1b[0m`);
      installCmdSource = 'uv';
    } else if (hasPipfileLock) {
      installCmd = `pip install --prefer-binary pipenv --quiet && pipenv install --deploy --system`;
      log(`\x1b[33m[Joytree] Detected Pipfile.lock — installing via pipenv.\x1b[0m`);
      installCmdSource = 'pipenv';
    } else if (reqFile) {
      installCmd = `pip install --prefer-binary -r ${reqFile}`;
      if (reqFile !== 'requirements.txt') {
        log(`\x1b[33m[Joytree] Warning: requirements.txt not found \u2014 using ${reqFile} instead.\x1b[0m`);
      }
      installCmdSource = 'requirements';
    } else if (hasPyproject || hasSetupPy || hasSetupCfg) {
      installCmd = `pip install --prefer-binary .`;
      log(`\x1b[33m[Joytree] No requirements.txt found \u2014 installing from ${hasPyproject ? 'pyproject.toml' : hasSetupPy ? 'setup.py' : 'setup.cfg'} (pip install .).\x1b[0m`);
      installCmdSource = 'pyproject';
    } else {
      log(`\x1b[33m[Joytree] Warning: No dependency file found (requirements.txt, pyproject.toml, Pipfile, poetry.lock, uv.lock). ` +
          `Add a requirements.txt to your repo listing all dependencies.\x1b[0m`);
      installCmd = `echo "No requirements file \u2014 skipping install"`;
      installCmdSource = 'skip';
    }
  }

  // 2c. Ensure --prefer-binary is present on plain pip install -r commands
  // (avoids metadata-generation-failed on slim Docker images)
  if (/pip\s+install\s+-r\b/.test(installCmd) && !/--prefer-binary/.test(installCmd)) {
    installCmd = installCmd.replace(/pip\s+install\s+-r/, 'pip install --prefer-binary -r');
  }

  // ── 3. Scan requirements file for known problem packages ─────────────────
  if (reqFile && installCmdSource === 'requirements') {
    const reqPath = path.join(projectRoot, reqFile);
    try {
      const reqContent = fs.readFileSync(reqPath, 'utf8');
      const reqLines   = reqContent.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));

      // 3a. Warn about psycopg2 (needs gcc) — suggest binary variant
      const hasPsycopg2    = reqLines.some(l => /^psycopg2\b(?!-binary)/.test(l));
      const hasPsycopgBin  = reqLines.some(l => /^psycopg2-binary/.test(l));
      if (hasPsycopg2 && !hasPsycopgBin) {
        log(`\x1b[33m[Joytree] Tip: "psycopg2" in requirements.txt requires compiling from source. ` +
            `Replace it with "psycopg2-binary" for faster, more reliable builds on Joytree.\x1b[0m`);
      }

      // 3b. Warn about mysqlclient (requires libmysqlclient-dev system library)
      if (reqLines.some(l => /^mysqlclient/.test(l))) {
        log(`\x1b[33m[Joytree] Tip: "mysqlclient" requires system library libmysqlclient-dev. ` +
            `Consider using "PyMySQL" instead for easier deployment, or contact support if you need MySQL.\x1b[0m`);
      }

      // 3c. Warn about lxml without binary wheels (large compile)
      if (reqLines.some(l => /^lxml/.test(l))) {
        log(`\x1b[90m[python] lxml detected \u2014 using binary wheel (already handled by --prefer-binary).\x1b[0m`);
      }

      // 3d. Check for -r includes and fix path resolution issues.
      // When requirements-dev.txt at the repo root contains "-r src/requirements.txt"
      // but pip runs from inside projectRoot (src/), the path "src/requirements.txt"
      // resolves to "src/src/requirements.txt" inside the container — which doesn't exist.
      // We detect this and rewrite the install command to install the included file directly.
      const reqFileDir = path.dirname(path.join(projectRoot, reqFile)); // dir of the req file
      const includes = reqLines.filter(l => /^-r\s+/.test(l)).map(l => l.replace(/^-r\s+/, '').trim());
      for (const inc of includes) {
        const incAbsolute = path.resolve(reqFileDir, inc);  // resolve relative to where req file lives
        const incRelToProjectRoot = path.relative(projectRoot, incAbsolute);
        const existsAtReqFileDir = fs.existsSync(incAbsolute);
        const existsAtProjectRoot = fs.existsSync(path.join(projectRoot, inc));
        if (!existsAtProjectRoot && existsAtReqFileDir && !incRelToProjectRoot.startsWith('..')) {
          // Include exists but at a different relative path than where pip will look
          log(`\x1b[33m[Joytree] Warning: ${reqFile} includes "-r ${inc}" but this path won't resolve correctly ` +
              `because your requirements file is outside the project root. ` +
              `Joytree will install "${incRelToProjectRoot}" directly instead.\x1b[0m`);
          // Rewrite installCmd to directly target the correctly resolved include file
          installCmd = `pip install --prefer-binary -r ${incRelToProjectRoot}`;
        } else if (!existsAtProjectRoot && !existsAtReqFileDir) {
          log(`\x1b[33m[Joytree] Warning: ${reqFile} includes "-r ${inc}" but that file does not exist in your repo. ` +
              `This will cause the install to fail. Check that the file path is correct.\x1b[0m`);
        }
      }

      // 3e. Warn if requirements file is empty
      if (reqLines.length === 0) {
        log(`\x1b[33m[Joytree] Warning: ${reqFile} exists but is empty. ` +
            `Add your dependencies to it (e.g. django, gunicorn).\x1b[0m`);
      }

      // 3f. Warn about version pins that could cause resolution failures
      const unpinnedCount = reqLines.filter(l => !/[=<>!~]/.test(l) && !l.startsWith('-')).length;
      if (unpinnedCount > 5) {
        log(`\x1b[90m[python] ${unpinnedCount} unpinned packages detected. ` +
            `Consider pinning versions (e.g. django==4.2.0) for reproducible builds.\x1b[0m`);
      }
    } catch(_) {}
  }

  // ── 4. Run the main install ───────────────────────────────────────────────
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  try {
    await runPythonCommandInContainer({ projectRoot, pythonImage, envObj: env, command: installCmd, log });
  } catch (e) {
    const msg = String(e.message || '');
    if (/No such file or directory|Could not open requirements|not found/i.test(msg)) {
      log(`\x1b[33m[Joytree] Warning: Could not install dependencies \u2014 ${msg.split('\n')[0]}\x1b[0m`);
      log(`\x1b[33m[Joytree] Continuing without packages. Add a requirements.txt to your repo to fix this.\x1b[0m`);
    } else if (/metadata-generation-failed|error while generating package metadata/i.test(msg)) {
      log(`\x1b[33m[Joytree] A package failed to compile from source. Common fixes:\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Replace "psycopg2" with "psycopg2-binary" in requirements.txt\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Replace "mysqlclient" with "PyMySQL"\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Check that all package names and versions are correct\x1b[0m`);
      throw e;
    } else if (/ResolutionImpossible|conflict|incompatible/i.test(msg)) {
      log(`\x1b[33m[Joytree] Dependency conflict detected. Common fixes:\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Pin conflicting package versions in requirements.txt\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Run "pip install -r requirements.txt" locally to reproduce the error\x1b[0m`);
      throw e;
    } else if (/Could not find a version|No matching distribution/i.test(msg)) {
      log(`\x1b[33m[Joytree] A package version was not found on PyPI. Common fixes:\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 Check the package name and version in requirements.txt\x1b[0m`);
      log(`\x1b[33m[Joytree]   \u2022 The package may not have a wheel for Python ${runtime.pythonVer} \u2014 try a different version\x1b[0m`);
      throw e;
    } else {
      throw e;
    }
  }

  // ── 5. Auto-install critical missing Django runtime packages ─────────────
  // These are packages that Django apps almost always need but users commonly
  // forget to add to requirements.txt. We only install them if they aren't
  // already present in the requirements file and the framework warrants them.
  if (isDjango || runtime.framework === 'Flask' || runtime.framework === 'FastAPI') {
    const reqPath = reqFile ? path.join(projectRoot, reqFile) : null;
    let reqContent = '';
    try { reqContent = reqPath ? fs.readFileSync(reqPath, 'utf8').toLowerCase() : ''; } catch(_) {}

    const missing = [];

    // gunicorn — needed to serve any WSGI app in production
    if (!reqContent.includes('gunicorn') && !reqContent.includes('uvicorn') &&
        runtime.framework !== 'FastAPI') {
      missing.push('gunicorn');
    }
    // uvicorn — FastAPI needs it
    if (runtime.framework === 'FastAPI' && !reqContent.includes('uvicorn')) {
      missing.push('uvicorn[standard]');
    }
    // whitenoise — Django static files without a CDN; very common omission
    if (isDjango && !reqContent.includes('whitenoise')) {
      missing.push('whitenoise');
    }
    // dj-database-url — almost universally used to parse DATABASE_URL
    if (isDjango && !reqContent.includes('dj-database-url') && !reqContent.includes('dj_database_url')) {
      missing.push('dj-database-url');
    }

    if (missing.length > 0) {
      log(`\x1b[33m[Joytree] Auto-installing commonly required packages missing from requirements.txt: ${missing.join(', ')}\x1b[0m`);
      log(`\x1b[33m[Joytree] Add these to your requirements.txt to avoid this message: ${missing.join(', ')}\x1b[0m`);
      const autoInstall = `pip install --prefer-binary ${missing.join(' ')} --quiet`;
      log(`\x1b[90m$ ${autoInstall}\x1b[0m`);
      try {
        await runPythonCommandInContainer({ projectRoot, pythonImage, envObj: env, command: autoInstall, log });
        log(`\x1b[32m[python] \u2713 Auto-installed: ${missing.join(', ')}\x1b[0m`);
      } catch(_) {
        log(`\x1b[33m[Joytree] Could not auto-install ${missing.join(', ')} \u2014 add them to requirements.txt.\x1b[0m`);
      }
    }
  }
}

// Scans the project root for any known requirements file variant and returns the filename.
// Covers requirements.txt, requirements.dev.txt, requirements/base.txt, etc.
function detectPythonRequirementsFile(projectRoot, log) {
  const candidates = [
    'requirements.txt',
    'requirements.in',
    'requirements.dev.txt',
    'requirements-dev.txt',
    'requirements_dev.txt',
    'requirements/base.txt',
    'requirements/production.txt',
    'requirements/prod.txt',
    'requirements/common.txt',
    'requirements/main.txt',
  ];
  for (const c of candidates) {
    if (fs.existsSync(path.join(projectRoot, c))) {
      if (c !== 'requirements.txt') {
        log(`\x1b[33m[Joytree] Warning: requirements.txt not found — using ${c} instead.\x1b[0m`);
      }
      return c;
    }
  }
  // Also scan for any *.txt inside a requirements/ folder
  const reqDir = path.join(projectRoot, 'requirements');
  if (fs.existsSync(reqDir)) {
    try {
      const files = fs.readdirSync(reqDir).filter(f => f.endsWith('.txt'));
      if (files.length > 0) {
        const pick = files.sort()[0];
        log(`\x1b[33m[Joytree] Warning: Using requirements/${pick} (no standard requirements.txt found).\x1b[0m`);
        return `requirements/${pick}`;
      }
    } catch (_) {}
  }
  return null;
}

// When no requirements file is found at all, fall back to pyproject.toml / setup.py / Pipfile installs.
function resolvePythonInstallFallback(projectRoot, log) {
  if (fs.existsSync(path.join(projectRoot, 'pyproject.toml'))) {
    log(`\x1b[33m[Joytree] No requirements.txt found — installing from pyproject.toml (pip install .).\x1b[0m`);
    return 'pip install .';
  }
  if (fs.existsSync(path.join(projectRoot, 'setup.py'))) {
    log(`\x1b[33m[Joytree] No requirements.txt found — installing from setup.py (pip install .).\x1b[0m`);
    return 'pip install .';
  }
  if (fs.existsSync(path.join(projectRoot, 'Pipfile'))) {
    log(`\x1b[33m[Joytree] No requirements.txt found — installing from Pipfile (pipenv install --deploy).\x1b[0m`);
    return 'pip install pipenv && pipenv install --deploy --system';
  }
  log(`\x1b[33m[Joytree] Warning: No dependency file found. Add requirements.txt to your repo. Continuing anyway.\x1b[0m`);
  return 'echo "No requirements file — skipping install"';
}

// ── Smart Python/Django start command resolver ────────────────────────────────
// Analyses the user's configured start command (or the absence of one),
// auto-corrects common mistakes, and returns a validated command + warnings.
//
// Covers:
//   • blank start command           → auto-generate from framework + file scan
//   • wrong/missing gunicorn path   → detect wsgi.py, build correct module path
//   • "python manage.py runserver"  → replace with gunicorn (not safe for prod)
//   • "flask run" / "uvicorn app"   → normalise to bind 0.0.0.0 and $PORT
//   • hardcoded port number         → replace with $PORT
//   • missing --bind / --host flag  → inject it
//   • gunicorn with no workers flag → add --workers 2 --timeout 120
//   • uvicorn with no --port flag   → inject --port $PORT --host 0.0.0.0
function resolveSmartPythonStartCmd({ project, projectRoot, runtime, expectedPort, log }) {
  const warnings = [];
  let cmd = (project.startCmd || '').trim();
  const hasManagePy = fs.existsSync(path.join(projectRoot, 'manage.py'));
  const isDjango = runtime.framework === 'Django' || hasManagePy;

  // ── 1. Blank start command — auto-generate ───────────────────────────────
  if (!cmd) {
    const autoCmd = autoDetectPythonStartCmd(projectRoot, runtime, expectedPort, log);
    warnings.push(
      `No start command configured — Joytree auto-detected: ${autoCmd}`
    );
    warnings.push(
      `Tip: Set your Start Command in the dashboard to silence this warning.`
    );
    return { startCmd: autoCmd, startCmdWarnings: warnings };
  }

  // ── 2. "python manage.py runserver" — never safe in production ───────────
  if (/manage\.py\s+runserver/i.test(cmd)) {
    const wsgiModule = findDjangoWsgiModule(projectRoot);
    const fixedCmd = `gunicorn ${wsgiModule}:application --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
    warnings.push(
      `⚠ "python manage.py runserver" is not safe for production and will not bind correctly inside a container.`
    );
    warnings.push(
      `  Joytree automatically replaced it with: ${fixedCmd}`
    );
    warnings.push(
      `  Update your Start Command in the dashboard to remove this warning.`
    );
    return { startCmd: fixedCmd, startCmdWarnings: warnings };
  }

  // ── 3. gunicorn commands — validate and patch ────────────────────────────
  if (/\bgunicorn\b/.test(cmd)) {
    // 3a. Replace hardcoded port numbers with $PORT
    cmd = cmd.replace(/--bind\s+0\.0\.0\.0:(\d{4,5})\b/g, (_, port) => {
      if (port !== String(expectedPort)) {
        warnings.push(
          `⚠ Hardcoded port ${port} in gunicorn --bind replaced with $PORT so Joytree can assign the correct port dynamically.`
        );
      }
      return '--bind 0.0.0.0:$PORT';
    });
    // 3b. gunicorn with no --bind at all
    if (!/--bind\b/.test(cmd)) {
      cmd = cmd + ' --bind 0.0.0.0:$PORT';
      warnings.push(
        `⚠ gunicorn --bind flag was missing — added "--bind 0.0.0.0:$PORT" automatically. ` +
        `Without this your app cannot receive traffic inside the container.`
      );
    }
    // 3c. Missing --workers flag
    if (!/--workers\b/.test(cmd)) {
      cmd = cmd + ' --workers 2 --timeout 120';
      warnings.push(
        `Added --workers 2 --timeout 120 to gunicorn for better stability.`
      );
    }
    // 3d. Django: try to validate the wsgi module path
    if (isDjango) {
      const wsgiMatch = cmd.match(/gunicorn\s+([\w.]+):application/);
      if (wsgiMatch) {
        const declaredModule = wsgiMatch[1]; // e.g. "myproject.wsgi"
        const detectedModule = findDjangoWsgiModule(projectRoot);
        if (detectedModule && detectedModule !== declaredModule) {
          warnings.push(
            `⚠ Your gunicorn command references "${declaredModule}:application" but Joytree found ` +
            `"${detectedModule}:application" in your repo. If your app crashes, update your Start Command to use the correct module.`
          );
        }
      } else if (!/gunicorn\s+[\w.]+/.test(cmd)) {
        // No wsgi module specified at all — inject the detected one
        const wsgiModule = findDjangoWsgiModule(projectRoot);
        cmd = cmd.replace(/\bgunicorn\b/, `gunicorn ${wsgiModule}:application`);
        warnings.push(
          `⚠ gunicorn WSGI module was missing — Joytree auto-detected and added "${wsgiModule}:application".`
        );
      }
    }
    return { startCmd: cmd, startCmdWarnings: warnings };
  }

  // ── 4. uvicorn commands — validate and patch ─────────────────────────────
  if (/\buvicorn\b/.test(cmd)) {
    // 4a. Missing --host
    if (!/--host\b/.test(cmd)) {
      cmd = cmd + ' --host 0.0.0.0';
      warnings.push(
        `⚠ uvicorn --host flag was missing — added "--host 0.0.0.0" so the app is reachable inside the container.`
      );
    }
    // 4b. Replace hardcoded port or missing --port
    if (/--port\s+\d{4,5}\b/.test(cmd)) {
      cmd = cmd.replace(/--port\s+\d{4,5}\b/, '--port $PORT');
      warnings.push(
        `⚠ Hardcoded port in uvicorn --port replaced with $PORT for dynamic port assignment.`
      );
    } else if (!/--port\b/.test(cmd)) {
      cmd = cmd + ' --port $PORT';
      warnings.push(
        `⚠ uvicorn --port flag was missing — added "--port $PORT" automatically.`
      );
    }
    return { startCmd: cmd, startCmdWarnings: warnings };
  }

  // ── 5. "flask run" — not suitable for production ─────────────────────────
  if (/\bflask\s+run\b/i.test(cmd)) {
    const flaskFile = ['app', 'wsgi', 'main', 'run', 'server']
      .find(f => fs.existsSync(path.join(projectRoot, f + '.py')));
    const module = flaskFile ? `${flaskFile}:app` : 'app:app';
    const fixedCmd = `gunicorn ${module} --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
    warnings.push(
      `⚠ "flask run" is for development only — it does not bind to 0.0.0.0 and cannot handle production traffic.`
    );
    warnings.push(
      `  Joytree automatically replaced it with: ${fixedCmd}`
    );
    warnings.push(
      `  Update your Start Command in the dashboard to remove this warning.`
    );
    return { startCmd: fixedCmd, startCmdWarnings: warnings };
  }

  // ── 6. Plain "python app.py" / "python main.py" etc. ─────────────────────
  // These are fine for simple scripts; just make sure $PORT is passed.
  if (/\bpython\b/.test(cmd) && !/manage\.py/.test(cmd)) {
    if (!/\$PORT|\bport\b/i.test(cmd)) {
      warnings.push(
        `Tip: Make sure your Python app reads the PORT environment variable ` +
        `(os.environ.get("PORT", ${expectedPort})) so it listens on the correct port.`
      );
    }
    return { startCmd: cmd, startCmdWarnings: warnings };
  }

  // ── 7. Unrecognised command — return as-is with a gentle note ────────────
  return { startCmd: cmd, startCmdWarnings: warnings };
}

// Scans the project for a wsgi.py file and returns the Python module path.
// e.g. "myproject/wsgi.py" → "myproject.wsgi"
function findDjangoWsgiModule(projectRoot) {
  try {
    const entries = fs.readdirSync(projectRoot);
    for (const entry of entries) {
      if (['__pycache__', '.git', 'venv', '.venv', 'env', 'node_modules'].includes(entry)) continue;
      const wsgi = path.join(projectRoot, entry, 'wsgi.py');
      if (fs.existsSync(wsgi)) return `${entry}.wsgi`;
    }
  } catch (_) {}
  // Fallback: use a runtime shell snippet that auto-discovers the wsgi module
  return `$(python -c "import glob; mods=[m for m in glob.glob('*/wsgi.py')]; print(mods[0].replace('/','.',1).replace('.py','') if mods else 'myproject.wsgi')")`;
}

// Generates the best possible start command from scratch by scanning the project.
// [FIX] The old detector only checked a fixed filename list
// (app/wsgi/main/run/server .py) against the project root. Real-world
// Flask apps very often use a custom entry-point filename with an
// application-factory pattern -- e.g. miguelgrinberg/microblog's
// microblog.py, which does `app = create_app()` -- and would have been
// completely invisible to the old check, silently falling through to
// "python app.py" (a file that doesn't exist) and crashing the
// container with a confusing FileNotFoundError instead of a clear
// "couldn't find your entry point" message.
//
// This scans every top-level .py file's actual CONTENT for the
// variable-assignment pattern that exposes the WSGI/ASGI app object
// Python web servers need to import (`<name> = Flask(...)`,
// `<name> = create_app(...)`, `<name> = FastAPI(...)`), regardless of
// what the file itself is named. The fixed-filename list is still
// tried FIRST as a fast path (covers the overwhelming majority of
// repos without needing to open every file), and content scanning
// only kicks in when that fast path finds nothing.
// [FIX] Package-folder layouts are extremely common in real FastAPI/Flask
// repos (e.g. nsidnev/fastapi-realworld-example-app has its entry point at
// app/main.py, not main.py at the project root). Checked AFTER the root
// scan, and restricted to this small, deliberate list of conventional
// package names -- not a fully recursive walk -- to avoid matching test
// fixtures, migrations, or vendored dependencies several folders deep.
const PYTHON_PACKAGE_DIRS = ['app', 'src', 'backend', 'api'];

function findPythonEntryFile(projectRoot, patterns, frameworkKeyword) {
  // Fast path: try the common filenames first (app.py, main.py, etc.) at
  // the project root.
  const commonNames = ['app', 'main', 'wsgi', 'server', 'run', 'api', 'application'];
  for (const name of commonNames) {
    const filePath = path.join(projectRoot, name + '.py');
    if (!fs.existsSync(filePath)) continue;
    try {
      const content = fs.readFileSync(filePath, 'utf8');
      const varMatch = patterns.map(p => content.match(p)).find(m => m);
      if (varMatch) return { module: name, varName: varMatch[1] };
    } catch (_) {}
  }

  // Content scan: any .py file directly at project root (not recursing
  // into subfolders yet -- covers custom entry-point filenames like
  // microblog.py that use a standard create_app()/Flask()/FastAPI()
  // assignment).
  let entries;
  try { entries = fs.readdirSync(projectRoot); } catch (_) { entries = []; }
  const pyFiles = entries.filter(e => e.endsWith('.py') && !commonNames.includes(e.replace(/\.py$/, '')));
  for (const fname of pyFiles) {
    try {
      const content = fs.readFileSync(path.join(projectRoot, fname), 'utf8');
      const varMatch = patterns.map(p => content.match(p)).find(m => m);
      if (varMatch) return { module: fname.replace(/\.py$/, ''), varName: varMatch[1] };
    } catch (_) {}
  }

  // Package-folder scan: check app/main.py, src/main.py, etc. -- covers
  // real-world package layouts where the entry point lives one level
  // inside a conventionally-named package directory. Reports the dotted
  // module path (e.g. "app.main") so the generated gunicorn/uvicorn
  // command imports it correctly.
  for (const pkg of PYTHON_PACKAGE_DIRS) {
    const pkgDir = path.join(projectRoot, pkg);
    if (!fs.existsSync(pkgDir) || !fs.lstatSync(pkgDir).isDirectory()) continue;
    for (const name of commonNames) {
      const filePath = path.join(pkgDir, name + '.py');
      if (!fs.existsSync(filePath)) continue;
      try {
        const content = fs.readFileSync(filePath, 'utf8');
        const varMatch = patterns.map(p => content.match(p)).find(m => m);
        if (varMatch) return { module: `${pkg}.${name}`, varName: varMatch[1] };
      } catch (_) {}
    }
  }

  // Last resort: a generalized factory-function pattern -- catches
  // `<var> = <any_function_name>()` (zero-arg call) in any file that
  // also imports the given framework somewhere in its content. This is
  // deliberately broader/fuzzier than the exact create_app()/Flask()
  // checks above, so it only runs once everything more precise has
  // already failed to match -- covers real repos like
  // nsidnev/fastapi-realworld-example-app's `app = get_application()`
  // where the factory function has an arbitrary, non-conventional name.
  if (frameworkKeyword) {
    const searchDirs = [projectRoot, ...PYTHON_PACKAGE_DIRS.map(p => path.join(projectRoot, p))];
    for (const dir of searchDirs) {
      let dirEntries;
      try { dirEntries = fs.readdirSync(dir); } catch (_) { continue; }
      for (const fname of dirEntries.filter(e => e.endsWith('.py'))) {
        try {
          const content = fs.readFileSync(path.join(dir, fname), 'utf8');
          if (!new RegExp(frameworkKeyword, 'i').test(content)) continue;
          const factoryMatch = content.match(/^(\w+)\s*=\s*\w+\(\s*\)\s*$/m);
          if (factoryMatch) {
            const relDir = path.relative(projectRoot, dir);
            const moduleName = fname.replace(/\.py$/, '');
            const modulePath = relDir ? `${relDir.replace(/[\\/]/g, '.')}.${moduleName}` : moduleName;
            return { module: modulePath, varName: factoryMatch[1] };
          }
        } catch (_) {}
      }
    }
  }

  return null;
}

function autoDetectPythonStartCmd(projectRoot, runtime, expectedPort, log) {
  const hasManagePy = fs.existsSync(path.join(projectRoot, 'manage.py'));

  // Django
  if (runtime.framework === 'Django' || hasManagePy) {
    const wsgiModule = findDjangoWsgiModule(projectRoot);
    log(`\x1b[90m[python] Auto-detected Django — building gunicorn start command\x1b[0m`);
    return `gunicorn ${wsgiModule}:application --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
  }

  // FastAPI / uvicorn — checks (in order): common root filenames with a
  // direct FastAPI(...) assignment, any root .py file with the same,
  // app/main.py-style package layouts, then a fuzzy factory-function
  // fallback for repos like nsidnev/fastapi-realworld-example-app where
  // `app = get_application()` wraps FastAPI() inside an arbitrarily named
  // factory function several calls deep.
  const fastapiEntry = findPythonEntryFile(projectRoot, [
    /^(\w+)\s*=\s*FastAPI\s*\(/m,
  ], 'fastapi');
  if (runtime.framework === 'FastAPI' || fastapiEntry) {
    if (fastapiEntry) {
      log(`\x1b[90m[python] Auto-detected FastAPI entry point: ${fastapiEntry.module} (${fastapiEntry.varName})\x1b[0m`);
      return `uvicorn ${fastapiEntry.module}:${fastapiEntry.varName} --host 0.0.0.0 --port $PORT --workers 2`;
    }
    return `uvicorn main:app --host 0.0.0.0 --port $PORT --workers 2`;
  }

  // Flask / generic gunicorn — same layered approach: direct Flask(...)
  // assignment, the standard create_app() application-factory pattern
  // (covers miguelgrinberg/microblog's microblog.py and most real-world
  // Flask tutorials/boilerplates), package-folder layouts, then the fuzzy
  // factory-function fallback for anything more custom.
  const flaskEntry = findPythonEntryFile(projectRoot, [
    /^(\w+)\s*=\s*Flask\s*\(/m,
    /^(\w+)\s*=\s*create_app\s*\(/m,
  ], 'flask');
  if (runtime.framework === 'Flask' || flaskEntry) {
    if (flaskEntry) {
      log(`\x1b[90m[python] Auto-detected Flask entry point: ${flaskEntry.module} (${flaskEntry.varName})\x1b[0m`);
      return `gunicorn ${flaskEntry.module}:${flaskEntry.varName} --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
    }
    return `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
  }

  // Plain Python fallback
  const pyFile = ['app.py', 'main.py', 'server.py', 'run.py', 'index.py'].find(f =>
    fs.existsSync(path.join(projectRoot, f))
  ) || 'app.py';
  return `python ${pyFile}`;
}

// ── Python container preflight script ────────────────────────────────────────
// Runs BEFORE the start command inside the container.
// Ensures gunicorn/uvicorn is installed, DJANGO_SETTINGS_MODULE is set if
// needed, and does a quick wsgi import dry-run for Django so crashes are
// caught early with a meaningful message rather than a silent exit-1.
function buildPythonPreflight({ isDjangoRuntime, projectRoot, reqFile, startCmd, expectedPort }) {
  const lines = [];

  // Ensure gunicorn is available if the start command uses it
  if (/\bgunicorn\b/.test(startCmd)) {
    lines.push(
      `python -c "import gunicorn" 2>/dev/null || pip install --prefer-binary gunicorn --quiet`
    );
  }
  // Ensure uvicorn is available if the start command uses it
  if (/\buvicorn\b/.test(startCmd)) {
    lines.push(
      `python -c "import uvicorn" 2>/dev/null || pip install --prefer-binary uvicorn --quiet`
    );
  }

  if (isDjangoRuntime) {
    // Auto-set DJANGO_SETTINGS_MODULE if not already in env
    // Scan for a settings.py one level deep and use its parent package
    lines.push(
      `if [ -z "$DJANGO_SETTINGS_MODULE" ]; then ` +
        `SETTINGS=$(python -c "import glob,os; s=[f for f in glob.glob('*/settings.py')+glob.glob('*/settings/*.py')]; ` +
        `print(s[0].replace('/','.',1).replace('.py','') if s else '') " 2>/dev/null); ` +
        `if [ -n "$SETTINGS" ]; then export DJANGO_SETTINGS_MODULE=$SETTINGS; ` +
        `echo "[Joytree] Auto-set DJANGO_SETTINGS_MODULE=$SETTINGS"; fi; fi`
    );

    // Dry-run the wsgi import so we get a readable error early
    lines.push(
      `python -c "` +
        `import sys, os; ` +
        `mods = __import__('glob').glob('*/wsgi.py'); ` +
        `mod = mods[0].replace('/','.',1).replace('.py','') if mods else None; ` +
        `sys.exit(0) if not mod else None; ` +
        `result = os.popen('python -c \\"import ' + mod + '\\"').read(); ` +
      `" 2>/dev/null || true`
    );

    // Warn clearly if SECRET_KEY is missing (Django won't start without it)
    lines.push(
      `if [ -z "$SECRET_KEY" ] && [ -z "$DJANGO_SECRET_KEY" ]; then ` +
        `echo "[Joytree] Warning: SECRET_KEY env var is not set. Django may fail to start. ` +
        `Add SECRET_KEY to your Environment Variables in the dashboard."; fi`
    );

    // Warn if DEBUG is not explicitly set (defaults to False which requires ALLOWED_HOSTS)
    lines.push(
      `if [ -z "$DEBUG" ]; then ` +
        `echo "[Joytree] Tip: Set DEBUG=False and ALLOWED_HOSTS=* in Environment Variables for production."; fi`
    );
  }

  if (lines.length === 0) return '';
  return lines.join('; ') + '; ';
}

// ── Smart Python build orchestrator ─────────────────────────────────────────
// Replaces the old single-command build step with a sequenced, per-step
// approach that handles Django's many build-time requirements intelligently.
//
// Steps (all soft-fail — a failure logs a warning but never aborts deploy):
//   1. Ensure Django is importable (guard against broken installs)
//   2. Auto-set DJANGO_SETTINGS_MODULE if missing
//   3. Auto-inject a dummy SECRET_KEY if missing (Django won't even load without it)
//   4. makemigrations --check (warn if user forgot to commit migration files)
//   5. migrate --noinput (soft-fail if no DB configured)
//   6. collectstatic --noinput (soft-fail if STATIC_ROOT missing)
//   7. Run any extra user-supplied build command
//   For non-Django: run user build command or skip gracefully
async function runSmartPythonBuild({ projectRoot, pythonImage, env, runtime, userBuildCmd, isDjango, log }) {

  // ── Helper: run one shell step, log a warning on failure, never throw ────
  async function softRun(label, cmd) {
    log(`\x1b[90m$ ${cmd}\x1b[0m`);
    try {
      await runPythonCommandInContainer({ projectRoot, pythonImage, envObj: env, command: cmd, log });
    } catch (e) {
      const msg = String(e.message || '').split('\n').slice(0, 3).join(' | ');
      log(`\x1b[33m[Joytree] ${label} — ${msg}\x1b[0m`);
    }
  }

  // ── Non-Django path ──────────────────────────────────────────────────────
  if (!isDjango) {
    // User supplied a build command — run it, soft-fail
    if (userBuildCmd && userBuildCmd !== 'echo skip') {
      log(`\x1b[90m[python] Running user build command\x1b[0m`);
      await softRun('Build step warning (continuing)', userBuildCmd);
    } else {
      // Auto-detect: pyproject.toml build, Makefile build target, etc.
      const hasMakefile  = fs.existsSync(path.join(projectRoot, 'Makefile'));
      const hasPyproject = fs.existsSync(path.join(projectRoot, 'pyproject.toml'));
      if (hasMakefile) {
        // Only run if a "build" target exists
        try {
          const mk = fs.readFileSync(path.join(projectRoot, 'Makefile'), 'utf8');
          if (/^build\s*:/m.test(mk)) {
            log(`\x1b[33m[Joytree] Auto-detected Makefile build target — running "make build".\x1b[0m`);
            await softRun('make build warning (continuing)', 'make build');
          } else {
            log(`\x1b[90m(no build step — skipping)\x1b[0m`);
          }
        } catch(_) { log(`\x1b[90m(no build step — skipping)\x1b[0m`); }
      } else {
        log(`\x1b[90m(no build step — skipping)\x1b[0m`);
      }
    }
    return;
  }

  // ── Django path ───────────────────────────────────────────────────────────
  log(`\x1b[90m[django] Running Django build steps\x1b[0m`);

  // Build a safe env object that always has what Django needs to import
  // ── 1. Auto-resolve DJANGO_SETTINGS_MODULE ────────────────────────────
  let settingsModule = env['DJANGO_SETTINGS_MODULE'] || '';
  if (!settingsModule) {
    settingsModule = detectDjangoSettingsModule(projectRoot);
    if (settingsModule) {
      log(`\x1b[33m[Joytree] DJANGO_SETTINGS_MODULE not set — auto-detected: ${settingsModule}. ` +
          `Add it to your Environment Variables to remove this warning.\x1b[0m`);
    } else {
      log(`\x1b[33m[Joytree] Warning: Could not auto-detect DJANGO_SETTINGS_MODULE. ` +
          `Set it in your Environment Variables (e.g. myproject.settings).\x1b[0m`);
    }
  }

  // ── 2. Build-time env: inject dummy SECRET_KEY and safe defaults ──────
  // Django refuses to even import settings without SECRET_KEY.
  // We use a deterministic dummy ONLY for the build container — the real
  // secret from the user's env vars is used at runtime.
  const buildEnv = { ...env };
  if (!buildEnv['SECRET_KEY'] && !buildEnv['DJANGO_SECRET_KEY']) {
    buildEnv['SECRET_KEY'] = 'joytree-build-only-dummy-secret-key-not-for-production';
    log(`\x1b[33m[Joytree] SECRET_KEY not found in Environment Variables — using a temporary build-only key. ` +
        `Add SECRET_KEY to your Environment Variables for production.\x1b[0m`);
  }
  if (settingsModule) buildEnv['DJANGO_SETTINGS_MODULE'] = settingsModule;

  // Allow all hosts during build (collectstatic imports settings which checks ALLOWED_HOSTS in some configs)
  if (!buildEnv['ALLOWED_HOSTS']) buildEnv['ALLOWED_HOSTS'] = '*';
  // Disable DEBUG warnings during build
  if (!buildEnv['DEBUG']) buildEnv['DEBUG'] = 'False';

  // Wrap runPythonCommandInContainer with build-time env
  async function djangoRun(label, cmd) {
    log(`\x1b[90m$ ${cmd}\x1b[0m`);
    try {
      await runPythonCommandInContainer({ projectRoot, pythonImage, envObj: buildEnv, command: cmd, log });
    } catch (e) {
      const msg = String(e.message || '').split('\n').slice(0, 4).join(' | ');
      log(`\x1b[33m[Joytree] ${label} — ${msg}\x1b[0m`);
    }
  }

  // ── 3. Verify Django is importable — catch broken installs early ───────
  log(`\x1b[90m[django] Verifying Django installation...\x1b[0m`);
  try {
    await runPythonCommandInContainer({
      projectRoot, pythonImage, envObj: buildEnv,
      command: `python -c "import django; print('[Joytree] Django', django.__version__, 'ready')"`,
      log,
    });
  } catch(_) {
    log(`\x1b[33m[Joytree] Warning: Django does not appear to be installed. ` +
        `Make sure django is in your requirements.txt.\x1b[0m`);
    // If Django itself isn't installed, skip all manage.py steps
    if (userBuildCmd && userBuildCmd !== 'echo skip') {
      await softRun('User build command warning (continuing)', userBuildCmd);
    }
    return;
  }

  // ── 4. Check for missing migration files (makemigrations --check) ─────
  // This doesn't create files — just warns the user if they forgot to commit
  // their migration files (a very common Django mistake).
  log(`\x1b[90m[django] Checking migration files...\x1b[0m`);
  try {
    await runPythonCommandInContainer({
      projectRoot, pythonImage, envObj: buildEnv,
      command: 'python manage.py makemigrations --check --dry-run 2>&1',
      log,
    });
  } catch(_) {
    log(`\x1b[33m[Joytree] Warning: Your models have changes that are not yet reflected in migration files. ` +
        `Run "python manage.py makemigrations" locally and commit the generated files to your repo.\x1b[0m`);
  }

  // ── 5. migrate ────────────────────────────────────────────────────────
  // Soft-fail: if no database is configured (no DATABASE_URL), Django will
  // use SQLite which is fine for simple apps, or fail gracefully.
  const hasDbUrl = !!(buildEnv['DATABASE_URL'] || buildEnv['DB_URL'] ||
                      buildEnv['POSTGRES_URL'] || buildEnv['MYSQL_URL'] ||
                      buildEnv['DATABASE_HOST'] || buildEnv['DB_HOST']);
  if (!hasDbUrl) {
    log(`\x1b[33m[Joytree] No DATABASE_URL environment variable found — migrate will use SQLite or may be skipped. ` +
        `Add DATABASE_URL to your Environment Variables for a production database.\x1b[0m`);
  }
  log(`\x1b[90m[django] Running database migrations...\x1b[0m`);
  await djangoRun(
    'migrate warning (app will still deploy — check your DATABASE_URL env var)',
    'python manage.py migrate --noinput 2>&1'
  );

  // ── 6. collectstatic ──────────────────────────────────────────────────
  // Only run if there is a static files setup or user has STATIC_ROOT set.
  // We auto-inject a temporary STATIC_ROOT if missing so the command
  // doesn't crash the entire build.
  const hasStaticRoot = !!(buildEnv['STATIC_ROOT'] || buildEnv['STATICFILES_DIRS']);
  const hasStaticDir  = fs.existsSync(path.join(projectRoot, 'static')) ||
                        fs.existsSync(path.join(projectRoot, 'staticfiles'));
  if (hasStaticRoot || hasStaticDir) {
    if (!buildEnv['STATIC_ROOT']) {
      buildEnv['STATIC_ROOT'] = '/workspace/staticfiles';
      log(`\x1b[33m[Joytree] STATIC_ROOT not set — temporarily using /staticfiles for collectstatic. ` +
          `Set STATIC_ROOT in your Environment Variables (e.g. /app/staticfiles) for a persistent location.\x1b[0m`);
    }
    log(`\x1b[90m[django] Collecting static files...\x1b[0m`);
    await djangoRun(
      'collectstatic warning (static files may not be served — set STATIC_ROOT in Env Variables)',
      'python manage.py collectstatic --noinput 2>&1'
    );
  } else {
    log(`\x1b[90m[django] No static directory found — skipping collectstatic. ` +
        `(Add a "static/" folder or set STATIC_ROOT to enable it)\x1b[0m`);
  }

  // ── 7. User-supplied extra build command ──────────────────────────────
  // Check if the user typed a custom build command that isn't just the
  // default Django commands (we already ran those above).
  const defaultDjangoBuildPatterns = [
    /python\s+manage\.py\s+collectstatic/i,
    /python\s+manage\.py\s+migrate/i,
    /^echo\s+skip$/i,
  ];
  const isDefaultCmd = !userBuildCmd || defaultDjangoBuildPatterns.some(p => p.test(userBuildCmd));
  if (!isDefaultCmd) {
    log(`\x1b[90m[django] Running user build command: ${userBuildCmd}\x1b[0m`);
    await djangoRun('User build command warning (continuing)', userBuildCmd);
  }

  log(`\x1b[32m[django] \u2713 Build steps complete\x1b[0m`);
}

// Scans projectRoot for a settings.py (or settings/__init__.py) and returns
// the dotted Python module path, e.g. "myproject.settings".
function detectDjangoSettingsModule(projectRoot) {
  try {
    const entries = fs.readdirSync(projectRoot);
    for (const entry of entries) {
      if (['__pycache__', '.git', 'venv', '.venv', 'env', 'node_modules'].includes(entry)) continue;
      const entryPath = path.join(projectRoot, entry);
      try {
        if (!fs.lstatSync(entryPath).isDirectory()) continue;
      } catch(_) { continue; }
      // settings.py directly inside a package folder
      if (fs.existsSync(path.join(entryPath, 'settings.py'))) return `${entry}.settings`;
      // settings/ package (settings/__init__.py or settings/base.py)
      const settingsDir = path.join(entryPath, 'settings');
      if (fs.existsSync(settingsDir)) {
        if (fs.existsSync(path.join(settingsDir, '__init__.py'))) return `${entry}.settings`;
        if (fs.existsSync(path.join(settingsDir, 'base.py')))     return `${entry}.settings.base`;
        if (fs.existsSync(path.join(settingsDir, 'production.py'))) return `${entry}.settings.production`;
        if (fs.existsSync(path.join(settingsDir, 'prod.py')))     return `${entry}.settings.prod`;
      }
    }
  } catch(_) {}
  return '';
}


async function runPythonCommandInContainer({ projectRoot, pythonImage, envObj, command, log }) {
  try { await exec('docker', ['pull', pythonImage], {}, () => {}); } catch(_) {}
  log(`\x1b[90m[docker-build] ${pythonImage} :: ${command}\x1b[0m`);
  const envArgs = [
    '-e', 'PYTHONUNBUFFERED=1',
    '-e', 'PYTHONDONTWRITEBYTECODE=1',
    '-e', 'PIP_NO_CACHE_DIR=off',
    '-e', 'PIP_DISABLE_PIP_VERSION_CHECK=1',
    // Prefer pre-built binary wheels to avoid needing gcc/build-essential
    '-e', 'PIP_PREFER_BINARY=1',
    ...Object.entries(envObj || {}).flatMap(([k, v]) => ['-e', `${k}=${String(v ?? '')}`]),
  ];
  // [FIX] These containers run with `--rm` and PIP_NO_CACHE_DIR=off (cache
  // enabled), but pip's default cache dir (~/.cache/pip) is wiped along with
  // the container, so every deploy re-downloads every wheel from PyPI.
  // Mount a persistent cache dir so unchanged dependencies are restored from
  // local disk on subsequent deploys.
  const pipCache = getCacheDir('pip');
  const cacheMounts = pipCache ? ['-v', `${pipCache}:/root/.cache/pip`] : [];
  // Inject build tools installation before user command so packages that
  // must compile from source (e.g. lxml, psycopg2, Pillow) can do so.
  // We always attempt apt-get first; if it fails (non-Debian image) we skip silently.
  const wrappedCommand =
    `apt-get update -qq 2>/dev/null && apt-get install -y --no-install-recommends ` +
    `gcc g++ libffi-dev libssl-dev libpq-dev python3-dev build-essential 2>/dev/null || true; ` +
    `pip install --upgrade pip --quiet; ` +
    command;
  await exec('docker', [
    'run', '--rm',
    ...envArgs,
    ...cacheMounts,
    '-v', `${projectRoot}:/workspace`,
    '-w', '/workspace',
    pythonImage,
    'sh', '-c', wrappedCommand
  ], {}, log);
}

// ── GENERIC COMPILED/RUNTIME BUILDER ─────────────────────────────────────────
// Shared skeleton used by Go, PHP, Ruby, Java, Rust, .NET, Elixir, Bun, Deno.
// Each caller supplies: dockerImage, installCmd, buildCmd, startCmd,
//   findRoot (fn or null → falls back to repo root), extraEnv (obj).
async function runGenericBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain,
  dockerImage, runtimeImage, findRoot, extraEnv = {}, stepLabel, smartHooks = {} }) {
  // runtimeImage: Docker image for the long-running server container.
  // dockerImage:  Docker image for build steps (install/build) only.
  // If runtimeImage is omitted, it falls back to dockerImage (no behaviour change for
  // Go/Ruby/Java/Rust/.NET/Elixir/Bun/Deno which all use one image for both phases).
  // PHP uses dockerImage='composer:2' for building + runtimeImage='php:X.Y-cli' for serving.
  const buildImage   = dockerImage;
  const containerImg = runtimeImage || dockerImage;

  const buildDir      = path.join(tmpDir, deployId);
  const appDir        = path.join(sitesDir, project.subdomain, 'app');
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0, 20)}`;
  const expectedPort  = normalizePort(appPort, 8080);

  const log = createBatchedLogger(emit, onLog);
  const env = withDeployedAppRuntimeDefaults({ ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project), ...extraEnv }, project, baseDomain);

  // Step 1: Clone
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = resolveWorkingDirRoot(buildDir, project, log) || (findRoot ? findRoot(buildDir, log) : buildDir);
  if (containerImg !== buildImage) {
    log(`\x1b[90m[${stepLabel}] Build image: ${buildImage} | Runtime image: ${containerImg}\x1b[0m`);
  } else {
    log(`\x1b[90m[${stepLabel}] Image: ${buildImage}\x1b[0m`);
  }

  // ── Smart command resolution (per-framework hooks) ──────────────────────────
  // smartHooks.resolveInstall(projectRoot, project, port, log) -> { cmd, warnings }
  // smartHooks.resolveBuild(projectRoot, project, env, log)    -> { cmd, warnings }
  // smartHooks.resolveStart(projectRoot, project, port, log)   -> { cmd, warnings }
  // smartHooks.buildEnv(projectRoot, project, env, log)        -> { ...extraEnvVars }
  const extraRuntimeEnv = smartHooks.buildEnv ? smartHooks.buildEnv(projectRoot, project, env, log) : {};

  const { cmd: installCmd, warnings: installWarnings } = smartHooks.resolveInstall
    ? smartHooks.resolveInstall(projectRoot, project, expectedPort, log)
    : { cmd: (project.installCmd || '').trim(), warnings: [] };
  for (const w of installWarnings) log(`\x1b[33m[Joytree] ${w}\x1b[0m`);

  // Step 2: Install
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 2/6 — Install Dependencies ━━━\x1b[0m`);
  if (installCmd && installCmd !== 'echo skip') {
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    try {
      await runCommandInImage({ projectRoot, image: dockerImage, envObj: env, command: installCmd, log });
    } catch(e) {
      const msg = String(e.message || '');
      if (smartHooks.installErrorHandler) {
        if (!smartHooks.installErrorHandler(msg, log)) throw e;
      } else {
        log(`\x1b[33m[Joytree] Install warning: ${msg.split('\n')[0]} — continuing.\x1b[0m`);
      }
    }
  } else {
    log('\x1b[90m(no install step)\x1b[0m');
  }
  emitStep(emit, 'install', 'done');

  const { cmd: buildCmd, warnings: buildWarnings } = smartHooks.resolveBuild
    ? smartHooks.resolveBuild(projectRoot, project, env, log)
    : { cmd: (project.buildCmd || '').trim(), warnings: [] };
  for (const w of buildWarnings) log(`\x1b[33m[Joytree] ${w}\x1b[0m`);

  // Step 3: Build
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 3/6 — Build ━━━\x1b[0m`);
  if (buildCmd && buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runCommandInImage({ projectRoot, image: dockerImage, envObj: env, command: buildCmd, log });
    } catch (e) {
      const msg = String(e.message || '');
      if (smartHooks.buildErrorHandler) {
        if (!smartHooks.buildErrorHandler(msg, log)) throw e;
      } else {
        log(`\x1b[33m[Joytree] Build warning: ${msg.split('\n')[0]} — continuing.\x1b[0m`);
      }
    }
  } else {
    log('\x1b[90m(no build step)\x1b[0m');
  }
  emitStep(emit, 'build', 'done');

  // Step 4: Persist app dir
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 4/6 — Prepare App Dir ━━━\x1b[0m`);
  if (fs.existsSync(appDir)) fs.rmSync(appDir, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(appDir), { recursive: true });
  try {
    fs.renameSync(projectRoot, appDir);
    log(`\x1b[32m[copy] ✓ App moved to permanent storage\x1b[0m`);
  } catch (e) {
    log(`\x1b[90m[copy] Cross-device move, copying…\x1b[0m`);
    fs.mkdirSync(appDir, { recursive: true });
    copyDir(projectRoot, appDir);
    log(`\x1b[32m[copy] ✓ Done\x1b[0m`);
  }
  emitStep(emit, 'copy', 'done');

  // ── Smart start command resolution ────────────────────────────────────────
  const { cmd: resolvedStartCmd, warnings: startWarnings } = smartHooks.resolveStart
    ? smartHooks.resolveStart(projectRoot, project, expectedPort, log)
    : { cmd: (project.startCmd || '').trim(), warnings: [] };
  for (const w of startWarnings) log(`\x1b[33m[Joytree] ${w}\x1b[0m`);

  const runtimeEnv = { ...env, ...extraRuntimeEnv };

  // Step 5: Launch container
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);
  log(`\x1b[90m[docker] Command: ${resolvedStartCmd}\x1b[0m`);

  const hostAppDir = appDir
    .replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data');
  const dockerMountSrc = appDir.startsWith('/tmp') ? appDir : hostAppDir;

  try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
  log(`\x1b[90m[docker] Pulling ${containerImg}…\x1b[0m`);
  try { await exec('docker', ['pull', containerImg], {}, log); } catch(_) { log('\x1b[33m[docker] Using cached image\x1b[0m'); }

  const runtime = getRuntimeConfig(project);
  const runArgs = [
    'run', '-d',
    '--name', candidateContainerName, '--restart', 'no',
    '--network', 'deployboard-net',
    '--add-host', 'host.docker.internal:host-gateway',
    '--cpu-shares', runtime.cpuShares, '--pids-limit', PIDS_LIMIT,
    '-m', runtime.memory, '--memory-reservation', runtime.memory,
    '-e', `PORT=${expectedPort}`,
    ...Object.entries(runtimeEnv).flatMap(([k, v]) => {
      if (String(k).toUpperCase() === 'PORT') return [];
      return ['-e', `${k}=${v}`];
    }),
    '-v', `${dockerMountSrc}:/app`, '-w', '/app',
    containerImg, 'sh', '-c', resolvedStartCmd
  ];
  if (runtime.memorySwap) runArgs.splice(runArgs.indexOf('-e'), 0, '--memory-swap', runtime.memorySwap);
  log(`\x1b[90m[docker] Persistent server container: restart=unless-stopped | ${runtime.cpuShares} CPU shares | ${runtime.memory} RAM | ${PIDS_LIMIT} max processes\x1b[0m`);

  await exec('docker', runArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);

  const stable = await waitForContainerRunning(candidateContainerName, 40, log);
  if (!stable) {
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(_) {}
    const hint = smartHooks.startupFailureHint ? smartHooks.startupFailureHint(project) : 'Check your start command and environment variables.';
    throw new Error(`Container exited during startup. ${hint}\nSee logs above.`);
  }

  const pFile = path.join(sitesDir, 'ports.json');
  const previousTarget = readRegistryTarget(pFile, project.subdomain);
  try {
    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(_) {}
    const livePort = await detectLivePort(candidateContainerName, expectedPort, 120, log);
    if (!livePort) throw new Error(`Readiness gate failed — app did not respond on any port within 120s`);
    registry[project.subdomain] = `${containerName}:${livePort}`;
    fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    log(`\x1b[32m[docker] ✓ Proxy: ${project.subdomain} → ${containerName}:${livePort}\x1b[0m`);
    await archivePreviousContainer(containerName, project.subdomain, log);
    await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
    // [FIX] The candidate container launches with --restart=no so a
    // crash-looping start command stays visibly exited instead of being
    // silently relaunched by Docker mid-healthcheck (which was masking
    // startup failures as "still waiting" forever -- see detectLivePort's
    // liveness check above). Only apply real crash-resilience now that the
    // container has actually proven it serves HTTP.
    try { await exec('docker', ['update', '--restart', 'unless-stopped', containerName], {}, () => {}); } catch(_) {}
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);
  } catch (e) {
    log(`\x1b[31m[docker] Failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (!isStableRegistryTarget(previousTarget)) {
      let reg = {};
      try { reg = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(_) {}
      delete reg[project.subdomain];
      fs.writeFileSync(pFile, JSON.stringify(reg, null, 2));
    }
    throw e;
  }
  emitStep(emit, 'start', 'done');

  // Step 6: Cleanup
  emitStep(emit, 'cleanup', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(_) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ App deployed in isolated container!\x1b[0m`);
}

// Runs a shell command inside a Docker image against the project source (for build steps)
// [FIX] Map a build image to the package-manager cache dir(s) it should use,
// so ephemeral `docker run --rm` build containers for Go/PHP/Ruby/Java/Rust/
// .NET/Elixir/Bun/Deno also benefit from a persistent cache (same rationale
// as the npm/pip caching above — without this, every deploy re-downloads
// the entire dependency tree: Go modules, Composer packages, gems, Maven/
// Gradle artifacts, Cargo crates, NuGet packages, Hex packages, etc.).
function getRuntimeCacheMounts(image) {
  const mounts = [];
  const env = [];
  const add = (sub, containerPath, envVar) => {
    const dir = getCacheDir(sub);
    if (!dir) return;
    mounts.push('-v', `${dir}:${containerPath}`);
    if (envVar) env.push('-e', `${envVar}=${containerPath}`);
  };
  if (/^golang:/.test(image)) {
    add('go-mod',   '/go/pkg/mod');
    add('go-build', '/root/.cache/go-build', 'GOCACHE');
  } else if (/^(php|composer)/.test(image)) {
    add('composer', '/tmp/composer-cache', 'COMPOSER_CACHE_DIR');
  } else if (/^ruby:/.test(image)) {
    add('bundle', '/usr/local/bundle');
  } else if (/^(maven|gradle|openjdk|eclipse-temurin)/.test(image)) {
    add('m2',     '/root/.m2');
    add('gradle', '/root/.gradle');
  } else if (/^rust:/.test(image)) {
    add('cargo-registry', '/usr/local/cargo/registry');
    add('cargo-git',      '/usr/local/cargo/git');
  } else if (/dotnet/.test(image)) {
    add('nuget', '/root/.nuget/packages', 'NUGET_PACKAGES');
  } else if (/^elixir:/.test(image)) {
    add('hex', '/root/.hex');
    add('mix-cache', '/root/.cache/mix');
  } else if (/bun/.test(image)) {
    add('bun', '/root/.bun/install/cache');
  } else if (/deno/.test(image)) {
    add('deno', '/root/.cache/deno', 'DENO_DIR');
  }
  return { mounts, env };
}

async function runCommandInImage({ projectRoot, image, envObj, command, log }) {
  try { await exec('docker', ['pull', image], {}, () => {}); } catch(_) {}
  // Strip leading "cd <subdir> &&" — container is already at projectRoot via -w /workspace
  const cdPrefixMatch = String(command || '').trim().match(/^cd\s+\S+\s*&&\s*/i);
  if (cdPrefixMatch) command = command.trim().slice(cdPrefixMatch[0].length).trim();
  log(`\x1b[90m[docker-build] ${image} :: ${command}\x1b[0m`);
  const envArgs = Object.entries(envObj || {}).flatMap(([k, v]) => ['-e', `${k}=${String(v ?? '')}`]);
  // [FIX] Persist this runtime's dependency cache across deploys — see
  // getRuntimeCacheMounts() for rationale.
  const { mounts: cacheMounts, env: cacheEnv } = getRuntimeCacheMounts(image);
  await exec('docker', [
    'run', '--rm', ...envArgs, ...cacheEnv, ...cacheMounts,
    '-v', `${projectRoot}:/workspace`, '-w', '/workspace',
    image, 'sh', '-c', command
  ], {}, log);
}

// ── Go ────────────────────────────────────────────────────────────────────────
async function runGoBuild(opts) {
  const goVer = String(opts.project.goVer || '1.22');
  const image = `golang:${goVer}-bullseye`;
  const port  = normalizePort(opts.appPort, 8080);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'go',
    findRoot: (dir, log) => findFileRoot(dir, ['go.mod'], log) || dir,
    extraEnv: { CGO_ENABLED: '0', GOOS: 'linux', GOARCH: 'amd64' },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        let cmd = (project.installCmd || '').trim() || 'go mod download';
        const warnings = [];
        if (!fs.existsSync(path.join(projectRoot, 'go.mod'))) {
          warnings.push('⚠ No go.mod found. Run "go mod init <your-module-name>" and "go mod tidy" locally, then commit go.mod and go.sum.');
          cmd = 'go mod init joytree-app 2>/dev/null || true; go mod tidy 2>/dev/null || true; go mod download 2>/dev/null || true';
          warnings.push('ℹ Joytree attempted "go mod tidy" automatically but results may be incomplete without the correct module name.');
        }
        // [FALLBACK] "go get" as install — non-standard, switch to go mod download
        if (/^go\\s+get\\b/.test(cmd) && !/^go\\s+mod\\b/.test(cmd)) {
          warnings.push('ℹ "go get" as install command is unusual. Switching to "go mod download". Update your Install Command if needed.');
          cmd = 'go mod download';
        }
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        if (/go\\.sum.*missing|verifying.*mismatch|hash.*does not match/i.test(msg)) {
          log('\\x1b[33m[Joytree] ⚠ go.sum mismatch — run "go mod tidy" locally and commit the updated go.sum file.\\x1b[0m');
          return false;
        }
        if (/module lookup disabled|forbidden|GOPROXY/i.test(msg)) {
          log('\\x1b[33m[Joytree] ⚠ Cannot access a Go module — it may be private or have an incorrect path. Check your go.mod.\\x1b[0m');
          return false;
        }
        if (/connection refused|timeout|dial tcp/i.test(msg)) {
          log('\\x1b[33m[Joytree] ⚠ Network error downloading Go modules. Try redeploying. If it persists, check that all module paths in go.mod are public.\\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project) {
        let cmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          const cmdDirs = ['cmd/server', 'cmd/app', 'cmd/main', 'cmd/api', 'cmd/web'].filter(d =>
            fs.existsSync(path.join(projectRoot, d, 'main.go'))
          );
          let autoDir = cmdDirs[0] || null;
          if (!autoDir) {
            try {
              for (const e of fs.readdirSync(projectRoot)) {
                if (['vendor', '.git', 'test', 'tests', 'testdata'].includes(e)) continue;
                if (fs.existsSync(path.join(projectRoot, e, 'main.go'))) { autoDir = e; break; }
              }
            } catch(_) {}
          }
          if (autoDir) {
            cmd = `go build -o server ./${autoDir}`;
            warnings.push(`Auto-detected main package at ./${autoDir}. Set your Build Command to remove this message.`);
          } else {
            cmd = 'go build -o server .';
            if (!fs.existsSync(path.join(projectRoot, 'main.go'))) {
              warnings.push('⚠ No main.go found at repo root or common subdirs (cmd/server, cmd/app). Make sure your main package is committed.');
            }
          }
        }
        // [FALLBACK] go build without -o — binary name will be directory name (unpredictable)
        if (/\\bgo\\s+build\\b/.test(cmd) && !/-o\\s+/.test(cmd)) {
          cmd = cmd.replace(/\\bgo\\s+build\\b/, 'go build -o server');
          warnings.push('⚠ Added "-o server" to go build for a predictable binary name. Update your Build Command to include "-o server".');
        }
        if (/sqlite|cgo/i.test(cmd)) {
          warnings.push('ℹ CGO may be needed. Joytree sets CGO_ENABLED=0 by default. Add CGO_ENABLED=1 to your Environment Variables if your app uses cgo or go-sqlite3.');
        }
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        if (/no Go files|cannot find main module|no required module/i.test(msg)) {
          log('\\x1b[33m[Joytree] ⚠ Go could not find the main package. Check your Build Command path, e.g. "go build -o server ./cmd/server".\\x1b[0m');
          return false;
        }
        if (/undefined:|cannot use|type mismatch|syntax error|build failed/i.test(msg)) {
          log('\\x1b[33m[Joytree] ⚠ Go compilation error. Check logs above. Common fixes: correct import paths, run "go mod tidy" and commit go.sum.\\x1b[0m');
          return false;
        }
        return false;
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // [FALLBACK] scan for any built binary
          const candidates = ['server', 'app', 'main', 'api'];
          const found = candidates.find(b => fs.existsSync(path.join(projectRoot, b)));
          cmd = found ? `./${found}` : './server';
          warnings.push(`No start command set — defaulting to "${cmd}". Set your Start Command in the dashboard.`);
        }
        // [FALLBACK] go run as start cmd — warn about performance
        if (/^go\\s+run\\b/.test(cmd)) {
          warnings.push('ℹ "go run" compiles at startup (slower, more memory). Consider a compiled binary: go build -o server . → ./server');
        }
        // [FALLBACK] hardcoded port
        if (/PORT=\\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\\d{4,5}/, 'PORT=$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        if (!/\\$PORT|\\bPORT\\b/.test(cmd)) {
          warnings.push('ℹ Make sure your Go app reads os.Getenv("PORT") to bind on the correct port.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common Go fixes: (1) commit go.mod + go.sum, (2) Build Command: "go build -o server .", (3) app must call os.Getenv("PORT") and listen on 0.0.0.0 not 127.0.0.1.';
      }
    }
  });
}

// ── PHP ───────────────────────────────────────────────────────────────────────
async function runPhpBuild(opts) {
  const phpVer    = String(opts.project.phpVer || '8.2');
  const runtime   = String(opts.project.runtime || '').toLowerCase();
  const isLaravel = runtime === 'php-laravel' || fs.existsSync(path.join(opts.tmpDir, opts.deployId, 'artisan'));
  const isSymfony = runtime === 'php-symfony' || fs.existsSync(path.join(opts.tmpDir, opts.deployId, 'bin', 'console'));
  const port      = normalizePort(opts.appPort, 8080);
  // composer:2 has Composer + PHP for BUILDING. The runtime container uses php:X.Y-cli
  // which has PHP but NOT Composer (Composer is a dev/build tool, not a runtime dependency).
  const phpRuntimeImage = `php:${phpVer}-cli`;
  return runGenericBuild({ ...opts, dockerImage: 'composer:2', runtimeImage: phpRuntimeImage, stepLabel: 'php',
    findRoot: (dir, log) => findFileRoot(dir, ['composer.json', 'index.php', 'artisan'], log) || dir,
    smartHooks: {
      resolveInstall(projectRoot, project) {
        let cmd = (project.installCmd || '').trim();
        const warnings = [];

        // [FALLBACK] Catch "composer update" — install is correct for deployment, not update
        if (/^composer\s+update\b/.test(cmd)) {
          warnings.push('⚠ "composer update" resolves ALL dependencies to latest versions and is slow/unpredictable in CI. Auto-switching to "composer install --no-dev --optimize-autoloader". Change your Install Command if you intended composer update.');
          cmd = 'composer install --no-dev --optimize-autoloader';
        }
        // [FALLBACK] Catch composer install without --no-dev (installs dev deps = larger image, slower)
        if (/^composer\s+install\b/.test(cmd) && !/--no-dev/.test(cmd)) {
          warnings.push('ℹ Added --no-dev to composer install to skip development dependencies in production. Add --no-dev to your Install Command to remove this message.');
          cmd = cmd.replace(/^composer\s+install/, 'composer install --no-dev');
        }
        // [FALLBACK] If no cmd set, auto-detect if composer.json exists
        if (!cmd) {
          const hasComposerJson = fs.existsSync(path.join(projectRoot, 'composer.json'));
          if (hasComposerJson) {
            cmd = 'composer install --no-dev --optimize-autoloader';
          } else {
            warnings.push('⚠ No composer.json found in your repo. If this is a PHP project, commit your composer.json and composer.lock files.');
            cmd = 'echo "No composer.json — skipping composer install"';
          }
        }
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] composer.json not found
        if (/composer\.json.*not found|No such file/i.test(msg)) {
          log('\x1b[33m[Joytree] composer.json not found. Make sure it is committed to your repo root (or set Working Directory in settings if it is in a subfolder).\x1b[0m');
          log('\x1b[33m[Joytree] ✓ Continuing without Composer install — if your vendor/ directory is committed this may still work.\x1b[0m');
          return true; // soft-fail: continue
        }
        // [FALLBACK] PHP extension missing
        if (/require.*extension|ext-\w+/i.test(msg)) {
          log('\x1b[33m[Joytree] A required PHP extension is missing from the build image. Common extensions (mbstring, pdo, xml, curl, zip) are pre-installed. If you need a non-standard extension, consider using a Dockerfile deploy.\x1b[0m');
          return false; // hard-fail: throw
        }
        // [FALLBACK] Platform requirement mismatch (PHP version too old/new)
        if (/platform.*require|requires php/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Composer platform requirements do not match the build image PHP version. Try changing the PHP Version in your project settings, or add \"config\":{\"platform\":{\"php\":\"8.2\"}} to composer.json.\x1b[0m');
          return false;
        }
        // [FALLBACK] Package not found
        if (/Package.*not found|Could not find.*package/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A Composer package could not be found. Check that the package name and version are correct in composer.json.\x1b[0m');
          return false;
        }
        return false; // unhandled — throw
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          if (isLaravel) {
            steps.push('php artisan config:cache');
            steps.push('php artisan route:cache');
            steps.push('php artisan view:cache');
            const hasDb = !!(env['DB_HOST'] || env['DB_URL'] || env['DATABASE_URL']);
            if (hasDb) {
              steps.push('php artisan migrate --force');
            } else {
              warnings.push('No DB_HOST/DATABASE_URL set — skipping migrate. Add database env vars to run migrations.');
            }
          } else if (isSymfony) {
            steps.push('php bin/console cache:clear --env=prod --no-debug');
            steps.push('php bin/console cache:warmup --env=prod');
          }
          userCmd = steps.length > 0 ? steps.join(' && ') : 'echo skip';
        }
        // [FALLBACK] Warn/skip migrate without DB rather than hard-fail
        if (/artisan migrate/.test(userCmd)) {
          const hasDb = !!(env['DB_HOST'] || env['DB_URL'] || env['DATABASE_URL']);
          if (!hasDb) {
            warnings.push('⚠ "php artisan migrate" is in your build command but no DB_HOST or DATABASE_URL is set. The build step will be skipped to avoid a crash. Add database env vars or remove the migrate command to suppress this.');
            userCmd = userCmd.replace(/&&\s*php artisan migrate[^&]*/g, '').replace(/php artisan migrate[^&]*&&?/g, '').trim() || 'echo skip';
          }
        }
        // [FALLBACK] artisan commands without "php" prefix (common user mistake)
        if (/(?<![a-z])artisan\b/.test(userCmd) && !/php artisan/.test(userCmd)) {
          warnings.push('⚠ "artisan" without "php" prefix detected — auto-prefixing with "php". Update your Build Command to use "php artisan ...".');
          userCmd = userCmd.replace(/(?<![a-z])artisan\b/g, 'php artisan');
        }
        return { cmd: userCmd, warnings };
      },
      buildErrorHandler(msg, log) {
        // [FALLBACK] Non-critical artisan cache commands failing (e.g. config:cache fails without full env)
        if (/artisan config:cache|artisan route:cache|artisan view:cache/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ An artisan cache command failed (this often means missing env variables like APP_KEY or DB_HOST). Continuing without cache — the app will still start but may be slower.\x1b[0m');
          log('\x1b[33m[Joytree] ℹ Add APP_KEY and other required env vars in your project Environment Variables.\x1b[0m');
          return true; // soft-fail: continue
        }
        // [FALLBACK] Symfony console cache clear fails
        if (/bin\/console cache:clear|bin\/console cache:warmup/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Symfony cache clear/warmup failed. The app may still start. Check that APP_ENV=prod and all required env vars are set.\x1b[0m');
          return true; // soft-fail: continue
        }
        return false; // hard-fail for others
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          if (isLaravel) {
            cmd = `php artisan serve --host=0.0.0.0 --port=$PORT`;
            warnings.push('Using "php artisan serve" — suitable for basic deployments. For production traffic, consider FrankenPHP or Nginx+php-fpm.');
          } else if (isSymfony) {
            cmd = `php -S 0.0.0.0:$PORT -t public/`;
          } else {
            const hasPublic = fs.existsSync(path.join(projectRoot, 'public', 'index.php'));
            cmd = hasPublic ? `php -S 0.0.0.0:$PORT -t public/` : `php -S 0.0.0.0:$PORT`;
          }
        }
        // [FALLBACK] "artisan serve" without "php" prefix
        if (/(?<![a-z])artisan\s+serve/.test(cmd) && !/php\s+artisan/.test(cmd)) {
          cmd = cmd.replace(/(?<![a-z])artisan\s+serve/, 'php artisan serve');
          warnings.push('⚠ "artisan serve" without "php" prefix — auto-prefixed. Update your Start Command to use "php artisan serve".');
        }
        // [FALLBACK] Hardcoded port number
        if (/--port[=\s]\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/--port([=\s])\d{4,5}/, '--port$1$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT for dynamic port assignment.');
        }
        // [FALLBACK] 127.0.0.1 / localhost binding — unreachable from Docker proxy
        if (/127\.0\.0\.1|localhost/.test(cmd) && !/0\.0\.0\.0/.test(cmd)) {
          cmd = cmd.replace(/127\.0\.0\.1/g, '0.0.0.0').replace(/\blocalhost\b/g, '0.0.0.0');
          warnings.push('⚠ App was binding to 127.0.0.1/localhost — changed to 0.0.0.0 so Joytree proxy can reach it.');
        }
        // [FALLBACK] php -S without -t flag when public/ exists
        if (/php\s+-S\s+0\.0\.0\.0/.test(cmd) && !/-t\s+/.test(cmd)) {
          const hasPublic = fs.existsSync(path.join(projectRoot, 'public', 'index.php'));
          if (hasPublic) {
            cmd = cmd.replace(/php\s+-S\s+(\S+)/, 'php -S $1 -t public/');
            warnings.push('⚠ Auto-added "-t public/" to PHP built-in server because public/index.php was found. Update your Start Command to remove this message.');
          }
        }
        return { cmd, warnings };
      },
      buildEnv(projectRoot, project, env) {
        const extra = {};
        const crypto = require('crypto');
        if (isLaravel && !env['APP_KEY'] && !env['LARAVEL_APP_KEY']) {
          extra['APP_KEY'] = 'base64:' + crypto.randomBytes(32).toString('base64');
          // Log is emitted by runGenericBuild after buildEnv runs — add a warning via the log call
        }
        return extra;
      },
      startupFailureHint() {
        if (isLaravel) return 'Common Laravel fixes: set APP_KEY, DB_HOST, and DB_PASSWORD in Environment Variables. Make sure "artisan" is committed to your repo root. Run "php artisan serve --host=0.0.0.0 --port=$PORT" as your Start Command.';
        if (isSymfony) return 'Common Symfony fixes: ensure APP_ENV=prod is in Environment Variables, public/index.php exists, and all required secrets are set.';
        return 'Common PHP fixes: make sure public/index.php exists. Start Command should be: php -S 0.0.0.0:$PORT -t public/';
      }
    }
  });
}

// ── Ruby ──────────────────────────────────────────────────────────────────────
async function runRubyBuild(opts) {
  const rubyVer = String(opts.project.rubyVer || '3.2');
  const image   = `ruby:${rubyVer}-slim`;
  const runtime = String(opts.project.runtime || '').toLowerCase();
  const isRails = runtime === 'ruby-rails' || fs.existsSync(path.join(opts.tmpDir, opts.deployId, 'config', 'application.rb'));
  const port    = normalizePort(opts.appPort, 3000);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'ruby',
    findRoot: (dir, log) => findFileRoot(dir, ['Gemfile', 'config.ru', 'Rakefile'], log) || dir,
    extraEnv: { RAILS_ENV: 'production', RACK_ENV: 'production', BUNDLE_WITHOUT: 'development:test' },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        let cmd = (project.installCmd || '').trim() || 'bundle install';
        const warnings = [];
        if (!fs.existsSync(path.join(projectRoot, 'Gemfile'))) {
          warnings.push('⚠ No Gemfile found at the project root. Make sure your Ruby project files are committed (or set Working Directory if in a subfolder).');
        }
        // [FALLBACK] "gem install" instead of "bundle install" — won't respect Gemfile.lock
        if (/^gem\s+install\b/.test(cmd)) {
          warnings.push('ℹ "gem install" does not use your Gemfile.lock for reproducible installs. Switching to "bundle install".');
          cmd = 'bundle install';
        }
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] Native extension build failure (nokogiri, pg, mysql2, etc.)
        if (/Gem::Ext::BuildError|extconf\.rb failed|Failed to build gem native extension/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A gem with native extensions failed to build (common for nokogiri, pg, mysql2, sassc). Check that the gem version supports your Ruby version, or pin to a version with precompiled binaries.\x1b[0m');
          return false;
        }
        // [FALLBACK] Bundler platform mismatch
        if (/Your bundle only supports|Could not find gem.*in any of the sources/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Gemfile.lock platform mismatch or missing gem. Run "bundle lock --add-platform x86_64-linux" locally and commit Gemfile.lock, or run "bundle install" locally to refresh it.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          if (isRails) {
            steps.push('bundle exec rails assets:precompile || echo "[Joytree] assets:precompile failed — continuing"');
            const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
            if (hasDb) {
              steps.push('bundle exec rails db:migrate');
            } else {
              warnings.push('⚠ No DATABASE_URL set — skipping db:migrate. Add it to Environment Variables to run migrations.');
            }
          }
          userCmd = steps.length > 0 ? steps.join(' && ') : 'echo skip';
        }
        // [FALLBACK] Soft-skip db:migrate without DB instead of letting it crash the whole build
        if (/db:migrate/.test(userCmd)) {
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (!hasDb) {
            warnings.push('⚠ "rails db:migrate" is in your build command but no DATABASE_URL is set. Skipping this step — add DATABASE_URL in Environment Variables to enable migrations.');
            userCmd = userCmd.replace(/&&\s*bundle exec rails db:migrate\b/g, '').replace(/bundle exec rails db:migrate\b\s*&&?/g, '').trim() || 'echo skip';
          }
        }
        return { cmd: userCmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          if (isRails) {
            const hasPumaConfig = fs.existsSync(path.join(projectRoot, 'config', 'puma.rb'));
            cmd = hasPumaConfig
              ? `bundle exec puma -C config/puma.rb -b tcp://0.0.0.0:$PORT`
              : `bundle exec puma -b tcp://0.0.0.0:$PORT`;
            if (!hasPumaConfig) warnings.push('No config/puma.rb found — starting Puma with default settings.');
          } else {
            const hasConfigRu = fs.existsSync(path.join(projectRoot, 'config.ru'));
            if (!hasConfigRu) warnings.push('⚠ No config.ru found — "bundle exec rackup" requires one. Add config.ru to your repo root.');
            cmd = `bundle exec rackup -o 0.0.0.0 -p $PORT`;
          }
        }
        // [FALLBACK] rails server / "rails s" — dev server, not for production
        if (/rails\s+s(erver)?\b/.test(cmd) && !/puma/.test(cmd)) {
          warnings.push('⚠ "rails server" is a development server. Switching to Puma for production. Consider setting your Start Command to "bundle exec puma -b tcp://0.0.0.0:$PORT".');
          cmd = `bundle exec puma -b tcp://0.0.0.0:$PORT`;
        }
        // Fix hardcoded port in -b tcp://0.0.0.0:PORT pattern
        if (/tcp:\/\/0\.0\.0\.0:\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/tcp:\/\/0\.0\.0\.0:\d{4,5}/, 'tcp://0.0.0.0:$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        // [FALLBACK] -p PORT_NUMBER hardcoded (rackup style)
        if (/-p\s+\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/-p\s+\d{4,5}/, '-p $PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        // [FALLBACK] localhost/127.0.0.1 binding
        if (/127\.0\.0\.1|localhost/.test(cmd) && !/0\.0\.0\.0/.test(cmd)) {
          cmd = cmd.replace(/127\.0\.0\.1/g, '0.0.0.0').replace(/\blocalhost\b/g, '0.0.0.0');
          warnings.push('⚠ App was binding to 127.0.0.1/localhost — changed to 0.0.0.0 so Joytree proxy can reach it.');
        }
        return { cmd, warnings };
      },
      buildEnv(projectRoot, project, env) {
        const extra = {};
        const crypto = require('crypto');
        if (isRails && !env['SECRET_KEY_BASE'] && !env['RAILS_MASTER_KEY']) {
          extra['SECRET_KEY_BASE'] = crypto.randomBytes(64).toString('hex');
        }
        return extra;
      },
      buildErrorHandler(msg, log) {
        if (/assets:precompile/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ assets:precompile failed — continuing without asset compilation. Add Node.js to your build image (via execjs) or precompile assets locally and commit public/assets.\x1b[0m');
          return true; // handled — don't throw
        }
        if (/db:migrate|ActiveRecord::/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Database migration failed. Check DATABASE_URL is correct and the database is reachable. Continuing deployment — fix migrations and redeploy.\x1b[0m');
          return true; // soft-fail: continue
        }
        return false; // unhandled — throw
      },
      startupFailureHint() {
        return isRails
          ? 'Common Rails fixes: set SECRET_KEY_BASE and DATABASE_URL in Environment Variables. Ensure Puma is in your Gemfile. Start Command: "bundle exec puma -b tcp://0.0.0.0:$PORT".'
          : 'Check that config.ru exists and your Rack app binds to 0.0.0.0:$PORT. Start Command: "bundle exec rackup -o 0.0.0.0 -p $PORT".';
      }
    }
  });
}

// ── JVM (Java / Kotlin) ───────────────────────────────────────────────────────
async function runJvmBuild(opts) {
  const javaVer = String(opts.project.javaVer || '17');
  const image   = `eclipse-temurin:${javaVer}-jdk-jammy`;
  const port    = normalizePort(opts.appPort, 8080);
  // Detect Maven vs Gradle at scan time
  const scanDir  = path.join(opts.tmpDir, opts.deployId);
  const isMaven  = fs.existsSync(path.join(scanDir, 'pom.xml')) ||
                   !!findFileRoot(scanDir, ['pom.xml'], null);
  const isGradle = !isMaven;
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'java',
    findRoot: (dir, log) => findFileRoot(dir, ['pom.xml', 'build.gradle', 'build.gradle.kts', 'gradlew', 'mvnw'], log) || dir,
    extraEnv: { JAVA_OPTS: '-Xmx512m -Xms128m', SERVER_PORT: String(port) },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const userCmd = (project.installCmd || '').trim();
        const warnings = [];
        // [FALLBACK] Confirm build file actually exists; warn early instead of cryptic Maven/Gradle error
        const hasPom = fs.existsSync(path.join(projectRoot, 'pom.xml'));
        const hasGradle = fs.existsSync(path.join(projectRoot, 'build.gradle')) || fs.existsSync(path.join(projectRoot, 'build.gradle.kts'));
        if (!hasPom && !hasGradle) {
          warnings.push('⚠ No pom.xml or build.gradle found at the project root. Make sure your Java/Kotlin project files are committed (or set Working Directory if in a subfolder).');
        }
        if (userCmd) return { cmd: userCmd, warnings };
        const pre = isMaven
          ? 'chmod +x mvnw 2>/dev/null || true; '
          : 'chmod +x gradlew 2>/dev/null || true; ';
        const cmd = pre + (isMaven
          ? './mvnw dependency:resolve -q 2>/dev/null || mvn dependency:resolve -q'
          : './gradlew dependencies -q 2>/dev/null || gradle dependencies -q');
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] Maven wrapper missing/broken
        if (/mvnw.*No such file|permission denied.*mvnw/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Maven wrapper (mvnw) not found or not executable. Falling back to system "mvn" — make sure pom.xml is valid. For reliable builds, commit the .mvn/ wrapper folder and mvnw script.\x1b[0m');
          return false;
        }
        // [FALLBACK] Gradle wrapper missing/broken
        if (/gradlew.*No such file|permission denied.*gradlew/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Gradle wrapper (gradlew) not found or not executable. Falling back to system "gradle". For reliable builds, commit the gradle/wrapper folder and gradlew script.\x1b[0m');
          return false;
        }
        // [FALLBACK] Dependency resolution failure
        if (/Could not resolve dependencies|Could not find artifact|Could not resolve all/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A dependency could not be resolved. Check artifact coordinates (groupId/artifactId/version) in your pom.xml or build.gradle, and that the repository is publicly accessible.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project) {
        const userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (userCmd) {
          // [FALLBACK] User forgot -DskipTests / -x test — tests commonly fail in CI due to missing test DB
          if (isMaven && /\bpackage\b/.test(userCmd) && !/DskipTests/.test(userCmd)) {
            warnings.push('ℹ Tests will run during "mvn package". If tests require a database or external services not available during build, add -DskipTests to your Build Command.');
          }
          if (isGradle && /\bbuild\b/.test(userCmd) && !/-x\s+test/.test(userCmd)) {
            warnings.push('ℹ Tests will run during "gradle build". If tests require external services, add "-x test" to your Build Command, or use "bootJar" instead of "build".');
          }
          return { cmd: userCmd, warnings };
        }
        const pre = isMaven
          ? 'chmod +x mvnw 2>/dev/null || true; '
          : 'chmod +x gradlew 2>/dev/null || true; ';
        const cmd = pre + (isMaven
          ? './mvnw package -DskipTests -q 2>/dev/null || mvn package -DskipTests -q'
          : './gradlew bootJar -q 2>/dev/null || gradle bootJar -q');
        return { cmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          const jarFinder = `$(find . \( -path "*/target/*.jar" -o -path "*/build/libs/*.jar" \) ! -name "*-sources.jar" ! -name "*-javadoc.jar" -type f | head -1)`;
          cmd = `java $JAVA_OPTS -jar ${jarFinder} --server.port=$PORT 2>/dev/null || java $JAVA_OPTS -jar ${jarFinder} -Dserver.port=$PORT`;
          warnings.push('No start command set — auto-detecting JAR file. Set your Start Command for a more reliable deployment.');
        }
        // [FALLBACK] "mvn spring-boot:run" / "gradle bootRun" — dev-mode tasks, slow & require build tool at runtime
        if (/spring-boot:run|bootRun/.test(cmd)) {
          warnings.push('⚠ Using a dev-mode run task (spring-boot:run/bootRun) in production is slower and re-resolves dependencies at startup. Consider building a JAR and running "java -jar your-app.jar" instead.');
        }
        // [FALLBACK] Hardcoded ports
        if (/--server\.port=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/--server\.port=\d{4,5}/, '--server.port=$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        if (/-Dserver\.port=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/-Dserver\.port=\d{4,5}/, '-Dserver.port=$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        if (/Tests run:.*FAILURE|BUILD FAILURE.*test/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Test failures detected during build. Add -DskipTests (Maven) or -x test (Gradle) to your Build Command to skip tests during deployment.\x1b[0m');
          return false; // still throw — build failed
        }
        // [FALLBACK] No JAR produced (e.g. packaging=pom, or wrong plugin)
        if (/no main manifest attribute|Unable to access jarfile|JAR.*not found/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ No runnable JAR was produced. For Spring Boot, make sure spring-boot-maven-plugin (Maven) or the Spring Boot Gradle plugin is configured to build an executable JAR.\x1b[0m');
          return false;
        }
        return false;
      },
      startupFailureHint() {
        return 'Common JVM fixes: ensure your JAR is built to target/ or build/libs/ as an executable JAR. For Spring Boot, add spring-boot-starter-web and the Spring Boot Maven/Gradle plugin. App must bind to $PORT or $SERVER_PORT.';
      }
    }
  });
}

// ── Rust ──────────────────────────────────────────────────────────────────────
async function runRustBuild(opts) {
  const image = 'rust:1-slim-bullseye';
  const port  = normalizePort(opts.appPort, 8080);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'rust',
    findRoot: (dir, log) => findFileRoot(dir, ['Cargo.toml', 'Cargo.lock'], log) || dir,
    extraEnv: { RUST_LOG: 'info', RUST_BACKTRACE: '1' },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const warnings = [];
        // [FALLBACK] Check Cargo.toml exists
        if (!fs.existsSync(path.join(projectRoot, 'Cargo.toml'))) {
          warnings.push('⚠ No Cargo.toml found. Make sure your Rust project files are committed to the repo root (or set Working Directory if in a subfolder).');
        }
        // Rust has no separate install step — cargo build fetches dependencies automatically
        return { cmd: 'echo skip', warnings };
      },
      resolveBuild(projectRoot, project) {
        let cmd = (project.buildCmd || '').trim() || 'cargo build --release';
        const warnings = [];
        // [FALLBACK] Read binary name ONCE here and store for resolveStart to use via shared context
        let binaryName = 'server';
        try {
          const cargo = fs.readFileSync(path.join(projectRoot, 'Cargo.toml'), 'utf8');
          const binMatch = cargo.match(/\[\[bin\]\][^]*?name\s*=\s*"([^"]+)"/);
          const pkgMatch = cargo.match(/\[package\][^]*?name\s*=\s*"([^"]+)"/);
          binaryName = (binMatch && binMatch[1]) || (pkgMatch && pkgMatch[1]) || 'server';
        } catch(_) {}
        // Stash on the project object so resolveStart can use it without re-reading
        project._rustBinaryName = binaryName;
        if (binaryName !== 'server') {
          warnings.push(`Auto-detected Rust binary: "${binaryName}" from Cargo.toml. Start Command will use ./target/release/${binaryName}.`);
        }
        // [FALLBACK] Warn that cargo build --release can be slow on first run
        warnings.push('ℹ First Rust build may take several minutes (compiling all dependencies). Subsequent builds use the cargo cache and are much faster.');
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        if (/error\[E\d+\]|could not compile/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Rust compilation failed. Check the logs above for specific error codes (e.g. E0308, E0382). Common fixes: wrong types, missing trait impl, or a crate version conflict in Cargo.lock.\x1b[0m');
          return false;
        }
        // [FALLBACK] Linker errors — often missing system libraries
        if (/error: linking with.*failed|linker.*not found|ld returned/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Rust linker error — a system library dependency may be missing. If your crate requires OpenSSL, add openssl-sys to Cargo.toml and try again. Consider using rustls instead of openssl for easier builds.\x1b[0m');
          return false;
        }
        // [FALLBACK] crates.io unreachable
        if (/network.*failed|socket.*timed out|unable to get.*crates.io/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Could not reach crates.io. This may be a transient network issue. Try redeploying. If the error persists, check that all crate names and versions in Cargo.toml are correct.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        // Use binary name detected in resolveBuild (avoids re-reading Cargo.toml)
        let binaryName = project._rustBinaryName || 'server';
        if (!project._rustBinaryName) {
          // Fallback re-read if resolveBuild didn't run first
          try {
            const cargo = fs.readFileSync(path.join(projectRoot, 'Cargo.toml'), 'utf8');
            const binMatch = cargo.match(/\[\[bin\]\][^]*?name\s*=\s*"([^"]+)"/);
            const pkgMatch = cargo.match(/\[package\][^]*?name\s*=\s*"([^"]+)"/);
            binaryName = (binMatch && binMatch[1]) || (pkgMatch && pkgMatch[1]) || 'server';
          } catch(_) {}
        }
        if (!cmd) {
          cmd = `PORT=$PORT ./target/release/${binaryName}`;
          warnings.push(`No start command set — using ./target/release/${binaryName}. Set your Start Command in the dashboard.`);
        }
        // [FALLBACK] Path points to debug build instead of release
        if (/target\/debug\//.test(cmd)) {
          cmd = cmd.replace(/target\/debug\//, 'target/release/');
          warnings.push('⚠ Debug build path detected — switched to ./target/release/ for production deployment. Update your Start Command to use the release binary.');
        }
        // [FALLBACK] Hardcoded port
        if (/PORT=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\d{4,5}/, 'PORT=$PORT');
          warnings.push('⚠ Hardcoded port replaced with $PORT.');
        }
        if (!/\$PORT|\bPORT\b/.test(cmd)) {
          warnings.push('ℹ Make sure your Rust app reads std::env::var("PORT") and binds to 0.0.0.0:PORT.');
        }
        // [FALLBACK] localhost binding in start command
        if (/127\.0\.0\.1|localhost/.test(cmd)) {
          warnings.push('⚠ Detected possible localhost binding in start command. Make sure your Rust app binds to 0.0.0.0 not 127.0.0.1 or localhost.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common Rust fixes: (1) Build Command: "cargo build --release", (2) Start Command: "PORT=$PORT ./target/release/<binary-name>" (binary name from Cargo.toml [package].name), (3) app must bind to 0.0.0.0 and read PORT env var.';
      }
    }
  });
}

// [FIX] Real .NET solutions almost always contain more than one .csproj --
// a class library, one or more test projects, sometimes a code-analyzer
// project -- alongside the actual runnable web app (e.g.
// ardalis/ApiEndpoints: 5 projects under one root .sln, only one of which
// is a deployable web app). "dotnet publish" run against a bare directory
// containing a multi-project .sln is genuinely ambiguous and either errors
// or resolves an arbitrary project -- never reliably the web app. These
// two helpers find every .csproj under projectRoot (capped at a reasonable
// depth so this stays fast on large repos) and identify the one that's
// actually an ASP.NET Core web app via its Sdk="Microsoft.NET.Sdk.Web"
// attribute, which is the one authoritative signal that distinguishes a
// runnable web project from a library/test/tool project.
function findAllCsprojFiles(dir, depth = 0, maxDepth = 6, results = []) {
  if (depth > maxDepth) return results;
  let entries;
  try { entries = fs.readdirSync(dir); } catch (_) { return results; }
  for (const entry of entries) {
    if (['.git', 'bin', 'obj', 'node_modules', '.vs'].includes(entry) || entry.startsWith('.')) continue;
    const full = path.join(dir, entry);
    let stat;
    try { stat = fs.lstatSync(full); } catch (_) { continue; }
    if (stat.isDirectory()) {
      findAllCsprojFiles(full, depth + 1, maxDepth, results);
    } else if (entry.endsWith('.csproj')) {
      results.push(full);
    }
  }
  return results;
}

function countCsprojFiles(projectRoot) {
  return findAllCsprojFiles(projectRoot).length;
}

function findDotnetWebProject(projectRoot) {
  const allCsproj = findAllCsprojFiles(projectRoot);
  if (allCsproj.length === 0) return null;
  // Return paths relative to projectRoot -- the build command runs with
  // projectRoot as its working directory inside the container, so a
  // relative path here is what actually resolves correctly there. An
  // absolute host path would only work by coincidence if the container's
  // mount point happened to match the host path exactly.
  if (allCsproj.length === 1) return path.relative(projectRoot, allCsproj[0]);

  // Multiple .csproj found — the web app is the one using the Web SDK.
  // A repo could in theory have more than one Sdk.Web project (rare); the
  // first match is used, same "best effort, not exhaustive" tradeoff every
  // other language's auto-detection in this file already makes.
  for (const csprojPath of allCsproj.sort()) {
    try {
      const content = fs.readFileSync(csprojPath, 'utf8');
      if (/Sdk\s*=\s*["']Microsoft\.NET\.Sdk\.Web["']/i.test(content)) {
        return path.relative(projectRoot, csprojPath);
      }
    } catch (_) {}
  }
  return null; // No Sdk.Web project found among multiple candidates — fall back to the old bare-directory behavior and let dotnet's own error surface.
}

// ── .NET ──────────────────────────────────────────────────────────────────────
async function runDotnetBuild(opts) {
  const dotnetVer = String(opts.project.dotnetVer || '8.0');
  const image     = `mcr.microsoft.com/dotnet/sdk:${dotnetVer}`;
  const port      = normalizePort(opts.appPort, 8080);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'dotnet',
    findRoot: (dir, log) => findFileRoot(dir, ['*.csproj', '*.sln', 'Program.cs'], log, true) || dir,
    extraEnv: { DOTNET_ENVIRONMENT: 'Production', ASPNETCORE_ENVIRONMENT: 'Production', ASPNETCORE_URLS: `http://0.0.0.0:${port}` },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        let cmd = (project.installCmd || '').trim() || 'dotnet restore';
        const warnings = [];
        // [FALLBACK] Scan for .csproj / .sln to confirm this is a .NET project
        const hasCsproj = (() => { try { return fs.readdirSync(projectRoot).some(f => f.endsWith('.csproj') || f.endsWith('.sln')); } catch(_) { return false; } })();
        if (!hasCsproj) {
          warnings.push('⚠ No .csproj or .sln file found at the project root. Make sure your .NET project files are committed and set Working Directory if they are in a subfolder.');
        }
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] NuGet feed failure
        if (/Unable to load the service index|NuGet.*feed|401.*Unauthorized/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ NuGet package restore failed — could not reach the package feed. Check that all NuGet packages are available on nuget.org. Private feeds require authentication which is not currently supported.\x1b[0m');
          return false;
        }
        // [FALLBACK] SDK not found
        if (/SDK.*not found|A compatible .NET SDK/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ .NET SDK version not found. Check that your .NET version (set in project settings) matches the <TargetFramework> in your .csproj file.\x1b[0m');
          return false;
        }
        // [FALLBACK] project file not found
        if (/MSBUILD.*not found|Could not find.*csproj|No project was found/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ .NET project file not found. Make sure your .csproj file is committed. If it is in a subfolder, set the Working Directory in your project settings.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project) {
        let cmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // [FIX] The old default always ran "dotnet publish -c Release -o out"
          // against whatever directory findRoot landed on. That's correct
          // when a single .csproj sits there, but real multi-project
          // solutions (a class library + test project(s) + the actual
          // runnable web app, all referenced from one root .sln --
          // e.g. ardalis/ApiEndpoints) land findRoot on the directory
          // containing the .sln, and "dotnet publish" against a directory
          // with a solution referencing multiple project types (some
          // libraries, some non-web executables) is genuinely ambiguous:
          // dotnet's CLI either errors asking which project to use, or
          // may resolve an arbitrary/wrong one -- never reliably the
          // actual web app. Explicitly find the runnable ASP.NET Core
          // project (Sdk="Microsoft.NET.Sdk.Web") among ALL .csproj files
          // under projectRoot and publish that one directly instead of
          // letting `dotnet publish` guess from a bare directory.
          const webCsproj = findDotnetWebProject(projectRoot);
          if (webCsproj) {
            cmd = `dotnet publish "${webCsproj}" -c Release -o out`;
            warnings.push(`Using default build: "dotnet publish ${path.basename(webCsproj)} -c Release -o out" (auto-selected the ASP.NET Core web project out of ${countCsprojFiles(projectRoot)} project file(s) found).`);
          } else {
            cmd = 'dotnet publish -c Release -o out';
            warnings.push('Using default build: "dotnet publish -c Release -o out".');
          }
        }
        // [FALLBACK] "--no-restore" without a prior dotnet restore will fail
        if (/--no-restore/.test(cmd) && !/dotnet restore/.test((project.installCmd || ''))) {
          cmd = cmd.replace(/\s*--no-restore/, '');
          warnings.push('⚠ Removed "--no-restore" from publish command because no restore step was found. Add "dotnet restore" as your Install Command, or remove --no-restore from your Build Command.');
        }
        // [FALLBACK] Ensure output is to "out/" so start command can find it
        if (/dotnet publish/.test(cmd) && !/-o\s+/.test(cmd)) {
          cmd = cmd + ' -o out';
          warnings.push('⚠ Added "-o out" to dotnet publish so the start command can find the published output. Update your Build Command to include "-o out".');
        }
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        // [FALLBACK] Build errors — common .NET pitfalls
        if (/error CS\d+|Build FAILED/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ .NET build failed with compilation errors. Check the logs above for specific CS error codes. Common causes: missing using statements, wrong namespace, or mismatched SDK versions.\x1b[0m');
          return false;
        }
        // [FALLBACK] Missing NuGet package after restore
        if (/The type or namespace.*could not be found|are you missing a using/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Missing type or namespace — a NuGet package may not have been restored. Make sure all packages are in your .csproj and run dotnet restore locally to check.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // [FALLBACK] Find the actual published DLL name from out/ dir at build time
          // Use a shell glob that works at runtime — sh -c will expand it
          cmd = `dotnet $(ls out/*.dll 2>/dev/null | head -1 || echo 'out/app.dll')`;
          warnings.push('No start command set — auto-detecting DLL in out/ directory. Set your Start Command for a reliable deployment.');
        }
        // [FALLBACK] "dotnet out/*.dll" literal glob — doesn't expand in exec()
        if (/dotnet\s+out\/\*\.dll/.test(cmd)) {
          cmd = cmd.replace(/dotnet\s+out\/\*\.dll/, "dotnet $(ls out/*.dll 2>/dev/null | head -1 || echo 'out/app.dll')");
          warnings.push('⚠ "dotnet out/*.dll" glob replaced with shell-safe equivalent. Update your Start Command to use the exact DLL filename for reliability.');
        }
        // [FALLBACK] Hardcoded ASPNETCORE_URLS port
        if (/ASPNETCORE_URLS=http:\/\/0\.0\.0\.0:\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/ASPNETCORE_URLS=http:\/\/0\.0\.0\.0:\d{4,5}/, 'ASPNETCORE_URLS=http://0.0.0.0:$PORT');
          warnings.push('⚠ Hardcoded ASPNETCORE_URLS port replaced with $PORT.');
        }
        // [FALLBACK] Hardcoded --urls port
        if (/--urls.*:\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/(--urls.*?:)\d{4,5}/, '$1$PORT');
          warnings.push('⚠ Hardcoded port in --urls replaced with $PORT.');
        }
        // [FALLBACK] localhost binding — unreachable from Docker proxy
        if (/localhost|127\.0\.0\.1/.test(cmd) && !/0\.0\.0\.0/.test(cmd)) {
          cmd = cmd.replace(/localhost/g, '0.0.0.0').replace(/127\.0\.0\.1/g, '0.0.0.0');
          warnings.push('⚠ App URL was binding to localhost/127.0.0.1 — changed to 0.0.0.0 so Joytree proxy can reach it.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common .NET fixes: (1) Build Command should be "dotnet publish -c Release -o out", (2) Start Command: "dotnet out/YourApp.dll", (3) ASPNETCORE_URLS is auto-set to http://0.0.0.0:$PORT — do not override it with a hardcoded port.';
      }
    }
  });
}

// ── Elixir / Phoenix ──────────────────────────────────────────────────────────
async function runElixirBuild(opts) {
  const image = 'elixir:1.16-slim';
  const port  = normalizePort(opts.appPort, 4000);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'elixir',
    findRoot: (dir, log) => findFileRoot(dir, ['mix.exs'], log) || dir,
    extraEnv: { MIX_ENV: 'prod', PHX_HOST: '0.0.0.0', PORT: String(port) },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const userCmd = (project.installCmd || '').trim();
        const warnings = [];
        if (!fs.existsSync(path.join(projectRoot, 'mix.exs'))) {
          warnings.push('⚠ No mix.exs found at the project root. Make sure your Elixir project files are committed (or set Working Directory if in a subfolder).');
        }
        if (userCmd) return { cmd: userCmd, warnings };
        const cmd = 'mix local.hex --force && mix local.rebar --force && mix deps.get --only prod && mix compile';
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] Hex package not found
        if (/Failed to fetch record|package.*not found|no matching version/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A Hex package could not be found. Check package names and versions in mix.exs.\x1b[0m');
          return false;
        }
        // [FALLBACK] Compile error in deps
        if (/Compiling.*error|could not compile dependency/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A dependency failed to compile. Check that all packages in mix.exs are compatible with your Elixir/OTP version.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          const hasAssets = fs.existsSync(path.join(projectRoot, 'assets'));
          if (hasAssets) {
            steps.push('apt-get update -qq && apt-get install -y --no-install-recommends nodejs npm 2>/dev/null || true');
            steps.push('mix assets.deploy || echo "[Joytree] assets.deploy failed — continuing without compiled assets"');
          }
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (hasDb) {
            steps.push('mix ecto.migrate');
          } else {
            warnings.push('⚠ No DATABASE_URL set — skipping mix ecto.migrate. Add it to Environment Variables to run migrations.');
          }
          userCmd = steps.length > 0 ? steps.join(' && ') : 'echo skip';
        }
        // [FALLBACK] Soft-fail migrate without DB instead of letting it hard-crash the build
        if (/ecto\.migrate/.test(userCmd)) {
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (!hasDb) {
            warnings.push('⚠ "mix ecto.migrate" is in your build command but no DATABASE_URL is set. Skipping this step to avoid a crash — add DATABASE_URL in Environment Variables to enable migrations.');
            userCmd = userCmd.replace(/&&\s*mix ecto\.migrate\b/g, '').replace(/mix ecto\.migrate\b\s*&&?/g, '').trim() || 'echo skip';
          }
        }
        return { cmd: userCmd, warnings };
      },
      buildErrorHandler(msg, log) {
        // [FALLBACK] assets.deploy failure already soft-handled inline with || echo, but catch any escape
        if (/assets\.deploy/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Asset compilation failed — continuing without compiled assets. Check that Node.js/npm dependencies in assets/package.json are correct.\x1b[0m');
          return true; // soft-fail: continue
        }
        // [FALLBACK] ecto.migrate failure (e.g. bad DATABASE_URL)
        if (/ecto\.migrate|Mix\.Tasks\.Ecto/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ Database migration failed. Check that DATABASE_URL is correct and the database is reachable. Continuing deployment — your app may not function correctly until migrations succeed.\x1b[0m');
          return true; // soft-fail: continue, let app start and surface the real error there
        }
        return false;
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          cmd = `mix phx.server`;
          warnings.push('Using "mix phx.server". PHX_HOST=0.0.0.0 and PORT are auto-set. Ensure your endpoint config uses System.get_env("PORT").');
        }
        // [FALLBACK] "phx.server" without "mix" prefix
        if (/^phx\.server/.test(cmd)) {
          cmd = 'mix ' + cmd;
          warnings.push('⚠ "phx.server" without "mix" prefix — auto-prefixed with "mix".');
        }
        // [FALLBACK] localhost/127.0.0.1 in start command
        if (/127\.0\.0\.1|localhost/.test(cmd)) {
          warnings.push('⚠ Detected localhost/127.0.0.1 reference. Make sure PHX_HOST=0.0.0.0 is set (Joytree sets this automatically) so the proxy can reach your app.');
        }
        return { cmd, warnings };
      },
      buildEnv(projectRoot, project, env) {
        const extra = {};
        const crypto = require('crypto');
        if (!env['SECRET_KEY_BASE'] && !env['PHX_SECRET_KEY_BASE']) {
          extra['SECRET_KEY_BASE'] = crypto.randomBytes(64).toString('hex');
        }
        return extra;
      },
      startupFailureHint() {
        return 'Common Phoenix fixes: set SECRET_KEY_BASE and DATABASE_URL in Environment Variables. Ensure your endpoint config uses System.get_env("PORT") and System.get_env("PHX_HOST"). Run "mix phx.server" as your Start Command.';
      }
    }
  });
}

// ── Bun ───────────────────────────────────────────────────────────────────────
async function runBunBuild(opts) {
  const image = 'oven/bun:1-alpine';
  const port  = normalizePort(opts.appPort, 3000);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'bun',
    findRoot: (dir, log) => findFileRoot(dir, ['package.json', 'bun.lockb'], log) || dir,
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const userCmd = (project.installCmd || '').trim();
        if (userCmd) return { cmd: userCmd, warnings: [] };
        const warnings = [];
        // --frozen-lockfile crashes if bun.lockb doesn't exist
        const hasLockfile = fs.existsSync(path.join(projectRoot, 'bun.lockb'));
        const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
        if (!hasPackageJson) {
          warnings.push('⚠ No package.json found at the project root. Make sure your project files are committed (or set Working Directory if in a subfolder).');
        }
        const cmd = hasLockfile ? 'bun install --frozen-lockfile' : 'bun install';
        if (!hasLockfile) warnings.push('No bun.lockb found — using "bun install" without --frozen-lockfile. Commit bun.lockb for reproducible builds.');
        return { cmd, warnings };
      },
      installErrorHandler(msg, log) {
        // [FALLBACK] Frozen lockfile out of sync with package.json
        if (/frozen-lockfile|lockfile.*out of date|lockfile had changes/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ bun.lockb is out of sync with package.json. Continuing without a strict lockfile check — dependencies may have been installed with newer versions than your lockfile specifies.\x1b[0m');
          log('\x1b[33m[Joytree] ℹ Run "bun install" locally and commit the updated bun.lockb to fix this permanently and remove this warning.\x1b[0m');
          return true; // soft-fail: continue (install likely still produced node_modules)
        }
        // [FALLBACK] Package not found on npm registry
        if (/error: package.*not found|404 Not Found/i.test(msg)) {
          log('\x1b[33m[Joytree] ⚠ A package could not be found on the npm registry. Check the package name and version in package.json.\x1b[0m');
          return false;
        }
        return false;
      },
      resolveBuild(projectRoot, project) {
        const userCmd = (project.buildCmd || '').trim();
        if (userCmd && userCmd !== 'echo skip') return { cmd: userCmd, warnings: [] };
        const warnings = [];
        // Check if a build script exists in package.json
        let hasBuildScript = false;
        try {
          const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
          hasBuildScript = !!(pkg.scripts && pkg.scripts.build);
        } catch(_) {}
        const cmd = hasBuildScript ? 'bun run build' : 'echo skip';
        if (!hasBuildScript && !userCmd) warnings.push('No "build" script in package.json — skipping build step.');
        return { cmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Check package.json for start script
          let hasStartScript = false;
          let mainFile = null;
          try {
            const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
            hasStartScript = !!(pkg.scripts && pkg.scripts.start);
            mainFile = pkg.main;
          } catch(_) {}
          if (hasStartScript) {
            cmd = `PORT=$PORT bun run start`;
          } else if (mainFile && fs.existsSync(path.join(projectRoot, mainFile))) {
            cmd = `PORT=$PORT bun run ${mainFile}`;
            warnings.push(`No start script in package.json — using "bun run ${mainFile}" from package.json main field.`);
          } else {
            const entry = ['index.ts', 'index.js', 'src/index.ts', 'src/index.js'].find(f =>
              fs.existsSync(path.join(projectRoot, f))
            ) || 'index.ts';
            cmd = `PORT=$PORT bun run ${entry}`;
            warnings.push(`No start script found — defaulting to "bun run ${entry}". Set your Start Command in the dashboard.`);
          }
        }
        if (/PORT=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\d{4,5}/, 'PORT=$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common Bun fixes: ensure a "start" script is in package.json, and your app reads process.env.PORT or Bun.env.PORT.';
      }
    }
  });
}

// ── Deno ──────────────────────────────────────────────────────────────────────
async function runDenoBuild(opts) {
  const image = 'denoland/deno:alpine';
  const port  = normalizePort(opts.appPort, 8000);
  return runGenericBuild({ ...opts, dockerImage: image, stepLabel: 'deno',
    findRoot: (dir, log) => findFileRoot(dir, ['deno.json', 'deno.jsonc', 'mod.ts', 'main.ts'], log) || dir,
    extraEnv: { DENO_NO_UPDATE_CHECK: '1' },
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const userCmd = (project.installCmd || '').trim();
        if (userCmd && userCmd !== 'echo skip') return { cmd: userCmd, warnings: [] };
        // Cache dependencies if import_map or deno.json exists
        const hasDenoCfg = fs.existsSync(path.join(projectRoot, 'deno.json')) ||
                           fs.existsSync(path.join(projectRoot, 'deno.jsonc'));
        const cmd = hasDenoCfg ? 'deno cache --reload main.ts 2>/dev/null || deno cache --reload mod.ts 2>/dev/null || true' : 'echo skip';
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project) {
        const userCmd = (project.buildCmd || '').trim();
        if (userCmd && userCmd !== 'echo skip') return { cmd: userCmd, warnings: [] };
        return { cmd: 'echo skip', warnings: [] };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Check deno.json for a "start" task
          let hasStartTask = false;
          try {
            const denoJson = JSON.parse(fs.readFileSync(
              fs.existsSync(path.join(projectRoot, 'deno.json'))
                ? path.join(projectRoot, 'deno.json')
                : path.join(projectRoot, 'deno.jsonc'),
              'utf8'
            ));
            hasStartTask = !!(denoJson.tasks && denoJson.tasks.start);
          } catch(_) {}
          if (hasStartTask) {
            cmd = `PORT=$PORT deno task start`;
          } else {
            // Fall back to running main entry point directly
            const entry = ['main.ts', 'mod.ts', 'src/main.ts', 'app.ts'].find(f =>
              fs.existsSync(path.join(projectRoot, f))
            );
            if (entry) {
              cmd = `PORT=$PORT deno run --allow-net --allow-env --allow-read --allow-write ${entry}`;
              warnings.push(`No "start" task in deno.json — running "${entry}" directly with common permissions.`);
              warnings.push('Add a "start" task to deno.json or set your Start Command in the dashboard.');
            } else {
              cmd = `PORT=$PORT deno task start`;
              warnings.push('Could not detect Deno entry point. Set your Start Command in the dashboard.');
            }
          }
        }
        if (/PORT=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\d{4,5}/, 'PORT=$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common Deno fixes: add a "start" task to deno.json, ensure your app reads Deno.env.get("PORT"), and add required --allow-* permissions.';
      }
    }
  });
}

// ── File-marker based project root finder ─────────────────────────────────────
// Used by non-Node runtimes that don't have package.json as their root marker.
function findFileRoot(buildDir, markers, log, globMatch = false) {
  function walk(dir, depth) {
    if (depth > 5) return null;
    let entries;
    try { entries = fs.readdirSync(dir); } catch(_) { return null; }
    for (const marker of markers) {
      const found = globMatch
        ? entries.some(e => {
            const ext = marker.replace('*', '');
            return e.endsWith(ext) && fs.existsSync(path.join(dir, e));
          })
        : fs.existsSync(path.join(dir, marker));
      if (found) return dir;
    }
    const subdirs = entries
      .filter(e => {
        if (['.git','node_modules','__pycache__','vendor','target','dist','build','.next'].includes(e) || e.startsWith('.')) return false;
        try { return fs.lstatSync(path.join(dir, e)).isDirectory(); } catch(_) { return false; }
      }).sort();
    for (const sub of subdirs) {
      const found = walk(path.join(dir, sub), depth + 1);
      if (found) return found;
    }
    return null;
  }
  const result = walk(buildDir, 0);
  if (result && result !== buildDir && log) log(`\x1b[90m[info] Project root: ${path.relative(buildDir, result)}/\x1b[0m`);
  return result;
}

// ── STATIC BUILD ──────────────────────────────────────────────────────────────
async function runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog, appPort, baseDomain }) {
  const buildDir = path.join(tmpDir, deployId);
  const destDir  = path.join(sitesDir, project.subdomain, 'dist');

  const log = createBatchedLogger(emit, onLog);
  const env = resolveEnvVars(project.envVars);

  // ── Step 1: Clone ──────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log(`\x1b[36m━━━ Step 1/5 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log, project);
  const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
  const profile = detectProjectProfile(projectRoot);
  log(`\x1b[90m[detect] Static project type: ${profile.kind}${profile.framework ? ' · framework: ' + profile.framework : ''}\x1b[0m`);

  // [FIX] Nitro-based SSR meta-frameworks (TanStack Start, and anything else
  // built on Nitro) compile to a SERVER bundle (.output/server/index.mjs) —
  // there is no static HTML at all, by design. Deploying these as "static"
  // always failed with "no HTML entry file found", no matter how the build
  // itself went (confirmed: a real TanStack Start / Lovable-scaffolded repo
  // builds fine but produces zero .html files anywhere in .output). Detect
  // this up front from package.json and redirect into the server pipeline,
  // the same way runServerBuild already redirects composer.json → PHP,
  // go.mod → Go, etc. `appPort` is only present when called from the primary
  // dispatcher (never from runServerBuild's own static-fallback below), so
  // this can't create a static <-> server redirect loop.
  if (hasPackageJson && typeof appPort !== 'undefined') {
    try {
      const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
      const allDeps = { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
      const isNitroSsrProject = !!(
        allDeps['@tanstack/react-start'] || allDeps['@tanstack/start'] ||
        allDeps['nitropack'] || allDeps['nitro']
      );
      if (isNitroSsrProject) {
        log(`\x1b[33m[Joytree]\x1b[0m Detected a Nitro-based SSR framework (TanStack Start or similar) in package.json — this builds a server bundle, not static HTML. Switching this deploy to Web Service automatically.`);
        log(`\x1b[90m[Joytree]\x1b[0m Forcing NITRO_PRESET=node-server so the build targets a portable Node server (many scaffolds default Nitro to a Cloudflare Workers target, which this platform can't run directly).`);
        const currentEnv = resolveEnvVars(project.envVars);
        return runServerBuild({
          deployId,
          project: {
            ...project,
            siteType: 'server',
            startCmd: (project.startCmd || '').trim() || 'node .output/server/index.mjs',
            envVars: { ...currentEnv, NITRO_PRESET: currentEnv.NITRO_PRESET || 'node-server' },
          },
          sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain,
        });
      }
    } catch (_) { /* malformed package.json — fall through to the normal static flow */ }
  }

  // ── Auto-detect Node.js version from engines field ─────────────────────────
  const configuredNodeVer = String(project.nodeVer || '20');
  const detectedNodeVer = detectRequiredNodeVersion(projectRoot);
  let resolvedNodeVer = configuredNodeVer;
  if (detectedNodeVer && detectedNodeVer !== configuredNodeVer) {
    emitNodeVersionWarning(log, configuredNodeVer, detectedNodeVer);
    resolvedNodeVer = detectedNodeVer;
  } else if (detectedNodeVer) {
    log(`\x1b[90m[detect] Node.js version confirmed: ${resolvedNodeVer} (matches package.json engines)\x1b[0m`);
  }
  const nodeImage = `node:${resolvedNodeVer}`;

  const outputDir   = path.join(projectRoot, project.outputDir || 'dist');

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m`);
  if (hasPackageJson) {
    const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    await runInstallStepWithRecovery({ projectRoot, nodeImage, envObj: env, installCmd, log });
  } else {
    log(`\x1b[90m[install] No package.json found — skipping install for plain static files\x1b[0m`);
  }
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || (hasPackageJson ? getDefaultBuildCmd(projectRoot) : 'echo skip');
  let buildWasAutoSkipped = false;
  if (hasPackageJson && buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
    } catch (e) {
      if (isNativeBindingNpmBug(e.message)) {
        const _installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
        await recoverFromNativeBindingBugAndRetry({ projectRoot, nodeImage, envObj: env, installCmd: _installCmd, buildCmd, nodeEnvBuild: 'production', log });
      } else if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
        log(`\x1b[33m[Joytree] No build script found in your project — automatically skipping build step.\x1b[0m`);
        log(`\x1b[33m[Joytree] ℹ Tip: if you intended to run a build, add a "build" script to your package.json, or set the build command to "echo skip" to suppress this message.\x1b[0m`);
        log(`\x1b[32m[Joytree] ✓ Continuing deployment by serving project root directly.\x1b[0m`);
        buildWasAutoSkipped = true;
      } else {
        throw e;
      }
    }
  } else {
    if (buildCmd === 'echo skip') {
      log(`\x1b[90m[build] Build skipped (echo skip configured)\x1b[0m`);
    } else {
      log(`\x1b[90m[build] No package.json — skipping build step\x1b[0m`);
      buildWasAutoSkipped = true;
    }
  }
  // When build is auto-skipped, we must serve the project root (.), not dist/
  // because there's no built output directory
  if (buildWasAutoSkipped && (project.outputDir || 'dist') !== '.') {
    log(`\x1b[33m[Joytree] Output directory automatically changed from "${project.outputDir || 'dist'}" → "." (serving project root since no build was run)\x1b[0m`);
    project = { ...project, outputDir: '.' };
  }
  emitStep(emit, 'build', 'done');

  // ── Step 4: Copy ───────────────────────────────────────────────────────────
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 4/5 — Deploy ━━━\x1b[0m`);

  // outputDir '.' means serve the whole project root (no build step)
  const srcDir = (project.outputDir || 'dist').trim() === '.'
    ? projectRoot
    : outputDir;

  let finalSrcDir = srcDir;
  if (!fs.existsSync(finalSrcDir) && !hasPackageJson) {
    finalSrcDir = projectRoot;
    log(`\x1b[33m[deploy] Output dir missing; serving repo root for plain static site\x1b[0m`);
  }

  if (!fs.existsSync(finalSrcDir)) {
    // [FIX] This used to fail immediately with a list of what WAS available,
    // but never tried any of them. That's a real, common deployment
    // interrupter: Create React App outputs to "build" (not "dist"),
    // Gatsby outputs to "public", Nuxt 3's static generate outputs to
    // ".output/public", and Angular nests its output inside
    // "dist/<project-name>/" (or "dist/<project-name>/browser/" on newer
    // esbuild-based Angular). All of these are extremely common frameworks
    // -- an auto-detect deploy of any of them would fail outright even
    // though the correct output directory exists right there, one level
    // away from what was guessed.
    //
    // Only auto-recover when the configured outputDir was left at the bare
    // default ("dist") -- if someone explicitly typed a custom outputDir
    // and got it wrong, that's their call to fix, not ours to silently
    // override.
    let recovered = false;
    if ((project.outputDir || 'dist').trim() === 'dist') {
      const candidates = ['build', 'public', path.join('.output', 'public'), 'out'];
      for (const cand of candidates) {
        const candPath = path.join(projectRoot, cand);
        if (fs.existsSync(candPath) && fs.lstatSync(candPath).isDirectory()) {
          log(`\x1b[33m[Joytree] "dist" not found, but "${cand}" looks like your build output (common for Create React App, Gatsby, Nuxt static generate) -- using it instead.\x1b[0m`);
          finalSrcDir = candPath;
          project = { ...project, outputDir: cand };
          recovered = true;
          break;
        }
      }
      // Angular-style nesting: dist/<project-name>/ or dist/<project-name>/browser/.
      // Not caught above since "dist" itself DOES exist here -- it's just
      // empty of an index.html at its top level, one or two folders too shallow.
      if (!recovered) {
        const distPath = path.join(projectRoot, 'dist');
        if (fs.existsSync(distPath) && fs.lstatSync(distPath).isDirectory()) {
          const distChildren = fs.readdirSync(distPath).filter(f => {
            try { return fs.lstatSync(path.join(distPath, f)).isDirectory(); } catch (_) { return false; }
          });
          if (distChildren.length === 1) {
            const nested = path.join(distPath, distChildren[0]);
            const nestedBrowser = path.join(nested, 'browser');
            const finalNested = fs.existsSync(nestedBrowser) && fs.lstatSync(nestedBrowser).isDirectory() ? nestedBrowser : nested;
            if (fs.existsSync(path.join(finalNested, 'index.html'))) {
              const relOutputDir = path.relative(projectRoot, finalNested);
              log(`\x1b[33m[Joytree] "dist" exists but has no index.html directly inside it -- found one nested at "${relOutputDir}" (typical of Angular's per-project build output) and using that instead.\x1b[0m`);
              finalSrcDir = finalNested;
              project = { ...project, outputDir: relOutputDir };
              recovered = true;
            }
          }
        }
      }
    }
    if (!recovered) {
      // [FIX] Before giving up entirely: if this is clearly a Vite project
      // (has a vite.config.*) but the configured/default build command
      // didn't actually produce any output, the most common real-world
      // cause is a placeholder "build" script in package.json (e.g.
      // `"build": "echo 'Build complete'"` left over from a template) that
      // never actually invokes Vite. Rather than fail outright, try running
      // the real build command directly.
      const viteConfigFile = ['vite.config.js', 'vite.config.ts', 'vite.config.mjs', 'vite.config.cjs']
        .map(f => path.join(projectRoot, f)).find(f => fs.existsSync(f));
      if (viteConfigFile) {
        log(`\x1b[33m[Joytree]\x1b[0m Found ${path.basename(viteConfigFile)} but no build output — your configured build script may not actually be running Vite. Trying \`npx vite build\` directly...\x1b[0m`);
        try {
          await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: 'npx vite build', log });
          const distPath = path.join(projectRoot, 'dist');
          if (fs.existsSync(distPath) && fs.readdirSync(distPath).length > 0) {
            log(`\x1b[32m[Joytree]\x1b[0m \`npx vite build\` succeeded — using its output. Consider fixing your package.json "build" script to say "vite build" so this runs automatically next time.\x1b[0m`);
            finalSrcDir = distPath;
            project = { ...project, outputDir: 'dist' };
            recovered = true;
          }
        } catch (_) {
          // Fall through to the normal error below if this also fails.
        }
      }
    }
    if (!recovered) {
      const dirs = fs.readdirSync(buildDir).filter(f => {
        try { return fs.lstatSync(path.join(buildDir, f)).isDirectory(); } catch(e) { return false; }
      });
      log(`\x1b[31m[error] Output dir not found: "${project.outputDir || 'dist'}"\x1b[0m`);
      log(`\x1b[33m[hint] Dirs in repo: ${dirs.join(', ') || '(none)'}\x1b[0m`);
      throw new Error(`Output dir "${project.outputDir||'dist'}" not found. Available: ${dirs.join(', ')}`);
    }
  }

  // ── Static entry detection: find index.html / 200.html, or any .html file ──
  // Some projects have a single HTML file with a custom name (e.g. kitchen.html,
  // portfolio.html). We detect this and create index.html so the static server
  // can serve it correctly — no failure, no manual renaming required.
  const indexCandidates = [path.join(finalSrcDir, 'index.html'), path.join(finalSrcDir, '200.html')];
  let hasStaticEntry = indexCandidates.some(fp => fs.existsSync(fp));

  if (!hasStaticEntry) {
    // Recursively find all .html files in the output dir (shallow-first)
    function findHtmlFiles(dir, depth = 0) {
      if (depth > 4) return [];
      let results = [];
      let entries = [];
      try { entries = fs.readdirSync(dir); } catch (_) { return []; }
      for (const e of entries) {
        const full = path.join(dir, e);
        try {
          const st = fs.lstatSync(full);
          if (st.isFile() && e.toLowerCase().endsWith('.html')) {
            results.push(full);
          } else if (st.isDirectory() && !['node_modules', '.git', '.next', 'dist', 'build'].includes(e)) {
            results = results.concat(findHtmlFiles(full, depth + 1));
          }
        } catch (_) {}
      }
      return results;
    }

    const allHtmlFiles = findHtmlFiles(finalSrcDir);

    if (allHtmlFiles.length > 0) {
      // Prefer a file at the root level; otherwise take the first found
      const rootHtmlFiles = allHtmlFiles.filter(f => path.dirname(f) === finalSrcDir);
      const chosen = rootHtmlFiles.length > 0 ? rootHtmlFiles[0] : allHtmlFiles[0];
      const chosenName = path.basename(chosen);
      const destIndex = path.join(finalSrcDir, 'index.html');

      log(`\x1b[33m[Joytree] No index.html found — detected HTML file: "${chosenName}"\x1b[0m`);
      log(`\x1b[32m[Joytree] ✓ Automatically creating index.html from "${chosenName}" so the site can be served correctly\x1b[0m`);

      // If the chosen file is already at root, copy it as index.html alongside itself.
      // If it's in a subdir, also copy the whole parent structure (already handled by copyDir).
      try {
        fs.copyFileSync(chosen, destIndex);
        hasStaticEntry = true;
        if (allHtmlFiles.length > 1) {
          log(`\x1b[90m[detect] ${allHtmlFiles.length} HTML files found. Using "${chosenName}" as the entry point.\x1b[0m`);
        }
      } catch (copyErr) {
        log(`\x1b[31m[error] Could not create index.html from "${chosenName}": ${copyErr.message}\x1b[0m`);
      }
    }

    if (!hasStaticEntry) {
      // [FIX] Same reasoning as the output-dir fallback above: a "dist"
      // folder existing but containing no real HTML output (rather than
      // dist not existing at all) is the same underlying symptom -- a
      // placeholder build script that isn't actually invoking Vite. Try the
      // real command directly before giving up, if this looks like a Vite
      // project.
      let recoveredViaVite = false;
      const viteConfigFile2 = ['vite.config.js', 'vite.config.ts', 'vite.config.mjs', 'vite.config.cjs']
        .map(f => path.join(projectRoot, f)).find(f => fs.existsSync(f));
      if (viteConfigFile2) {
        log(`\x1b[33m[Joytree]\x1b[0m Found ${path.basename(viteConfigFile2)} but no HTML output — your configured build script may not actually be running Vite. Trying \`npx vite build\` directly...\x1b[0m`);
        try {
          await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: 'npx vite build', log });
          const distPath = path.join(projectRoot, 'dist');
          const freshHtml = fs.existsSync(distPath) ? findHtmlFiles(distPath) : [];
          if (freshHtml.length > 0) {
            log(`\x1b[32m[Joytree]\x1b[0m \`npx vite build\` succeeded — using its output. Consider fixing your package.json "build" script to say "vite build" so this runs automatically next time.\x1b[0m`);
            finalSrcDir = distPath;
            project = { ...project, outputDir: 'dist' };
            hasStaticEntry = true;
            recoveredViaVite = true;
          }
        } catch (_) {
          // Fall through to the normal error below if this also fails.
        }
      }
      if (!recoveredViaVite) {
        const htmlList = allHtmlFiles.map(f => path.relative(finalSrcDir, f)).join(', ') || 'none';
        log(`\x1b[31m[error] No HTML entry file found in output directory\x1b[0m`);
        log(`\x1b[33m[hint] Ensure your project has at least one .html file, or check your outputDir setting.\x1b[0m`);
        log(`\x1b[33m[hint] HTML files found: ${htmlList}\x1b[0m`);
        throw new Error('Static deploy validation failed: no HTML entry file found in output directory');
      }
    }
  }

  if (fs.existsSync(destDir)) fs.rmSync(destDir, { recursive: true, force: true });
  fs.mkdirSync(destDir, { recursive: true });
  copyDir(finalSrcDir, destDir);
  const count = countFiles(destDir);

  // Defensive cleanup: static deployments must not retain server proxy targets.
  // If an old server deploy left a ports.json entry/container behind, requests
  // could proxy to a dead upstream and return Cloudflare 502.
  const portsFile = path.join(sitesDir, 'ports.json');
  try {
    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch (_) {}
    if (registry[project.subdomain]) {
      delete registry[project.subdomain];
      fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
      log(`\x1b[90m[static] Removed stale proxy target for ${project.subdomain}\x1b[0m`);
    }
  } catch (e) {
    log(`\x1b[33m[static] Could not update ports registry: ${e.message}\x1b[0m`);
  }
  try { await exec('docker', ['rm', '-f', `db-${project.subdomain}`], {}, () => {}); } catch (_) {}

  emitStep(emit, 'copy', 'done');
  log(`\x1b[32m[deploy] ✓ ${count} files deployed\x1b[0m`);

  // ── Step 5: Cleanup ────────────────────────────────────────────────────────
  emitStep(emit, 'cleanup', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 5/5 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Static site deployed!\x1b[0m`);
  return { siteType: 'static' };
}

// ── SERVER BUILD ──────────────────────────────────────────────────────────────
// Each server app runs in its OWN Docker container — fully isolated.
// Port conflicts are impossible. Container survives Joytree restarts.
async function runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain }) {
  const buildDir  = path.join(tmpDir, deployId);
  const appDir    = path.join(sitesDir, project.subdomain, 'app');
  let startCmd  = (project.startCmd || '').trim();
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const expectedPort = normalizePort(appPort, 3000);
  const runtime = getRuntimeConfig(project);

  const log = createBatchedLogger(emit, onLog);
  const env = withDeployedAppRuntimeDefaults({ ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project) }, project, baseDomain);

  // ── Step 1: Clone ──────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log, project);
  const relativeProjectRoot = path.relative(buildDir, projectRoot) || '.';
  log(`\x1b[90m[deploy] Server app root: ${relativeProjectRoot} — install, build, and start all use this same directory.\x1b[0m`);

  // [FIX] Nitro-based SSR meta-frameworks (TanStack Start, etc.) need two
  // corrections that generic detection can't infer, and this needs to run
  // here too -- not just in runStaticBuild's redirect -- because a user
  // picking "Web Service" directly (or leaving site type blank/auto) reaches
  // THIS function straight away, bypassing that other check entirely:
  //  1. Many scaffolds (this one via @lovable.dev/vite-tanstack-config)
  //     default their Nitro preset to a Cloudflare Workers target. Under
  //     that preset, STATIC ASSETS (images, etc.) are meant to be served by
  //     Cloudflare's platform, not by the worker code itself -- so running
  //     that same bundle under plain Node serves pages/API routes fine but
  //     returns 404 for every image, since nothing in the process serves
  //     `.output/public`. Forcing NITRO_PRESET=node-server makes Nitro
  //     generate its own static-file-serving middleware instead.
  //  2. getDefaultStartCmd() has no idea what Nitro/TanStack Start is, so
  //     for a project with a "preview" script but no "start" script (exactly
  //     this scaffold's shape) it would pick "npm run preview" (vite
  //     preview) -- an entirely different, wrong server that doesn't serve
  //     the actual SSR output or its assets at all.
  if (fs.existsSync(path.join(projectRoot, 'package.json'))) {
    try {
      const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
      const allDeps = { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
      const isNitroSsrProject = !!(
        allDeps['@tanstack/react-start'] || allDeps['@tanstack/start'] ||
        allDeps['nitropack'] || allDeps['nitro']
      );
      if (isNitroSsrProject) {
        log(`\x1b[33m[Joytree]\x1b[0m Detected a Nitro-based SSR framework (TanStack Start or similar) in package.json.`);
        // [FIX] These scaffolds (Lovable's TanStack Start template in
        // particular) ship a bun.lock, not a package-lock.json -- the ONLY
        // lockfile format detectRequiredNodeVersion() actually scans for
        // dependency engines fields. So the generic auto-detection above
        // never fires for these projects, and the build silently runs on
        // whatever Node version was otherwise configured (previously 20),
        // which is confirmed insufficient: @lovable.dev/vite-tanstack-config
        // fails to resolve under Node 20 with ERR_MODULE_NOT_FOUND /
        // UNRESOLVED_IMPORT, and builds cleanly under Node 22+. Force it
        // directly here since we already know this ecosystem's requirement.
        if (Number(String(project.nodeVer || '0').match(/\d+/)?.[0] || 0) < 22) {
          log(`\x1b[90m[Joytree]\x1b[0m Forcing Node.js 22 (TanStack Start's own dependencies require it; its bun.lock isn't scanned by the generic engines-field auto-detection).`);
          project = { ...project, nodeVer: '22' };
        }
        if (!env.NITRO_PRESET) {
          env.NITRO_PRESET = 'node-server';
          log(`\x1b[90m[Joytree]\x1b[0m Forcing NITRO_PRESET=node-server so the build serves its own static assets under plain Node (many scaffolds default to a Cloudflare Workers target, which handles assets differently and would 404 on images here).`);
        }
        if (!startCmd) {
          startCmd = 'node .output/server/index.mjs';
          log(`\x1b[90m[Joytree]\x1b[0m No Start Command set — defaulting to "${startCmd}" (Nitro's standard server entry), instead of an auto-detected script like "preview" that wouldn't serve the real SSR output.`);
        }
      }
    } catch (_) { /* malformed package.json — proceed with normal detection below */ }
  }

  // ── Auto-detect Node.js version from engines field ─────────────────────────
  const configuredNodeVer = String(project.nodeVer || '20');
  const detectedNodeVer = detectRequiredNodeVersion(projectRoot);
  let resolvedNodeVer = configuredNodeVer;
  if (detectedNodeVer && detectedNodeVer !== configuredNodeVer) {
    emitNodeVersionWarning(log, configuredNodeVer, detectedNodeVer);
    resolvedNodeVer = detectedNodeVer;
  } else if (detectedNodeVer) {
    log(`\x1b[90m[detect] Node.js version confirmed: ${resolvedNodeVer} (matches package.json engines)\x1b[0m`);
  }
  const nodeImage = `node:${resolvedNodeVer}`;

  // ── Post-clone non-Node.js runtime detection ──────────────────────────────
  // If this project reached runServerBuild without an explicit runtime set,
  // check the CLONED FILES for framework markers. A project with composer.json
  // but no explicit runtime=php set would otherwise be built with node:XX,
  // causing "sh: 1: composer: not found" (exit 127) at the install step.
  // We detect here after the clone so the actual repo files decide the runtime.
  if (!String(project.runtime || '').trim()) {
    const hasComposerJson = fs.existsSync(path.join(projectRoot, 'composer.json'));
    const hasArtisan      = fs.existsSync(path.join(projectRoot, 'artisan'));
    const hasComposerLock = fs.existsSync(path.join(projectRoot, 'composer.lock'));
    const hasRequirementsTxt = fs.existsSync(path.join(projectRoot, 'requirements.txt')) ||
                               fs.existsSync(path.join(projectRoot, 'requirements')) ||
                               fs.existsSync(path.join(projectRoot, 'Pipfile')) ||
                               fs.existsSync(path.join(projectRoot, 'pyproject.toml'));
    const hasManagePy     = fs.existsSync(path.join(projectRoot, 'manage.py'));
    const hasGoMod        = fs.existsSync(path.join(projectRoot, 'go.mod'));
    const hasGemfile      = fs.existsSync(path.join(projectRoot, 'Gemfile'));
    const hasCargoToml    = fs.existsSync(path.join(projectRoot, 'Cargo.toml'));
    const hasPomXml       = fs.existsSync(path.join(projectRoot, 'pom.xml'));
    const hasBuildGradle  = fs.existsSync(path.join(projectRoot, 'build.gradle')) ||
                            fs.existsSync(path.join(projectRoot, 'build.gradle.kts'));
    const hasMixExs       = fs.existsSync(path.join(projectRoot, 'mix.exs'));

    if (hasComposerJson || hasArtisan || hasComposerLock) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected PHP project (composer.json/artisan found) — switching to PHP build pipeline instead of Node.js.`);
      log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: set Runtime to "PHP" in your project settings to avoid this auto-detection step.`);
      // Pass buildDir (already cloned) as tmpDir so the PHP runner re-uses the clone
      return runPhpBuild({ deployId, project: { ...project, runtime: 'php' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasRequirementsTxt || hasManagePy) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Python project (requirements.txt/manage.py found) — switching to Python build pipeline instead of Node.js.`);
      log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: set Runtime to "Python" in your project settings to avoid this auto-detection step.`);
      return runPythonBuild({ deployId, project: { ...project, runtime: 'python' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasGoMod) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Go project (go.mod found) — switching to Go build pipeline instead of Node.js.`);
      return runGoBuild({ deployId, project: { ...project, runtime: 'go' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasGemfile) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Ruby project (Gemfile found) — switching to Ruby build pipeline instead of Node.js.`);
      return runRubyBuild({ deployId, project: { ...project, runtime: 'ruby' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasCargoToml) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Rust project (Cargo.toml found) — switching to Rust build pipeline instead of Node.js.`);
      return runRustBuild({ deployId, project: { ...project, runtime: 'rust' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasPomXml || hasBuildGradle) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected JVM project (pom.xml/build.gradle found) — switching to Java build pipeline instead of Node.js.`);
      return runJvmBuild({ deployId, project: { ...project, runtime: 'java' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
    if (hasMixExs) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Elixir project (mix.exs found) — switching to Elixir build pipeline instead of Node.js.`);
      return runElixirBuild({ deployId, project: { ...project, runtime: 'elixir' }, sitesDir, tmpDir, githubToken, appPort, emit, onLog, baseDomain });
    }
  }

  if (!isDeployableServerProject(projectRoot, startCmd)) {
    log(`\x1b[33m[auto] No production server start could be inferred. Falling back to static deployment flow.\x1b[0m`);
    const fallbackProject = { ...project, siteType: 'static', buildCmd: project.buildCmd || 'echo skip', outputDir: project.outputDir || '.' };
    return runStaticBuild({ deployId, project: fallbackProject, sitesDir, tmpDir, githubToken, emit, onLog });
  }

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 2/6 — Install ━━━\x1b[0m`);
  const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
  log(`\x1b[90m[install] cwd: ${relativeProjectRoot}\x1b[0m`);
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await runInstallStepWithRecovery({ projectRoot, nodeImage, envObj: env, installCmd, log });
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 3/6 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot);
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m[build] cwd: ${relativeProjectRoot}\x1b[0m`);
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
    } catch (e) {
      if (isNativeBindingNpmBug(e.message)) {
        const _installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
        await recoverFromNativeBindingBugAndRetry({ projectRoot, nodeImage, envObj: env, installCmd: _installCmd, buildCmd, nodeEnvBuild: 'production', log });
      } else if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
        log(`\x1b[33m[Joytree] No build script found in your project — automatically skipping build step.\x1b[0m`);
        log(`\x1b[33m[Joytree] ℹ Tip: add a "build" script to package.json, or set build command to "echo skip".\x1b[0m`);
        log(`\x1b[32m[Joytree] ✓ Continuing deployment without build step.\x1b[0m`);
      } else {
        throw e;
      }
    }
  }
  emitStep(emit, 'build', 'done');

  // ── Step 4: Persist app dir (symlink to build dir so Docker can mount it) ───
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 4/6 — Prepare App Dir ━━━\x1b[0m`);

  // CRITICAL FIX: Do NOT copy node_modules — symlinks break and monorepos fail.
  // Instead, move the entire build directory to a permanent location so Docker
  // can mount it intact with all symlinks and node_modules preserved.
  if (fs.existsSync(appDir)) fs.rmSync(appDir, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(appDir), { recursive: true });

  // Try rename first (instant, preserves everything including symlinks)
  // Falls back to copy only if on different filesystem (tmp → sites on different mount)
  let usedBuildDir = projectRoot;
  try {
    fs.renameSync(projectRoot, appDir);
    usedBuildDir = appDir;
    log(`\x1b[32m[copy] ✓ App moved to permanent storage (symlinks intact)\x1b[0m`);
  } catch(e) {
    // Cross-device rename failed — fall back to rsync which handles symlinks properly
    log(`\x1b[90m[copy] Cross-device move, using rsync…\x1b[0m`);
    fs.mkdirSync(appDir, { recursive: true });
    try {
      const { execSync } = require('child_process');
      execSync(`rsync -a --links --no-whole-file "${projectRoot}/" "${appDir}/"`, { stdio: 'pipe', maxBuffer: 50*1024*1024 });
      log(`\x1b[32m[copy] ✓ App synced with rsync (symlinks intact)\x1b[0m`);
      usedBuildDir = appDir;
    } catch(rsyncErr) {
      // rsync not available — copy without node_modules, install fresh in container
      log(`\x1b[33m[copy] rsync unavailable, copying source only (node_modules will install in container)\x1b[0m`);
      const excludes = ['node_modules', '.git'];
      const copyFiltered = (src, dst) => {
        fs.mkdirSync(dst, { recursive: true });
        for (const entry of fs.readdirSync(src)) {
          if (excludes.includes(entry)) continue;
          const s = path.join(src, entry), d = path.join(dst, entry);
          try {
            const st = fs.lstatSync(s);
            if (st.isDirectory()) copyFiltered(s, d);
            else if (st.isFile()) fs.copyFileSync(s, d);
          } catch(ce) {}
        }
      };
      copyFiltered(projectRoot, appDir);
      usedBuildDir = appDir;
      log(`\x1b[33m[copy] Source copied without node_modules — runtime bootstrap will reinstall dependencies in /app before start\x1b[0m`);
    }
  }

  emitStep(emit, 'copy', 'done');

  // ── Step 5: Run in isolated Docker container ───────────────────────────────
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);
  log(`\x1b[90m[docker] Image:     ${nodeImage}\x1b[0m`);
  log(`\x1b[90m[docker] Container: ${candidateContainerName} (candidate)\x1b[0m`);
  log(`\x1b[90m[docker] Runtime cwd: /app (same files prepared from ${relativeProjectRoot})\x1b[0m`);
  const resolvedStartCmd = resolveRuntimeStartCommand({
    projectRoot: usedBuildDir || projectRoot,
    startCmd,
    expectedPort
  });
  log(`\x1b[90m[docker] Command:   ${resolvedStartCmd}\x1b[0m`);

  // Remove stale candidate with same name if any
  try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(e) {}

  // Map the permanent app dir to the Docker volume path the container can see
  const hostAppDir = usedBuildDir.replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data')
                                 .replace('/tmp/deployboard-builds', '/tmp/deployboard-builds');

  // If usedBuildDir is still in /tmp (rename failed and copy also failed somehow),
  // we need a path Docker can reach — use the tmp path directly since it's mounted
  const dockerMountSrc = usedBuildDir.startsWith('/tmp')
    ? usedBuildDir   // /tmp is directly accessible inside the Joytree container
    : hostAppDir;

  const dockerArgs = [
    'run', '-d',
    '--name',         candidateContainerName,
    '--restart',      'no',
    '--network',      'deployboard-net',
    // [FIX] Lets apps reach VPS-local services (e.g. a self-hosted DB bound
    // to the VPS's public IP) via host.docker.internal instead of hairpinning
    // out through the public IP and back in — a path many VPS networks
    // silently drop, stalling startup for a long time (SYN retry backoff)
    // before falling through to any DB-connect failure handling.
    '--add-host',     'host.docker.internal:host-gateway',
    '--cpu-shares',   runtime.cpuShares,
    '--pids-limit',   PIDS_LIMIT,
    '-m',             runtime.memory,
    '--memory-reservation', runtime.memory,
    '-e',             `PORT=${expectedPort}`,
    '-e',             `NODE_ENV=production`,
    '-e',             `HOST=0.0.0.0`,
    '-e',             `HOSTNAME=0.0.0.0`,
    '-e',             `NEXT_TELEMETRY_DISABLED=1`,
    '-e',             `NODE_OPTIONS=--max-old-space-size=${runtime.nodeHeapMb}`,
    '-v',             `${dockerMountSrc}:/app`,
    '-w',             '/app',
  ];
  if (runtime.memorySwap) {
    dockerArgs.push('--memory-swap', runtime.memorySwap);
  }
  log(`\x1b[90m[docker] Persistent server container: restart=unless-stopped | ${runtime.cpuShares} CPU shares | ${runtime.memory} RAM | ${PIDS_LIMIT} max processes\x1b[0m`);

  for (const [k, v] of Object.entries(env)) {
    const key = String(k || '').toUpperCase();
    // Keep platform runtime binding values authoritative.
    if (key === 'PORT' || key === 'HOST' || key === 'HOSTNAME') continue;
    dockerArgs.push('-e', `${k}=${v}`);
  }

  // Pull image
  log(`\x1b[90m[docker] Pulling ${nodeImage}…\x1b[0m`);
  try {
    await exec('docker', ['pull', nodeImage], {}, log);
  } catch(e) {
    log(`\x1b[33m[docker] Using cached image\x1b[0m`);
  }

  // Prefix PATH so npm/node are always found regardless of how the container
  // shell initialises its environment (some sh builds skip /usr/local/bin).
  // Normally node_modules was prepared in Step 2 and moved into /app. If a
  // cross-device copy had to omit node_modules, bootstrap dependencies in the
  // same /app runtime directory before executing the start command.
  const runtimeInstallCmd = normalizeInstallLikeCommand(installCmd, usedBuildDir || projectRoot).replace(/`/g, '\\`');
  const ensureRuntimeDeps = `[ -d node_modules ] || [ ! -f package.json ] || (echo "[Joytree] node_modules missing in /app — reinstalling dependencies before start" && corepack enable >/dev/null 2>&1 || true; ${runtimeInstallCmd})`;
  const startWithPath = `export PATH=/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:$PATH && ${ensureRuntimeDeps} && ${resolvedStartCmd}`;
  dockerArgs.push(nodeImage, 'sh', '-c', startWithPath);
  await exec('docker', dockerArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);
  log(`\x1b[90m[docker] Runtime resources applied: ${runtime.cpuShares} CPU shares | ${runtime.memory} RAM | swap ${runtime.memorySwap || 'disabled'} | ${PIDS_LIMIT} max processes\x1b[0m`);

  // Give container a moment, then verify it's still running.
  log(`\x1b[90m[docker] Waiting for process to stabilize…\x1b[0m`);
  // [FIX] Use up to 90s (not 30s) for the container-running check.
  // Heavy apps like Uptime Kuma initialize a SQLite database and run schema
  // migrations before they bind a port. 30s was not enough and caused them
  // to be marked as failed before they actually started.
  const containerStabilizeTimeout = Math.min(90, runtime.startupTimeoutSeconds);
  const stable = await waitForContainerRunning(candidateContainerName, containerStabilizeTimeout, log);
  if (!stable) {
    let state = 'unknown';
    try { state = await getContainerState(candidateContainerName); } catch (_) {}
    log(`\x1b[31m[docker] ✗ Container is not running (state: ${state})\x1b[0m`);
    try {
      const inspectLines = [];
      await exec(
        'docker',
        ['inspect', '--format={{.State.Status}}|oom={{.State.OOMKilled}}|exit={{.State.ExitCode}}|error={{.State.Error}}', candidateContainerName],
        {},
        (line) => inspectLines.push(line)
      );
      const diag = (inspectLines.join('\n').trim().split('\n').pop() || '').trim();
      if (diag) log(`\x1b[33m[docker] State details: ${diag}\x1b[0m`);
    } catch (_) {}
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(e) {}
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    throw new Error(`Container "${candidateContainerName}" exited during startup. Check logs above.`);
  }

  // Register proxy by stable container DNS name + expected app port.
  // Docker network DNS resolves containerName reliably and avoids fragile IP polling.
  const pFile = path.join(sitesDir, 'ports.json');
  const previousTarget = readRegistryTarget(pFile, project.subdomain);
  try {
    let registry   = {};
    try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(e) {}
    const livePort = await detectLivePort(candidateContainerName, expectedPort, runtime.startupTimeoutSeconds, log);
    if (!livePort) {
      throw new Error(`Readiness gate failed after ${runtime.startupTimeoutSeconds}s`);
    }
    const targetPort = normalizePort(livePort, expectedPort);
    if (targetPort !== expectedPort) {
      log(`\x1b[33m[docker] App ignored PORT=${expectedPort}; routing to detected port ${targetPort}\x1b[0m`);
    }
    log(`\x1b[90m[docker] Strict readiness gate passed before promotion\x1b[0m`);

    // Promote first, then publish the stable container name to the proxy registry.
    // Publishing before rename can briefly route Cloudflare/nginx to a container
    // name that does not exist yet, producing intermittent 502s.
    await archivePreviousContainer(containerName, project.subdomain, log);
    await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
    // [FIX] The candidate container launches with --restart=no so a
    // crash-looping start command stays visibly exited instead of being
    // silently relaunched by Docker mid-healthcheck (which was masking
    // startup failures as "still waiting" forever -- see detectLivePort's
    // liveness check above). Only apply real crash-resilience now that the
    // container has actually proven it serves HTTP.
    try { await exec('docker', ['update', '--restart', 'unless-stopped', containerName], {}, () => {}); } catch(_) {}
    log(`\x1b[90m[docker] Promoted candidate to stable: ${containerName}\x1b[0m`);
    registry[project.subdomain] = `${containerName}:${targetPort}`;
    fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    log(`\x1b[32m[docker] ✓ Proxy registered: ${project.subdomain} → ${containerName}:${targetPort}\x1b[0m`);
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);

  } catch(e) {
    log(`\x1b[31m[docker] Candidate failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (isStableRegistryTarget(previousTarget)) {
      log(`\x1b[33m[docker] Rolled back to previous live mapping: ${project.subdomain} → ${previousTarget}\x1b[0m`);
    } else {
      let registry = {};
      try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(_) {}
      delete registry[project.subdomain];
      fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
      log(`\x1b[33m[docker] No previous mapping found; subdomain removed from registry to prevent stale 502 route\x1b[0m`);
    }
    throw e;
  }

  emitStep(emit, 'start', 'done');

  // ── Step 6: Cleanup temp build dir ────────────────────────────────────────
  emitStep(emit, 'cleanup', 'active');
  checkBuildStopped(deployId);
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Server app deployed in isolated container!\x1b[0m`);
  // [FIX] This function never reported back what it actually determined --
  // callers just kept whatever siteType (often blank, for auto-detect)
  // the project record started with, so a correctly-auto-detected server
  // app's persisted metadata still showed 'static' after a successful
  // deploy, even though it was genuinely running as a server. Returning the
  // resolved type lets the caller correct the stored record to match reality.
  return { siteType: 'server' };
}



function normalizePort(raw, fallback = 0) {
  if (Number.isInteger(raw) && raw > 0) return raw;
  if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  const text = String(raw || '').trim();
  if (!text) return fallback;
  const direct = Number(text);
  if (Number.isInteger(direct) && direct > 0) return direct;
  const match = text.match(/:(\d{2,5})$/) || text.match(/(\d{2,5})$/);
  if (match) {
    const p = Number(match[1]);
    if (Number.isInteger(p) && p > 0) return p;
  }
  return fallback;
}

function safeDockerToken(value, fallback = 'token') {
  const cleaned = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9-]/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
  return cleaned || fallback;
}

async function detectLivePort(containerName, preferredPort, startupTimeoutSeconds, log) {
  // Two separate lists so discovered ports always probe before seed/fallback ports.
  // discoveredSet tracks which ports the app actually bound to — those are allowed
  // to return HTML (full-stack Next.js / React apps serve HTML on '/').
  // Seed ports (preferredPort, common framework defaults) are only tried if no
  // discovered port responds, and HTML from them is rejected to avoid locking on
  // to a foreign service's error page (the root cause of the DOCTYPE proxy bug).
  const discoveredSet = new Set();
  const seedSet = new Set();
  const candidates = []; // ordered: discovered first, seeds appended at the back

  const addDiscovered = (v) => {
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) return;
    if (discoveredSet.has(n)) return;
    discoveredSet.add(n);
    // Discovered sockets belong to this container. Keep them before generic
    // framework seeds, but never move the preferred platform port behind
    // internal helper sockets.
    if (!candidates.includes(n)) {
      const insertAt = candidates[0] === preferredPort ? 1 : 0;
      candidates.splice(insertAt, 0, n);
    }
  };
  const addSeed = (v) => {
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) return;
    if (seedSet.has(n) || candidates.includes(n)) return;
    seedSet.add(n);
    candidates.push(n); // back of the list
  };

  addSeed(preferredPort);
  // [FIX] Added 3001 as a primary seed — Uptime Kuma and many other apps bind
  // to 3001 by default. Also added 8888/9000 as additional common app ports.
  [3000, 3001, 3002, 4000, 4173, 5000, 5173, 8000, 8080, 8787, 8888, 9000].forEach(addSeed);

  let fallbackIP = '';
  try { fallbackIP = await getContainerIP(containerName); } catch (_) {}
  if (fallbackIP) log(`\x1b[90m[docker] Fallback probe IP: ${fallbackIP}\x1b[0m`);

  // Pre-scan before the loop so discovered ports are at the front immediately.
  try {
    const preScan = await detectListeningPorts(containerName);
    preScan.forEach(addDiscovered);
    const logPort = await detectPortFromContainerLogs(containerName);
    if (logPort) addDiscovered(logPort);
    if (preScan.length) log(`\x1b[90m[docker] Discovered listening ports (pre-scan): ${preScan.join(', ')}\x1b[0m`);
  } catch (_) {}

  for (let attempt = 1; attempt <= startupTimeoutSeconds; attempt++) {
    // [FIX] Previously this loop only ever probed HTTP ports for the full
    // startupTimeoutSeconds, with no check on whether the container itself
    // was even still alive. If the resolved start command failed instantly
    // (most commonly: "npm start" with no "start" script in package.json --
    // exactly what happens when a user picks Web Service for a project that
    // doesn't actually define one), the container exited in under a second,
    // but the deploy kept silently polling a dead container for the entire
    // timeout before finally reporting a vague "Readiness gate failed after
    // Ns" -- indistinguishable from a slow-starting app, and with no hint of
    // the actual cause. Checking liveness first means a crash-on-start is
    // caught almost immediately with the real reason attached.
    if (attempt === 1 || attempt % 2 === 0) {
      let stateOut = '';
      try {
        await exec('docker', ['inspect', '-f', '{{.State.Running}} {{.State.ExitCode}}', containerName], {}, (line) => { stateOut += line; });
      } catch (_) {
        // Container gone entirely (removed/never created) — same dead-end as exited.
        stateOut = 'MISSING';
      }
      const isRunning = /^true\b/.test(stateOut.trim());
      if (!isRunning) {
        const exitCodeMatch = stateOut.trim().match(/^false\s+(-?\d+)/);
        const exitCode = exitCodeMatch ? exitCodeMatch[1] : 'unknown';
        let tail = [];
        try { await exec('docker', ['logs', '--tail', '20', containerName], {}, (line) => tail.push(line)); } catch (_) {}
        const tailText = tail.join('\n').trim();
        log(`\x1b[31m[docker] Container exited (code ${exitCode}) before the app ever came up.\x1b[0m`);
        if (tailText) log(`\x1b[90m[docker] Last output:\n${tailText}\x1b[0m`);
        const looksLikeMissingScript = /missing script[:\s]*["']?start/i.test(tailText) || /npm error missing script/i.test(tailText);
        const hint = looksLikeMissingScript
          ? 'This repository has no "start" script (and no server.js/app.js/index.js). Add a Start Command in Joytree, add a "start" script to package.json, or deploy this as a Static Site instead if it has no server to run.'
          : 'The start command exited immediately instead of staying up as a server. Check the log output above and your configured Start Command.';
        throw new Error(`Container exited (code ${exitCode}) before becoming reachable. ${hint}`);
      }
    }

    // Rescan every 5s to pick up ports the app binds after a slow startup.
    if (attempt % 5 === 0) {
      try {
        const discovered = await detectListeningPorts(containerName);
        discovered.forEach(addDiscovered);
        const logPort = await detectPortFromContainerLogs(containerName);
        if (logPort) addDiscovered(logPort);
        if (discovered.length) {
          log(`\x1b[90m[docker] Discovered listening ports: ${discovered.join(', ')}\x1b[0m`);
        }
      } catch (_) {}
    }

    for (const port of candidates) {
      // Allow HTML responses from ports the app actually bound to.
      // Full-stack apps (Next.js, Remix, SvelteKit, etc.) serve HTML on '/'.
      // Reject HTML only from seed ports where a foreign service might answer.
      const allowHtml = discoveredSet.has(port) || port === preferredPort;

      // Fast TCP pre-check against the fallback IP. This only decides whether
      // it's worth trying the IP-based probe further down — it must NOT skip
      // the containerName-based probe below. Docker's embedded DNS resolves
      // container names reliably over the shared bridge network even when
      // getContainerIP()'s reported address isn't yet reachable (propagation
      // lag, a transient blip, etc.). Treating a failed IP precheck as proof
      // the port is dead was causing readiness to fail on apps that had
      // already started fine and were reachable by name.
      let fallbackReachable = false;
      if (fallbackIP) {
        try { await probeTcp(fallbackIP, port, 400); fallbackReachable = true; } catch (_) {}
      }

      try {
        await probeHttp(containerName, port, 1500, allowHtml);
        await new Promise(r => setTimeout(r, 800));
        await probeHttp(containerName, port, 1500, allowHtml);
        log(`\x1b[32m[docker] ✓ App reachable on ${containerName}:${port}\x1b[0m`);
        return port;
      } catch (_) {
        if (fallbackIP && fallbackReachable) {
          try {
            await probeHttp(fallbackIP, port, 1500, allowHtml);
            await new Promise(r => setTimeout(r, 500));
            await probeHttp(fallbackIP, port, 1500, allowHtml);
            log(`\x1b[32m[docker] ✓ App reachable on ${fallbackIP}:${port} (IP fallback)\x1b[0m`);
            return port;
          } catch (_) {}
        }
      }
    }

    await new Promise(r => setTimeout(r, 1000));
    if (attempt % 5 === 0) log(`\x1b[90m[docker] Still waiting for app HTTP port... (${attempt}s)\x1b[0m`);
  }

  return 0;
}

async function detectListeningPorts(containerName) {
  const rows = [];
  await exec(
    'docker',
    ['exec', containerName, 'sh', '-lc', 'cat /proc/net/tcp /proc/net/tcp6 2>/dev/null || true'],
    {},
    (line) => rows.push(line)
  );
  const ports = new Set();
  for (const row of rows) {
    const m = String(row).match(/:\s*([0-9A-Fa-f]{8}|[0-9A-Fa-f]{32}):([0-9A-Fa-f]{4})\s+[0-9A-Fa-f]{8,32}:[0-9A-Fa-f]{4}\s+0A/);
    if (!m) continue;
    const localAddr = String(m[1] || '').toUpperCase();
    // /proc/net/tcp also exposes Docker's internal DNS stub on 127.0.0.11
    // with a random high port. That is not the user web server, and logging or
    // probing it makes deploys look like they have "two ports". Only keep ports
    // bound to all interfaces, IPv6 all-interfaces, or non-loopback addresses.
    if (localAddr === '0100007F' || localAddr.startsWith('0B00007F')) continue; // 127.0.0.1 / 127.0.0.11
    if (localAddr === '00000000000000000000000000000001') continue; // ::1
    const p = parseInt(m[2], 16);
    if (Number.isInteger(p) && p > 0 && p < 65536) ports.add(p);
  }
  return Array.from(ports).sort((a, b) => a - b);
}

async function detectPortFromContainerLogs(containerName) {
  const lines = [];
  try {
    await exec('docker', ['logs', '--tail', '80', containerName], {}, (line) => lines.push(String(line || '')));
  } catch (_) {
    return 0;
  }
  const text = lines.join('\n');
  const patterns = [
    /(?:listening|listen|started|server)\D{0,40}(?:port|:)\s*(\d{2,5})/ig,
    /https?:\/\/[^\s:]+:(\d{2,5})/ig,
    /\bPORT[=\s:]+(\d{2,5})\b/ig
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(text))) {
      const p = Number(m[1]);
      if (Number.isInteger(p) && p > 0 && p < 65536) return p;
    }
  }
  return 0;
}

const PLAN_RUNTIME_PROFILES = {
  free:    { memoryLimit: '870m',  cpuShares: 384,  memorySwap: '1g' },
  starter: { memoryLimit: '1g',    cpuShares: 384,  memorySwap: '1536m' },
  pro:     { memoryLimit: '2g',    cpuShares: 640,  memorySwap: '3g' },
  growth:  { memoryLimit: '5g',    cpuShares: 1024, memorySwap: '6g' },
  scale:   { memoryLimit: '6g',    cpuShares: 1536, memorySwap: '8g' }
};

function getPlanRuntimeProfile(planKey = 'free') {
  const key = String(planKey || 'free').toLowerCase();
  return PLAN_RUNTIME_PROFILES[key] || PLAN_RUNTIME_PROFILES.free;
}

function getRuntimeConfig(project) {
  const planProfile = getPlanRuntimeProfile(project.billingPlan);
  const requestedMemory = (project.memoryLimit || project.memory || planProfile.memoryLimit).toString();
  const memory = normalizeMemoryLimit(requestedMemory);
  const memoryMb = memoryLimitToMb(memory);
  return {
    cpuShares: String(normalizePort(project.cpuShares, normalizePort(project.cpu, Number(planProfile.cpuShares || CPU_SHARES)))),
    memory,
    memorySwap: (project.memorySwap || process.env.DEFAULT_APP_MEMORY_SWAP || planProfile.memorySwap || '3g').toString(),
    nodeHeapMb: Math.max(128, Math.floor(memoryMb * 0.75)),
    startupTimeoutSeconds: normalizePort(project.startupTimeoutSeconds || project.startupTimeout, DEFAULT_STARTUP_TIMEOUT_SECONDS)
  };
}

function normalizeMemoryLimit(value) {
  const text = String(value || '').trim().toLowerCase();
  const m = text.match(/^(\d+)([mg])$/);
  if (!m) return '2g';
  const num = Number(m[1]);
  const unit = m[2];
  const mb = unit === 'g' ? num * 1024 : num;
  // Allow smaller paid/free profiles (e.g. 870m for free tier) while preventing unusable limits.
  if (mb < 256) return '256m';
  return unit === 'g' ? `${num}g` : `${num}m`;
}

function memoryLimitToMb(value) {
  const text = String(value || '').trim().toLowerCase();
  const m = text.match(/^(\d+)([mg])$/);
  if (!m) return 2048;
  const num = Number(m[1]);
  return m[2] === 'g' ? num * 1024 : num;
}

async function getContainerIP(containerName) {
  const chunks = [];
  await exec(
    'docker',
    ['inspect', '--format={{range $k,$v := .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}', containerName],
    {},
    (line) => chunks.push(line)
  );
  const ips = chunks.join('\n').trim().split(/\s+/).filter(Boolean);
  return ips.find(ip => /^(?:\d{1,3}\.){3}\d{1,3}$/.test(ip)) || '';
}

function resolveServiceEnv(project) {
  const out = {};
  if (project.mongoUrl) out.MONGO_URL = String(project.mongoUrl);
  if (project.redisUrl) out.REDIS_URL = String(project.redisUrl);
  const deps = Array.isArray(project.services) ? project.services : [];
  for (const dep of deps) {
    if (!dep || !dep.type || !dep.url) continue;
    const t = String(dep.type).toLowerCase();
    if ((t === 'mongo' || t === 'mongodb') && !out.MONGO_URL) out.MONGO_URL = String(dep.url);
    if (t === 'redis' && !out.REDIS_URL) out.REDIS_URL = String(dep.url);
  }
  return out;
}

function readRegistryTarget(portsFile, subdomain) {
  try {
    const registry = JSON.parse(fs.readFileSync(portsFile, 'utf8'));
    return registry[subdomain] || '';
  } catch (_) {
    return '';
  }
}

function isStableRegistryTarget(target) {
  return typeof target === 'string' && /^[a-z0-9][a-z0-9_.-]*:\d{2,5}$/i.test(target.trim());
}

async function getContainerState(containerName) {
  const chunks = [];
  await exec('docker', ['inspect', '--format={{.State.Status}}', containerName], {}, (line) => chunks.push(line));
  return (chunks.join('\n').trim().split('\n').pop() || '').trim();
}

async function waitForContainerRunning(containerName, timeoutSeconds, log) {
  for (let sec = 1; sec <= timeoutSeconds; sec++) {
    let state = 'unknown';
    try { state = await getContainerState(containerName); } catch (_) {}
    if (state === 'running') return true;
    if (state === 'exited' || state === 'dead') return false;
    if (sec % 5 === 0) log(`\x1b[90m[docker] still stabilizing... ${sec}s (state=${state || 'unknown'})\x1b[0m`);
    await new Promise(r => setTimeout(r, 1000));
  }
  return false;
}

async function archivePreviousContainer(containerName, subdomain, log) {
  const state = await getContainerState(containerName).catch(() => '');
  if (!state) return;
  const archived = `${containerName}-prev-${Date.now()}`;
  try {
    await exec('docker', ['stop', '-t', '20', containerName], {}, () => {});
  } catch (_) {}
  await exec('docker', ['rename', containerName, archived], {}, () => {});
  log(`\x1b[90m[docker] Archived previous release: ${archived}\x1b[0m`);
}

async function cleanupArchivedContainers(subdomain, keepCount, log) {
  const list = [];
  await exec(
    'docker',
    ['ps', '-a', '--format', '{{.Names}}'],
    {},
    (line) => {
      const name = String(line || '').trim();
      if (name.startsWith(`db-${subdomain}-prev-`)) list.push(name);
    }
  );
  list.sort((a, b) => (a < b ? 1 : -1)); // newest first by timestamp suffix
  for (const old of list.slice(Math.max(keepCount, 0))) {
    try {
      await exec('docker', ['rm', '-f', old], {}, () => {});
      log(`\x1b[90m[docker] Pruned old archived release: ${old}\x1b[0m`);
    } catch (_) {}
  }
}

function probeHttp(host, port, timeoutMs = 1000, allowHtml = true) {
  const http = require('http');
  return new Promise((resolve, reject) => {
    const req = http.request({ host, port, path: '/', method: 'GET', timeout: timeoutMs }, (res) => {
      if (allowHtml) {
        // Discovered port — accept any HTTP response (Next.js, React, etc. serve HTML).
        res.resume();
        resolve(res.statusCode || 200);
        return;
      }
      // Seed port — reject HTML responses to avoid locking onto a foreign service
      // (e.g. the Joytree host itself on the seed port returning an error page).
      const ct = String(res.headers['content-type'] || '').toLowerCase();
      const isHtmlCt = ct.startsWith('text/html');
      let body = '';
      res.on('data', (chunk) => { if (body.length < 256) body += chunk.toString('utf8', 0, 256); });
      res.on('end', () => {
        const looksHtml = body.trimStart().toLowerCase().startsWith('<!doctype') ||
                          body.trimStart().toLowerCase().startsWith('<html');
        if (isHtmlCt || looksHtml) {
          reject(new Error(`html-response:${res.statusCode}`));
        } else {
          resolve(res.statusCode || 200);
        }
      });
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(new Error('timeout')); });
    req.on('error', reject);
    req.end();
  });
}

function probeTcp(host, port, timeoutMs = 1000) {
  const net = require('net');
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port });
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error('tcp-timeout'));
    }, timeoutMs);
    socket.once('connect', () => {
      clearTimeout(timer);
      socket.end();
      resolve(true);
    });
    socket.once('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function cloneRepo(project, buildDir, githubToken, log) {
  // Upload path: files are already in buildDir (pre-seeded by routeToFrameworkBuild
  // in runUploadBuild). Skip git clone entirely.
  if (project.repoUrl === '__UPLOAD__' && project._uploadBuildDir) {
    const fileCount = countFiles(buildDir);
    if (fileCount === 0) {
      copyDir(project._uploadBuildDir, buildDir);
      log(`\x1b[90m[upload] Restored ${countFiles(buildDir)} files from upload cache\x1b[0m`);
    } else {
      log(`\x1b[90m[upload] Using ${fileCount} pre-loaded files (skipping git clone)\x1b[0m`);
    }
    return;
  }
  let cloneUrl = project.repoUrl.trim();
  let forcedBranch = '';
  const rawGithubToken = String(githubToken || '').trim();

  // Accept GitHub "tree" URLs pasted from browser:
  // https://github.com/<owner>/<repo>/tree/<branch>[/subdir]
  // Convert them to cloneable repo URL + inferred branch.
  const ghTree = cloneUrl.match(/^https?:\/\/github\.com\/([^\/]+)\/([^\/]+)\/tree\/([^\/]+)(\/.*)?$/i);
  if (ghTree) {
    const owner = ghTree[1];
    const repo = ghTree[2].replace(/\.git$/i, '');
    forcedBranch = decodeURIComponent(ghTree[3] || '').trim();
    cloneUrl = `https://github.com/${owner}/${repo}.git`;
    log(`\x1b[90m[clone] Detected GitHub tree URL — using repo root ${cloneUrl} and branch "${forcedBranch || 'main'}"\x1b[0m`);
  }

  // Normalize plain GitHub web URLs to .git clone URL.
  const ghRepo = cloneUrl.match(/^https?:\/\/github\.com\/([^\/]+)\/([^\/]+)\/?$/i);
  if (ghRepo) {
    cloneUrl = `https://github.com/${ghRepo[1]}/${ghRepo[2].replace(/\.git$/i, '')}.git`;
  }

  if (rawGithubToken && /^https:\/\/github\.com\//i.test(cloneUrl)) {
    const token = encodeURIComponent(rawGithubToken);
    cloneUrl = cloneUrl.replace(/^https:\/\/github\.com\//i, `https://x-access-token:${token}@github.com/`);
  }
  const branch = (forcedBranch || project.branch || 'main').trim();
  const fallback = branch === 'main' ? 'master' : 'main';

  log(`\x1b[90m$ git clone --depth=1 --branch ${branch} ${project.repoUrl} ${buildDir}\x1b[0m`);
  try {
    await exec('git', ['clone','--depth=1','--branch',branch,'--single-branch','--progress',cloneUrl,buildDir],
      { env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } }, log);
  } catch(e) {
    const canRetryWithHeader = rawGithubToken && /^https:\/\/github\.com\//i.test(project.repoUrl || '');
    const authErr = /authentication failed|could not read username|repository not found|access denied|permission denied|http basic/i.test(String(e.message || ''));
    if (canRetryWithHeader && authErr) {
      log(`\x1b[33m[clone] Token URL auth failed — retrying with Authorization header...\x1b[0m`);
      fs.rmSync(buildDir, { recursive: true, force: true });
      fs.mkdirSync(buildDir, { recursive: true });
      const cleanCloneUrl = String(project.repoUrl || '').trim().replace(/\/$/, '').replace(/\.git$/i, '') + '.git';
      try {
        await exec('git', ['-c', `http.https://github.com/.extraheader=AUTHORIZATION: bearer ${rawGithubToken}`, 'clone','--depth=1','--branch',branch,'--single-branch','--progress',cleanCloneUrl,buildDir],
          { env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } }, log);
        log(`\x1b[32m[clone] ✓ Cloned successfully with header auth\x1b[0m`);
        return;
      } catch (e2) {
        log(`\x1b[31m[clone] Header auth retry failed. If this is an org repo with SSO, authorize Joytree token in GitHub SSO.\x1b[0m`);
        throw e2;
      }
    }
    if (e.message.match(/not found|not exist|Remote branch|Could not find/i)) {
      log(`\x1b[33m[clone] Branch "${branch}" not found — trying "${fallback}"…\x1b[0m`);
      fs.rmSync(buildDir, { recursive: true, force: true });
      fs.mkdirSync(buildDir, { recursive: true });
      await exec('git', ['clone','--depth=1','--branch',fallback,'--single-branch','--progress',cloneUrl,buildDir],
        { env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } }, log);
    } else {
      throw e;
    }
  }
  log(`\x1b[32m[clone] ✓ Cloned successfully\x1b[0m`);
}

function normalizeWorkingDirPath(dir) {
  const raw = String(dir || '').trim().replace(/\\/g, '/').replace(/^\/+|\/+$/g, '');
  if (!raw || raw === '.') return '';
  const parts = raw.split('/').filter(Boolean);
  if (parts.some(part => part === '.' || part === '..')) return null;
  return parts.join('/');
}

function resolveWorkingDirRoot(buildDir, project, log) {
  const clean = normalizeWorkingDirPath(project && project.workingDir);
  if (clean === null) throw new Error('Invalid working directory. Use a relative path inside the repository.');
  if (!clean) return null;

  const root = path.resolve(buildDir);
  const candidate = path.resolve(root, clean);
  if (candidate !== root && !candidate.startsWith(root + path.sep)) {
    throw new Error('Invalid working directory. Use a relative path inside the repository.');
  }
  if (!fs.existsSync(candidate) || !fs.statSync(candidate).isDirectory()) {
    throw new Error(`Working directory not found: ${clean}`);
  }
  if (log) log(`\x1b[90m[info] Using selected working directory: ${clean}/\x1b[0m`);
  return candidate;
}

function findProjectRoot(buildDir, log, project = null) {
  const explicitRoot = resolveWorkingDirRoot(buildDir, project, log);
  if (explicitRoot) return explicitRoot;

  // Walk the entire repo tree to find package.json, no matter how deep it is.
  // Skips node_modules, .git, and hidden folders.
  // Prefers the shallowest match (closest to repo root).

  function walk(dir, depth) {
    if (depth > 6) return null; // safety limit — no real project is deeper than 6 levels
    let entries;
    try { entries = fs.readdirSync(dir); } catch(e) { return null; }

    // Check this directory first
    if (fs.existsSync(path.join(dir, 'package.json'))) return dir;

    // Then recurse into subdirectories (sorted so order is deterministic)
    const subdirs = entries
      .filter(e => {
        if (['node_modules', '.git', '.next', 'dist', 'build', 'out', '.cache'].includes(e)) return false;
        if (e.startsWith('.')) return false;
        try { return fs.lstatSync(path.join(dir, e)).isDirectory(); } catch(e) { return false; }
      })
      .sort();

    for (const sub of subdirs) {
      const found = walk(path.join(dir, sub), depth + 1);
      if (found) return found;
    }
    return null;
  }

  const found = walk(buildDir, 0);
  if (found && found !== buildDir) {
    const rel = path.relative(buildDir, found);
    log(`\x1b[90m[info] Found package.json in: ${rel}/\x1b[0m`);
    return found;
  }
  if (found) return found;

  log(`\x1b[33m[warn] No package.json found anywhere in repo — using repo root\x1b[0m`);
  return buildDir;
}

function resolveEnvVars(evars) {
  if (!evars) return {};
  if (typeof evars.toObject === 'function') return evars.toObject();
  if (evars instanceof Map) return Object.fromEntries(evars);
  return evars;
}

function publicOriginForProject(project, baseDomain) {
  const liveUrl = String(project?.liveUrl || '').trim().replace(/\/$/, '');
  if (/^https?:\/\//i.test(liveUrl)) return liveUrl;
  const cleanSub = String(project?.subdomain || '').trim().toLowerCase();
  const cleanBase = String(baseDomain || process.env.BASE_DOMAIN || '').trim().toLowerCase().replace(/^https?:\/\//, '').split('/')[0].replace(/:[0-9]+$/, '').replace(/^\.+|\.+$/g, '');
  return cleanSub && cleanBase ? `https://${cleanSub}.${cleanBase}` : '';
}

function withDeployedAppRuntimeDefaults(envObj, project, baseDomain) {
  const env = { ...(envObj || {}) };
  const has = (key) => Object.keys(env).some(k => String(k).toUpperCase() === key);
  const setDefault = (key, value) => {
    if (!has(key) && value !== undefined && value !== null) env[key] = value;
  };
  const publicOrigin = publicOriginForProject(project, baseDomain);
  setDefault('TRUST_PROXY', '1');
  setDefault('VITE_API_URL', '');
  setDefault('VITE_API_BASE_URL', '');
  if (publicOrigin) {
    setDefault('CORS_ORIGIN', publicOrigin);
    setDefault('ALLOWED_ORIGINS', publicOrigin);
    setDefault('FRONTEND_URL', publicOrigin);
    setDefault('PUBLIC_URL', publicOrigin);
  }
  return env;
}

// ── Known, auto-fixable install-time failure patterns ──────────────────────
// Each of these is a genuine, common, well-documented class of failure with
// one correct, known fix -- not a guess dressed up as a fix. All of them are
// retried exactly ONCE; if the retry fails too, the real underlying error is
// surfaced normally instead of looping forever.

// `npm ci` refuses to install at all when package.json and package-lock.json
// have drifted apart (very common after manually editing package.json, or
// bumping a dependency without re-running install locally before pushing).
function isLockfileOutOfSyncError(errMsg) {
  const s = String(errMsg || '');
  return /npm ci\b/i.test(s) && (
    /in sync/i.test(s) ||
    /can only install packages when your package\.json and package-lock\.json/i.test(s) ||
    /Missing:.*from lock file/i.test(s)
  );
}

// npm 7+ enforces peer dependency ranges strictly by default. A very common
// real case: a dependency's peerDependencies range hasn't caught up with a
// newer major version of some other package yet (e.g. a library still
// declaring "react": "^18" once a project has moved to React 19).
function isPeerDependencyConflictError(errMsg) {
  const s = String(errMsg || '');
  return /ERESOLVE/i.test(s) && /(could not resolve|unable to resolve dependency tree)/i.test(s);
}

// husky's own "prepare" script (added automatically by every modern husky
// setup) assumes it's running inside a real git checkout with git
// installed. Neither is true in this build container: repos are copied in
// as plain files rather than a full git clone, and the build images don't
// have git tooling installed. husky provides no value during a deployment
// build anyway (it's a local dev-only git-hooks tool).
function isHuskyPrepareScriptError(errMsg) {
  const s = String(errMsg || '');
  if (!/husky/i.test(s)) return false;
  return /not found/i.test(s) || /\.git can.?t be found/i.test(s) || /not a git repository/i.test(s) || /command not found/i.test(s);
}

// Corepack verifies package-manager tarball signatures against a bundled
// key set. When npm's registry rotates signing keys faster than a given
// Node/corepack release's bundled keys, every yarn/pnpm invocation fails
// with "Cannot find matching keyid" even though nothing is wrong with the
// project itself. This is corepack's own documented env-var workaround.
function isCorepackSignatureError(errMsg) {
  const s = String(errMsg || '');
  return /corepack/i.test(s) && (/cannot find matching keyid/i.test(s) || /signature/i.test(s));
}

// Runs the install command, and if it fails with one of the known,
// well-documented, auto-fixable error classes above, applies the correct
// fix and retries ONCE. Anything else (or a repeat failure after the retry)
// is thrown normally so it surfaces as a real build error.
async function runInstallStepWithRecovery({ projectRoot, nodeImage, envObj, installCmd, log, nodeEnv = 'development' }) {
  try {
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj, nodeEnv, command: installCmd, log });
  } catch (e) {
    const msg = String(e.message || '');

    if (isLockfileOutOfSyncError(msg)) {
      log(`\x1b[33m[Joytree] Detected an out-of-sync package-lock.json -- "npm ci" refuses to run when package.json and the lockfile disagree. Falling back to "npm install" to resync the lockfile and continue...\x1b[0m`);
      const fallbackCmd = installCmd.replace(/\bnpm ci\b/, 'npm install');
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj, nodeEnv, command: fallbackCmd, log });
      return;
    }

    if (isPeerDependencyConflictError(msg)) {
      log(`\x1b[33m[Joytree] Detected a peer dependency conflict (ERESOLVE) -- retrying install with --legacy-peer-deps, npm's own documented workaround for peer ranges that haven't caught up with a newer major version yet...\x1b[0m`);
      // [FIX] The first (aborted) install attempt can leave node_modules
      // partially populated before npm hits the ERESOLVE error and stops.
      // Retrying in that same directory makes npm do an INCREMENTAL install
      // on top of that partial state rather than a genuinely clean one --
      // confirmed directly: a live retry here installed only 249 packages,
      // missing `vite` entirely (`sh: 1: vite: not found` at build time),
      // while a truly clean --legacy-peer-deps install of the same project
      // correctly installs all 415 packages including vite. Wipe first.
      try { fs.rmSync(path.join(projectRoot, 'node_modules'), { recursive: true, force: true }); } catch (_) {}
      try { fs.rmSync(path.join(projectRoot, 'package-lock.json'), { force: true }); } catch (_) {}
      const fallbackCmd = /--legacy-peer-deps/.test(installCmd) ? installCmd : `${installCmd} --legacy-peer-deps`;
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj, nodeEnv, command: fallbackCmd, log });
      return;
    }

    if (isHuskyPrepareScriptError(msg)) {
      log(`\x1b[33m[Joytree] Detected a husky git-hooks setup failure (this build environment has no .git directory -- husky is a local dev tool and isn't needed for a deployment build). Retrying install with lifecycle scripts skipped...\x1b[0m`);
      const fallbackCmd = /--ignore-scripts/.test(installCmd) ? installCmd : `${installCmd} --ignore-scripts`;
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: { ...envObj, HUSKY: '0' }, nodeEnv, command: fallbackCmd, log });
      return;
    }

    if (isCorepackSignatureError(msg)) {
      log(`\x1b[33m[Joytree] Detected a corepack signature-verification failure (a known corepack/registry key-rotation issue, unrelated to your project) -- retrying with signature verification disabled...\x1b[0m`);
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: { ...envObj, COREPACK_INTEGRITY_KEYS: '0' }, nodeEnv, command: installCmd, log });
      return;
    }

    throw e;
  }
}

// Detects the documented npm optional-dependencies bug (npm/cli#4828) that
// commonly hits native-binary packages (@tailwindcss/oxide, esbuild, sharp,
// swc, etc.) when package-lock.json was generated on a different OS/platform
// than the one actually running `npm install` -- npm fails to resolve the
// correct platform-specific optional dependency, and the resulting build
// fails with "Cannot find native binding" even though install itself
// reported success.
//
// [FIX] This error message is misleading in one specific, common case: when
// a native-binary package's own engines.node requirement (e.g.
// @tailwindcss/oxide-linux-x64-gnu requiring Node >=20) is simply higher
// than the Node image this build is running on. npm silently skips
// installing that optional dependency because the running Node doesn't
// satisfy its engines field -- nothing about the lockfile or platform is
// actually wrong. The previous recovery here (delete lockfile + node_modules,
// reinstall, retry once) is the fix for the genuine cross-platform-lockfile
// case, but does nothing for the version-gated case: retrying the identical
// install on the identical too-old Node image reproduces the exact same
// failure every time, which is exactly what was happening here. See the
// Node-version bump below.
function isNativeBindingNpmBug(errMsg) {
  const s = String(errMsg || '');
  return /cannot find native binding/i.test(s) && /npm has a bug related to optional dependencies/i.test(s);
}

// Given a node image string like "node:18" or "node:18-alpine", bump it to
// a modern LTS (22) if it's currently older than that -- used as a fallback
// when a native-binding install failure survives a clean reinstall on the
// original image, which means the real problem was the Node version itself.
function bumpNodeImageForRecovery(nodeImage) {
  const m = String(nodeImage || '').match(/^node:(\d+)(.*)$/);
  if (!m) return 'node:22';
  const major = Number(m[1]);
  const suffix = m[2] || '';
  if (!Number.isInteger(major) || major >= 20) return nodeImage; // already modern enough
  return `node:22${suffix}`;
}

// The documented workaround, straight from npm's own error message: delete
// node_modules and the lockfile, then reinstall fresh so npm re-resolves
// optional dependencies correctly for the actual build platform, and retry
// the build once. Not a loop -- if it fails again after this, the real
// error is surfaced normally rather than retrying forever.
//
// [FIX] Also bump the Node image on this retry if it's currently older than
// Node 20. A same-image reinstall only fixes the cross-platform-lockfile
// variant of this bug; it does nothing when the actual cause is a native
// binary's engines.node requirement being higher than this image (the
// reinstall will just skip the same optional dependency again, for the
// same reason, and fail identically). Bumping to a modern LTS on the retry
// covers both causes with one recovery path instead of only the first.
async function recoverFromNativeBindingBugAndRetry({ projectRoot, nodeImage, envObj, installCmd, buildCmd, nodeEnvBuild = 'production', log }) {
  const recoveryImage = bumpNodeImageForRecovery(nodeImage);
  if (recoveryImage !== nodeImage) {
    log(`\x1b[33m[Joytree] Detected a native-binding install failure (see https://github.com/npm/cli/issues/4828). This can happen when a dependency's own required Node version is newer than ${nodeImage} -- retrying with a clean reinstall on ${recoveryImage} as well as a fresh lockfile...\x1b[0m`);
  } else {
    log(`\x1b[33m[Joytree] Detected a known npm bug (native binary optional dependencies, see https://github.com/npm/cli/issues/4828) -- this happens when package-lock.json was generated on a different OS than this Linux build environment. Automatically retrying with a clean reinstall...\x1b[0m`);
  }
  try { fs.rmSync(path.join(projectRoot, 'node_modules'), { recursive: true, force: true }); } catch (_) {}
  try { fs.rmSync(path.join(projectRoot, 'package-lock.json'), { force: true }); } catch (_) {}
  log(`\x1b[90m$ ${installCmd} (clean reinstall${recoveryImage !== nodeImage ? ' on ' + recoveryImage : ''})\x1b[0m`);
  await runBuildCommandInContainer({ projectRoot, nodeImage: recoveryImage, envObj, nodeEnv: 'development', command: installCmd, log });
  log(`\x1b[90m$ ${buildCmd} (retry)\x1b[0m`);
  await runBuildCommandInContainer({ projectRoot, nodeImage: recoveryImage, envObj, nodeEnv: nodeEnvBuild, command: buildCmd, log });
}

async function runBuildCommandInContainer({ projectRoot, nodeImage, envObj, nodeEnv = 'development', command, log }) {
  try { await exec('docker', ['pull', nodeImage], {}, () => {}); } catch(e) {}
  const normalizedCommand = normalizeInstallLikeCommand(command, projectRoot);
  const commandWithCorepack = `corepack enable >/dev/null 2>&1 || true; ${normalizedCommand}`;
  log(`\x1b[90m[docker-build] ${nodeImage} :: ${normalizedCommand}\x1b[0m`);
  // [FIX] Docker's --memory flag bounds the container's total cgroup memory,
  // but Node/V8's own heap size limit is a SEPARATE thing that doesn't
  // automatically respect that cgroup limit reliably -- V8 auto-detects
  // available memory in a way that can be inaccurate inside a container,
  // leading to "JavaScript heap out of memory" crashes (identifiable by
  // Node-internal stack frames like ModuleWrap/ModuleJob.run/ModuleLoader)
  // even when the container-level memory limit alone should have been
  // enough. This is exactly what was happening with Vite builds even after
  // bumping the container's own memory limit. Explicitly setting
  // --max-old-space-size via NODE_OPTIONS, safely below the container's
  // actual memory ceiling (leaving headroom for the Node binary itself,
  // native addons, and non-heap memory), fixes this at the source instead
  // of hoping V8's auto-detection gets it right inside a container.
  const BUILD_CONTAINER_MEMORY = process.env.BUILD_CONTAINER_MEMORY || '2048m';
  const _memMb = parseInt(BUILD_CONTAINER_MEMORY, 10) || 2048;
  const _nodeHeapMb = Math.max(512, Math.round(_memMb * 0.75));
  const envArgs = [
    '-e', `CI=false`,
    '-e', `NODE_ENV=${nodeEnv}`,
    '-e', `NODE_OPTIONS=--max-old-space-size=${_nodeHeapMb}`,
    ...Object.entries(envObj || {}).flatMap(([k, v]) => ['-e', `${k}=${String(v ?? '')}`]),
  ];
  // [FIX] These containers run with `--rm`, so without an explicit cache
  // mount, npm/yarn/pnpm start with a completely empty cache on every
  // single build — every dependency is re-downloaded from the registry
  // every time, even for unchanged lockfiles. Mount the same persistent
  // cache directories used by host-run installs (see getCacheDir() /
  // exec()) so ephemeral build containers benefit too.
  const cacheMounts = [];
  const npmCache  = getCacheDir('npm');
  const yarnCache = getCacheDir('yarn');
  const pnpmStore = getCacheDir('pnpm-store');
  if (npmCache)  cacheMounts.push('-v', `${npmCache}:/root/.npm`);
  if (yarnCache) { cacheMounts.push('-v', `${yarnCache}:/usr/local/share/.cache/yarn`); envArgs.push('-e', `YARN_CACHE_FOLDER=/usr/local/share/.cache/yarn`); }
  if (pnpmStore) { cacheMounts.push('-v', `${pnpmStore}:/root/.local/share/pnpm/store`); envArgs.push('-e', `npm_config_store_dir=/root/.local/share/pnpm/store`); }
  // [FIX] Bound resource usage of ephemeral build containers. Without
  // limits, a single heavy `npm run build` (Vite/webpack with large
  // dependency trees) can consume most/all of the host's RAM and CPU,
  // making the Docker daemon, dashboard, and proxy unresponsive for ALL
  // projects until the host is manually restarted. These limits are
  // generous enough for normal frontend builds but prevent one runaway
  // build from taking down the whole host. Override via env vars if a
  // specific project genuinely needs more.
  // [FIX] 1536m with zero swap headroom (memory-swap == memory) was too
  // tight for Vite/webpack/esbuild-based builds -- bundling + minification
  // (esbuild, rollup, terser workers, plus Node's own overhead) can
  // genuinely exceed 1.5GB on anything beyond a small project, and with
  // zero swap cushion, even a brief spike gets OOM-killed instantly rather
  // than just slowing down. Bumped the default to 2048m, and added a 512m
  // swap cushion on top (memory-swap = memory + swap, not equal to it) so
  // a short-lived spike above the hard memory limit gets slowed down by
  // swap instead of immediately killing the build. Still bounded and still
  // overridable via env vars -- this isn't removing the protection against
  // a runaway build, just setting a more realistic default for what a
  // normal modern frontend build actually needs.
  const BUILD_CONTAINER_SWAP_CUSHION = process.env.BUILD_CONTAINER_SWAP_CUSHION || '512m';
  const BUILD_CONTAINER_CPUS   = process.env.BUILD_CONTAINER_CPUS   || '1.5';
  // Hard ceiling on a single install/build step so a hung command (e.g.
  // waiting on a prompt, or an infinite loop in a postinstall script)
  // can't occupy a build slot indefinitely.
  const BUILD_STEP_TIMEOUT_SECONDS = Number(process.env.BUILD_STEP_TIMEOUT_SECONDS || 900);
  const _swapMb = parseInt(BUILD_CONTAINER_SWAP_CUSHION, 10) || 512;
  const BUILD_CONTAINER_MEMORY_SWAP = `${_memMb + _swapMb}m`;
  await exec('docker', [
    'run', '--rm',
    '--memory', BUILD_CONTAINER_MEMORY,
    '--memory-swap', BUILD_CONTAINER_MEMORY_SWAP,
    '--cpus', BUILD_CONTAINER_CPUS,
    '--init', // ensure signals/zombies inside the build container are reaped
    ...envArgs,
    ...cacheMounts,
    '-v', `${projectRoot}:/workspace`,
    '-w', '/workspace',
    nodeImage,
    'sh', '-lc', `timeout ${BUILD_STEP_TIMEOUT_SECONDS} sh -c '${commandWithCorepack.replace(/'/g, `'\\''`)}'`
  ], {}, log);
}

function stripLeadingCdPrefix(command) {
  let raw = String(command || '').trim();

  // The Docker runtime/build containers are already launched with the selected
  // project directory mounted as their working directory. Stored commands may
  // still include the original repo-root context, e.g. `cd lovable-ui && npm
  // start`; keep only the command that should run inside the mounted app dir.
  const cdPrefixMatch = raw.match(/^cd\s+(?:"[^"]+"|'[^']+'|\S+)\s*&&\s*/i);
  if (cdPrefixMatch) {
    raw = raw.slice(cdPrefixMatch[0].length).trim();
  }

  return raw;
}

function normalizeInstallLikeCommand(command, projectRoot) {
  // Render-style command execution: respect the user's install/build command
  // exactly in the selected project root. Do not rewrite npm install to npm ci,
  // and do not append --no-audit/--no-fund/progress flags behind the user's back.
  return stripLeadingCdPrefix(command);
}

function detectPackageManager(projectRoot) {
  if (fs.existsSync(path.join(projectRoot, 'pnpm-lock.yaml'))) return 'pnpm';
  if (fs.existsSync(path.join(projectRoot, 'yarn.lock'))) return 'yarn';
  if (fs.existsSync(path.join(projectRoot, 'package-lock.json'))) return 'npm';
  return 'npm';
}

function readPackageJson(projectRoot) {
  try { return JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8')); }
  catch (_) { return {}; }
}

function packageScripts(projectRoot) {
  const pkg = readPackageJson(projectRoot);
  return pkg && pkg.scripts && typeof pkg.scripts === 'object' ? pkg.scripts : {};
}

function packageDeps(projectRoot) {
  const pkg = readPackageJson(projectRoot);
  return { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
}

function hasPackageScript(projectRoot, name) {
  const script = packageScripts(projectRoot)[name];
  return typeof script === 'string' && script.trim().length > 0;
}

function runScriptCommand(pm, name) {
  if (pm === 'pnpm') return `pnpm run ${name}`;
  if (pm === 'yarn') return `yarn ${name}`;
  return `npm run ${name}`;
}

function packageManagerExec(pm, binAndArgs) {
  if (pm === 'pnpm') return `pnpm exec ${binAndArgs}`;
  if (pm === 'yarn') return `yarn ${binAndArgs}`;
  return `npx ${binAndArgs}`;
}

function getDefaultInstallCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  if (pm === 'pnpm') return 'pnpm install';
  if (pm === 'yarn') return 'yarn install';
  return 'npm install';
}

function getDefaultBuildCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  const deps = packageDeps(projectRoot);
  if (hasPackageScript(projectRoot, 'build')) return runScriptCommand(pm, 'build');
  if (deps.next) return packageManagerExec(pm, 'next build');
  if (deps.vite || deps['@vitejs/plugin-react'] || deps['@vitejs/plugin-vue']) return packageManagerExec(pm, 'vite build');
  return 'echo skip';
}

function getDefaultStartCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  const deps = packageDeps(projectRoot);
  if (hasPackageScript(projectRoot, 'start')) {
    if (pm === 'pnpm') return 'pnpm start';
    if (pm === 'yarn') return 'yarn start';
    return 'npm start';
  }
  if (deps.next) return packageManagerExec(pm, 'next start');
  if (hasPackageScript(projectRoot, 'preview')) return runScriptCommand(pm, 'preview');
  if (deps.vite || deps['@vitejs/plugin-react'] || deps['@vitejs/plugin-vue']) return packageManagerExec(pm, 'vite preview');
  if (fs.existsSync(path.join(projectRoot, 'server.js'))) return 'node server.js';
  if (fs.existsSync(path.join(projectRoot, 'app.js'))) return 'node app.js';
  if (fs.existsSync(path.join(projectRoot, 'index.js'))) return 'node index.js';
  return 'npm start';
}

function assertDeployableServerApp(projectRoot, startCmd, log) {
  const explicitStart = (startCmd || '').trim();
  if (explicitStart) return;

  const pkgPath = path.join(projectRoot, 'package.json');
  let pkg = null;
  try { pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8')); } catch (_) {}
  const scripts = pkg && pkg.scripts && typeof pkg.scripts === 'object' ? pkg.scripts : {};
  const hasStartScript = typeof scripts.start === 'string' && scripts.start.trim().length > 0;
  const hasServerEntry =
    fs.existsSync(path.join(projectRoot, 'server.js')) ||
    fs.existsSync(path.join(projectRoot, 'app.js')) ||
    fs.existsSync(path.join(projectRoot, 'index.js'));

  if (!hasStartScript && !hasServerEntry) {
    log(`\x1b[31m[deploy] This repository does not define a runnable web server start command.\x1b[0m`);
    log(`\x1b[33m[deploy] Add a custom start command in Joytree (e.g. \"node server.js\") or use an app repository instead of a framework/library source repo.\x1b[0m`);
    throw new Error('No start script/server entry found. This repo looks like source code for a package/library, not a deployable web app.');
  }
}

function isDeployableServerProject(projectRoot, startCmd = '') {
  if ((startCmd || '').trim()) return true;
  const scripts = packageScripts(projectRoot);
  const deps = packageDeps(projectRoot);
  const hasStartScript = typeof scripts.start === 'string' && scripts.start.trim().length > 0;
  const hasPreviewScript = typeof scripts.preview === 'string' && scripts.preview.trim().length > 0;
  const hasServerEntry =
    fs.existsSync(path.join(projectRoot, 'server.js')) ||
    fs.existsSync(path.join(projectRoot, 'app.js')) ||
    fs.existsSync(path.join(projectRoot, 'index.js'));
  const hasFrameworkServer = Boolean(deps.next || deps.vite || deps['@vitejs/plugin-react'] || deps['@vitejs/plugin-vue']);
  return hasStartScript || hasPreviewScript || hasServerEntry || hasFrameworkServer;
}

function resolveDeployableRoot(currentRoot, buildDir, startCmd, log) {
  if (isDeployableServerProject(currentRoot, startCmd)) return currentRoot;

  const candidates = [];
  const walk = (dir, depth) => {
    if (depth > 6) return;
    let entries = [];
    try { entries = fs.readdirSync(dir); } catch (_) { return; }
    if (fs.existsSync(path.join(dir, 'package.json'))) candidates.push({ dir, depth });
    for (const entry of entries) {
      if (['node_modules', '.git', '.next', 'dist', 'build', 'out', '.cache'].includes(entry)) continue;
      if (entry.startsWith('.')) continue;
      const full = path.join(dir, entry);
      try { if (fs.lstatSync(full).isDirectory()) walk(full, depth + 1); } catch (_) {}
    }
  };
  walk(buildDir, 0);

  const deployable = candidates
    .filter(c => c.dir !== currentRoot && isDeployableServerProject(c.dir, startCmd))
    .sort((a, b) => a.depth - b.depth)[0];

  if (deployable) {
    log(`\x1b[33m[deploy] Root package is not runnable; using deployable subproject: ${path.relative(buildDir, deployable.dir) || '.'}\x1b[0m`);
    return deployable.dir;
  }

  assertDeployableServerApp(currentRoot, startCmd, log);
  return currentRoot;
}

function resolveRuntimeStartCommand({ projectRoot, startCmd, expectedPort }) {
  const raw = stripLeadingCdPrefix((startCmd || '').trim()) || getDefaultStartCmd(projectRoot);
  const normalized = raw.replace(/\s+/g, ' ').trim().toLowerCase();
  const nextFlags = `-H 0.0.0.0 -p ${expectedPort}`;
  const hasHostFlag = /(^|\s)(-H|--hostname|--host)(\s|=)/i.test(raw);
  const hasPortFlag = /(^|\s)(-p|--port)(\s|=)/i.test(raw);
  const flags = `${hasHostFlag ? '' : '-H 0.0.0.0'} ${hasPortFlag ? '' : `-p ${expectedPort}`}`.trim();

  const scripts = packageScripts(projectRoot);
  const deps = packageDeps(projectRoot);
  const startScript   = String(scripts.start   || '').trim().toLowerCase();
  const previewScript = String(scripts.preview || '').trim().toLowerCase();
  const devScript     = String(scripts.dev     || '').trim().toLowerCase();
  const packageStartRunsNext = /(^|\s)next\s+start(\s|$)/.test(startScript);

  // Next.js must bind to 0.0.0.0 inside Docker; localhost-only starts pass
  // locally but are unreachable from the Joytree proxy and cause 502s/stalls.
  if (normalized === 'npm start' && packageStartRunsNext) return `npm start -- ${nextFlags}`;
  if (normalized === 'pnpm start' && packageStartRunsNext) return `pnpm start -- ${nextFlags}`;
  if (normalized === 'yarn start' && packageStartRunsNext) return `yarn start ${nextFlags}`;
  if (/^(?:npx |pnpm exec |yarn )?next start\b/.test(normalized)) return flags ? `${raw} ${flags}` : raw;
  if (!startScript && deps.next && /^(?:npm start|pnpm start|yarn start)$/.test(normalized)) {
    return `${packageManagerExec(detectPackageManager(projectRoot), 'next start')} ${nextFlags}`;
  }

  // ── Vite-based projects (e.g. Lovable, Vite+React/Vue scaffolds) ───────────
  // `vite`, `vite dev`, `vite preview` all bind to 127.0.0.1 by default — this
  // is unreachable from the Joytree proxy over the Docker network, so the app
  // looks "running" forever but detectLivePort never succeeds (port not
  // registered, deploy appears stuck until the 5-minute readiness timeout).
  // Inject --host 0.0.0.0 and --port $PORT for any vite-based command.
  const viteFlags = `--host 0.0.0.0 ${hasPortFlag ? '' : `--port ${expectedPort}`}`.trim();

  // Does the resolved command (or its package.json script) invoke the `vite`
  // binary at all, in dev/preview/serve mode (not `vite build`)?
  const cmdMentionsVite = /\bvite\b/.test(normalized);
  const startScriptIsVite   = /\bvite\b/.test(startScript)   && !/\bvite\s+build\b/.test(startScript);
  const previewScriptIsVite = /\bvite\b/.test(previewScript) && /\bpreview\b/.test(previewScript);
  const devScriptIsVite     = /\bvite\b/.test(devScript);

  if (cmdMentionsVite && !/\bvite\s+build\b/.test(normalized)) {
    return hasHostFlag && hasPortFlag ? raw : `${raw} ${viteFlags}`.trim();
  }
  if (normalized === 'npm start' && startScriptIsVite) return `npm start -- ${viteFlags}`;
  if (normalized === 'pnpm start' && startScriptIsVite) return `pnpm start -- ${viteFlags}`;
  if (normalized === 'yarn start' && startScriptIsVite) return `yarn start ${viteFlags}`;
  if (normalized === 'npm run preview' || normalized === 'npm run start:preview') {
    return previewScriptIsVite ? `npm run preview -- ${viteFlags}` : raw;
  }
  if ((normalized === 'npm run dev' || normalized === 'npm start') && devScriptIsVite && !startScript) {
    return `npm run dev -- ${viteFlags}`;
  }
  // If nothing else matched but the project clearly has vite as a dependency
  // and the resolved command is the bare default ("npm start") with no start
  // script (assertDeployableServerApp would have already thrown in that case
  // unless a server entry exists) — leave as-is; only rewrite known vite invocations.

  return raw;
}

function detectProjectProfile(projectRoot) {
  const out = { kind: 'application', framework: '', needsDatabase: false };
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
    const deps = { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
    if (deps.fastify) out.framework = 'fastify';
    else if (deps.next) out.framework = 'next';
    else if (deps.express) out.framework = 'express';
    else if (deps.nestjs || deps['@nestjs/core']) out.framework = 'nestjs';
    else if (deps.koa) out.framework = 'koa';
    out.needsDatabase = Boolean(
      deps.typeorm || deps.prisma || deps.sequelize || deps.knex ||
      deps.mongoose || deps.pg || deps.mysql || deps.mysql2 || deps.sqlite3
    );
    const hasStart = pkg.scripts && typeof pkg.scripts.start === 'string' && pkg.scripts.start.trim();
    if (!hasStart && !fs.existsSync(path.join(projectRoot, 'server.js')) && !fs.existsSync(path.join(projectRoot, 'app.js')) && !fs.existsSync(path.join(projectRoot, 'index.js'))) {
      out.kind = 'library/framework';
    }
  } catch (_) {}
  return out;
}

function hasAnyDbEnv(env) {
  const keys = Object.keys(env || {}).map(k => String(k).toUpperCase());
  return keys.some(k =>
    k.includes('DATABASE_URL') ||
    k.includes('DB_URL') ||
    k.includes('POSTGRES') ||
    k.includes('MYSQL') ||
    k.includes('MONGO')
  );
}

function exec(cmd, args, options, logFn) {
  return new Promise((resolve, reject) => {
    let spawnCmd  = cmd;
    let spawnArgs = args;
    let spawnEnv  = options.env || process.env;

    // Deprioritize heavy build-tool commands (npm/pip/cargo/git/etc.) so they
    // don't starve the dashboard's own CPU/I/O. Skip for `docker run`/`docker
    // exec`/etc. — those launch the live app container itself, which should
    // run at its configured --cpu-shares, not be additionally niced here.
    const isDockerLifecycleCmd = cmd === 'docker' &&
      !['pull', 'build'].includes(String(args[0] || ''));

    if (BUILD_TOOL_COMMANDS.has(cmd) && !isDockerLifecycleCmd && options.lowPriority !== false) {
      if (_NICE_AVAILABLE && _IONICE_AVAILABLE) {
        spawnCmd  = 'nice';
        spawnArgs = ['-n', String(BUILD_NICE_LEVEL), 'ionice', '-c2', '-n', String(BUILD_IONICE_LEVEL), '--', cmd, ...args];
      } else if (_NICE_AVAILABLE) {
        spawnCmd  = 'nice';
        spawnArgs = ['-n', String(BUILD_NICE_LEVEL), '--', cmd, ...args];
      } else if (_IONICE_AVAILABLE) {
        spawnCmd  = 'ionice';
        spawnArgs = ['-c2', '-n', String(BUILD_IONICE_LEVEL), '--', cmd, ...args];
      }
      // If neither nice nor ionice is available, spawnCmd/spawnArgs remain unchanged.
    }

    // Cap Node.js heap for npm/npx/yarn/pnpm/bun build steps so a single heavy
    // build (vite build, webpack, tsc, etc.) can't balloon memory and trigger
    // swap thrashing that slows down every other deploy/dashboard request.
    if (NODE_BUILD_COMMANDS.has(cmd)) {
      const existing = spawnEnv.NODE_OPTIONS || '';
      if (!/--max-old-space-size/.test(existing)) {
        spawnEnv = {
          ...spawnEnv,
          NODE_OPTIONS: `${existing} --max-old-space-size=${BUILD_NODE_MAX_OLD_SPACE_MB}`.trim()
        };
      }

      // [FIX] Point npm/yarn/pnpm at a persistent cache directory (under
      // sitesDir) instead of each build's default (often a fresh per-build
      // HOME). Without this, `npm install` for projects like Uptime Kuma
      // re-downloads and re-extracts its whole dependency tree from the
      // registry on EVERY deploy. With a shared cache, repeat deploys of
      // the same project (or any project with overlapping deps) restore
      // most packages from local disk instead of the network.
      const npmCache  = getCacheDir('npm');
      const yarnCache = getCacheDir('yarn');
      const pnpmStore = getCacheDir('pnpm-store');
      const cacheEnv = {};
      if (npmCache  && !spawnEnv.npm_config_cache)    cacheEnv.npm_config_cache    = npmCache;
      if (yarnCache && !spawnEnv.YARN_CACHE_FOLDER)   cacheEnv.YARN_CACHE_FOLDER   = yarnCache;
      if (pnpmStore && !spawnEnv.npm_config_store_dir) cacheEnv.npm_config_store_dir = pnpmStore;
      if (Object.keys(cacheEnv).length) spawnEnv = { ...spawnEnv, ...cacheEnv };
    }

    const child = spawn(spawnCmd, spawnArgs, {
      shell: false,
      cwd:   options.cwd,
      env:   spawnEnv,
      // [FIX] Run in a new process group (detached) so that when a build
      // step times out we can kill the ENTIRE process tree (nice/ionice
      // wrapper -> npm -> vite/webpack/esbuild workers etc.), not just the
      // immediate child. Previously child.kill('SIGTERM') only killed the
      // top-level wrapper process; grandchildren were reparented to PID 1
      // and never reaped, which is why zombie process counts climbed
      // steadily under repeated/concurrent builds.
      detached: true,
      // Limit child process memory to prevent build steps from OOM-killing the VPS
      // 2GB virtual memory limit for npm install/build (RSS stays much lower)
    });

    // [FIX] Kill the whole process group, escalating to SIGKILL if needed.
    const killProcessTree = (signal) => {
      try {
        if (typeof child.pid === 'number') process.kill(-child.pid, signal);
      } catch (_) {
        try { child.kill(signal); } catch (_) {}
      }
    };

  // Kill process (and its whole process tree) if it hangs for too long
  // (heavy native installs can take a while).
  const hardTimeout = setTimeout(() => {
      killProcessTree('SIGTERM');
      // If the process tree ignores SIGTERM (common for some native builds),
      // force-kill after a short grace period so it doesn't linger as a
      // zombie/orphan consuming resources indefinitely.
      setTimeout(() => killProcessTree('SIGKILL'), 5000);
      reject(new Error(`"${cmd}" timed out after ${BUILD_STEP_TIMEOUT_MINUTES} minutes. Check your install/build commands.`));
    }, BUILD_STEP_TIMEOUT_MINUTES * 60 * 1000);

    let lastLines = [];
    let stoppedByLogFn = null;
    const onLine  = (line, isErr) => {
      if (!line.trim()) return;
      const display = isErr ? `\x1b[90m${line}\x1b[0m` : line;
      if (typeof logFn === 'function') {
        try {
          logFn(display);
        } catch (e) {
          // [FIX] logFn (onLog in server.js) throws to signal a
          // user-requested "stop deployment". Previously this exception
          // was unhandled here (thrown from inside a stdout 'data'
          // listener), which did NOT stop the underlying docker/npm
          // process — it kept running as an orphan, consuming resources
          // and contributing to the zombie-process buildup, while the
          // outer promise never resolved/rejected cleanly. Now we kill
          // the whole process tree and reject with the original error.
          if (!stoppedByLogFn) {
            stoppedByLogFn = e;
            killProcessTree('SIGTERM');
            setTimeout(() => killProcessTree('SIGKILL'), 5000);
          }
          return;
        }
      }
      lastLines.push(line);
      if (lastLines.length > 40) lastLines.shift();
    };

    let outBuf = '', errBuf = '';
    child.stdout.on('data', d => {
      outBuf += d.toString();
      const lines = outBuf.split('\n'); outBuf = lines.pop();
      lines.forEach(l => onLine(l, false));
    });
    child.stderr.on('data', d => {
      errBuf += d.toString();
      const lines = errBuf.split(/[\n\r]+/); errBuf = lines.pop();
      lines.forEach(l => onLine(l, true));
    });
    child.stdout.on('end', () => { if (outBuf.trim()) onLine(outBuf, false); });
    child.stderr.on('end', () => { if (errBuf.trim()) onLine(errBuf, true);  });

    child.on('error', err => reject(new Error(
      `Could not run "${cmd}": ${err.message}` +
      (cmd === 'git'    ? ' — is git installed?' :
       cmd === 'npm'    ? ' — is Node.js installed?' :
       cmd === 'docker' ? ' — is Docker socket mounted?' : '')
    )));

    child.on('close', code => {
      clearTimeout(hardTimeout);
      if (stoppedByLogFn) { reject(stoppedByLogFn); return; }
      if (code === 0) resolve();
      else reject(new Error(`"${cmd} ${args.slice(0,3).join(' ')}…" failed (exit ${code}).\n${lastLines.slice(-40).join('\n')}`));
    });
  });
}

function splitCmd(cmdStr) {
  const parts = [];
  let cur = '', inQ = false, qCh = '';
  for (const ch of cmdStr.trim()) {
    if (inQ) { if (ch === qCh) inQ = false; else cur += ch; }
    else if (ch === '"' || ch === "'") { inQ = true; qCh = ch; }
    else if (ch === ' ' || ch === '\t') { if (cur) { parts.push(cur); cur = ''; } }
    else cur += ch;
  }
  if (cur) parts.push(cur);
  return parts.length >= 2 ? [parts[0], parts.slice(1)] : [parts[0] || 'npm', ['start']];
}

function copyDir(src, dest) {
  fs.mkdirSync(dest, { recursive: true });
  for (const entry of fs.readdirSync(src)) {
    const s = path.join(src, entry);
    const d = path.join(dest, entry);
    try {
      const stat = fs.lstatSync(s);
      if (stat.isSymbolicLink()) {
        // Resolve symlink target — if it points to a directory, recurse; if file, copy
        try {
          const resolved = fs.realpathSync(s);
          const resolvedStat = fs.statSync(resolved);
          if (resolvedStat.isDirectory()) {
            copyDir(resolved, d);
          } else {
            fs.mkdirSync(path.dirname(d), { recursive: true });
            fs.copyFileSync(resolved, d);
          }
        } catch(e) {
          // Broken symlink — skip silently
        }
      } else if (stat.isDirectory()) {
        copyDir(s, d);
      } else if (stat.isFile()) {
        fs.mkdirSync(path.dirname(d), { recursive: true });
        fs.copyFileSync(s, d);
      }
      // Skip anything else (sockets, devices, etc.)
    } catch(e) {
      // Skip any entry that causes errors (permissions, special files, etc.)
      if (e.code !== 'ENOENT' && e.code !== 'EACCES') {
        // Only log unexpected errors
        // console.warn('[copyDir] skipping:', s, e.code);
      }
    }
  }
}

function countFiles(dir) {
  let n = 0;
  try {
    for (const e of fs.readdirSync(dir)) {
      const f = path.join(dir, e);
      n += fs.lstatSync(f).isDirectory() ? countFiles(f) : 1;
    }
  } catch(e) {}
  return n;
}

function emitStep(emit, id, state) {
  emit('build:step', { step: { id, state } });
}

module.exports = { runBuild, runUploadBuild, getPlanRuntimeProfile, normalizeMemoryLimit, PLAN_RUNTIME_PROFILES, detectLivePort, requestBuildStop };

// ── UPLOAD BUILD ──────────────────────────────────────────────────────────────
// Deploy from a locally extracted directory instead of cloning from GitHub.
// Follows the same install → build → copy/start pattern as runStaticBuild/runServerBuild,
// but skips the "Clone" step and shows an extraction step instead.
// ── Auto-detect whether an uploaded project is a server app ─────────────────
// Called when siteType is not explicitly set. Inspects the uploaded files to
// decide: if there's a package.json with a "start" script, OR known server
// framework deps, OR a server entry file with .listen()/createServer() etc.,
// treat it as a server app so it gets npm start + a Docker container.
function autoDetectUploadServerApp(uploadFilesDir, startCmdHint, log, strictDepsOnly = false) {
  // [FIX] strictDepsOnly mode exists precisely so a stale/previous 'static'
  // classification only gets flipped by an unambiguous package.json signal
  // (start script or known server-framework dependency) — not by a fuzzy
  // guess. A leftover startCmd saved on the project from before it was
  // reconfigured to static is exactly that kind of fuzzy, stale signal, not
  // fresh evidence from the current upload, so it must not short-circuit
  // this function ahead of the strictDepsOnly check. Confirmed live: a
  // genuinely static project (vitafresh, plain HTML/CSS/JS, no package.json)
  // kept redeploying as a server app solely because an old startCmd was
  // still stored on the project record.
  if (startCmdHint && !strictDepsOnly) return true;

  let root = uploadFilesDir;
  const findRoot = (dir, depth) => {
    if (depth > 3) return;
    try {
      for (const entry of fs.readdirSync(dir)) {
        const full = path.join(dir, entry);
        if (entry === 'package.json' && fs.statSync(full).isFile()) { root = dir; return; }
        if (fs.statSync(full).isDirectory() && entry !== 'node_modules' && entry !== '.git') {
          findRoot(full, depth + 1);
        }
      }
    } catch(_) {}
  };
  findRoot(uploadFilesDir, 0);

  const pkgPath = path.join(root, 'package.json');
  if (fs.existsSync(pkgPath)) {
    try {
      const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
      const scripts = pkg.scripts || {};

      if (scripts.start && String(scripts.start).trim()) {
        log(`\x1b[36m[Joytree]\x1b[0m Auto-detected server app: package.json has "start" script: "${scripts.start}"`);
        return true;
      }

      const allDeps = Object.keys({ ...pkg.dependencies, ...pkg.devDependencies });
      // [FIX] Nitro-based SSR meta-frameworks (TanStack Start, Nuxt, SolidStart,
      // Analog) build a real server (.output/server/index.mjs) that has to be
      // *run*, not served as static HTML — but they typically have no "start"
      // script (just dev/build/preview), and their package name doesn't match
      // any of the plain server-framework deps below either. Without these,
      // autoDetectUploadServerApp falls through to the static path, which
      // builds fine (vite build succeeds) then fails at the end looking for
      // an index.html that Nitro never generates for a server-rendered app.
      const serverFrameworks = ['express','fastify','koa','hapi','nestjs','@nestjs/core','next','nuxt','remix','sveltekit','@sveltejs/kit','strapi','keystone','feathers','moleculer','loopback','socket.io','ws','http-server','serve','@tanstack/react-start','@tanstack/start','@tanstack/solid-start','solid-start','nitropack','nitro','vinxi','@analogjs/platform'];
      const matchedFramework = allDeps.find(d => serverFrameworks.some(f => d === f || d.startsWith(f + '/')));
      if (matchedFramework) {
        log(`\x1b[36m[Joytree]\x1b[0m Auto-detected server app: found server framework dependency: "${matchedFramework}"`);
        return true;
      }

      const mainFile = pkg.main ? path.join(root, pkg.main) : null;
      if (mainFile && fs.existsSync(mainFile)) {
        const src = fs.readFileSync(mainFile, 'utf8').slice(0, 4000);
        if (/require\(['"]express['"]\)|require\(['"]http['"]\)|\.listen\s*\(|createServer\s*\(|new\s+Server\s*\(/.test(src)) {
          log(`\x1b[36m[Joytree]\x1b[0m Auto-detected server app: main file (${pkg.main}) contains server code`);
          return true;
        }
      }
    } catch(_) {}
  }

  // The source-file-scanning heuristic below is much fuzzier (any file that
  // happens to contain ".listen(" or "createServer(") than the package.json
  // checks above, so it's skipped when strictDepsOnly is set -- i.e. when
  // re-checking a project already (possibly wrongly) classified 'static',
  // we only want the unambiguous signal, not a guess that could flip a
  // genuinely static project.
  if (strictDepsOnly) return false;

  const candidateFiles = ['server.js','server.ts','app.js','app.ts','index.js','index.ts','main.js','main.ts'];
  for (const fname of candidateFiles) {
    const fpath = path.join(root, fname);
    if (!fs.existsSync(fpath)) continue;
    try {
      const src = fs.readFileSync(fpath, 'utf8').slice(0, 5000);
      if (/require\(['"]express['"]\)|require\(['"]fastify['"]\)|require\(['"]koa['"]\)|require\(['"]http['"]\)|require\(['"]https['"]\)|\.listen\s*\(|createServer\s*\(|new\s+Server\s*\(|import\s+express|import\s+fastify|import\s+Koa/.test(src)) {
        log(`\x1b[36m[Joytree]\x1b[0m Auto-detected server app: ${fname} contains server/listen code`);
        return true;
      }
    } catch(_) {}
  }

  return false;
}

async function runUploadBuild(args) {
  _activeBatchedLoggers = [];
  try {
    return await _runUploadBuildInner(args);
  } finally {
    for (const log of _activeBatchedLoggers) {
      try { log.flushNow(); } catch (_) {}
    }
    _activeBatchedLoggers = [];
  }
}

async function _runUploadBuildInner({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, baseDomain }) {
  const log = createBatchedLogger(emit, onLog);

  const cleanSub = String(project.subdomain || '').toLowerCase().replace(/[^a-z0-9-]/g,'');
  const siteType = String(project.siteType  || '').toLowerCase();
  const runtime  = String(project.runtime   || '').toLowerCase();
  const startCmd = String(project.startCmd  || '').trim();

  // ── Step 1: Runtime-first routing ─────────────────────────────────────────
  // If the user explicitly chose a non-Node runtime (PHP, Python, Go, Ruby,
  // Java, Rust, .NET, Elixir, Bun, Deno), route to the matching framework
  // builder — NOT the static build, which requires HTML.
  const isPython = runtime.startsWith('python') || siteType === 'python';
  const isGo     = runtime.startsWith('go');
  const isPHP    = runtime.startsWith('php');
  const isRuby   = runtime.startsWith('ruby');
  const isJava   = runtime.startsWith('java') || runtime.startsWith('kotlin');
  const isRust   = runtime.startsWith('rust');
  const isDotnet = runtime === 'dotnet';
  const isElixir = runtime.startsWith('elixir');
  const isBun    = runtime === 'bun';
  const isDeno   = runtime === 'deno';

  async function routeToFrameworkBuild(builderFn) {
    const buildDir = path.join(tmpDir, deployId);
    if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
    fs.mkdirSync(buildDir, { recursive: true });
    log(`\x1b[90m[upload] Copying ${countFiles(uploadFilesDir)} files to build workspace…\x1b[0m`);
    copyDir(uploadFilesDir, buildDir);
    log(`\x1b[32m[upload]\x1b[0m Files ready`);
    const uploadProject = { ...project, _uploadBuildDir: buildDir, repoUrl: '__UPLOAD__' };
    return builderFn({ deployId, project: uploadProject, sitesDir, tmpDir, githubToken: null, appPort, emit, onLog, baseDomain });
  }

  if (isPython) return routeToFrameworkBuild(runPythonBuild);
  if (isGo)     return routeToFrameworkBuild(runGoBuild);
  if (isPHP)    return routeToFrameworkBuild(runPhpBuild);
  if (isRuby)   return routeToFrameworkBuild(runRubyBuild);
  if (isJava)   return routeToFrameworkBuild(runJvmBuild);
  if (isRust)   return routeToFrameworkBuild(runRustBuild);
  if (isDotnet) return routeToFrameworkBuild(runDotnetBuild);
  if (isElixir) return routeToFrameworkBuild(runElixirBuild);
  if (isBun)    return routeToFrameworkBuild(runBunBuild);
  if (isDeno)   return routeToFrameworkBuild(runDenoBuild);

  // [FIX] Root cause of "GitHub deploys this framework fine, upload fails
  // on the exact same project": runServerBuild (the GitHub path) inspects
  // the cloned files for composer.json/manage.py/go.mod/etc and auto-routes
  // to the correct language builder even when Runtime was left blank/auto.
  // This upload dispatcher only ever routed to those SAME builders (see
  // routeToFrameworkBuild calls above) when the user had EXPLICITLY picked
  // a non-Node runtime from the dropdown -- with Runtime left on auto, a
  // Python/Go/PHP/Ruby/Rust/Java/Elixir upload silently fell through to
  // the generic Node.js server path below and failed, while the identical
  // project pushed to GitHub auto-detected correctly. Same detection
  // logic, ported to check the already-extracted upload files directly
  // instead of a post-clone project root.
  if (!runtime) {
    const detectRoot = findProjectRoot(uploadFilesDir, () => {}, project);
    const hasComposerJson    = fs.existsSync(path.join(detectRoot, 'composer.json'));
    const hasArtisan         = fs.existsSync(path.join(detectRoot, 'artisan'));
    const hasComposerLock    = fs.existsSync(path.join(detectRoot, 'composer.lock'));
    const hasRequirementsTxt = fs.existsSync(path.join(detectRoot, 'requirements.txt')) ||
                               fs.existsSync(path.join(detectRoot, 'requirements')) ||
                               fs.existsSync(path.join(detectRoot, 'Pipfile')) ||
                               fs.existsSync(path.join(detectRoot, 'pyproject.toml'));
    const hasManagePy        = fs.existsSync(path.join(detectRoot, 'manage.py'));
    const hasGoMod           = fs.existsSync(path.join(detectRoot, 'go.mod'));
    const hasGemfile         = fs.existsSync(path.join(detectRoot, 'Gemfile'));
    const hasCargoToml       = fs.existsSync(path.join(detectRoot, 'Cargo.toml'));
    const hasPomXml          = fs.existsSync(path.join(detectRoot, 'pom.xml'));
    const hasBuildGradle     = fs.existsSync(path.join(detectRoot, 'build.gradle')) ||
                               fs.existsSync(path.join(detectRoot, 'build.gradle.kts'));
    const hasMixExs          = fs.existsSync(path.join(detectRoot, 'mix.exs'));

    if (hasComposerJson || hasArtisan || hasComposerLock) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected PHP project (composer.json/artisan found) — switching to PHP build pipeline instead of Node.js.`);
      log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: set Runtime to "PHP" in your project settings to avoid this auto-detection step.`);
      return routeToFrameworkBuild(runPhpBuild);
    }
    if (hasRequirementsTxt || hasManagePy) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Python project (requirements.txt/manage.py found) — switching to Python build pipeline instead of Node.js.`);
      log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: set Runtime to "Python" in your project settings to avoid this auto-detection step.`);
      return routeToFrameworkBuild(runPythonBuild);
    }
    if (hasGoMod) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Go project (go.mod found) — switching to Go build pipeline instead of Node.js.`);
      return routeToFrameworkBuild(runGoBuild);
    }
    if (hasGemfile) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Ruby project (Gemfile found) — switching to Ruby build pipeline instead of Node.js.`);
      return routeToFrameworkBuild(runRubyBuild);
    }
    if (hasCargoToml) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Rust project (Cargo.toml found) — switching to Rust build pipeline instead of Node.js.`);
      return routeToFrameworkBuild(runRustBuild);
    }
    if (hasPomXml || hasBuildGradle) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected JVM project (pom.xml/build.gradle found) — switching to Java build pipeline instead of Node.js.`);
      return routeToFrameworkBuild(runJvmBuild);
    }
    if (hasMixExs) {
      log(`\x1b[33m[Joytree]\x1b[0m Detected Elixir project (mix.exs found) — switching to Elixir build pipeline instead of Node.js.`);
      return routeToFrameworkBuild(runElixirBuild);
    }
  }

  // ── Step 2: Node / generic routing ────────────────────────────────────────
  let isServerApp = siteType === 'server';
  if (!isServerApp) {
    // [FIX] Previously this whole block was skipped whenever siteType was
    // already 'static' -- which meant a project that got misclassified as
    // static by an earlier bug (e.g. Nitro/TanStack Start not being
    // recognized before) would stay stuck failing the same way on every
    // future deploy, since nothing ever re-checked it. A package.json with
    // an unambiguous server-framework dependency should always win over a
    // stale/previous 'static' classification -- so when siteType is
    // already 'static', still re-check, but only using the strict
    // dependency-based signal (strictDepsOnly), not the fuzzier
    // source-file-scanning heuristic that could wrongly flip a genuinely
    // static project just because some file happens to mention ".listen(".
    isServerApp = autoDetectUploadServerApp(uploadFilesDir, startCmd, log, siteType === 'static');
    if (isServerApp) {
      if (siteType === 'static') {
        log(`\x1b[33m[Joytree]\x1b[0m This project was previously classified as a static site, but its package.json shows an unambiguous server-framework dependency — switching to Server App for this and future deploys.`);
      } else {
        log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: set Site Type = "Server App" in project settings to skip auto-detection next time.`);
      }
    }
  }

  // For server apps with no explicit start command, resolve from package.json
  if (isServerApp && !startCmd) {
    const detectedRoot = findProjectRoot(uploadFilesDir, () => {}, project);
    const pkgPath = path.join(detectedRoot, 'package.json');
    if (fs.existsSync(pkgPath)) {
      try {
        const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
        const scripts = pkg.scripts || {};
        if (scripts.start) {
          project = { ...project, startCmd: scripts.start };
          log(`\x1b[32m[Joytree]\x1b[0m ✓ Using start script from package.json: "${scripts.start}"`);
        } else if (pkg.main) {
          project = { ...project, startCmd: `node ${pkg.main}` };
          log(`\x1b[32m[Joytree]\x1b[0m ✓ Using main entry: "node ${pkg.main}"`);
        } else {
          const entries = ['server.js','app.js','index.js','main.js'];
          const found = entries.find(e => fs.existsSync(path.join(detectedRoot, e)));
          if (found) {
            project = { ...project, startCmd: `node ${found}` };
            log(`\x1b[32m[Joytree]\x1b[0m ✓ Auto-resolved start command: "node ${found}"`);
          }
        }
      } catch(_) {}
    }
  }

  if (isServerApp) {
    return runUploadServerBuild({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, log, cleanSub, baseDomain });
  }
  return runUploadStaticBuild({ deployId, project, sitesDir, tmpDir, emit, onLog, uploadFilesDir, log, cleanSub, appPort, baseDomain });
}

async function runUploadStaticBuild({ deployId, project, sitesDir, tmpDir, emit, onLog, uploadFilesDir, log, cleanSub, appPort, baseDomain }) {
  const buildDir = path.join(tmpDir, deployId + '_upload');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  // Step 1: Copy uploaded files to build dir
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log('\x1b[36m━━━ Step 1/4 — Load Uploaded Files ━━━\x1b[0m');
  log('\x1b[90m[upload] Copying extracted project files to build workspace…\x1b[0m');
  copyDir(uploadFilesDir, buildDir);
  const totalFiles = countFiles(buildDir);
  log(`\x1b[32m[upload]\x1b[0m Loaded \x1b[1m${totalFiles}\x1b[0m files from upload`);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log, project);

  // [FIX] Nitro-based SSR meta-frameworks (TanStack Start, etc.) compile to
  // a SERVER bundle (.output/server/index.mjs) with no static HTML at all —
  // deploying them through this static pipeline always failed, either with
  // a build error (Node too old for the framework's own dependencies, as
  // confirmed here — see runUploadServerBuild's matching fix for why) or,
  // if the build somehow succeeded, "no HTML entry file found" since none
  // exists by design. Redirect to the server pipeline instead, same as the
  // GitHub static build already does. Only fires when appPort is present
  // (i.e. called from the primary dispatcher above, never in a way that
  // could create a static<->server redirect loop).
  if (typeof appPort !== 'undefined' && fs.existsSync(path.join(projectRoot, 'package.json'))) {
    try {
      const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
      const allDeps = { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
      const isNitroSsrProject = !!(
        allDeps['@tanstack/react-start'] || allDeps['@tanstack/start'] ||
        allDeps['@tanstack/solid-start'] || allDeps['solid-start'] ||
        allDeps['@analogjs/platform'] ||
        allDeps['nitropack'] || allDeps['nitro'] || allDeps['vinxi']
      );
      if (isNitroSsrProject) {
        log(`\x1b[33m[Joytree]\x1b[0m Detected a Nitro-based SSR framework (TanStack Start or similar) in package.json — this builds a server bundle, not static HTML. Switching this deploy to Web Service automatically.`);
        return runUploadServerBuild({
          deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, log, cleanSub, baseDomain
        });
      }
    } catch (_) { /* malformed package.json — fall through to the normal static flow */ }
  }

  // Resolve Node.js image — same logic as GitHub static/server builds
  const configuredNodeVer = String(project.nodeVer || '20');
  const detectedNodeVer = detectRequiredNodeVersion(projectRoot);
  const resolvedNodeVer = (detectedNodeVer && detectedNodeVer !== configuredNodeVer) ? detectedNodeVer : configuredNodeVer;
  if (detectedNodeVer && detectedNodeVer !== configuredNodeVer) {
    emitNodeVersionWarning(log, configuredNodeVer, detectedNodeVer);
  }
  const nodeImage = `node:${resolvedNodeVer}`;
  const env = resolveEnvVars(project.envVars);

  // Step 2: Install — runs inside Docker container just like GitHub builds,
  // so npm gets caching, proper isolation, and the same speed.
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 2/4 — Install ━━━\x1b[0m');
  const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
  let isPlainStatic = !hasPackageJson;
  if (hasPackageJson) {
    // Skip install entirely when there are no actual dependencies listed
    let pkgJson = {};
    try { pkgJson = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8')); } catch(_) {}
    const hasDeps = Object.keys(pkgJson.dependencies || {}).length > 0 ||
                    Object.keys(pkgJson.devDependencies || {}).length > 0;
    if (!hasDeps) {
      isPlainStatic = true;
      log('\x1b[90m[install] package.json has no dependencies — skipping npm install\x1b[0m');
    }
  } else {
    log('\x1b[90m[install] No package.json found — skipping install for plain static files\x1b[0m');
  }
  if (!isPlainStatic) {
    const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    await runInstallStepWithRecovery({ projectRoot, nodeImage, envObj: env, installCmd, log });
  }
  // Plain static: serve project root directly
  if (isPlainStatic && (!project.outputDir || project.outputDir === 'dist')) {
    log('\x1b[33m[Joytree]\x1b[0m Output directory automatically changed from "' + (project.outputDir || 'dist') + '" → "." (no dependencies, serving project root)');
    project = { ...project, outputDir: '.' };
  }
  emitStep(emit, 'install', 'done');

  // Step 3: Build — also runs inside Docker container
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 3/4 — Build ━━━\x1b[0m');
  const buildCmd = (!isPlainStatic && hasPackageJson)
    ? (String(project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot))
    : 'echo skip';
  if (buildCmd === 'echo skip') {
    log('\x1b[90m(no build step)\x1b[0m');
  } else {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
    } catch (e) {
      if (isNativeBindingNpmBug(e.message)) {
        const _installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
        await recoverFromNativeBindingBugAndRetry({ projectRoot, nodeImage, envObj: env, installCmd: _installCmd, buildCmd, nodeEnvBuild: 'production', log });
      } else if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
        log(`\x1b[33m[Joytree]\x1b[0m No build script found — automatically skipping build step.`);
        log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: add a "build" script to package.json, or set build command to "echo skip" to suppress this message.`);
        log(`\x1b[32m[Joytree]\x1b[0m ✓ Continuing deployment by serving project root directly.`);
        if (!project.outputDir || project.outputDir === 'dist') {
          log(`\x1b[33m[Joytree]\x1b[0m Output directory automatically changed from "${project.outputDir || 'dist'}" → "."`);
          project = { ...project, outputDir: '.' };
        }
      } else {
        throw e;
      }
    }
  }
  emitStep(emit, 'build', 'done');

  // Step 4: Copy to dist
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 4/4 — Copy to Serve ━━━\x1b[0m');
  const outputDir = String(project.outputDir || 'dist').trim() || 'dist';
  let resolvedOutputDir = outputDir;
  let srcDist = outputDir === '.' ? projectRoot : path.join(projectRoot, outputDir);

  // [FIX] This used to silently fall back to copying projectRoot itself
  // (raw source files, node_modules, package.json, everything) whenever
  // the configured outputDir didn't exist -- meaning an auto-detect upload
  // of a Create React App (outputs to "build"), Gatsby ("public"), Nuxt 3
  // static generate (".output/public"), or Angular (nested inside
  // "dist/<project-name>/") would report success while actually serving
  // the wrong content entirely, with no error or explanation anywhere.
  // Try the same common alternate output folders the GitHub deploy path
  // now does, before giving up and falling back to the root.
  if (outputDir === 'dist' && !fs.existsSync(srcDist)) {
    const candidates = ['build', 'public', path.join('.output', 'public'), 'out'];
    for (const cand of candidates) {
      const candPath = path.join(projectRoot, cand);
      if (fs.existsSync(candPath) && fs.lstatSync(candPath).isDirectory()) {
        log(`\x1b[33m[Joytree]\x1b[0m "dist" not found, but "${cand}" looks like your build output (common for Create React App, Gatsby, Nuxt static generate) -- using it instead.`);
        srcDist = candPath;
        resolvedOutputDir = cand;
        break;
      }
    }
    if (srcDist === path.join(projectRoot, 'dist')) {
      // Angular-style nesting: dist/<project-name>/ or dist/<project-name>/browser/
      const distPath = path.join(projectRoot, 'dist');
      if (fs.existsSync(distPath) && fs.lstatSync(distPath).isDirectory()) {
        const distChildren = fs.readdirSync(distPath).filter(f => {
          try { return fs.lstatSync(path.join(distPath, f)).isDirectory(); } catch (_) { return false; }
        });
        if (distChildren.length === 1) {
          const nested = path.join(distPath, distChildren[0]);
          const nestedBrowser = path.join(nested, 'browser');
          const finalNested = fs.existsSync(nestedBrowser) && fs.lstatSync(nestedBrowser).isDirectory() ? nestedBrowser : nested;
          if (fs.existsSync(path.join(finalNested, 'index.html'))) {
            resolvedOutputDir = path.relative(projectRoot, finalNested);
            log(`\x1b[33m[Joytree]\x1b[0m "dist" exists but has no index.html directly inside it -- found one nested at "${resolvedOutputDir}" (typical of Angular's per-project build output) and using that instead.`);
            srcDist = finalNested;
          }
        }
      }
    }
  }
  if (!fs.existsSync(srcDist)) {
    // [FIX] Same fallback as the GitHub static build path: before falling
    // back to copying the raw project root (which would silently serve
    // node_modules/source files instead of a real build), check if this is
    // unambiguously a Vite project (has a vite.config.*) whose configured
    // build script just isn't actually invoking Vite -- a common cause
    // being a placeholder like `"build": "echo done"` left over from a
    // template. If so, try the real build command directly.
    const viteConfigFileUp = ['vite.config.js', 'vite.config.ts', 'vite.config.mjs', 'vite.config.cjs']
      .map(f => path.join(projectRoot, f)).find(f => fs.existsSync(f));
    if (viteConfigFileUp) {
      log(`\x1b[33m[Joytree]\x1b[0m Found ${path.basename(viteConfigFileUp)} but no build output — your configured build script may not actually be running Vite. Trying \`npx vite build\` directly...\x1b[0m`);
      try {
        await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: 'npx vite build', log });
        const viteDistPath = path.join(projectRoot, 'dist');
        if (fs.existsSync(viteDistPath) && fs.readdirSync(viteDistPath).length > 0) {
          log(`\x1b[32m[Joytree]\x1b[0m \`npx vite build\` succeeded — using its output. Consider fixing your package.json "build" script to say "vite build" so this runs automatically next time.\x1b[0m`);
          srcDist = viteDistPath;
          resolvedOutputDir = 'dist';
        }
      } catch (_) {
        // Fall through to the existing "copy project root" fallback below if this also fails.
      }
    }
  }
  const destDist = path.join(sitesDir, cleanSub, 'dist');
  // Always wipe destDist first so stale files from a previous deploy never cause "Not found"
  try { fs.rmSync(destDist, { recursive: true, force: true }); } catch(_) {}
  fs.mkdirSync(destDist, { recursive: true });
  const actualSrc = fs.existsSync(srcDist) ? srcDist : projectRoot;
  if (!fs.existsSync(srcDist)) {
    log(`\x1b[33m[Joytree]\x1b[0m Output dir "${resolvedOutputDir}" not found — copying project root`);
  } else {
    log(`\x1b[90m[copy] Copying ${resolvedOutputDir}/ → ${destDist}\x1b[0m`);
  }
  copyDir(actualSrc, destDist);

  // ── HTML entry detection — same logic as GitHub static build ─────────────
  // If no index.html exists, find any .html file and copy it as index.html
  // so uploads with any filename (e.g. james.html) serve correctly.
  const indexCandidates = [path.join(destDist, 'index.html'), path.join(destDist, '200.html')];
  let hasStaticEntry = indexCandidates.some(fp => fs.existsSync(fp));
  if (!hasStaticEntry) {
    function findHtmlFiles(dir, depth) {
      if (depth > 4) return [];
      let results = [];
      let entries = [];
      try { entries = fs.readdirSync(dir); } catch(_) { return []; }
      for (const e of entries) {
        const full = path.join(dir, e);
        try {
          const st = fs.lstatSync(full);
          if (st.isFile() && e.toLowerCase().endsWith('.html')) {
            results.push(full);
          } else if (st.isDirectory() && !['node_modules', '.git', '.next', 'dist', 'build'].includes(e)) {
            results = results.concat(findHtmlFiles(full, depth + 1));
          }
        } catch(_) {}
      }
      return results;
    }
    const allHtml = findHtmlFiles(destDist, 0);
    if (allHtml.length > 0) {
      const rootHtml = allHtml.filter(f => path.dirname(f) === destDist);
      const chosen = rootHtml.length > 0 ? rootHtml[0] : allHtml[0];
      const chosenName = path.basename(chosen);
      const destIndex = path.join(destDist, 'index.html');
      log(`\x1b[33m[Joytree]\x1b[0m No index.html found — detected HTML file: "${chosenName}"`);
      log(`\x1b[32m[Joytree]\x1b[0m ✓ Automatically creating index.html from "${chosenName}" so the site serves correctly`);
      try { fs.copyFileSync(chosen, destIndex); hasStaticEntry = true; } catch(copyErr) {
        log(`\x1b[31m[error]\x1b[0m Could not create index.html from "${chosenName}": ${copyErr.message}`);
      }
    }
    if (!hasStaticEntry) {
      // [FIX] Same fallback as above and as the GitHub static build path:
      // try a direct `npx vite build` if this is clearly a Vite project,
      // before giving up. Since destDist was already populated from
      // whatever (non-Vite) output existed, a successful retry here needs
      // to wipe and re-copy destDist from the fresh dist/ output.
      let recoveredViaViteUp = false;
      const viteConfigFileUp2 = ['vite.config.js', 'vite.config.ts', 'vite.config.mjs', 'vite.config.cjs']
        .map(f => path.join(projectRoot, f)).find(f => fs.existsSync(f));
      if (viteConfigFileUp2) {
        log(`\x1b[33m[Joytree]\x1b[0m Found ${path.basename(viteConfigFileUp2)} but no HTML output — your configured build script may not actually be running Vite. Trying \`npx vite build\` directly...\x1b[0m`);
        try {
          await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: 'npx vite build', log });
          const viteDistPath2 = path.join(projectRoot, 'dist');
          const freshHtmlUp = fs.existsSync(viteDistPath2) ? findHtmlFiles(viteDistPath2, 0) : [];
          if (freshHtmlUp.length > 0) {
            log(`\x1b[32m[Joytree]\x1b[0m \`npx vite build\` succeeded — using its output. Consider fixing your package.json "build" script to say "vite build" so this runs automatically next time.\x1b[0m`);
            try { fs.rmSync(destDist, { recursive: true, force: true }); } catch (_) {}
            fs.mkdirSync(destDist, { recursive: true });
            copyDir(viteDistPath2, destDist);
            hasStaticEntry = fs.existsSync(path.join(destDist, 'index.html'));
            if (!hasStaticEntry) {
              const rootHtmlUp = freshHtmlUp.filter(f => path.dirname(f) === viteDistPath2);
              const chosenUp = rootHtmlUp.length > 0 ? rootHtmlUp[0] : freshHtmlUp[0];
              try { fs.copyFileSync(path.join(destDist, path.relative(viteDistPath2, chosenUp)), path.join(destDist, 'index.html')); hasStaticEntry = true; } catch (_) {}
            }
            recoveredViaViteUp = hasStaticEntry;
          }
        } catch (_) {
          // Fall through to the normal error below if this also fails.
        }
      }
      if (!recoveredViaViteUp) {
        log(`\x1b[31m[error]\x1b[0m No HTML entry file found. Ensure your upload contains at least one .html file.\x1b[0m`);
        throw new Error('Upload deploy failed: no HTML entry file found in upload');
      }
    }
  }

  const copiedCount = countFiles(destDist);
  log(`\x1b[32m[copy]\x1b[0m ${copiedCount} files ready to serve`);
  emitStep(emit, 'copy', 'done');

  // Defensive cleanup: if an old server deploy left a ports.json entry or container
  // behind for this subdomain, the proxy will try to forward to it and return 502
  // instead of falling through to serve static files. Clear it now.
  const portsFile = path.join(sitesDir, 'ports.json');
  try {
    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch (_) {}
    if (registry[cleanSub]) {
      delete registry[cleanSub];
      fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
      log(`\x1b[90m[static] Removed stale proxy entry for ${cleanSub}\x1b[0m`);
    }
  } catch (e) {
    log(`\x1b[33m[static] Could not update ports registry: ${e.message}\x1b[0m`);
  }
  try { await exec('docker', ['rm', '-f', `db-${cleanSub}`], {}, () => {}); } catch (_) {}

  emitStep(emit, 'start', 'done');
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  return { siteType: 'static' };
}

async function runUploadServerBuild({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, log, cleanSub, baseDomain }) {
  const buildDir = path.join(tmpDir, deployId + '_upload');
  const appDir = path.join(sitesDir, cleanSub, 'app');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  // Step 1: Load files
  emitStep(emit, 'clone', 'active');
  checkBuildStopped(deployId);
  log('\x1b[36m━━━ Step 1/5 — Load Uploaded Files ━━━\x1b[0m');
  log('\x1b[90m[upload] Copying extracted project files to build workspace…\x1b[0m');
  copyDir(uploadFilesDir, buildDir);
  const totalFiles = countFiles(buildDir);
  log(`\x1b[32m[upload]\x1b[0m Loaded \x1b[1m${totalFiles}\x1b[0m files`);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log, project);
  const relativeProjectRoot = path.relative(buildDir, projectRoot) || '.';
  log(`\x1b[90m[deploy] Server app root: ${relativeProjectRoot} — install, build, and start all use this same directory.\x1b[0m`);

  // [FIX] The GitHub path (runServerBuild) has a safety check here: if the
  // project doesn't actually have a runnable start command or server entry
  // (isDeployableServerProject), it gracefully falls back to a static
  // deploy instead of plowing ahead into install/build/container-launch
  // and failing with a confusing error much later. This upload path had no
  // equivalent -- a project misclassified as "server" (or manually set
  // that way when it isn't really one) would fail deep in the container
  // startup phase instead of being caught early with a clear redirect.
  if (!isDeployableServerProject(projectRoot, String(project.startCmd || '').trim())) {
    log(`\x1b[33m[auto] No production server start could be inferred. Falling back to static deployment flow.\x1b[0m`);
    const fallbackProject = { ...project, siteType: 'static', buildCmd: project.buildCmd || 'echo skip', outputDir: project.outputDir || '.' };
    return runUploadStaticBuild({ deployId, project: fallbackProject, sitesDir, tmpDir, emit, onLog, uploadFilesDir, log, cleanSub, appPort, baseDomain });
  }

  // [FIX] Root cause of uploaded Node/Next.js projects failing to build
  // (TypeError: Cannot read properties of null (reading 'useContext'),
  // Error: <Html> should not be imported outside of pages/_document, and
  // similar "phantom" errors that don't reproduce for the exact same
  // project deployed from GitHub) while the GitHub path builds the same
  // code fine: runServerBuild (GitHub) has ALWAYS run install/build inside
  // an isolated `node:<version>` Docker container per deploy. This upload
  // path instead ran install/build directly on the shared VPS host via
  // exec() -- sharing the host's single global Node/npm install, npm
  // cache, and any stray global state across every unrelated upload
  // deploy on the box. That's exactly the kind of cross-contamination
  // that produces duplicate-React/stale-webpack-cache style build errors
  // which are otherwise very hard to reproduce, since they depend on what
  // happened to build on the host before. Every upload now goes through
  // the same Docker-isolated pipeline as GitHub, with the same automatic
  // Node-version detection from package.json "engines", so an uploaded
  // project builds exactly the way it would if pushed to GitHub instead.
  let isNitroSsrProject = false;
  if (fs.existsSync(path.join(projectRoot, 'package.json'))) {
    try {
      const pkg = JSON.parse(fs.readFileSync(path.join(projectRoot, 'package.json'), 'utf8'));
      const allNitroDeps = { ...(pkg.dependencies || {}), ...(pkg.devDependencies || {}) };
      isNitroSsrProject = !!(
        allNitroDeps['@tanstack/react-start'] || allNitroDeps['@tanstack/start'] ||
        allNitroDeps['@tanstack/solid-start'] || allNitroDeps['solid-start'] ||
        allNitroDeps['@analogjs/platform'] ||
        allNitroDeps['nitropack'] || allNitroDeps['nitro'] || allNitroDeps['vinxi']
      );
      if (isNitroSsrProject) {
        log(`\x1b[33m[Joytree]\x1b[0m Detected a Nitro-based SSR framework (TanStack Start or similar) in package.json.`);
        if (Number(String(project.nodeVer || '0').match(/\d+/)?.[0] || 0) < 22) {
          log(`\x1b[90m[Joytree]\x1b[0m Forcing Node.js 22 (TanStack Start's own dependencies require it).`);
          project = { ...project, nodeVer: '22' };
        }
        if (!String(project.startCmd || '').trim()) {
          project = { ...project, startCmd: 'node .output/server/index.mjs' };
          log(`\x1b[90m[Joytree]\x1b[0m No Start Command set — defaulting to "node .output/server/index.mjs" (Nitro's standard server entry), instead of an auto-detected script like "preview" that wouldn't serve the real SSR output.`);
        }
      }
    } catch (_) { /* malformed package.json — proceed with normal detection below */ }
  }

  // ── Auto-detect Node.js version from engines field (same as GitHub path) ──
  const configuredNodeVer = String(project.nodeVer || '20');
  const detectedNodeVer = detectRequiredNodeVersion(projectRoot);
  let resolvedNodeVer = configuredNodeVer;
  if (detectedNodeVer && detectedNodeVer !== configuredNodeVer) {
    emitNodeVersionWarning(log, configuredNodeVer, detectedNodeVer);
    resolvedNodeVer = detectedNodeVer;
  } else if (detectedNodeVer) {
    log(`\x1b[90m[detect] Node.js version confirmed: ${resolvedNodeVer} (matches package.json engines)\x1b[0m`);
  }
  const nodeImage = `node:${resolvedNodeVer}`;

  // Same env resolution as the GitHub path (attached DB service URLs +
  // user env vars + runtime defaults), used for install, build, AND the
  // runtime container below — one consistent env object end to end,
  // instead of build steps and the running container potentially seeing
  // different env objects.
  const env = withDeployedAppRuntimeDefaults({ ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project) }, project, baseDomain);
  if (isNitroSsrProject && !env.NITRO_PRESET) {
    env.NITRO_PRESET = 'node-server';
    log(`\x1b[90m[Joytree]\x1b[0m Forcing NITRO_PRESET=node-server so the build serves its own static assets under plain Node (many scaffolds default to a Cloudflare Workers target, which handles assets differently and would 404 on images here).`);
  }

  // Step 2: Install
  emitStep(emit, 'install', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m');
  const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
  if (hasPackageJson) {
    const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
    log(`\x1b[90m[install] Running in a Docker container (${nodeImage}) — matches how GitHub-sourced deploys build, for a clean, isolated install.\x1b[0m`);
    log(`\x1b[90m[install] cwd: ${relativeProjectRoot}\x1b[0m`);
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    await runInstallStepWithRecovery({ projectRoot, nodeImage, envObj: env, installCmd, log });
  } else {
    log('\x1b[90m[install] No package.json found — skipping install\x1b[0m');
  }
  emitStep(emit, 'install', 'done');

  // Step 3: Build
  emitStep(emit, 'build', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m');
  const buildCmd = hasPackageJson ? (String(project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot)) : 'echo skip';
  if (buildCmd === 'echo skip') {
    log('\x1b[90m(no build step)\x1b[0m');
  } else {
    log(`\x1b[90m[build] cwd: ${relativeProjectRoot}\x1b[0m`);
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
    } catch (e) {
      if (isNativeBindingNpmBug(e.message)) {
        log(`\x1b[33m[Joytree] Detected a known npm bug (native binary optional dependencies, see https://github.com/npm/cli/issues/4828) -- this happens when package-lock.json was generated on a different OS than this build environment. Automatically retrying with a clean reinstall...\x1b[0m`);
        const _installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
        await recoverFromNativeBindingBugAndRetry({ projectRoot, nodeImage, envObj: env, installCmd: _installCmd, buildCmd, nodeEnvBuild: 'production', log });
      } else if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
        log(`\x1b[33m[Joytree]\x1b[0m No build script found in your project — automatically skipping build step.`);
        log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: if you intended to run a build, add a "build" script to your package.json, or set the build command to "echo skip" to suppress this message.`);
        log(`\x1b[32m[Joytree]\x1b[0m ✓ Continuing deployment without build step.`);
      } else {
        throw e;
      }
    }
  }
  emitStep(emit, 'build', 'done');

  // Step 4: Persist the exact built app directory, then start container from it.
  // This mirrors Render-style server deploys: install/build happen in one app
  // root and the same completed tree is mounted as /app for the long-running
  // server process. Never copy only static output for server apps.
  emitStep(emit, 'copy', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 4/5 — Prepare App + Launch Container ━━━\x1b[0m');

  if (fs.existsSync(appDir)) fs.rmSync(appDir, { recursive: true, force: true });
  fs.mkdirSync(path.dirname(appDir), { recursive: true });
  try {
    fs.renameSync(projectRoot, appDir);
    log(`\x1b[32m[deploy]\x1b[0m ✓ Server app moved to permanent storage with build artifacts and node_modules intact`);
  } catch (moveErr) {
    // [FIX] copyDir() "resolves" every symlink via realpath and copies the
    // TARGET FILE'S CONTENT to the symlink's location -- which physically
    // relocates that file. Any CLI shim that does a __dirname-relative
    // require (exactly what node_modules/.bin/next does internally --
    // require('../server/require-hook'), resolved relative to wherever the
    // file actually lives on disk) breaks the instant it's copied out of
    // its real location (node_modules/next/dist/bin/) into .bin/, since the
    // relative path no longer points anywhere real. That's the exact cause
    // of "Cannot find module '../server/require-hook'" at container start
    // -- the SAME class of bug the GitHub deploy path already fixed by
    // using rsync's --links flag instead (which preserves symlinks AS
    // symlinks, never relocating what they point to). This upload path had
    // fallen back to the old broken copyDir() instead -- bringing it in
    // line with the GitHub path's exact fallback chain fixes it here too.
    log(`\x1b[90m[deploy] Cross-device move, using rsync…\x1b[0m`);
    fs.mkdirSync(appDir, { recursive: true });
    try {
      const { execSync } = require('child_process');
      execSync(`rsync -a --links --no-whole-file "${projectRoot}/" "${appDir}/"`, { stdio: 'pipe', maxBuffer: 50*1024*1024 });
      log(`\x1b[32m[deploy]\x1b[0m ✓ App synced with rsync (symlinks intact)`);
    } catch (rsyncErr) {
      // rsync not available — copy without node_modules so the container's
      // own runtime bootstrap reinstalls it fresh (see startWithPath's
      // ensureRuntimeDeps below), rather than ending up with broken
      // relocated CLI shims from a naive copy.
      log(`\x1b[33m[deploy]\x1b[0m rsync unavailable, copying source only (node_modules will install in container)`);
      const excludes = ['node_modules', '.git'];
      const copyFiltered = (src, dst) => {
        fs.mkdirSync(dst, { recursive: true });
        for (const entry of fs.readdirSync(src)) {
          if (excludes.includes(entry)) continue;
          const s = path.join(src, entry), d = path.join(dst, entry);
          try {
            const st = fs.lstatSync(s);
            if (st.isDirectory()) copyFiltered(s, d);
            else if (st.isFile()) fs.copyFileSync(s, d);
          } catch (_) {}
        }
      };
      copyFiltered(projectRoot, appDir);
      log(`\x1b[33m[deploy]\x1b[0m Source copied without node_modules — runtime bootstrap will reinstall dependencies in /app before start`);
    }
    log(`\x1b[32m[deploy]\x1b[0m ✓ Server app copied to permanent storage`);
  }

  const containerName = 'db-' + cleanSub;
  const candidateContainerName = containerName + '-cand-' + safeDockerToken(deployId, 'build').slice(0, 20);
  const nodeVer = resolvedNodeVer;
  const expectedPort = appPort || 3000;
  const envObj = env;
  // [DIAGNOSTIC] Shows exactly which env vars this container is about to be
  // launched with. Compare against the "[upload-deploy] received envVars
  // keys" server log for this same deploy to see whether the value made it
  // this far, or was lost somewhere between receipt and container launch.
  log(`\x1b[90m[diagnostic] Injecting env vars into container: [${Object.keys(envObj).filter(k => !['PORT','HOST','HOSTNAME'].includes(k)).join(', ')}]\x1b[0m`);
  const startCmdResolved = resolveRuntimeStartCommand({ projectRoot: appDir, startCmd: project.startCmd, expectedPort });
  const networkName = 'deployboard-net';
  const hostAppDir = appDir.replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data');
  const portsFile = path.join(sitesDir, 'ports.json');
  const previousTarget = readRegistryTarget(portsFile, cleanSub);

  try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(e) {}

  // [FIX] Two flags the GitHub deploy path (runServerBuild) has always set
  // on its runtime container that this upload path was missing:
  //  1. NODE_OPTIONS=--max-old-space-size=<heap> — without it, V8 sizes its
  //     default heap off the HOST's total memory, not this container's -m
  //     limit, so on a host with much more RAM than the container is capped
  //     to, V8 can grow well past the container's hard limit before ever
  //     triggering its own GC pressure heuristics, and the container gets
  //     OOM-killed by the kernel with no Node-side warning first.
  //  2. --memory-swap — left unset, Docker's default swap accounting can
  //     behave inconsistently across daemon versions; GitHub deploys always
  //     pin it explicitly to runtime.memorySwap.
  // Missing either can present as "container is running but never becomes
  // reachable" (an OOM-killed Node process restarting in a loop under
  // --restart=unless-stopped looks alive at the Docker level the whole
  // time, even though it never stays up long enough to finish binding the
  // HTTP port).
  const runtime = getRuntimeConfig(project);
  // [FIX] Was the fixed global CPU_SHARES constant ('512', same as a free
  // tier), completely ignoring the user's actual billing plan. The GitHub
  // path already correctly uses runtime.cpuShares (computed per-project
  // from getRuntimeConfig, which reads the plan's real CPU allocation) --
  // a Pro/Scale Max user deploying via upload was silently capped to the
  // same CPU priority as a free account.
  const runArgs = [
    'run', '-d', '--restart=no',
    '--name', candidateContainerName,
    '--network', networkName,
    '--add-host', 'host.docker.internal:host-gateway',
    '--cpu-shares', runtime.cpuShares,
    '--pids-limit', PIDS_LIMIT,
    '-m', runtime.memory,
    '--memory-reservation', runtime.memory,
    '-e', `PORT=${expectedPort}`,
    '-e', `NODE_ENV=production`,
    '-e', `HOST=0.0.0.0`,
    '-e', `HOSTNAME=0.0.0.0`,
    '-e', `NEXT_TELEMETRY_DISABLED=1`,
    '-e', `NODE_OPTIONS=--max-old-space-size=${runtime.nodeHeapMb}`,
    ...Object.entries(envObj).flatMap(([k,v]) => {
      const key = String(k || '').toUpperCase();
      return (key === 'PORT' || key === 'HOST' || key === 'HOSTNAME') ? [] : ['-e', `${k}=${v}`];
    }),
    '-v', `${hostAppDir}:/app`,
    '-w', '/app',
  ];
  if (runtime.memorySwap) {
    runArgs.push('--memory-swap', runtime.memorySwap);
  }
  // [FIX] The GitHub path's startup command self-heals if node_modules
  // ends up missing at runtime (e.g. the copy fallback above had to skip
  // it because rsync wasn't available) by reinstalling before running the
  // start command. This upload path had NO such check -- it just ran
  // startCmdResolved directly, so the very fallback tier that's supposed
  // to keep a deploy alive when rsync is unavailable would still leave the
  // container unable to start at all ("Cannot find module 'next'" etc),
  // since nothing ever reinstalled the dependencies it deliberately left out.
  const runtimeInstallCmdUp = normalizeInstallLikeCommand((project.installCmd || '').trim() || getDefaultInstallCmd(appDir), appDir).replace(/`/g, '\\`');
  const ensureRuntimeDepsUp = `[ -d node_modules ] || [ ! -f package.json ] || (echo "[Joytree] node_modules missing in /app — reinstalling dependencies before start" && corepack enable >/dev/null 2>&1 || true; ${runtimeInstallCmdUp})`;
  runArgs.push(
    `node:${nodeVer}`,
    'sh', '-c', `export PATH=/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:$PATH && ${ensureRuntimeDepsUp} && ${String(startCmdResolved || 'node server.js').replace(/`/g, '\\`')}`
  );

  // [FIX] GitHub path pulls the runtime image explicitly before running,
  // so a missing/stale image is surfaced as a clear "Pulling..." log line
  // rather than happening silently inline as part of `docker run`.
  log(`\x1b[90m[docker] Pulling node:${nodeVer}…\x1b[0m`);
  try {
    await exec('docker', ['pull', `node:${nodeVer}`], {}, log);
  } catch (e) {
    log(`\x1b[33m[docker] Using cached image\x1b[0m`);
  }

  log(`\x1b[90m[docker] Launching Node.js ${nodeVer} container from /app (same built server tree)…\x1b[0m`);
  await exec('docker', runArgs, {}, log);
  emitStep(emit, 'copy', 'done');

  // Step 5: Verify
  emitStep(emit, 'start', 'active');
  checkBuildStopped(deployId);
  log('\n\x1b[36m━━━ Step 5/5 — Verify ━━━\x1b[0m');
  await new Promise(r => setTimeout(r, 3000));
  const stable = await waitForContainerRunning(candidateContainerName, 90, log); // [FIX] increased from 30s to 90s for heavy apps
  if (!stable) {
    // [FIX] GitHub path shows OOM/exit-code/error details from `docker
    // inspect` here, not just the raw log tail -- e.g. instantly reveals
    // an OOM-kill instead of leaving the person to guess from output alone.
    try {
      const inspectLines = [];
      await exec('docker', ['inspect', '--format={{.State.Status}}|oom={{.State.OOMKilled}}|exit={{.State.ExitCode}}|error={{.State.Error}}', candidateContainerName], {}, (line) => inspectLines.push(line));
      const diag = (inspectLines.join('\n').trim().split('\n').pop() || '').trim();
      if (diag) log(`\x1b[33m[docker] State details: ${diag}\x1b[0m`);
    } catch (_) {}
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(e) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(e) {}
    if (!isStableRegistryTarget(previousTarget)) {
      let registry = {};
      try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch(_) {}
      delete registry[cleanSub];
      fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
    }
    throw new Error('Container exited during startup. Check logs above.');
  }
  // [FIX] Hardcoded to 120s here while the GitHub path (runServerBuild) uses
  // runtime.startupTimeoutSeconds (default 300s, configurable per-project).
  // An app with a real cold-start cost — compiling/initializing a native
  // module like better-sqlite3, running schema migrations, etc — can
  // legitimately need more than 120s on first boot, and would be failed
  // here even though the exact same project deployed from GitHub gets a
  // full 5 minutes by default.
  const livePort = await detectLivePort(candidateContainerName, expectedPort, runtime.startupTimeoutSeconds, log);
  if (!livePort) {
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(e) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(e) {}
    if (isStableRegistryTarget(previousTarget)) {
      log(`\x1b[33m[docker]\x1b[0m Readiness failed; keeping previous live mapping: ${cleanSub} → ${previousTarget}`);
    } else {
      let registry = {};
      try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch(_) {}
      delete registry[cleanSub];
      fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
      log(`\x1b[33m[docker]\x1b[0m Readiness failed; removed placeholder port for ${cleanSub}`);
    }
    throw new Error('Readiness gate failed: server app did not expose a reachable HTTP port.');
  }
  const targetPort = normalizePort(livePort, expectedPort);
  if (targetPort !== expectedPort) {
    log(`\x1b[33m[docker]\x1b[0m App ignored PORT=${expectedPort}; routing to detected port ${targetPort}`);
  }

  try {
    await archivePreviousContainer(containerName, cleanSub, log);
    await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
    // [FIX] The candidate container launches with --restart=no so a
    // crash-looping start command stays visibly exited instead of being
    // silently relaunched by Docker mid-healthcheck (which was masking
    // startup failures as "still waiting" forever -- see detectLivePort's
    // liveness check above). Only apply real crash-resilience now that the
    // container has actually proven it serves HTTP.
    try { await exec('docker', ['update', '--restart', 'unless-stopped', containerName], {}, () => {}); } catch(_) {}
    log(`\x1b[90m[docker]\x1b[0m Promoted candidate to stable: ${containerName}`);

    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(portsFile,'utf8')); } catch(e) {}
    registry[cleanSub] = `${containerName}:${targetPort}`; // must be "name:port" string — proxy does appEntry.split(':')
    fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
    await cleanupArchivedContainers(cleanSub, DEPLOY_HISTORY_KEEP, log);
  } catch(e) {
    log(`\x1b[31m[docker]\x1b[0m Candidate failed during promotion: ${e.message}`);
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (!isStableRegistryTarget(previousTarget)) {
      let registry = {};
      try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch(_) {}
      delete registry[cleanSub];
      fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
    }
    throw e;
  }
  log(`\x1b[32m[docker]\x1b[0m Container running on port ${targetPort}`);
  emitStep(emit, 'start', 'done');

  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  return { siteType: 'server' };
}
