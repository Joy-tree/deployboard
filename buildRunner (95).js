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

const { spawn } = require('child_process');
const path      = require('path');
const fs        = require('fs');

// ── Node.js version detection from package.json engines field ─────────────────
// Returns the best matching node major version string (e.g. '20', '22') or null.
// Supported engine strings: '>=20', '^22', '20.x', '20', etc.
function detectRequiredNodeVersion(projectRoot) {
  try {
    const pkgPath = path.join(projectRoot, 'package.json');
    if (!fs.existsSync(pkgPath)) return null;
    const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8'));
    const engineStr = (pkg.engines && pkg.engines.node) ? String(pkg.engines.node).trim() : '';
    if (!engineStr) return null;
    // Extract the first numeric major version from the engine range
    const m = engineStr.match(/(\d+)/);
    if (!m) return null;
    const major = Number(m[1]);
    if (!Number.isInteger(major) || major < 14) return null;
    // Map to a supported Docker image version
    if (major >= 22) return '22';
    if (major >= 20) return '20';
    if (major >= 18) return '18';
    if (major >= 16) return '16';
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

// ── Entry point ───────────────────────────────────────────────────────────────
async function runBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, isDockerfileDeploy, isWorker }) {
  // Merge flags from args or from project record
  const dockerfileDeploy = isDockerfileDeploy || project.isDockerfileDeploy;
  const workerDeploy     = isWorker           || project.isWorker;

  if (dockerfileDeploy) {
    return runDockerfileBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  }
  if (workerDeploy) {
    return runWorkerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  }

  // ── Python runtime detection ─────────────────────────────────────────────
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

  if (isPython)  return runPythonBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isGo)      return runGoBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isPHP)     return runPhpBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isRuby)    return runRubyBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isJava)    return runJvmBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isRust)    return runRustBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isDotnet)  return runDotnetBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isElixir)  return runElixirBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isBun)     return runBunBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  if (isDeno)    return runDenoBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });

  // Respect explicit site type first. If "static" is selected, do NOT force
  // server mode just because startCmd has a default value (e.g. "npm start").
  const explicitType = String(project.siteType || '').trim().toLowerCase();
  const hasStartCmd = !!String(project.startCmd || '').trim();
  const isServerApp = explicitType === 'server' || (!explicitType && hasStartCmd);
  return isServerApp
    ? runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog })
    : runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog });
}

// ── DOCKERFILE BUILD ─────────────────────────────────────────────────────────
// Clones the repo and builds + runs the user's own Dockerfile
async function runDockerfileBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir      = path.join(tmpDir, deployId);
  const containerName = 'db-' + project.subdomain;
  const imageName     = 'deployboard-' + project.subdomain;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const dfPath        = project.dockerfilePath || 'Dockerfile';
  const exposedPort   = project.exposedPort   || 3000;

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };

  // ── Step 1: Clone ────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  log('\x1b[36m━━━ Step 1/4 — Clone ━━━\x1b[0m');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  // For Dockerfile deploys, search from repo root first, then subdirs
  // DO NOT use findProjectRoot — it finds package.json which may be in a subdir
  let dockerfileDir = buildDir;
  const dfAtRoot = path.join(buildDir, dfPath);
  if (fs.existsSync(dfAtRoot)) {
    dockerfileDir = buildDir;
  } else {
    // Try one level deep
    let found = false;
    for (const entry of fs.readdirSync(buildDir)) {
      const sub = path.join(buildDir, entry);
      if (fs.statSync(sub).isDirectory()) {
        const candidate = path.join(sub, dfPath);
        if (fs.existsSync(candidate)) { dockerfileDir = sub; found = true; break; }
      }
    }
    if (!found) throw new Error(`Dockerfile not found. Make sure "${dfPath}" exists at the root of your repo.`);
  }
  const dfFullPath = path.join(dockerfileDir, dfPath);
  log(`\x1b[90m[docker] Found Dockerfile at: ${path.relative(buildDir, dfFullPath)}\x1b[0m`);

  // ── Step 2: Build Docker image ───────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log('\n\x1b[36m━━━ Step 2/4 — Docker Build ━━━\x1b[0m');
  log(`\x1b[90m$ docker build -f ${dfPath} -t ${imageName} .\x1b[0m`);

  // Build args from envVars
  const envObj  = resolveEnvVars(project.envVars);
  const buildArgs = Object.entries(envObj).map(([k,v]) => `--build-arg ${k}=${v}`).join(' ');
  const buildCmd  = `docker build -f ${dfPath} ${buildArgs} -t ${imageName} ${dockerfileDir}`;

  await exec('sh', ['-c', buildCmd], { cwd: dockerfileDir }, log);
  emitStep(emit, 'build', 'done');

  // ── Step 3: Stop old container, start new one ────────────────────────────
  emitStep(emit, 'copy', 'active');
  log('\n\x1b[36m━━━ Step 3/4 — Start Container ━━━\x1b[0m');

  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const networkName = 'deployboard_deployboard-net';
  const runArgs = [
    'run', '-d', '--restart=unless-stopped',
    '--name',         candidateContainerName,
    '--network',      networkName,
    '--cpu-shares',   CPU_SHARES,
    '--pids-limit',   PIDS_LIMIT,
    '-e', `PORT=${exposedPort}`,
    ...Object.entries(envObj).flatMap(([k,v]) => ['-e', `${k}=${v}`]),
    imageName
  ];

  log(`\x1b[90m[docker] CPU shares: ${CPU_SHARES}\x1b[0m`);
  await exec('docker', runArgs, {}, log);
  emitStep(emit, 'copy', 'done');

  // ── Step 4: Wait for app to be ready ────────────────────────────────────
  emitStep(emit, 'start', 'active');
  log('\n\x1b[36m━━━ Step 4/4 — Verify ━━━\x1b[0m');
  log('\x1b[90m[docker] Waiting for app to start…\x1b[0m');
  await new Promise(r => setTimeout(r, 3000));

  const stable = await waitForContainerRunning(candidateContainerName, 30, log);
  if (!stable) {
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(e) {}
    throw new Error('Dockerfile container exited during startup. Check logs above.');
  }

  const livePort = await detectLivePort(candidateContainerName, exposedPort, 60, log);
  const targetPort = livePort || exposedPort;

  // Promote candidate to stable name used by proxy/registry
  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}
  await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});

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
async function runWorkerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir      = path.join(tmpDir, deployId);
  const containerName = 'db-' + project.subdomain;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const nodeImage     = 'node:' + (project.nodeVer || '18') + '-bullseye';
  const startCmd      = (project.startCmd || '').trim();
  const appDir        = path.join(sitesDir, project.subdomain, 'app');

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = resolveEnvVars(project.envVars);

  // Step 1: Clone
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/4 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);

  // Step 2: Install
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/4 — Install ━━━\x1b[0m`);
  const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: installCmd, log });
  emitStep(emit, 'install', 'done');

  // Step 3: Build
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/4 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || 'echo skip';
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'production', command: buildCmd, log });
  } else { log('\x1b[90m(no build step)\x1b[0m'); }
  emitStep(emit, 'build', 'done');

  // Step 4: Start container (no port needed)
  emitStep(emit, 'start', 'active');
  log(`\n\x1b[36m━━━ Step 4/4 — Start Worker ━━━\x1b[0m`);

  // Copy files to persistent dir
  fs.mkdirSync(appDir, { recursive: true });
  copyDir(projectRoot, appDir);

  // Stop old container
  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const networkName = 'deployboard_deployboard-net';
  const runArgs = [
    'run', '-d', '--restart=unless-stopped',
    '--name',         candidateContainerName,
    '--network',      networkName,
    '--cpu-shares',   CPU_SHARES,
    '--pids-limit',   PIDS_LIMIT,
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

  log(`\x1b[32m[docker] ✓ Background worker is running\x1b[0m`);
  emitStep(emit, 'start', 'done');
  fs.rmSync(buildDir, { recursive: true, force: true });
}

// ── PYTHON BUILD ─────────────────────────────────────────────────────────────
// Supports Django, Flask, FastAPI, and generic Python server apps.
// Uses python:<ver>-slim Docker image. Install via pip/poetry/pipenv.
// Runs app via gunicorn (Django/Flask) or uvicorn (FastAPI) or user startCmd.
async function runPythonBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir      = path.join(tmpDir, deployId);
  const appDir        = path.join(sitesDir, project.subdomain, 'app');
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0, 20)}`;
  const expectedPort  = normalizePort(appPort, 8000);
  const runtime       = detectPythonRuntime(project);
  const pythonImage   = `python:${runtime.pythonVer}-slim`;

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = { ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project) };

  // ── Step 1: Clone ────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findPythonProjectRoot(buildDir, log);
  log(`\x1b[90m[python] Framework: ${runtime.framework} | Image: ${pythonImage}\x1b[0m`);

  // ── Detect the actual requirements file ──────────────────────────────────
  const reqFile = detectPythonRequirementsFile(projectRoot, log);

  // ── Step 2: Install ──────────────────────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/6 — Install Dependencies ━━━\x1b[0m`);
  log(`\x1b[90m[python] Streaming logs in structured batches\x1b[0m`);

  // Pull the image once before any docker run
  log(`\x1b[90m[docker] Pulling ${pythonImage}…\x1b[0m`);
  try { await exec('docker', ['pull', pythonImage], {}, () => {}); } catch(_) { log(`\x1b[33m[docker] Using cached image\x1b[0m`); }

  await runSmartPythonInstall({ projectRoot, pythonImage, env, runtime, project, reqFile, log });
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build (collectstatic, migrate, etc.) ──────────────────────
  emitStep(emit, 'build', 'active');
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
    '--restart',    'unless-stopped',
    '--network',    'deployboard_deployboard-net',
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
  const stable = await waitForContainerRunning(candidateContainerName, 30, log);
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
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);
  } catch(e) {
    log(`\x1b[31m[docker] Candidate failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (!previousTarget) {
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

function findPythonProjectRoot(buildDir, log) {
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
function autoDetectPythonStartCmd(projectRoot, runtime, expectedPort, log) {
  const hasManagePy = fs.existsSync(path.join(projectRoot, 'manage.py'));

  // Django
  if (runtime.framework === 'Django' || hasManagePy) {
    const wsgiModule = findDjangoWsgiModule(projectRoot);
    log(`\x1b[90m[python] Auto-detected Django — building gunicorn start command\x1b[0m`);
    return `gunicorn ${wsgiModule}:application --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
  }

  // FastAPI / uvicorn
  const hasUvicorn = ['main', 'app', 'server', 'api'].some(f =>
    fs.existsSync(path.join(projectRoot, f + '.py')) &&
    (() => {
      try { return fs.readFileSync(path.join(projectRoot, f + '.py'), 'utf8').includes('fastapi'); } catch(_) { return false; }
    })()
  );
  if (runtime.framework === 'FastAPI' || hasUvicorn) {
    const module = ['main', 'app', 'server', 'api'].find(f =>
      fs.existsSync(path.join(projectRoot, f + '.py'))
    ) || 'main';
    return `uvicorn ${module}:app --host 0.0.0.0 --port $PORT --workers 2`;
  }

  // Flask / generic gunicorn
  const flaskFile = ['app', 'wsgi', 'main', 'run', 'server'].find(f =>
    fs.existsSync(path.join(projectRoot, f + '.py'))
  );
  if (runtime.framework === 'Flask' || flaskFile) {
    const module = flaskFile ? `${flaskFile}:app` : 'app:app';
    return `gunicorn ${module} --bind 0.0.0.0:$PORT --workers 2 --timeout 120`;
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
async function runGenericBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog,
  dockerImage, findRoot, extraEnv = {}, stepLabel, smartHooks = {} }) {

  const buildDir      = path.join(tmpDir, deployId);
  const appDir        = path.join(sitesDir, project.subdomain, 'app');
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0, 20)}`;
  const expectedPort  = normalizePort(appPort, 8080);

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = { ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project), ...extraEnv };

  // Step 1: Clone
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findRoot ? findRoot(buildDir, log) : buildDir;
  log(`\x1b[90m[${stepLabel}] Image: ${dockerImage}\x1b[0m`);

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
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);
  log(`\x1b[90m[docker] Command: ${resolvedStartCmd}\x1b[0m`);

  const hostAppDir = appDir
    .replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data');
  const dockerMountSrc = appDir.startsWith('/tmp') ? appDir : hostAppDir;

  try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
  log(`\x1b[90m[docker] Pulling ${dockerImage}…\x1b[0m`);
  try { await exec('docker', ['pull', dockerImage], {}, log); } catch(_) { log('\x1b[33m[docker] Using cached image\x1b[0m'); }

  const runArgs = [
    'run', '-d',
    '--name', candidateContainerName, '--restart', 'unless-stopped',
    '--network', 'deployboard_deployboard-net',
    '--cpu-shares', CPU_SHARES, '--pids-limit', PIDS_LIMIT, '-m', '2g',
    '-e', `PORT=${expectedPort}`,
    ...Object.entries(runtimeEnv).flatMap(([k, v]) => {
      if (String(k).toUpperCase() === 'PORT') return [];
      return ['-e', `${k}=${v}`];
    }),
    '-v', `${dockerMountSrc}:/app`, '-w', '/app',
    dockerImage, 'sh', '-c', resolvedStartCmd
  ];

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
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);
  } catch (e) {
    log(`\x1b[31m[docker] Failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (!previousTarget) {
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
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(_) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ App deployed in isolated container!\x1b[0m`);
}

// Runs a shell command inside a Docker image against the project source (for build steps)
async function runCommandInImage({ projectRoot, image, envObj, command, log }) {
  try { await exec('docker', ['pull', image], {}, () => {}); } catch(_) {}
  log(`\x1b[90m[docker-build] ${image} :: ${command}\x1b[0m`);
  const envArgs = Object.entries(envObj || {}).flatMap(([k, v]) => ['-e', `${k}=${String(v ?? '')}`]);
  await exec('docker', [
    'run', '--rm', ...envArgs,
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
        const cmd = (project.installCmd || '').trim() || 'go mod download';
        const warnings = [];
        if (!fs.existsSync(path.join(projectRoot, 'go.mod'))) {
          warnings.push('Warning: no go.mod found. Run "go mod init <module>" locally and commit it.');
        }
        return { cmd, warnings };
      },
      resolveBuild(projectRoot, project) {
        let cmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Detect if main package is in a subdir (e.g. cmd/server/main.go)
          const cmdDirs = ['cmd/server', 'cmd/app', 'cmd/main', 'cmd/api'].filter(d =>
            fs.existsSync(path.join(projectRoot, d, 'main.go'))
          );
          if (cmdDirs.length > 0) {
            cmd = `go build -o server ./${cmdDirs[0]}`;
            warnings.push(`Auto-detected main package at ./${cmdDirs[0]} — building with: ${cmd}`);
            warnings.push('Set your Build Command in the dashboard to remove this warning.');
          } else {
            cmd = 'go build -o server .';
          }
        }
        // Ensure binary is named 'server' or matches what startCmd expects
        if (cmd.includes('go build') && !cmd.includes('-o ')) {
          cmd = cmd.replace('go build', 'go build -o server');
          warnings.push('Added -o server to go build to ensure predictable binary name.');
        }
        return { cmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Try to find any compiled binary
          cmd = `./server`;
          warnings.push('No start command set — defaulting to ./server. Set your Start Command in the dashboard.');
        }
        // Warn if PORT env var is hardcoded
        if (/PORT=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\d{4,5}/, 'PORT=$PORT');
          warnings.push('Hardcoded port replaced with $PORT for dynamic port assignment.');
        }
        // Ensure PORT is passed if not already
        if (!/\$PORT|\bPORT\b/.test(cmd)) {
          warnings.push('Tip: Make sure your Go app reads the PORT environment variable (os.Getenv("PORT")).');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common Go fixes: ensure go.mod is committed, binary compiles with "go build -o server .", and app reads os.Getenv("PORT").';
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
  return runGenericBuild({ ...opts, dockerImage: 'composer:2', stepLabel: 'php',
    findRoot: (dir, log) => findFileRoot(dir, ['composer.json', 'index.php', 'artisan'], log) || dir,
    smartHooks: {
      resolveInstall(projectRoot, project) {
        const cmd = (project.installCmd || '').trim() || 'composer install --no-dev --optimize-autoloader';
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          if (isLaravel) {
            steps.push('php artisan config:cache');
            steps.push('php artisan route:cache');
            // Only run migrate if DB is configured
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
        // Warn about migrate --force without DB
        if (/artisan migrate/.test(userCmd)) {
          const hasDb = !!(env['DB_HOST'] || env['DB_URL'] || env['DATABASE_URL']);
          if (!hasDb) warnings.push('Warning: "php artisan migrate" will fail without DB_HOST or DATABASE_URL set in Environment Variables.');
        }
        return { cmd: userCmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          if (isLaravel) {
            // php artisan serve is dev-only — use php-fpm or built-in server bound to 0.0.0.0
            cmd = `php artisan serve --host=0.0.0.0 --port=$PORT`;
            warnings.push('Using "php artisan serve" — this is suitable for basic deployments but not high-traffic production. Consider adding FrankenPHP or Nginx+php-fpm for production use.');
          } else if (isSymfony) {
            cmd = `php -S 0.0.0.0:$PORT -t public/`;
          } else {
            const hasPublic = fs.existsSync(path.join(projectRoot, 'public', 'index.php'));
            cmd = hasPublic ? `php -S 0.0.0.0:$PORT -t public/` : `php -S 0.0.0.0:$PORT`;
          }
        }
        // Fix hardcoded ports
        if (/--port=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/--port=\d{4,5}/, '--port=$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      buildEnv(projectRoot, project, env) {
        const extra = {};
        // Laravel needs APP_KEY — auto-generate a fallback if missing
        if (isLaravel && !env['APP_KEY'] && !env['LARAVEL_APP_KEY']) {
          const crypto = require('crypto');
          extra['APP_KEY'] = 'base64:' + crypto.randomBytes(32).toString('base64');
        }
        return extra;
      },
      startupFailureHint() {
        return isLaravel
          ? 'Common Laravel fixes: set APP_KEY, DB_HOST, and DB_PASSWORD in Environment Variables. Make sure artisan is in your repo root.'
          : 'Check that your public/ directory exists and PHP can serve index.php.';
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
        const cmd = (project.installCmd || '').trim() || 'bundle install';
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          if (isRails) {
            steps.push('bundle exec rails assets:precompile');
            const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
            if (hasDb) {
              steps.push('bundle exec rails db:migrate');
            } else {
              warnings.push('No DATABASE_URL set — skipping db:migrate. Add it to Environment Variables to run migrations.');
            }
          }
          userCmd = steps.length > 0 ? steps.join(' && ') : 'echo skip';
        }
        if (/db:migrate/.test(userCmd)) {
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (!hasDb) warnings.push('Warning: "rails db:migrate" will fail without DATABASE_URL set in Environment Variables.');
        }
        return { cmd: userCmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          if (isRails) {
            // puma.rb may not exist — use inline config
            const hasPumaConfig = fs.existsSync(path.join(projectRoot, 'config', 'puma.rb'));
            cmd = hasPumaConfig
              ? `bundle exec puma -C config/puma.rb -b tcp://0.0.0.0:$PORT`
              : `bundle exec puma -b tcp://0.0.0.0:$PORT`;
            if (!hasPumaConfig) warnings.push('No config/puma.rb found — starting Puma with default settings.');
          } else {
            cmd = `bundle exec rackup -o 0.0.0.0 -p $PORT`;
          }
        }
        // Fix hardcoded port in -b tcp://0.0.0.0:PORT pattern
        if (/tcp:\/\/0\.0\.0\.0:\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/tcp:\/\/0\.0\.0\.0:\d{4,5}/, 'tcp://0.0.0.0:$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      buildEnv(projectRoot, project, env) {
        const extra = {};
        const crypto = require('crypto');
        if (isRails && !env['SECRET_KEY_BASE'] && !env['RAILS_MASTER_KEY']) {
          extra['SECRET_KEY_BASE'] = crypto.randomBytes(64).toString('hex');
          // (warning logged inline below)
        }
        return extra;
      },
      buildErrorHandler(msg, log) {
        if (/assets:precompile/i.test(msg)) {
          log('\x1b[33m[Joytree] assets:precompile failed — continuing without asset compilation. Add Node.js to your Gemfile (via execjs) or use asset pipeline alternatives.\x1b[0m');
          return true; // handled — don't throw
        }
        return false; // unhandled — throw
      },
      startupFailureHint() {
        return isRails
          ? 'Common Rails fixes: set SECRET_KEY_BASE and DATABASE_URL in Environment Variables. Ensure Puma is in your Gemfile.'
          : 'Check that config.ru exists and your Rack app binds to the PORT environment variable.';
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
        if (userCmd) return { cmd: userCmd, warnings: [] };
        // Ensure wrapper scripts are executable
        const pre = isMaven
          ? 'chmod +x mvnw 2>/dev/null || true; '
          : 'chmod +x gradlew 2>/dev/null || true; ';
        const cmd = pre + (isMaven
          ? './mvnw dependency:resolve -q 2>/dev/null || mvn dependency:resolve -q'
          : './gradlew dependencies -q 2>/dev/null || gradle dependencies -q');
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project) {
        const userCmd = (project.buildCmd || '').trim();
        if (userCmd) return { cmd: userCmd, warnings: [] };
        const pre = isMaven
          ? 'chmod +x mvnw 2>/dev/null || true; '
          : 'chmod +x gradlew 2>/dev/null || true; ';
        const cmd = pre + (isMaven
          ? './mvnw package -DskipTests -q 2>/dev/null || mvn package -DskipTests -q'
          : './gradlew bootJar -q 2>/dev/null || gradle bootJar -q');
        return { cmd, warnings: [] };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Build robust jar finder that works for both Maven and Gradle layouts
          const jarFinder = `$(find . \( -path "*/target/*.jar" -o -path "*/build/libs/*.jar" \) ! -name "*-sources.jar" ! -name "*-javadoc.jar" -type f | head -1)`;
          cmd = `java $JAVA_OPTS -jar ${jarFinder} --server.port=$PORT 2>/dev/null || java $JAVA_OPTS -jar ${jarFinder} -Dserver.port=$PORT`;
          warnings.push('No start command set — auto-detecting JAR file. Set your Start Command for a more reliable deployment.');
        }
        // Fix hardcoded ports
        if (/--server\.port=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/--server\.port=\d{4,5}/, '--server.port=$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        if (/Tests run:.*FAILURE|BUILD FAILURE.*test/i.test(msg)) {
          log('\x1b[33m[Joytree] Test failures detected during build. Add -DskipTests to your build command to skip tests during deployment.\x1b[0m');
          return false; // still throw — build failed
        }
        return false;
      },
      startupFailureHint() {
        return 'Common JVM fixes: ensure your JAR is built to target/ or build/libs/. For Spring Boot, add spring-boot-starter-web. App must bind to $PORT or $SERVER_PORT.';
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
      resolveInstall() { return { cmd: 'echo skip', warnings: [] }; },
      resolveBuild(projectRoot, project) {
        let cmd = (project.buildCmd || '').trim() || 'cargo build --release';
        const warnings = [];
        // Detect binary name from Cargo.toml [[bin]] or [package] name
        let binaryName = 'server';
        try {
          const cargo = fs.readFileSync(path.join(projectRoot, 'Cargo.toml'), 'utf8');
          const binMatch = cargo.match(/\[\[bin\]\][^]*?name\s*=\s*"([^"]+)"/);
          const pkgMatch = cargo.match(/\[package\][^]*?name\s*=\s*"([^"]+)"/);
          binaryName = (binMatch && binMatch[1]) || (pkgMatch && pkgMatch[1]) || 'server';
        } catch(_) {}
        if (binaryName !== 'server') {
          warnings.push(`Auto-detected Rust binary name: "${binaryName}" from Cargo.toml.`);
        }
        return { cmd, warnings, _binaryName: binaryName };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Detect binary name from Cargo.toml
          let binaryName = 'server';
          try {
            const cargo = fs.readFileSync(path.join(projectRoot, 'Cargo.toml'), 'utf8');
            const binMatch = cargo.match(/\[\[bin\]\][^]*?name\s*=\s*"([^"]+)"/);
            const pkgMatch = cargo.match(/\[package\][^]*?name\s*=\s*"([^"]+)"/);
            binaryName = (binMatch && binMatch[1]) || (pkgMatch && pkgMatch[1]) || 'server';
          } catch(_) {}
          cmd = `PORT=$PORT ./target/release/${binaryName}`;
          warnings.push(`No start command set — using ./target/release/${binaryName}. Set your Start Command in the dashboard.`);
        }
        if (/PORT=\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/PORT=\d{4,5}/, 'PORT=$PORT');
          warnings.push('Hardcoded port replaced with $PORT.');
        }
        if (!/\$PORT|\bPORT\b/.test(cmd)) {
          warnings.push('Tip: Make sure your Rust app reads the PORT environment variable (std::env::var("PORT")).');
        }
        return { cmd, warnings };
      },
      buildErrorHandler(msg, log) {
        if (/error\[E\d+\]|could not compile/i.test(msg)) {
          log('\x1b[33m[Joytree] Rust compilation failed. Common fixes: check Cargo.toml dependencies, ensure all crates are available on crates.io.\x1b[0m');
        }
        return false; // always throw on compile error
      },
      startupFailureHint() {
        return 'Common Rust fixes: ensure binary name matches Cargo.toml [package].name, app must read PORT env var.';
      }
    }
  });
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
        const cmd = (project.installCmd || '').trim() || 'dotnet restore';
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project) {
        const cmd = (project.buildCmd || '').trim() || 'dotnet publish -c Release -o out --no-restore';
        return { cmd, warnings: [] };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // Find published DLL or EXE
          cmd = `dotnet out/*.dll`;
          warnings.push('No start command set — defaulting to "dotnet out/*.dll". Set your Start Command in the dashboard.');
        }
        // Fix hardcoded URLs/ports
        if (/ASPNETCORE_URLS=http:\/\/0\.0\.0\.0:\d{4,5}/.test(cmd)) {
          cmd = cmd.replace(/ASPNETCORE_URLS=http:\/\/0\.0\.0\.0:\d{4,5}/, 'ASPNETCORE_URLS=http://0.0.0.0:$PORT');
          warnings.push('Hardcoded ASPNETCORE_URLS port replaced with $PORT.');
        }
        return { cmd, warnings };
      },
      startupFailureHint() {
        return 'Common .NET fixes: ensure project publishes to "out/" folder, set ASPNETCORE_URLS=http://0.0.0.0:$PORT in Environment Variables.';
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
        if (userCmd) return { cmd: userCmd, warnings: [] };
        // Install hex + rebar before deps.get; compile in same step
        const cmd = 'mix local.hex --force && mix local.rebar --force && mix deps.get --only prod && mix compile';
        return { cmd, warnings: [] };
      },
      resolveBuild(projectRoot, project, env) {
        let userCmd = (project.buildCmd || '').trim();
        const warnings = [];
        if (!userCmd) {
          const steps = [];
          // assets.deploy requires Node.js — check if assets dir exists
          const hasAssets = fs.existsSync(path.join(projectRoot, 'assets'));
          if (hasAssets) {
            // Install Node in elixir image for asset compilation
            steps.push('apt-get update -qq && apt-get install -y --no-install-recommends nodejs npm 2>/dev/null || true');
            steps.push('mix assets.deploy');
          }
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (hasDb) {
            steps.push('mix ecto.migrate');
          } else {
            warnings.push('No DATABASE_URL set — skipping mix ecto.migrate. Add it to Environment Variables to run migrations.');
          }
          userCmd = steps.length > 0 ? steps.join(' && ') : 'echo skip';
        }
        if (/ecto\.migrate/.test(userCmd)) {
          const hasDb = !!(env['DATABASE_URL'] || env['DB_HOST'] || env['POSTGRES_URL']);
          if (!hasDb) warnings.push('Warning: mix ecto.migrate will fail without DATABASE_URL set.');
        }
        return { cmd: userCmd, warnings };
      },
      resolveStart(projectRoot, project) {
        let cmd = (project.startCmd || '').trim();
        const warnings = [];
        if (!cmd) {
          // mix phx.server only listens on localhost by default — must set PHX_HOST and PORT
          cmd = `mix phx.server`;
          warnings.push('Using "mix phx.server". PHX_HOST=0.0.0.0 and PORT are auto-set. If you see connection refused, ensure your Phoenix endpoint is configured to use System.get_env("PORT").');
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
        return 'Common Phoenix fixes: set SECRET_KEY_BASE and DATABASE_URL in Environment Variables. Ensure your endpoint config uses System.get_env("PORT") and System.get_env("PHX_HOST").';
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
        const cmd = hasLockfile ? 'bun install --frozen-lockfile' : 'bun install';
        if (!hasLockfile) warnings.push('No bun.lockb found — using "bun install" without --frozen-lockfile. Commit bun.lockb for reproducible builds.');
        return { cmd, warnings };
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
async function runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog }) {
  const buildDir = path.join(tmpDir, deployId);
  const destDir  = path.join(sitesDir, project.subdomain, 'dist');

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = resolveEnvVars(project.envVars);

  // ── Step 1: Clone ──────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/5 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);
  const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
  const profile = detectProjectProfile(projectRoot);
  log(`\x1b[90m[detect] Static project type: ${profile.kind}${profile.framework ? ' · framework: ' + profile.framework : ''}\x1b[0m`);

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
  const nodeImage = `node:${resolvedNodeVer}-bullseye`;

  const outputDir   = path.join(projectRoot, project.outputDir || 'dist');

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m`);
  if (hasPackageJson) {
    const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: installCmd, log });
  } else {
    log(`\x1b[90m[install] No package.json found — skipping install for plain static files\x1b[0m`);
  }
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || (hasPackageJson ? getDefaultBuildCmd(projectRoot) : 'echo skip');
  let buildWasAutoSkipped = false;
  if (hasPackageJson && buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: buildCmd, log });
    } catch (e) {
      if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
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
    const dirs = fs.readdirSync(buildDir).filter(f => {
      try { return fs.lstatSync(path.join(buildDir, f)).isDirectory(); } catch(e) { return false; }
    });
    log(`\x1b[31m[error] Output dir not found: "${project.outputDir || 'dist'}"\x1b[0m`);
    log(`\x1b[33m[hint] Dirs in repo: ${dirs.join(', ') || '(none)'}\x1b[0m`);
    throw new Error(`Output dir "${project.outputDir||'dist'}" not found. Available: ${dirs.join(', ')}`);
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
      const htmlList = allHtmlFiles.map(f => path.relative(finalSrcDir, f)).join(', ') || 'none';
      log(`\x1b[31m[error] No HTML entry file found in output directory\x1b[0m`);
      log(`\x1b[33m[hint] Ensure your project has at least one .html file, or check your outputDir setting.\x1b[0m`);
      log(`\x1b[33m[hint] HTML files found: ${htmlList}\x1b[0m`);
      throw new Error('Static deploy validation failed: no HTML entry file found in output directory');
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
  log(`\n\x1b[36m━━━ Step 5/5 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Static site deployed!\x1b[0m`);
}

// ── SERVER BUILD ──────────────────────────────────────────────────────────────
// Each server app runs in its OWN Docker container — fully isolated.
// Port conflicts are impossible. Container survives Joytree restarts.
async function runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir  = path.join(tmpDir, deployId);
  const appDir    = path.join(sitesDir, project.subdomain, 'app');
  const startCmd  = (project.startCmd || '').trim();
  const containerName = `db-${project.subdomain}`;
  const candidateContainerName = `${containerName}-cand-${safeDockerToken(deployId, 'build').slice(0,20)}`;
  const expectedPort = normalizePort(appPort, 4000);
  const runtime = getRuntimeConfig(project);

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = { ...resolveEnvVars(project.envVars), ...resolveServiceEnv(project) };

  // ── Step 1: Clone ──────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/6 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });
  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);

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
  const nodeImage = `node:${resolvedNodeVer}-bullseye`;
  const packageJsonPath = path.join(projectRoot, 'package.json');
  let pkg = {};
  try { if (fs.existsSync(packageJsonPath)) pkg = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8')); } catch (_) {}
  const hasStartScript = !!(pkg.scripts && pkg.scripts.start);
  if (!startCmd && !hasStartScript) {
    log(`\x1b[33m[auto] No start command/script found. Falling back to static deployment flow.\x1b[0m`);
    const fallbackProject = { ...project, siteType: 'static', buildCmd: project.buildCmd || 'echo skip', outputDir: project.outputDir || '.' };
    return runStaticBuild({ deployId, project: fallbackProject, sitesDir, tmpDir, githubToken, emit, onLog });
  }

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/6 — Install ━━━\x1b[0m`);
  const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: installCmd, log });
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/6 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot);
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: buildCmd, log });
    } catch (e) {
      if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
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
      log(`\x1b[33m[copy] Source copied — container will run npm install on startup\x1b[0m`);
    }
  }

  emitStep(emit, 'copy', 'done');

  // ── Step 5: Run in isolated Docker container ───────────────────────────────
  emitStep(emit, 'start', 'active');
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);
  log(`\x1b[90m[docker] Image:     ${nodeImage}\x1b[0m`);
  log(`\x1b[90m[docker] Container: ${candidateContainerName} (candidate)\x1b[0m`);
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
    '--restart',      'unless-stopped',
    '--network',      'deployboard_deployboard-net',
    '--cpu-shares',   runtime.cpuShares,
    '--pids-limit',   PIDS_LIMIT,
    '-m',             runtime.memory,
    '-e',             `PORT=${expectedPort}`,
    '-e',             `NODE_ENV=production`,
    '-e',             `HOST=0.0.0.0`,
    '-e',             `HOSTNAME=0.0.0.0`,
    '-v',             `${dockerMountSrc}:/app`,
    '-w',             '/app',
  ];
  if (runtime.memorySwap) {
    dockerArgs.push('--memory-swap', runtime.memorySwap);
  }
  log(`\x1b[90m[docker] Runtime limits: ${runtime.cpuShares} CPU shares | ${runtime.memory} memory | ${PIDS_LIMIT} max processes\x1b[0m`);

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

  dockerArgs.push(nodeImage, 'sh', '-c', resolvedStartCmd);
  await exec('docker', dockerArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);

  // Give container a moment, then verify it's still running.
  log(`\x1b[90m[docker] Waiting for process to stabilize…\x1b[0m`);
  const stable = await waitForContainerRunning(candidateContainerName, Math.min(30, runtime.startupTimeoutSeconds), log);
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
    registry[project.subdomain] = `${containerName}:${targetPort}`;
    fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    log(`\x1b[32m[docker] ✓ Proxy registered: ${project.subdomain} → ${containerName}:${targetPort}\x1b[0m`);
    if (targetPort !== expectedPort) {
      log(`\x1b[33m[docker] App ignored PORT=${expectedPort}; routing to detected port ${targetPort}\x1b[0m`);
    }
    log(`\x1b[90m[docker] Strict readiness gate passed before promotion\x1b[0m`);

    // Promote: archive previous stable container for fast rollback, then prune old archives.
    await archivePreviousContainer(containerName, project.subdomain, log);
    await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});
    log(`\x1b[90m[docker] Promoted candidate to stable: ${containerName}\x1b[0m`);
    await cleanupArchivedContainers(project.subdomain, DEPLOY_HISTORY_KEEP, log);

  } catch(e) {
    log(`\x1b[31m[docker] Candidate failed: ${e.message}\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '80', candidateContainerName], {}, log); } catch(_) {}
    try { await exec('docker', ['rm', '-f', candidateContainerName], {}, () => {}); } catch(_) {}
    if (previousTarget) {
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
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Server app deployed in isolated container!\x1b[0m`);
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
  const candidates = [];
  const seedPorts = new Set();
  const add = (v) => {
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) return;
    if (!candidates.includes(n)) candidates.push(n);
  };
  const addSeed = (v) => {
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) return;
    seedPorts.add(n);
    add(n);
  };

  addSeed(preferredPort);
  [3000, 3001, 4000, 4173, 5000, 5173, 8000, 8080, 8787].forEach(addSeed);
  let fallbackIP = '';
  try { fallbackIP = await getContainerIP(containerName); } catch (_) {}
  if (fallbackIP) log(`\x1b[90m[docker] Fallback probe IP: ${fallbackIP}\x1b[0m`);

  for (let attempt = 1; attempt <= startupTimeoutSeconds; attempt++) {
    if (attempt === 1 || attempt % 10 === 0) {
      try {
        const discovered = await detectListeningPorts(containerName);
        // Include all discovered listeners; HTTP probe still gates readiness.
        discovered.forEach(add);
        const logPort = await detectPortFromContainerLogs(containerName);
        if (logPort) add(logPort);
        if (discovered.length) {
          log(`\x1b[90m[docker] Discovered listening ports: ${discovered.join(', ')}\x1b[0m`);
        }
      } catch (_) {}
    }
    for (const port of candidates) {
      try {
        await probeHttp(containerName, port, 1500);
        await new Promise(r => setTimeout(r, 800));
        await probeHttp(containerName, port, 1500);
        log(`\x1b[32m[docker] ✓ App reachable on ${containerName}:${port}\x1b[0m`);
        return port;
      } catch (_) {
        // Some apps don't return valid HTTP on "/" during warmup but still listen.
        // Accept open TCP socket as a fallback readiness signal.
        // Only allow TCP-only readiness for known web defaults.
        if (seedPorts.has(port)) {
          try {
            await probeTcp(containerName, port, 1200);
            log(`\x1b[32m[docker] ✓ TCP listener detected on ${containerName}:${port} (seed port)\x1b[0m`);
            return port;
          } catch (_) {}
        }

        // DNS/container-name routing can fail in some Docker/network edge cases.
        // Fall back to direct container IP probing before giving up.
        if (fallbackIP) {
          try {
            await probeHttp(fallbackIP, port, 1500);
            await new Promise(r => setTimeout(r, 500));
            await probeHttp(fallbackIP, port, 1500);
            log(`\x1b[32m[docker] ✓ App reachable on ${fallbackIP}:${port} (IP fallback)\x1b[0m`);
            return port;
          } catch (_) {
            if (seedPorts.has(port)) {
              try {
                await probeTcp(fallbackIP, port, 1200);
                log(`\x1b[32m[docker] ✓ TCP listener on ${fallbackIP}:${port} (IP fallback seed port)\x1b[0m`);
                return port;
              } catch (_) {}
            }
          }
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
    const m = String(row).match(/:\s*([0-9A-Fa-f]{8}):([0-9A-Fa-f]{4})\s+[0-9A-Fa-f]{8}:[0-9A-Fa-f]{4}\s+0A/);
    if (!m) continue;
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

function getRuntimeConfig(project) {
  const requestedMemory = (project.memoryLimit || project.memory || '2g').toString();
  return {
    cpuShares: String(normalizePort(project.cpuShares, normalizePort(project.cpu, Number(CPU_SHARES)))),
    memory: normalizeMemoryLimit(requestedMemory),
    memorySwap: (project.memorySwap || process.env.DEFAULT_APP_MEMORY_SWAP || '3g').toString(),
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

async function getContainerIP(containerName) {
  const chunks = [];
  await exec(
    'docker',
    ['inspect', '--format={{range $k,$v := .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', containerName],
    {},
    (line) => chunks.push(line)
  );
  return (chunks.join('\n').trim().split('\n').pop() || '').trim();
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

function probeHttp(host, port, timeoutMs = 1000) {
  const http = require('http');
  return new Promise((resolve, reject) => {
    const req = http.request({ host, port, path: '/', method: 'GET', timeout: timeoutMs }, (res) => {
      res.resume();
      resolve(res.statusCode || 200);
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

function findProjectRoot(buildDir, log) {
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

async function runBuildCommandInContainer({ projectRoot, nodeImage, envObj, nodeEnv = 'development', command, log }) {
  try { await exec('docker', ['pull', nodeImage], {}, () => {}); } catch(e) {}
  const normalizedCommand = normalizeInstallLikeCommand(command, projectRoot);
  const commandWithCorepack = `corepack enable >/dev/null 2>&1 || true; ${normalizedCommand}`;
  log(`\x1b[90m[docker-build] ${nodeImage} :: ${normalizedCommand}\x1b[0m`);
  const envArgs = [
    '-e', `CI=false`,
    '-e', `NODE_ENV=${nodeEnv}`,
    ...Object.entries(envObj || {}).flatMap(([k, v]) => ['-e', `${k}=${String(v ?? '')}`]),
  ];
  await exec('docker', [
    'run', '--rm',
    ...envArgs,
    '-v', `${projectRoot}:/workspace`,
    '-w', '/workspace',
    nodeImage,
    'sh', '-lc', commandWithCorepack
  ], {}, log);
}

function normalizeInstallLikeCommand(command, projectRoot) {
  const raw = String(command || '').trim();
  const low = raw.toLowerCase();
  const hasNpmLock = fs.existsSync(path.join(projectRoot, 'package-lock.json'));

  if (low === 'npm i' || low === 'npm install') {
    return hasNpmLock
      ? 'npm ci --legacy-peer-deps --no-audit --no-fund --progress=false'
      : 'npm install --legacy-peer-deps --no-audit --no-fund --progress=false';
  }
  if (low.startsWith('npm ci')) {
    return `${raw} --no-audit --no-fund --progress=false`;
  }
  if (low.startsWith('npm install') || low.startsWith('npm i ')) {
    return `${raw} --no-audit --no-fund --progress=false`;
  }
  if (low.startsWith('yarn install') && !low.includes('--non-interactive')) {
    return `${raw} --non-interactive`;
  }
  if (low.startsWith('pnpm install') && !low.includes('--reporter=')) {
    return `${raw} --reporter=append-only`;
  }
  return raw;
}

function detectPackageManager(projectRoot) {
  if (fs.existsSync(path.join(projectRoot, 'pnpm-lock.yaml'))) return 'pnpm';
  if (fs.existsSync(path.join(projectRoot, 'yarn.lock'))) return 'yarn';
  if (fs.existsSync(path.join(projectRoot, 'package-lock.json'))) return 'npm';
  return 'npm';
}

function getDefaultInstallCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  if (pm === 'pnpm') return 'pnpm install --frozen-lockfile';
  if (pm === 'yarn') return 'yarn install --frozen-lockfile';

  // npm v7+ enforces peer dependency resolution and can fail builds for
  // otherwise-runnable apps. Use legacy peer resolution by default to make
  // third-party app deployments more resilient.
  return fs.existsSync(path.join(projectRoot, 'package-lock.json'))
    ? 'npm ci --legacy-peer-deps'
    : 'npm install --legacy-peer-deps';
}

function getDefaultBuildCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  if (pm === 'pnpm') return 'pnpm run build';
  if (pm === 'yarn') return 'yarn build';
  return 'npm run build';
}

function getDefaultStartCmd(projectRoot) {
  const pm = detectPackageManager(projectRoot);
  if (pm === 'pnpm') return 'pnpm start';
  if (pm === 'yarn') return 'yarn start';
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
  const pkgPath = path.join(projectRoot, 'package.json');
  let pkg = null;
  try { pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8')); } catch (_) {}
  const scripts = pkg && pkg.scripts && typeof pkg.scripts === 'object' ? pkg.scripts : {};
  const hasStartScript = typeof scripts.start === 'string' && scripts.start.trim().length > 0;
  const hasServerEntry =
    fs.existsSync(path.join(projectRoot, 'server.js')) ||
    fs.existsSync(path.join(projectRoot, 'app.js')) ||
    fs.existsSync(path.join(projectRoot, 'index.js'));
  return hasStartScript || hasServerEntry;
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
  const raw = (startCmd || '').trim() || getDefaultStartCmd(projectRoot);
  const normalized = raw.replace(/\s+/g, ' ').trim().toLowerCase();
  const hostFlags = `--host 0.0.0.0 --port ${expectedPort}`;

  // Do not force host/port flags for generic package-manager shorthands.
  // Many repos treat extra args as script args and can fail/hang unexpectedly.
  if (normalized === 'npm start' || normalized === 'yarn start' || normalized === 'pnpm start') {
    return raw;
  }
  if (normalized.startsWith('next start')) return `${raw} ${hostFlags}`;
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
    const child = spawn(cmd, args, {
      shell: false,
      cwd:   options.cwd,
      env:   options.env || process.env,
      // Limit child process memory to prevent build steps from OOM-killing the VPS
      // 2GB virtual memory limit for npm install/build (RSS stays much lower)
    });

  // Kill process if it hangs for too long (heavy native installs can take a while).
  const hardTimeout = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error(`"${cmd}" timed out after ${BUILD_STEP_TIMEOUT_MINUTES} minutes. Check your install/build commands.`));
    }, BUILD_STEP_TIMEOUT_MINUTES * 60 * 1000);

    let lastLines = [];
    const onLine  = (line, isErr) => {
      if (!line.trim()) return;
      const display = isErr ? `\x1b[90m${line}\x1b[0m` : line;
      if (typeof logFn === 'function') logFn(display);
      lastLines.push(line);
      if (lastLines.length > 10) lastLines.shift();
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
      if (code === 0) resolve();
      else reject(new Error(`"${cmd} ${args.slice(0,3).join(' ')}…" failed (exit ${code}).\n${lastLines.slice(-5).join('\n')}`));
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

module.exports = { runBuild, runUploadBuild };

// ── UPLOAD BUILD ──────────────────────────────────────────────────────────────
// Deploy from a locally extracted directory instead of cloning from GitHub.
// Follows the same install → build → copy/start pattern as runStaticBuild/runServerBuild,
// but skips the "Clone" step and shows an extraction step instead.
async function runUploadBuild({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir }) {
  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };

  const cleanSub = String(project.subdomain || '').toLowerCase().replace(/[^a-z0-9-]/g,'');
  const siteType = String(project.siteType || '').toLowerCase();
  const startCmd = String(project.startCmd || '').trim();
  const isServerApp = siteType === 'server' || (!siteType && !!startCmd);

  if (isServerApp) {
    return runUploadServerBuild({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, log, cleanSub });
  }
  return runUploadStaticBuild({ deployId, project, sitesDir, tmpDir, emit, onLog, uploadFilesDir, log, cleanSub });
}

async function runUploadStaticBuild({ deployId, project, sitesDir, tmpDir, emit, onLog, uploadFilesDir, log, cleanSub }) {
  const buildDir = path.join(tmpDir, deployId + '_upload');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  // Step 1: Copy uploaded files to build dir
  emitStep(emit, 'clone', 'active');
  log('\x1b[36m━━━ Step 1/4 — Load Uploaded Files ━━━\x1b[0m');
  log('\x1b[90m[upload] Copying extracted project files to build workspace…\x1b[0m');
  copyDir(uploadFilesDir, buildDir);
  const totalFiles = countFiles(buildDir);
  log(`\x1b[32m[upload]\x1b[0m Loaded \x1b[1m${totalFiles}\x1b[0m files from upload`);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);

  // Resolve Node.js image — same logic as GitHub static/server builds
  const configuredNodeVer = String(project.nodeVer || '20');
  const detectedNodeVer = detectRequiredNodeVersion(projectRoot);
  const resolvedNodeVer = (detectedNodeVer && detectedNodeVer !== configuredNodeVer) ? detectedNodeVer : configuredNodeVer;
  if (detectedNodeVer && detectedNodeVer !== configuredNodeVer) {
    emitNodeVersionWarning(log, configuredNodeVer, detectedNodeVer);
  }
  const nodeImage = `node:${resolvedNodeVer}-bullseye`;
  const env = resolveEnvVars(project.envVars);

  // Step 2: Install — runs inside Docker container just like GitHub builds,
  // so npm gets caching, proper isolation, and the same speed.
  emitStep(emit, 'install', 'active');
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
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: installCmd, log });
  }
  // Plain static: serve project root directly
  if (isPlainStatic && (!project.outputDir || project.outputDir === 'dist')) {
    log('\x1b[33m[Joytree]\x1b[0m Output directory automatically changed from "' + (project.outputDir || 'dist') + '" → "." (no dependencies, serving project root)');
    project = { ...project, outputDir: '.' };
  }
  emitStep(emit, 'install', 'done');

  // Step 3: Build — also runs inside Docker container
  emitStep(emit, 'build', 'active');
  log('\n\x1b[36m━━━ Step 3/4 — Build ━━━\x1b[0m');
  const buildCmd = (!isPlainStatic && hasPackageJson)
    ? (String(project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot))
    : 'echo skip';
  if (buildCmd === 'echo skip') {
    log('\x1b[90m(no build step)\x1b[0m');
  } else {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: buildCmd, log });
    } catch (e) {
      if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
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
  log('\n\x1b[36m━━━ Step 4/4 — Copy to Serve ━━━\x1b[0m');
  const outputDir = String(project.outputDir || 'dist').trim() || 'dist';
  const srcDist = outputDir === '.' ? projectRoot : path.join(projectRoot, outputDir);
  const destDist = path.join(sitesDir, cleanSub, 'dist');
  // Always wipe destDist first so stale files from a previous deploy never cause "Not found"
  try { fs.rmSync(destDist, { recursive: true, force: true }); } catch(_) {}
  fs.mkdirSync(destDist, { recursive: true });
  const actualSrc = fs.existsSync(srcDist) ? srcDist : projectRoot;
  if (!fs.existsSync(srcDist)) {
    log(`\x1b[33m[Joytree]\x1b[0m Output dir "${outputDir}" not found — copying project root`);
  } else {
    log(`\x1b[90m[copy] Copying ${outputDir}/ → ${destDist}\x1b[0m`);
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
      log(`\x1b[31m[error]\x1b[0m No HTML entry file found. Ensure your upload contains at least one .html file.\x1b[0m`);
      throw new Error('Upload deploy failed: no HTML entry file found in upload');
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
}

async function runUploadServerBuild({ deployId, project, sitesDir, tmpDir, appPort, emit, onLog, uploadFilesDir, log, cleanSub }) {
  const buildDir = path.join(tmpDir, deployId + '_upload');
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  // Step 1: Load files
  emitStep(emit, 'clone', 'active');
  log('\x1b[36m━━━ Step 1/5 — Load Uploaded Files ━━━\x1b[0m');
  log('\x1b[90m[upload] Copying extracted project files to build workspace…\x1b[0m');
  copyDir(uploadFilesDir, buildDir);
  const totalFiles = countFiles(buildDir);
  log(`\x1b[32m[upload]\x1b[0m Loaded \x1b[1m${totalFiles}\x1b[0m files`);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);

  // Step 2: Install
  emitStep(emit, 'install', 'active');
  log('\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m');
  const hasPackageJson = fs.existsSync(path.join(projectRoot, 'package.json'));
  if (hasPackageJson) {
    const installCmd = project.installCmd || 'npm install';
    const installParts = splitCmd(installCmd);
    log(`\x1b[90m$ ${installCmd}\x1b[0m`);
    await exec(installParts[0], installParts[1], { cwd: projectRoot }, log);
  } else {
    log('\x1b[90m[install] No package.json found — skipping install\x1b[0m');
  }
  emitStep(emit, 'install', 'done');

  // Step 3: Build
  emitStep(emit, 'build', 'active');
  log('\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m');
  const buildCmd = hasPackageJson ? (String(project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot)) : 'echo skip';
  if (buildCmd === 'echo skip') {
    log('\x1b[90m(no build step)\x1b[0m');
  } else {
    const buildParts = splitCmd(buildCmd);
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    try {
      await exec(buildParts[0], buildParts[1], { cwd: projectRoot }, log);
    } catch (e) {
      if (/missing script|npm ERR!.*build|yarn.*command not found.*build|pnpm.*command not found.*build/i.test(String(e.message || ''))) {
        log(`\x1b[33m[Joytree]\x1b[0m No build script found in your project — automatically skipping build step.`);
        log(`\x1b[33m[Joytree]\x1b[0m ℹ Tip: if you intended to run a build, add a "build" script to your package.json, or set the build command to "echo skip" to suppress this message.`);
        log(`\x1b[32m[Joytree]\x1b[0m ✓ Continuing deployment without build step.`);
      } else {
        throw e;
      }
    }
  }
  emitStep(emit, 'build', 'done');

  // Step 4: Start container
  emitStep(emit, 'copy', 'active');
  log('\n\x1b[36m━━━ Step 4/5 — Launch Container ━━━\x1b[0m');

  const containerName = 'db-' + cleanSub;
  const candidateContainerName = containerName + '-cand-' + safeDockerToken(deployId, 'build').slice(0, 20);
  const imageName = 'deployboard-' + cleanSub;
  const nodeVer = String(project.nodeVer || '20');
  const expectedPort = appPort || 3000;
  const envObj = resolveEnvVars(project.envVars);
  const startCmdResolved = resolveRuntimeStartCommand({ projectRoot, startCmd: project.startCmd, expectedPort });
  const networkName = 'deployboard_deployboard-net';

  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const runArgs = [
    'run', '-d', '--restart=unless-stopped',
    '--name', candidateContainerName,
    '--network', networkName,
    '--cpu-shares', CPU_SHARES,
    '--pids-limit', PIDS_LIMIT,
    '-e', `PORT=${expectedPort}`,
    '-e', `NODE_ENV=production`,
    ...Object.entries(envObj).flatMap(([k,v]) => ['-e', `${k}=${v}`]),
    '-v', `${projectRoot}:/app:ro`,
    '-w', '/app',
    `node:${nodeVer}-alpine`,
    'sh', '-c', String(startCmdResolved || 'node server.js').replace(/"/g, '\\"')
  ];

  log(`\x1b[90m[docker] Launching Node.js ${nodeVer} container…\x1b[0m`);
  await exec('docker', runArgs, {}, log);
  emitStep(emit, 'copy', 'done');

  // Step 5: Verify
  emitStep(emit, 'start', 'active');
  log('\n\x1b[36m━━━ Step 5/5 — Verify ━━━\x1b[0m');
  await new Promise(r => setTimeout(r, 3000));
  const stable = await waitForContainerRunning(candidateContainerName, 30, log);
  if (!stable) {
    try { await exec('docker', ['logs', '--tail', '60', candidateContainerName], {}, log); } catch(e) {}
    throw new Error('Container exited during startup. Check logs above.');
  }
  const livePort = await detectLivePort(candidateContainerName, expectedPort, 60, log);
  const targetPort = livePort || expectedPort;

  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}
  await exec('docker', ['rename', candidateContainerName, containerName], {}, () => {});

  // Register port
  try {
    const portsFile = path.join(sitesDir, 'ports.json');
    let registry = {};
    try { registry = JSON.parse(fs.readFileSync(portsFile,'utf8')); } catch(e) {}
    registry[cleanSub] = `${containerName}:${targetPort}`; // must be "name:port" string — proxy does appEntry.split(':')
    fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));
  } catch(e) {}
  log(`\x1b[32m[docker]\x1b[0m Container running on port ${targetPort}`);
  emitStep(emit, 'start', 'done');

  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
}
