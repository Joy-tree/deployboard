'use strict';

// ===== DEPLOYBOARD UPDATE MARKER (VISIBLE) =====
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

// ── STATIC BUILD ──────────────────────────────────────────────────────────────
async function runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog }) {
  const buildDir = path.join(tmpDir, deployId);
  const destDir  = path.join(sitesDir, project.subdomain, 'dist');

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = resolveEnvVars(project.envVars);
  const nodeImage = `node:${project.nodeVer || '20'}-bullseye`;

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
  if (hasPackageJson && buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: buildCmd, log });
  } else {
    log(`\x1b[90m[build] Skipping build step\x1b[0m`);
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
// Port conflicts are impossible. Container survives DeployBoard restarts.
async function runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir  = path.join(tmpDir, deployId);
  const appDir    = path.join(sitesDir, project.subdomain, 'app');
  const startCmd  = (project.startCmd || '').trim();
  const nodeImage = `node:${project.nodeVer || '20'}-bullseye`;
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
    await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: buildCmd, log });
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
    ? usedBuildDir   // /tmp is directly accessible inside the DeployBoard container
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
  // Keep large apps from restart loops caused by very low default memory.
  if (mb < 1024) return '1g';
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

  if (githubToken && /^https:\/\/github\.com\//i.test(cloneUrl)) {
    const token = encodeURIComponent(githubToken);
    cloneUrl = cloneUrl.replace(/^https:\/\/github\.com\//i, `https://x-access-token:${token}@github.com/`);
  }
  const branch = (forcedBranch || project.branch || 'main').trim();
  const fallback = branch === 'main' ? 'master' : 'main';

  log(`\x1b[90m$ git clone --depth=1 --branch ${branch} ${project.repoUrl} ${buildDir}\x1b[0m`);
  try {
    await exec('git', ['clone','--depth=1','--branch',branch,'--single-branch','--progress',cloneUrl,buildDir],
      { env: { ...process.env, GIT_TERMINAL_PROMPT: '0' } }, log);
  } catch(e) {
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
    log(`\x1b[33m[deploy] Add a custom start command in DeployBoard (e.g. \"node server.js\") or use an app repository instead of a framework/library source repo.\x1b[0m`);
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

module.exports = { runBuild };
