'use strict';

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
  const isServerApp = (project.siteType === 'server') || !!(project.startCmd || '').trim();
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
    '--name',         containerName,
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

  let containerIP = '';
  for (let i = 0; i < 5; i++) {
    try {
      const { execSync } = require('child_process');
      const ip = execSync(
        `docker inspect -f '{{range $k,$v := .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${containerName}`,
        { encoding: 'utf8' }
      ).trim();
      if (ip) { containerIP = ip; break; }
    } catch(e) {}
    await new Promise(r => setTimeout(r, 2000));
  }

  if (!containerIP) throw new Error('Could not get container IP. Check docker logs: docker logs ' + containerName);
  log(`\x1b[90m[docker] Container IP: ${containerIP}\x1b[0m`);

  // Register in port registry
  const portsFile = path.join(sitesDir, 'ports.json');
  let registry = {};
  try { registry = JSON.parse(fs.readFileSync(portsFile, 'utf8')); } catch(e) {}
  registry[project.subdomain] = containerIP + ':' + exposedPort;
  fs.writeFileSync(portsFile, JSON.stringify(registry, null, 2));

  log(`\x1b[32m[docker] ✓ Dockerfile app is live at ${project.subdomain}\x1b[0m`);
  emitStep(emit, 'start', 'done');

  // Cleanup
  fs.rmSync(buildDir, { recursive: true, force: true });
}

// ── WORKER BUILD ─────────────────────────────────────────────────────────────
// Same as server build but skips port polling — workers don't expose HTTP ports
async function runWorkerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const buildDir      = path.join(tmpDir, deployId);
  const containerName = 'db-' + project.subdomain;
  const nodeImage     = 'node:' + (project.nodeVer || '18') + '-alpine';
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
    '--name',         containerName,
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
    const state = execSync(`docker inspect --format='{{.State.Status}}' ${containerName}`, { encoding: 'utf8' }).trim();
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
  const nodeImage = `node:${project.nodeVer || '20'}-alpine`;

  // ── Step 1: Clone ──────────────────────────────────────────────────────────
  emitStep(emit, 'clone', 'active');
  log(`\x1b[36m━━━ Step 1/5 — Clone ━━━\x1b[0m`);
  if (fs.existsSync(buildDir)) fs.rmSync(buildDir, { recursive: true, force: true });
  fs.mkdirSync(buildDir, { recursive: true });

  await cloneRepo(project, buildDir, githubToken, log);
  emitStep(emit, 'clone', 'done');

  const projectRoot = findProjectRoot(buildDir, log);
  const outputDir   = path.join(projectRoot, project.outputDir || 'dist');

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m`);
  const installCmd = (project.installCmd || '').trim() || getDefaultInstallCmd(projectRoot);
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await runBuildCommandInContainer({ projectRoot, nodeImage, envObj: env, nodeEnv: 'development', command: installCmd, log });
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m`);
  const buildCmd = (project.buildCmd || '').trim() || getDefaultBuildCmd(projectRoot);
  if (buildCmd !== 'echo skip') {
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

  if (!fs.existsSync(srcDir)) {
    const dirs = fs.readdirSync(buildDir).filter(f => {
      try { return fs.lstatSync(path.join(buildDir, f)).isDirectory(); } catch(e) { return false; }
    });
    log(`\x1b[31m[error] Output dir not found: "${project.outputDir || 'dist'}"\x1b[0m`);
    log(`\x1b[33m[hint] Dirs in repo: ${dirs.join(', ') || '(none)'}\x1b[0m`);
    throw new Error(`Output dir "${project.outputDir||'dist'}" not found. Available: ${dirs.join(', ')}`);
  }

  if (fs.existsSync(destDir)) fs.rmSync(destDir, { recursive: true, force: true });
  fs.mkdirSync(destDir, { recursive: true });
  copyDir(srcDir, destDir);
  const count = countFiles(destDir);
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
  const nodeImage = `node:${project.nodeVer || '20'}-alpine`;
  const containerName = `db-${project.subdomain}`;

  const log = line => { emit('build:log', { line }); if (typeof onLog === 'function') onLog(line); };
  const env = resolveEnvVars(project.envVars);

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
  log(`\x1b[90m[docker] Container: ${containerName}\x1b[0m`);
  const resolvedStartCmd = startCmd || getDefaultStartCmd(usedBuildDir || projectRoot);
  log(`\x1b[90m[docker] Command:   ${resolvedStartCmd}\x1b[0m`);

  // Stop previous container
  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

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
    '--name',         containerName,
    '--restart',      'unless-stopped',
    '--network',      'deployboard_deployboard-net',
    '--cpu-shares',   CPU_SHARES,
    '--pids-limit',   PIDS_LIMIT,
    '-e',             `PORT=${appPort}`,
    '-e',             `NODE_ENV=production`,
    '-v',             `${dockerMountSrc}:/app`,
    '-w',             '/app',
  ];
  log(`\x1b[90m[docker] Runtime limits: ${CPU_SHARES} CPU shares | ${PIDS_LIMIT} max processes\x1b[0m`);

  for (const [k, v] of Object.entries(env)) {
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
  await new Promise(r => setTimeout(r, 3000));
  let state = 'unknown';
  try {
    const { execSync } = require('child_process');
    state = execSync(`docker inspect --format='{{.State.Status}}' ${containerName}`, { encoding: 'utf8' }).trim();
  } catch(e) {}
  if (state !== 'running') {
    log(`\x1b[31m[docker] ✗ Container is not running (state: ${state})\x1b[0m`);
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '60', containerName], {}, log); } catch(e) {}
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    throw new Error(`Container "${containerName}" exited during startup. Check logs above.`);
  }

  // Register proxy by stable container DNS name + expected app port.
  // Docker network DNS resolves containerName reliably and avoids fragile IP polling.
  try {
    const sitesDir = (usedBuildDir.startsWith('/tmp') ? appDir : usedBuildDir).replace(`/${project.subdomain}/app`, '');
    const pFile    = path.join(sitesDir, 'ports.json');
    let registry   = {};
    try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(e) {}
    registry[project.subdomain] = `${containerName}:${appPort}`;
    fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
    log(`\x1b[32m[docker] ✓ Proxy registered: ${project.subdomain} → ${containerName}:${appPort}\x1b[0m`);
    log(`\x1b[90m[docker] Health checks happen on live traffic (Render/Vercel-style startup)\x1b[0m`);
  } catch(e) {
    log(`\x1b[33m[docker] Could not update port registry: ${e.message}\x1b[0m`);
  }

  emitStep(emit, 'start', 'done');

  // ── Step 6: Cleanup temp build dir ────────────────────────────────────────
  emitStep(emit, 'cleanup', 'active');
  log(`\n\x1b[36m━━━ Step 6/6 — Cleanup ━━━\x1b[0m`);
  try { fs.rmSync(buildDir, { recursive: true, force: true }); } catch(e) {}
  emitStep(emit, 'cleanup', 'done');
  log(`\n\x1b[32;1m✓ Server app deployed in isolated container!\x1b[0m`);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async function cloneRepo(project, buildDir, githubToken, log) {
  let cloneUrl = project.repoUrl.trim();
  if (githubToken && /^https:\/\/github\.com\//i.test(cloneUrl)) {
    const token = encodeURIComponent(githubToken);
    cloneUrl = cloneUrl.replace(/^https:\/\/github\.com\//i, `https://x-access-token:${token}@github.com/`);
  }
  const branch = (project.branch || 'main').trim();
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
  const commandWithCorepack = `corepack enable >/dev/null 2>&1 || true; ${command}`;
  log(`\x1b[90m[docker-build] ${nodeImage} :: ${command}\x1b[0m`);
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
  return fs.existsSync(path.join(projectRoot, 'package-lock.json')) ? 'npm ci' : 'npm install';
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

function exec(cmd, args, options, logFn) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      shell: false,
      cwd:   options.cwd,
      env:   options.env || process.env,
      // Limit child process memory to prevent build steps from OOM-killing the VPS
      // 2GB virtual memory limit for npm install/build (RSS stays much lower)
    });

    // Kill process if it hangs for more than 10 minutes (e.g. npm install waiting for input)
    const hardTimeout = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error(`"${cmd}" timed out after 10 minutes. Check your install/build commands.`));
    }, 10 * 60 * 1000);

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
