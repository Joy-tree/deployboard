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

// ── Entry point ───────────────────────────────────────────────────────────────
async function runBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog, isDockerfileDeploy, isWorker }) {
  // Merge flags from args or from project record
  const dockerfileDeploy = isDockerfileDeploy || project.isDockerfileDeploy;
  const workerDeploy     = isWorker           || project.isWorker;

  if (dockerfileDeploy) {
    return runDockerfileBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog });
  }
  const isServerApp = workerDeploy || (project.siteType === 'server') || !!(project.startCmd || '').trim();
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

  const projectRoot = findProjectRoot(buildDir, log);
  const dfFullPath  = path.join(projectRoot, dfPath);
  if (!fs.existsSync(dfFullPath)) throw new Error(`Dockerfile not found at: ${dfPath}`);

  // ── Step 2: Build Docker image ───────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log('\n\x1b[36m━━━ Step 2/4 — Docker Build ━━━\x1b[0m');
  log(`\x1b[90m$ docker build -f ${dfPath} -t ${imageName} .\x1b[0m`);

  // Build args from envVars
  const envObj  = resolveEnvVars(project.envVars);
  const buildArgs = Object.entries(envObj).map(([k,v]) => `--build-arg ${k}=${v}`).join(' ');
  const buildCmd  = `docker build -f ${dfPath} ${buildArgs} -t ${imageName} ${projectRoot}`;

  await exec('sh', ['-c', buildCmd], { cwd: projectRoot }, log);
  emitStep(emit, 'build', 'done');

  // ── Step 3: Stop old container, start new one ────────────────────────────
  emitStep(emit, 'copy', 'active');
  log('\n\x1b[36m━━━ Step 3/4 — Start Container ━━━\x1b[0m');

  try { await exec('docker', ['rm', '-f', containerName], {}, () => {}); } catch(e) {}

  const networkName = 'deployboard_deployboard-net';
  const runArgs = [
    'run', '-d', '--restart=unless-stopped',
    '--name', containerName,
    '--network', networkName,
    '-e', `PORT=${exposedPort}`,
    ...Object.entries(envObj).flatMap(([k,v]) => ['-e', `${k}=${v}`]),
    imageName
  ];

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
  const outputDir   = path.join(projectRoot, project.outputDir || 'dist');

  // ── Step 2: Install ────────────────────────────────────────────────────────
  emitStep(emit, 'install', 'active');
  log(`\n\x1b[36m━━━ Step 2/5 — Install ━━━\x1b[0m`);
  const installCmd = project.installCmd || 'npm install';
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await exec(...splitCmd(installCmd), {
    cwd: projectRoot,
    env: { ...process.env, ...env, NODE_ENV: 'development', CI: 'false' }
  }, log);
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/5 — Build ━━━\x1b[0m`);
  const buildCmd = project.buildCmd || 'npm run build';
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    const localBin = path.join(projectRoot, 'node_modules', '.bin');
    await exec(...splitCmd(buildCmd), {
      cwd: projectRoot,
      env: { ...process.env, ...env,
             PATH: localBin + ':' + process.env.PATH,
             NODE_ENV: 'production', CI: 'false' }
    }, log);
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
    const dirs = fs.readdirSync(buildDir).filter(f =>
      fs.statSync(path.join(buildDir, f)).isDirectory());
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
  const startCmd  = (project.startCmd || 'npm start').trim();
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
  const installCmd = project.installCmd || 'npm install';
  log(`\x1b[90m$ ${installCmd}\x1b[0m`);
  await exec(...splitCmd(installCmd), {
    cwd: projectRoot,
    env: { ...process.env, ...env, NODE_ENV: 'development', CI: 'false' }
  }, log);
  emitStep(emit, 'install', 'done');

  // ── Step 3: Build ──────────────────────────────────────────────────────────
  emitStep(emit, 'build', 'active');
  log(`\n\x1b[36m━━━ Step 3/6 — Build ━━━\x1b[0m`);
  const buildCmd = project.buildCmd || 'npm run build';
  if (buildCmd !== 'echo skip') {
    log(`\x1b[90m$ ${buildCmd}\x1b[0m`);
    const localBin = path.join(projectRoot, 'node_modules', '.bin');
    await exec(...splitCmd(buildCmd), {
      cwd: projectRoot,
      env: { ...process.env, ...env,
             PATH: localBin + ':' + process.env.PATH,
             NODE_ENV: 'production', CI: 'false' }
    }, log);
  }
  emitStep(emit, 'build', 'done');

  // ── Step 4: Copy to permanent app dir ─────────────────────────────────────
  emitStep(emit, 'copy', 'active');
  log(`\n\x1b[36m━━━ Step 4/6 — Copy to app dir ━━━\x1b[0m`);
  if (fs.existsSync(appDir)) fs.rmSync(appDir, { recursive: true, force: true });
  fs.mkdirSync(appDir, { recursive: true });
  copyDir(projectRoot, appDir);
  emitStep(emit, 'copy', 'done');
  log(`\x1b[32m[copy] ✓ App files ready at ${appDir}\x1b[0m`);

  // ── Step 5: Run in isolated Docker container ───────────────────────────────
  emitStep(emit, 'start', 'active');
  log(`\n\x1b[36m━━━ Step 5/6 — Launch Container ━━━\x1b[0m`);
  log(`\x1b[90m[docker] Image:     ${nodeImage}\x1b[0m`);
  log(`\x1b[90m[docker] Container: ${containerName}\x1b[0m`);
  log(`\x1b[90m[docker] Command:   ${startCmd}\x1b[0m`);

  // Stop and remove any previous container for this app
  try {
    await exec('docker', ['rm', '-f', containerName], {}, () => {});
  } catch(e) {}

  const hostAppDir = appDir.replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data');

  // KEY: No -p port mapping needed.
  // We connect via Docker internal network IP directly.
  // This means the app can listen on ANY port (3000, 8080, etc.)
  // and it will work regardless of what PORT env var it uses.
  const dockerArgs = [
    'run', '-d',
    '--name',    containerName,
    '--restart', 'unless-stopped',
    '--network', 'deployboard_deployboard-net', // same network as DeployBoard
    '-e',        `PORT=${appPort}`,
    '-e',        `NODE_ENV=production`,
    '-v',        `${hostAppDir}:/app`,
    '-w',        '/app',
  ];

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

  dockerArgs.push(nodeImage, 'sh', '-c', startCmd);
  await exec('docker', dockerArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);

  // Wait for container to start and get its IP
  log(`\x1b[90m[docker] Waiting for app to start…\x1b[0m`);
  await new Promise(r => setTimeout(r, 3000));

  // Get container's internal IP (retry a few times in case container is still initialising)
  let containerIP = '';
  for (let attempt = 1; attempt <= 5; attempt++) {
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
  if (containerIP) {
    log(`\x1b[90m[docker] Container IP: ${containerIP}\x1b[0m`);
  } else {
    log(`\x1b[31m[docker] Could not get container IP after 5 attempts\x1b[0m`);
  }

  // Poll for app to start listening — retry for up to 60 seconds
  // Checks the ports the app is most likely to use. Most Node.js apps respect
  // the PORT env var we set, but some frameworks hardcode 3000/8080.
  const portsToCheck = [appPort, 3000, 8080, 8000, 5000, 4000, 3001].filter((p, i, a) => p && a.indexOf(p) === i);
  let actualPort = 0;

  if (containerIP) {
    log(`\x1b[90m[docker] Polling for open port (up to 60s)…\x1b[0m`);
    const deadline = Date.now() + 60000;
    outer:
    while (Date.now() < deadline) {
      for (const p of portsToCheck) {
        const open = await checkTcpPort(containerIP, p, 1500);
        if (open) { actualPort = p; break outer; }
      }
      await new Promise(r => setTimeout(r, 3000));
    }
    if (actualPort) {
      log(`\x1b[32m[docker] ✓ App is listening on port ${actualPort}\x1b[0m`);
    }
  }

  if (!actualPort) {
    // App not responding — show logs then fail the deployment clearly
    log(`\x1b[31m[docker] ✗ App did not open any port within 60s — showing container logs:\x1b[0m`);
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    try { await exec('docker', ['logs', '--tail', '60', containerName], {}, log); } catch(e) {}
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    log(`\x1b[31m[hint] Make sure your app calls server.listen(process.env.PORT || 3000)\x1b[0m`);
    log(`\x1b[31m[hint] If your start command is wrong, check it in project settings.\x1b[0m`);
    throw new Error(`Container "${containerName}" started but app never opened a port. Check logs above.`);
  } else {
    // Save containerIP:actualPort to ports registry so server.js proxies correctly
    try {
      const sitesDir = appDir.replace(`/${project.subdomain}/app`, '');
      const pFile    = path.join(sitesDir, 'ports.json');
      let registry   = {};
      try { registry = JSON.parse(fs.readFileSync(pFile, 'utf8')); } catch(e) {}
      // Store as "ip:port" string so server.js knows the full address
      registry[project.subdomain] = `${containerIP}:${actualPort}`;
      fs.writeFileSync(pFile, JSON.stringify(registry, null, 2));
      log(`\x1b[32m[docker] ✓ Proxy registered: ${project.subdomain} → ${containerIP}:${actualPort}\x1b[0m`);
    } catch(e) {
      log(`\x1b[33m[docker] Could not update port registry: ${e.message}\x1b[0m`);
    }
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
  if (githubToken) cloneUrl = cloneUrl.replace(/^https:\/\//, `https://${githubToken}@`);
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
        try { return fs.statSync(path.join(dir, e)).isDirectory(); } catch(e) { return false; }
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

/** Check if a TCP port is open on a specific host (works across Docker network) */
function checkTcpPort(host, port, timeoutMs) {
  return new Promise(resolve => {
    const net    = require('net');
    const socket = new net.Socket();
    const timer  = setTimeout(() => { socket.destroy(); resolve(false); }, timeoutMs);
    socket.connect(port, host, () => { clearTimeout(timer); socket.destroy(); resolve(true); });
    socket.on('error', () => { clearTimeout(timer); resolve(false); });
  });
}

/** Wait for a TCP port on a specific host to open (polls checkTcpPort).
 *  Works correctly across Docker networks — /proc/net/tcp only shows the
 *  host's own ports, so we use an actual TCP connect instead. */
function waitForPort(host, port, timeoutMs) {
  return new Promise(async resolve => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (await checkTcpPort(host, port, 1500)) return resolve(true);
      await new Promise(r => setTimeout(r, 2000));
    }
    resolve(false);
  });
}

function exec(cmd, args, options, logFn) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      shell: false,
      cwd:   options.cwd,
      env:   options.env || process.env
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
    if (fs.lstatSync(s).isDirectory()) copyDir(s, d);
    else fs.copyFileSync(s, d);
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
