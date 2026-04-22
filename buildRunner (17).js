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
async function runBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog }) {
  const isServerApp = (project.siteType === 'server') || !!(project.startCmd || '').trim();
  return isServerApp
    ? runServerBuild({ deployId, project, sitesDir, tmpDir, githubToken, appPort, emit, onLog })
    : runStaticBuild({ deployId, project, sitesDir, tmpDir, githubToken, emit, onLog });
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
  log(`\x1b[90m[docker] Port:      ${appPort}\x1b[0m`);
  log(`\x1b[90m[docker] Command:   ${startCmd}\x1b[0m`);

  // Stop and remove any previous container for this app
  try {
    await exec('docker', ['rm', '-f', containerName], {}, () => {});
    log(`\x1b[90m[docker] Removed previous container\x1b[0m`);
  } catch(e) {}

  // Build docker run args
  // KEY: PORT env var is set to appPort so app knows what port to listen on
  // Use actual Docker volume host path — sibling containers need host path not container path
  const hostAppDir = appDir.replace('/var/www/user-sites', '/var/lib/docker/volumes/deployboard_sites-data/_data');

  const dockerArgs = [
    'run', '-d',
    '--name',    containerName,
    '--restart', 'unless-stopped',
    '-p',        `${appPort}:${appPort}`,
    '-e',        `PORT=${appPort}`,
    '-e',        `NODE_ENV=production`,
    '-v',        `${hostAppDir}:/app`,
    '-w',        '/app',
  ];

  // Add user-defined env vars
  for (const [k, v] of Object.entries(env)) {
    dockerArgs.push('-e', `${k}=${v}`);
  }

  // Pull the Node image first so the user sees progress
  log(`\x1b[90m[docker] Pulling ${nodeImage}…\x1b[0m`);
  try {
    await exec('docker', ['pull', nodeImage], {}, log);
  } catch(e) {
    log(`\x1b[33m[docker] Pull failed (will try cached): ${e.message}\x1b[0m`);
  }

  // Add image and command
  dockerArgs.push(nodeImage, 'sh', '-c', startCmd);

  await exec('docker', dockerArgs, {}, log);
  log(`\x1b[32m[docker] ✓ Container started\x1b[0m`);

  // Wait for app to boot
  log(`\x1b[90m[docker] Waiting for app to bind to port ${appPort}…\x1b[0m`);
  const ready = await waitForPort(appPort, 30000);
  if (ready) {
    log(`\x1b[32m[docker] ✓ App is live on port ${appPort}\x1b[0m`);
  } else {
    // Show container logs so user can see crash reason right in build output
    log(`\x1b[33m[docker] Port ${appPort} not open after 30s — showing container logs:\x1b[0m`);
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    try {
      await exec('docker', ['logs', '--tail', '40', containerName], {}, log);
    } catch(e) {}
    log(`\x1b[33m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\x1b[0m`);
    log(`\x1b[33m[hint] If app ignores PORT env var, it's hardcoding a port.\x1b[0m`);
    log(`\x1b[33m[hint] Fix: const PORT = process.env.PORT || 3000; app.listen(PORT)\x1b[0m`);
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
  if (fs.existsSync(path.join(buildDir, 'package.json'))) return buildDir;
  const entries = fs.readdirSync(buildDir);
  for (const entry of entries) {
    const sub = path.join(buildDir, entry);
    if (fs.statSync(sub).isDirectory() && fs.existsSync(path.join(sub, 'package.json'))) {
      log(`\x1b[90m[info] Found package.json in: ${entry}/\x1b[0m`);
      return sub;
    }
  }
  return buildDir;
}

function resolveEnvVars(evars) {
  if (!evars) return {};
  if (typeof evars.toObject === 'function') return evars.toObject();
  if (evars instanceof Map) return Object.fromEntries(evars);
  return evars;
}

/** Wait for a port to open by polling /proc/net/tcp */
function waitForPort(port, timeoutMs) {
  return new Promise(resolve => {
    const start    = Date.now();
    const portHex  = port.toString(16).toUpperCase().padStart(4, '0');
    const interval = setInterval(() => {
      try {
        for (const f of ['/proc/net/tcp', '/proc/net/tcp6']) {
          try {
            const lines = fs.readFileSync(f, 'utf8').split('\n');
            for (const line of lines) {
              const parts = line.trim().split(/\s+/);
              if (parts[3] === '0A') {
                const p = (parts[1] || '').split(':').pop();
                if (p && parseInt(p, 16) === port) {
                  clearInterval(interval);
                  return resolve(true);
                }
              }
            }
          } catch(e) {}
        }
      } catch(e) {}
      if (Date.now() - start > timeoutMs) {
        clearInterval(interval);
        resolve(false);
      }
    }, 1000);
  });
}

function exec(cmd, args, options, logFn) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      shell: false,
      cwd:   options.cwd,
      env:   options.env || process.env
    });

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
