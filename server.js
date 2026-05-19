import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import fs from 'fs/promises';
import path from 'path';
import { exec } from 'child_process';
import { promisify } from 'util';
import { Client as PgClient } from 'pg';
import mysql from 'mysql2/promise';
import { MongoClient } from 'mongodb';
import Redis from 'ioredis';

const run = promisify(exec);
dotenv.config();
const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static('.'));

const registry = new Map();
const schemas = new Map();
const flows = new Map();

const engines = {
  postgres: { image: 'postgres:alpine', port: 5432 },
  mysql: { image: 'mysql:8.0', port: 3306 },
  mongo: { image: 'mongo:latest', port: 27017 },
  redis: { image: 'redis:alpine', port: 6379 },
  excel: { local: true },
  access: { local: true }
};

const localRoot = path.join(process.cwd(), 'data', 'local_storage');

const sh = (cmd) => run(cmd, { maxBuffer: 1024 * 1024 * 4 });

async function ensureDirs() {
  await fs.mkdir(path.join(localRoot, 'excel'), { recursive: true });
  await fs.mkdir(path.join(localRoot, 'access'), { recursive: true });
}

async function provisionContainer({ name, engine, username, password }) {
  const def = engines[engine];
  const randomPort = Math.floor(Math.random() * 10000) + 20000;
  const container = `logiflow_${engine}_${name}_${Date.now()}`.replace(/[^a-zA-Z0-9_]/g, '');
  let env = '';
  if (engine === 'postgres') env = `-e POSTGRES_DB=${name} -e POSTGRES_USER=${username} -e POSTGRES_PASSWORD=${password}`;
  if (engine === 'mysql') env = `-e MYSQL_DATABASE=${name} -e MYSQL_USER=${username} -e MYSQL_PASSWORD=${password} -e MYSQL_ROOT_PASSWORD=${password}`;
  if (engine === 'mongo') env = `-e MONGO_INITDB_ROOT_USERNAME=${username} -e MONGO_INITDB_ROOT_PASSWORD=${password}`;
  await sh(`docker run -d --name ${container} -p ${randomPort}:${def.port} ${env} ${def.image}`);
  return { container, port: randomPort };
}

app.get('/api/health', (_req, res) => res.json({ ok: true }));

app.get('/api/infrastructure', (_req, res) => res.json([...registry.values()]));

app.post('/api/infrastructure', async (req, res) => {
  try {
    const { name, engine, username, password } = req.body;
    if (!name || !engine) return res.status(400).json({ error: 'name and engine required' });
    if (!engines[engine]) return res.status(400).json({ error: 'unsupported engine' });
    const id = `${engine}_${name}`;

    if (engines[engine].local) {
      const dir = path.join(localRoot, engine, name);
      await fs.mkdir(dir, { recursive: true });
      if (engine === 'excel') await fs.writeFile(path.join(dir, 'sheet.csv'), 'id,data\n', { flag: 'a' });
      if (engine === 'access') await fs.writeFile(path.join(dir, 'database.json'), JSON.stringify({ tables: {} }, null, 2), { flag: 'a' });
      registry.set(id, { id, name, engine, localPath: dir, createdAt: new Date().toISOString() });
      return res.status(201).json(registry.get(id));
    }

    const containerInfo = await provisionContainer({ name, engine, username: username || 'admin', password: password || 'admin123' });
    const record = { id, name, engine, username, password, host: '127.0.0.1', ...containerInfo, createdAt: new Date().toISOString() };
    registry.set(id, record);
    res.status(201).json(record);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/schema/:dbId', (req, res) => {
  schemas.set(req.params.dbId, req.body);
  res.json({ ok: true });
});

app.post('/api/query/:dbId', async (req, res) => {
  const db = registry.get(req.params.dbId);
  if (!db) return res.status(404).json({ error: 'database not found' });
  const { query, values = [] } = req.body;
  try {
    if (db.engine === 'postgres') {
      const client = new PgClient({ host: db.host, port: db.port, database: db.name, user: db.username, password: db.password });
      await client.connect();
      const result = await client.query(query, values);
      await client.end();
      return res.json(result.rows);
    }
    if (db.engine === 'mysql') {
      const conn = await mysql.createConnection({ host: db.host, port: db.port, database: db.name, user: db.username, password: db.password });
      const [rows] = await conn.query(query, values);
      await conn.end();
      return res.json(rows);
    }
    if (db.engine === 'mongo') {
      const client = new MongoClient(`mongodb://${db.username}:${db.password}@${db.host}:${db.port}`);
      await client.connect();
      const body = typeof query === 'string' ? JSON.parse(query) : query;
      const result = await client.db(db.name).collection(body.collection).find(body.filter || {}).limit(body.limit || 100).toArray();
      await client.close();
      return res.json(result);
    }
    if (db.engine === 'redis') {
      const redis = new Redis(db.port, db.host);
      const [op, key, value] = String(query).split(' ');
      const result = op.toUpperCase() === 'SET' ? await redis.set(key, value) : await redis.get(key);
      redis.disconnect();
      return res.json({ result });
    }
    if (db.engine === 'excel') {
      const file = path.join(db.localPath, 'sheet.csv');
      if (query === 'READ') return res.type('text/plain').send(await fs.readFile(file, 'utf8'));
      await fs.appendFile(file, `${Date.now()},${JSON.stringify(values[0] || {})}\n`);
      return res.json({ ok: true });
    }
    if (db.engine === 'access') {
      const file = path.join(db.localPath, 'database.json');
      const data = JSON.parse(await fs.readFile(file, 'utf8') || '{"tables":{}}');
      if (query?.action === 'insert') {
        data.tables[query.table] = data.tables[query.table] || [];
        data.tables[query.table].push(query.row);
      }
      await fs.writeFile(file, JSON.stringify(data, null, 2));
      return res.json(data);
    }
    return res.status(400).json({ error: 'unsupported engine' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/flows/:flowId', (req, res) => {
  flows.set(req.params.flowId, req.body.nodes || []);
  res.json({ ok: true });
});

app.all('/api/simulated/:flowId/*', async (req, res) => {
  const flow = flows.get(req.params.flowId);
  if (!flow) return res.status(404).json({ error: 'flow not found' });
  const runtimeCtx = { req: { body: req.body, query: req.query, params: req.params, headers: req.headers, ip: req.ip }, db_result: {}, vars: {} };
  const visited = new Set();
  let idx = 0;
  while (idx < flow.length) {
    if (visited.has(idx)) return res.status(508).json({ error: 'Loop Detected' });
    visited.add(idx);
    const node = flow[idx];
    if (node.type === 'TRANSFORM_DATA') runtimeCtx.vars[node.key] = node.value;
    if (node.type === 'HTTP_RESPONSE') return res.status(node.status || 200).json({ runtimeCtx, message: node.message || 'ok' });
    idx += 1;
  }
  res.json(runtimeCtx);
});

setInterval(async () => {
  for (const db of registry.values()) {
    if (!db.container) continue;
    try {
      const { stdout } = await sh(`docker inspect -f '{{.State.Running}}' ${db.container}`);
      if (!stdout.includes('true')) {
        const restored = await provisionContainer({ name: db.name, engine: db.engine, username: db.username || 'admin', password: db.password || 'admin123' });
        registry.set(db.id, { ...db, ...restored, recoveredAt: new Date().toISOString() });
      }
    } catch (_error) {}
  }
}, 20000);

await ensureDirs();
app.listen(process.env.PORT || 3000, '0.0.0.0', () => console.log('LogiFlow running'));
