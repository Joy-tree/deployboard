'use strict';

/**
 * Database migration engine.
 *
 * Moves data between databases regardless of engine, including across
 * fundamentally different data models (document <-> relational <-> key/value)
 * and from external sources (a real MongoDB Atlas cluster, a Firebase
 * Realtime Database) into any JoyTree-provisioned database.
 *
 * Everything is normalized to the same shape internally: a "collection" is
 * just a name plus a list of plain JS objects (documents/rows). Reading
 * always produces this shape; writing always consumes it. That's what makes
 * cross-engine migration possible without a combinatorial explosion of
 * engine-to-engine converters -- there's one reader per source type and one
 * writer per destination type, and any reader can feed any writer.
 */

const { Pool } = require('pg');
const mysql = require('mysql2/promise');
const { createClient: createRedisClient } = require('redis');
const { MongoClient } = require('mongodb');
const https = require('https');
const http = require('http');
const { URL } = require('url');

// ── Helpers ──────────────────────────────────────────────────────────────

function inferSqlType(values) {
  // Look at a sample of real values for a column and pick a reasonable SQL
  // type. Falls back to TEXT for anything mixed/complex -- nested
  // objects/arrays get JSON-stringified and stored as TEXT rather than
  // dropped, so no data is ever silently lost even when the destination
  // can't natively represent the shape.
  let sawNumber = false, sawInt = true, sawBool = false, sawOther = false;
  for (const v of values) {
    if (v === null || v === undefined) continue;
    if (typeof v === 'number') { sawNumber = true; if (!Number.isInteger(v)) sawInt = false; }
    else if (typeof v === 'boolean') sawBool = true;
    else if (typeof v === 'string') { /* fine, text */ }
    else sawOther = true;
  }
  if (sawOther) return 'TEXT';
  if (sawBool && !sawNumber) return 'BOOLEAN';
  if (sawNumber) return sawInt ? 'BIGINT' : 'DOUBLE PRECISION';
  return 'TEXT';
}

function sanitizeIdent(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/^[^a-z_]/, '_').slice(0, 63) || 'col';
}

function serializeForSql(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'object') return JSON.stringify(v);
  return v;
}

function fetchJson(urlStr) {
  return new Promise((resolve, reject) => {
    const u = new URL(urlStr);
    const lib = u.protocol === 'http:' ? http : https;
    lib.get(u, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode >= 400) return reject(new Error(`HTTP ${res.statusCode} fetching ${u.hostname}`));
        try { resolve(JSON.parse(data)); } catch (e) { reject(new Error('Response was not valid JSON: ' + e.message)); }
      });
    }).on('error', reject);
  });
}

// ── Readers: source -> { collections: [{ name, rows: [obj,...] }] } ───────

async function readFromMongo(connectionString) {
  const client = new MongoClient(connectionString, { serverSelectionTimeoutMS: 8000 });
  await client.connect();
  try {
    const db = client.db(); // uses the db named in the connection string
    const collInfos = await db.listCollections().toArray();
    const collections = [];
    for (const info of collInfos) {
      const docs = await db.collection(info.name).find({}).limit(50000).toArray();
      collections.push({
        name: info.name,
        rows: docs.map(d => JSON.parse(JSON.stringify(d))), // drop ObjectId/Date wrappers to plain JSON
      });
    }
    return collections;
  } finally {
    await client.close().catch(() => {});
  }
}

async function readFromFirebaseRtdb(databaseUrl, authSecret) {
  // Firebase RTDB's REST API returns the entire tree as one JSON document
  // at <databaseUrl>/.json -- no SDK needed, just a plain HTTPS GET. Each
  // top-level key becomes a "collection"; its immediate children become
  // that collection's documents (or, if the value at a key is a scalar
  // rather than an object of children, it's wrapped as a single-field row
  // so nothing is silently skipped).
  const base = databaseUrl.replace(/\/$/, '');
  const qs = authSecret ? `?auth=${encodeURIComponent(authSecret)}` : '';
  const tree = await fetchJson(`${base}/.json${qs}`);
  if (!tree || typeof tree !== 'object') return [];

  const collections = [];
  for (const [topKey, value] of Object.entries(tree)) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const childKeys = Object.keys(value);
      const looksLikeCollection = childKeys.every(k => value[k] && typeof value[k] === 'object');
      if (looksLikeCollection && childKeys.length) {
        collections.push({
          name: topKey,
          rows: childKeys.map(k => ({ _id: k, ...value[k] })),
        });
        continue;
      }
    }
    // Scalar or array at the top level -- still capture it as a
    // single-row collection rather than dropping it.
    collections.push({ name: topKey, rows: [{ _id: topKey, value }] });
  }
  return collections;
}

async function readFromSql(engine, connectionString) {
  if (engine === 'postgres') {
    const pool = new Pool({ connectionString, connectionTimeoutMillis: 8000 });
    try {
      const tablesRes = await pool.query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`);
      const collections = [];
      for (const t of tablesRes.rows) {
        const rowsRes = await pool.query(`SELECT * FROM "${t.tablename}" LIMIT 50000`);
        collections.push({ name: t.tablename, rows: rowsRes.rows });
      }
      return collections;
    } finally {
      await pool.end().catch(() => {});
    }
  }

  // mysql + mariadb share the same wire protocol / client
  const conn = await mysql.createConnection(connectionString);
  try {
    const [tables] = await conn.query('SHOW TABLES');
    const collections = [];
    for (const row of tables) {
      const tableName = Object.values(row)[0];
      const [rows] = await conn.query(`SELECT * FROM \`${tableName}\` LIMIT 50000`);
      collections.push({ name: tableName, rows });
    }
    return collections;
  } finally {
    await conn.end().catch(() => {});
  }
}

async function readFromRedis(connectionString) {
  const client = createRedisClient({ url: connectionString });
  await client.connect();
  try {
    const keys = await client.keys('*');
    const rows = [];
    for (const key of keys) {
      const type = await client.type(key);
      let value;
      if (type === 'string') {
        const raw = await client.get(key);
        try { value = JSON.parse(raw); } catch { value = raw; }
      } else if (type === 'hash') {
        value = await client.hGetAll(key);
      } else if (type === 'list') {
        value = await client.lRange(key, 0, -1);
      } else if (type === 'set') {
        value = await client.sMembers(key);
      } else {
        value = null;
      }
      rows.push({ _id: key, _redisType: type, value });
    }
    return [{ name: 'redis_keys', rows }];
  } finally {
    await client.quit().catch(() => {});
  }
}

async function readSource(source) {
  if (source.kind === 'mongo') return readFromMongo(source.connectionString);
  if (source.kind === 'firebase') return readFromFirebaseRtdb(source.databaseUrl, source.authSecret);
  if (source.kind === 'sql') return readFromSql(source.engine, source.connectionString);
  if (source.kind === 'redis') return readFromRedis(source.connectionString);
  throw new Error(`Unknown source kind: ${source.kind}`);
}

// ── Writers: { collections } -> destination ────────────────────────────────

async function writeToMongo(connectionString, collections, log) {
  const client = new MongoClient(connectionString, { serverSelectionTimeoutMS: 8000 });
  await client.connect();
  try {
    const db = client.db();
    for (const col of collections) {
      if (!col.rows.length) continue;
      const docs = col.rows.map(r => { const { _id, ...rest } = r; return rest; }); // let Mongo assign fresh _ids
      for (let i = 0; i < docs.length; i += 500) {
        await db.collection(col.name).insertMany(docs.slice(i, i + 500), { ordered: false }).catch(e => log(`  ! ${col.name}: ${e.message}`));
      }
      log(`  -> ${col.name}: ${docs.length} documents`);
    }
  } finally {
    await client.close().catch(() => {});
  }
}

async function writeToSql(engine, connectionString, collections, log) {
  const isPg = engine === 'postgres';
  const pool = isPg ? new Pool({ connectionString, connectionTimeoutMillis: 8000 }) : null;
  const conn = isPg ? null : await mysql.createConnection(connectionString);
  const run = async (sql, params) => isPg ? pool.query(sql, params) : conn.query(sql, params);
  const PK_COL = '_jt_row_id'; // namespaced so it doesn't collide with real source data, including data this same engine wrote on a previous hop

  try {
    for (const col of collections) {
      if (!col.rows.length) continue;
      const table = sanitizeIdent(col.name);
      const allKeys = new Set();
      col.rows.forEach(r => Object.keys(r).forEach(k => allKeys.add(k)));
      // Drop any source column that would collide with our own PK column name
      // (e.g. migrating a table that was itself previously written by this
      // same engine on an earlier hop) rather than crash on a duplicate.
      allKeys.delete(PK_COL);
      const columns = [...allKeys].map(sanitizeIdent);
      const uniqueColumns = [...new Set(columns)].filter(c => c !== PK_COL);

      const colDefs = uniqueColumns.map(c => {
        const values = col.rows.map(r => r[c]).filter(v => v !== undefined);
        const type = inferSqlType(values);
        return `${isPg ? `"${c}"` : `\`${c}\``} ${type}`;
      }).join(', ');

      if (isPg) {
        await run(`CREATE TABLE IF NOT EXISTS "${table}" (${PK_COL} SERIAL PRIMARY KEY${colDefs ? ', ' + colDefs : ''})`);
      } else {
        await run(`CREATE TABLE IF NOT EXISTS \`${table}\` (${PK_COL} INT AUTO_INCREMENT PRIMARY KEY${colDefs ? ', ' + colDefs : ''})`);
      }

      let inserted = 0;
      for (const row of col.rows) {
        const rawKeys = Object.keys(row).filter(k => k !== PK_COL);
        const cols = uniqueColumns.filter(c => row[rawKeys.find(k => sanitizeIdent(k) === c)] !== undefined);
        const values = cols.map(c => {
          const rawKey = rawKeys.find(k => sanitizeIdent(k) === c);
          return serializeForSql(row[rawKey]);
        });
        if (!cols.length) continue;
        const colList = cols.map(c => isPg ? `"${c}"` : `\`${c}\``).join(', ');
        const placeholders = isPg ? cols.map((_, i) => `$${i + 1}`).join(', ') : cols.map(() => '?').join(', ');
        try {
          await run(`INSERT INTO ${isPg ? `"${table}"` : `\`${table}\``} (${colList}) VALUES (${placeholders})`, values);
          inserted++;
        } catch (e) {
          log(`  ! ${table} row skipped: ${e.message}`);
        }
      }
      log(`  -> ${table}: ${inserted}/${col.rows.length} rows`);
    }
  } finally {
    if (pool) await pool.end().catch(() => {});
    if (conn) await conn.end().catch(() => {});
  }
}

async function writeToRedis(connectionString, collections, log) {
  const client = createRedisClient({ url: connectionString });
  await client.connect();
  try {
    for (const col of collections) {
      let count = 0;
      for (const row of col.rows) {
        const key = `${col.name}:${row._id || row.id || count}`;
        await client.set(key, JSON.stringify(row));
        count++;
      }
      log(`  -> ${col.name}: ${count} keys`);
    }
  } finally {
    await client.quit().catch(() => {});
  }
}

async function writeDestination(destination, collections, log) {
  if (destination.kind === 'mongo') return writeToMongo(destination.connectionString, collections, log);
  if (destination.kind === 'sql') return writeToSql(destination.engine, destination.connectionString, collections, log);
  if (destination.kind === 'redis') return writeToRedis(destination.connectionString, collections, log);
  throw new Error(`Unknown destination kind: ${destination.kind}`);
}

/**
 * Runs a full migration: read everything from source, write it all to
 * destination, return a summary. `onLog(line)` is called for progress
 * lines the caller can stream/store as it goes.
 */
async function runMigration(source, destination, onLog) {
  const log = (line) => { try { onLog(line); } catch (_) {} };
  log(`Connecting to source (${source.kind})...`);
  const collections = await readSource(source);
  log(`Read ${collections.length} collection(s): ${collections.map(c => `${c.name} (${c.rows.length})`).join(', ') || 'none'}`);

  log(`Writing to destination (${destination.kind}/${destination.engine || ''})...`);
  await writeDestination(destination, collections, log);

  const totalRows = collections.reduce((sum, c) => sum + c.rows.length, 0);
  log(`Done. ${collections.length} collection(s), ${totalRows} row(s)/document(s) total.`);
  return { collections: collections.length, rows: totalRows };
}

module.exports = { runMigration, readSource, writeDestination };
