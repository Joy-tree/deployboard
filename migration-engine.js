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
const crypto = require('crypto');

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

// Many managed Postgres providers (Supabase, Neon, Render, Heroku, etc.)
// require SSL, but the connection string you copy from their dashboard
// doesn't always include `sslmode=require` -- node-postgres defaults to no
// SSL unless told otherwise, so a plain paste of those strings can fail to
// connect at all. Auto-enable SSL unless the string explicitly opts out
// (sslmode=disable) or already specifies its own ssl mode. rejectUnauthorized
// is left off (accepting the provider's cert without strict CA validation)
// since that's the common pragmatic default for this kind of one-off
// migration tool -- these are managed providers' own valid certs, not
// self-signed ones from an untrusted source.
function pgPoolOptions(connectionString) {
  const opts = { connectionString, connectionTimeoutMillis: 8000 };
  const explicitlyDisabled = /[?&]sslmode=disable\b/i.test(connectionString);
  const alreadySpecifiesSsl = /[?&]sslmode=/i.test(connectionString) || /[?&]ssl=/i.test(connectionString);
  if (!explicitlyDisabled && !alreadySpecifiesSsl) {
    opts.ssl = { rejectUnauthorized: false };
  }
  return opts;
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

// [FIX] Extracts the database name from a Mongo connection string without
// using the WHATWG URL parser -- that throws outright on replica-set style
// strings with multiple comma-separated hosts
// (mongodb://host1:27017,host2:27017,host3:27017/db?replicaSet=rs0), which
// are completely valid and common for self-hosted/non-Atlas clusters.
// Anchoring on the last '@' (auth/host separator -- a literal '@' in a
// password must be percent-encoded per the connection string spec, so the
// last one always marks the real boundary) and the first '/' after that
// handles both srv and standard, single-host and multi-host forms.
function extractMongoDbName(connectionString) {
  const afterScheme = String(connectionString).replace(/^mongodb(\+srv)?:\/\//, '');
  const afterAuth = afterScheme.includes('@') ? afterScheme.slice(afterScheme.lastIndexOf('@') + 1) : afterScheme;
  if (!afterAuth.includes('/')) return '';
  const pathAndQuery = afterAuth.slice(afterAuth.indexOf('/') + 1);
  return pathAndQuery.split('?')[0].trim();
}

async function readFromMongo(connectionString) {
  // [FIX] client.db() with no name uses whatever database is in the
  // connection string's path -- but if there isn't one (very common: the
  // default "Copy connection string" button in Atlas gives you
  // mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
  // with NO db name before the ?), the driver silently falls back to a
  // database literally named "test". That means a migration could run
  // "successfully" against the wrong database with no error at all.
  // Checked before connecting so it fails fast/clearly rather than only
  // after establishing a network connection.
  if (!extractMongoDbName(connectionString)) {
    throw new Error(
      'Your MongoDB connection string doesn\'t include a database name (the part after the last "/" before any "?"). ' +
      'Atlas\'s default "Copy connection string" button omits it -- add it explicitly, ' +
      'e.g. mongodb+srv://user:pass@cluster.mongodb.net/YOUR_DB_NAME?retryWrites=true, ' +
      'otherwise MongoDB silently defaults to a database named "test".'
    );
  }

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
  // that collection's documents.
  const base = databaseUrl.replace(/\/$/, '');
  const qs = authSecret ? `?auth=${encodeURIComponent(authSecret)}` : '';
  const tree = await fetchJson(`${base}/.json${qs}`);
  if (!tree || typeof tree !== 'object') return [];

  // [FIX] A single child-to-row conversion used for both object-map and
  // array-style RTDB nodes below. Object children get their fields spread
  // into the row (unchanged prior behavior); scalar or array children
  // become a row shaped { _id, value } instead of being silently collapsed.
  const childToRow = (id, child) =>
    (child && typeof child === 'object' && !Array.isArray(child)) ? { _id: id, ...child } : { _id: id, value: child };

  const collections = [];
  for (const [topKey, value] of Object.entries(tree)) {
    if (Array.isArray(value) && value.length) {
      // [FIX] Previously fell through to the scalar/array branch below and
      // got collapsed into ONE row wrapping the entire array -- a common
      // RTDB shape (ordered lists of children) lost its per-element
      // structure entirely. Firebase arrays can contain holes (sparse
      // arrays from deleted indices come back as `null`), so those are
      // skipped rather than turned into meaningless empty rows.
      const rows = value.map((child, i) => (child === null ? null : childToRow(i, child))).filter(Boolean);
      if (rows.length) { collections.push({ name: topKey, rows }); continue; }
    }
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const childKeys = Object.keys(value);
      if (childKeys.length) {
        // [FIX] Previously only treated this as a "collection of documents"
        // when EVERY child was itself an object. A very common RTDB shape --
        // a flat map of scalar values, e.g. counters: { user1: 5, user2: 10 }
        // -- failed that check and got collapsed into a single row for the
        // whole node, silently losing the per-child structure. Now handles
        // children of any kind (object, scalar, or a mix) uniformly, one
        // row per child either way.
        collections.push({ name: topKey, rows: childKeys.map(k => childToRow(k, value[k])) });
        continue;
      }
    }
    // Scalar, empty object, or empty array at the top level -- still
    // capture it as a single-row collection rather than dropping it.
    collections.push({ name: topKey, rows: [{ _id: topKey, value }] });
  }
  return collections;
}

async function readFromSql(engine, connectionString) {
  if (engine === 'postgres') {
    const pool = new Pool(pgPoolOptions(connectionString));
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
      // [FIX] Was `if (!col.rows.length) continue` -- an empty source
      // collection was skipped entirely, so it never even existed in the
      // destination. A migration should move everything it found, not just
      // the collections that happened to have documents at that moment --
      // otherwise the destination's schema is silently incomplete and
      // there's no way to tell "this collection doesn't exist" apart from
      // "this collection was never migrated". insertMany can't create an
      // empty collection (nothing to insert), so create it explicitly.
      if (!col.rows.length) {
        const exists = await db.listCollections({ name: col.name }).hasNext();
        if (!exists) await db.createCollection(col.name).catch(e => log(`  ! ${col.name}: could not create empty collection: ${e.message}`));
        log(`  -> ${col.name}: 0 documents (source collection was empty)`);
        continue;
      }
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
  const pool = isPg ? new Pool(pgPoolOptions(connectionString)) : null;
  const conn = isPg ? null : await mysql.createConnection(connectionString);
  const run = async (sql, params) => isPg ? pool.query(sql, params) : conn.query(sql, params);
  const PK_COL = '_jt_row_id'; // namespaced so it doesn't collide with real source data, including data this same engine wrote on a previous hop

  try {
    for (const col of collections) {
      // [FIX] Was `if (!col.rows.length) continue` -- an empty source
      // collection/table never got created in the destination at all, with
      // no log line explaining why. Same reasoning as the writeToMongo fix
      // above: a migration should account for everything it found. There's
      // no data to infer column types from, so an empty table gets created
      // with just the PK column -- schema-complete once real data starts
      // flowing in, rather than silently nonexistent.
      if (!col.rows.length) {
        const table = sanitizeIdent(col.name);
        if (isPg) {
          await run(`CREATE TABLE IF NOT EXISTS "${table}" (${PK_COL} SERIAL PRIMARY KEY)`);
        } else {
          await run(`CREATE TABLE IF NOT EXISTS \`${table}\` (${PK_COL} INT AUTO_INCREMENT PRIMARY KEY)`);
        }
        log(`  -> ${table}: 0 rows (source collection was empty; created with no columns beyond the primary key)`);
        continue;
      }
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

// ── Cross-engine database diff ──────────────────────────────────────────────
// Compares two databases and reports exactly what's different, collection by
// collection, row by row -- even when the two sides are completely different
// engines (a MongoDB collection vs. a Postgres table vs. a Redis keyspace).
// This works at all because readSource() already normalizes every engine
// down to the same shape ({ name, rows: [...] }); the diff logic below never
// needs to know which engine either side actually is.
//
// No mainstream database offers this: comparing two live databases for
// exactly what changed is something people currently do per-engine at best
// (e.g. Postgres-to-Postgres schema/data diff tools), and never across
// fundamentally different data models. Because JoyTree already treats
// "a database" as an engine-agnostic concept (see the migration engine
// above), this is a natural, low-risk extension rather than new
// infrastructure -- and it's something no other database platform can offer
// today, since none of them manage multiple heterogeneous engines under one
// roof the way JoyTree does.

// Deterministic stringify: object keys are sorted recursively so two
// objects with the same data in a different key order still compare equal.
function stableStringify(value) {
  if (value === null || value === undefined) return 'null';
  if (typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(stableStringify).join(',') + ']';
  const keys = Object.keys(value).sort();
  return '{' + keys.map(k => JSON.stringify(k) + ':' + stableStringify(value[k])).join(',') + '}';
}

function stableHash(value) {
  return crypto.createHash('sha1').update(stableStringify(value)).digest('hex');
}

// Rows are matched across the two sides by identity, not position -- so
// reordered/reinserted rows still match correctly. Tries common id-like
// fields first (works across engines: Mongo's _id, a SQL primary key
// commonly named id, JoyTree's own _jt_row_id namespaced PK); falls back to
// a content hash of the whole row so even schemaless/keyless data (e.g. a
// Redis value with no id concept at all) can still be matched by what it
// actually contains rather than being reported as 100% added+removed.
const ROW_ID_FIELDS = ['_id', 'id', 'ID', '_jt_row_id', 'uuid', 'key'];
function rowIdentity(row) {
  if (row && typeof row === 'object') {
    for (const f of ROW_ID_FIELDS) {
      if (row[f] !== undefined && row[f] !== null && row[f] !== '') return `${f}:${String(row[f])}`;
    }
  }
  return `hash:${stableHash(row)}`;
}

// Field-level diff between two rows that matched (same identity). Skips the
// field that was actually used to match them (comparing it again is always
// a no-op) plus a couple of engine-internal bookkeeping fields that aren't
// meaningful to show as "changed" data.
const DIFF_IGNORE_FIELDS = new Set(['_jt_row_id']);
function diffFields(rowA, rowB) {
  const a = rowA && typeof rowA === 'object' ? rowA : {};
  const b = rowB && typeof rowB === 'object' ? rowB : {};
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  const changes = [];
  for (const key of keys) {
    if (DIFF_IGNORE_FIELDS.has(key)) continue;
    const va = a[key], vb = b[key];
    if (stableStringify(va) !== stableStringify(vb)) {
      changes.push({ field: key, before: va === undefined ? null : va, after: vb === undefined ? null : vb });
    }
  }
  return changes;
}

/**
 * Compares two databases (each described the same way a migration source is:
 * { kind: 'joytree'|'mongo'|'firebase'|'sql'|'redis', ... }, already resolved
 * to real connection info) and returns a structured report of exactly what
 * differs between them, collection by collection.
 *
 * `sampleLimit` caps how many example rows are returned per added/removed/
 * changed bucket in each collection (counts are always exact; this only
 * bounds how much example data comes back in the response).
 */
async function diffDatabases(sourceA, sourceB, { sampleLimit = 200 } = {}) {
  const [collectionsA, collectionsB] = await Promise.all([readSource(sourceA), readSource(sourceB)]);
  const byNameA = new Map(collectionsA.map(c => [c.name, c]));
  const byNameB = new Map(collectionsB.map(c => [c.name, c]));
  const allNames = [...new Set([...byNameA.keys(), ...byNameB.keys()])].sort();

  const report = {
    collectionsOnlyInA: [],
    collectionsOnlyInB: [],
    collections: [],
    summary: { collectionsCompared: 0, rowsAdded: 0, rowsRemoved: 0, rowsChanged: 0, rowsUnchanged: 0 },
  };

  for (const name of allNames) {
    const colA = byNameA.get(name);
    const colB = byNameB.get(name);
    if (!colB) { report.collectionsOnlyInA.push(name); continue; }
    if (!colA) { report.collectionsOnlyInB.push(name); continue; }

    const mapA = new Map(colA.rows.map(r => [rowIdentity(r), r]));
    const mapB = new Map(colB.rows.map(r => [rowIdentity(r), r]));

    const added = [], removed = [], changed = [];
    let unchanged = 0;

    for (const [key, rowA] of mapA) {
      if (!mapB.has(key)) { removed.push(rowA); continue; }
      const rowB = mapB.get(key);
      const fieldChanges = diffFields(rowA, rowB);
      if (fieldChanges.length) changed.push({ identity: key, before: rowA, after: rowB, fieldChanges });
      else unchanged++;
    }
    for (const [key, rowB] of mapB) {
      if (!mapA.has(key)) added.push(rowB);
    }

    report.collections.push({
      name,
      addedCount: added.length, removedCount: removed.length, changedCount: changed.length, unchangedCount: unchanged,
      added: added.slice(0, sampleLimit),
      removed: removed.slice(0, sampleLimit),
      changed: changed.slice(0, sampleLimit),
    });
    report.summary.collectionsCompared++;
    report.summary.rowsAdded += added.length;
    report.summary.rowsRemoved += removed.length;
    report.summary.rowsChanged += changed.length;
    report.summary.rowsUnchanged += unchanged;
  }

  return report;
}

module.exports = { runMigration, readSource, writeDestination, diffDatabases };
