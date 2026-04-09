import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

// ========== CORS ==========
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');
}

// ========== Main handler with full error capture ==========
export default async function handler(req, res) {
  setCors(res);

  // OPTIONS preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Authentication
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized: invalid API key' });
  }

  try {
    // ----- 1. Initialize SQL.js (with safe error) -----
    let SQL;
    try {
      SQL = await initSqlJs({
        locateFile: file => `https://sql.js.org/dist/${file}`
      });
    } catch (err) {
      console.error('SQL.js init failed', err);
      return res.status(500).json({ error: 'Failed to load SQL.js: ' + err.message });
    }

    // ----- 2. Load database (from Redis or file) -----
    let database;
    let persisted = null;

    // Try Redis first (graceful if Redis not configured)
    let redis = null;
    try {
      redis = Redis.fromEnv();
      const buf = await redis.get('sqlite_db');
      if (buf) {
        persisted = new Uint8Array(buf);
        console.log('Loaded from Redis');
      }
    } catch (e) {
      console.log('Redis not available or empty: ' + e.message);
    }

    // If no Redis, load northwind.db from repo
    if (!persisted) {
      try {
        const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
        console.log('Looking for DB at:', dbPath);
        const fileBuffer = fs.readFileSync(dbPath);
        persisted = new Uint8Array(fileBuffer);
        console.log('Loaded northwind.db from repo');
      } catch (err) {
        console.error('Failed to load northwind.db:', err.message);
        // Fallback: empty in‑memory database
        persisted = null;
      }
    }

    // Create database instance
    database = new SQL.Database(persisted);
    if (!persisted) {
      console.log('Created empty in‑memory database (no seed file)');
    }

    // Persist to Redis for next time (if Redis exists and we have a database)
    if (redis && database) {
      try {
        const data = database.export();
        await redis.set('sqlite_db', Buffer.from(data));
      } catch (e) {
        console.log('Redis persist failed: ' + e.message);
      }
    }

    // ----- 3. Handle request -----
    const { method, query, body } = req;
    const { table, id, sql } = query;

    // SQL endpoint (for SELECT and other commands)
    if (method === 'GET' && sql) {
      const sqlQuery = sql;
      if (sqlQuery.trim().toUpperCase().startsWith('SELECT')) {
        const result = database.exec(sqlQuery);
        if (result.length === 0) {
          return res.status(200).json({ success: true, columns: [], rows: [] });
        }
        const { columns, values } = result[0];
        const rows = values.map(row => {
          const obj = {};
          columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        });
        return res.status(200).json({ success: true, columns, rows });
      } else {
        database.run(sqlQuery);
        if (redis) await redis.set('sqlite_db', Buffer.from(database.export()));
        return res.status(200).json({ success: true, message: 'SQL executed' });
      }
    }

    // CRUD endpoints (table‑based)
    if (!table) {
      return res.status(400).json({ error: 'Missing table name (or use ?sql=...)' });
    }

    if (method === 'GET') {
      const result = database.exec(`SELECT * FROM "${table}"${id ? ` WHERE id = ${id}` : ''}`);
      const rows = result[0]?.values.map(row => {
        const obj = {};
        result[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
        return obj;
      }) || [];
      return res.status(200).json({ success: true, data: rows });
    }

    if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      database.run(`INSERT INTO "${table}" (${keys.join(',')}) VALUES (${placeholders})`, Object.values(record));
      const lastId = database.exec('SELECT last_insert_rowid()')[0].values[0][0];
      if (redis) await redis.set('sqlite_db', Buffer.from(database.export()));
      return res.status(201).json({ success: true, id: lastId });
    }

    if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      database.run(`UPDATE "${table}" SET ${setClause} WHERE id = ?`, [...Object.values(updates), id]);
      if (redis) await redis.set('sqlite_db', Buffer.from(database.export()));
      return res.status(200).json({ success: true });
    }

    if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      database.run(`DELETE FROM "${table}" WHERE id = ?`, [id]);
      if (redis) await redis.set('sqlite_db', Buffer.from(database.export()));
      return res.status(200).json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    // Catch any unhandled error and return it as JSON (so the function never crashes)
    console.error('Unhandled error:', err);
    return res.status(500).json({ error: err.message, stack: err.stack });
  }
}
