import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

// Only initialize Redis if environment variables exist
let redis = null;
try {
  redis = Redis.fromEnv();
} catch (e) {
  console.log('Redis not configured, will not persist');
}

let db = null;

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}

async function getDb() {
  if (db) return db;

  const SQL = await initSqlJs({
    locateFile: file => `https://sql.js.org/dist/${file}`
  });

  let persisted = null;

  // Try Redis first (if available)
  if (redis) {
    try {
      const buf = await redis.get('sqlite_db');
      if (buf) {
        persisted = new Uint8Array(buf);
        console.log('Loaded from Redis');
      }
    } catch (e) {
      console.log('Redis read failed', e.message);
    }
  }

  // If no Redis, load northwind.db from repository
  if (!persisted) {
    try {
      const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
      console.log('Looking for database at:', dbPath);
      const fileBuffer = fs.readFileSync(dbPath);
      persisted = new Uint8Array(fileBuffer);
      console.log('Loaded northwind.db from repo');
    } catch (err) {
      console.error('Failed to load northwind.db:', err.message);
      // Start empty database as fallback
      persisted = null;
    }
  }

  db = new SQL.Database(persisted);

  // If we have Redis, persist the database (even if empty)
  if (redis) {
    try {
      await persistDb();
    } catch (e) {
      console.log('Redis persist failed', e.message);
    }
  }

  return db;
}

async function persistDb() {
  if (!db || !redis) return;
  const data = db.export();
  await redis.set('sqlite_db', Buffer.from(data));
}

export default async function handler(req, res) {
  setCors(res);

  // OPTIONS preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Auth
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { method, query, body } = req;
  const { table, id, sql } = query;

  try {
    const database = await getDb();

    // SQL endpoint
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
        if (redis) await persistDb();
        return res.status(200).json({ success: true, message: 'SQL executed' });
      }
    }

    // CRUD endpoints (simplified)
    if (!table) {
      return res.status(400).json({ error: 'Missing table name or ?sql=' });
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
      if (redis) await persistDb();
      return res.status(201).json({ success: true, id: lastId });
    }

    if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      database.run(`UPDATE "${table}" SET ${setClause} WHERE id = ?`, [...Object.values(updates), id]);
      if (redis) await persistDb();
      return res.status(200).json({ success: true });
    }

    if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      database.run(`DELETE FROM "${table}" WHERE id = ?`, [id]);
      if (redis) await persistDb();
      return res.status(200).json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error('Handler error:', err);
    return res.status(500).json({ error: err.message });
  }
}
