import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

const redis = Redis.fromEnv();
let db = null;

// ========== CORS ==========
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}

// ========== Database init ==========
async function getDb() {
  if (db) return db;
  const SQL = await initSqlJs({ locateFile: file => `https://sql.js.org/dist/${file}` });
  let persisted = null;
  try {
    const buf = await redis.get('sqlite_db');
    if (buf) persisted = new Uint8Array(buf);
  } catch (e) {}
  if (!persisted) {
    try {
      const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
      const fileBuffer = fs.readFileSync(dbPath);
      persisted = new Uint8Array(fileBuffer);
    } catch (err) {}
  }
  db = new SQL.Database(persisted);
  await persistDb();
  return db;
}

async function persistDb() {
  if (!db) return;
  await redis.set('sqlite_db', Buffer.from(db.export()));
}

// ========== Main handler ==========
export default async function handler(req, res) {
  // 1. Set CORS on EVERY response
  setCors(res);

  // 2. Handle preflight OPTIONS immediately
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // 3. Auth
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
        if (result.length === 0) return res.status(200).json({ success: true, columns: [], rows: [] });
        const { columns, values } = result[0];
        const rows = values.map(row => {
          const obj = {};
          columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        });
        return res.status(200).json({ success: true, columns, rows });
      } else {
        database.run(sqlQuery);
        await persistDb();
        return res.status(200).json({ success: true, message: 'SQL executed' });
      }
    }

    // CRUD endpoints (keep existing)
    if (!table) return res.status(400).json({ error: 'Missing table' });

    if (method === 'GET') {
      let rows;
      if (id) {
        const r = database.exec(`SELECT * FROM "${table}" WHERE id = ?`, [id]);
        rows = r[0]?.values.map(row => {
          const obj = {};
          r[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        }) || null;
      } else {
        const r = database.exec(`SELECT * FROM "${table}"`);
        rows = r[0]?.values.map(row => {
          const obj = {};
          r[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        }) || [];
      }
      return res.status(200).json({ success: true, data: rows });
    }

    if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      database.run(`INSERT INTO "${table}" (${keys.join(',')}) VALUES (${placeholders})`, Object.values(record));
      const lastId = database.exec('SELECT last_insert_rowid()')[0].values[0][0];
      await persistDb();
      return res.status(201).json({ success: true, id: lastId });
    }

    if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      database.run(`UPDATE "${table}" SET ${setClause} WHERE id = ?`, [...Object.values(updates), id]);
      await persistDb();
      return res.status(200).json({ success: true });
    }

    if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      database.run(`DELETE FROM "${table}" WHERE id = ?`, [id]);
      await persistDb();
      return res.status(200).json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
