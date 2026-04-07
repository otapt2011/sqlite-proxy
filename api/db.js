import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';

const redis = Redis.fromEnv();

let db = null;

async function getDb() {
  if (db) return db;
  const SQL = await initSqlJs({
    locateFile: file => `https://sql.js.org/dist/${file}`
  });
  // Try to load persisted database from Redis
  let persisted = null;
  try {
    const buf = await redis.get('sqlite_db');
    if (buf) {
      persisted = new Uint8Array(buf);
    }
  } catch (e) {
    console.log('No persisted DB found, starting fresh');
  }
  db = new SQL.Database(persisted);
  // Create tables if not exist
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      email TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      endpoint TEXT,
      timestamp TEXT
    );
  `);
  // Save after init
  await persistDb();
  return db;
}

async function persistDb() {
  if (!db) return;
  const data = db.export();
  await redis.set('sqlite_db', Buffer.from(data));
}

export default async function handler(req, res) {
  // CORS
  if (req.method === 'OPTIONS') {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');
    return res.status(200).end();
  }

  // Auth (same API_SECRET_KEY as your other proxies)
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { method, query, body } = req;
  const { table, id } = query;

  if (!table) return res.status(400).json({ error: 'Missing table name' });

  try {
    const database = await getDb();

    if (method === 'GET') {
      let rows;
      if (id) {
        rows = database.exec(`SELECT * FROM ${table} WHERE id = ?`, [id]);
        rows = rows[0]?.values.map(row => {
          const obj = {};
          rows[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        }) || null;
      } else {
        const result = database.exec(`SELECT * FROM ${table}`);
        if (result.length === 0) rows = [];
        else {
          rows = result[0].values.map(row => {
            const obj = {};
            result[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
            return obj;
          });
        }
      }
      res.status(200).json({ success: true, data: rows });
    }
    else if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      const values = Object.values(record);
      database.run(`INSERT INTO ${table} (${keys.join(',')}) VALUES (${placeholders})`, values);
      const lastId = database.exec('SELECT last_insert_rowid()')[0].values[0][0];
      await persistDb();
      res.status(201).json({ success: true, id: lastId });
    }
    else if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      const values = [...Object.values(updates), id];
      database.run(`UPDATE ${table} SET ${setClause} WHERE id = ?`, values);
      await persistDb();
      res.status(200).json({ success: true });
    }
    else if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      database.run(`DELETE FROM ${table} WHERE id = ?`, [id]);
      await persistDb();
      res.status(200).json({ success: true });
    }
    else {
      res.status(405).json({ error: 'Method not allowed' });
    }
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
