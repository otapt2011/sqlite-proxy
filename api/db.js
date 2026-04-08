import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

const redis = Redis.fromEnv();
let db = null;

async function getDb() {
  if (db) return db;

  const SQL = await initSqlJs({
    locateFile: file => `https://sql.js.org/dist/${file}`
  });

  let persisted = null;

  // 1. Try to load from Redis first
  try {
    const buf = await redis.get('sqlite_db');
    if (buf) {
      persisted = new Uint8Array(buf);
      console.log('Loaded database from Redis');
    }
  } catch (e) {
    console.log('No persisted DB found in Redis');
  }

  // 2. If Redis empty, load northwind.db from repository
  if (!persisted) {
    try {
      const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
      const fileBuffer = fs.readFileSync(dbPath);
      persisted = new Uint8Array(fileBuffer);
      console.log('Loaded northwind.db from repository');
    } catch (err) {
      console.log('No northwind.db found, starting fresh empty database');
      // If no file, start with empty database (will create tables later if needed)
      persisted = null;
    }
  }

  // 3. Create database instance
  db = new SQL.Database(persisted);

  // 4. Optional: create any missing tables (if you want to keep your own schema)
  // For northwind, tables already exist, so this is not necessary.
  // Uncomment if you need additional tables:
  /*
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      email TEXT UNIQUE
    );
  `);
  */

  // 5. Persist to Redis immediately (so next cold start uses Redis)
  await persistDb();

  return db;
}

async function persistDb() {
  if (!db) return;
  const data = db.export();
  await redis.set('sqlite_db', Buffer.from(data));
}

export default async function handler(req, res) {
  // CORS headers (allow any origin for local HTML tester)
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');

  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Authentication
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { method, query, body } = req;
  const { table, id, sql } = query;

  try {
    const database = await getDb();

    // ----- CUSTOM SQL ENDPOINT (for explorer) -----
    if (method === 'GET' && sql) {
      const sqlQuery = sql;
      // For SELECT queries, return rows as JSON
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
        // Non-SELECT: INSERT, UPDATE, DELETE, CREATE, ALTER, etc.
        database.run(sqlQuery);
        await persistDb();
        return res.status(200).json({ success: true, message: 'SQL executed successfully' });
      }
    }

    // ----- STANDARD CRUD ENDPOINTS (table-based) -----
    if (!table) {
      return res.status(400).json({ error: 'Missing table name (or use ?sql=)' });
    }

    if (method === 'GET') {
      let rows;
      if (id) {
        const result = database.exec(`SELECT * FROM "${table}" WHERE id = ?`, [id]);
        rows = result[0]?.values.map(row => {
          const obj = {};
          result[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
          return obj;
        }) || null;
      } else {
        const result = database.exec(`SELECT * FROM "${table}"`);
        if (result.length === 0) rows = [];
        else {
          rows = result[0].values.map(row => {
            const obj = {};
            result[0].columns.forEach((col, idx) => { obj[col] = row[idx]; });
            return obj;
          });
        }
      }
      return res.status(200).json({ success: true, data: rows });
    }

    else if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      const values = Object.values(record);
      database.run(`INSERT INTO "${table}" (${keys.join(',')}) VALUES (${placeholders})`, values);
      const lastId = database.exec('SELECT last_insert_rowid()')[0].values[0][0];
      await persistDb();
      return res.status(201).json({ success: true, id: lastId });
    }

    else if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      const values = [...Object.values(updates), id];
      database.run(`UPDATE "${table}" SET ${setClause} WHERE id = ?`, values);
      await persistDb();
      return res.status(200).json({ success: true });
    }

    else if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      database.run(`DELETE FROM "${table}" WHERE id = ?`, [id]);
      await persistDb();
      return res.status(200).json({ success: true });
    }

    else {
      return res.status(405).json({ error: 'Method not allowed' });
    }
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
