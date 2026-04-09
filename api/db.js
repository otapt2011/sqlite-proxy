import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

const redis = Redis.fromEnv();
let db = null;

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}

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

export default async function handler(req, res) {
  // 1. CORS headers for EVERY response
  setCors(res);

  // 2. OPTIONS preflight → immediate 200
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // 3. Auth
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  // 4. Only SQL endpoint for testing (SELECT 1)
  const { sql } = req.query;
  if (req.method === 'GET' && sql) {
    try {
      const database = await getDb();
      const result = database.exec(sql);
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
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  return res.status(400).json({ error: 'Use ?sql=SELECT ...' });
}
