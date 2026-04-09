import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';

export default async function handler(req, res) {
  // Set CORS headers first
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth check (simple)
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized: invalid key' });
  }

  try {
    // 1. Try to load SQL.js
    let SQL;
    try {
      SQL = await initSqlJs({ locateFile: file => `https://sql.js.org/dist/${file}` });
    } catch (err) {
      console.error('SQL.js init error:', err);
      return res.status(500).json({ error: 'SQL.js init failed: ' + err.message });
    }

    // 2. Load database from Redis or file
    let dbData = null;
    let redis = null;
    try {
      redis = Redis.fromEnv();
      const buf = await redis.get('sqlite_db');
      if (buf) dbData = new Uint8Array(buf);
    } catch (e) {
      console.log('Redis not available:', e.message);
    }

    if (!dbData) {
      try {
        const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
        console.log('Looking for DB at:', dbPath);
        const fileBuf = fs.readFileSync(dbPath);
        dbData = new Uint8Array(fileBuf);
        console.log('Loaded northwind.db from repo');
      } catch (err) {
        console.error('File read error:', err.message);
        return res.status(500).json({ error: 'Database file not found: ' + err.message });
      }
    }

    // 3. Create database instance
    const db = new SQL.Database(dbData);

    // 4. Handle request
    const { sql } = req.query;
    if (req.method === 'GET' && sql) {
      try {
        const result = db.exec(sql);
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
        return res.status(500).json({ error: 'SQL execution error: ' + err.message });
      }
    }

    return res.status(400).json({ error: 'Missing ?sql parameter' });
  } catch (err) {
    console.error('Unhandled error in handler:', err);
    return res.status(500).json({ error: 'Internal server error: ' + err.message });
  }
}
