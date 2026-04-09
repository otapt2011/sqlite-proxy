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

// ========== Main handler ==========
export default async function handler(req, res) {
  setCors(res);

  // OPTIONS preflight
  if (req.method === 'OPTIONS') return res.status(200).end();

  // Auth
  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // ----- Init SQL.js -----
    const SQL = await initSqlJs({
      locateFile: file => `https://sql.js.org/dist/${file}`
    });

    // ----- Load database (Redis first, then file) -----
    let dbData = null;
    let redis = null;
    try {
      redis = Redis.fromEnv();
      const buf = await redis.get('sqlite_db');
      if (buf) dbData = new Uint8Array(buf);
    } catch (e) { /* Redis not configured or empty */ }

    if (!dbData) {
      try {
        const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
        const fileBuf = fs.readFileSync(dbPath);
        dbData = new Uint8Array(fileBuf);
      } catch (e) {
        // No file – start empty
        dbData = null;
      }
    }

    const db = new SQL.Database(dbData);

    // Persist to Redis if available and we have data
    if (redis && db) {
      try {
        await redis.set('sqlite_db', Buffer.from(db.export()));
      } catch (e) {}
    }

    // ----- Handle request -----
    const { method, query, body } = req;
    const { table, id, sql } = query;

    // SQL endpoint
    if (method === 'GET' && sql) {
      if (sql.trim().toUpperCase().startsWith('SELECT')) {
        const result = db.exec(sql);
        if (!result.length) return res.status(200).json({ success: true, columns: [], rows: [] });
        const { columns, values } = result[0];
        const rows = values.map(row => {
          const obj = {};
          columns.forEach((col, i) => obj[col] = row[i]);
          return obj;
        });
        return res.status(200).json({ success: true, columns, rows });
      } else {
        db.run(sql);
        if (redis) await redis.set('sqlite_db', Buffer.from(db.export()));
        return res.status(200).json({ success: true, message: 'SQL executed' });
      }
    }

    // CRUD endpoints
    if (!table) return res.status(400).json({ error: 'Missing table or ?sql' });

    if (method === 'GET') {
      const result = db.exec(`SELECT * FROM "${table}"${id ? ` WHERE id = ${id}` : ''}`);
      const rows = result[0]?.values.map(row => {
        const obj = {};
        result[0].columns.forEach((col, i) => obj[col] = row[i]);
        return obj;
      }) || [];
      return res.status(200).json({ success: true, data: rows });
    }

    if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      db.run(`INSERT INTO "${table}" (${keys.join(',')}) VALUES (${placeholders})`, Object.values(record));
      const lastId = db.exec('SELECT last_insert_rowid()')[0].values[0][0];
      if (redis) await redis.set('sqlite_db', Buffer.from(db.export()));
      return res.status(201).json({ success: true, id: lastId });
    }

    if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      db.run(`UPDATE "${table}" SET ${setClause} WHERE id = ?`, [...Object.values(updates), id]);
      if (redis) await redis.set('sqlite_db', Buffer.from(db.export()));
      return res.status(200).json({ success: true });
    }

    if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      db.run(`DELETE FROM "${table}" WHERE id = ?`, [id]);
      if (redis) await redis.set('sqlite_db', Buffer.from(db.export()));
      return res.status(200).json({ success: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
