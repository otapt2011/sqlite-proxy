import initSqlJs from 'sql.js';
import { Redis } from '@upstash/redis';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// For ES modules, get __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Path to the WASM file inside node_modules
const wasmPath = path.join(__dirname, '..', 'node_modules', 'sql.js', 'dist', 'sql-wasm.wasm');

export default async function handler(req, res) {
  // CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'X-API-Key, Authorization, Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();

  const authHeader = req.headers['x-api-key'] || req.headers['authorization'];
  if (authHeader !== process.env.API_SECRET_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    // Load SQL.js with local WASM file
    const SQL = await initSqlJs({
      locateFile: () => wasmPath
    });

    let dbData = null;
    let redis = null;
    try {
      redis = Redis.fromEnv();
      const buf = await redis.get('sqlite_db');
      if (buf) dbData = new Uint8Array(buf);
    } catch (e) {
      console.log('Redis not available');
    }

    if (!dbData) {
      try {
        const dbPath = path.join(process.cwd(), 'db', 'northwind.db');
        const fileBuf = fs.readFileSync(dbPath);
        dbData = new Uint8Array(fileBuf);
      } catch (err) {
        return res.status(500).json({ error: 'Database file not found: ' + err.message });
      }
    }

    const db = new SQL.Database(dbData);

    const { sql } = req.query;
    if (req.method === 'GET' && sql) {
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
    }

    return res.status(400).json({ error: 'Missing ?sql parameter' });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ error: err.message });
  }
}
