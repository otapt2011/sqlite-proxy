import Database from 'better-sqlite3';
import fs from 'fs';

const DB_PATH = '/tmp/mydb.sqlite';

// Initialize database if not exists
if (!fs.existsSync(DB_PATH)) {
  const db = new Database(DB_PATH);
  db.exec(`
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
  db.close();
}

function getDb() {
  return new Database(DB_PATH);
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const { method, query, body } = req;
  const { table, id } = query;

  if (!table) return res.status(400).json({ error: 'Missing table name' });

  try {
    const db = getDb();

    // GET /api/db?table=users
    if (method === 'GET') {
      let rows;
      if (id) {
        rows = db.prepare(`SELECT * FROM ${table} WHERE id = ?`).get(id);
      } else {
        rows = db.prepare(`SELECT * FROM ${table}`).all();
      }
      res.status(200).json({ success: true, data: rows });
    }
    // POST /api/db?table=users - body: { record: { name, email } }
    else if (method === 'POST') {
      const { record } = body;
      if (!record) return res.status(400).json({ error: 'Missing record' });
      const keys = Object.keys(record);
      const placeholders = keys.map(() => '?').join(',');
      const values = Object.values(record);
      const stmt = db.prepare(`INSERT INTO ${table} (${keys.join(',')}) VALUES (${placeholders})`);
      const result = stmt.run(values);
      res.status(201).json({ success: true, id: result.lastInsertRowid });
    }
    // PUT /api/db?table=users&id=1 - body: { updates: { name } }
    else if (method === 'PUT') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      const { updates } = body;
      if (!updates) return res.status(400).json({ error: 'Missing updates' });
      const setClause = Object.keys(updates).map(k => `${k} = ?`).join(',');
      const values = [...Object.values(updates), id];
      const stmt = db.prepare(`UPDATE ${table} SET ${setClause} WHERE id = ?`);
      stmt.run(values);
      res.status(200).json({ success: true });
    }
    // DELETE /api/db?table=users&id=1
    else if (method === 'DELETE') {
      if (!id) return res.status(400).json({ error: 'Missing id' });
      db.prepare(`DELETE FROM ${table} WHERE id = ?`).run(id);
      res.status(200).json({ success: true });
    }
    else {
      res.status(405).json({ error: 'Method not allowed' });
    }
    db.close();
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
