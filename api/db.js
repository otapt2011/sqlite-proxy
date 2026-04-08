// Inside handler, after the existing GET block for table, add:
if (method === 'GET' && query.sql) {
  const sql = query.sql;
  const database = await getDb();
  try {
    // For SELECT queries, return rows as JSON
    if (sql.trim().toUpperCase().startsWith('SELECT')) {
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
      res.status(200).json({ success: true, columns, rows });
    } else {
      // For non-SELECT (INSERT, UPDATE, DELETE, CREATE, etc.)
      database.run(sql);
      await persistDb();
      res.status(200).json({ success: true, message: 'SQL executed successfully' });
    }
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
  return;
}
