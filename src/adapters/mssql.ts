import fs from 'fs';
import zlib from 'zlib';
import sql from 'mssql';
import { BackupAdapter, Connection } from './base';

function buildConfig(conn: Connection): sql.config {
  return {
    server: conn.host ?? 'localhost',
    port: conn.port ?? 1433,
    database: conn.database,
    user: conn.username,
    password: conn.password,
    options: {
      encrypt: (conn.extra?.encrypt as boolean) ?? false,
      trustServerCertificate: true,
    },
  };
}

export class MSSQLAdapter implements BackupAdapter {
  async test(conn: Connection): Promise<boolean> {
    try {
      const pool = await sql.connect(buildConfig(conn));
      await pool.request().query('SELECT 1');
      await pool.close();
      return true;
    } catch {
      return false;
    }
  }

  async backup(conn: Connection, outPath: string): Promise<void> {
    const pool = await sql.connect(buildConfig(conn));
    try {
      const result = await pool.request().query(`
        SELECT
          t.name AS table_name,
          c.name AS column_name,
          tp.name AS data_type,
          c.max_length,
          c.is_nullable
        FROM sys.tables t
        JOIN sys.columns c ON t.object_id = c.object_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        ORDER BY t.name, c.column_id
      `);

      const tables: Record<string, { column: string; type: string; nullable: boolean }[]> = {};
      for (const row of result.recordset) {
        if (!tables[row.table_name]) tables[row.table_name] = [];
        tables[row.table_name].push({
          column: row.column_name,
          type: row.data_type,
          nullable: row.is_nullable,
        });
      }

      let dump = `-- MSSQL Dump: ${conn.database} (${new Date().toISOString()})\n`;
      dump += `USE [${conn.database}];\nGO\n\n`;

      for (const tableName of Object.keys(tables)) {
        const dataResult = await pool.request().query(`SELECT * FROM [${tableName}]`);
        const cols = tables[tableName].map((c) => c.column);

        dump += `-- Table: ${tableName}\n`;
        for (const row of dataResult.recordset) {
          const values = cols.map((col) => {
            const v = row[col];
            if (v === null || v === undefined) return 'NULL';
            if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
            if (v instanceof Date) return `'${v.toISOString()}'`;
            return String(v);
          });
          dump += `INSERT INTO [${tableName}] (${cols.map((c) => `[${c}]`).join(', ')}) VALUES (${values.join(', ')});\n`;
        }
        dump += '\n';
      }

      await new Promise<void>((resolve, reject) => {
        const gzip = zlib.createGzip();
        const write = fs.createWriteStream(outPath);
        gzip.pipe(write);
        gzip.write(dump, 'utf8');
        gzip.end();
        write.on('finish', resolve);
        write.on('error', reject);
      });
    } finally {
      await pool.close();
    }
  }

  async restore(conn: Connection, filePath: string): Promise<void> {
    const sql_content = await new Promise<string>((resolve, reject) => {
      const chunks: Buffer[] = [];
      const read = fs.createReadStream(filePath);
      const gunzip = zlib.createGunzip();
      read.pipe(gunzip);
      gunzip.on('data', (chunk: Buffer) => chunks.push(chunk));
      gunzip.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      gunzip.on('error', reject);
    });

    const pool = await sql.connect(buildConfig(conn));
    try {
      const statements = sql_content
        .split(/\nGO\b/i)
        .map((s) => s.trim())
        .filter((s) => s && !s.startsWith('--'));
      for (const stmt of statements) {
        if (stmt) await pool.request().query(stmt);
      }
    } finally {
      await pool.close();
    }
  }
}
