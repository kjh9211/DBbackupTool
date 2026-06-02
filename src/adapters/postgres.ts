import { spawn } from 'child_process';
import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';
import { Client } from 'pg';
import { BackupAdapter, Connection } from './base';

export class PostgresAdapter implements BackupAdapter {
  async test(conn: Connection): Promise<boolean> {
    const client = new Client({
      host: conn.host,
      port: conn.port,
      database: conn.database,
      user: conn.username,
      password: conn.password,
    });
    try {
      await client.connect();
      await client.end();
      return true;
    } catch {
      return false;
    }
  }

  async backup(conn: Connection, outPath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const env = { ...process.env };
      if (conn.password) env['PGPASSWORD'] = conn.password;

      const args = [
        '-h', conn.host ?? 'localhost',
        '-p', String(conn.port ?? 5432),
        '-U', conn.username ?? 'postgres',
        '-d', conn.database,
        '--no-password',
      ];

      const pgdump = spawn('pg_dump', args, { env });
      const writeStream = fs.createWriteStream(outPath);
      const gzip = zlib.createGzip();

      pgdump.stdout.pipe(gzip).pipe(writeStream);
      pgdump.stderr.on('data', () => {});

      writeStream.on('finish', resolve);
      pgdump.on('error', reject);
      writeStream.on('error', reject);
    });
  }

  async restore(conn: Connection, filePath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const env = { ...process.env };
      if (conn.password) env['PGPASSWORD'] = conn.password;

      const args = [
        '-h', conn.host ?? 'localhost',
        '-p', String(conn.port ?? 5432),
        '-U', conn.username ?? 'postgres',
        '-d', conn.database,
        '--no-password',
      ];

      const psql = spawn('psql', args, { env });
      const readStream = fs.createReadStream(filePath);
      const gunzip = zlib.createGunzip();

      pipeline(readStream, gunzip, psql.stdin as NodeJS.WritableStream).catch(reject);

      psql.stderr.on('data', () => {});
      psql.on('close', (code) => {
        if (code === 0) resolve();
        else reject(new Error(`psql exited with code ${code}`));
      });
      psql.on('error', reject);
    });
  }
}
