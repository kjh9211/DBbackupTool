import { spawn } from 'child_process';
import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';
import mysql2 from 'mysql2/promise';
import { BackupAdapter, Connection } from './base';

export class MySQLAdapter implements BackupAdapter {
  async test(conn: Connection): Promise<boolean> {
    try {
      const db = await mysql2.createConnection({
        host: conn.host,
        port: conn.port,
        database: conn.database,
        user: conn.username,
        password: conn.password,
      });
      await db.ping();
      await db.end();
      return true;
    } catch {
      return false;
    }
  }

  async backup(conn: Connection, outPath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const args = [
        '--no-tablespaces',
        '-h', conn.host ?? 'localhost',
        '-P', String(conn.port ?? 3306),
        '-u', conn.username ?? 'root',
        conn.database,
      ];
      if (conn.password) {
        args.unshift(`-p${conn.password}`);
      }

      const mysqldump = spawn('mysqldump', args);
      const writeStream = fs.createWriteStream(outPath);
      const gzip = zlib.createGzip();

      mysqldump.stdout.pipe(gzip).pipe(writeStream);

      mysqldump.stderr.on('data', () => {});

      writeStream.on('finish', resolve);
      mysqldump.on('error', reject);
      writeStream.on('error', reject);
    });
  }

  async restore(conn: Connection, filePath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const args = [
        '-h', conn.host ?? 'localhost',
        '-P', String(conn.port ?? 3306),
        '-u', conn.username ?? 'root',
        conn.database,
      ];
      if (conn.password) {
        args.unshift(`-p${conn.password}`);
      }

      const mysql = spawn('mysql', args);
      const readStream = fs.createReadStream(filePath);
      const gunzip = zlib.createGunzip();

      pipeline(readStream, gunzip, mysql.stdin as NodeJS.WritableStream).catch(reject);

      mysql.stderr.on('data', () => {});
      mysql.on('close', (code) => {
        if (code === 0) resolve();
        else reject(new Error(`mysql exited with code ${code}`));
      });
      mysql.on('error', reject);
    });
  }
}
