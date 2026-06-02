import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';
import { BackupAdapter, Connection } from './base';

export class SQLiteAdapter implements BackupAdapter {
  async test(conn: Connection): Promise<boolean> {
    return fs.existsSync(conn.database);
  }

  async backup(conn: Connection, outPath: string): Promise<void> {
    if (!fs.existsSync(conn.database)) {
      throw new Error(`SQLite 파일을 찾을 수 없습니다: ${conn.database}`);
    }
    await pipeline(
      fs.createReadStream(conn.database),
      zlib.createGzip(),
      fs.createWriteStream(outPath),
    );
  }

  async restore(conn: Connection, filePath: string): Promise<void> {
    await pipeline(
      fs.createReadStream(filePath),
      zlib.createGunzip(),
      fs.createWriteStream(conn.database),
    );
  }
}
