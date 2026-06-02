import { spawn } from 'child_process';
import fs from 'fs';
import zlib from 'zlib';
import { pipeline } from 'stream/promises';
import { MongoClient } from 'mongodb';
import { BackupAdapter, Connection } from './base';

function buildMongoUri(conn: Connection): string {
  const auth = conn.username ? `${encodeURIComponent(conn.username)}:${encodeURIComponent(conn.password ?? '')}@` : '';
  return `mongodb://${auth}${conn.host ?? 'localhost'}:${conn.port ?? 27017}/${conn.database}`;
}

export class MongoDBAdapter implements BackupAdapter {
  async test(conn: Connection): Promise<boolean> {
    const client = new MongoClient(buildMongoUri(conn), { serverSelectionTimeoutMS: 5000 });
    try {
      await client.connect();
      await client.db().command({ ping: 1 });
      await client.close();
      return true;
    } catch {
      return false;
    }
  }

  async backup(conn: Connection, outPath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const uri = buildMongoUri(conn);
      const args = ['--uri', uri, '--archive', '--gzip'];

      const mongodump = spawn('mongodump', args);
      const writeStream = fs.createWriteStream(outPath);

      mongodump.stdout.pipe(writeStream);
      mongodump.stderr.on('data', () => {});

      writeStream.on('finish', resolve);
      mongodump.on('error', (err) => {
        reject(new Error(`mongodump를 찾을 수 없습니다. MongoDB Database Tools를 설치해주세요: ${err.message}`));
      });
      writeStream.on('error', reject);
      mongodump.on('close', (code) => {
        if (code !== 0) reject(new Error(`mongodump exited with code ${code}`));
      });
    });
  }

  async restore(conn: Connection, filePath: string): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const uri = buildMongoUri(conn);
      const args = ['--uri', uri, '--archive=' + filePath, '--gzip', '--drop'];

      const mongorestore = spawn('mongorestore', args);
      mongorestore.stderr.on('data', () => {});
      mongorestore.on('close', (code) => {
        if (code === 0) resolve();
        else reject(new Error(`mongorestore exited with code ${code}`));
      });
      mongorestore.on('error', reject);
    });
  }
}
