import fs from 'fs';
import path from 'path';

export function buildBackupPath(storagePath: string, connectionName: string, ext = 'sql.gz'): string {
  const dir = path.join(storagePath, connectionName);
  fs.mkdirSync(dir, { recursive: true });
  const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  return path.join(dir, `${ts}.${ext}`);
}

export function getFileSize(filePath: string): number {
  try {
    return fs.statSync(filePath).size;
  } catch {
    return 0;
  }
}
