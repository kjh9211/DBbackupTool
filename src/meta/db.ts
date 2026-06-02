import sqlite3 from 'sqlite3';
import fs from 'fs';
import path from 'path';

export interface ConnectionRecord {
  id: string;
  name: string;
  type: string;
  host?: string;
  port?: number;
  database: string;
  username?: string;
  password?: string;
  extra?: string;
  created_at: string;
}

export interface BackupRecord {
  id: string;
  connection_id: string;
  file_path: string;
  file_size: number;
  status: 'success' | 'failed';
  error?: string;
  started_at: string;
  finished_at?: string;
}

export interface ScheduleRecord {
  id: string;
  connection_id: string;
  cron: string;
  enabled: number;
  last_run?: string;
}

export class MetaDB {
  private db!: sqlite3.Database;
  private readonly dbPath: string;

  constructor(dbPath: string) {
    this.dbPath = dbPath;
  }

  async init(): Promise<void> {
    fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
    this.db = new sqlite3.Database(this.dbPath);
    await this.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA foreign_keys = ON;

      CREATE TABLE IF NOT EXISTS connections (
        id         TEXT PRIMARY KEY,
        name       TEXT UNIQUE NOT NULL,
        type       TEXT NOT NULL,
        host       TEXT,
        port       INTEGER,
        database   TEXT NOT NULL,
        username   TEXT,
        password   TEXT,
        extra      TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS backups (
        id            TEXT PRIMARY KEY,
        connection_id TEXT NOT NULL,
        file_path     TEXT NOT NULL,
        file_size     INTEGER NOT NULL DEFAULT 0,
        status        TEXT NOT NULL,
        error         TEXT,
        started_at    TEXT NOT NULL,
        finished_at   TEXT
      );

      CREATE TABLE IF NOT EXISTS schedules (
        id            TEXT PRIMARY KEY,
        connection_id TEXT NOT NULL,
        cron          TEXT NOT NULL,
        enabled       INTEGER NOT NULL DEFAULT 1,
        last_run      TEXT
      );
    `);
  }

  close(): void {
    this.db?.close();
  }

  private exec(sql: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.exec(sql, (err) => (err ? reject(err) : resolve()));
    });
  }

  run(sql: string, params: unknown[] = []): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.run(sql, params, (err) => (err ? reject(err) : resolve()));
    });
  }

  get<T>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      this.db.get(sql, params, (err: Error | null, row: unknown) =>
        err ? reject(err) : resolve(row as T),
      );
    });
  }

  all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.db.all(sql, params, (err: Error | null, rows: unknown[]) =>
        err ? reject(err) : resolve(rows as T[]),
      );
    });
  }

  // --- connections ---
  getConnections(): Promise<ConnectionRecord[]> {
    return this.all<ConnectionRecord>('SELECT * FROM connections ORDER BY created_at');
  }

  getConnectionByName(name: string): Promise<ConnectionRecord | undefined> {
    return this.get<ConnectionRecord>('SELECT * FROM connections WHERE name = ?', [name]);
  }

  getConnectionById(id: string): Promise<ConnectionRecord | undefined> {
    return this.get<ConnectionRecord>('SELECT * FROM connections WHERE id = ?', [id]);
  }

  insertConnection(c: ConnectionRecord): Promise<void> {
    return this.run(
      `INSERT INTO connections (id, name, type, host, port, database, username, password, extra, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [c.id, c.name, c.type, c.host ?? null, c.port ?? null,
       c.database, c.username ?? null, c.password ?? null, c.extra ?? null, c.created_at],
    );
  }

  deleteConnection(name: string): Promise<void> {
    return this.run('DELETE FROM connections WHERE name = ?', [name]);
  }

  // --- backups ---
  getBackups(connectionId?: string): Promise<BackupRecord[]> {
    if (connectionId) {
      return this.all<BackupRecord>(
        'SELECT * FROM backups WHERE connection_id = ? ORDER BY started_at DESC',
        [connectionId],
      );
    }
    return this.all<BackupRecord>('SELECT * FROM backups ORDER BY started_at DESC');
  }

  insertBackup(b: BackupRecord): Promise<void> {
    return this.run(
      `INSERT INTO backups (id, connection_id, file_path, file_size, status, error, started_at, finished_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [b.id, b.connection_id, b.file_path, b.file_size,
       b.status, b.error ?? null, b.started_at, b.finished_at ?? null],
    );
  }

  // --- schedules ---
  getSchedules(): Promise<ScheduleRecord[]> {
    return this.all<ScheduleRecord>('SELECT * FROM schedules');
  }

  insertSchedule(s: ScheduleRecord): Promise<void> {
    return this.run(
      'INSERT INTO schedules (id, connection_id, cron, enabled) VALUES (?, ?, ?, ?)',
      [s.id, s.connection_id, s.cron, s.enabled],
    );
  }

  updateSchedule(id: string, patch: { enabled?: number; last_run?: string }): Promise<void> {
    const fields: string[] = [];
    const params: unknown[] = [];
    if (patch.enabled !== undefined) { fields.push('enabled = ?'); params.push(patch.enabled); }
    if (patch.last_run !== undefined) { fields.push('last_run = ?'); params.push(patch.last_run); }
    if (fields.length === 0) return Promise.resolve();
    params.push(id);
    return this.run(`UPDATE schedules SET ${fields.join(', ')} WHERE id = ?`, params);
  }

  deleteSchedule(id: string): Promise<void> {
    return this.run('DELETE FROM schedules WHERE id = ?', [id]);
  }
}
