import path from 'path';
import cron from 'node-cron';
import { v4 as uuidv4 } from 'uuid';
import { MetaDB, ConnectionRecord, BackupRecord, ScheduleRecord } from './meta/db';
import { buildBackupPath, getFileSize } from './storage/local';
import { encrypt, decrypt } from './utils/crypto';
import { DbType, BackupAdapter, Connection } from './adapters/base';
import { MySQLAdapter } from './adapters/mysql';
import { PostgresAdapter } from './adapters/postgres';
import { MongoDBAdapter } from './adapters/mongodb';
import { MSSQLAdapter } from './adapters/mssql';
import { SQLiteAdapter } from './adapters/sqlite';

export interface DBBackupToolConfig {
  metaPath: string;
  storagePath: string;
}

export interface ConnectionConfig {
  name: string;
  type: DbType;
  host?: string;
  port?: number;
  database: string;
  username?: string;
  password?: string;
  extra?: Record<string, unknown>;
}

export interface BackupResult {
  id: string;
  connectionName: string;
  filePath: string;
  fileSize: number;
  status: 'success' | 'failed';
  error?: string;
  startedAt: string;
  finishedAt: string;
}

export interface ScheduleConfig {
  cron: string;
  onBackup?: (result: BackupResult) => void;
  onError?: (err: Error) => void;
}

function getAdapter(type: DbType): BackupAdapter {
  switch (type) {
    case 'mysql':    return new MySQLAdapter();
    case 'postgres': return new PostgresAdapter();
    case 'mongodb':  return new MongoDBAdapter();
    case 'mssql':    return new MSSQLAdapter();
    case 'sqlite':   return new SQLiteAdapter();
  }
}

function recordToConnection(row: ConnectionRecord): Connection {
  return {
    id: row.id,
    name: row.name,
    type: row.type as DbType,
    host: row.host,
    port: row.port,
    database: row.database,
    username: row.username,
    password: row.password ? decrypt(row.password) : undefined,
    extra: row.extra ? JSON.parse(row.extra) : undefined,
    created_at: row.created_at,
  };
}

export class DBBackupTool {
  private readonly db: MetaDB;
  private readonly storagePath: string;
  private readonly cronTasks = new Map<string, cron.ScheduledTask>();
  private initialized = false;

  constructor(config: DBBackupToolConfig) {
    const metaPath = path.resolve(config.metaPath);
    this.storagePath = path.resolve(config.storagePath);
    this.db = new MetaDB(metaPath);
  }

  async init(): Promise<this> {
    if (!this.initialized) {
      await this.db.init();
      this.initialized = true;
    }
    return this;
  }

  private ensureInit(): void {
    if (!this.initialized) {
      throw new Error('DBBackupTool.init() 을 먼저 호출하세요.');
    }
  }

  // ── 커넥션 관리 ────────────────────────────────────────────

  async addConnection(config: ConnectionConfig): Promise<void> {
    this.ensureInit();
    const record: ConnectionRecord = {
      id: uuidv4(),
      name: config.name,
      type: config.type,
      host: config.host,
      port: config.port,
      database: config.database,
      username: config.username,
      password: config.password ? encrypt(config.password) : undefined,
      extra: config.extra ? JSON.stringify(config.extra) : undefined,
      created_at: new Date().toISOString(),
    };
    try {
      await this.db.insertConnection(record);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes('UNIQUE')) throw new Error(`이미 '${config.name}' 커넥션이 존재합니다.`);
      throw e;
    }
  }

  async removeConnection(name: string): Promise<void> {
    this.ensureInit();
    await this.db.deleteConnection(name);
  }

  async listConnections(): Promise<ConnectionConfig[]> {
    this.ensureInit();
    const rows = await this.db.getConnections();
    return rows.map((r) => ({
      name: r.name,
      type: r.type as DbType,
      host: r.host,
      port: r.port,
      database: r.database,
      username: r.username,
    }));
  }

  async testConnection(name: string): Promise<boolean> {
    this.ensureInit();
    const row = await this.db.getConnectionByName(name);
    if (!row) throw new Error(`커넥션 '${name}'을 찾을 수 없습니다.`);
    return getAdapter(row.type as DbType).test(recordToConnection(row));
  }

  // ── 백업 ────────────────────────────────────────────────────

  async backup(name: string): Promise<BackupResult> {
    this.ensureInit();
    const row = await this.db.getConnectionByName(name);
    if (!row) throw new Error(`커넥션 '${name}'을 찾을 수 없습니다.`);

    const conn = recordToConnection(row);
    const ext = row.type === 'mongodb' ? 'archive.gz' : 'sql.gz';
    const outPath = buildBackupPath(this.storagePath, name, ext);
    const startedAt = new Date().toISOString();

    let status: 'success' | 'failed' = 'success';
    let errorMsg: string | undefined;

    try {
      await getAdapter(conn.type).backup(conn, outPath);
    } catch (e: unknown) {
      status = 'failed';
      errorMsg = e instanceof Error ? e.message : String(e);
    }

    const record: BackupRecord = {
      id: uuidv4(),
      connection_id: row.id,
      file_path: outPath,
      file_size: status === 'success' ? getFileSize(outPath) : 0,
      status,
      error: errorMsg,
      started_at: startedAt,
      finished_at: new Date().toISOString(),
    };
    await this.db.insertBackup(record);

    const result: BackupResult = {
      id: record.id,
      connectionName: name,
      filePath: outPath,
      fileSize: record.file_size,
      status,
      error: errorMsg,
      startedAt: record.started_at,
      finishedAt: record.finished_at!,
    };

    if (status === 'failed') throw Object.assign(new Error(errorMsg), { result });
    return result;
  }

  async backupAll(): Promise<BackupResult[]> {
    this.ensureInit();
    const connections = await this.db.getConnections();
    const results: BackupResult[] = [];
    for (const conn of connections) {
      try {
        results.push(await this.backup(conn.name));
      } catch (e: unknown) {
        const err = e as Error & { result?: BackupResult };
        if (err.result) results.push(err.result);
      }
    }
    return results;
  }

  // ── 복원 ────────────────────────────────────────────────────

  async restore(name: string, filePath: string): Promise<void> {
    this.ensureInit();
    const row = await this.db.getConnectionByName(name);
    if (!row) throw new Error(`커넥션 '${name}'을 찾을 수 없습니다.`);
    const conn = recordToConnection(row);
    await getAdapter(conn.type).restore(conn, filePath);
  }

  // ── 이력 ────────────────────────────────────────────────────

  async getHistory(name?: string, limit = 20): Promise<BackupResult[]> {
    this.ensureInit();
    let records: BackupRecord[];
    if (name) {
      const row = await this.db.getConnectionByName(name);
      if (!row) throw new Error(`커넥션 '${name}'을 찾을 수 없습니다.`);
      records = await this.db.getBackups(row.id);
    } else {
      records = await this.db.getBackups();
    }
    return records.slice(0, limit).map((r) => ({
      id: r.id,
      connectionName: name ?? r.connection_id,
      filePath: r.file_path,
      fileSize: r.file_size,
      status: r.status,
      error: r.error,
      startedAt: r.started_at,
      finishedAt: r.finished_at ?? r.started_at,
    }));
  }

  // ── 스케줄 ────────────────────────────────────────────────────

  async addSchedule(name: string, config: ScheduleConfig): Promise<string> {
    this.ensureInit();
    const row = await this.db.getConnectionByName(name);
    if (!row) throw new Error(`커넥션 '${name}'을 찾을 수 없습니다.`);
    if (!cron.validate(config.cron)) throw new Error(`잘못된 cron 표현식: ${config.cron}`);

    const scheduleId = uuidv4();
    const record: ScheduleRecord = {
      id: scheduleId,
      connection_id: row.id,
      cron: config.cron,
      enabled: 1,
    };
    await this.db.insertSchedule(record);
    this._startCronTask(scheduleId, name, config);
    return scheduleId;
  }

  async removeSchedule(scheduleId: string): Promise<void> {
    this.ensureInit();
    this.cronTasks.get(scheduleId)?.stop();
    this.cronTasks.delete(scheduleId);
    await this.db.deleteSchedule(scheduleId);
  }

  async listSchedules(): Promise<Array<ScheduleRecord & { connectionName: string }>> {
    this.ensureInit();
    const schedules = await this.db.getSchedules();
    const connections = await this.db.getConnections();
    return schedules.map((s) => ({
      ...s,
      connectionName: connections.find((c) => c.id === s.connection_id)?.name ?? s.connection_id,
    }));
  }

  async startAllSchedules(callbacks?: { onBackup?: ScheduleConfig['onBackup']; onError?: ScheduleConfig['onError'] }): Promise<void> {
    this.ensureInit();
    const schedules = (await this.db.getSchedules()).filter((s) => s.enabled);
    const connections = await this.db.getConnections();
    for (const sched of schedules) {
      const conn = connections.find((c) => c.id === sched.connection_id);
      if (!conn) continue;
      if (!this.cronTasks.has(sched.id)) {
        this._startCronTask(sched.id, conn.name, { cron: sched.cron, ...callbacks });
      }
    }
  }

  stopAllSchedules(): void {
    for (const [, task] of this.cronTasks) task.stop();
    this.cronTasks.clear();
  }

  private _startCronTask(scheduleId: string, connectionName: string, config: ScheduleConfig): void {
    const task = cron.schedule(config.cron, async () => {
      try {
        const result = await this.backup(connectionName);
        await this.db.updateSchedule(scheduleId, { last_run: new Date().toISOString() });
        config.onBackup?.(result);
      } catch (e: unknown) {
        const err = e instanceof Error ? e : new Error(String(e));
        config.onError?.(err);
      }
    });
    this.cronTasks.set(scheduleId, task);
  }

  destroy(): void {
    this.stopAllSchedules();
    this.db.close();
  }
}
