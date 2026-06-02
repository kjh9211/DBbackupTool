export type DbType = 'mysql' | 'postgres' | 'mongodb' | 'mssql' | 'sqlite';

export interface Connection {
  id: string;
  name: string;
  type: DbType;
  host?: string;
  port?: number;
  database: string;
  username?: string;
  password?: string;
  extra?: Record<string, unknown>;
  created_at: string;
}

export interface BackupAdapter {
  backup(conn: Connection, outPath: string): Promise<void>;
  restore(conn: Connection, filePath: string): Promise<void>;
  test(conn: Connection): Promise<boolean>;
}
