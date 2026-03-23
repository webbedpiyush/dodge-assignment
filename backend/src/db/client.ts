import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { Kysely, SqliteDialect } from "kysely";
import { env } from "../config.js";

let sqliteInstance: Database.Database | null = null;
let kyselyInstance: Kysely<unknown> | null = null;

function ensureDbDirectory(dbPath: string): void {
  const dir = path.dirname(dbPath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

export function getSqlite(): Database.Database {
  if (!sqliteInstance) {
    ensureDbDirectory(env.SQLITE_PATH);
    sqliteInstance = new Database(env.SQLITE_PATH);
    sqliteInstance.pragma("journal_mode = WAL");
  }
  return sqliteInstance;
}

export function getKysely(): Kysely<unknown> {
  if (!kyselyInstance) {
    const sqlite = getSqlite();
    kyselyInstance = new Kysely({
      dialect: new SqliteDialect({ database: sqlite }),
    });
  }
  return kyselyInstance;
}
