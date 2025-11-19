import duckdb from "duckdb";

export class DuckDBStorageAdapter {
  private db: duckdb.Database;
  private conn: duckdb.Connection;

  constructor(private filePath: string) {}

  async init() {
    this.db = new duckdb.Database(this.filePath);
    this.conn = this.db.connect();

    await new Promise<void>((resolve, reject) => {
      this.conn.run(
        `CREATE TABLE IF NOT EXISTS resource_state (
          key TEXT PRIMARY KEY,
          value TEXT
        );`,
        (err) => (err ? reject(err) : resolve())
      );
    });
  }

  async getState(key: string) {
    return await new Promise<any | null>((resolve, reject) => {
      this.conn.all(
        "SELECT value FROM resource_state WHERE key = ?",
        key, // ← single param as vararg, not [key]
        (err, rows) => {
          if (err) return reject(err);
          if (!rows || rows.length === 0) return resolve(null);

          try {
            resolve(JSON.parse((rows as any)[0].value));
          } catch {
            // Handle bad JSON from previous runs
            resolve(null);
          }
        }
      );
    });
  }

  async setState(key: string, value: any) {
    if (key === undefined || key === null) {
      throw new Error("DuckDBStorageAdapter.setState → key is missing");
    }
    if (value === undefined) {
      throw new Error("DuckDBStorageAdapter.setState → value is undefined");
    }

    const serialized = JSON.stringify(value);

    return await new Promise<void>((resolve, reject) => {
      this.conn.run(
        `INSERT INTO resource_state(key, value)
         VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        key,                // ← param 1
        serialized,         // ← param 2
        (err) => (err ? reject(err) : resolve())
      );
    });
  }

  async deleteState(key: string) {
    return await new Promise<void>((resolve, reject) => {
      this.conn.run(
        "DELETE FROM resource_state WHERE key = ?",
        key, // ← single param as vararg, not [key]
        (err) => (err ? reject(err) : resolve())
      );
    });
  }
}
