// Read one query's result from a stopped server's publisher.db, as JSON.
// Usage: bun run dbq.ts <publisher.db> "<sql>"
import { DuckDBInstance } from "@duckdb/node-api";

const [file, sql] = process.argv.slice(2);
const instance = await DuckDBInstance.create(file, { access_mode: "READ_ONLY" });
const conn = await instance.connect();
const reader = await conn.runAndReadAll(sql);
console.log(JSON.stringify(reader.getRowObjectsJson()));
