import { DuckDBStorageAdapter } from "./duckdb-storage-adapter";
import path from "path";
import fs from "fs";

export function createStorage(serverRoot: string) {
  const storageDir = path.join(serverRoot, "storage");
  if (!fs.existsSync(storageDir)) {
    fs.mkdirSync(storageDir, { recursive: true });
  }

  const dbPath = path.join(storageDir, "publisher_state.duckdb");
  const shouldInit = process.env.INITIALIZE_STORAGE === "true";

  if (shouldInit && fs.existsSync(dbPath)) {
    fs.rmSync(dbPath);
  }

  return new DuckDBStorageAdapter(dbPath);
}
