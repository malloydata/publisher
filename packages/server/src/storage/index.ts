import { DuckDBStorageAdapter  } from "./duckdb-storage-adapter";
import path from "path";
import fs from "fs";

export function createStorage(serverRoot: string) {
  const dbPath = path.join(serverRoot, "storage/publisher_state.duckdb");
  const shouldInit = process.env.INITIALIZE_STORAGE === "true";

  if (shouldInit && fs.existsSync(dbPath)) {
    fs.rmSync(dbPath);
  }

  return new DuckDBStorageAdapter(dbPath);
}
