/**
 * Worker thread that owns one capped DuckDB connection and answers
 * schema-introspection requests for parquet/csv files. Running this
 * off the main thread isolates the native DuckDB thread pool — when
 * the worker exits, its threads die with it, which puts a hard
 * ceiling on the leak class that OOM-killed prod
 * (worker-76b49bdb89-8bsv4: 466 leaked Bun Pool threads).
 *
 * Protocol (parent ↔ worker):
 *   parent → worker:  { id, packagePath, databasePath }
 *   worker → parent:  { id, ok: true,  result: SchemaResult }
 *                  |  { id, ok: false, error: { message, stack? } }
 *
 * One request at a time per worker — the pool in the parent
 * (`schema_worker_pool.ts`) handles fan-out. Keeping the worker
 * single-threaded from the JS side matches DuckDB's behavior on a
 * single connection and avoids head-of-line blocking inside the
 * worker itself.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import "@malloydata/db-duckdb/native";
import {
   ConnectionRuntime,
   EmptyURLReader,
   SourceDef,
} from "@malloydata/malloy";
import * as path from "path";
import { parentPort } from "worker_threads";

export interface SchemaRequest {
   id: number;
   packagePath: string;
   databasePath: string;
}

export interface SchemaResponse {
   id: number;
   ok: boolean;
   result?: {
      name: string;
      rowCount: number;
      columns: Array<{ type: string; name: string }>;
   };
   error?: { message: string; stack?: string };
}

if (!parentPort) {
   // Defensive: schema_worker.ts must only be loaded as a worker. If
   // someone accidentally imports it from the main thread the
   // connection below would still allocate its native pool there,
   // recreating the exact leak this file exists to fix.
   throw new Error("schema_worker.ts loaded outside a worker thread");
}

// One DuckDB connection per worker, capped tight. Schema introspection
// reads parquet footers / csv headers — it does not need parallelism
// or a large memory arena. The cap is what keeps the per-worker
// native-thread cost bounded.
const connection = new DuckDBConnection({
   name: "duckdb",
   databasePath: ":memory:",
   threads: 1,
   memoryLimit: "256MB",
});

async function handleRequest(req: SchemaRequest): Promise<SchemaResponse> {
   try {
      const fullPath = path.join(req.packagePath, req.databasePath);
      // DuckDB on Windows supports forward slashes, and this avoids
      // escaping issues in the inline SQL below.
      const normalizedPath = fullPath.replace(/\\/g, "/");

      const runtime = new ConnectionRuntime({
         urlReader: new EmptyURLReader(),
         connections: [connection],
      });
      const model = runtime.loadModel(
         `source: temp is duckdb.table('${normalizedPath}')`,
      );
      const modelDef = await model.getModel();
      const fields = (modelDef._modelDef.contents["temp"] as SourceDef).fields;
      const columns = fields.map((field) => ({
         type: String(field.type),
         name: field.name,
      }));

      const runner = model.loadQuery(
         "run: temp->{aggregate: row_count is count()}",
      );
      const result = await runner.run();
      const rowCount = result.data.value[0].row_count?.valueOf() as number;

      return {
         id: req.id,
         ok: true,
         result: { name: req.databasePath, rowCount, columns },
      };
   } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      return {
         id: req.id,
         ok: false,
         error: { message: error.message, stack: error.stack },
      };
   }
}

parentPort.on("message", async (msg: SchemaRequest) => {
   const response = await handleRequest(msg);
   parentPort!.postMessage(response);
});

// On any termination signal, close the connection so DuckDB releases
// its native threads cleanly instead of leaking them past worker exit.
const shutdown = async () => {
   try {
      await connection.close();
   } catch {
      // best effort
   }
   process.exit(0);
};
parentPort.on("close", () => void shutdown());
