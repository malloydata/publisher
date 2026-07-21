// End-to-end routing contract for the `storage=` virtual-source serve path,
// exercised through the real Model.getQueryResults against a real in-memory
// DuckDB. The ORIGINAL source returns 0; the serve binding points at a table
// returning 60 — so the value observed proves whether the query was routed
// through the serve-shape transform (60) or served live from the original
// model (0). This pins the mode gate, the binding gate, and the safe fallback.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { Model } from "./model";
import type { ServeBinding } from "./materialization_serve_transform";

const ROOT = "file:///storage-serve-e2e/";
const QUERY = "run: X -> { aggregate: t is total.sum() }";
const BINDING: ServeBinding = {
   sourceName: "X",
   connectionName: "duckdb",
   virtualHandle: "h",
   tablePath: "mz_real",
   schema: [{ name: "total", type: "BIGINT" }],
};

/**
 * A Model whose original source `X` yields total=0, with a real DuckDB
 * connection carrying `mz_real` (total=60) that the serve binding rebinds to.
 */
async function buildModel(): Promise<Model> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   // DuckDB shares a process-global :memory: db by connection name, so a prior
   // test's table can linger — replace it.
   await duckdb.runSQL("CREATE OR REPLACE TABLE mz_real AS SELECT 60 AS total");
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);
   const originalText = `source: X is duckdb.sql("SELECT 0 AS total")`;
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, originalText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = (compiled as any)._modelDef;
   const modelInfo = modelDefToModelInfo(modelDef);
   const model = new Model(
      "pkg",
      "m.malloy",
      {},
      "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      mm as any,
      modelDef,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      modelInfo,
   );
   model.setServeMalloyConfig(connMap);
   return model;
}

async function runTotal(model: Model): Promise<number> {
   const res = await model.getQueryResults(
      undefined,
      undefined,
      QUERY,
      {},
      true,
   );
   // compactResult is the row value array: [{ t: <sum> }].
   const rows = res.compactResult as unknown as { t: number }[];
   return Number(rows[0].t);
}

describe("storage= serve routing (end-to-end)", () => {
   afterEach(() => {
      delete process.env.PERSIST_STORAGE_MODE;
   });

   it("mode=on + a binding routes the query to the materialized table", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(60);
   });

   it("mode=off serves live even with a binding present (kill switch)", async () => {
      process.env.PERSIST_STORAGE_MODE = "off";
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(0);
   });

   it("mode=write-only serves live (build-only rung)", async () => {
      process.env.PERSIST_STORAGE_MODE = "write-only";
      const model = await buildModel();
      model.setServeBindings([BINDING]);
      expect(await runTotal(model)).toBe(0);
   });

   it("mode=on with no bindings serves live", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      expect(await runTotal(model)).toBe(0);
   });

   it("falls back to live when the serve shape cannot compile (bad connection)", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      // A binding on a connection the config doesn't have — the serve-shape
      // compile throws, and the query must still succeed, served live.
      model.setServeBindings([{ ...BINDING, connectionName: "missing_conn" }]);
      expect(await runTotal(model)).toBe(0);
   });
});
