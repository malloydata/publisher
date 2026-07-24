// Model-level routing contract for JOINS and VIEWS in the `storage=` serve path,
// through the real Model.getQueryResults against a real in-memory DuckDB. This
// exercises the glue the transform-level tests can't: serveBindingsWithRefinements
// reading a REAL compiled modelDef, mapping a join's `sourceID` to the joined
// source name, and lifting join/view declarations verbatim from the on-disk
// source file (as it does in production — hence a real temp file, not
// InMemoryURLReader alone), plus the shape-compile escalation that keeps one
// un-carriable refinement from disabling all storage serving.
//
// Signals: the live sources return region_name 'LIVE' and amount 10; the bound
// (materialized) tables return 'STORE' and amount 99 — so the value observed
// tells which path ran.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { pathToFileURL } from "url";
import { Model } from "./model";
import type { ServeBinding } from "./materialization_serve_transform";

const MODEL_SRC = `source: regions is duckdb.sql("SELECT 'r1' AS region_id, 'LIVE' AS region_name")
source: orders is duckdb.sql("SELECT 10 AS amount, 'r1' AS region_id") extend {
  join_one: regions is regions on region_id = regions.region_id
  measure: total is amount.sum()
  view: by_region is { group_by: regions.region_name; aggregate: total }
}
`;

const ORDERS_BINDING: ServeBinding = {
   sourceName: "orders",
   connectionName: "duckdb",
   virtualHandle: "orders_h",
   tablePath: "orders_mz",
   schema: [
      { name: "amount", type: "BIGINT" },
      { name: "region_id", type: "VARCHAR" },
   ],
};
const REGIONS_BINDING: ServeBinding = {
   sourceName: "regions",
   connectionName: "duckdb",
   virtualHandle: "regions_h",
   tablePath: "regions_mz",
   schema: [
      { name: "region_id", type: "VARCHAR" },
      { name: "region_name", type: "VARCHAR" },
   ],
};

const tmpDirs: string[] = [];

async function buildModel(): Promise<Model> {
   const dir = mkdtempSync(join(tmpdir(), "mz-join-"));
   tmpDirs.push(dir);
   const file = join(dir, "m.malloy");
   writeFileSync(file, MODEL_SRC);
   const fileUrl = pathToFileURL(file).toString();

   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   // Materialized tables carry distinct values (region_name 'STORE', amount 99)
   // from the live sources ('LIVE', 10), so a result value proves which ran.
   await duckdb.runSQL(
      "CREATE OR REPLACE TABLE orders_mz AS SELECT 99 AS amount, 'r1' AS region_id",
   );
   await duckdb.runSQL(
      "CREATE OR REPLACE TABLE regions_mz AS SELECT 'r1' AS region_id, 'STORE' AS region_name",
   );
   const connMap = new Map<string, DuckDBConnection>([["duckdb", duckdb]]);

   // Register the source under its real file:// URL so the compiled modelDef's
   // locations point at the file liftText reads back from disk.
   const urlReader = new InMemoryURLReader(new Map([[fileUrl, MODEL_SRC]]));
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connMap, "duckdb"),
   });
   const mm = runtime.loadModel(new URL(fileUrl), {
      importBaseURL: new URL(".", fileUrl),
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

/** Run a query and return the first result row. */
async function runRow<T>(model: Model, query: string): Promise<T> {
   const res = await model.getQueryResults(
      undefined,
      undefined,
      query,
      {},
      true,
   );
   return (res.compactResult as unknown as T[])[0];
}

const JOIN_QUERY =
   "run: orders -> { group_by: regions.region_name; aggregate: t is amount.sum() }";
const VIEW_QUERY = "run: orders -> by_region";
const PLAIN_QUERY = "run: orders -> { aggregate: t is amount.sum() }";

describe("storage= serve routing with joins and views (end-to-end)", () => {
   afterEach(() => {
      delete process.env.PERSIST_STORAGE_MODE;
   });
   afterAll(() => {
      for (const d of tmpDirs) rmSync(d, { recursive: true, force: true });
   });

   it("serves a join query from the materialized tables when both sources are bound", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      model.setServeBindings([ORDERS_BINDING, REGIONS_BINDING]);
      const row = await runRow<{ region_name: string }>(model, JOIN_QUERY);
      expect(row.region_name).toBe("STORE"); // join ran over the bound tables
   });

   it("serves a named view (grouping by a joined field) from the materialized tables", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      model.setServeBindings([ORDERS_BINDING, REGIONS_BINDING]);
      const row = await runRow<{ region_name: string; total: number }>(
         model,
         VIEW_QUERY,
      );
      expect(row.region_name).toBe("STORE");
      expect(Number(row.total)).toBe(99); // amount from orders_mz, not live 10
   });

   it("falls back to live when the joined source is not materialized (the gate)", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      // Only `orders` is bound; the join to `regions` is not carried, so a query
      // traversing it cannot compile against the serve shape → live.
      model.setServeBindings([ORDERS_BINDING]);
      expect(
         (await runRow<{ region_name: string }>(model, JOIN_QUERY)).region_name,
      ).toBe("LIVE");
   });

   it("a view that reaches a non-materialized join does not disable base serving (escalation)", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      // Only `orders` bound: the join is gated out, so the emitted `by_region`
      // view references an absent join and the RICH shape fails to compile. The
      // escalation must drop the view category and keep serving base + measures,
      // rather than losing all storage serving. A plain aggregate must still be
      // served from storage (amount 99), while the view query falls back to live.
      model.setServeBindings([ORDERS_BINDING]);
      expect(Number((await runRow<{ t: number }>(model, PLAIN_QUERY)).t)).toBe(
         99,
      );
      expect(
         (await runRow<{ region_name: string }>(model, VIEW_QUERY)).region_name,
      ).toBe("LIVE");
   });

   it("mode=off serves live even with both bindings present", async () => {
      process.env.PERSIST_STORAGE_MODE = "off";
      const model = await buildModel();
      model.setServeBindings([ORDERS_BINDING, REGIONS_BINDING]);
      expect(
         (await runRow<{ region_name: string }>(model, JOIN_QUERY)).region_name,
      ).toBe("LIVE");
   });
});
