// Model-level routing contract for JOINS in the `storage=` serve path, through
// the real Model.getQueryResults against a real in-memory DuckDB. This exercises
// the glue the transform-level tests can't: serveBindingsWithRefinements reading
// a REAL compiled modelDef, mapping the join's `sourceID` to the joined source
// name, and lifting the join declaration verbatim from the on-disk source file
// (as it does in production — hence a real temp file, not InMemoryURLReader
// alone). The live sources return region_name 'LIVE'; the bound (materialized)
// tables return 'STORE', so the value observed tells which path ran.
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
}
`;
const QUERY =
   "run: orders -> { group_by: regions.region_name; aggregate: t is amount.sum() }";

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
   // Materialized tables (distinct region_name from the live sources).
   await duckdb.runSQL(
      "CREATE OR REPLACE TABLE orders_mz AS SELECT 10 AS amount, 'r1' AS region_id",
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

async function runRegionName(model: Model): Promise<string> {
   const res = await model.getQueryResults(
      undefined,
      undefined,
      QUERY,
      {},
      true,
   );
   const rows = res.compactResult as unknown as { region_name: string }[];
   return rows[0].region_name;
}

describe("storage= serve routing with joins (end-to-end)", () => {
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
      // 'STORE' => the join ran over the bound tables (regions_mz), not live.
      expect(await runRegionName(model)).toBe("STORE");
   });

   it("falls back to live when the joined source is not materialized (the gate)", async () => {
      process.env.PERSIST_STORAGE_MODE = "on";
      const model = await buildModel();
      // Only `orders` is bound; `regions` is not, so the join is not carried and
      // a query traversing it cannot compile against the serve shape → live.
      model.setServeBindings([ORDERS_BINDING]);
      expect(await runRegionName(model)).toBe("LIVE");
   });

   it("mode=off serves the join live even with both bindings present", async () => {
      process.env.PERSIST_STORAGE_MODE = "off";
      const model = await buildModel();
      model.setServeBindings([ORDERS_BINDING, REGIONS_BINDING]);
      expect(await runRegionName(model)).toBe("LIVE");
   });
});
