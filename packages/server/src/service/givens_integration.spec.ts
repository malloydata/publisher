import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "givens-integration-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS orders (
   order_id INTEGER,
   region VARCHAR,
   order_date DATE
);
INSERT INTO orders VALUES
   (1, 'US', '2024-01-15'),
   (2, 'EU', '2024-02-10'),
   (3, 'APAC', '2024-03-05');
`;

const MODEL_WITH_GIVENS = `
##! experimental.givens

given: region_filter :: string is 'US'
given: cutoff_date :: date is @2024-02-01

source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure: order_count is count()
}
`;

const MODEL_WITHOUT_GIVENS = `
source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure: order_count is count()
}
`;

const MODEL_WITH_ANNOTATED_GIVEN = `
##! experimental.givens

#(doc) Region code, e.g. US, EU
#(label) Region
given: region_filter :: string is 'US'

source: orders is duckdb.table('orders') extend {
   primary_key: order_id
}
`;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
   // Each fixture lives in its own file. Tests share `beforeAll` for harness
   // setup but never edit these files at runtime, so no `beforeEach` /
   // `afterEach` cleanup is needed.
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders.malloy"),
      MODEL_WITH_GIVENS,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders_no_givens.malloy"),
      MODEL_WITHOUT_GIVENS,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders_annotated.malloy"),
      MODEL_WITH_ANNOTATED_GIVEN,
      "utf-8",
   );
});

afterAll(async () => {
   try {
      await duckdbConnection.close();
      await new Promise((resolve) => setTimeout(resolve, 100));
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // Ignore cleanup errors
   }
});

function getConnections(): Map<string, Connection> {
   const map = new Map<string, Connection>();
   map.set("duckdb", duckdbConnection);
   return map;
}

describe("givens introspection", () => {
   it("surfaces declared givens on the compiled-model response", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      expect(compiledModel.givens).toBeDefined();
      expect(compiledModel.givens).toHaveLength(2);

      const byName = new Map(
         (compiledModel.givens ?? []).map((g) => [g.name, g]),
      );
      const region = byName.get("region_filter");
      const cutoff = byName.get("cutoff_date");

      expect(region).toBeDefined();
      expect(region?.type).toBe("string");
      expect(cutoff).toBeDefined();
      expect(cutoff?.type).toBe("date");
   });

   it("attaches the model-level givens list to every source", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders.malloy",
         getConnections(),
      );

      const sources = model.getSources();
      expect(sources).toBeDefined();
      expect(sources).toHaveLength(1);

      const ordersSource = sources?.[0];
      expect(ordersSource?.name).toBe("orders");
      expect(ordersSource?.givens).toBeDefined();
      expect(ordersSource?.givens).toHaveLength(2);

      const names = (ordersSource?.givens ?? []).map((g) => g.name).sort();
      expect(names).toEqual(["cutoff_date", "region_filter"]);
   });

   it("returns undefined for givens when the model declares none", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders_no_givens.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      // Absent rather than empty: matches how `sources`/`queries` behave when
      // there are none, and lets OpenAPI clients distinguish "feature
      // unsupported" from "supported but no declarations."
      expect(compiledModel.givens).toBeUndefined();
      expect(model.getSources()?.[0]?.givens).toBeUndefined();
   });

   it("surfaces only `#(...)` annotations, not pragmas or doc comments", async () => {
      const model = await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders_annotated.malloy",
         getConnections(),
      );

      const compiledModel = await model.getModel();

      expect(compiledModel.givens).toHaveLength(1);
      const region = compiledModel.givens?.[0];
      expect(region?.name).toBe("region_filter");

      // The given declares two app-route annotations (`#(doc)`, `#(label)`).
      // Only app routes land on the wire; Malloy-reserved routes — the
      // model-level `##!` pragma, plain `#` tags, `#"` doc strings — must
      // not leak onto the given's surface.
      const annotations = region?.annotations ?? [];
      expect(annotations.length).toBeGreaterThanOrEqual(2);
      expect(annotations.some((a) => a.startsWith("##"))).toBe(false);
      expect(annotations.some((a) => a.startsWith('#"'))).toBe(false);
   });
});
