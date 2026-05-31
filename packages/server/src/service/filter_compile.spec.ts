import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Model } from "./model";

/**
 * End-to-end compile + execute matrix for filter injection.
 *
 * Each case takes a `query` string the user might write (with shapes that
 * caused compile failures with the old tail-`+ {where:...}` injection) and
 * confirms that the new source-extending injection produces a query that
 * compiles and returns the correctly filtered rows.
 *
 * Failure modes the matrix catches:
 *   - Inline multi-stage pipeline that drops the filtered dimension after
 *     stage 1 (James's bug).
 *   - Refinement appended after a `limit` clause (Adam's bug umbrella).
 *   - Pipeline stages with `calculate:` window functions that strip fields.
 */

const TEST_DIR = path.join(os.tmpdir(), "filter-compile-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

const SEED_SQL = `
CREATE TABLE IF NOT EXISTS orders (
   order_id INTEGER,
   region VARCHAR,
   status VARCHAR,
   customer_id VARCHAR,
   amount DOUBLE
);
INSERT INTO orders VALUES
   (1, 'US', 'active', 'cust_a', 100.0),
   (2, 'US', 'active', 'cust_a', 200.0),
   (3, 'EU', 'active', 'cust_b', 150.0),
   (4, 'EU', 'cancelled', 'cust_b', 75.0),
   (5, 'APAC', 'active', 'cust_c', 300.0),
   (6, 'APAC', 'cancelled', 'cust_c', 50.0);
`;

const MODEL = `
#(filter) name=region dimension=region type=in
#(filter) name=status dimension=status type=equal
source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure:
      order_count is count()
      total_amount is sum(amount)

   view: by_region_with_limit is {
      group_by: region
      aggregate: order_count
      limit: 10
   }

   view: pipelined_view is {
      group_by: region
      aggregate: order_count
   } -> {
      select: region, order_count
      order_by: region
   }
}
`;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
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

type Row = Record<string, unknown>;
const asRows = (compactResult: unknown): Row[] => compactResult as Row[];

describe("filter compile matrix", () => {
   beforeEach(async () => {
      await fs.writeFile(path.join(TEST_PKG_DIR, "orders.malloy"), MODEL);
   });

   afterEach(async () => {
      const files = await fs.readdir(TEST_PKG_DIR);
      for (const f of files) {
         if (f.endsWith(".malloy")) {
            await fs.unlink(path.join(TEST_PKG_DIR, f));
         }
      }
   });

   async function loadModel() {
      return await Model.create(
         "test-pkg",
         TEST_PKG_DIR,
         "orders.malloy",
         getConnections(),
      );
   }

   it("named view + filter compiles and filters correctly", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> by_region_with_limit",
         { region: ["US"] },
      );
      const r = asRows(compactResult);
      expect(r.length).toBe(1);
      expect(r[0].region).toBe("US");
      expect(Number(r[0].order_count)).toBe(2);
   });

   it("ad-hoc query with internal limit + filter compiles", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> { group_by: region; aggregate: order_count; limit: 10 }",
         { status: "active" },
      );
      const r = asRows(compactResult);
      const total = r.reduce((acc, row) => acc + Number(row.order_count), 0);
      expect(total).toBe(4);
   });

   it("ad-hoc query with +{limit:N} refinement on view + filter compiles", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> by_region_with_limit + {limit: 2}",
         { status: "active" },
      );
      const r = asRows(compactResult);
      expect(r.length).toBeLessThanOrEqual(2);
      const total = r.reduce((acc, row) => acc + Number(row.order_count), 0);
      expect(total).toBeGreaterThan(0);
   });

   it("inline multi-stage pipeline where the filter dimension never appears in any stage still applies filter", async () => {
      // Filter targets `status`, but no stage projects it. With the old tail
      // `+ {where: status = ...}` injection, the refinement attaches to the
      // last stage's output (which has no `status`) and compilation fails with
      // "'status' is not defined". Source-level extend hoists the predicate
      // to the source, where `status` is in scope.
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> { group_by: region; aggregate: order_count } -> { select: region, order_count }",
         { status: "active" },
      );
      const r = asRows(compactResult);
      const total = r.reduce((acc, row) => acc + Number(row.order_count), 0);
      expect(total).toBe(4);
   });

   it("inline pipeline with calculate window function still applies filter", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> { group_by: region; aggregate: order_count; calculate: rk is rank() } -> { select: rk, order_count }",
         { status: "active" },
      );
      const r = asRows(compactResult);
      // 3 active regions (US, EU, APAC), each gets a rank.
      expect(r.length).toBe(3);
      const totalActive = r.reduce(
         (acc, row) => acc + Number(row.order_count),
         0,
      );
      expect(totalActive).toBe(4);
   });

   it("named view containing internal pipeline + filter compiles", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "run: orders -> pipelined_view",
         { status: "active" },
      );
      const r = asRows(compactResult);
      const totalActive = r.reduce(
         (acc, row) => acc + Number(row.order_count),
         0,
      );
      expect(totalActive).toBe(4);
   });

   it("query with sourceName+queryName path applies filter", async () => {
      const model = await loadModel();
      const { compactResult } = await model.getQueryResults(
         "orders",
         "by_region_with_limit",
         undefined,
         { region: ["EU", "APAC"] },
      );
      const r = asRows(compactResult);
      const regions = r.map((row) => row.region).sort();
      expect(regions).toEqual(["APAC", "EU"]);
   });
});
