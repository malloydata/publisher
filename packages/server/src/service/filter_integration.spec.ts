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
import { BadRequestError } from "../errors";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "filter-integration-tests");
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

const MODEL_WITH_REQUIRED = `
#(filter) dimension=region type=in
#(filter) dimension=status type=equal
#(filter) name=tenant dimension=customer_id type=equal implicit required
source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure:
      order_count is count()
      total_amount is sum(amount)

   view: summary is {
      aggregate: order_count, total_amount
   }

   view: by_region is {
      group_by: region
      aggregate: order_count, total_amount
      order_by: region
   }
}
`;

const MODEL_OPTIONAL_ONLY = `
#(filter) dimension=region type=in
#(filter) dimension=status type=equal
source: orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure:
      order_count is count()
      total_amount is sum(amount)

   view: summary is {
      aggregate: order_count, total_amount
   }

   view: by_region is {
      group_by: region
      aggregate: order_count, total_amount
      order_by: region
   }
}
`;

const NOTEBOOK_MALLOYNB = `>>>markdown
# Test Notebook

>>>malloy
import "orders_optional.malloy"

>>>malloy
run: orders -> summary
`;

// Base source with 3 filters: region (in), status (equal), customer_id (equal, required)
const MODEL_BASE_FOR_EXTEND = `
#(filter) name=region dimension=region type=in
#(filter) name=status dimension=status type=equal
#(filter) name=tenant dimension=customer_id type=equal required
source: base_orders is duckdb.table('orders') extend {
   primary_key: order_id

   measure:
      order_count is count()
      total_amount is sum(amount)

   view: summary is {
      aggregate: order_count, total_amount
   }
}
`;

// Extending source: overrides region (in → equal), overrides tenant
// (removes required), keeps status from base unchanged
const MODEL_CHILD_EXTEND = `
import "base_orders.malloy"

#(filter) name=region dimension=region type=equal
#(filter) name=tenant dimension=customer_id type=equal
source: child_orders is base_orders extend {}
`;

// Notebook against the extended source
const NOTEBOOK_EXTEND = `>>>markdown
# Extend Test

>>>malloy
import "child_orders.malloy"

>>>malloy
run: child_orders -> summary
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

async function writeFile(filename: string, content: string): Promise<void> {
   await fs.writeFile(path.join(TEST_PKG_DIR, filename), content, "utf-8");
}

type Row = Record<string, unknown>;

/**
 * Malloy's compactResult (queryResults.data.value) is the raw array of row objects.
 */
function asRows(compactResult: unknown): Row[] {
   return compactResult as Row[];
}

/**
 * Parse a notebook cell result (JSON-stringified Malloy result).
 * The shape is: { schema, data: { kind, array_value: [{ record_value: { field_name: {kind, ...value} } }, ...] }, ... }
 * We extract column values from the record structure.
 */
function parseNotebookResult(resultJson: string): Row[] {
   const parsed = JSON.parse(resultJson);
   const arrayValue = parsed?.data?.array_value;
   if (!Array.isArray(arrayValue)) {
      throw new Error(
         `Cannot extract rows from notebook result: ${JSON.stringify(Object.keys(parsed?.data ?? {}))}`,
      );
   }

   const schema = parsed.schema?.fields ?? [];
   const fieldNames = schema.map((f: { name: string }) => f.name);

   return arrayValue.map(
      (record: { record_value?: Array<Record<string, unknown>> }) => {
         const row: Row = {};
         const cells = record.record_value ?? [];
         for (let i = 0; i < fieldNames.length; i++) {
            const cell = cells[i];
            if (!cell) continue;
            row[fieldNames[i]] =
               cell.number_value ??
               cell.string_value ??
               cell.boolean_value ??
               cell.timestamp_value ??
               null;
         }
         return row;
      },
   );
}

describe("filter integration", () => {
   beforeEach(async () => {
      await writeFile("orders.malloy", MODEL_WITH_REQUIRED);
      await writeFile("orders_optional.malloy", MODEL_OPTIONAL_ONLY);
      await writeFile("test_notebook.malloynb", NOTEBOOK_MALLOYNB);
   });

   afterEach(async () => {
      const files = await fs.readdir(TEST_PKG_DIR);
      for (const f of files) {
         if (f.endsWith(".malloy") || f.endsWith(".malloynb")) {
            await fs.unlink(path.join(TEST_PKG_DIR, f));
         }
      }
   });

   // -----------------------------------------------------------------------
   // Model loading & filter metadata
   // -----------------------------------------------------------------------
   describe("model loading", () => {
      it("parses filter annotations and exposes them via getSources()", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         expect(sources).toBeDefined();
         expect(sources!.length).toBeGreaterThanOrEqual(1);

         const ordersSource = sources!.find((s) => s.name === "orders");
         expect(ordersSource).toBeDefined();
         expect(ordersSource!.filters).toBeDefined();
         expect(ordersSource!.filters!.length).toBe(3);

         const regionFilter = ordersSource!.filters!.find(
            (f) => f.dimension === "region",
         );
         expect(regionFilter).toBeDefined();
         expect(regionFilter!.type).toBe("in");
         expect(regionFilter!.required).toBe(false);
         expect(regionFilter!.implicit).toBe(false);

         const statusFilter = ordersSource!.filters!.find(
            (f) => f.dimension === "status",
         );
         expect(statusFilter).toBeDefined();
         expect(statusFilter!.type).toBe("equal");
         expect(statusFilter!.required).toBe(false);

         const tenantFilter = ordersSource!.filters!.find(
            (f) => f.dimension === "customer_id",
         );
         expect(tenantFilter).toBeDefined();
         expect(tenantFilter!.name).toBe("tenant");
         expect(tenantFilter!.type).toBe("equal");
         expect(tenantFilter!.implicit).toBe(true);
         expect(tenantFilter!.required).toBe(true);
      });

      it("loads a model with optional-only filters", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         const ordersSource = sources!.find((s) => s.name === "orders");
         expect(ordersSource!.filters!.length).toBe(2);
         expect(ordersSource!.filters!.every((f) => f.required === false)).toBe(
            true,
         );
      });
   });

   // -----------------------------------------------------------------------
   // Query execution with optional filters
   // -----------------------------------------------------------------------
   describe("query execution with optional filters", () => {
      it("runs unfiltered query (no filterParams provided)", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(6);
         expect(Number(r[0].total_amount)).toBe(875);
      });

      it("applies region=in filter with single value", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { region: ["US"] },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(2);
         expect(Number(r[0].total_amount)).toBe(300);
      });

      it("applies region=in filter with multiple values", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { region: ["US", "EU"] },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(4);
         expect(Number(r[0].total_amount)).toBe(525);
      });

      it("applies status=equal filter", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { status: "active" },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(4);
         expect(Number(r[0].total_amount)).toBe(750);
      });

      it("applies combined region + status filters", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { region: ["EU"], status: "cancelled" },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(1);
         expect(Number(r[0].total_amount)).toBe(75);
      });

      it("works with by_region view and filters", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "by_region",
            undefined,
            { status: "cancelled" },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(2);
         const regions = r.map((row) => row.region);
         expect(regions).toContain("EU");
         expect(regions).toContain("APAC");
      });

      it("works with ad-hoc query string and filters", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            undefined,
            undefined,
            "run: orders -> { aggregate: order_count is count() }",
            { region: ["APAC"] },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(2);
      });
   });

   // -----------------------------------------------------------------------
   // Required filter enforcement
   // -----------------------------------------------------------------------
   describe("required filter enforcement", () => {
      it("throws when required filter is missing", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         await expect(
            model.getQueryResults("orders", "summary", undefined, {
               region: ["US"],
            }),
         ).rejects.toThrow(BadRequestError);
      });

      it("throws descriptive error for missing required filter", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         try {
            await model.getQueryResults("orders", "summary", undefined, {});
            throw new Error("Should have thrown");
         } catch (error) {
            expect(error).toBeInstanceOf(BadRequestError);
            expect((error as Error).message).toContain("tenant");
         }
      });

      it("succeeds when required filter is provided", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { tenant: "cust_a", region: ["US"] },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(2);
         expect(Number(r[0].total_amount)).toBe(300);
      });

      it("applies required + optional filters together", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { tenant: "cust_b", status: "cancelled" },
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(1);
         expect(Number(r[0].total_amount)).toBe(75);
      });
   });

   // -----------------------------------------------------------------------
   // bypassFilters
   // -----------------------------------------------------------------------
   describe("bypassFilters", () => {
      it("skips required filter validation when bypassFilters=true", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            {},
            true,
         );

         const r = asRows(compactResult);
         expect(r.length).toBe(1);
         expect(Number(r[0].order_count)).toBe(6);
         expect(Number(r[0].total_amount)).toBe(875);
      });

      it("ignores provided filters when bypassFilters=true", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "orders_optional.malloy",
            getConnections(),
         );

         const { compactResult } = await model.getQueryResults(
            "orders",
            "summary",
            undefined,
            { region: ["US"] },
            true,
         );

         const r = asRows(compactResult);
         expect(Number(r[0].order_count)).toBe(6);
      });
   });

   // -----------------------------------------------------------------------
   // Notebook cell execution with filters
   // -----------------------------------------------------------------------
   describe("notebook cell execution", () => {
      it("executes notebook code cell without filters", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "test_notebook.malloynb",
            getConnections(),
         );

         // Cell 0 = markdown ("# Test Notebook")
         // Cell 1 = code (model definition — no query, just source)
         // Cell 2 = code (run: orders -> summary)
         const codeCell = await model.executeNotebookCell(2);
         expect(codeCell.type).toBe("code");
         expect(codeCell.result).toBeDefined();

         const notebookRows = parseNotebookResult(codeCell.result!);
         expect(notebookRows.length).toBe(1);
         expect(Number(notebookRows[0].order_count)).toBe(6);
         expect(Number(notebookRows[0].total_amount)).toBe(875);
      });

      it("applies filterParams to notebook cell execution", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "test_notebook.malloynb",
            getConnections(),
         );

         const codeCell = await model.executeNotebookCell(2, {
            region: ["US"],
         });
         expect(codeCell.result).toBeDefined();

         const notebookRows = parseNotebookResult(codeCell.result!);
         expect(notebookRows.length).toBe(1);
         expect(Number(notebookRows[0].order_count)).toBe(2);
         expect(Number(notebookRows[0].total_amount)).toBe(300);
      });

      it("applies status filter to notebook cell execution", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "test_notebook.malloynb",
            getConnections(),
         );

         const codeCell = await model.executeNotebookCell(2, {
            status: "cancelled",
         });
         expect(codeCell.result).toBeDefined();

         const notebookRows = parseNotebookResult(codeCell.result!);
         expect(notebookRows.length).toBe(1);
         expect(Number(notebookRows[0].order_count)).toBe(2);
         expect(Number(notebookRows[0].total_amount)).toBe(125);
      });

      it("bypassFilters skips filter injection on notebook cells", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "test_notebook.malloynb",
            getConnections(),
         );

         const codeCell = await model.executeNotebookCell(
            2,
            { region: ["US"] },
            true,
         );
         expect(codeCell.result).toBeDefined();

         const notebookRows = parseNotebookResult(codeCell.result!);
         expect(notebookRows.length).toBe(1);
         expect(Number(notebookRows[0].order_count)).toBe(6);
      });

      it("returns markdown cells unchanged", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "test_notebook.malloynb",
            getConnections(),
         );

         const markdownCell = await model.executeNotebookCell(0);
         expect(markdownCell.type).toBe("markdown");
         expect(markdownCell.text).toContain("Test Notebook");
      });
   });

   // -----------------------------------------------------------------------
   // Extended source filter inheritance
   // -----------------------------------------------------------------------
   describe("extended source filter inheritance", () => {
      beforeEach(async () => {
         await writeFile("base_orders.malloy", MODEL_BASE_FOR_EXTEND);
         await writeFile("child_orders.malloy", MODEL_CHILD_EXTEND);
         await writeFile("extend_notebook.malloynb", NOTEBOOK_EXTEND);
      });

      it("inherits base-only filters on extended source", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         const child = sources!.find((s) => s.name === "child_orders");
         expect(child).toBeDefined();
         expect(child!.filters).toBeDefined();

         // status is defined only on the base — it should carry through
         const statusFilter = child!.filters!.find(
            (f) => f.name === "status",
         );
         expect(statusFilter).toBeDefined();
         expect(statusFilter!.type).toBe("equal");
      });

      it("child overrides base filter type", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         const child = sources!.find((s) => s.name === "child_orders");
         expect(child).toBeDefined();

         // region: base=in, child overrides to equal
         const regionFilter = child!.filters!.find(
            (f) => f.name === "region",
         );
         expect(regionFilter).toBeDefined();
         expect(regionFilter!.type).toBe("equal");
      });

      it("child can remove required flag by overriding", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         const child = sources!.find((s) => s.name === "child_orders");
         expect(child).toBeDefined();

         // tenant: base=required, child overrides without required
         const tenantFilter = child!.filters!.find(
            (f) => f.name === "tenant",
         );
         expect(tenantFilter).toBeDefined();
         expect(tenantFilter!.required).toBeFalsy();
      });

      it("has exactly the expected merged filter set", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         const sources = model.getSources();
         const child = sources!.find((s) => s.name === "child_orders");
         expect(child).toBeDefined();

         // 3 unique filter names: region, status (from base), tenant
         const filterNames = child!.filters!.map((f) => f.name).sort();
         expect(filterNames).toEqual(["region", "status", "tenant"]);
      });

      it("applies inherited filter to query on extended source", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         // status=active is inherited from the base; should work on child
         const { compactResult } = await model.getQueryResults(
            "child_orders",
            "summary",
            undefined,
            { status: "active" },
         );
         const rows = asRows(compactResult);
         expect(rows.length).toBe(1);
         // 4 active rows: US(2), EU(1), APAC(1)
         expect(Number(rows[0].order_count)).toBe(4);
      });

      it("applies overridden filter to query on extended source", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         // region is overridden to type=equal on the child
         const { compactResult } = await model.getQueryResults(
            "child_orders",
            "summary",
            undefined,
            { region: "US" },
         );
         const rows = asRows(compactResult);
         expect(rows.length).toBe(1);
         // 2 US rows
         expect(Number(rows[0].order_count)).toBe(2);
      });

      it("no longer requires base's required filter after child override", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "child_orders.malloy",
            getConnections(),
         );

         // On the base, tenant is required. On the child, it's not.
         // Running without tenant should NOT throw.
         const { compactResult } = await model.getQueryResults(
            "child_orders",
            "summary",
         );
         const rows = asRows(compactResult);
         expect(rows.length).toBe(1);
         expect(Number(rows[0].order_count)).toBe(6);
      });

      it("applies inherited filters to notebook cells", async () => {
         const model = await Model.create(
            "test-pkg",
            TEST_PKG_DIR,
            "extend_notebook.malloynb",
            getConnections(),
         );

         // Apply status=cancelled (inherited from base) via notebook cell
         const codeCell = await model.executeNotebookCell(2, {
            status: "cancelled",
         });
         expect(codeCell.result).toBeDefined();

         const rows = parseNotebookResult(codeCell.result!);
         expect(rows.length).toBe(1);
         // 2 cancelled rows: EU(1), APAC(1)
         expect(Number(rows[0].order_count)).toBe(2);
      });
   });
});
