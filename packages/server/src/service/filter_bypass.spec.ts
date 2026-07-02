/**
 * Filter-param (`#(filter)`) enforcement across source-derivation paths.
 *
 * A `#(filter)` annotation marks a source as protected: a required filter (e.g.
 * the implicit multi-tenant `org_id` boundary) must be supplied before the
 * source can be read. The aim is to enforce that without narrowing the query
 * shapes `execute_query` accepts.
 *
 * Cases guarded here:
 *   - Direct read of a protected source: rejected (400) naming the missing
 *     required filter; scoped to the supplied values once provided.
 *   - Reaching a protected source under an ad-hoc name (alias / extend / chain):
 *     enforced identically — the query still runs, scoped — rather than being
 *     refused for its shape.
 *   - Unprotected sources are unaffected; `bypassFilters` skips enforcement.
 *
 * Language-level escapes (import, raw tables, raw SQL) are out of scope here and
 * covered in `restricted_mode.spec.ts`.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { BadRequestError } from "../errors";
import { Model } from "./model";
import type { FilterParams } from "./filter";

const TEST_DIR = path.join(os.tmpdir(), "filter-bypass-tests");
const TEST_DB_DIR = path.join(TEST_DIR, "db");
const TEST_DB_PATH = path.join(TEST_DB_DIR, "test.duckdb");
const TEST_PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdbConnection: DuckDBConnection;

// Three tenants. org_id is the partition key, so any cross-org leak shows up
// directly as extra rows when we `group_by: org_id` (acme has 2 products).
const SEED_SQL = `
CREATE TABLE IF NOT EXISTS products (
   org_id VARCHAR,
   product_name VARCHAR
);
INSERT INTO products VALUES
   ('acme', 'AcmeAnvil'),
   ('acme', 'AcmeRocket'),
   ('globex', 'GlobexPhone'),
   ('initech', 'InitechStapler');

CREATE TABLE IF NOT EXISTS orders (
   org_id VARCHAR,
   category VARCHAR,
   region VARCHAR
);
INSERT INTO orders VALUES
   ('acme', 'widgets', 'US'),
   ('acme', 'gadgets', 'EU'),
   ('globex', 'widgets', 'US'),
   ('initech', 'gadgets', 'APAC');

CREATE TABLE IF NOT EXISTS events (
   org_id VARCHAR,
   kind VARCHAR
);
INSERT INTO events VALUES
   ('acme', 'click'),
   ('globex', 'view'),
   ('initech', 'scroll');
`;

// products: implicit Organization filter only (the multi-tenant boundary).
const PRODUCTS_MODEL = `
#(filter) name=Organization dimension=org_id type=equal implicit required
source: products is duckdb.table('products') extend {
   measure: n is count()
   view: by_org is {
      group_by: org_id, product_name
      aggregate: n
   }
}
`;

// orders: implicit Organization + required-explicit Category + optional Region.
const ORDERS_MODEL = `
#(filter) name=Organization dimension=org_id type=equal implicit required
#(filter) name=Category dimension=category type=equal required
#(filter) name=Region dimension=region type=in
source: orders is duckdb.table('orders') extend {
   measure: n is count()
}
`;

// A model that neither defines nor imports any protected source. Used as the
// target for the unprotected-source regression.
const ANALYTICS_MODEL = `
source: metrics is duckdb.table('events') extend {
   measure: c is count()
}
`;

beforeAll(async () => {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   await fs.mkdir(TEST_PKG_DIR, { recursive: true });
   duckdbConnection = new DuckDBConnection("duckdb", TEST_DB_PATH, TEST_DB_DIR);
   for (const stmt of SEED_SQL.trim().split(";").filter(Boolean)) {
      await duckdbConnection.runSQL(stmt.trim() + ";");
   }
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "products.malloy"),
      PRODUCTS_MODEL,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "orders.malloy"),
      ORDERS_MODEL,
      "utf-8",
   );
   await fs.writeFile(
      path.join(TEST_PKG_DIR, "analytics.malloy"),
      ANALYTICS_MODEL,
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

type Row = Record<string, unknown>;

function asRows(compactResult: unknown): Row[] {
   return compactResult as Row[];
}

async function makeModel(modelPath: string): Promise<Model> {
   return Model.create("test-pkg", TEST_PKG_DIR, modelPath, getConnections());
}

/** Run an ad-hoc query string and return the result rows. */
async function runAdHoc(
   model: Model,
   query: string,
   filterParams?: FilterParams,
): Promise<Row[]> {
   const { compactResult } = await model.getQueryResults(
      undefined,
      undefined,
      query,
      filterParams,
   );
   return asRows(compactResult);
}

/**
 * Assert an ad-hoc query is rejected with a 400 whose message names a missing
 * required filter (`expectedSubstring`, e.g. "Organization"). If the query
 * instead succeeds, the assertion fails reporting the row count — a filter
 * bypass / data leak slipped through.
 */
async function expectFilterRejected(
   model: Model,
   query: string,
   filterParams: FilterParams | undefined,
   expectedSubstring: string,
): Promise<void> {
   let leakedRows: number | undefined;
   try {
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         query,
         filterParams,
      );
      leakedRows = asRows(compactResult).length;
   } catch (error) {
      expect(error).toBeInstanceOf(BadRequestError);
      expect((error as Error).message).toContain(expectedSubstring);
      return;
   }
   throw new Error(
      `Expected a 400 naming "${expectedSubstring}", but the query succeeded ` +
         `and returned ${leakedRows} rows (FILTER BYPASS / LEAK).`,
   );
}

/** Assert every returned row is scoped to acme and the count matches. */
function expectAcmeScoped(rows: Row[], expectedRows: number): void {
   expect(rows.length).toBe(expectedRows);
   for (const row of rows) expect(row.org_id).toBe("acme");
}

// ===========================================================================
// Enforced: direct reads of a protected source.
// ===========================================================================

describe("direct read of a protected source is enforced", () => {
   it("rejects a direct query with no filter params", async () => {
      const model = await makeModel("products.malloy");
      await expectFilterRejected(
         model,
         "run: products -> { group_by: org_id, product_name; aggregate: n is count() }",
         undefined,
         "Organization",
      );
   });

   it("scopes a direct query when Organization is supplied", async () => {
      const model = await makeModel("products.malloy");
      const rows = await runAdHoc(
         model,
         "run: products -> { group_by: org_id, product_name; aggregate: n is count() }",
         { Organization: "acme" },
      );
      expectAcmeScoped(rows, 2); // acme has exactly 2 products
   });

   // A predefined view on the trusted source is still the direct path.
   it("enforces filters on a direct named-view read", async () => {
      const model = await makeModel("products.malloy");
      await expectFilterRejected(
         model,
         "run: products -> by_org",
         undefined,
         "Organization",
      );
      const rows = await runAdHoc(model, "run: products -> by_org", {
         Organization: "acme",
      });
      expect(rows.length).toBe(2);
   });

   // orders — implicit Organization + required-explicit Category. Both required
   // filters are enforced; the error names whichever is still missing.
   it("rejects orders with no filter params (names Organization)", async () => {
      const model = await makeModel("orders.malloy");
      await expectFilterRejected(
         model,
         "run: orders -> { group_by: org_id, category; aggregate: n is count() }",
         undefined,
         "Organization",
      );
   });

   it("rejects orders with only Category supplied (still missing Organization)", async () => {
      const model = await makeModel("orders.malloy");
      await expectFilterRejected(
         model,
         "run: orders -> { group_by: org_id, category; aggregate: n is count() }",
         { Category: "widgets" },
         "Organization",
      );
   });

   it("rejects orders with only Organization supplied (still missing Category)", async () => {
      const model = await makeModel("orders.malloy");
      await expectFilterRejected(
         model,
         "run: orders -> { group_by: org_id, category; aggregate: n is count() }",
         { Organization: "acme" },
         "Category",
      );
   });

   it("scopes orders when both Organization and Category are supplied", async () => {
      const model = await makeModel("orders.malloy");
      const rows = await runAdHoc(
         model,
         "run: orders -> { group_by: org_id, category; aggregate: n is count() }",
         { Organization: "acme", Category: "widgets" },
      );
      expect(rows.length).toBe(1); // acme + widgets → one group
      expect(rows[0].org_id).toBe("acme");
      expect(rows[0].category).toBe("widgets");
   });
});

// ===========================================================================
// Enforced: alias / extend / chain of a protected source.
//
// Reaching a protected source under an ad-hoc name carries the SAME filter
// requirement. The query is not rejected for its shape — it runs, scoped — so
// the `execute_query` surface stays intact.
// ===========================================================================

interface EnforcedVector {
   label: string;
   modelPath: string;
   query: string;
   /** Required filter named in the missing-param rejection. */
   missing: string;
   /** Params satisfying every required filter on the protected source. */
   validParams: FilterParams;
   /** Rows expected once scoped to acme. */
   expectedRows: number;
}

const enforcedVectors: EnforcedVector[] = [
   {
      // Plain alias — a pure rename of the protected source.
      label: "alias (source a is products)",
      modelPath: "products.malloy",
      query: "source: a is products\nrun: a -> { group_by: org_id, product_name; aggregate: n is count() }",
      missing: "Organization",
      validParams: { Organization: "acme" },
      expectedRows: 2,
   },
   {
      // Extend with an extra measure. The body adds to the curated surface but
      // does not touch the filter dimension, so enforcement is unaffected.
      label: "extend (source e is products extend { … })",
      modelPath: "products.malloy",
      query: "source: e is products extend { measure: rc is count() }\nrun: e -> { group_by: org_id, product_name; aggregate: rc }",
      missing: "Organization",
      validParams: { Organization: "acme" },
      expectedRows: 2,
   },
   {
      // Chained derivation — protection is inherited link by link and resolved
      // back to products.
      label: "chained (a is products; b is a)",
      modelPath: "products.malloy",
      query: "source: a is products\nsource: b is a\nrun: b -> { group_by: org_id, product_name; aggregate: n is count() }",
      missing: "Organization",
      validParams: { Organization: "acme" },
      expectedRows: 2,
   },
   {
      // Ad-hoc `#(filter)` annotation in the query text declaring a NON-required
      // Organization filter on the alias, attempting to drop the requirement.
      // Annotations written in the query are not honored — only the protected
      // source's own annotation counts — so the requirement still stands.
      label: "ad-hoc #(filter) annotation override is ignored",
      modelPath: "products.malloy",
      query:
         "#(filter) name=Organization dimension=org_id type=equal\n" +
         "source: a is products extend {}\n" +
         "run: a -> { group_by: org_id, product_name; aggregate: n is count() }",
      missing: "Organization",
      validParams: { Organization: "acme" },
      expectedRows: 2,
   },
   {
      // orders alias — confirms enforcement on a multi-required-filter source.
      label: "orders alias (source a is orders)",
      modelPath: "orders.malloy",
      query: "source: a is orders\nrun: a -> { group_by: org_id, category; aggregate: n is count() }",
      missing: "Organization",
      validParams: { Organization: "acme", Category: "widgets" },
      expectedRows: 1,
   },
   {
      // orders extend.
      label: "orders extend",
      modelPath: "orders.malloy",
      query: "source: e is orders extend { measure: rc is count() }\nrun: e -> { group_by: org_id, category; aggregate: rc }",
      missing: "Organization",
      validParams: { Organization: "acme", Category: "widgets" },
      expectedRows: 1,
   },
];

describe("alias/extend/chain of a protected source is enforced (not restricted)", () => {
   for (const v of enforcedVectors) {
      describe(v.label, () => {
         it("rejects when the required filter is missing", async () => {
            const model = await makeModel(v.modelPath);
            await expectFilterRejected(model, v.query, undefined, v.missing);
         });

         it("runs scoped when valid filter params are supplied", async () => {
            const model = await makeModel(v.modelPath);
            const rows = await runAdHoc(model, v.query, v.validParams);
            expectAcmeScoped(rows, v.expectedRows);
         });
      });
   }
});

// ===========================================================================
// Regression: unprotected sources stay open; bypassFilters short-circuits
// filter enforcement.
// ===========================================================================

describe("regressions", () => {
   it("runs an unprotected source with no filter params (no spurious demand)", async () => {
      const model = await makeModel("analytics.malloy");
      const rows = await runAdHoc(
         model,
         "run: metrics -> { group_by: org_id; aggregate: c is count() }",
      );
      expect(rows.length).toBe(3); // all three orgs, unfiltered
   });

   it("bypassFilters short-circuits filter enforcement on a derivation", async () => {
      const model = await makeModel("products.malloy");
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         "source: a is products\nrun: a -> { group_by: org_id; aggregate: n is count() }",
         {},
         true,
      );
      expect(asRows(compactResult).length).toBe(3);
   });
});
