// End-to-end proof for the colocated `#@ persist` authorize relaxation
// (`assertColocatedPersistNotAuthorizeGated`'s `row_level` + `attributed`
// branch, `materialization_eligibility.ts`). `materialization_eligibility.spec.ts`
// and `build_plan_gate_classification.spec.ts` already pin the compile-time
// DECISION (classify, then admit/refuse); this file proves the thing that
// decision is a proxy FOR — that a query served from a bound colocated
// artifact enforces the same row filter a live query would, over a REAL
// compiled package with a REAL bound manifest, not a mock.
//
// The refusal made this composition untestable before this relaxation
// existed: a colocated `#@ persist` source with any `#(authorize)` gate never
// built at all. Every scenario below binds a manifest entry pointing at a
// table that is NOT what the live query would read from (deliberately
// distinguishable data), so a passing assertion proves BOTH that the
// same-connection substitution took effect AND that the row filter still
// applied on top of it.
//
// Every persist source here is a `query_source` (`X is <sql_select> -> {...}`)
// rather than a bare `#@ persist` directly on a `sql_select`/`table`-typed
// source. That is not an arbitrary style choice: an experiment against a bare
// `duckdb.sql(...)` persisted directly (ungated, to isolate the variable) found
// its colocated `buildManifest` substitution never engages at query time
// either — a pre-existing property of how `run:` resolves a raw source type,
// unrelated to this relaxation. Every persist fixture elsewhere in this
// codebase already uses the query_source shape for exactly this reason.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

let rootDir: string;
let envPath: string;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(
      path.join(os.tmpdir(), "publisher-colocated-relax-"),
   );
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackageFiles(
   files: Record<string, string>,
): Promise<Package> {
   const dir = path.join(envPath, "pkg");
   await fs.mkdir(dir, { recursive: true });
   await fs.writeFile(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "pkg", description: "fixture" }),
   );
   for (const [name, contents] of Object.entries(files)) {
      await fs.writeFile(path.join(dir, name), contents);
   }
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage("pkg", false);
}

/** Run `query` against `model.malloy` with the given `givens` and return rows. */
async function run(
   pkg: Package,
   query: string,
   givens: Record<string, unknown> = {},
) {
   const model = pkg.getModel("model.malloy");
   if (!model) throw new Error("model.malloy did not load");
   const { compactResult } = await model.getQueryResults(
      undefined,
      undefined,
      query,
      undefined,
      undefined,
      givens as never,
   );
   return (compactResult as Record<string, unknown>[]).map((row) =>
      Object.fromEntries(
         Object.entries(row).map(([k, v]) => [
            k,
            typeof v === "bigint" ? Number(v) : v,
         ]),
      ),
   );
}

/**
 * Create a physical table in the package's own duckdb connection carrying
 * DIFFERENT data than the source's live SQL would compute, and bind it as
 * `sourceName`'s colocated artifact. Distinguishable data is the point: a
 * query that still answered from the LIVE relation would silently pass a
 * weaker test.
 */
async function buildAndBindColocated(
   pkg: Package,
   sourceName: string,
   physicalTableName: string,
   rows: string,
): Promise<void> {
   const connection = (await pkg.getMalloyConnection(
      "duckdb",
   )) as DuckDBConnection;
   await connection.runSQL(
      `CREATE OR REPLACE TABLE ${physicalTableName} AS ${rows};`,
   );
   const plan = pkg.getBuildPlan();
   const source = Object.values(plan?.sources ?? {}).find(
      (s) => s.name === sourceName,
   );
   if (!source) {
      throw new Error(`persist source '${sourceName}' not in build plan`);
   }
   pkg.bindColocatedServeManifest({
      [source.sourceEntityId]: {
         tableName: physicalTableName,
         connectionName: "duckdb",
      },
   });
}

describe("colocated persist + row-level #(authorize): compose end to end", () => {
   it(
      "a direct entry-point gate: two principals see DIFFERENT rows from the SAME bound artifact",
      async () => {
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (1, 20), (2, 30), (2, 40)) AS t(org_id, amount)
""")

#(authorize) "org_id = $ORG"
#@ persist name="orders"
source: orders is base -> { select: org_id, amount }
`,
         });
         // Materialized data is DISTINCT from the live SQL above, so a
         // correct answer proves the FROM substitution took effect.
         await buildAndBindColocated(
            pkg,
            "orders",
            "orders_materialized",
            `SELECT * FROM (VALUES (1, 1000), (1, 2000), (2, 3000), (2, 4000)) AS t(org_id, amount)`,
         );

         const org1 = await run(
            pkg,
            "run: orders -> { select: org_id, amount; order_by: amount }",
            { ORG: 1 },
         );
         const org2 = await run(
            pkg,
            "run: orders -> { select: org_id, amount; order_by: amount }",
            { ORG: 2 },
         );

         expect(org1).toEqual([
            { org_id: 1, amount: 1000 },
            { org_id: 1, amount: 2000 },
         ]);
         expect(org2).toEqual([
            { org_id: 2, amount: 3000 },
            { org_id: 2, amount: 4000 },
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "a gate inherited via extend (no annotation of its own): the persisted extend still filters",
      async () => {
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

#(authorize) "org_id = $ORG"
source: locked is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#@ persist name="derived"
source: derived is locked extend {} -> { select: org_id, amount }
`,
         });
         await buildAndBindColocated(
            pkg,
            "derived",
            "derived_materialized",
            `SELECT * FROM (VALUES (1, 100), (2, 200)) AS t(org_id, amount)`,
         );

         const rows = await run(
            pkg,
            "run: derived -> { select: org_id, amount }",
            { ORG: 1 },
         );
         expect(rows).toEqual([{ org_id: 1, amount: 100 }]);
      },
      { timeout: 60000 },
   );

   it(
      "an always-false gate produces a bound artifact with zero rows for every principal",
      async () => {
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#(authorize) "false"
#@ persist name="locked_out"
source: locked_out is base -> { select: org_id, amount }
`,
         });
         await buildAndBindColocated(
            pkg,
            "locked_out",
            "locked_out_materialized",
            `SELECT * FROM (VALUES (1, 999), (2, 888)) AS t(org_id, amount)`,
         );

         const rows = await run(
            pkg,
            "run: locked_out -> { select: org_id, amount }",
         );
         expect(rows).toEqual([]);
      },
      { timeout: 60000 },
   );

   it(
      "a gate referencing a given: filters when the caller supplies a value, and fails closed (never leaks unfiltered) when the caller supplies none",
      async () => {
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#(authorize) "org_id = $ORG"
#@ persist name="orders"
source: orders is base -> { select: org_id, amount }
`,
         });
         await buildAndBindColocated(
            pkg,
            "orders",
            "orders_materialized",
            `SELECT * FROM (VALUES (1, 111), (2, 222)) AS t(org_id, amount)`,
         );

         const supplied = await run(
            pkg,
            "run: orders -> { select: org_id, amount }",
            { ORG: 1 },
         );
         expect(supplied).toEqual([{ org_id: 1, amount: 111 }]);

         // No given declared with no default: an unsupplied required given
         // must not fall through to serving every row unfiltered.
         await expect(
            run(pkg, "run: orders -> { select: org_id, amount }"),
         ).rejects.toThrow(/no value and no default/i);
      },
      { timeout: 60000 },
   );

   it(
      "a chained persist over a gated persist: the downstream's OWN bound artifact still enforces the upstream's inherited gate",
      async () => {
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#(authorize) "org_id = $ORG"
#@ persist name="base_orders"
source: base_orders is raw -> { select: org_id, amount }

#@ persist name="rollup"
source: rollup is base_orders -> { select: org_id, amount }
`,
         });
         await buildAndBindColocated(
            pkg,
            "base_orders",
            "base_materialized",
            `SELECT * FROM (VALUES (1, 1000), (2, 2000)) AS t(org_id, amount)`,
         );
         await buildAndBindColocated(
            pkg,
            "rollup",
            "rollup_materialized",
            `SELECT * FROM (VALUES (1, 9999), (2, 8888)) AS t(org_id, amount)`,
         );

         const rows = await run(
            pkg,
            "run: rollup -> { select: org_id, amount }",
            { ORG: 1 },
         );
         // Serves from rollup's OWN materialized table, but the gate it
         // inherited (via the query-source derivation base) from `base_orders`
         // still filters it.
         expect(rows).toEqual([{ org_id: 1, amount: 9999 }]);
      },
      { timeout: 60000 },
   );

   it(
      "a derived query_source whose OWN projection drops the gate column: the whole package fails to load, never silently unfiltered",
      async () => {
         // `orders` authors its OWN `#(authorize)`, and its own projection
         // drops `org_id` — the field the gate needs. `validateAuthorizeProbes`
         // runs the same row-level probe at LOAD time; with no annotation to
         // blame on an ancestor, an unexpressible gate aborts the whole
         // package load rather than degrading to a silent, unfiltered persist.
         // The colocated relaxation never even gets a chance to consider this
         // source: it is unqueryable, gated or not.
         await expect(
            loadPackageFiles({
               "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#(authorize) "org_id = $ORG"
#@ persist name="orders"
source: orders is base -> { select: amount }
`,
            }),
         ).rejects.toThrow(/org_id/);
      },
      { timeout: 60000 },
   );
});
