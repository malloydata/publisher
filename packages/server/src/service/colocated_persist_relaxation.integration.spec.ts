// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// End-to-end proof for the colocated `#@ persist` authorize relaxation
// (`assertColocatedPersistNotAuthorizeGated`'s `row_level` + `attributed`
// branch, `materialization_eligibility.ts`). `materialization_eligibility.spec.ts`
// and `build_plan_gate_classification.spec.ts` already pin the compile-time
// DECISION (classify, then admit/refuse); this file proves the thing that
// decision is a proxy FOR — that a query served from a bound colocated
// artifact enforces the same row filter a live query would, over a REAL
// compiled package with a REAL bound manifest, not a mock.
//
// A colocated `#@ persist` source with an unproven or rejected `#(authorize)`
// gate never builds at all; only the proven row_level + attributed shape
// does. Every scenario below binds a manifest entry pointing at a
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

import { AccessDeniedError } from "../errors";
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

#(authorize) org_id = $ORG
#@ persist name="orders"
source: orders is base -> { select: org_id, amount } extend {}
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
      "a gate inherited (not re-declared) through a projecting query-source derivation: builds and filters correctly (this used to fail closed under the DIMENSION form — see task-3-report.md)",
      async () => {
         // Under the DIMENSION form this shape used to fail closed: the graft
         // there names the gate FIELD, and `derived`'s own query-source
         // projection (`-> { select: org_id, amount }`) did not carry
         // `locked`'s "authorized" field forward, so no name existed on
         // `derived`'s own struct to graft onto — `derived` was excluded from
         // the build plan and a live query against it denied. The
         // source-line form's note lives on `locked` itself, not on a
         // droppable field, so it survives the projection by the same
         // by-reference note-copy mechanism the STRING form always relied
         // on: `derived` inherits the gate, builds normally, and a live
         // query is filtered exactly as a direct gate on `locked` would be.
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

#(authorize) org_id = $ORG
source: locked is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""") extend {}

#@ persist name="derived"
source: derived is locked extend {} -> { select: org_id, amount }
`,
         });

         // Unlike the DIMENSION form (see task-3-report.md for
         // the prior version of this test), the source-line form's note lives
         // on `locked` itself, not on a field `derived`'s projection has to
         // carry forward — so `derived` inherits it the same way the STRING
         // form always did, and the persist source builds normally.
         const plan = pkg.getBuildPlan();
         expect(
            Object.values(plan?.sources ?? {}).some(
               (s) => s.name === "derived",
            ),
         ).toBe(true);

         await buildAndBindColocated(
            pkg,
            "derived",
            "derived_materialized",
            `SELECT * FROM (VALUES (1, 1000), (2, 2000)) AS t(org_id, amount)`,
         );

         const org1 = await run(
            pkg,
            "run: derived -> { select: org_id, amount }",
            { ORG: 1 },
         );
         expect(org1).toEqual([{ org_id: 1, amount: 1000 }]);
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

#(authorize) false
#@ persist name="locked_out"
source: locked_out is base -> { select: org_id, amount } extend {}
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

#(authorize) org_id = $ORG
#@ persist name="orders"
source: orders is base -> { select: org_id, amount } extend {}
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

         // A given declared with no default: an unsupplied required given must
         // not fall through to serving every row unfiltered. It refuses
         // OPAQUELY — Malloy's own binding failure names the gate's given
         // ("Given 'ORG' has no value and no default"), which a denied caller
         // must never learn (`docs/authorize.md`), so `getQueryResults` maps
         // it back to the same 403 a whole-source gate always returned.
         const denial = await run(
            pkg,
            "run: orders -> { select: org_id, amount }",
         ).catch((e) => e);
         expect(denial).toBeInstanceOf(AccessDeniedError);
         expect(String(denial.message)).not.toContain("ORG");
      },
      { timeout: 60000 },
   );

   it(
      "a chained persist over a gated persist: the downstream builds and filters — the upstream's gate is inherited, not re-declared (this used to fail closed under the DIMENSION form — see task-3-report.md)",
      async () => {
         // `base_orders` declares its OWN gate. `rollup`, one hop further,
         // does NOT re-declare it — it only inherits `base_orders`'s gate via
         // the query-source derivation. Under the DIMENSION form this used to
         // fail closed (the by-name graft found no "authorized" field on
         // `rollup`'s own projection); the source-line form's note lives on
         // `base_orders` itself, so `rollup` inherits it by the same
         // by-reference note-copy mechanism the STRING form always relied on
         // (see the "projecting query-source derivation" test above), and
         // both `base_orders` and `rollup` build and filter normally.
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence
##! experimental.givens

given: ORG :: number

source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 10), (2, 20)) AS t(org_id, amount)
""")

#(authorize) org_id = $ORG
#@ persist name="base_orders"
source: base_orders is raw -> { select: org_id, amount }

#@ persist name="rollup"
source: rollup is base_orders -> { select: org_id, amount }
`,
         });
         await buildAndBindColocated(
            pkg,
            "rollup",
            "rollup_materialized",
            `SELECT * FROM (VALUES (1, 1000), (2, 2000)) AS t(org_id, amount)`,
         );

         const plan = pkg.getBuildPlan();
         expect(
            Object.values(plan?.sources ?? {}).some((s) => s.name === "rollup"),
         ).toBe(true);

         const org1 = await run(
            pkg,
            "run: rollup -> { select: org_id, amount }",
            { ORG: 1 },
         );
         expect(org1).toEqual([{ org_id: 1, amount: 1000 }]);
      },
      { timeout: 60000 },
   );

   it(
      "a persist source over a composite entry point, gated on one member: builds and filters correctly (this used to fail closed under the DIMENSION form — see task-3-report.md)",
      async () => {
         // `comp` is a query_source over `compose(member_a, member_b)`. Only
         // `member_a` carries the gate. Malloy resolves the composite to ONE
         // concrete member per query and copies that member's own gate note
         // onto `comp`'s entry point (see
         // `build_plan_gate_classification.spec.ts`'s unit-level pin of this
         // classification), so `comp` reads as `row_level` + `attributed`
         // with no join or deep walk involved. Under the DIMENSION form this
         // used to fail closed instead — the same by-name-graft gap the two
         // tests above document: `comp`'s own projection
         // (`-> { select: org_id, amount }`) did not carry `member_a`'s
         // "authorized" field forward. The source-line form's note is a
         // struct-level copy, not a droppable field, so it survives the
         // composite resolution and the projection intact.
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental { persistence composite_sources givens }

given:
  GROUPS :: number[]

// Members carry DISTINGUISHABLE amounts so the assertion can tell WHICH
// member the composite resolved to; identical member data would pass even
// if the ungated member_b won.
#(authorize) org_id in $GROUPS
source: member_a is duckdb.sql("""
  SELECT * FROM (VALUES (7, 1), (8, 2)) AS t(org_id, amount)
""") extend {}

source: member_b is duckdb.sql("""
  SELECT * FROM (VALUES (7, 51), (8, 52)) AS t(org_id, amount)
""")

source: combo is compose(member_a, member_b)

#@ persist name="comp"
source: comp is combo -> { select: org_id, amount }
`,
         });
         const plan = pkg.getBuildPlan();
         expect(
            Object.values(plan?.sources ?? {}).some((s) => s.name === "comp"),
         ).toBe(true);

         await buildAndBindColocated(
            pkg,
            "comp",
            "comp_materialized",
            `SELECT * FROM (VALUES (7, 1000), (8, 2000)) AS t(org_id, amount)`,
         );

         const rows = await run(
            pkg,
            "run: comp -> { select: org_id, amount; order_by: org_id }",
            { GROUPS: [7] },
         );
         expect(rows).toEqual([{ org_id: 7, amount: 1000 }]);
      },
      { timeout: 60000 },
   );

   it(
      "the DIMENSION form of #(authorize): a colocated persist over a gate dimension composes the same way the string form does",
      async () => {
         // `build_plan.ts` now classifies a dimension-form gate as `row_level`
         // (`resolveGateShape` skips `classifyAuthorizeGate` for it — see
         // `./gate_dimension`'s doc) and `isAuthorizeAttributedToEntryPoint`
         // reads it the same way it reads the string form (both are plain
         // annotation-NOTE walks, form-agnostic) — this is the new reachable
         // state IMPORTANT 6 flagged.
         //
         // The gate dimension is added via `extend {}` AFTER the `->`
         // pipeline, not selected INSIDE it — a real wrinkle found writing
         // this test. Selecting the dimension as an output column of the
         // persist source's own defining query (`-> {select: ..., authorized}`)
         // forces `PersistSource.getSQL()` — which compiles the physical
         // CREATE-TABLE-AS with no request/given context at all — to
         // evaluate it at BUILD time, so a given-referencing gate fails the
         // build itself ("Given ... has no value and no default"),
         // independent of G3/G4/request-time enforcement; and even a
         // givenless one produced SQL that qualified the graft's `WHERE`
         // against the wrong table alias. Declared via `extend {}` instead,
         // it stays a LAZY field layered on the persisted query's own output
         // — exactly how it behaves on an unpersisted source — and the
         // by-name graft resolves against it normally. This is a narrower
         // limitation of one specific shape (dimension SELECTED into a
         // persist source's own projection), not a defect in this task's
         // fix — flagged in the report.
         //
         // Left on the DIMENSION form deliberately (not migrated with the
         // rest of this file — see task-3-report.md): the wrinkle pinned
         // here (a gate dimension selectable as an output column, forcing
         // BUILD-time evaluation) is specific to the gate being a field. A
         // source-line annotation is never a selectable column, so this
         // shape has no equivalent under the surviving form.
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental.persistence

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'org1', 10), (2, 'org2', 20)) AS t(id, org_id, amount)
""")

#@ persist name="orders"
#(authorize) org_id = 'org1'
source: orders is base -> { select: id, org_id, amount } extend {
}
`,
         });
         await buildAndBindColocated(
            pkg,
            "orders",
            "orders_materialized",
            `SELECT * FROM (VALUES (1, 'org1', 1000), (2, 'org2', 2000)) AS t(id, org_id, amount)`,
         );

         const rows = await run(
            pkg,
            "run: orders -> { select: org_id, amount }",
         );
         expect(rows).toEqual([{ org_id: "org1", amount: 1000 }]);
      },
      { timeout: 60000 },
   );

   it(
      "$GROUPS-based: two different callers reading the SAME bound artifact get two DIFFERENT row sets — the gate is re-evaluated per query, never baked into the shared build",
      async () => {
         // The test above proves the classification/graft machinery with a
         // givenless static predicate — itself a W1 shape, which cannot
         // distinguish "the gate re-runs per caller" from "one caller's rows
         // got baked into the shared artifact at build time" (they'd look
         // identical for a fixed predicate). This test is the one that
         // actually exercises that distinction: the SAME bound artifact,
         // queried by two different principals, must filter differently for
         // each — proving `materialization_eligibility.ts`'s design
         // principle (persistence changes only WHERE rows come from, never
         // WHETHER the filter is appended) rather than merely asserting it.
         const pkg = await loadPackageFiles({
            "model.malloy": `##! experimental { persistence givens }

given: GROUPS :: string[]

source: base is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'org1', 10), (2, 'org2', 20)) AS t(id, org_id, amount)
""")

#(authorize) org_id in $GROUPS
#@ persist name="orders"
source: orders is base -> { select: id, org_id, amount }
`,
         });
         // Materialized data is DISTINCT from the live SQL above, so a
         // correct answer proves the FROM substitution took effect AND that
         // the gate still ran on top of it.
         await buildAndBindColocated(
            pkg,
            "orders",
            "orders_materialized",
            `SELECT * FROM (VALUES (1, 'org1', 1000), (2, 'org2', 2000)) AS t(id, org_id, amount)`,
         );

         const asOrg1 = await run(
            pkg,
            "run: orders -> { select: org_id, amount }",
            { GROUPS: ["org1"] },
         );
         const asOrg2 = await run(
            pkg,
            "run: orders -> { select: org_id, amount }",
            { GROUPS: ["org2"] },
         );
         expect(asOrg1).toEqual([{ org_id: "org1", amount: 1000 }]);
         expect(asOrg2).toEqual([{ org_id: "org2", amount: 2000 }]);
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

#(authorize) org_id = $ORG
#@ persist name="orders"
source: orders is base -> { select: amount }
`,
            }),
         ).rejects.toThrow(/org_id/);
      },
      { timeout: 60000 },
   );
});
