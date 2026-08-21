// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Integration coverage for the CURRENT `#(authorize)` form: an unquoted,
 * natural Malloy boolean expression annotated directly on the `source:`
 * line, e.g.
 *
 *   #(authorize) org_id in $GROUPS
 *   source: accounts is duckdb.table('accounts') extend {}
 *
 * Most tests go through the REAL `Model.create` + `Model.getQueryResults`
 * path against a real DuckDB connection — never hand-built IR — so a
 * passing test is proof the whole compile → classify → graft → enforce
 * pipeline works for this form, not just one function in isolation.
 *
 * `buildGatedModel` (copied from `row_level_authorize.integration.spec.ts`,
 * which owns the canonical version — see ITS doc comment for what it is and
 * isn't bypassing) is used ONLY for the fail-closed-reversal / misbinding
 * cases below, which deliberately construct a derivation whose gate is
 * genuinely unexpressible at that one entry point. Going through
 * `Model.create` for those does not reach request time at all: `except:`ing
 * or `accept:`-dropping the column a source-line gate reads leaves Malloy's
 * BY-REFERENCE copy of the base's own annotation note sitting on the
 * deriving struct's OWN annotations (same mechanism documented in
 * `gate_registry_walk.ts`'s header) — `validateAuthorizeProbes`'s load-time
 * own-vs-inherited check reads that copied note as "declared here" (it tests
 * presence, not identity against an ancestor), so an unexpressible probe
 * there throws and aborts the WHOLE model's load rather than warning and
 * denying only that entry point. See the "load-time abort" describe block
 * below, which pins that (surprising, pre-existing, NOT introduced by this
 * form) behavior directly, empirically, rather than assuming it.
 *
 * Scope is Task 1 of the source-line migration: prove the spine (compile,
 * enforce, override, join non-propagation, query-source inheritance,
 * at-most-one-gate) and pin two known edge cases (the except:+rename:
 * misbinding hole; whether a bare `false` literal compiles). It does not
 * migrate the existing dimension-form corpus (Task 2), touch
 * `gate_dimension.ts`'s validation (Task 3/4), or fix
 * `validateAuthorizeProbes`'s own-vs-inherited heuristic.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   modelDefToModelInfo,
   Runtime,
   type Connection,
   type ModelDef,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { Model } from "./model";

const ROOT = "file:///source-line-authorize-tests/";

/**
 * `org_id`/`owner` are deliberately DIFFERENT per row so a query that binds
 * to the wrong column (the except:+rename: misbinding test) returns a
 * DIFFERENT row set than the correctly-bound query would, rather than merely
 * an empty one — the asymmetry is what proves misbinding rather than mere
 * denial.
 */
const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, owner INTEGER, val VARCHAR);
INSERT INTO orgtable VALUES
   (1, 1, 2, 'a'), (2, 1, 1, 'b'), (3, 2, 1, 'c'), (4, 2, 2, 'd');
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

/** Load `text` through the real `Model.create`, against a fresh seeded
 *  DuckDB. Caller is responsible for `duckdb.close()` / `fs.rmSync(dir)`. */
async function createModel(
   text: string,
   fileName = "m.malloy",
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "source-line-authz-"));
   fs.writeFileSync(path.join(dir, fileName), text);
   const model = await Model.create(
      "test-pkg",
      dir,
      fileName,
      new Map<string, Connection>([["duckdb", duckdb]]),
   );
   return { model, duckdb, dir };
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

async function cleanup(duckdb: DuckDBConnection, dir?: string): Promise<void> {
   await duckdb.close();
   if (dir) fs.rmSync(dir, { recursive: true, force: true });
}

/**
 * Build a `Model` from real compiled Malloy IR while skipping `Model.create`'s
 * pre-flight `validateAuthorizeProbes` call — copied from
 * `row_level_authorize.integration.spec.ts`'s helper of the same name (that
 * file owns the canonical version; duplicated rather than imported, same
 * convention it already documents for itself). Used here ONLY for gates that
 * are deliberately unexpressible at the one entry point under test, so the
 * test reaches request time instead of aborting at load — see this file's
 * header.
 */
async function buildGatedModel(
   text: string,
   opts?: { duckdb?: DuckDBConnection },
): Promise<{ model: Model; duckdb: DuckDBConnection }> {
   const duckdb = opts?.duckdb ?? (await newDuckdb());
   const modelPath = "m.malloy";
   const fullText = text.includes("experimental.givens")
      ? text
      : `##! experimental.givens\n\n${text}`;
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}${modelPath}`, fullText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${modelPath}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   const modelDef = (compiled as unknown as { _modelDef: ModelDef })._modelDef;
   const modelInfo = modelDefToModelInfo(modelDef);
   const malloyGivens = Array.from(
      compiled.givens.values(),
   ) as unknown as MalloyGiven[];
   const givens =
      malloyGivens.length > 0 ? malloyGivens.map(malloyGivenToApi) : undefined;
   const model = new Model(
      "test-pkg",
      modelPath,
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
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      givens as any,
      modelInfo,
   );
   (model as unknown as { setGateRuntime(r: Runtime): void }).setGateRuntime(
      runtime,
   );
   return { model, duckdb };
}

/**
 * `gated_parent` is gated on `org_id in $GROUPS`. `child` is a plain
 * `extend {}` (should inherit). `child_own` declares its own gate (should
 * override, not OR, the inherited one). `joiner` joins `gated_parent` into
 * an otherwise ungated source (the join must never propagate the gate).
 * `qchild` is a query-source derivation of `gated_parent` (should inherit).
 * This model loads CLEANLY through `Model.create` — every entry point here
 * can express the inherited gate in its own field space. Compare the
 * separate `except:`/`accept:`/rename: fixtures below, which deliberately
 * cannot and are therefore built via `buildGatedModel` instead.
 */
const MODEL = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: child is gated_parent extend {}

#(authorize) org_id = 999
source: child_own is gated_parent extend {}

source: joiner is duckdb.table('orgtable') extend {
   join_one: gp is gated_parent on id = gp.id
   measure: n is count()
}

source: qchild is gated_parent -> { group_by: id, org_id, val; aggregate: n is count() }
`;

describe("source-line #(authorize) — spine", () => {
   it("compiles cleanly and enforces on the declaring source itself", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();

         const filtered = await model.getQueryResults(
            undefined,
            undefined,
            "run: gated_parent -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect(
            (filtered.compactResult as unknown as { n: number }[])[0].n,
         ).toBe(2);

         // An empty array with `in` matches nothing — 200, zero rows, not a
         // 403 (the gate attaches fine; it just admits no rows).
         const empty = await model.getQueryResults(
            undefined,
            undefined,
            "run: gated_parent -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
         );
         expect((empty.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a plain `extend {}` INHERITS the base's gate and filters", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: child -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );

         const zero = await model.getQueryResults(
            undefined,
            undefined,
            "run: child -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
         );
         expect((zero.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a child declaring its OWN gate REPLACES the inherited one, not OR's with it", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // If the inherited gate (org_id in $GROUPS) were still active
         // alongside the own gate (org_id = 999), GROUPS=[1] would still
         // match the org_id=1 rows. Only the OWN gate applying (org_id=999,
         // never true in this seed) proves override rather than OR.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: child_own -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a gate reached only through join_one: NEVER fires — the joining source stays ungated", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // `joiner` carries no #(authorize) of its own; joining the gated
         // `gated_parent` in as `gp` must not gate `joiner` itself. No
         // GROUPS given supplied at all — if the join propagated the gate,
         // this would either deny or throw for an unbound given.
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: joiner -> { aggregate: n is count() }",
            {},
            true,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            4,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a query-source derivation (`child is parent -> {...}`) carries the base's gate", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: qchild -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );

         const zero = await model.getQueryResults(
            undefined,
            undefined,
            "run: qchild -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [] },
         );
         expect((zero.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("at most ONE gate per source: two #(authorize) notes on one source is a load error naming both", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
#(authorize) org_id = 1
source: dual is duckdb.table('orgtable') extend {}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/"dual"/);
         expect(err?.message).toMatch(/org_id in \$GROUPS/);
         expect(err?.message).toMatch(/org_id = 1/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Fail-closed reversal + the misbinding hole.
//
// Each of these derives `gated_parent` in a way that breaks its OWN field
// space's ability to express the inherited gate. Malloy's by-reference copy
// of the base's own annotation note lands on the deriving struct's OWN
// annotations regardless (see this file's header), which makes
// `validateAuthorizeProbes`'s load-time preflight treat the probe failure as
// "declared here" and throw, aborting the WHOLE model load rather than
// denying just this one entry point — see the "load-time abort" describe
// block below, which pins that directly. `buildGatedModel` skips that
// preflight so these tests can reach request time and prove the GRAFT's own
// behavior, which is what the task is actually about.
// ---------------------------------------------------------------------------

describe("source-line #(authorize) — fail-closed reversal", () => {
   it("`except:`-ing the gated column DENIES (403) rather than serving unfiltered", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_except is gated_parent extend { except: org_id }
`);
      try {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_except -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb);
      }
   });

   it("an `accept:` list omitting the gated column DENIES (403) rather than serving unfiltered", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_accept is gated_parent extend { accept: id, val, n }
`);
      try {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_accept -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb);
      }
   });

   it("KNOWN HOLE — except: the gated column then rename: another column onto its exact name grafts successfully and MISBINDS to the wrong column", async () => {
      // Not a passing guarantee: this pins the OBSERVED behavior so a future
      // reader knows it is a known gap, not a security property. `w_misbind`
      // drops `org_id` and renames `owner` onto that name — the graft
      // compiles (there IS a field named `org_id` again) and filters on
      // `owner`'s values instead of the real `org_id`'s.
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_misbind is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`);
      try {
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: w_misbind -> { group_by: id; aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         const ids = (result.compactResult as unknown as { id: number }[])
            .map((r) => r.id)
            .sort();
         // The CORRECT (org_id-bound) answer would be [1, 2] (org_id=1 rows).
         // The MISBOUND (owner-bound) answer is [2, 3] (owner=1 rows) — a
         // DIFFERENT set, not a subset or superset, proving the bind
         // actually moved to the wrong column rather than merely degrading.
         expect(ids).toEqual([2, 3]);
      } finally {
         await cleanup(duckdb);
      }
   });
});

describe("source-line #(authorize) — load-time abort (pre-existing validateAuthorizeProbes gap, not introduced by this form)", () => {
   it("PINNED FINDING — an except:'d derivation of a source-line-gated base aborts the WHOLE model load, not just that entry point", async () => {
      // `validateAuthorizeProbes`'s own-vs-inherited check
      // (`authorize.ts`'s `ownNotesOf.get(sourceName) ?? []`) tests
      // PRESENCE, not identity against an ancestor — Malloy's by-reference
      // copy of `gated_parent`'s own note lands on `w_except`'s own
      // `annotations` (same mechanism as a plain `extend {}`, documented in
      // `gate_registry_walk.ts`'s header), so this reads as "w_except
      // declares its own gate" and throws when the probe can't resolve
      // `org_id` there — instead of the "inherited, unexpressible, warn and
      // deny at request time" outcome the dimension form's equivalent shape
      // gets via its own separate machinery (`gate_dimension.ts`). This is
      // NOT something Task 1 fixes (it predates the source-line form
      // entirely — `validateAuthorizeProbes` is unchanged by it); pinned
      // here so it is a documented, empirically-verified finding rather
      // than a surprise for whoever writes real models against this form.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_except is gated_parent extend { except: org_id }
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/org_id/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("source-line #(authorize) — constant expressions", () => {
   it("`1 = 1` compiles and denies nothing (the allow-everyone / locked-base-passthrough idiom)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) 1 = 1
source: open_source is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: open_source -> { aggregate: n is count() }",
            {},
            true,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            4,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) false` — the deny-everyone locked-base idiom: recorded finding on whether it compiles", async () => {
      // The pre-plan experiment measured `1 = 1` compiling as a source-level
      // `where:` but did not confirm a BARE `false` literal does too — Malloy
      // discriminates a bare boolean literal as its own IR node kind rather
      // than a generic comparison, and grammar support for it as a whole
      // `where:` expression (as opposed to a sub-expression) was unverified.
      // This test records the actual, empirically-observed outcome either
      // way rather than assuming one.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) false
source: locked_out is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         const err = compilationErrorOf(model);
         if (err === undefined) {
            // FINDING: a bare `false` literal DOES compile as a source-level
            // `where:` gate, and denies every request — 200, zero rows.
            const result = await model.getQueryResults(
               undefined,
               undefined,
               "run: locked_out -> { aggregate: n is count() }",
               {},
               true,
               {},
            );
            expect(
               (result.compactResult as unknown as { n: number }[])[0].n,
            ).toBe(0);
         } else {
            // FINDING: a bare `false` literal does NOT compile as a
            // source-level `where:` gate — the model fails to load. An
            // author reaching for "deny everyone" must currently spell it
            // as a tautologically-false comparison (e.g. `1 = 0`) instead.
            expect(err).toBeInstanceOf(ModelCompilationError);
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`1 = 0` compiles and denies every request (a working deny-everyone spelling)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) 1 = 0
source: locked_out2 is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: locked_out2 -> { aggregate: n is count() }",
            {},
            true,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
