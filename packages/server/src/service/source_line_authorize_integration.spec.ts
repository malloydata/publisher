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
 * isn't bypassing) is used for the fail-closed-reversal / misbinding cases
 * below, which deliberately construct a derivation whose gate is genuinely
 * unexpressible at that one entry point and predate the fix described next
 * (kept as-is rather than migrated to `createModel`, since both now load
 * cleanly and the point of those tests is the GRAFT's own behavior, not the
 * load path).
 *
 * `except:`ing or `accept:`-dropping the column a source-line gate reads
 * leaves Malloy's BY-REFERENCE copy of the base's own annotation note
 * sitting on the deriving struct's OWN `annotations` (same mechanism
 * documented in `gate_registry_walk.ts`'s header). This USED TO make
 * `validateAuthorizeProbes`'s load-time own-vs-inherited check read that
 * copied note as "declared here" (it tested presence, not who actually wrote
 * it) and abort the WHOLE model's load on an unexpressible probe, instead of
 * warning and denying only that one entry point.
 * `source_extraction.ts`'s `authorizeNoteDeclaredBy` (location-based note
 * ownership — see its doc for the mechanism and why simpler alternatives
 * don't work) fixed this: see the "an inheriting derivation's unexpressible
 * gate scopes to that entry point, not the whole load" describe block below,
 * which proves the load succeeds, the warning names the RIGHT entry point,
 * that entry point denies at request time, siblings and the declaring
 * source are unaffected, and a genuinely broken gate at the DECLARING
 * source still aborts the load as it always has.
 *
 * Scope is Task 1 (spine: compile, enforce, override, join non-propagation,
 * query-source inheritance, at-most-one-gate; two known edge cases: the
 * except:+rename: misbinding hole, a bare `false` literal) plus Task 2 (the
 * load-abort fix above). It does not migrate the existing dimension-form
 * corpus (Task 3) or touch `gate_dimension.ts`'s validation (Task 4).
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
import { describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { logger } from "../logger";
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
   return createModelWithFiles({ [fileName]: text }, fileName);
}

/**
 * Multi-file sibling of `createModel`, for exercising the
 * `location.url !== note.at.url` cross-file half of
 * `considerAuthorizeNoteOwner`'s attribution — `createModel` writes exactly
 * one file, so it can never exercise that comparison. `files` maps each
 * relative filename to its contents; `entryFileName` is the one loaded as the
 * package's model. Caller is responsible for `duckdb.close()` /
 * `fs.rmSync(dir)`, same as `createModel`.
 */
async function createModelWithFiles(
   files: Record<string, string>,
   entryFileName: string,
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "source-line-authz-"));
   for (const [fileName, text] of Object.entries(files)) {
      fs.writeFileSync(path.join(dir, fileName), text);
   }
   const model = await Model.create(
      "test-pkg",
      dir,
      entryFileName,
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

#(authorize) 1 = 1
source: child_relaxed is gated_parent extend {}
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

   it("a child declaring its OWN gate REPLACES the inherited one, not AND's/OR's with it", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // If the inherited gate (org_id in $GROUPS) were still active
         // alongside the own gate (org_id = 999) as an OR, GROUPS=[1] would
         // still match the org_id=1 rows. Only the OWN gate applying
         // (org_id=999, never true in this seed) proves NOT-OR.
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

         // `n=0` above is ALSO consistent with an AND (the base's `org_id in
         // $GROUPS` AND the own `org_id = 999` — both false-or-vacuous here,
         // same result either way), so it alone doesn't discriminate
         // override from AND. `child_relaxed` declares its own `1 = 1` over
         // the SAME gated base, queried with a GROUPS value that fails the
         // base's own condition for every row (`[999]` matches no
         // `org_id`) — this is the *relaxing* direction the locked-base /
         // curated-re-exposure idiom depends on: under AND, the base's
         // always-false-here condition would still deny everything (0 rows);
         // only a real REPLACE serves all 4.
         const relaxed = await model.getQueryResults(
            undefined,
            undefined,
            "run: child_relaxed -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [999] },
         );
         expect(
            (relaxed.compactResult as unknown as { n: number }[])[0].n,
         ).toBe(4);
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

   // The legacy quoted-string form's own load-time refusal
   // (`findLegacyStringGates`/`assertNoLegacyStringGate`) for a single-file,
   // top-level source is covered in `authorize_integration.spec.ts`'s "the
   // legacy quoted-string #(authorize) form" describe block ("still refuses
   // to load a genuinely top-level gate, naming the expression as authored")
   // — not duplicated here. This file's own cross-file describe block below
   // adds the two-hop-import variant of that same refusal, which was
   // previously untested anywhere.

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

   it("at most ONE gate per source: a block-list source with no derivation at all still refuses (sanity, single candidate)", async () => {
      // Same rule as above, through the block-list `source: a is ..., b is
      // ...` syntax instead of two separate `source:` statements — no
      // derivation anywhere in this model, so there is exactly one candidate
      // for `authorizeNoteDeclaredBy` and this must refuse under both the old
      // presence check and the new attribution-based one.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
#(authorize) org_id = 1
source:
  solo is duckdb.table('orgtable') extend {}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/"solo"/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("at most ONE gate per source: a block-list source list where the shared block note attributes to a SIBLING still refuses for its OWN second gate", async () => {
      // Regression for the fail-open finding 1 opened: Malloy puts the block
      // note `#(authorize) org_id in $GROUPS` AND `b`'s own item note
      // `#(authorize) owner in $GROUPS` both at `b`'s own annotation level,
      // and the block note is reference-identical to `a`'s. The
      // attribution-based `authorizeNoteDeclaredBy` correctly attributes the
      // block note to `a` (earliest, same file) — but that must narrow only
      // `validateAuthorizeProbes`'s own-vs-inherited signal, NOT this load
      // refusal: `b` still, by TEXT, carries two of its own `#(authorize)`
      // notes (the inherited block note plus its own item note), and a
      // source declaring two gates must be refused at load regardless of
      // which of the two the attribution heuristic thinks it "owns".
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source:
  a is duckdb.table('orgtable') extend { measure: n is count() },
  #(authorize) owner in $GROUPS
  b is a extend {}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/"b"/);
         expect(err?.message).toMatch(/org_id in \$GROUPS/);
         expect(err?.message).toMatch(/owner in \$GROUPS/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Fail-closed reversal + the misbinding hole.
//
// Each of these derives `gated_parent` in a way that breaks its OWN field
// space's ability to express the inherited gate. Written against
// `buildGatedModel` (which skips the `validateAuthorizeProbes` preflight)
// before the load-abort fix described in this file's header existed — the
// preflight now correctly classifies these as inherited-and-unexpressible
// rather than aborting, so `createModel` would reach request time too (see
// the "an inheriting derivation's unexpressible gate scopes to that entry
// point" describe block below for the same shapes through the real
// `Model.create`). Kept on `buildGatedModel` since these tests are about the
// GRAFT's own behavior at request time, not the load path.
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

describe("source-line #(authorize) — an inheriting derivation's unexpressible gate scopes to that entry point, not the whole load", () => {
   it("`except:`-ing the gated column: package loads, warns naming the derivation, and gated_parent itself is unaffected", async () => {
      // `validateAuthorizeProbes`'s own-vs-inherited check used to test
      // PRESENCE on `struct.annotations`, not who actually WROTE the note —
      // Malloy's by-reference copy of `gated_parent`'s own note lands on
      // `w_except`'s own `annotations` too (same mechanism as a plain
      // `extend {}`, documented in `gate_registry_walk.ts`'s header), which
      // used to read as "w_except declares its own gate" and throw when the
      // probe couldn't resolve `org_id` there, aborting the WHOLE package
      // load. `source_extraction.ts`'s `authorizeNoteDeclaredBy` (location-
      // based note ownership — see its doc) now resolves the note back to
      // `gated_parent`, so `w_except` reads as INHERITED-and-unexpressible:
      // the load succeeds, `onRowLevelGateUnexpressible` warns naming
      // `w_except`, and `w_except` alone denies at request time.
      const warnSpy = spyOn(logger, "warn");
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
         expect(compilationErrorOf(model)).toBeUndefined();
         // Match on the message text, not just a `sourceName` metadata key —
         // two DIFFERENT `logger.warn` call sites in `model.ts` carry a
         // `sourceName` (this one, and the separate gate-dimension warning),
         // so filtering on the key alone would accept a warning from either.
         const warnings = warnSpy.mock.calls
            .filter((c) =>
               String(c[0]).includes("not expressible at this entry point"),
            )
            .map((c) =>
               String(
                  (c as unknown as [string, { sourceName?: string }?])[1]
                     ?.sourceName ?? "",
               ),
            );
         expect(warnings).toContain("w_except");

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

         // `gated_parent` itself still serves correctly with a matching given.
         const gated = await model.getQueryResults(
            undefined,
            undefined,
            "run: gated_parent -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((gated.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("an `accept:` list omitting the gated column: package loads, warns naming the derivation, and denies at request time", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_accept is gated_parent extend { accept: id, val, n }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const warnings = warnSpy.mock.calls
            .filter((c) =>
               String(c[0]).includes("not expressible at this entry point"),
            )
            .map((c) =>
               String(
                  (c as unknown as [string, { sourceName?: string }?])[1]
                     ?.sourceName ?? "",
               ),
            );
         expect(warnings).toContain("w_accept");

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
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("a plain `extend {}` derivation of a gated base loads with NO warning at all — the probe never fails for this shape", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_plain is gated_parent extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const unexpressibleWarnings = warnSpy.mock.calls.filter((c) =>
            String(c[0]).includes("not expressible at this entry point"),
         );
         expect(unexpressibleWarnings).toHaveLength(0);

         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: w_plain -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("two siblings independently `except:`-ing the gated column each get their OWN warning and each independently deny", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_except_a is gated_parent extend { except: org_id }
source: w_except_b is gated_parent extend { except: org_id }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const warnedSources = warnSpy.mock.calls
            .filter((c) =>
               String(c[0]).includes("not expressible at this entry point"),
            )
            .map((c) =>
               String(
                  (c as unknown as [string, { sourceName?: string }?])[1]
                     ?.sourceName ?? "",
               ),
            );
         expect(warnedSources).toContain("w_except_a");
         expect(warnedSources).toContain("w_except_b");

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_except_a -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_except_b -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("the DECLARING source's own broken gate still aborts the whole load — this fix must not swallow a genuine authoring mistake at the site it was written", async () => {
      // `gated_parent` here references `nonexistent_column`, which it does
      // not have — its OWN probe fails, and `authorizeNoteDeclaredBy`
      // resolves the note back to `gated_parent` ITSELF (the earliest, and
      // only, candidate in this file), so this must still throw rather than
      // being waved through as "inherited".
      //
      // NOTE: this model has no derivation of `gated_parent` at all, so
      // there is exactly one candidate for `authorizeNoteDeclaredBy` and this
      // passes identically under the old presence check and the new
      // attribution check — it does not, by itself, exercise attribution.
      // See the two variants directly below, which add a derivation
      // alongside the broken gate.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) nonexistent_column in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toContain(
            'Invalid #(authorize) annotation on source "gated_parent"',
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the DECLARING source's own broken gate still aborts naming ITSELF, not a plain `extend {}` derivation present in the same model", async () => {
      // Unlike the test above, `child` (a plain `extend {}`) is ALSO present
      // — now there are TWO candidates sharing the by-reference-copied note,
      // and `authorizeNoteDeclaredBy` must resolve it back to `gated_parent`
      // (earliest position, same file), not to `child`. If attribution ever
      // picked the wrong candidate here, the error would name "child" instead
      // of "gated_parent", or the probe's own genuine authoring mistake would
      // get misclassified as "inherited-and-unexpressible" (a warning) rather
      // than aborting the load.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) nonexistent_column in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: child is gated_parent extend {}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toContain(
            'Invalid #(authorize) annotation on source "gated_parent"',
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the DECLARING source's own broken gate still aborts naming ITSELF, not an `except:` derivation present in the same model", async () => {
      // Same discrimination as above, through an `except:` derivation
      // instead of a plain `extend {}` — a different shape that also carries
      // the base's by-reference-copied note.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) nonexistent_column in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_except is gated_parent extend { except: org_id }
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toContain(
            'Invalid #(authorize) annotation on source "gated_parent"',
         );
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
         // `n=4` alone is also what a SILENTLY DROPPED gate would produce —
         // the catastrophic failure mode for this whole feature (a gate that
         // parses but never attaches). Pair it with introspection proving
         // the gate is actually present on `open_source` before trusting the
         // row count as "allow-everyone" rather than "no gate at all".
         const sources = model.getSources();
         expect(
            sources?.find((s) => s.name === "open_source")?.authorize,
         ).toEqual(["1 = 1"]);

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

   it("`#(authorize) false` — the deny-everyone locked-base idiom compiles and denies every request", async () => {
      // The pre-plan experiment measured `1 = 1` compiling as a source-level
      // `where:` but did not confirm a BARE `false` literal does too — Malloy
      // discriminates a bare boolean literal as its own IR node kind rather
      // than a generic comparison, and grammar support for it as a whole
      // `where:` expression (as opposed to a sub-expression) was unverified.
      // MEASURED (real Model.create + real DuckDB): it compiles clean and
      // denies every request, 200/zero-rows, exactly like `1 = 1`/`1 = 0` do
      // — asserted unconditionally now that the outcome is known, not
      // branched on the observed result (a branch that passes under either
      // outcome is not evidence of either one).
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) false
source: locked_out is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: locked_out -> { aggregate: n is count() }",
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

describe("source-line #(authorize) referencing a field whose OWN expression reads a given", () => {
   it("`#(authorize) authorized` over `dimension: authorized is org_id in $GROUPS`, given unsupplied, DENIES opaquely (403) rather than leaking $GROUPS in a MalloyError", async () => {
      // `refSummary.givenUsage` is populated only for a DIRECT given
      // reference in the annotated expression itself — a bare field
      // reference like `authorized` carries no `givenUsage` of its own even
      // though `authorized`'s OWN expression (`org_id in $GROUPS`) does. A
      // classifier that read `condition.refSummary?.givenUsage` directly
      // (the bug this test exists to catch) would see an empty given set for
      // this gate, so `authorizeReferencedGivenNames` would never learn
      // about `GROUPS`, and the query-time opaque-403 backstop
      // (`queryHadRowLevelFilterAttached` + membership check, `model.ts`)
      // would stay blind — the request would instead fail with Malloy's raw
      // given-binding `MalloyError`, naming `GROUPS` directly to an
      // unauthenticated caller. The fix (`expandRefSummaryGivenIds` walking
      // `fieldUsage` transitively) must resolve `authorized` -> `GROUPS` and
      // feed it into `authorizeReferencedGivenNames` so this same failure
      // instead surfaces as an opaque `AccessDeniedError`.
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

source: field_ref_gated is duckdb.table('orgtable') extend {
   dimension: authorized is org_id in $GROUPS
   measure: n is count()
}

#(authorize) authorized
source: gated_by_field_ref is field_ref_gated extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         let caught: unknown;
         try {
            await model.getQueryResults(
               undefined,
               undefined,
               "run: gated_by_field_ref -> { aggregate: n is count() }",
               {},
               true,
               {}, // GROUPS deliberately unsupplied.
            );
         } catch (err) {
            caught = err;
         }
         expect(caught).toBeInstanceOf(AccessDeniedError);
         expect((caught as Error).message).not.toContain("GROUPS");
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Cross-file coverage. Every test above builds a single-file model, so
// `considerAuthorizeNoteOwner`'s `location.url !== note.at.url` comparison —
// the one thing the location heuristic does that a bare presence check
// doesn't — was previously never exercised anywhere in this suite.
// ---------------------------------------------------------------------------

describe("source-line #(authorize) — cross-file attribution", () => {
   it("one import hop: a derivation with no annotation of its own attributes correctly to the IMPORTED base, not to itself", async () => {
      // `gated_base` is declared and gated in `base.malloy`. `m.malloy`
      // imports it and derives `derived` via `except:`, which breaks
      // `derived`'s own field space's ability to express the inherited gate.
      // `derived`'s own struct carries the SAME note object as `gated_base`
      // by reference (the by-reference-copy mechanism, same as the
      // single-file case) — but `derived.location` is in `m.malloy` while
      // `note.at.url` names `base.malloy`, so `considerAuthorizeNoteOwner`
      // never even considers `derived` a candidate. `gated_base` (same file
      // as the note) IS a candidate and is the one `authorizeNoteDeclaredBy`
      // resolves to. This proves the cross-file comparison does real work:
      // it is what keeps a plain same-file presence check from crediting
      // `derived` with declaring a gate it merely inherited.
      const BASE = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_base is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
      const M = `##! experimental.givens
import { gated_base, GROUPS } from "base.malloy"

source: derived is gated_base extend { except: org_id }
`;
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModelWithFiles(
         { "base.malloy": BASE, "m.malloy": M },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const warnings = warnSpy.mock.calls
            .filter((c) => String(c[0]).includes("not expressible"))
            .map((c) =>
               String(
                  (c as unknown as [string, { sourceName?: string }?])[1]
                     ?.sourceName ?? "",
               ),
            );
         expect(warnings).toContain("derived");
         expect(warnings).not.toContain("gated_base");

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: derived -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);

         // `gated_base` itself is unaffected — still serves correctly.
         const gated = await model.getQueryResults(
            undefined,
            undefined,
            "run: gated_base -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((gated.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("cross-file attribution still can't reach two import hops for the entry_point_unexpressible label, but that no longer risks a G4 bypass", async () => {
      // `gated_base` is declared in `base.malloy`. `mid.malloy` imports it
      // and derives `mid_src` (a plain `extend {}`, itself one hop from
      // `gated_base`). `m.malloy` imports `mid.malloy` and derives `m_src`
      // (an `except:`, TWO hops from `gated_base`).
      //
      // Malloy merges only ONE import level into a model's own
      // `modelDef.contents`/`sourceRegistry`, so `gated_base` never appears
      // in `m.malloy`'s own compile as a full `SourceDef` — only as a
      // `source_registry_reference`, which the attribution sweep skips (see
      // `extractSourcesFromModelDef`'s doc). No candidate's `location.url`
      // can ever match the note's `at.url` (`base.malloy`) for `m.malloy`'s
      // own compile, so `authorizeNoteDeclaredBy` gets NO entry for this note
      // at all — not `gated_base`, not `mid_src`, not `m_src`.
      //
      // THIS TEST USED TO DOCUMENT THAT GAP AS A MERE BLAME-PRECISION LOSS —
      // that characterization was wrong. `validateAuthorizeProbes` used to
      // gate its G4/W1/W2 check (`onOwnRowLevelConditionCompiled`) on this
      // same attribution map, so an attribution gap at import depth didn't
      // just mislabel a genuinely broken gate's rejection cause — it SKIPPED
      // G4 there entirely. Combined with an entry model that re-declares the
      // gate's given WITH a default (see "the exact repro" test below, which
      // this file's fixture set never exercised until now), that skip let a
      // model load clean and admit a caller-defaulted value it was never
      // supposed to see. See `authorize.ts`'s `onOwnRowLevelConditionCompiled`
      // doc for the fix: G4/W1/W2 now run at EVERY entry point whose probe
      // compiles, not only the one attribution can blame.
      //
      // What survives, and what THIS test still pins, is narrower: the
      // `entry_point_unexpressible` REJECTION-CAUSE LABEL is still
      // attribution-limited. `m_src` here genuinely cannot express
      // `gated_base`'s gate (the `except: org_id` below), and there is no
      // defaulted given anywhere in this model to trip G4 on, so it is
      // diagnosed the same "inherited and unexpressible" way whether or not
      // attribution could name the true declarer. That throw-vs-warn
      // distinction is a separate, deliberate axis (see `authorize.ts`'s
      // module doc) and is unaffected by the G4 fix: the load still
      // succeeds, and the entry point that cannot express the gate still
      // DENIES at request time.
      const BASE = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated_base is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
      const MID = `##! experimental.givens
import { gated_base, GROUPS } from "base.malloy"

source: mid_src is gated_base extend {}
`;
      const M = `##! experimental.givens
import { mid_src, GROUPS } from "mid.malloy"

source: m_src is mid_src extend { except: org_id }
`;
      const { model, duckdb, dir } = await createModelWithFiles(
         { "base.malloy": BASE, "mid.malloy": MID, "m.malloy": M },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: m_src -> { aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the exact repro — G4 refuses a defaulted given two import hops out, where it used to load clean and admit the default (security fix)", async () => {
      // `gated_base` (`base.malloy`) gates on `$TENANT`, declared with NO
      // default. `mid.malloy` imports it and derives `mid_src` (a plain
      // `extend {}`, one hop). `m.malloy` imports `mid.malloy`, derives
      // `passthru` (a plain `extend {}`, TWO hops from `gated_base`), and
      // — the crux — re-declares `TENANT` itself, WITH a default of `1`.
      //
      // Before the fix, this loaded with `compilationError` undefined and a
      // caller supplying NO givens at all got back rows filtered on the
      // default `org_id = 1`: `validateAuthorizeProbes` gated its G4 check
      // on `attributedAuthorizeOwnNotes`, which cannot reach `gated_base`
      // from `m.malloy`'s own compile at two import hops (see the
      // attribution test above), so the callback that runs G4 never fired
      // for `mid_src` or `passthru` even though each one's own probe
      // compiled successfully and its lifted condition already carried the
      // exact given id (`m.malloy`'s own `TENANT`, default `1`) the
      // request-time graft goes on to bind. Collapsing `mid.malloy` so
      // `m.malloy` imports
      // `base.malloy` directly (one hop) was correctly refused throughout —
      // this is the identical defaulted-given shape, differing only in
      // import depth.
      //
      // After the fix, G4 runs unconditionally on every entry point whose
      // probe compiles, so this is refused at load exactly like the
      // one-hop shape.
      const BASE = `##! experimental.givens
given: TENANT :: number

#(authorize) org_id = $TENANT
source: gated_base is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
      const MID = `##! experimental.givens
import { gated_base } from "base.malloy"

source: mid_src is gated_base extend {}
`;
      const M = `##! experimental.givens
import { mid_src } from "mid.malloy"
given: TENANT :: number is 1

source: passthru is mid_src extend {}
`;
      const { model, duckdb, dir } = await createModelWithFiles(
         { "base.malloy": BASE, "mid.malloy": MID, "m.malloy": M },
         "m.malloy",
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toMatch(/\$TENANT.*declared with a default/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the legacy string form at the same two-hop shape is STILL refused at LOAD time — unlike the attribution gap above, presence-based detection does not depend on import depth", async () => {
      // Same two-hop shape as the attribution test above, but
      // `gated_base` uses the legacy quoted-string form. One might expect the
      // same import-depth blindness to apply here too — but it does not:
      // `findLegacyStringGates`/`assertNoLegacyStringGate` read the
      // PRESENCE-based `authorizeOwnNotes` (see `extractSourcesFromModelDef`'s
      // doc for why finding 1's fix keeps that map presence-only), which
      // checks only whether a struct's OWN annotations literally carry an
      // `#(authorize)`-tagged note — with no reference to WHERE that note was
      // declared or how many import hops away. Malloy's by-reference copy
      // carries the note onto `mid_src` and then `m_src` regardless of import
      // depth, so both are still caught and the load is refused, naming both,
      // exactly as the single-file case is (see
      // `authorize_integration.spec.ts`'s "still refuses to load a genuinely
      // top-level gate" test). MEASURED: this is a byproduct of finding 1's
      // fix, not something this round set out to fix — the presence-based
      // refusal was never attribution-limited to begin with, only the
      // now-separate `validateAuthorizeProbes` diagnostic is.
      const BASE = `##! experimental.givens

#(authorize) "org_id = 999"
source: gated_base is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
      const MID = `##! experimental.givens
import { gated_base } from "base.malloy"

source: mid_src is gated_base extend {}
`;
      const M = `##! experimental.givens
import { mid_src } from "mid.malloy"

source: m_src is mid_src extend {}
`;
      const { model, duckdb, dir } = await createModelWithFiles(
         { "base.malloy": BASE, "mid.malloy": MID, "m.malloy": M },
         "m.malloy",
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(ModelCompilationError);
         expect(err?.message).toContain("no longer accepted");
         expect(err?.message).toContain('"mid_src"');
         expect(err?.message).toContain('"m_src"');
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// W1/W2 producers. `validateSourceLineGateGivenUsage`'s two `onWarning` calls
// (`gate_dimension.ts`) had no producing test at all: deleting both left the
// whole suite green, in the very commit that added them. The nearest coverage
// was `authorize_metrics.spec.ts` calling `recordRowLevelGateRejected` with
// each cause by hand, which pins the counter's plumbing and says nothing about
// whether a real model load ever reaches it.
//
// These go through the real `Model.create` and assert on `logger.warn`'s
// `cause` field, which is the operator-facing surface (the paired
// `publisher_authorize_row_level_rejected_total{cause=...}` increment happens
// in the same callback, so it is reached by the same evidence). Both gates
// LOAD -- W1 and W2 warn rather than refuse -- so each test also asserts the
// model compiled, keeping "warned" distinct from "refused".
// ---------------------------------------------------------------------------

/** The `cause` values `logger.warn("Row-level #(authorize) gate warning", …)`
 *  carried during this model load, in call order. */
function gateWarningCauses(
   warnSpy: ReturnType<typeof spyOn<typeof logger, "warn">>,
): string[] {
   return warnSpy.mock.calls
      .filter((c) => String(c[0]) === "Row-level #(authorize) gate warning")
      .map((c) =>
         String(
            (c as unknown as [string, { cause?: string }?])[1]?.cause ?? "",
         ),
      );
}

describe("source-line gate authoring warnings (W1/W2) fire from a real model load", () => {
   it("W1: a gate referencing no given warns `source_line_gate_no_given_reference` and still loads", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

#(authorize) 1 = 1
source: fixed_gate is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(gateWarningCauses(warnSpy)).toContain(
            "source_line_gate_no_given_reference",
         );
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("W2: a gate negating a membership test warns `source_line_gate_negated_membership` and still loads", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) not (org_id in $GROUPS)
source: negated_gate is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const causes = gateWarningCauses(warnSpy);
         expect(causes).toContain("source_line_gate_negated_membership");
         // Discriminating, not merely non-empty: this gate DOES reference a
         // given, so W1 must stay silent. A callback that fired
         // unconditionally would satisfy the assertion above.
         expect(causes).not.toContain("source_line_gate_no_given_reference");
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });

   it("an ordinary gate warns neither", async () => {
      const warnSpy = spyOn(logger, "warn");
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: plain_gate is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(gateWarningCauses(warnSpy)).toEqual([]);
      } finally {
         warnSpy.mockRestore();
         await cleanup(duckdb, dir);
      }
   });
});
