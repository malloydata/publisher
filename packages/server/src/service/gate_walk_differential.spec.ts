// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Differential spec over the three gate-inheritance walks Publisher runs
 * today, one per consumer:
 *
 *   A. `collectEntryPointGates` (./gate_classification.ts): the request path,
 *      what enforcement decides and grafts.
 *   B. `gateExprsForRoute` (./gate_registry_walk.ts): package load, what
 *      `source_extraction.ts` reports as a source's `authorize` /
 *      `accessFilter` (and so what get_context and the access record show).
 *   C. `resolveGates` (./entities/access_gates.ts): the value-index walk over
 *      raw `structDef` JSON (`dim_index_discovery.ts`).
 *
 * For every source in every fixture package (../test_helpers/gate_shape_fixtures.ts),
 * each walk's gate set per route (expressions, whitespace-normalized, order,
 * grouping and duplicates ignored) must agree, modulo the documented
 * composite rule: on a bare composite C reports, per route, its own gate else
 * its FIRST GATED MEMBER's (a deliberate over-report, since an index cannot
 * know which branch a query takes), while A and B read only the composite's
 * own gate and leave the member to the branch Malloy resolves per query. A
 * source DERIVED from a composite (`qs is combo -> {...}`) has its branch
 * resolved at compile time, and there all three agree.
 *
 * Phase 3 (V3) replaces the three walks with one `gatePlan`; this spec is its
 * guardrail. Where the walks disagree beyond the composite rule, the
 * disagreement is pinned under an `it` naming it, with the reason and a control
 * showing what enforcement actually does. Those are inputs to `gatePlan`, not
 * bugs fixed here: no production code changes with this spec.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   expectAccessDenied,
   queryRows,
   withAuthorizeModel,
} from "../test_helpers/authorize_fixture";
import {
   GATE_WALK_PACKAGES,
   inheritanceMatrixPackage,
} from "../test_helpers/gate_shape_fixtures";
import { ACCESS_FILTER_ROUTE, AUTHORIZE_ROUTE } from "./authorize_routes";
import { resolveGates, type GateStructDef } from "./entities/access_gates";
import { collectEntryPointGates } from "./gate_classification";
import { gateExprsForRoute } from "./gate_registry_walk";

const ROOT = "file:///gate-walk-differential/";
const ROUTES = [AUTHORIZE_ROUTE, ACCESS_FILTER_ROUTE] as const;

/** One walk's answer for one source: the gate exprs per route, or what it threw. */
type WalkResult =
   | { authorize: string[]; access_filter: string[] }
   | { throws: string };

interface SourceVerdict {
   A: WalkResult;
   B: WalkResult;
   C: WalkResult;
}

const norm = (expr: string) => expr.replace(/\s+/g, " ").trim();

const perRoute = (exprsOf: (route: string) => string[]): WalkResult => {
   const [authorize, access_filter] = ROUTES.map((route) =>
      Array.from(new Set(exprsOf(route).map(norm))).sort(),
   );
   return { authorize, access_filter };
};

const guard = (run: () => WalkResult): WalkResult => {
   try {
      return run();
   } catch (err) {
      return { throws: (err as Error).message };
   }
};

/** Compile `entry` of `files` and run all three walks over every source it exposes. */
async function verdictsFor(
   duckdb: DuckDBConnection,
   files: Record<string, string>,
   entry: string,
): Promise<Map<string, SourceVerdict>> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map(
            Object.entries(files).map(([name, text]) => [
               `${ROOT}${name}`,
               text,
            ]),
         ),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL(`${ROOT}${entry}`), { importBaseURL: new URL(ROOT) })
      .getModel();
   const modelDef: ModelDef = model._modelDef;
   // C's string-ref lookup, built the way `dim_index_discovery.ts` builds it.
   const sourceDefsByName = new Map<string, GateStructDef>(
      model.explores.map((e) => [e.name, e.structDef as GateStructDef]),
   );
   const out = new Map<string, SourceVerdict>();
   for (const explore of model.explores) {
      const struct = modelDef.contents[explore.name] as SourceDef | undefined;
      if (!struct) continue;
      const A = guard(() => {
         // As the request path calls it: the run target is the entry point.
         const entries = collectEntryPointGates(
            struct,
            modelDef,
            new Set(),
            true,
         );
         return perRoute((route) =>
            entries.filter((e) => e.route === route).flatMap((e) => e.exprs),
         );
      });
      const B = guard(() =>
         perRoute((route) => gateExprsForRoute(struct, modelDef, route).flat()),
      );
      const C = guard(() => {
         const { gates } = resolveGates(
            struct as unknown as GateStructDef,
            sourceDefsByName,
         );
         return perRoute((route) =>
            gates.filter((g) => g.route === route).map((g) => g.expr),
         );
      });
      out.set(explore.name, { A, B, C });
   }
   return out;
}

/** Every source of every fixture, keyed `package › entry › source`. */
const verdicts = new Map<string, SourceVerdict>();
let duckdb: DuckDBConnection;

beforeAll(async () => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const pkg of GATE_WALK_PACKAGES) {
      const files = pkg.files();
      for (const entry of Object.keys(files)) {
         for (const [source, verdict] of await verdictsFor(
            duckdb,
            files,
            entry,
         )) {
            verdicts.set(`${pkg.name} › ${entry} › ${source}`, verdict);
         }
      }
   }
});
afterAll(async () => {
   await duckdb.close();
});

const MATRIX = "inheritance matrix › matrix.malloy";

/**
 * Bare composites, where C follows the documented composite rule: per route,
 * the composite's own gate, else its first gated member's. A and B report
 * the composite's own gate only.
 */
const COMPOSITE_RULE: Record<string, WalkResult> = {
   // compose(locked, open): locked's lock is the first gated member.
   [`${MATRIX} › comp_bare`]: {
      authorize: ["'admin' in $ROLES"],
      access_filter: [],
   },
   // compose(open, scoped): the first member is open, so scoped's filter.
   [`${MATRIX} › comp_bare_second_gated`]: {
      authorize: [],
      access_filter: ["org_id in $ORGS"],
   },
   // Own lock on the authorize route; on access_filter, none of its own, so
   // the first member gated on THAT route (scoped, not locked).
   [`${MATRIX} › comp_own`]: {
      authorize: ["'composer' in $ROLES"],
      access_filter: ["org_id in $ORGS"],
   },
};

/** Disagreements beyond the composite rule, each pinned by its own `it` below. */
const PINNED_DISAGREEMENTS = new Set([
   `${MATRIX} › qs_then_extend_own`,
   `${MATRIX} › sql_embeds_locked`,
]);

const verdict = (key: string): SourceVerdict => {
   const v = verdicts.get(key);
   if (!v) throw new Error(`no source ${key} in the fixtures`);
   return v;
};

describe("gate walk differential", () => {
   it("covers every fixture package and the shapes it names", () => {
      for (const pkg of GATE_WALK_PACKAGES) {
         expect(
            [...verdicts.keys()].some((k) => k.startsWith(`${pkg.name} › `)),
         ).toBe(true);
      }
      // Every exception still names a real source, so a renamed fixture
      // cannot silently drop one from the comparison.
      for (const key of [
         ...Object.keys(COMPOSITE_RULE),
         ...PINNED_DISAGREEMENTS,
      ]) {
         verdict(key);
      }
      // Guards against a walk that answers "no gate" everywhere agreeing
      // trivially: most fixture sources are gated.
      const gated = [...verdicts.values()].filter(
         ({ A }) =>
            "authorize" in A && A.authorize.length + A.access_filter.length > 0,
      );
      expect(gated.length).toBeGreaterThan(30);
   });

   it("the three walks agree on every source outside the composite rule and the pinned disagreements", () => {
      const disagreements = [...verdicts.entries()]
         .filter(
            ([key]) =>
               !(key in COMPOSITE_RULE) && !PINNED_DISAGREEMENTS.has(key),
         )
         .filter(
            ([, { A, B, C }]) =>
               JSON.stringify(A) !== JSON.stringify(B) ||
               JSON.stringify(B) !== JSON.stringify(C),
         )
         .map(([key, v]) => ({ key, ...v }));
      expect(disagreements).toEqual([]);
   });

   it("composite rule: on a bare composite C reports the first gated member, A and B the composite's own gate", () => {
      for (const [key, expectedC] of Object.entries(COMPOSITE_RULE)) {
         const { A, B, C } = verdict(key);
         expect({ key, B }).toEqual({ key, B: A });
         expect({ key, C }).toEqual({ key, C: expectedC });
      }
      // The composite's own gate (or none) is what A reports.
      expect(verdict(`${MATRIX} › comp_bare`).A).toEqual({
         authorize: [],
         access_filter: [],
      });
      expect(verdict(`${MATRIX} › comp_own`).A).toEqual({
         authorize: ["'composer' in $ROLES"],
         access_filter: [],
      });
   });

   it("composite rule: a source derived from a composite has one resolved branch, and all three agree", () => {
      for (const name of ["qs_over_comp", "qs_over_comp_second"]) {
         const { A, B, C } = verdict(`${MATRIX} › ${name}`);
         expect(B).toEqual(A);
         expect(C).toEqual(A);
      }
   });

   it("PINNED DISAGREEMENT: B over-reports the replaced gate on an extension of a query-derived source", async () => {
      // `qs_then_extend_own` is `qs_own_over_locked extend { }` carrying its
      // own `#(access_filter) region = $REGION`, over
      // `#(access_filter) org_id in $ORGS  qs_own_over_locked is locked -> {...}`.
      //
      // Malloy keeps an extension of a query_source a `query_source`, so B's
      // `gateExprsForRoute` takes its "derivation is additive" branch and
      // returns own + inherited on the same route, reporting BOTH filters. A
      // and C apply extend's own-wins-over-ancestor on that route (region
      // replaces org_id) and then follow `query.structRef`, which still names
      // `locked`, for its lock. Enforcement is A, so load-time metadata (and
      // the access record built from it) lists a filter that is not applied.
      // B errs toward over-reporting, never toward a missing gate.
      //
      // For `gatePlan` (Phase 3): decide whether an extension of a
      // query_source is "extend" (replace on the same route) or "derivation"
      // (additive), and make the report match enforcement.
      const { A, B, C } = verdict(`${MATRIX} › qs_then_extend_own`);
      expect(A).toEqual({
         authorize: ["'admin' in $ROLES"],
         access_filter: ["region = $REGION"],
      });
      expect(C).toEqual(A);
      expect(B).toEqual({
         authorize: ["'admin' in $ROLES"],
         access_filter: ["org_id in $ORGS", "region = $REGION"],
      });

      // Control: enforcement does what A says. With no ORGS the query is
      // still answered (org_id is not grafted), filtered by REGION only.
      await withAuthorizeModel(
         inheritanceMatrixPackage(),
         async ({ model, error }) => {
            expect(error).toBeUndefined();
            expect(
               await queryRows(
                  model,
                  "run: qs_then_extend_own -> { group_by: region, org_id }",
                  { ROLES: ["admin"], REGION: "east" },
               ),
            ).toEqual([{ region: "east", org_id: "o1" }]);
            // And the lock A reports is enforced.
            await expectAccessDenied(
               queryRows(
                  model,
                  "run: qs_then_extend_own -> { group_by: region }",
                  {
                     ROLES: ["viewer"],
                     REGION: "east",
                  },
               ),
            );
         },
         "matrix.malloy",
      );
   });

   it("PINNED DISAGREEMENT: C refuses a sql_select embedding a gated source, A and B report no gate", async () => {
      // `sql_embeds_locked is duckdb.sql("""... %{ locked -> {...} } ...""")`.
      // A and B never look inside `selectSegments`, so they report the source
      // ungated. C throws (its caller withholds the source from the value
      // index: fail closed) because it cannot walk the embedded query.
      //
      // For `gatePlan` (Phase 3): the embedded query's source is reachable and
      // gated; a single walk must decide whether that gate applies to the
      // sql_select and report it consistently.
      const { A, B, C } = verdict(`${MATRIX} › sql_embeds_locked`);
      expect(A).toEqual({ authorize: [], access_filter: [] });
      expect(B).toEqual(A);
      expect(C).toEqual({
         throws:
            "resolveGates does not walk an embedded query/source in sql_select.selectSegments",
      });
      // Control: enforcement agrees with A. A caller with no ROLES is answered
      // with `locked`'s rows through the author's sql_select. This is the
      // model AUTHOR's construct (restricted mode refuses `duckdb.sql` in
      // caller text), the same stance as a join, which is not walked either;
      // but unlike a join it reads `locked` as a derivation would, and a
      // derivation (`->`) is additive. Pinned so `gatePlan` changes it on
      // purpose, not by accident.
      await withAuthorizeModel(
         inheritanceMatrixPackage(),
         async ({ model }) => {
            const outcome = await queryRows(
               model,
               "run: sql_embeds_locked -> { aggregate: n is count() }",
               {},
            ).then(
               (rows) => ({ rows }),
               (err: Error) => ({ refused: err.constructor.name }),
            );
            expect(outcome).toEqual({ rows: [{ n: 2 }] });
         },
         "matrix.malloy",
      );
   });
});
