// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07: a `#(materialize)` table that lacks a list column its source
 * declares (a `storage=` table built before its build stored arrays, such as
 * a BigQuery `ARRAY<STRING>`, until it is next built in full; see
 * storage_array_columns.component.spec.ts) leaves that column out per query, as
 * docs/materialization.md says of a stored list no declaration reads back:
 * a query that does not read it is served from the table, and a query that
 * reads it (directly, in a filter, a nest, or through the model's own
 * measure) is answered live. It no longer sends the whole source live.
 *
 * The table is made as such a build left it: built, then the list column
 * dropped in the destination, with the run that bound it carrying the
 * schema without it. A list stored as a scalar is left out the same way.
 *
 * Gates and givens are unchanged on both paths: a gated source still scopes
 * each caller to its own rows from the table, answers a query reading the
 * list live under the same gate, and refuses a caller with no given.
 *
 * Where a source's own text reads the missing list, its served copy cannot
 * compile without it and the source answers live as a whole: the
 * `served-copy-not-served` problem names the column, its declared type and
 * that the table does not hold it, and so does the path report's `detail`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../../src/errors";
import {
   installSettings,
   useMaterializationOn,
} from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const SEED = [
   `CREATE TABLE ex AS SELECT * FROM (VALUES
      (1, 'acme', 1.5, ['x', 'y'], [{'a': 'p', 'n': 1}]),
      (2, 'acme', 2.5, ['y'], [{'a': 'r', 'n': 3}, {'a': 's', 'n': 4}]),
      (3, 'globex', 3.5, ['x', 'z', 'w'], [{'a': 'p', 'n': 6}]),
      (4, 'globex', 4.5, []::VARCHAR[], [{'a': 'q', 'n': 2}])) AS t(id, org, amount, v, lr)`,
];

const MODEL = `#(materialize) storage="lake"
source: s is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: reads_v is duckdb.table('ex') extend {
  primary_key: id
  measure: nv is count(v.each)
}

#(materialize) storage="lake"
source: scalar_v is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}
`;

/** As an older build left it: no list `column` written, nor bound. */
async function dropList(
   fixture: R4Fixture,
   source: string,
   column = "v",
): Promise<void> {
   await fixture.storedSQL(
      (t) => `ALTER TABLE ${t(source)} DROP COLUMN ${column}`,
   );
   for (const b of fixture.bindings.filter((b) => b.sourceName === source)) {
      expect(b.schema.map((c) => c.name)).toContain(column);
      b.schema = b.schema.filter((c) => c.name !== column);
   }
}

async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
   } catch (error) {
      return error;
   }
   return undefined;
}

describe("F-07: a #(materialize) storage= table missing a declared list column", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: SEED,
         materialize: true,
      });
      // As an older build left it: no `v` written.
      for (const source of ["s", "reads_v"]) await dropList(fixture, source);
      // A list of records, missing the same way.
      await dropList(fixture, "s", "lr");
      // A list stored as text: mistyped rather than missing.
      await fixture.storedSQL(
         (t) => `ALTER TABLE ${t("scalar_v")} ALTER v TYPE VARCHAR`,
      );
      for (const b of fixture.bindings.filter(
         (b) => b.sourceName === "scalar_v",
      )) {
         b.schema = b.schema.map((c) =>
            c.name === "v" ? { ...c, type: "VARCHAR" } : c,
         );
      }
   }, 120_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it("re-roots every source that does not read the list in its own text; the one that does is named with the column", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["reads_v", "column_types"],
      ]);
      expect(problems[0].detail).toBe(
         "column `v` (declared `string[]`) is not in the stored table",
      );
   });

   describe.each(["s", "scalar_v"])("%s", (source) => {
      it.each([
         `run: ${source} -> { aggregate: n }`,
         `run: ${source} -> { group_by: org; aggregate: n, total is amount.sum(); order_by: org }`,
         `run: ${source} -> { select: id, amount; order_by: id }`,
         `run: ${source} -> { where: amount > 2; aggregate: n }`,
      ])(
         "serves a query that does not read it from the table, equal to live: %s",
         async (malloy) => {
            const run = await fixture.run({ name: malloy, malloy });
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBe("served_copy");
            expect(run.servedFrom).toBe("storage");
         },
      );

      it.each([
         `run: ${source} -> { select: id, v; order_by: id }`,
         `run: ${source} -> { aggregate: m is count(v.each) }`,
         `run: ${source} -> { group_by: e is v.each; aggregate: c is count(); order_by: e }`,
         `run: ${source} -> { group_by: org; nest: es is { group_by: e is v.each; order_by: e }; order_by: org }`,
         `run: ${source} -> { where: v.each = 'x'; aggregate: c is count() }`,
      ])(
         "answers a query reading it live, equal to live, naming the column: %s",
         async (malloy) => {
            const run = await fixture.run({
               name: malloy,
               malloy,
               expectLive: true,
            });
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBeUndefined();
            expect(run.servedFrom).not.toBe("storage");
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy);
            expect(path?.servedBy?.kind).toBe("live");
            const considered = path?.considered ?? [];
            expect(considered.map((c) => [c.outcome, c.reason])).toEqual([
               ["skipped", "MISSING_FIELD"],
            ]);
            expect(considered[0].detail).toContain("column `v`");
         },
      );
   });

   it.each([
      "run: s -> { select: id, lr; order_by: id }",
      "run: s -> { group_by: lr.a; aggregate: t is lr.n.sum(); order_by: a }",
   ])(
      "answers a query reading a missing list of records live, naming it: %s",
      async (malloy) => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.[0]?.detail).toContain(
            "column `lr` (declared `{ a::string, n::number }[]`) is not in the stored table",
         );
      },
   );

   it("names the stored and declared types of a list stored as a scalar in the path detail", async () => {
      const malloy = "run: scalar_v -> { select: id, v; order_by: id }";
      fixture.setBindings(fixture.bindings);
      const { path } = await fixture.runAsIs(malloy);
      expect(path?.considered?.[0]?.detail).toContain(
         "column `v` (declared `string[]`, stored as VARCHAR)",
      );
   });

   it("answers a source whose own measure reads the list live as a whole, equal to live, and the path detail names the column", async () => {
      for (const malloy of [
         "run: reads_v -> { aggregate: nv }",
         "run: reads_v -> { group_by: org; aggregate: c is count(); order_by: org }",
      ]) {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.servedBy?.kind).toBe("live");
         expect(
            (path?.considered ?? []).map((c) => [
               c.outcome,
               c.reason,
               c.code,
               c.detail,
            ]),
         ).toEqual([
            [
               "skipped",
               "NOT_SERVED",
               "served_copy",
               "column `v` (declared `string[]`) is not in the stored table",
            ],
         ]);
      }
   });
});

const GATED_MODEL = `##! experimental { givens }
given: ORG :: string

#(access_filter) org = $ORG
#(materialize) storage="lake"
source: gated is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}
`;

describe.each([true, false])(
   "F-07, gated (gated serving %p): the same gate and givens on both paths",
   (gatedServing) => {
      let fixture: R4Fixture;
      let restore: () => void;
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               ...(gatedServing
                  ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
                  : {}),
            },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": GATED_MODEL },
            seedSql: SEED,
            materialize: true,
         });
         await dropList(fixture, "gated");
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("re-roots the gated source", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it("scopes each caller to its own rows from the table", async () => {
         for (const [org, n] of [
            ["acme", 2],
            ["globex", 2],
         ] as const) {
            const run = await fixture.run({
               name: org,
               malloy: "run: gated -> { aggregate: n, total is amount.sum() }",
               givens: { ORG: org },
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.served).toEqual([{ n, total: org === "acme" ? 4 : 8 }]);
            expect(run.servedFrom).toBe(gatedServing ? "storage" : null);
         }
      });

      it("answers a caller's query reading the list live, under the caller's gate", async () => {
         const malloy =
            "run: gated -> { group_by: e is v.each; aggregate: c is count(); order_by: e }";
         const acme = await fixture.run({
            name: "acme",
            malloy,
            givens: { ORG: "acme" },
            expectLive: true,
         });
         expect(rowDifference(acme.live, acme.served)).toBeUndefined();
         expect(acme.served).toEqual([
            { e: "x", c: 1 },
            { e: "y", c: 2 },
         ]);
         expect(acme.servedFrom).not.toBe("storage");
         const globex = await fixture.run({
            name: "globex",
            malloy,
            givens: { ORG: "globex" },
            expectLive: true,
         });
         expect(rowDifference(globex.live, globex.served)).toBeUndefined();
         // Row 4's empty list unnests to one null element.
         expect(globex.served.map((r) => r.e)).toEqual(["w", "x", "z", null]);
      });

      it.each([
         "run: gated -> { aggregate: n }",
         "run: gated -> { select: id, v }",
      ])(
         "refuses a caller with no given, from the table as live: %s",
         async (malloy) => {
            fixture.setBindings([]);
            const live = await failure(() => fixture.runAsIs(malloy));
            expect(live).toBeInstanceOf(AccessDeniedError);
            fixture.setBindings(fixture.bindings);
            const served = await failure(() => fixture.runAsIs(malloy));
            expect(served).toBeInstanceOf(AccessDeniedError);
         },
      );
   },
);
