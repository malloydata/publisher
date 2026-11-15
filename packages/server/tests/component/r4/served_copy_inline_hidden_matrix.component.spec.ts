// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 regression matrix: an inlined join (a join_one's record, or a
 * join_many's pair table) beside a hidden column on the same source, in
 * every shape the served copy hides a column (an `include` on the source's
 * own head or its base's, a hidden dimension of its own or a base's, a
 * hidden timeframe), with the join public, internal or private, on
 * `storage=` and colocated. Each must be served from its table, equal to
 * live, with no served-copy problem.
 *
 * A join_one beside a hidden column once made the served copy keep the join
 * as written ("Can't find field 'carriers__code' to set access modifier");
 * #1678 fixed it by hiding join columns with a `private rename:` in the
 * re-root's first block. A pair table beside a hidden column kept under its
 * own name (the `include` path) failed the same way on its own renames
 * (known limitation 30, its pair half): they are now written ahead of the
 * include. The one shape still refused is an author `rename:` behind that
 * include (limitation 30's inverse-rename half, Malloy core): that source
 * alone answers live, `definition_not_parsed`.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE carriers AS SELECT * FROM (VALUES
      ('AA', 'American'), ('UA', 'United')) AS t(code, nickname)`,
   `CREATE TABLE legs AS SELECT * FROM (VALUES
      (1, 1, 5.0), (2, 1, 6.0), (3, 2, 7.0)) AS t(lid, fid, miles)`,
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA', 100.0, 's1', TIMESTAMP '2020-01-02 03:00:00'),
      (2, 'UA', 250.0, 's2', TIMESTAMP '2020-02-02 03:00:00'),
      (3, 'XX', 75.0, 's3', TIMESTAMP '2020-03-02 03:00:00')
    ) AS t(id, carrier, distance, secret, ts)`,
];

/** Each way the source hides a column, and the problem it is left with. */
const hiddenShapes: Record<
   string,
   { head: string; own?: string; pre?: string; problem?: string }
> = {
   headInclude: {
      head: "duckdb.table('flights') include { private: secret; public: * }",
   },
   ownPrivate: {
      head: "duckdb.table('flights')",
      own: "private dimension: hush is concat(secret, 'x')",
   },
   ownInternal: {
      head: "duckdb.table('flights')",
      own: "internal dimension: hush is concat(secret, 'x')",
   },
   baseInternal: {
      pre: "source: fb is duckdb.table('flights') extend { internal dimension: hush is concat(secret, 'x') }",
      head: "fb",
   },
   baseIncludeInternal: {
      pre: "source: fb is duckdb.table('flights') include { internal: secret; public: * }",
      head: "fb",
   },
   baseIncludeRename: {
      pre: "source: fb is duckdb.table('flights') include { internal: secret; public: * }",
      head: "fb",
      own: "rename: dist is distance",
      problem: "definition_not_parsed",
   },
   privateTimeframe: {
      head: "duckdb.table('flights')",
      own: "private dimension: m is ts.month",
   },
};

afterAll(async () => {
   await disposeScenarios();
});

for (const kind of ["join_one", "pair"] as const) {
   for (const colocated of [false, true]) {
      // A hidden join_many is refused for inlining (known limitation 24).
      for (const access of kind === "pair"
         ? [""]
         : ["", "internal ", "private "]) {
         for (const [shape, h] of Object.entries(hiddenShapes)) {
            const label = `${kind} ${colocated ? "colocated" : "storage"}, join ${access || "public "}, hidden by ${shape}`;
            it(label, async () => {
               const { restore } = installSettings({
                  env: {
                     PERSIST_STORAGE_MODE: colocated ? "off" : "on",
                     PUBLISHER_MATERIALIZE: "true",
                  },
               });
               try {
                  const join =
                     kind === "join_one"
                        ? `${access}join_one: carriers with carrier\n  dimension: nick is carriers.nickname`
                        : `${access}join_many: legs on legs.fid = id`;
                  const inline = kind === "join_one" ? "carriers" : "legs";
                  const model = `##! experimental { access_modifiers }
source: carriers is duckdb.table('carriers') extend { primary_key: code }
source: legs is duckdb.table('legs') extend { primary_key: lid }
${h.pre ?? ""}
#(materialize) ${colocated ? 'name="w_tbl"' : 'storage="lake"'} inline=["${inline}"]
source: wide is ${h.head} extend {
  primary_key: id
  ${join}
  ${h.own ?? ""}
  measure: n is count()
  measure: miles is ${kind === "pair" ? "legs.miles.sum()" : h.own?.startsWith("rename") ? "dist.sum()" : "distance.sum()"}
}
`;
                  const fixture = await buildScenario({
                     files: { "m.malloy": model },
                     seedSql,
                     materialize: true,
                  });
                  const problems = (await fixture.servedCopyProblems()).map(
                     (p): string => p.reason,
                  );
                  expect(problems).toEqual(h.problem ? [h.problem] : []);
                  const run = await fixture.run({
                     name: label,
                     malloy:
                        kind === "join_one"
                           ? "run: wide -> { group_by: nick; aggregate: n, miles }"
                           : "run: wide -> { group_by: carrier; aggregate: n, miles }",
                     expectLive: h.problem !== undefined,
                  });
                  expect(rowDifference(run.live, run.served)).toBeUndefined();
                  expect(run.live.length).toBeGreaterThan(0);
                  if (h.problem) {
                     expect(run.servedVia).toBeUndefined();
                  } else {
                     expect(run.servedVia).toBe("served_copy");
                     expect(run.servedFrom).toBe(colocated ? null : "storage");
                  }
               } finally {
                  restore();
               }
            });
         }
      }
   }
}

/** The error a run throws, or undefined when it answers. */
async function failure(run: () => Promise<unknown>): Promise<unknown> {
   try {
      await run();
      return undefined;
   } catch (error) {
      return error;
   }
}

/**
 * A pair table beside a column hidden by an include: every caller path to the
 * hidden column, and to the names the re-root renames the pair's stored
 * columns to, is refused exactly where live refuses, the served copy's own
 * compile included; what live answers is answered from storage with live's
 * rows.
 */
describe("R4: a pair table beside a hidden column refuses exactly where live refuses", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `##! experimental { access_modifiers }
source: legs is duckdb.table('legs') extend { primary_key: lid }

#(materialize) storage="lake" inline=["legs"]
source: wide is duckdb.table('flights') include { private: secret; internal: ts; public: * } extend {
  primary_key: id
  join_many: legs on legs.fid = id
  measure: n is count(), miles is legs.miles.sum()
}
`,
         },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
   });

   async function parity(malloy: string, answers: boolean): Promise<void> {
      fixture.setBindings([]);
      let live: Row[] | undefined;
      const liveError = await failure(async () => {
         live = (await fixture.runAsIs(malloy)).rows;
      });
      fixture.setBindings(fixture.bindings);
      let served: { rows: Row[]; servedFrom: string | null } | undefined;
      const servedError = await failure(async () => {
         served = await fixture.runAsIs(malloy);
      });
      if (!answers) {
         expect(liveError).toBeDefined();
         expect(servedError).toBeDefined();
         expect(String(servedError)).not.toMatch(/s1|s2|s3/);
         const copyError = await failure(async () => {
            const { runnable, virtualMap } = await fixture
               .servedCopy()
               .loadQuery(malloy, {});
            await runnable.getSQL({ virtualMap } as never);
         });
         expect(copyError).toBeDefined();
         return;
      }
      expect(liveError).toBeUndefined();
      expect(servedError).toBeUndefined();
      expect(served?.servedFrom).toBe("storage");
      expect(rowDifference(live ?? [], served?.rows ?? [])).toBeUndefined();
   }

   it("re-roots the pair source, with no problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const refused: [string, string][] = [
      ["select", "run: wide -> { select: secret }"],
      ["a wildcard and the name", "run: wide -> { select: *, secret }"],
      ["a nest", "run: wide -> { nest: x is { group_by: secret } }"],
      ["index by name", "run: wide -> { index: secret }"],
      [
         "a caller extend",
         "run: wide extend { dimension: s is secret } -> { group_by: s }",
      ],
      [
         "a caller rename",
         "run: wide extend { rename: s is secret } -> { group_by: s }",
      ],
      [
         "a caller include",
         "run: wide include { public: secret } -> { select: secret }",
      ],
      [
         "raw SQL interpolating it",
         'run: wide -> { group_by: x is sql_string("${secret}") }',
      ],
      [
         "a caller join",
         "run: legs extend { join_one: w is wide on fid = w.id } -> { group_by: w.secret }",
      ],
      ["the parent-row flag", "run: wide -> { group_by: __parent_row }"],
      ["a stored child column", "run: wide -> { group_by: legs__miles }"],
      [
         "a stored child column's internal name",
         "run: wide -> { group_by: legs__miles__stored }",
      ],
      ["an internal column", "run: wide -> { group_by: ts }"],
   ];
   const answered: [string, string][] = [
      [
         "a parent measure beside the join's",
         "run: wide -> { group_by: carrier; aggregate: n, miles }",
      ],
      [
         "an aggregate through the join",
         "run: wide -> { group_by: carrier; aggregate: miles }",
      ],
   ];
   for (const [name, malloy] of refused) {
      it(`refuses ${name}, as live does`, async () => {
         await parity(malloy, false);
      });
   }
   for (const [name, malloy] of answered) {
      it(`answers ${name} with live's rows`, async () => {
         await parity(malloy, true);
      });
   }
});
