// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-07 follow-up: a `#(materialize)` table that lacks a record (struct)
 * column its source declares (a `storage=` table built before its build
 * stored structs, such as a BigQuery `STRUCT`, until it is next built in
 * full; see storage_struct_columns.component.spec.ts) leaves that column out
 * per query, as one lacking a list does (materialize_missing_list_column):
 * a query that does not read it is served from the table, and a query that
 * reads one of its fields (selected, grouped, filtered, nested, or through
 * the model's own measure) is answered live.
 *
 * The table is made as such a build left it: built, then the struct column
 * dropped in the destination, with the run that bound it carrying the
 * schema without it.
 *
 * Gates and givens over struct fields are unchanged on both paths: a source
 * whose `#(access_filter)` reads a stored struct's field scopes each caller
 * to its own rows from the table, answers a query reading a missing struct
 * live under the same gate, and refuses a caller with no given.
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
      (1, 'acme', 1.5, {'category': 'mobile', 'os': 'ios'}, {'country': 'US', 'loc': {'city': 'nyc'}}, {'tags': ['x', 'y'], 'src': 'ads'}, {'org': 'acme', 'tier': 'gold'}),
      (2, 'acme', 2.5, {'category': 'desktop', 'os': 'mac'}, {'country': 'US', 'loc': {'city': 'sf'}}, {'tags': ['y'], 'src': 'mail'}, {'org': 'acme', 'tier': 'silver'}),
      (3, 'globex', 3.5, {'category': 'mobile', 'os': 'android'}, {'country': 'FR', 'loc': {'city': 'paris'}}, {'tags': ['x', 'z'], 'src': 'ads'}, {'org': 'globex', 'tier': 'gold'}),
      (4, 'globex', 4.5, NULL, {'country': 'FR', 'loc': NULL}, {'tags': []::VARCHAR[], 'src': NULL}, {'org': 'globex', 'tier': NULL})) AS t(id, org, amount, device, geo, params, acct)`,
];

const MODEL = `#(materialize) storage="lake"
source: s is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: reads_device is duckdb.table('ex') extend {
  primary_key: id
  measure: mobile is count() { where: device.category = 'mobile' }
}
`;

/** As an older build left it: no struct `column` written, nor bound. */
async function dropColumn(
   fixture: R4Fixture,
   source: string,
   column: string,
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

describe("F-07: a #(materialize) storage= table missing a declared struct column", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: SEED,
         materialize: true,
      });
      // As an older build left it: no struct written. `s` keeps `acct`.
      for (const column of ["device", "geo", "params"]) {
         await dropColumn(fixture, "s", column);
      }
      await dropColumn(fixture, "reads_device", "device");
   }, 120_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it("re-roots the source that does not read a struct in its own text; the one that does is named with the column", async () => {
      const problems = await fixture.servedCopyProblems();
      expect(problems.map((p) => [p.sourceName, p.reason])).toEqual([
         ["reads_device", "column_types"],
      ]);
      expect(problems[0].detail).toBe(
         "column `device` (declared `{ category::string, os::string }`) is not in the stored table",
      );
   });

   it.each([
      "run: s -> { aggregate: n }",
      "run: s -> { group_by: org; aggregate: n, total is amount.sum(); order_by: org }",
      "run: s -> { select: id, acct; order_by: id }",
      "run: s -> { group_by: acct.tier; aggregate: n; order_by: tier }",
      "run: s -> { where: acct.org = 'acme'; aggregate: n }",
   ])(
      "serves a query that does not read a missing struct from the table, equal to live: %s",
      async (malloy) => {
         const run = await fixture.run({ name: malloy, malloy });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedVia).toBe("served_copy");
         expect(run.servedFrom).toBe("storage");
      },
   );

   it.each([
      ["run: s -> { select: id, device; order_by: id }", "device"],
      ["run: s -> { select: id, device.os; order_by: id }", "device"],
      [
         "run: s -> { group_by: device.category; aggregate: n; order_by: category }",
         "device",
      ],
      ["run: s -> { where: device.os = 'ios'; aggregate: n }", "device"],
      [
         "run: s -> { group_by: org; nest: oss is { group_by: device.os; order_by: os }; order_by: org }",
         "device",
      ],
      [
         "run: s -> { group_by: geo.loc.city; aggregate: n; order_by: city }",
         "geo",
      ],
      [
         "run: s -> { group_by: t is params.tags.each; aggregate: c is count(); order_by: t }",
         "params",
      ],
   ])(
      "answers a query reading it live, equal to live, naming the column: %s",
      async (malloy, column) => {
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
         expect(considered[0].detail).toContain(
            `column \`${column}\` (declared \``,
         );
      },
   );

   it("answers a source whose own measure reads the struct live as a whole, equal to live, and the path detail names the column", async () => {
      for (const malloy of [
         "run: reads_device -> { aggregate: mobile }",
         "run: reads_device -> { group_by: org; aggregate: c is count(); order_by: org }",
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
               "column `device` (declared `{ category::string, os::string }`) is not in the stored table",
            ],
         ]);
      }
   });
});

const GATED_MODEL = `##! experimental { givens }
given: ORG :: string
given: OS :: string

#(access_filter) acct.org = $ORG
#(materialize) storage="lake"
source: gated is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: by_os is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
  view: for_os is { where: device.os = $OS; aggregate: n }
}
`;

describe.each([true, false])(
   "F-07, struct gates (gated serving %p): the same gate and givens over struct fields on both paths",
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
         // The gate's struct is stored; another struct is missing.
         await dropColumn(fixture, "gated", "params");
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("re-roots both sources", async () => {
         expect(await fixture.servedCopyProblems()).toEqual([]);
      });

      it("scopes each caller to its own rows from the table, by a struct field", async () => {
         for (const [org, n] of [
            ["acme", 2],
            ["globex", 2],
         ] as const) {
            const run = await fixture.run({
               name: org,
               malloy:
                  "run: gated -> { group_by: device.os; aggregate: n, total is amount.sum(); order_by: os }",
               givens: { ORG: org },
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.served.reduce((t, r) => t + Number(r.n), 0)).toBe(n);
            expect(run.servedFrom).toBe(gatedServing ? "storage" : null);
         }
      });

      it("answers a caller's query reading the missing struct live, under the caller's gate", async () => {
         const malloy =
            "run: gated -> { group_by: t is params.tags.each; aggregate: c is count(); order_by: t }";
         const acme = await fixture.run({
            name: "acme",
            malloy,
            givens: { ORG: "acme" },
            expectLive: true,
         });
         expect(rowDifference(acme.live, acme.served)).toBeUndefined();
         expect(acme.served).toEqual([
            { t: "x", c: 1 },
            { t: "y", c: 2 },
         ]);
         expect(acme.servedFrom).not.toBe("storage");
      });

      it("serves a view whose given filters a struct field from the table, equal to live", async () => {
         for (const [os, n] of [
            ["ios", 1],
            ["android", 1],
            ["mac", 1],
         ] as const) {
            const run = await fixture.run({
               name: os,
               malloy: "run: by_os -> for_os",
               givens: { OS: os },
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.served).toEqual([{ n }]);
            expect(run.servedFrom).toBe("storage");
         }
      });

      it.each([
         "run: gated -> { aggregate: n }",
         "run: gated -> { select: id, acct }",
         "run: gated -> { select: id, params }",
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

const DATABASE_MODEL = `#(materialize)
source: db_s is duckdb.table('ex') extend {
  primary_key: id
  measure: n is count()
}
`;

describe("F-07: a database #(materialize) table with its struct columns", () => {
   let fixture: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      fixture = await buildScenario({
         files: { "m.malloy": DATABASE_MODEL },
         seedSql: SEED,
         materialize: true,
      });
   }, 120_000);
   afterAll(async () => {
      await disposeScenarios();
   });

   it("re-roots the source", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it.each([
      "run: db_s -> { aggregate: n }",
      "run: db_s -> { select: id, device, geo, params; order_by: id }",
      "run: db_s -> { group_by: device.os; aggregate: n; order_by: os }",
      "run: db_s -> { group_by: geo.loc.city; aggregate: n; order_by: city }",
      "run: db_s -> { where: acct.org = 'acme'; group_by: t is params.tags.each; aggregate: c is count(); order_by: t }",
   ])("serves from the table, equal to live: %s", async (malloy) => {
      const run = await fixture.run({ name: malloy, malloy });
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedVia).toBe("served_copy");
   });
});
