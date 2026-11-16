// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a `storage=` rollup whose stored column types drifted from live's (the
 * upstream column's type changed under the same content address, or the
 * table was altered out of band) is never read with its stored type. Each
 * grain column and each partial is checked against the type the rollup's
 * build source declares for it, whatever expression the partial aggregates:
 * a faithful cast reads it back where one exists, and any other drifted
 * column declines the rollup (`MISSING_FIELD`), so the query answers live.
 * Every answer equals live's.
 *
 * | Rollup     | Column                       | Declared | Stored    | Read            |
 * | ---------- | ---------------------------- | -------- | --------- | --------------- |
 * | by `code`  | code (grain)                 | number   | VARCHAR   | declined: live  |
 * | by `region`| hi__partial (max)            | number   | VARCHAR   | declined: live  |
 * | by `g`     | lo_d__partial (min)          | date     | VARCHAR   | declined: live  |
 * | by `r2`    | gm's, lm's partials (exprs)  | number   | VARCHAR   | declined: live  |
 * | by `flag`  | flag (grain)                 | boolean  | INTEGER   | cast to BOOLEAN |
 * | by `flag`  | first_d__partial (min)       | date     | TIMESTAMP | cast to DATE    |
 * | by `k`     | big's partial, undrifted     | number   | INTEGER   | as stored       |
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (7, 'east', 'x', true, 9.0::DECIMAL(10,1), DATE '2026-01-02', 8.0::DECIMAL(10,1), 1, 'p', 'east'),
      (10, 'east', 'x', false, 10.0::DECIMAL(10,1), DATE '2026-01-03', 7.0::DECIMAL(10,1), 2, 'p', 'east'),
      (100, 'west', 'y', true, 2.5::DECIMAL(10,1), DATE '2026-02-01', 3.0::DECIMAL(10,1), 0, 'q', 'west'),
      (100, 'west', 'y', NULL, NULL::DECIMAL(10,1), NULL::DATE, NULL::DECIMAL(10,1), NULL, 'q', 'west')
   ) AS t(code, region, g, flag, amt, d, amt2, n, k, r2)`,
];

/** Each drifted stored column, by name: the type its table is altered to. */
const DRIFT: Record<string, string> = {
   code: "VARCHAR",
   hi__partial: "VARCHAR",
   lo_d__partial: "VARCHAR",
   flag: "INTEGER",
   first_d__partial: "TIMESTAMP",
};

/**
 * The type a stored column is altered to: {@link DRIFT}'s, or text for every
 * partial of the measures over expressions (`gm`, `lm`), whatever the
 * decomposition names them.
 */
function driftOf(column: string): string | undefined {
   return (
      DRIFT[column] ??
      (/^(gm|lm)__/.test(column) && column.includes("partial")
         ? "VARCHAR"
         : undefined)
   );
}

const ROUTES = {
   v2: (grain: string) =>
      `#(preaggregate) grain=["${grain}"] storage="${DESTINATION}"`,
   v1: (grain: string) =>
      `#@ preaggregate grain="${grain}" storage="${DESTINATION}"`,
};

const DECLINED = [
   [
      "a grain column stored as text",
      "run: ev -> { group_by: code; aggregate: top; order_by: code }",
   ],
   [
      "a max partial stored as text",
      "run: ev -> { group_by: region; aggregate: hi; order_by: region }",
   ],
   [
      "a min partial over a date stored as text",
      "run: ev -> { group_by: g; aggregate: lo_d; order_by: g }",
   ],
   // Partials of expressions, in measures of several partials: each is
   // checked against its build source's declared type, not its argument's.
   [
      "max partials over expressions stored as text, under greatest()",
      "run: ev -> { group_by: r2; aggregate: gm; order_by: r2 }",
   ],
   [
      "a max and a min partial over expressions stored as text, under least()",
      "run: ev -> { group_by: r2; aggregate: lm; order_by: r2 }",
   ],
] as const;

for (const [route, annotate] of Object.entries(ROUTES)) {
   describe(`R4: a ${route} storage= rollup whose stored column types drifted`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence }
source: ev is duckdb.table('ev') extend {
  ${annotate("code")}
  measure: top is amt.max()
  ${annotate("region")}
  measure: hi is amt.max()
  ${annotate("g")}
  measure: lo_d is min(d)
  ${annotate("flag")}
  measure: first_d is min(d)
  ${annotate("r2")}
  measure: gm is greatest(max(amt * 1), max(amt2 * 1))
  ${annotate("r2")}
  measure: lm is least(max(amt * 1), min(amt2 * 1))
  ${annotate("k")}
  measure: big is max(n * 1) > 1
}
`,
            },
            seedSql: SEED,
            preaggregate: true,
         });
         // Alter each rollup's table, and its binding's schema with it.
         const altered: string[] = [];
         for (const b of fixture.bindings) {
            for (const c of b.schema) {
               const type = driftOf(c.name);
               if (!type) continue;
               await fixture.duckdb.runSQL(
                  `ALTER TABLE ${b.tablePath} ALTER "${c.name}" TYPE ${type}`,
               );
               altered.push(c.name);
            }
            b.schema = b.schema.map((c) => {
               const type = driftOf(c.name);
               return type ? { ...c, type } : c;
            });
         }
         // Each named column, and at least two partials of each expression
         // measure.
         for (const name of Object.keys(DRIFT)) expect(altered).toContain(name);
         for (const m of ["gm", "lm"]) {
            expect(
               altered.filter((n) => n.startsWith(`${m}__`)).length,
            ).toBeGreaterThanOrEqual(2);
         }
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      for (const [name, malloy] of DECLINED) {
         it(`${name}: declined (MISSING_FIELD), answered live, equal to live`, async () => {
            const run = await fixture.run({ name, malloy });
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedBy?.kind).toBe("live");
            fixture.setBindings(fixture.bindings);
            const { path } = await fixture.runAsIs(malloy);
            expect(path?.considered).toContainEqual(
               expect.objectContaining({
                  kind: "rollup",
                  outcome: "skipped",
                  reason: "MISSING_FIELD",
               }),
            );
         });
      }

      it("greatest() over max partials stored as text would answer 9.0; live's 10 is answered", async () => {
         const run = await fixture.run({
            name: "greatest",
            malloy: "run: ev -> { where: r2 = 'east'; aggregate: gm }",
         });
         expect(run.served).toEqual([{ gm: 10 }]);
      });

      it("an undrifted rollup whose measure is a boolean over a partial: served from the rollup, equal to live", async () => {
         const run = await fixture.run({
            name: "boolean over a partial",
            malloy: "run: ev -> { group_by: k; aggregate: big; order_by: k }",
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).toBe("rollup");
      });

      it("a max over text would answer 9.0; live's 10 is answered", async () => {
         const run = await fixture.run({
            name: "max",
            malloy: "run: ev -> { where: region = 'east'; aggregate: hi }",
         });
         expect(run.served).toEqual([{ hi: 10 }]);
      });

      it("a grain and a min partial read through a faithful cast: served from the rollup, equal to live", async () => {
         const run = await fixture.run({
            name: "cast",
            malloy:
               "run: ev -> { group_by: flag; aggregate: first_d; order_by: flag }",
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedBy?.kind).toBe("rollup");
      });
   });
}
