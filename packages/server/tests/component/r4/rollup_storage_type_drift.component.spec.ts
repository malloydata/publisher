// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a `storage=` rollup whose stored column types drifted from live's (the
 * upstream column's type changed under the same content address, or the
 * table was altered out of band) is never read with its stored type. A grain
 * column or a `min`/`max` partial that a faithful cast reads back as live's
 * type is cast; any other drifted column declines the rollup
 * (`MISSING_FIELD`), and the query answers live. Every answer equals live's.
 *
 * | Rollup       | Column           | Live    | Stored    | Read             |
 * | ------------ | ---------------- | ------- | --------- | ---------------- |
 * | by `code`    | code (grain)     | INTEGER | VARCHAR   | declined: live   |
 * | by `region`  | hi__partial (max)| DECIMAL | VARCHAR   | declined: live   |
 * | by `g`       | lo_d__partial (min) | DATE | VARCHAR  | declined: live   |
 * | by `flag`    | flag (grain)     | BOOLEAN | INTEGER   | cast to BOOLEAN  |
 * | by `flag`    | first_d__partial (min) | DATE | TIMESTAMP | cast to DATE |
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
      (7, 'east', 'x', true, 9.0::DECIMAL(10,1), DATE '2026-01-02'),
      (10, 'east', 'x', false, 10.0::DECIMAL(10,1), DATE '2026-01-03'),
      (100, 'west', 'y', true, 2.5::DECIMAL(10,1), DATE '2026-02-01'),
      (100, 'west', 'y', NULL, NULL::DECIMAL(10,1), NULL::DATE)
   ) AS t(code, region, g, flag, amt, d)`,
];

/** Each drifted stored column, by name: the type its table is altered to. */
const DRIFT: Record<string, string> = {
   code: "VARCHAR",
   hi__partial: "VARCHAR",
   lo_d__partial: "VARCHAR",
   flag: "INTEGER",
   first_d__partial: "TIMESTAMP",
};

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
}
`,
            },
            seedSql: SEED,
            preaggregate: true,
         });
         // Alter each rollup's table, and its binding's schema with it.
         let altered = 0;
         for (const b of fixture.bindings) {
            for (const c of b.schema) {
               const type = DRIFT[c.name];
               if (!type) continue;
               await fixture.duckdb.runSQL(
                  `ALTER TABLE ${b.tablePath} ALTER "${c.name}" TYPE ${type}`,
               );
               altered++;
            }
            b.schema = b.schema.map((c) =>
               DRIFT[c.name] ? { ...c, type: DRIFT[c.name] } : c,
            );
         }
         expect(altered).toBe(Object.keys(DRIFT).length);
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
