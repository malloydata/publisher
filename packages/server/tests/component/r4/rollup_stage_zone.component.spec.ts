// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a rollup over a base whose query STAGE sets the zone
 * (`daily is raw -> { timezone: 'America/Los_Angeles'; group_by: d is ts.day … }`).
 * The stage truncated `d` in Los Angeles, so the stored day is
 * `2024-01-06 08:00` (UTC). The rollup's table held that value, but its serve
 * rewrite read `d` as a truncation in the model's zone and truncated it again
 * in UTC, answering `2024-01-06 00:00` where live answers `08:00`: the plan
 * looked for a zone on the source alone, never in its query's stages.
 *
 * Only a `storage=` rollup restates its truncations (`d is d__stored.day`),
 * so a `storage=` rollup whose grain holds one is no longer planned
 * (`TIME_ZONE`); a database rollup returns the stored value as it is and
 * keeps serving.
 *
 * Both shapes the reviewer found (the rollup in the base's own extend, and on
 * an extension of the zoned base), and a stage that inherits its base's
 * zone, read with no zone, in the zone the stage computed in, and in
 * another, for a day, a week and an hour truncation, over a DST boundary, on
 * a database and a `storage=` rollup, declared by `#(rollup)` and by
 * `#@ preaggregate`. Every answer equals live as strings.
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
import {
   READ_ZONES,
   ROUTES,
   STAGE_ZONE_QUERIES,
   STAGE_ZONE_SEED,
   stageZoneModel,
   UNZONED_READS,
} from "./rollup_stage_zone_shapes";

for (const route of Object.keys(ROUTES) as (keyof typeof ROUTES)[])
   for (const [placement, persist, p] of [
      ["database", "", ""],
      [
         "storage=",
         `#@ persist storage="${DESTINATION}"`,
         ` storage="${DESTINATION}"`,
      ],
   ] as const) {
      describe(`R4: a ${route} rollup over a base whose query stage sets the zone [${placement}]`, () => {
         let restore: () => void;
         let fixture: R4Fixture;
         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            }).restore;
            fixture = await buildScenario({
               files: {
                  "m.malloy": stageZoneModel(
                     "duckdb.table('ev')",
                     persist,
                     p,
                     route,
                  ),
               },
               seedSql: STAGE_ZONE_SEED,
               preaggregate: true,
            });
         });
         afterAll(async () => {
            restore();
            await disposeScenarios();
         });

         const database = placement === "database";
         it(
            database
               ? "plans every rollup"
               : "refuses every rollup (TIME_ZONE)",
            () => {
               if (database) {
                  expect(fixture.rollupRefusals).toEqual([]);
               } else {
                  expect(fixture.rollupRefusals.length).toBe(7);
                  for (const r of fixture.rollupRefusals) {
                     expect(r.reason).toBe("time_zone");
                  }
               }
            },
         );
         for (const [zoneName, z] of READ_ZONES) {
            // A database rollup returns the stored truncation as it is; a
            // zoned query is declined by the caller-zone rule
            // (rollup_caller_zone), and a `storage=` one is never planned.
            const served = database && z === "";
            for (const [name, malloy] of [
               ...STAGE_ZONE_QUERIES,
               ...UNZONED_READS,
            ]) {
               const readsTruncation = STAGE_ZONE_QUERIES.some(
                  ([n]) => n === name,
               );
               const fromRollup = database && (served || !readsTruncation);
               it(`${name}, ${zoneName}: ${fromRollup ? "" : "not "}read from the rollup, equal to live`, async () => {
                  const run = await fixture.run({ name, malloy: malloy(z) });
                  expect(rowDifference(run.live, run.served)).toBeUndefined();
                  expect(JSON.stringify(run.served)).toBe(
                     JSON.stringify(run.live),
                  );
                  if (fromRollup) expect(run.servedBy?.kind).toBe("rollup");
                  else expect(run.servedBy?.kind).not.toBe("rollup");
               });
            }
         }
      });
   }
