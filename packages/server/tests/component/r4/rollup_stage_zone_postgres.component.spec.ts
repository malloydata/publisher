// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * rollup_stage_zone on a Postgres warehouse, through the real build and
 * bind: a rollup over a base whose query stage truncates in Los Angeles,
 * built in Postgres (database) or into a DuckLake destination (`storage=`).
 * A read of one of those truncations with no zone was truncated again in
 * UTC from the `storage=` rollup, which is now not planned; the database
 * rollup keeps serving reads with no zone. Every read equals live as
 * strings.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL.
 */

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   type Answer,
   openPostgresLake,
   POSTGRES_URL,
   type PostgresLake,
} from "./postgres_lake";
import {
   READ_ZONES,
   STAGE_ZONE_QUERIES,
   STAGE_ZONE_SEED,
   stageZoneModel,
   UNZONED_READS,
} from "./rollup_stage_zone_shapes";

setDefaultTimeout(600_000);

const CASES = READ_ZONES.flatMap(([zoneName, z]) => [
   ...STAGE_ZONE_QUERIES.map(([name, q]) => ({
      name: `${name}, ${zoneName}`,
      query: q(z),
      readsTruncation: true,
   })),
   ...UNZONED_READS.map(([name, q]) => ({
      name: `${name}, ${zoneName}`,
      query: q(z),
      readsTruncation: false,
   })),
]);

for (const [placement, persist, p] of [
   ["database", "", ""],
   ["storage=", `#@ persist storage="lake"`, ` storage="lake"`],
] as const) {
   describe.skipIf(!POSTGRES_URL)(
      `a rollup over a base whose query stage sets the zone, on Postgres [${placement}]`,
      () => {
         let lake: PostgresLake;
         let restore = () => {};
         const live: Record<string, Answer> = {};
         const served: Record<string, Answer> = {};

         beforeAll(async () => {
            restore = installSettings({
               env: { PUBLISHER_MATERIALIZE: "true" },
            }).restore;
            lake = await openPostgresLake(
               "rsz",
               {
                  "m.malloy": stageZoneModel(
                     "warehouse.table('public.ev')",
                     persist,
                     p,
                  ),
               },
               STAGE_ZONE_SEED,
            );
            for (const c of CASES) {
               live[c.name] = await lake.ask("m.malloy", c.query);
            }
            await lake.build();
            for (const c of CASES) {
               served[c.name] = await lake.ask("m.malloy", c.query);
            }
         });
         afterAll(async () => {
            await lake?.close();
            restore();
         });

         for (const c of CASES) {
            // A database rollup returns the stored truncation as it is (a
            // zoned query is declined by the caller-zone rule); a
            // `storage=` one is never planned.
            const fromRollup =
               placement === "database" &&
               (!c.readsTruncation || !c.query.includes("timezone:"));
            it(`${c.name}: equal to live, ${fromRollup ? "" : "not "}read from the rollup`, () => {
               expect(served[c.name].rows).toEqual(live[c.name].rows);
               if (fromRollup) expect(served[c.name].kind).toBe("rollup");
               else expect(served[c.name].kind).not.toBe("rollup");
            });
         }
      },
   );
}
