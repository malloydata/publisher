// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * g-mixed (a gated entry point over an ungated `storage=` source, gated
 * sources off) when the column the gate reads drifted on the stored table:
 * the gate is grafted on the served copy, so it reads the stored column. A
 * column no cast reads as live's type is left out, and the answer must then
 * be live's, never a denial live does not give nor another caller's rows. A
 * column read through a faithful cast keeps the answer served, equal to
 * live's.
 *
 * | Gate reads           | Live      | Stored           |
 * | -------------------- | --------- | ---------------- |
 * | `acct = $ACCT`       | HUGEINT   | DOUBLE (DuckLake) |
 * | `region in $REGIONS` | VARCHAR   | INTEGER (altered) |
 * | `ts.day = @…`        | TIMESTAMP | TIMESTAMPTZ      |
 * | `flag`               | BOOLEAN   | INTEGER (cast)    |
 *
 * The binding is marked as holding only ungated rows, as the bind marks it
 * (bind.spec.ts pins the marking); the router, the served copy and
 * `govern()` are the package's own.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { GivenValue } from "@malloydata/malloy";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, CAST('9007199254740993' AS HUGEINT), 'north', TIMESTAMP '2026-01-15 23:30:00', true),
      (2, CAST('9007199254740992' AS HUGEINT), 'south', TIMESTAMP '2026-01-16 01:00:00', false),
      (3, CAST('9007199254740993' AS HUGEINT), 'east',  TIMESTAMP '2026-02-28 12:00:00', true),
      (4, CAST('7' AS HUGEINT),                NULL,    NULL::TIMESTAMP,                 NULL::BOOLEAN)
   ) AS t(id, acct, region, ts, flag)`,
];

const MODEL = `##! experimental { givens persistence }
given: ACCT :: number
given: REGIONS :: string[]

source: ev is duckdb.table('ev')

#(materialize) storage="duckdb"
source: stored is ev extend {
  primary_key: id
  measure: n is count()
}

#(access_filter) acct = $ACCT
source: by_acct is stored extend {}

#(access_filter) region in $REGIONS
source: by_region is stored extend {}

#(access_filter) ts.day = @2026-01-15
source: by_day is stored extend {}

#(access_filter) flag
source: by_flag is stored extend {}
`;

const G3_ONLY = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
};

/** Each drifted column: the type the stored table is altered to. */
const ALTERED: Record<string, string> = {
   region: "INTEGER",
   ts: "TIMESTAMP WITH TIME ZONE",
   flag: "INTEGER",
};

type Outcome = { rows: Row[]; via?: string } | { error: string };

describe("g-mixed: a gate whose column drifted on the stored table (gated sources off)", () => {
   let restore: () => void;
   let fixture: R4Fixture;

   const run = async (
      malloy: string,
      givens: Record<string, GivenValue>,
      served: boolean,
   ): Promise<Outcome> => {
      fixture.setBindings(served ? fixture.bindings : []);
      try {
         const result = await fixture.model.runQuery(
            unrecordedAudit(),
            { query: malloy, givens } as never,
            QueryPrivileges.of({}),
         );
         return {
            rows: result.compactResult as unknown as Row[],
            ...(result.path?.servedBy?.via
               ? { via: result.path.servedBy.via }
               : {}),
         };
      } catch (error) {
         return {
            error: error instanceof Error ? error.message : String(error),
         };
      }
   };

   beforeAll(async () => {
      restore = installSettings({ env: G3_ONLY }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": MODEL },
         seedSql: SEED,
         materialize: true,
      });
      // `region` cannot be altered to INTEGER while it holds text: rebuild
      // the column as the cast of nothing a number reads, so every value is
      // NULL on the stored table, as a broken upstream retype would leave it.
      await fixture.storedSQL(
         (t) =>
            `ALTER TABLE ${t("stored")} ALTER region TYPE INTEGER USING NULL`,
      );
      for (const [column, type] of Object.entries(ALTERED)) {
         if (column === "region") continue;
         await fixture.storedSQL(
            (t) => `ALTER TABLE ${t("stored")} ALTER ${column} TYPE ${type}`,
         );
      }
      // The run that bound the table carries its schema; DuckLake returns a
      // HUGEINT as a DOUBLE. Marked as the bind marks a table every source
      // of which is ungated.
      for (const b of fixture.bindings) {
         b.schema = b.schema.map((c) =>
            c.name === "acct"
               ? { ...c, type: "DOUBLE" }
               : ALTERED[c.name]
                 ? { ...c, type: ALTERED[c.name] }
                 : c,
         );
         (b as { holdsOnlyUngatedRows?: boolean }).holdsOnlyUngatedRows = true;
      }
   }, 120_000);
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const CASES: {
      name: string;
      malloy: string;
      callers: Record<string, GivenValue>[];
   }[] = [
      {
         name: "a big integer read back as DOUBLE",
         malloy: "run: by_acct -> { group_by: id; aggregate: n; order_by: id }",
         callers: [
            { ACCT: 9007199254740994 },
            { ACCT: 9007199254740992 },
            { ACCT: 7 },
            {},
         ],
      },
      {
         name: "a string column altered to another type",
         malloy:
            "run: by_region -> { group_by: id, region; aggregate: n; order_by: id }",
         callers: [{ REGIONS: ["north"] }, { REGIONS: ["east", "south"] }, {}],
      },
      {
         name: "a timestamp stored with a time zone",
         malloy: "run: by_day -> { group_by: id; aggregate: n; order_by: id }",
         callers: [{}],
      },
      {
         name: "a boolean stored as an integer, read through a cast",
         malloy: "run: by_flag -> { group_by: id; aggregate: n; order_by: id }",
         callers: [{}],
      },
   ];

   for (const kase of CASES) {
      it(`${kase.name}: every caller gets live's answer`, async () => {
         for (const givens of kase.callers) {
            const live = await run(kase.malloy, givens, false);
            const served = await run(kase.malloy, givens, true);
            if ("error" in live) {
               expect("error" in served ? served.error : "answered").toBe(
                  live.error,
               );
               continue;
            }
            if ("error" in served) {
               throw new Error(
                  `${kase.name} ${JSON.stringify(givens)}: ${served.error}`,
               );
            }
            expect(rowDifference(live.rows, served.rows)).toBeUndefined();
         }
      });
   }

   it("answers a gate over a column no cast reads exactly live, and one read through a faithful cast from the table", async () => {
      for (const [malloy, via] of [
         [
            "run: by_acct -> { group_by: id; aggregate: n; order_by: id }",
            undefined,
         ],
         [
            "run: by_region -> { group_by: id; aggregate: n; order_by: id }",
            undefined,
         ],
         [
            "run: by_flag -> { group_by: id; aggregate: n; order_by: id }",
            "served_copy",
         ],
      ] as const) {
         const served = await run(
            malloy,
            { ACCT: 7, REGIONS: ["north"] },
            true,
         );
         expect([
            malloy,
            "error" in served ? served.error : served.via,
         ]).toEqual([malloy, via]);
      }
   });
});
