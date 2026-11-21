// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a database v1 `#@ persist` table the served copy cannot carry whole.
 *
 * - A source the served copy leaves as written (here `fields_not_carried`: a
 *   persisted extension of a base with zoned dimensions) answers live, and
 *   its path report names its own table `NOT_SERVED` with the served copy's
 *   reason, as the package warning does. Through 0.17.0 the report considered
 *   nothing, so the live answer gave no reason at all.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** Rows with every value an exact string, in the order they came. */
function asStrings(rows: Record<string, unknown>[]): Record<string, string>[] {
   return rows.map((r) =>
      Object.fromEntries(
         Object.entries(r).map(([k, v]) => [
            k,
            v instanceof Date
               ? v.toISOString()
               : typeof v === "object" && v !== null
                 ? JSON.stringify(v, (_k, x) =>
                      typeof x === "bigint" ? `${x}n` : x,
                   )
                 : String(v),
         ]),
      ),
   );
}

describe("R4: a database v1 #@ persist source the served copy leaves as written names why in its path report", () => {
   let r4: R4Fixture;
   useMaterializationOn();
   beforeAll(async () => {
      r4 = await buildScenario({
         files: {
            "m.malloy": `##! experimental { persistence }
source: ev_ny is duckdb.sql("SELECT * FROM ev") extend {
  primary_key: id
  timezone: 'America/New_York'
  dimension: ts_hr is hour(ts), ts_day is ts.day
}

#@ persist name="r4lo_inh"
source: inh is ev_ny extend { measure: total is amt.sum() }
`,
         },
         seedSql: [
            `CREATE TABLE ev AS SELECT * FROM (VALUES
               (1, 10.5, TIMESTAMP '2024-03-15 01:45:10'),
               (2, 20.0, TIMESTAMP '2024-03-15 03:05:00')
             ) AS t(id, amt, ts)`,
         ],
         database: ["inh"],
      });
   });
   afterAll(disposeScenarios);

   it("is bound in the database tier, and left as written for its inherited fields", async () => {
      expect(r4.bindings.map((b) => `${b.sourceName}:${b.tier}`)).toEqual([
         "inh:database",
      ]);
      expect(await r4.servedCopyProblems()).toEqual([
         { sourceName: "inh", reason: "fields_not_carried" },
      ]);
   });

   it("answers live, equal to live, naming its own table NOT_SERVED with the reason", async () => {
      const malloy =
         "run: inh -> { group_by: h is hour(ts); aggregate: n is count(), total; order_by: h }";
      const run = await r4.run({ name: "inh", malloy });
      expect(run.live.length).toBe(2);
      expect(asStrings(run.served)).toEqual(asStrings(run.live));
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      r4.setBindings(r4.bindings);
      const { path } = await r4.runAsIs(malloy);
      expect(path?.servedBy?.kind).toBe("live");
      expect(
         (path?.considered ?? []).map((c) => ({
            tableId: c.tableId,
            outcome: c.outcome,
            reason: c.reason,
            detail: c.detail,
         })),
      ).toEqual([
         {
            tableId: r4.bindings[0].virtualHandle,
            outcome: "skipped",
            reason: "NOT_SERVED",
            detail: expect.stringContaining("fields_not_carried"),
         },
      ]);
   });
});
