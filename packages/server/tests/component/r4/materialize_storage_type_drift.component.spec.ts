// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a `#(materialize)` table whose stored column types drifted from live's
 * (the upstream column's type changed under the same content address, or the
 * table was altered out of band) is never read with its stored type. Each
 * drifted column is read through a faithful cast, or left out so that only a
 * query reading it answers live: every answer equals live's.
 *
 * The drift is made after the build, by altering the stored table and the
 * binding's schema together, as a run that bound the altered table would.
 *
 * | Column | Live      | Stored      | Read                     |
 * | ------ | --------- | ----------- | ------------------------ |
 * | amt    | DECIMAL   | VARCHAR     | left out: live           |
 * | n      | INTEGER   | BIGINT      | as stored (both numbers) |
 * | ts     | TIMESTAMP | TIMESTAMPTZ | as stored (UTC session)  |
 * | d      | DATE      | VARCHAR     | left out: live           |
 * | flag   | BOOLEAN   | INTEGER     | cast to BOOLEAN          |
 * | d2     | DATE      | TIMESTAMP   | cast to DATE             |
 *
 * The `storage=` table and a colocated one (its binding carrying the drifted
 * types) are held to the same answers, and the `storage=` one reports the
 * `storage-column-type-mismatch` warning and its `stage="serve"` tick as a
 * `#@ persist` table does.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { resetMaterializationTelemetryForTesting } from "../../../src/materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../src/test_helpers/metrics_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type R4Query,
} from "./harness";

const SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 9.0::DECIMAL(10,1), 1::INTEGER, TIMESTAMP '2026-01-15 23:30:00', DATE '2026-01-15', true, DATE '2026-01-15'),
      (2, 10.0::DECIMAL(10,1), 2::INTEGER, TIMESTAMP '2026-02-03 01:00:00', DATE '2026-02-03', false, DATE '2026-02-03'),
      (3, 2.5::DECIMAL(10,1), 3::INTEGER, TIMESTAMP '2026-02-28 12:00:00', DATE '2026-02-28', true, DATE '2026-02-28'),
      (4, NULL::DECIMAL(10,1), NULL::INTEGER, NULL::TIMESTAMP, NULL::DATE, NULL::BOOLEAN, NULL::DATE)
   ) AS t(id, amt, n, ts, d, flag, d2)`,
];

/** Each drifted column: the type the stored table is altered to. */
const DRIFT: Record<string, string> = {
   amt: "VARCHAR",
   n: "BIGINT",
   ts: "TIMESTAMP WITH TIME ZONE",
   d: "VARCHAR",
   flag: "INTEGER",
   d2: "TIMESTAMP",
};

const TIERS = [
   {
      label: "storage=",
      place: (_name: string) => ` storage="${DESTINATION}"`,
      servedFrom: "storage",
   },
   {
      label: "colocated",
      place: (name: string) => ` name="${name}"`,
      servedFrom: "database",
   },
] as const;

/** Queries that read no left-out column: served from the table. */
const SERVED: R4Query[] = [
   {
      name: "an integer stored as BIGINT",
      malloy:
         "run: drift -> { group_by: id, n; aggregate: s is n.sum(); order_by: id }",
   },
   {
      name: "a timestamp stored as TIMESTAMPTZ, by hour",
      malloy:
         "run: drift -> { group_by: h is ts.hour; aggregate: c is count(); order_by: h }",
   },
   {
      name: "a timestamp stored as TIMESTAMPTZ, by day under a query time zone",
      malloy:
         "run: drift -> { timezone: 'America/New_York'; group_by: dd is ts.day; aggregate: c is count(); order_by: dd }",
   },
   {
      name: "a boolean stored as INTEGER, read through a cast",
      malloy:
         "run: drift -> { group_by: flag; aggregate: c is count(); order_by: flag }",
   },
   {
      name: "a boolean read through a cast, filtered",
      malloy: "run: drift -> { where: flag; aggregate: c is count() }",
   },
   {
      name: "a date stored as TIMESTAMP, read through a cast, by month",
      malloy:
         "run: drift -> { group_by: m is d2.month; aggregate: c is count(); order_by: m }",
   },
   {
      name: "the source's measure, over a column with no drift",
      malloy: "run: drift -> { aggregate: total }",
   },
   {
      name: "a column with no drift",
      malloy: "run: drift -> { group_by: id; order_by: id }",
   },
];

/** Queries that read a left-out column: answered live. */
const LIVE: R4Query[] = [
   // The review's repro: served from the VARCHAR column, each was the string
   // "9.0" where live answers 10.
   {
      name: "a number stored as text: its max",
      malloy: "run: drift -> { aggregate: m is amt.max() }",
      expectLive: true,
   },
   {
      name: "a number stored as text: grouped, ordered, first",
      malloy: "run: drift -> { group_by: amt; order_by: amt desc; limit: 1 }",
      expectLive: true,
   },
   {
      name: "a number stored as text: max per group",
      malloy:
         "run: drift -> { group_by: flag; aggregate: m is amt.max(); order_by: flag }",
      expectLive: true,
   },
   {
      name: "a sum of a number stored as text",
      malloy: "run: drift -> { aggregate: s is amt.sum() }",
      expectLive: true,
   },
   {
      name: "a date stored as text: grouped",
      malloy:
         "run: drift -> { group_by: d; aggregate: c is count(); order_by: d }",
      expectLive: true,
   },
   {
      name: "a date stored as text: filtered",
      malloy:
         "run: drift -> { where: d > @2026-02-01; aggregate: c is count() }",
      expectLive: true,
   },
];

for (const tier of TIERS) {
   describe(`R4 (${tier.label}): a #(materialize) table whose column types drifted`, () => {
      let restore: () => void;
      let metrics: MetricsHarness;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         metrics = await startMetricsHarness();
         resetMaterializationTelemetryForTesting();
         fixture = await buildScenario({
            files: {
               "m.malloy": `source: ev is duckdb.table('ev')

#(materialize)${tier.place("drift_t")}
source: drift is ev extend {
  measure: total is n.sum()
}

#(materialize)${tier.place("drift_m_t")}
source: drift_m is ev extend {
  measure: total is amt.sum()
}
`,
            },
            seedSql: SEED,
            materialize: true,
         });
         // Alter each stored table, and its binding's schema with it.
         for (const source of ["drift", "drift_m"]) {
            for (const [column, type] of Object.entries(DRIFT)) {
               await fixture.storedSQL(
                  (t) =>
                     `ALTER TABLE ${t(source)} ALTER ${column} TYPE ${type}`,
               );
            }
            const [binding] = fixture.bindings.filter(
               (b) => b.sourceName === source,
            );
            binding.schema = binding.schema.map((c) =>
               DRIFT[c.name] ? { ...c, type: DRIFT[c.name] } : c,
            );
         }
      }, 120_000);
      afterAll(async () => {
         restore();
         resetMaterializationTelemetryForTesting();
         await metrics.shutdown();
         await disposeScenarios();
      });

      it("the stored table holds the drifted types", async () => {
         const [row] = await fixture.storedSQL(
            (t) =>
               `SELECT typeof(amt) AS amt, typeof(d) AS d, typeof(flag) AS flag FROM ${t("drift")} LIMIT 1`,
         );
         expect(row).toEqual({ amt: "VARCHAR", d: "VARCHAR", flag: "INTEGER" });
      });

      for (const q of SERVED) {
         it(`served from the table, equal to live: ${q.name}`, async () => {
            const run = await fixture.run(q);
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBe("served_copy");
            expect(run.servedFrom).toBe(tier.servedFrom);
         });
      }

      for (const q of LIVE) {
         it(`answered live: ${q.name}`, async () => {
            const run = await fixture.run(q);
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedFrom).not.toBe(tier.servedFrom);
         });
      }

      it("a source whose own measure reads a left-out column is not re-rooted", async () => {
         const problems = await fixture.servedCopyProblems();
         expect(problems).toEqual([
            { sourceName: "drift_m", reason: "column_types" },
         ]);
      });

      for (const malloy of [
         "run: drift_m -> { aggregate: total }",
         "run: drift_m -> { group_by: flag; aggregate: c is count(); order_by: flag }",
      ]) {
         it(`answers that source live, equal to live: ${malloy}`, async () => {
            const run = await fixture.run({ name: malloy, malloy });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedFrom).not.toBe(tier.servedFrom);
         });
      }

      it("the max a stored VARCHAR would give is not what is answered", async () => {
         const run = await fixture.run(LIVE[0]);
         expect(run.served).toEqual([{ m: 10 }]);
      });

      if (tier.label === "storage=") {
         it("warns storage-column-type-mismatch for each drifted column, and how it is read", () => {
            const found = fixture.typeMismatches();
            expect(found.map((f) => f.sourceName).sort()).toEqual([
               "drift",
               "drift_m",
            ]);
            for (const f of found) {
               expect(
                  f.mismatches
                     .map((m) => [m.column, m.declaredType, m.handling])
                     .sort(),
               ).toEqual([
                  ["amt", "number", "live"],
                  ["d", "date", "live"],
                  ["d2", "date", "cast"],
                  ["flag", "boolean", "cast"],
               ]);
            }
         });

         it('counts each drifted column once at stage="serve"', async () => {
            fixture.typeMismatches();
            const count = (declared: string) =>
               metrics.collectCounter(
                  "publisher_storage_column_type_mismatch_total",
                  { stage: "serve", declared },
               );
            // Two tables, each with the same four drifted columns.
            expect(await count("number")).toBe(2);
            expect(await count("date")).toBe(4);
            expect(await count("boolean")).toBe(2);
         });
      }
   });
}

describe("R4 (storage=): a #(materialize) table whose inlined join's column type drifted", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: {
            "m.malloy": `source: carriers is duckdb.table('carriers') extend {
  primary_key: code
}

#(materialize) storage="${DESTINATION}" inline=["carriers"]
source: flights_wide is duckdb.table('flights') extend {
  join_one: carriers with carrier
}
`,
         },
         seedSql: [
            `CREATE TABLE carriers AS SELECT * FROM (VALUES
               ('AA', 9.0::DECIMAL(10,1)), ('UA', 10.0::DECIMAL(10,1))
             ) AS t(code, fee)`,
            `CREATE TABLE flights AS SELECT * FROM (VALUES
               (1, 'AA'), (2, 'UA'), (3, 'UA')
             ) AS t(id, carrier)`,
         ],
         materialize: true,
      });
      // The joined `fee`, stored under its own column name, altered to text.
      const column = fixture.materializePlans.flights_wide.columns.find(
         (c) => c.join === "carriers" && c.dimension === "fee",
      )?.column;
      expect(column).toBeDefined();
      await fixture.storedSQL(
         (t) =>
            `ALTER TABLE ${t("flights_wide")} ALTER "${column}" TYPE VARCHAR`,
      );
      const [binding] = fixture.bindings.filter(
         (b) => b.sourceName === "flights_wide",
      );
      binding.schema = binding.schema.map((c) =>
         c.name === column ? { ...c, type: "VARCHAR" } : c,
      );
   }, 120_000);
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("a record field has no cast: the source is not re-rooted", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([
         { sourceName: "flights_wide", reason: "column_types" },
      ]);
   });

   for (const malloy of [
      "run: flights_wide -> { aggregate: m is carriers.fee.max() }",
      "run: flights_wide -> { group_by: id; aggregate: c is count(); order_by: id }",
   ]) {
      it(`answers live, equal to live: ${malloy}`, async () => {
         const run = await fixture.run({ name: malloy, malloy });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).not.toBe("storage");
      });
   }
});
