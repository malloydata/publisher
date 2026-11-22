// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4: a stored table whose column types drifted from live's (the upstream
 * column's type changed under the same content address, or the table was
 * altered out of band) is never read with its stored type. Each drifted
 * column is read through a faithful cast, or left out so that only a query
 * reading it answers live: every answer equals live's.
 *
 * The drift is made after the build with a real `ALTER TABLE`. A database
 * table's types are then read from its warehouse as a package's bind reads
 * them (`observeDatabaseColumns`), for each database mechanism:
 * `#(materialize)`, `#@ persist` and a rollup. A `storage=` table's binding
 * carries its schema from the run that bound it, so its schema is altered
 * with it, as a run that bound the altered table would.
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
 * The `storage=` table and the database ones are held to the same answers.
 * A database rollup whose column drifted is read through a typed source, as
 * a `storage=` rollup is, so only a query reading that column answers live
 * (database_table_drift_substituted.component.spec.ts holds every drift kind
 * on a rollup, a persisted view and a persisted query). The
 * `storage-column-type-mismatch` warning's cast or live handling is pinned in
 * storage_bindings.spec.ts.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
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

/** Each mechanism and tier whose table is altered: how its model declares it. */
const TIERS = [
   {
      label: "#(materialize) storage=",
      model: (name: string, measure: string) =>
         `#(materialize) storage="${DESTINATION}"
source: ${name} is ev extend {
  measure: total is ${measure}
}
`,
      database: undefined,
      servedFrom: "storage",
   },
   {
      label: "#(materialize) database",
      model: (name: string, measure: string, table: string) =>
         `#(materialize) name="${table}"
source: ${name} is ev extend {
  measure: total is ${measure}
}
`,
      database: undefined,
      servedFrom: "database",
   },
   {
      label: "#@ persist database",
      model: (name: string, measure: string) =>
         `#@ persist
source: ${name} is ev -> { select: * } extend {
  measure: total is ${measure}
}
`,
      database: ["drift", "drift_m"],
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
   describe(`R4 (${tier.label}): a table whose column types drifted`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: {
               "m.malloy": `##! experimental { persistence }
source: ev is duckdb.table('ev')

${tier.model("drift", "n.sum()", "drift_t")}
${tier.model("drift_m", "amt.sum()", "drift_m_t")}`,
            },
            seedSql: SEED,
            ...(tier.database
               ? { database: [...tier.database] }
               : { materialize: true }),
         });
         // Alter each stored table after its build.
         for (const source of ["drift", "drift_m"]) {
            for (const [column, type] of Object.entries(DRIFT)) {
               await fixture.storedSQL(
                  (t) =>
                     `ALTER TABLE ${t(source)} ALTER ${column} TYPE ${type}`,
               );
            }
         }
         if (tier.servedFrom === "storage") {
            // The run that bound the altered table carries its schema.
            for (const source of ["drift", "drift_m"]) {
               const [binding] = fixture.bindings.filter(
                  (b) => b.sourceName === source,
               );
               binding.schema = binding.schema.map((c) =>
                  DRIFT[c.name] ? { ...c, type: DRIFT[c.name] } : c,
               );
            }
         } else {
            // A database table's types, read from its warehouse at bind.
            // Each table no longer matches the contract its build recorded:
            // left out, never cast (the two sources may share one table).
            const observed = await fixture.observeDatabaseColumns();
            expect(observed.unread).toEqual([]);
            expect(observed.drifted).toEqual([]);
            expect(observed.incompatible.length).toBeGreaterThan(0);
            for (const d of observed.incompatible) {
               expect(d.code).toBe("column_retyped");
               expect([...d.columns].sort()).toEqual(
                  ["amt", "d", "d2", "flag", "n", "ts"].sort(),
               );
            }
         }
      }, 120_000);
      afterAll(async () => {
         restore();
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
         // A database table that no longer matches its contract answers
         // live; a storage= binding carrying the altered schema reads it.
         const stored = tier.servedFrom === "storage";
         it(`${stored ? "served from the table" : "answered live, its table incompatible"}, equal to live: ${q.name}`, async () => {
            const run = await fixture.run(q);
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            if (stored) {
               expect(run.servedVia).toBe("served_copy");
               expect(run.servedFrom).toBe(tier.servedFrom);
            } else {
               expect(run.servedFrom).not.toBe(tier.servedFrom);
            }
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
         if (tier.servedFrom === "storage") {
            // Named, with its declared and stored types (F-07).
            expect(problems).toEqual([
               {
                  sourceName: "drift_m",
                  reason: "column_types",
                  detail:
                     "column `amt` (declared `number`, stored as VARCHAR) has no declaration that reads it back as live answers it",
               },
            ]);
         } else {
            // A database table that no longer matches its contract is not
            // bound at all, so the served copy re-roots nothing over it.
            expect(problems).toEqual([]);
         }
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

describe("R4 (rollup database): a rollup table whose column types drifted", () => {
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
  #(rollup) grain=["flag"]
  measure: hi is amt.max()
  #(rollup) grain=["id"]
  measure: c is count()
}
`,
         },
         seedSql: SEED,
         preaggregate: true,
      });
      // The max partial of the rollup by `flag`, altered to text after its
      // build; the rollup by `id` is left as built.
      const altered: string[] = [];
      for (const table of Object.values(fixture.rollupTables)) {
         const columns = (await fixture.duckdb.runSQL(`DESCRIBE ${table}`))
            .rows as { column_name: string }[];
         for (const { column_name } of columns) {
            if (
               !column_name.includes("partial") ||
               !column_name.startsWith("hi")
            ) {
               continue;
            }
            await fixture.duckdb.runSQL(
               `ALTER TABLE ${table} ALTER "${column_name}" TYPE VARCHAR`,
            );
            altered.push(column_name);
         }
      }
      expect(altered.length).toBe(1);
      const observed = await fixture.observeDatabaseColumns();
      expect(observed.unread).toEqual([]);
      // The altered rollup's table no longer matches its contract.
      expect(observed.drifted).toEqual([]);
      expect(observed.incompatible).toEqual([
         expect.objectContaining({
            code: "column_retyped",
            columns: [altered[0]],
         }),
      ]);
   }, 120_000);
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("a max over the drifted partial is answered live: live's 10, never the stored text's 9.0", async () => {
      const run = await fixture.run({
         name: "max",
         malloy: "run: ev -> { aggregate: hi }",
      });
      expect(run.served).toEqual([{ hi: 10 }]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedBy?.kind).toBe("live");
   });

   it("the rollup left as built still answers, equal to live", async () => {
      const run = await fixture.run({
         name: "count by id",
         malloy: "run: ev -> { group_by: id; aggregate: c; order_by: id }",
      });
      expect(run.live.length).toBeGreaterThan(0);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(run.servedBy?.kind).toBe("rollup");
   });
});

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const PG_SCHEMA = "r4_observed_drift";

describe.skipIf(!POSTGRES_URL)(
   "R4 (#(materialize) database, Postgres): a table altered after its build",
   () => {
      let restore: () => void;
      let fixture: R4Fixture;
      let pg: PostgresConnection;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "off", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         pg = new PostgresConnection({
            name: "pg",
            connectionString: POSTGRES_URL!,
         });
         await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
         await pg.runSQL(`CREATE SCHEMA ${PG_SCHEMA}`);
         fixture = await buildScenario({
            files: {
               "m.malloy": `source: ev is pg.table('${PG_SCHEMA}.ev')

#(materialize) name="pg_drift_t"
source: drift is ev extend {
  measure: total is n.sum()
}
`,
            },
            seedSql: [
               `CREATE TABLE ${PG_SCHEMA}.ev AS SELECT * FROM (VALUES
                  (1, 9.0::NUMERIC(10,1), 1, DATE '2026-01-15', true),
                  (2, 10.0::NUMERIC(10,1), 2, DATE '2026-02-03', false),
                  (3, 2.5::NUMERIC(10,1), 3, DATE '2026-02-28', true)
                ) AS t(id, amt, n, d, flag)`,
            ],
            materialize: true,
            warehouse: { name: "pg", connection: pg, schema: PG_SCHEMA },
         });
         const table = fixture.physicalTables.drift;
         expect(table).toBeDefined();
         // Out of band: the warehouse's own ALTER, after the build.
         await pg.runSQL(
            `ALTER TABLE ${table} ALTER COLUMN amt TYPE VARCHAR USING amt::varchar`,
         );
         await pg.runSQL(
            `ALTER TABLE ${table} ALTER COLUMN flag TYPE INTEGER USING flag::int`,
         );
         const observed = await fixture.observeDatabaseColumns();
         expect(observed.unread).toEqual([]);
         expect(observed.drifted).toEqual([
            expect.objectContaining({
               columns: [
                  expect.objectContaining({ column: "amt" }),
                  expect.objectContaining({ column: "flag" }),
               ],
            }),
         ]);
      }, 120_000);
      afterAll(async () => {
         restore();
         await disposeScenarios();
         await pg.runSQL(`DROP SCHEMA IF EXISTS ${PG_SCHEMA} CASCADE`);
         await pg.close();
      });

      it("a number now stored as text: answered live, live's 10", async () => {
         const run = await fixture.run({
            name: "max",
            malloy: "run: drift -> { aggregate: m is amt.max() }",
         });
         expect(run.served).toEqual([{ m: 10 }]);
         expect(run.servedFrom).not.toBe("database");
      });

      it("a boolean now stored as an integer: read through a cast, served, equal to live", async () => {
         const run = await fixture.run({
            name: "flag",
            malloy:
               "run: drift -> { group_by: flag; aggregate: c is count(); order_by: flag }",
         });
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("database");
      });

      it("a column left as built: served, equal to live", async () => {
         const run = await fixture.run({
            name: "total",
            malloy: "run: drift -> { aggregate: total }",
         });
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(run.servedFrom).toBe("database");
      });
   },
);
