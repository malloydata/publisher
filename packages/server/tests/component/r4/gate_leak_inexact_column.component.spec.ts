// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * L2: a row-level gate over a column a `storage=` table stores inexactly,
 * through the real package path (plan, bind, router, `govern()`).
 *
 * A Postgres build whose column-type probe failed used to store an exact
 * `numeric` gate column as DOUBLE. Two ids either side of 2^53 then round to
 * one value, and a gate `acct = $ACCT` on the stored table admits both
 * tenants' rows for either caller. The build now fails instead
 * (`run/postgres_numeric_precision.spec.ts`); this spec pins the second layer:
 * whatever wrote it, a table whose gate column is stored as a float is never
 * read past the gate. The answer is live's, under G3 and with the
 * gated-sources switch on.
 *
 * The inexact table is made here as such a build left it: the exact column
 * altered to DOUBLE in the destination and bound with that type. A control
 * table left exact is still served from storage.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

const ACCOUNTS = `
source: accounts is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(acct AS DECIMAL(20,0)) AS acct, region,
         CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    (1, '9007199254740992', 'east', 10),
    (2, '9007199254740992', 'east', 20),
    (3, '9007199254740993', 'west', 1000),
    (4, '9007199254740993', 'west', 2000),
    (5, '9007199254740995', 'north', 7)
  ) AS t(id, acct, region, amount)
""") extend { primary_key: id }
source: accounts_bigint is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(acct AS BIGINT) AS acct, region,
         CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    (1, 9007199254740992, 'east', 10),
    (2, 9007199254740992, 'east', 20),
    (3, 9007199254740993, 'west', 1000),
    (4, 9007199254740993, 'west', 2000),
    (5, 9007199254740995, 'north', 7)
  ) AS t(id, acct, region, amount)
""") extend { primary_key: id }
`;

const MODEL = `##! experimental { givens persistence access_modifiers composite_sources }
given: ACCT :: number
given: ROLE :: string
${ACCOUNTS}

#(access_filter) acct = $ACCT
#(materialize) name="acct_mz" storage="${LAKE}"
source: acct_mz is accounts -> { where: id > -1; select: * } extend { measure: n is count() }

#(access_filter) acct = $ACCT
#@ persist name="acct_v1" storage="${LAKE}"
source: acct_v1 is accounts -> { where: id > -2; select: id, acct, region, amount }

// The control: the same shape, its gate column left exact.
#(access_filter) acct = $ACCT
#(materialize) name="acct_exact" storage="${LAKE}"
source: acct_exact is accounts -> { where: id > -3; select: * } extend { measure: n is count() }

// Entry points over an ungated stored base, each gated on its column under
// another shape: a query stage that renames it, a grouping stage, an
// extension, a dimension over it, and a join to it.
#(materialize) name="acct_ub" storage="${LAKE}"
source: ub is accounts -> { where: id > -4; select: * } extend { measure: n is count() }
#(access_filter) k = $ACCT
source: renamed is ub -> { select: id, region, amount, k is acct }
#(access_filter) k = $ACCT
source: renamed_grouped is ub -> { group_by: k is acct, region; aggregate: c2 is count() }
#(access_filter) acct = $ACCT
source: ub_ext is ub extend {}
#(access_filter) k2 = $ACCT
source: ub_dim is ub extend { dimension: k2 is acct }
#(access_filter) u.acct = $ACCT
source: via_join is accounts extend { join_one: u is ub on id = u.id }

// The row scope in a where: that reads a given: under a lock that reads
// only a given, in a composite's member, and with no gate at all. An exact
// stored base scoped the same way is the control, still served.
#(authorize) 'analyst' = $ROLE
source: w_lock is ub extend { where: acct = $ACCT }
#(authorize) 'analyst' = $ROLE
source: w_comp is compose(ub extend { where: acct = $ACCT }, ub)
source: w_plain is ub extend { where: acct = $ACCT }
#(materialize) name="acct_ub_exact" storage="${LAKE}"
source: ub_exact is accounts -> { where: id > -9; select: * } extend { measure: n is count() }
source: w_plain_exact is ub_exact extend { where: acct = $ACCT }

// Database tables whose exact gate column is altered to DOUBLE in the
// warehouse after the build, and rebound: the bind observes the drift.
#(access_filter) acct = $ACCT
#(materialize) name="acct_db_mz"
source: acct_db_mz is accounts -> { where: id > -5; select: * }
#(access_filter) acct = $ACCT
#@ persist name="acct_db_v1"
source: acct_db_v1 is accounts -> { where: id > -6; select: id, acct, region, amount }
#(access_filter) acct = $ACCT
#(materialize) name="acct_db_mz_bigint"
source: acct_db_mz_bigint is accounts_bigint -> { where: id > -7; select: * }
#(access_filter) acct = $ACCT
#@ persist name="acct_db_v1_bigint"
source: acct_db_v1_bigint is accounts_bigint -> { where: id > -8; select: id, acct, region, amount }
// The control: an ungated database table altered the same way, still served.
#(materialize) name="acct_db_open"
source: acct_db_open is accounts -> { where: id > -10; select: * }
`;

/** The tables whose gate column is altered to DOUBLE after the build. */
const INEXACT = ["acct_mz", "acct_v1", "ub"];

/** Database tables whose gate column is altered in the warehouse after the build. */
const DB_ALTERED = [
   "acct_db_mz",
   "acct_db_v1",
   "acct_db_mz_bigint",
   "acct_db_v1_bigint",
];

/**
 * An altered database table whose gate column the served copy still reads (a
 * DECIMAL): the gate policy's exactness rule is what sends it live, so the
 * path report names the served copy skipped for its gate (`CONTRACT`), not
 * the copy's own decline (`WARNED`) that a BIGINT one, left out of the copy
 * as a float, gets with or without that rule. (The `#@ persist` table's path
 * report names no served-copy skip; that it answers live is what this spec
 * holds it to.)
 */
const DB_ALTERED_DECIMAL = ["acct_db_mz"];

/** Entry points that read an inexact table's gate column under another shape. */
const OVER_INEXACT = [
   "renamed",
   "renamed_grouped",
   "ub_ext",
   "ub_dim",
   "w_lock",
   "w_comp",
   "w_plain",
];

const QUERIES: { source: string; malloy: string }[] = [
   ...["acct_mz", "acct_v1", "acct_exact"].map((source) => ({
      source,
      malloy: `run: ${source} -> { select: id, region; order_by: id }`,
   })),
   {
      source: "acct_mz",
      malloy: "run: acct_mz -> { group_by: region; aggregate: n }",
   },
   {
      source: "acct_exact",
      malloy: "run: acct_exact -> { group_by: region; aggregate: n }",
   },
   ...[...DB_ALTERED, "acct_db_open", "w_plain_exact"].map((source) => ({
      source,
      malloy: `run: ${source} -> { select: id, region; order_by: id }`,
   })),
   ...[...OVER_INEXACT, "via_join"].map((source) => ({
      source,
      malloy: `run: ${source} -> { group_by: region; aggregate: c is count(); order_by: region }`,
   })),
];

// Big ids are sent as strings: a JSON number cannot carry 2^53 + 1.
const CALLERS: Record<string, Record<string, string>> = {
   "the ...992 caller": { ACCT: "9007199254740992", ROLE: "analyst" },
   "the ...993 caller": { ACCT: "9007199254740993", ROLE: "analyst" },
   "the ...994 caller (no rows)": {
      ACCT: "9007199254740994",
      ROLE: "analyst",
   },
   "the ...995 caller": { ACCT: "9007199254740995", ROLE: "analyst" },
};
const ORDER = [...Object.keys(CALLERS), ...Object.keys(CALLERS).reverse()];

function show(o: Outcome): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   return rowDifference(live.answer.rows, served.answer.rows);
}

const MODES: [string, Record<string, string>][] = [
   ["G3 (gated sources off)", { PUBLISHER_MATERIALIZE_GATED_SOURCES: "false" }],
   [
      "the gated-sources switch on",
      { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
   ],
];

describe("L2: a gate over a column stored inexactly is never read from the table", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   const key = (malloy: string, caller: string) => `${malloy}\u0000${caller}`;

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
         },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": MODEL });
      for (const q of QUERIES) {
         for (const caller of Object.keys(CALLERS)) {
            live.set(
               key(q.malloy, caller),
               await loader.run("m.malloy", q.malloy, CALLERS[caller]),
            );
         }
      }
      fixture = await loader.bind();
      // What a build whose type probe failed stored: the exact column as
      // DOUBLE, bound with that type.
      for (const entry of Object.values(fixture.stored)) {
         const name = String(entry.sourceName);
         if (!INEXACT.includes(name)) continue;
         const table = `"${String(entry.physicalTableName).replace(/"/g, '""')}"`;
         await loader.lake.runSQL(
            `ALTER TABLE ${table} ALTER COLUMN acct TYPE DOUBLE`,
         );
         entry.schema = (entry.schema as { name: string; type: string }[]).map(
            (c) => (c.name === "acct" ? { ...c, type: "DOUBLE" } : c),
         );
      }
      // A database table altered in its warehouse after the build.
      const warehouse = (await loader.pkg.getMalloyConnection(
         "duckdb",
      )) as unknown as { runSQL(sql: string): Promise<unknown> };
      for (const name of [...DB_ALTERED, "acct_db_open"]) {
         await warehouse.runSQL(
            `ALTER TABLE "${name}" ALTER COLUMN acct TYPE DOUBLE`,
         );
      }
      // Bound as a server binds: each database table read as its warehouse
      // holds it now.
      await fixture.rebind({ observe: true });
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("builds every table, and the altered ones hold the gate column as DOUBLE", async () => {
      expect(fixture.buildErrors).toEqual({});
      for (const entry of Object.values(fixture.stored)) {
         const name = String(entry.sourceName);
         if (!INEXACT.includes(name)) continue;
         const described = await loader.lake.runSQL(
            `DESCRIBE "${String(entry.physicalTableName)}"`,
         );
         const acct = (
            described.rows as { column_name: string; column_type: string }[]
         ).find((r) => r.column_name === "acct");
         expect(acct?.column_type).toBe("DOUBLE");
      }
      const built = fixture.built.map((b) => `${b.name}:${b.tier}`);
      const warehouse = (await loader.pkg.getMalloyConnection(
         "duckdb",
      )) as unknown as {
         runSQL(sql: string): Promise<{ rows: Record<string, unknown>[] }>;
      };
      for (const name of DB_ALTERED) {
         expect(built).toContain(`${name}:colocated`);
         const described = await warehouse.runSQL(`DESCRIBE "${name}"`);
         const acct = described.rows.find((r) => r.column_name === "acct");
         expect(acct?.column_type).toBe("DOUBLE");
      }
   });

   for (const [mode, env] of MODES) {
      describe(mode, () => {
         const served = new Map<string, Outcome[]>();
         beforeAll(async () => {
            const { restore: undo } = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  ...env,
               },
            });
            try {
               for (const q of QUERIES) {
                  for (const caller of ORDER) {
                     const outcomes = served.get(key(q.malloy, caller)) ?? [];
                     outcomes.push(
                        await fixture.run(
                           "m.malloy",
                           q.malloy,
                           CALLERS[caller],
                        ),
                     );
                     served.set(key(q.malloy, caller), outcomes);
                  }
               }
            } finally {
               undo();
            }
         }, 600_000);

         for (const q of QUERIES) {
            it(`${q.malloy}: every caller gets live's answer`, () => {
               for (const caller of Object.keys(CALLERS)) {
                  const want = live.get(key(q.malloy, caller))!;
                  for (const got of served.get(key(q.malloy, caller)) ?? []) {
                     const diff = difference(want, got);
                     expect(
                        diff,
                        `${caller}: ${diff ?? ""}\n${show(got)}`,
                     ).toBeUndefined();
                  }
               }
            });
            // A join to the stored base is held to live's rows above; where
            // it is answered from depends on the switch, not on this rule.
            if (q.source === "via_join") continue;
            const goesLive =
               INEXACT.includes(q.source) ||
               OVER_INEXACT.includes(q.source) ||
               DB_ALTERED.includes(q.source);
            it(`${q.malloy}: is answered from ${goesLive ? "live" : "the table"}`, () => {
               const got = served.get(key(q.malloy, "the ...992 caller"))![0];
               expect("answer" in got).toBe(true);
               if (!("answer" in got)) return;
               if (goesLive) {
                  expect(got.answer.servedBy).toBe("live");
                  if (DB_ALTERED_DECIMAL.includes(q.source)) {
                     expect(got.answer.servedSkip?.reason).toBe("CONTRACT");
                  }
               } else {
                  expect(got.answer.via).toBe("served_copy");
                  expect(got.answer.servedFrom).toBe(
                     q.source === "acct_db_open" ? "database" : "storage",
                  );
               }
            });
         }
      });
   }
});
