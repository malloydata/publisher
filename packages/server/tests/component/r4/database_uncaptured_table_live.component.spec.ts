// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database table whose build captured neither its columns nor their
 * native types (a catalog read that failed), altered in its warehouse after
 * the build. With nothing captured, the bind has no built type to compare
 * the table's columns with, so no alter can be seen: a column altered to a
 * float, or to an exact type that changes its values (DECIMAL(20,1) to
 * DECIMAL(20,0) rounds 1.4 to 1), reads as unmoved. A gate re-applied to the
 * rows then admits another caller's, and an ungated read returns the altered
 * values. Such a table now answers live on every leg: served copy, rollup,
 * persisted view. Each answer equals live, compared as strings, per caller,
 * and every surface names why: the serve verdict and the path report say
 * `types_unrecorded`, and the package warns `materialization-types-unrecorded`.
 * This is how a host's typeless manifest entry is read too.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence givens }
given: ACCT :: number
source: big_accounts is duckdb.sql("""
  SELECT CAST(acct AS DECIMAL(20,0)) AS acct, region, CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    ('9007199254740992', 'east', 10),
    ('9007199254740993', 'west', 1000)
  ) AS t(acct, region, amount)
""")
source: tenths is duckdb.sql("""
  SELECT CAST(acct AS DECIMAL(20,1)) AS acct, region, CAST(amount AS DOUBLE) AS amount
  FROM (VALUES ('1.0', 'east', 10), ('1.4', 'west', 1000)) AS t(acct, region, amount)
""")

// A rollup over a gated source, its grain holding the gate column.
#(access_filter) acct = $ACCT
source: gated_big is big_accounts extend {
  #(rollup) grain=["acct", "region"]
  measure: total is amount.sum()
}

// A database table whose gate column an exact-typed alter rounds.
#(access_filter) acct = $ACCT
#(materialize) name="tenths_db"
source: tenths_db is tenths -> { where: amount > 0; select: * }

// The same table ungated.
#(materialize) name="tenths_open"
source: tenths_open is tenths -> { where: amount > -1; select: * }

// A persisted view over it.
source: tenths_v is tenths extend {
  #@ persist name="tenths_view"
  view: by_acct is { group_by: acct; aggregate: n is count() }
}
`;

const CASES: { name: string; query: string; givens?: { ACCT: number } }[] = [
   {
      name: "a gated rollup, its gate column altered to a DOUBLE",
      query: "run: gated_big -> { group_by: region; aggregate: total }",
      givens: { ACCT: 9007199254740992 },
   },
   {
      name: "a gated table, its gate column altered from DECIMAL(20,1) to DECIMAL(20,0)",
      query: "run: tenths_db -> { select: region, amount; order_by: region }",
      givens: { ACCT: 1 },
   },
   {
      name: "an ungated table, its column altered from DECIMAL(20,1) to DECIMAL(20,0)",
      query: "run: tenths_open -> { select: acct, region; order_by: region }",
   },
   {
      name: "a persisted view, its column altered from DECIMAL(20,1) to DECIMAL(20,0)",
      query: "run: tenths_v -> by_acct",
   },
];

/** Each built table's columns altered as the cases describe. */
const ALTERS = [
   { table: /gated_big|rollup/i, sql: "ALTER COLUMN acct TYPE DOUBLE" },
   {
      table: /^tenths_(db|open|view)$/,
      sql: "ALTER COLUMN acct TYPE DECIMAL(20,0)",
   },
];

function asStrings(o: Outcome): string {
   if ("error" in o) return `error: ${o.error}`;
   return JSON.stringify(o.answer.rows, (_k, v) =>
      typeof v === "bigint" ? v.toString() : (v as unknown),
   );
}

describe("a database table whose build captured nothing, altered after its build", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();

   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      const loader = await loadFixture({ "m.malloy": MODEL });
      for (const c of CASES) {
         live.set(c.name, await loader.run("m.malloy", c.query, c.givens));
      }
      fixture = await loader.bind({
         includeRollups: true,
         captureNoNativeSchema: true,
      });
      const warehouse = (await loader.pkg.getMalloyConnection(
         loader.origin,
      )) as unknown as { runSQL(sql: string): Promise<unknown> };
      for (const table of Object.values(fixture.tables)) {
         const alter = ALTERS.find((a) => a.table.test(table));
         if (!alter) continue;
         await warehouse.runSQL(
            `ALTER TABLE "${table.replace(/"/g, '""')}" ${alter.sql}`,
         );
      }
      await fixture.rebind();
   });

   afterAll(async () => {
      await fixture?.dispose();
      restore?.();
      await freshLoadPool();
   });

   it("builds every table the cases read", () => {
      expect(Object.values(fixture.tables).sort()).toEqual(
         expect.arrayContaining(["tenths_db", "tenths_open", "tenths_view"]),
      );
      expect(
         Object.values(fixture.tables).some((t) => /rollup|gated_big/i.test(t)),
      ).toBe(true);
   });

   it("gives each table the types_unrecorded verdict and warning, on every leg", () => {
      const verdicts = fixture.pkg
         .materialization()
         .serveVerdicts()
         .filter((v) => /tenths|gated_big|rollup/i.test(v.sourceName));
      const byOrigin = new Map(verdicts.map((v) => [v.origin, v]));
      for (const origin of ["materialize", "rollup", "view"]) {
         expect(byOrigin.get(origin as never), origin).toMatchObject({
            verdict: "live",
            reason: "types_unrecorded",
         });
      }
      const codes = (fixture.pkg.getPackageMetadata().warnings ?? []).map(
         (w) => (w as { code?: string }).code,
      );
      expect(codes).toContain("materialization-types-unrecorded");
   });

   for (const c of CASES) {
      it(`${c.name}: the path report says types_unrecorded`, async () => {
         const result = await fixture.pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: c.query,
               ...(c.givens ? { givens: c.givens } : {}),
            } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         expect(result.path?.servedBy?.kind).toBe("live");
         const codes = (result.path?.considered ?? []).map(
            (x) => (x as { code?: string }).code,
         );
         expect(codes).toContain("types_unrecorded");
      });
   }

   for (const c of CASES) {
      it(`${c.name}: answers live's rows`, async () => {
         expect(asStrings(live.get(c.name)!)).not.toContain("error");
         const served = await fixture.run("m.malloy", c.query, c.givens);
         expect(asStrings(served)).toBe(asStrings(live.get(c.name)!));
      });
   }
});
