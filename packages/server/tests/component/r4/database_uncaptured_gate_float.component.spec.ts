// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database table whose build captured no schema or native types (a
 * catalog read that failed), its exact gate column then altered to a DOUBLE
 * in the warehouse. With no capture the binding spells each column as the
 * plan does (every number a DOUBLE), so the bind read the altered column as
 * unmoved, and a gate `acct = $ACCT` over it was kept on the table: two
 * accounts either side of 2^53 round to one double, and either caller saw
 * both accounts' rows. Found by strict-origin mode
 * (conformance/strict_origin.ts). Every caller now gets live's rows,
 * compared as strings.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence givens }
given: ACCT :: number
source: accounts is duckdb.sql("""
  SELECT CAST(acct AS DECIMAL(20,0)) AS acct, region, CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    ('9007199254740992', 'east', 10),
    ('9007199254740993', 'west', 1000)
  ) AS t(acct, region, amount)
""")
#(access_filter) acct = $ACCT
#(materialize) name="acct_db"
source: acct_db is accounts -> { where: amount > 0; select: * }
`;

const QUERY = "run: acct_db -> { select: region, amount; order_by: region }";
// 2^53 is exact as a JavaScript number; its neighbour is not, so only it
// is a caller. Its account's rows are one, the other account's another.
const CALLERS = [9007199254740992];

function asStrings(o: Outcome): string {
   if ("error" in o) return `error: ${o.error}`;
   return JSON.stringify(o.answer.rows, (_k, v) =>
      typeof v === "bigint" ? v.toString() : (v as unknown),
   );
}

describe("a database table whose build captured no schema, its gate column altered to a DOUBLE", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   const live = new Map<number, Outcome>();

   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      const loader = await loadFixture({ "m.malloy": MODEL });
      for (const acct of CALLERS) {
         live.set(acct, await loader.run("m.malloy", QUERY, { ACCT: acct }));
      }
      fixture = await loader.bind({ captureNoNativeSchema: true });
      const warehouse = (await loader.pkg.getMalloyConnection(
         loader.origin,
      )) as unknown as { runSQL(sql: string): Promise<unknown> };
      await warehouse.runSQL(
         `ALTER TABLE "acct_db" ALTER COLUMN acct TYPE DOUBLE`,
      );
      await fixture.rebind();
   });

   afterAll(async () => {
      await fixture?.dispose();
      restore?.();
      await freshLoadPool();
   });

   for (const acct of CALLERS) {
      it(`gives the caller of account ${acct} live's rows`, async () => {
         expect(asStrings(live.get(acct)!)).not.toContain("error");
         const served = await fixture.run("m.malloy", QUERY, { ACCT: acct });
         expect(asStrings(served)).toBe(asStrings(live.get(acct)!));
      });
   }
});
