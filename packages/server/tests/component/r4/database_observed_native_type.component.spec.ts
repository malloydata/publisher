// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database table bound with no captured native types (a build whose
 * catalog read failed) left every big-integer column out, so a query reading
 * an id answered live though the bind's own column read sees the column's
 * BIGINT. Found by strict-origin mode (conformance/strict_origin.ts). A
 * 64-bit integer the read sees is now cast to that type at read, and served;
 * the answer equals live, compared as strings.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence }
#(materialize) name="orders_mz"
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (CAST(9007199254740993 AS BIGINT), 'a', 1.5),
    (CAST(2 AS BIGINT), 'b', 2.5),
    (CAST(-9007199254740993 AS BIGINT), 'a', 4.0)
  ) AS t(id, g, amt)
""") extend {
  primary_key: id
}
`;

const QUERY =
   "run: orders -> { group_by: id; aggregate: t is amt.sum(); order_by: id }";

function asStrings(o: Outcome): string {
   if ("error" in o) return `error: ${o.error}`;
   return JSON.stringify(o.answer.rows, (_k, v) =>
      typeof v === "bigint" ? v.toString() : (v as unknown),
   );
}

describe("a database table whose build captured no native types", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   let live: Outcome;

   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      const loader = await loadFixture({ "m.malloy": MODEL });
      live = await loader.run("m.malloy", QUERY);
      fixture = await loader.bind({ captureNoNativeSchema: true });
   });

   afterAll(async () => {
      await fixture?.dispose();
      restore?.();
      await freshLoadPool();
   });

   it("serves a BIGINT id through the type the bind read, equal to live", async () => {
      expect(asStrings(live)).toContain("9007199254740993");
      const served = await fixture.run("m.malloy", QUERY);
      expect(asStrings(served)).toBe(asStrings(live));
      expect("answer" in served && served.answer.servedBy).toBe("materialized");
   });

   it("warns of no missing native type", () => {
      expect(
         fixture.warnings.filter(
            (w) => w.code === "database-native-type-missing",
         ),
      ).toEqual([]);
   });
});
