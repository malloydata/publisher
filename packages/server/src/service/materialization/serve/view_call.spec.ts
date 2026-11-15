// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Phase 0 Evidence 4 (materialization v2 plan): a query that calls a view is
 * recognized two ways. An exact call shows in the compiled result schema
 * (`resultExplore.name` is the view's name; a hand-written query with the
 * same SQL is `__stage0`). A query that only STARTS from the view, refined
 * or with later stages, shows only in its text, read by `extractRunViewCall`.
 * The two must agree wherever both answer, and the text reader must not take
 * a hand-written look-alike for a call.
 */

import type { Runtime } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { materializationFixture } from "../../../test_helpers/materialization_fixture";
import { extractRunViewCall } from "../../query_text";

const URL_ = "file:///vc/m.malloy";
const mz = materializationFixture();
let runtime: Runtime;

beforeAll(async () => {
   await mz.duckdb.runSQL(
      `CREATE TABLE f AS SELECT * FROM (VALUES ('aa', 1), ('bb', 2), ('aa', 3)) t(carrier, d)`,
   );
   runtime = mz.runtime(
      `source: ops is duckdb.table('f') extend {
  measure: n is count()
  view: by_carrier is { group_by: carrier; aggregate: n }
}`,
      "file:///vc/",
   );
});

async function compiled(query: string) {
   const q = runtime.loadModel(new URL(URL_)).loadQuery(query);
   const prepared = await q.getPreparedResult();
   return { name: prepared.resultExplore.name, sql: prepared.sql };
}

describe("view-call recognition: result schema and query text agree", () => {
   it("an exact call: both name the view", async () => {
      const query = "run: ops -> by_carrier";
      expect((await compiled(query)).name).toBe("by_carrier");
      expect(extractRunViewCall(query)).toEqual({
         source: "ops",
         view: "by_carrier",
         refined: false,
         laterStages: 0,
         otherNames: new Set(["run"]),
      });
   });

   it("a hand-written look-alike: same SQL, and neither calls it the view", async () => {
      const call = await compiled("run: ops -> by_carrier");
      const query = "run: ops -> { group_by: carrier; aggregate: n }";
      const handWritten = await compiled(query);
      expect(handWritten.sql).toBe(call.sql);
      expect(handWritten.name).toBe("__stage0");
      expect(extractRunViewCall(query)).toBeUndefined();
   });

   it("a query starting from the view: only the text names it", async () => {
      const query = "run: ops -> by_carrier -> { select: carrier }";
      expect((await compiled(query)).name).not.toBe("by_carrier");
      expect(extractRunViewCall(query)).toMatchObject({
         view: "by_carrier",
         laterStages: 1,
      });
   });

   it("a refined call: only the text names it, as refined", async () => {
      const query = "run: ops -> by_carrier + { limit: 1 }";
      expect((await compiled(query)).name).not.toBe("by_carrier");
      expect(extractRunViewCall(query)).toMatchObject({
         view: "by_carrier",
         refined: true,
      });
   });
});
