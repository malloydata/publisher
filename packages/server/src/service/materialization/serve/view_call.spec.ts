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

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { extractRunViewCall } from "../../query_text";

const URL_ = "file:///vc/m.malloy";
let duckdb: DuckDBConnection;
let runtime: Runtime;

beforeAll(async () => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   await duckdb.runSQL(
      `CREATE TABLE f AS SELECT * FROM (VALUES ('aa', 1), ('bb', 2), ('aa', 3)) t(carrier, d)`,
   );
   runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([
            [
               URL_,
               `source: ops is duckdb.table('f') extend {
  measure: n is count()
  view: by_carrier is { group_by: carrier; aggregate: n }
}`,
            ],
         ]),
      ),
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
});

afterAll(async () => {
   await duckdb.close();
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
