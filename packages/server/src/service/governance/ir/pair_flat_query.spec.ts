// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { flatPairQueryRefusal, pairMeasureLevels } from "./pair_flat_query";

const model = `source: segments is duckdb.sql("select 1 as seg_id, 1 as flight_id, 2.0 as miles") extend {
  primary_key: seg_id
}
source: carriers is duckdb.sql("select 'AA' as code") extend {
  primary_key: code
  measure: carrier_n is count()
}
source: flights is duckdb.sql("select 1 as id, 'AA' as carrier, 3.0 as distance") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: carriers with carrier
  measure:
    flight_count is count()
    total is distance.sum()
    ratio is total / flight_count
    filtered is flight_count { where: carrier = 'AA' }
    child_sum is segments.miles.sum()
    child_n is segments.count()
    child_max is max(segments.miles)
    by_refs is child_sum / flight_count
    mixed is segments.miles.sum() / count()
    child_filtered_parent is count() { where: segments.miles > 1 }
    through_other is carriers.carrier_n
    windowless_all is all(count())
}`;

let conn: DuckDBConnection;
let runtime: SingleConnectionRuntime;
beforeAll(() => {
   conn = new DuckDBConnection("duckdb", ":memory:");
   runtime = new SingleConnectionRuntime({ connection: conn });
});
afterAll(async () => {
   await conn.close();
});

describe("pairMeasureLevels", () => {
   it("places each measure's aggregates, through the measures it reads", async () => {
      const def = (await runtime.loadModel(model).getModel())._modelDef;
      const levels = pairMeasureLevels(
         (def.contents.flights as { fields: unknown[] }).fields,
         "segments",
      );
      const of = (n: string) => [...(levels.get(n) ?? [])].sort();
      expect(of("flight_count")).toEqual(["parent"]);
      expect(of("total")).toEqual(["parent"]);
      expect(of("ratio")).toEqual(["parent"]);
      expect(of("filtered")).toEqual(["parent"]);
      expect(of("child_sum")).toEqual(["child"]);
      expect(of("child_n")).toEqual(["child"]);
      expect(of("child_max")).toEqual(["child"]);
      expect(of("by_refs")).toEqual(["child", "parent"]);
      expect(of("mixed")).toEqual(["child", "parent"]);
      // Unreadable: absent, so never re-declared.
      for (const n of [
         "child_filtered_parent",
         "through_other",
         "windowless_all",
      ]) {
         expect(levels.has(n)).toBe(false);
      }
   });
});

describe("flatPairQueryRefusal", () => {
   it("refuses a query whose source has no record for the join", async () => {
      const query = runtime
         .loadModel(model)
         .loadQuery("run: flights -> { aggregate: child_n }");
      const prepared = (await query.getPreparedQuery()) as unknown as {
         _query: { pipeline: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      expect(
         flatPairQueryRefusal(
            prepared._query,
            prepared._modelDef.contents.flights as never,
            {
               join: "segments",
               parentRowField: "pr",
               annotatedChildFields: new Set(),
            },
         ),
      ).toMatch(/does not read `segments` from its pair table/);
   });
});
