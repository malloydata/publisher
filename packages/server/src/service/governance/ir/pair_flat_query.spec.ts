// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   flatPairQueryRefusal,
   flatPairQueryVerdict,
   pairMeasureLevels,
   pairQueryReadsChild,
} from "./pair_flat_query";

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
      expect(of("flight_count")).toEqual([0]);
      expect(of("total")).toEqual([0]);
      expect(of("ratio")).toEqual([0]);
      expect(of("filtered")).toEqual([0]);
      expect(of("child_sum")).toEqual([1]);
      expect(of("child_n")).toEqual([1]);
      expect(of("child_max")).toEqual([1]);
      expect(of("by_refs")).toEqual([0, 1]);
      expect(of("mixed")).toEqual([0, 1]);
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
            },
         ),
      ).toMatch(/does not read `segments` from its pair table/);
   });
});

// The served source's shape: the join's name is a record of child columns on
// the same row, parent measures re-declared under a flag.
const flat = `source: flights is duckdb.sql("select 1 as id, 'AA' as carrier, 3.0 as distance, 1 as sid, 2.0 as smiles, true as pr") extend {
  primary_key: id
  dimension: segments is { seg_id is sid, miles is smiles }
  dimension:
    long_haul is distance > 2
    leg is segments.miles
  measure:
    flight_count is count() { where: pr }
    total is distance.sum() { where: pr }
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    long_legs is seg_n { where: segments.miles > 1 }
    per_flight is seg_total / flight_count
  view:
    parent_view is { group_by: carrier; aggregate: flight_count }
    child_view is { group_by: carrier; aggregate: seg_total }
}`;

describe("pairQueryReadsChild and flatPairQueryVerdict", () => {
   const pair = {
      join: "segments",
      parentRowField: "pr",
   };
   const read = async (malloy: string) => {
      const prepared = (await runtime
         .loadModel(flat)
         .loadQuery(malloy)
         .getPreparedQuery()) as unknown as {
         _query: { pipeline: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      const source = prepared._modelDef.contents.flights as never;
      return {
         child: pairQueryReadsChild(prepared._query, source, "segments"),
         verdict: flatPairQueryVerdict(prepared._query, source, pair),
      };
   };

   const parentOnly = [
      "run: flights -> { group_by: carrier; aggregate: flight_count, total }",
      "run: flights -> { group_by: carrier }",
      "run: flights -> { where: long_haul; group_by: carrier; aggregate: flight_count }",
      // Not exact over the pair table either: still the next candidate's.
      "run: flights -> { group_by: carrier; aggregate: c is count() }",
      "run: flights -> parent_view",
      "run: flights -> { group_by: carrier; nest: parent_view }",
      "run: flights -> { select: id, carrier }",
      "run: flights -> { group_by: carrier; aggregate: flight_count } -> { group_by: carrier }",
   ];
   for (const malloy of parentOnly) {
      it(`reads no child: ${malloy}`, async () => {
         const r = await read(malloy);
         expect(r.child).toBe(false);
         expect(r.verdict).toEqual({ kind: "parent_only" });
      });
   }

   const child = [
      // Child aggregates, as measures, written in the query, by count.
      ["run: flights -> { group_by: carrier; aggregate: seg_total }", true],
      ["run: flights -> { aggregate: n is segments.count() }", true],
      ["run: flights -> { aggregate: d is count(segments.seg_id) }", true],
      // A child field grouped by, directly or through a parent dimension.
      ["run: flights -> { group_by: segments.miles }", true],
      ["run: flights -> { group_by: leg; aggregate: seg_n }", true],
      // A filter on the child, in a stage or in a filtered measure.
      [
         "run: flights -> { where: segments.miles > 1; group_by: carrier; aggregate: seg_n }",
         true,
      ],
      ["run: flights -> { aggregate: long_legs }", true],
      // A ratio across both, a nest or a view that reads the child.
      ["run: flights -> { aggregate: per_flight }", true],
      [
         "run: flights -> { group_by: carrier; aggregate: flight_count; nest: n is { group_by: segments.miles; aggregate: seg_n } }",
         true,
      ],
      ["run: flights -> child_view", true],
      ["run: flights -> { group_by: carrier; nest: child_view }", true],
   ] as const;
   for (const [malloy, served] of child) {
      it(`reads the child: ${malloy}`, async () => {
         const r = await read(malloy);
         expect(r.child).toBe(true);
         expect(r.verdict.kind).toBe(served ? "served" : "refused");
      });
   }

   it("reads a source-level filter on the child", async () => {
      const filtered = `${flat}
source: long_leg_flights is flights extend { where: segments.miles > 1 }`;
      const prepared = (await runtime
         .loadModel(filtered)
         .loadQuery("run: long_leg_flights -> { group_by: carrier }")
         .getPreparedQuery()) as unknown as {
         _query: { pipeline: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      expect(
         pairQueryReadsChild(
            prepared._query,
            prepared._modelDef.contents.long_leg_flights as never,
            "segments",
         ),
      ).toBe(true);
   });

   it("fails closed on a query it cannot read", () => {
      const source = { fields: [{ name: "carrier", type: "string" }] };
      expect(
         pairQueryReadsChild(
            {
               pipeline: [
                  {
                     type: "reduce",
                     queryFields: [{ type: "fieldref", path: ["nope"] }],
                  },
               ],
            },
            source,
            "segments",
         ),
      ).toBeUndefined();
      expect(
         pairQueryReadsChild({ pipeline: [] }, source, "segments"),
      ).toBeUndefined();
      expect(flatPairQueryVerdict({ pipeline: [] }, source, pair).kind).toBe(
         "refused",
      );
   });
});
