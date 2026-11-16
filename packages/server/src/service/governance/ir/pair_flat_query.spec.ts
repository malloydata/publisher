// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   flatPairQueryRefusal,
   flatPairQueryVerdict,
   pairConditionReads,
   pairMeasureLevels,
   pairMeasurePresence,
   pairQueryReadLevel,
   type FlatPair,
} from "./pair_flat_query";

const model = `source: segments is duckdb.sql("select 1 as seg_id, 1 as flight_id, 2.0 as miles") extend {
  primary_key: seg_id
  dimension: doubled is miles * 2
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
    child_ones is segments.sum(1)
    child_parent_sum is segments.sum(distance)
    child_or_zero_avg is segments.avg(segments.miles ?? 0)
    child_n_filtered is segments.count() { where: segments.miles > 1 }
    child_n_and_floor is segments.count() + max(segments.miles ?? 0)
    child_doubled_sum is segments.doubled.sum()
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
    seg_present is segments.seg_id is not null
  measure:
    flight_count is count() { where: pr }
    total is distance.sum() { where: pr }
    seg_total is segments.miles.sum()
    seg_n is segments.count()
    seg_n_present is segments.count() { where: seg_present }
    long_legs is seg_n { where: segments.miles > 1 }
    per_flight is seg_total / flight_count
  view:
    parent_view is { group_by: carrier; aggregate: flight_count }
    child_view is { group_by: carrier; aggregate: seg_total }
}`;

/** Whether a query reads through the join, as flatPairQueryVerdict asks. */
function readsChild(
   query: Parameters<typeof pairQueryReadLevel>[0],
   source: Parameters<typeof pairQueryReadLevel>[1],
): boolean | undefined {
   const level = pairQueryReadLevel(query, source, "segments");
   return level === undefined ? undefined : level > 0;
}

describe("pairQueryReadLevel and flatPairQueryVerdict", () => {
   const pair = {
      join: "segments",
      parentRowField: "pr",
      leafKeyChecked: true,
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
         child: readsChild(prepared._query, source),
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
      // Child aggregates, as measures, written in the query, by count. A
      // count at the child is not exact over the pair table: live skips a
      // childless parent's null-extended row (malloydata/malloy#3208).
      ["run: flights -> { group_by: carrier; aggregate: seg_total }", true],
      ["run: flights -> { aggregate: n is segments.count() }", false],
      ["run: flights -> { aggregate: d is count(segments.seg_id) }", true],
      // A child field grouped by, directly or through a parent dimension.
      ["run: flights -> { group_by: segments.miles }", true],
      ["run: flights -> { group_by: leg; aggregate: seg_total }", true],
      // A filter on the child, in a stage or in a filtered measure.
      [
         "run: flights -> { where: segments.miles > 1; group_by: carrier; aggregate: seg_total }",
         true,
      ],
      ["run: flights -> { aggregate: long_legs }", false],
      // A ratio across both, a nest or a view that reads the child.
      ["run: flights -> { aggregate: per_flight }", true],
      [
         "run: flights -> { group_by: carrier; aggregate: flight_count; nest: n is { group_by: segments.miles; aggregate: seg_total } }",
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
         readsChild(
            prepared._query,
            prepared._modelDef.contents.long_leg_flights as never,
         ),
      ).toBe(true);
   });

   it("fails closed on a query it cannot read", () => {
      const source = { fields: [{ name: "carrier", type: "string" }] };
      expect(
         readsChild(
            {
               pipeline: [
                  {
                     type: "reduce",
                     queryFields: [{ type: "fieldref", path: ["nope"] }],
                  },
               ],
            },
            source,
         ),
      ).toBeUndefined();
      expect(readsChild({ pipeline: [] }, source)).toBeUndefined();
      expect(flatPairQueryVerdict({ pipeline: [] }, source, pair).kind).toBe(
         "refused",
      );
   });
});

describe("pairConditionReads", () => {
   // A pair source inlining `segments`, beside a join_many it does not inline
   // (`notes`), a join_one (`carriers`) and a join_one that itself joins many
   // (`hubs.gates`).
   const gated = `source: segments is duckdb.sql("select 1 as seg_id, 1 as flight_id, 'west' as region") extend {
  primary_key: seg_id
}
source: notes is duckdb.sql("select 1 as note_id, 1 as flight_id, 'red' as color") extend {
  primary_key: note_id
}
source: carriers is duckdb.sql("select 'AA' as code, 'x' as alliance") extend {
  primary_key: code
}
source: hubs is duckdb.sql("select 'AA' as code") extend {
  primary_key: code
  join_many: gates is duckdb.sql("select 'AA' as code, 'A1' as gate") on gates.code = code
}
source: flights is duckdb.sql("select 1 as id, 'AA' as carrier") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_many: notes on notes.flight_id = id
  join_one: carriers with carrier
  join_one: hubs with carrier
  dimension:
    red_note is notes.color = 'red'
    hub_gate is hubs.gates.gate
}`;
   // The condition a gate grafts, as the probe lifts it: the filter of a
   // stage that applies it.
   const reads = async (condition: string) => {
      const prepared = (await runtime
         .loadModel(gated)
         .loadQuery(
            `run: flights -> { where: ${condition}; group_by: carrier }`,
         )
         .getPreparedQuery()) as unknown as {
         _query: { pipeline: { filterList: { e: unknown }[] }[] };
         _modelDef: { contents: Record<string, unknown> };
      };
      return pairConditionReads(
         prepared._query.pipeline[0].filterList[0].e,
         prepared._modelDef.contents.flights as never,
         "segments",
      );
   };

   const cases = [
      ["carrier = 'AA'", { level: 0, fansOut: false }],
      ["segments.region = 'west'", { level: 1, fansOut: false }],
      ["carriers.alliance = 'x'", { level: 0, fansOut: false }],
      ["notes.color = 'red'", { level: 0, fansOut: true }],
      // Through a dimension that reads the join_many.
      ["red_note", { level: 0, fansOut: true }],
      // A join_many reached through a join_one, directly and by a dimension.
      ["hubs.gates.gate = 'A1'", { level: 0, fansOut: true }],
      ["hub_gate = 'A1'", { level: 0, fansOut: true }],
      // Both: the child read does not end the walk before the fan-out.
      [
         "segments.region = 'west' and notes.color = 'red'",
         { level: 1, fansOut: true },
      ],
   ] as const;
   for (const [condition, expected] of cases) {
      it(condition, async () => {
         expect(await reads(condition)).toEqual(expected);
      });
   }
});

describe("flatPairQueryRefusal at the leaf, beside a childless parent's null row", () => {
   // Since malloydata/malloy#3208 live's count, sum and avg at a join skip
   // the row a LEFT JOIN writes for a parent with no child; the pair table
   // holds that row, null-extended.
   let pair: Partial<FlatPair> = { leafKeyChecked: true };
   const refusal = async (malloy: string) => {
      const prepared = (await runtime
         .loadModel(flat)
         .loadQuery(malloy)
         .getPreparedQuery()) as unknown as {
         _query: { pipeline: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      return flatPairQueryRefusal(
         prepared._query,
         prepared._modelDef.contents.flights as never,
         { join: "segments", parentRowField: "pr", ...pair },
      );
   };
   for (const malloy of [
      "run: flights -> { group_by: carrier; aggregate: seg_total }",
      "run: flights -> { aggregate: a is segments.miles.avg() }",
      "run: flights -> { aggregate: m is max(segments.miles + 1) }",
      "run: flights -> { aggregate: d is count(segments.seg_id) }",
   ]) {
      it(`serves ${malloy}`, async () => {
         expect(await refusal(malloy)).toBeUndefined();
      });
   }
   it("refuses a count at the leaf", async () => {
      expect(
         await refusal("run: flights -> { aggregate: n is segments.count() }"),
      ).toMatch(/a count through the inlined join/);
   });
   for (const malloy of [
      "run: flights -> { aggregate: s is segments.sum(1) }",
      "run: flights -> { aggregate: s is segments.sum(segments.miles + 1) }",
   ]) {
      it(`refuses a sum of a value not null on that row: ${malloy}`, async () => {
         expect(await refusal(malloy)).toMatch(
            /not null on the pair table's row/,
         );
      });
   }

   // Under the leaf's presence field (its key not null) the flat rows are
   // the leaf's own rows, as live's guard reads them (known limitation 40).
   describe("under the leaf's presence field", () => {
      beforeAll(() => {
         pair = { leafKeyChecked: true, levelPresentFields: ["seg_present"] };
      });
      afterAll(() => {
         pair = { leafKeyChecked: true };
      });
      for (const malloy of [
         "run: flights -> { group_by: carrier; aggregate: seg_n_present }",
         "run: flights -> { aggregate: s is segments.sum(1) { where: seg_present } }",
         "run: flights -> { aggregate: s is segments.sum(distance) { where: seg_present } }",
         "run: flights -> { aggregate: a is segments.avg(segments.miles ?? 0) { where: seg_present } }",
         "run: flights -> { aggregate: r is seg_n_present / flight_count }",
         "run: flights -> { aggregate: m is max(segments.miles) { where: seg_present } }",
      ]) {
         it(`serves ${malloy}`, async () => {
            expect(await refusal(malloy)).toBeUndefined();
         });
      }
      it("refuses a max of a value not null on the null row under it, which live reads that row for", async () => {
         expect(
            await refusal(
               "run: flights -> { aggregate: m is max(segments.miles ?? 0) { where: seg_present } }",
            ),
         ).toMatch(/minimum, maximum or distinct count/);
      });
      it("is not a presence field for a count when it is not declared one", async () => {
         pair = { leafKeyChecked: true };
         expect(
            await refusal("run: flights -> { aggregate: seg_n_present }"),
         ).toMatch(/a count through the inlined join/);
         pair = { leafKeyChecked: true, levelPresentFields: ["seg_present"] };
      });
   });

   // Without the leaf's key check on record, a NULL key or two equal
   // children of one key cannot be ruled out: no count, sum or average at
   // the leaf is exact, a stored column's sum included.
   describe("without the leaf's key check", () => {
      beforeAll(() => {
         pair = { levelPresentFields: ["seg_present"] };
      });
      afterAll(() => {
         pair = { leafKeyChecked: true };
      });
      for (const malloy of [
         "run: flights -> { group_by: carrier; aggregate: seg_total }",
         "run: flights -> { aggregate: a is segments.miles.avg() }",
         "run: flights -> { aggregate: seg_n_present }",
      ]) {
         it(`refuses ${malloy}`, async () => {
            expect(await refusal(malloy)).toMatch(
               /not known unique and never null/,
            );
         });
      }
      for (const malloy of [
         "run: flights -> { aggregate: m is max(segments.miles + 1) }",
         "run: flights -> { aggregate: d is count(segments.seg_id) }",
      ]) {
         it(`serves ${malloy}`, async () => {
            expect(await refusal(malloy)).toBeUndefined();
         });
      }
   });
});

describe("pairMeasurePresence", () => {
   it("says which measures through the join need the presence field, and which it would make wrong", async () => {
      const def = (await runtime.loadModel(model).getModel())._modelDef;
      const fields = (def.contents.flights as { fields: unknown[] }).fields;
      const presence = pairMeasurePresence(fields, { join: "segments" });
      expect(Object.fromEntries(presence)).toEqual({
         child_sum: "unneeded",
         child_n: "needed",
         child_max: "unneeded",
         child_ones: "needed",
         child_parent_sum: "needed",
         child_or_zero_avg: "needed",
         child_n_filtered: "needed",
         child_n_and_floor: "unsafe",
         child_doubled_sum: "unneeded",
      });
      // A stored computed field need not be null on the null row.
      expect(
         pairMeasurePresence(fields, {
            join: "segments",
            computedFields: ["segments.doubled"],
         }).get("child_doubled_sum"),
      ).toBe("needed");
   });
});

describe("flatPairQueryRefusal through a join_one's join_many", () => {
   // The served shape, beside a join_one whose joined source joins many.
   const served = `source: hubs is duckdb.sql("select 'AA' as code") extend {
  primary_key: code
  join_many: gates is duckdb.sql("select 'AA' as code, 'A1' as gate") on gates.code = code
}
source: flights is duckdb.sql("select 1 as id, 'AA' as carrier, 1 as sid, 2.0 as smiles, true as pr") extend {
  primary_key: id
  dimension: segments is { seg_id is sid, miles is smiles }
  join_one: hubs with carrier
  measure: seg_n is segments.miles.sum()
}`;
   const refusal = async (malloy: string) => {
      const prepared = (await runtime
         .loadModel(served)
         .loadQuery(malloy)
         .getPreparedQuery()) as unknown as {
         _query: { pipeline: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      return flatPairQueryRefusal(
         prepared._query,
         prepared._modelDef.contents.flights as never,
         { join: "segments", parentRowField: "pr", leafKeyChecked: true },
      );
   };
   it("serves a join_one's own field", async () => {
      expect(
         await refusal(
            "run: flights -> { group_by: hubs.code; aggregate: seg_n }",
         ),
      ).toBeUndefined();
   });
   for (const malloy of [
      "run: flights -> { group_by: hubs.gates.gate; aggregate: seg_n }",
      "run: flights -> { where: hubs.gates.gate = 'A1'; aggregate: seg_n }",
      "run: flights -> { aggregate: r is seg_n { where: hubs.gates.gate = 'A1' } }",
   ]) {
      it(`refuses a read through it: ${malloy}`, async () => {
         expect(await refusal(malloy)).toMatch(/fans out a pair table's rows/);
      });
   }
});
