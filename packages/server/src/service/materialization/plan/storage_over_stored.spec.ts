// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` persisted view or named query over an upstream stored in its
 * own destination is planned with the text it builds from there, proven at
 * plan over a stand-in of the upstream's table, or refused at plan as
 * `upstream_table_unusable`, naming the destination. Never recomputed from
 * raw.
 */

import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { derivePackagePlan } from "./build_plan";
import { UPSTREAM_TABLE_UNUSABLE_REASON } from "./downstream_upstreams";

const RAW = `##! experimental { persistence }
source: raw is duckdb.sql("""
  select * from (values ('a', 'x', 1.5), ('b', 'y', 2.5)) as t(cat, region, amt)
""")

#@ persist storage="lake"
source: a is raw -> { select: cat, region, amt }
`;

const packages = planPackages({
   PUBLISHER_MATERIALIZE: "true",
   PERSIST_STORAGE_MODE: "on",
});

type Stored = {
   upstreams: { sourceID: string; columns: { name: string; type: string }[] }[];
   downstreamName: string;
};

describe("a storage= view or query over an upstream in its own destination", () => {
   it("is planned with its text there, the upstream stood in by the columns its build writes", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
source: s is a extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist storage="lake"
query: q is a -> { group_by: region; aggregate: n is count() }
`,
      });
      for (const name of ["s__by_cat__view", "q__query"]) {
         const table = Object.values(c.tables).find(
            (t) => t.source.name === name,
         ) as { storedInDestination?: Stored } | undefined;
         const stored = table?.storedInDestination;
         expect(stored?.downstreamName).toBe(name);
         expect(stored?.upstreams).toHaveLength(1);
         expect(stored?.upstreams[0].sourceID.startsWith("a@")).toBe(true);
         expect(stored?.upstreams[0].columns).toEqual([
            { name: "cat", type: "VARCHAR" },
            { name: "region", type: "VARCHAR" },
            { name: "amt", type: "DOUBLE" },
         ]);
      }
   });

   it("stands the upstream in by its bound columns when the package binds its table", async () => {
      const { c } = await packages.plan(
         {
            "m.malloy": `${RAW}
#@ persist storage="lake"
query: q is a -> { group_by: region; aggregate: n is count() }
`,
         },
         {
            boundStorageColumns: (sourceID, destination) =>
               sourceID.startsWith("a@") && destination === "lake"
                  ? [
                       { name: "cat", type: "VARCHAR" },
                       { name: "region", type: "VARCHAR" },
                       { name: "amt", type: "DECIMAL(10,1)" },
                    ]
                  : undefined,
         },
      );
      const table = Object.values(c.tables).find(
         (t) => t.source.name === "q__query",
      ) as { storedInDestination?: Stored } | undefined;
      expect(table?.storedInDestination?.upstreams[0].columns).toContainEqual({
         name: "amt",
         type: "DECIMAL(10,1)",
      });
   });

   it("is refused at plan, naming the destination, when it reads the warehouse beside the upstream", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${RAW}
source: regions is duckdb.sql("select 'x' as region, 'X' as label")
source: s is a extend {
  join_one: r is regions on region = r.region
  #@ persist storage="lake"
  view: by_label is { group_by: r.label; aggregate: total is amt.sum() }
}
`,
      });
      const plan = derivePackagePlan(c, null).plan!;
      expect(
         Object.values(c.tables).some(
            (t) => t.source.name === "s__by_label__view",
         ),
      ).toBe(false);
      const refused = Object.values(plan.refusedSources ?? {}) as {
         reason: string;
         message: string;
      }[];
      const ours = refused.find((r) =>
         r.message.includes("its storage= destination 'lake'"),
      );
      expect(ours?.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
      expect(ours?.message).toContain("never built by recomputing");
   });

   it("is refused at plan when no served copy writes its text", async () => {
      const { c } = await packages.plan(
         {
            "m.malloy": `${RAW}
#@ persist storage="lake"
query: q is a -> { group_by: region; aggregate: n is count() }
`,
         },
         { planChainBuildText: undefined },
      );
      expect(
         Object.values(c.tables).some((t) => t.source.name === "q__query"),
      ).toBe(false);
   });
});

const MATERIALIZED = `##! experimental { persistence }
source: raw is duckdb.sql("""
  select * from (values (1, 'a', 'x', 1.5), (2, 'b', 'y', 2.5)) as t(id, cat, region, amt)
""")

#(materialize) storage="lake"
source: m is raw extend {
  primary_key: id
  measure: total is amt.sum()
}
`;

describe("a storage= view or query over a #(materialize) table in its own destination", () => {
   it("is planned with its text there, the table stood in by its stored columns", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${MATERIALIZED}
source: s is m extend {
  #@ persist storage="lake"
  view: by_cat is { group_by: cat; aggregate: total }
}

#@ persist storage="lake"
query: q is m -> { group_by: region; aggregate: n is count() }
`,
      });
      const plan = derivePackagePlan(c, null).plan!;
      expect(plan.refusedSources ?? {}).toEqual({});
      for (const name of ["s__by_cat__view", "q__query"]) {
         const table = Object.values(c.tables).find(
            (t) => t.source.name === name,
         ) as { storedInDestination?: Stored } | undefined;
         const stored = table?.storedInDestination;
         expect(stored?.downstreamName).toBe(name);
         expect(stored?.upstreams.map((u) => u.sourceID.split("@")[0])).toEqual(
            ["m"],
         );
         expect(stored?.upstreams[0].columns).toContainEqual({
            name: "amt",
            type: "DOUBLE",
         });
      }
   });

   /** The refusal naming `name`, from the package plan. */
   const refusalOf = async (files: Record<string, string>, name: string) => {
      const { c } = await packages.plan(files);
      expect(Object.values(c.tables).some((t) => t.source.name === name)).toBe(
         false,
      );
      const plan = derivePackagePlan(c, null).plan!;
      return (
         Object.values(plan.refusedSources ?? {}) as {
            name: string;
            reason: string;
            message: string;
         }[]
      ).find((r) => r.name === name.split("__")[0] || r.name === name);
   };

   it("is refused at plan over a pair table, which holds a row per pair", async () => {
      const refused = await refusalOf(
         {
            "m.malloy": `##! experimental { persistence }
source: items is duckdb.sql("select 1 as item_id, 1 as order_id, 2.5 as price") extend {
  primary_key: item_id
}
#(materialize) storage="lake" inline=["items"]
source: orders is duckdb.sql("select 1 as id, 'x' as cust") extend {
  primary_key: id
  join_many: items is items on items.order_id = id
}

#@ persist storage="lake"
query: q is orders -> { group_by: id; aggregate: n is count() }
`,
         },
         "q__query",
      );
      expect(refused?.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
      expect(refused?.message).toContain(
         "its upstream 'orders' is a pair table, one row per pair of an inlined join_many",
      );
   });

   it("is refused at plan over an extension of a pair parent, built over the pair table", async () => {
      const files = {
         "m.malloy": `##! experimental { persistence }
source: items is duckdb.sql("select 1 as item_id, 1 as order_id, 2.5 as price") extend {
  primary_key: item_id
}
#(materialize) storage="lake" inline=["items"]
source: orders is duckdb.sql("select 1 as id, 'x' as cust") extend {
  primary_key: id
  join_many: items is items on items.order_id = id
}
#(materialize) storage="lake"
source: tagged is orders extend {
  dimension: loud is upper(cust)
}

#@ persist storage="lake"
query: q is tagged -> { group_by: loud; aggregate: n is count() }
`,
      };
      const { c } = await packages.plan(files);
      // The extension's own plan declares no pair: it is refused through the
      // table it is built over.
      const ext = Object.values(c.tables).find(
         (t) => t.source.name === "tagged",
      ) as
         | { materialize?: { pair?: unknown; buildsOver?: unknown } }
         | undefined;
      expect(ext?.materialize?.pair).toBeUndefined();
      expect(ext?.materialize?.buildsOver).toBeDefined();
      const refused = await refusalOf(files, "q__query");
      expect(refused?.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
      expect(refused?.message).toContain(
         "its upstream 'tagged' is a pair table, one row per pair of an inlined join_many",
      );
   });

   it("is refused at plan over a #(materialize) table in another destination", async () => {
      const refused = await refusalOf(
         {
            "m.malloy": `${MATERIALIZED.replace('storage="lake"', 'storage="other"')}
#@ persist storage="lake"
query: q is m -> { group_by: region; aggregate: n is count() }
`,
         },
         "q__query",
      );
      expect(refused?.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
      expect(refused?.message).toContain(
         "its upstream 'm' is stored in the storage= destination 'other'",
      );
   });
});

describe("a storage= #@ persist source over a #(materialize) table in its own destination", () => {
   it("is refused at plan, naming the table, rather than recomputed from raw", async () => {
      const { c } = await packages.plan({
         "m.malloy": `${MATERIALIZED}
#@ persist storage="lake"
source: p is m -> { group_by: region; aggregate: n is count() }

#@ persist storage="lake"
source: joins is raw -> { group_by: id, region } extend {
  join_one: mm is m on id = mm.id
}
`,
      });
      const plan = derivePackagePlan(c, null).plan!;
      const refused = Object.values(plan.refusedSources ?? {}) as {
         name: string;
         reason: string;
         message: string;
      }[];
      const p = refused.find((r) => r.name === "p");
      expect(p?.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
      expect(p?.message).toContain(
         "its upstream 'm' is a #(materialize) table in its storage= destination 'lake', which a #@ persist source is not yet built over",
      );
      expect(p?.message).toContain("never built by recomputing");
      // A join only its read applies is not what its build reads.
      expect(refused.find((r) => r.name === "joins")).toBeUndefined();
      expect(
         Object.values(c.tables).some((t) => t.source.name === "joins"),
      ).toBe(true);
   });
});
