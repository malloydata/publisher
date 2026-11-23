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
