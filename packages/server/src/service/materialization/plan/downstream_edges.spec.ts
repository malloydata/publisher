// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A downstream's edges: a persisted view's, a named query's and a rollup's
 * build source depend on the planned tables their definitions reach, though
 * their companions blank the `#@` lines Malloy would read them from, and a
 * run orders every source after the sources it depends on, across builds.
 */

import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { sourcesInDependencyOrder } from "../domain/compiled_build_plan";
import type { SourceBuild } from "../domain/package_plan";

const MODEL = `##! experimental { persistence }
source: raw is duckdb.sql("""
  select * from (values ('a', 1.5), ('b', 2.5)) as t(cat, amt)
""")

#@ persist name="de_a"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="de_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="de_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }

#(materialize)
source: m is raw extend {
  measure: total is amt.sum()
  #(rollup) grain=["cat"]
  measure: rolled is amt.sum()
}

#(materialize)
source: plain is raw extend { dimension: c2 is cat }

source: sv is plain extend {
  #@ persist name="de_pv"
  view: by_c2 is { group_by: c2; aggregate: n is count() }
}
`;

const packages = planPackages();

/** Each planned source's edges, by its name. */
function edgesByName(c: {
   builds: SourceBuild[];
   tables: Record<string, { source: { name: string } }>;
}): Record<string, string[]> {
   const nameOf = (id: string) => c.tables[id]?.source.name ?? id;
   const out: Record<string, string[]> = {};
   for (const build of c.builds) {
      for (const [id, reads] of Object.entries(build.dependsOn)) {
         if (!c.tables[id]) continue;
         out[nameOf(id)] = [
            ...new Set([...(out[nameOf(id)] ?? []), ...reads.map(nameOf)]),
         ].sort();
      }
   }
   return out;
}

describe("a downstream's dependsOn edges", () => {
   it("names its upstream for a persisted view, a named query and a rollup", async () => {
      const { c } = await packages.plan({ "m.malloy": MODEL });
      const edges = edgesByName(c);
      // `s` extends `a`, so it is planned on `a`'s table too.
      expect(edges["s__by_cat__view"]).toEqual(["a", "s"]);
      expect(edges["q__query"]).toEqual(["a"]);
      expect(edges["sv__by_c2__view"]).toEqual(["plain"]);
      const rollup = Object.values(c.tables).find((t) => t.kind === "rollup")
         ?.source.name;
      expect(rollup).toBeDefined();
      expect(edges[rollup!]).toEqual(["m"]);
      // An upstream reads nothing persisted.
      expect(edges["a"]).toEqual([]);
   });

   it("orders each downstream after its upstream, though its build comes first", async () => {
      const { c } = await packages.plan({ "m.malloy": MODEL });
      const nameOf = (id: string) => c.tables[id]?.source.name ?? id;
      const order = sourcesInDependencyOrder(c.builds)
         .map((i) => i.sourceID)
         .filter((id) => c.tables[id])
         .map(nameOf);
      // The companions' builds are collected before the author model's.
      const firstBuildOrder = c.builds
         .flatMap((b) => b.order)
         .filter((id) => c.tables[id])
         .map(nameOf);
      expect(firstBuildOrder.indexOf("s__by_cat__view")).toBeLessThan(
         firstBuildOrder.indexOf("a"),
      );
      expect(order.indexOf("a")).toBeLessThan(order.indexOf("s__by_cat__view"));
      expect(order.indexOf("a")).toBeLessThan(order.indexOf("q__query"));
      expect(order.indexOf("plain")).toBeLessThan(
         order.indexOf("sv__by_c2__view"),
      );
   });
});

describe("sourcesInDependencyOrder", () => {
   it("keeps the builds' order where no edge says otherwise, and breaks a cycle", () => {
      const builds: SourceBuild[] = [
         {
            connectionName: "c",
            order: ["v", "x"],
            dependsOn: { v: ["u"], x: [] },
         },
         { connectionName: "c", order: ["u"], dependsOn: { u: [] } },
      ];
      expect(sourcesInDependencyOrder(builds).map((i) => i.sourceID)).toEqual([
         "x",
         "u",
         "v",
      ]);
      const cycle: SourceBuild[] = [
         {
            connectionName: "c",
            order: ["p", "q"],
            dependsOn: { p: ["q"], q: ["p"] },
         },
      ];
      expect(sourcesInDependencyOrder(cycle).map((i) => i.sourceID)).toEqual([
         "p",
         "q",
      ]);
   });
});
