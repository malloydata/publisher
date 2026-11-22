// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Plan-time classification under strict upstreams: a database downstream
 * that cannot be built from its upstream's table is refused when the
 * package is planned, with a reason and a message naming which upstream and
 * why, and answered live: a persisted view or named query over a
 * `storage=` source, and a rollup over a persisted base it reads through a
 * build copy (a given-reading term, a private grain dimension).
 */

import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { derivePackagePlan } from "./build_plan";
import { UPSTREAM_TABLE_UNUSABLE_REASON } from "./downstream_upstreams";

const RAW = `##! experimental { persistence }
source: raw is duckdb.sql("""
  select * from (values ('a', 'x', 1.5), ('b', 'y', 2.5)) as t(cat, region, amt)
""")
`;

const packages = planPackages();

type Refused = { name: string; reason: string; message: string };

async function refusedOf(
   files: Record<string, string>,
): Promise<{ refused: Refused[]; names: string[]; kinds: string[] }> {
   const { c } = await packages.plan(files);
   const plan = derivePackagePlan(c, null).plan!;
   return {
      refused: Object.values(plan.refusedSources ?? {}) as Refused[],
      names: Object.values(c.tables).map((t) => t.source.name),
      kinds: Object.values(c.tables).map((t) => t.kind),
   };
}

describe("a database view or query over a storage= source", () => {
   it("is refused at plan, naming the upstream and its destination", async () => {
      packages.settings({
         PUBLISHER_MATERIALIZE: "true",
         PERSIST_STORAGE_MODE: "on",
      });
      const { refused, names, kinds } = await refusedOf({
         "m.malloy": `${RAW}
#@ persist storage="lake"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="dr_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}

#@ persist name="dr_q"
query: q is a -> { group_by: cat; aggregate: total is amt.sum() }

source: rolled is a extend {
  #(rollup) grain=["cat"]
  measure: total is amt.sum()
}
`,
      });
      for (const name of ["s__by_cat__view", "q__query"]) {
         expect(names).not.toContain(name);
      }
      // A database rollup over it is unchanged: it builds over its author's
      // rows, pending a decision.
      expect(kinds).toContain("rollup");
      const ours = refused.filter((r) =>
         r.message.includes("is stored in the storage= destination 'lake'"),
      );
      expect(ours).toHaveLength(2);
      for (const r of ours) {
         expect(r.reason).toBe(UPSTREAM_TABLE_UNUSABLE_REASON);
         expect(r.message).toContain("its upstream 'a'");
      }
   });

   it("is refused whatever PERSIST_STORAGE_MODE says, from the declaration alone", async () => {
      packages.settings({
         PUBLISHER_MATERIALIZE: "true",
         PERSIST_STORAGE_MODE: "off",
      });
      const { names, refused } = await refusedOf({
         "m.malloy": `${RAW}
#@ persist storage="lake"
source: a is raw -> { select: cat, amt }

source: s is a extend {
  #@ persist name="dr_v"
  view: by_cat is { group_by: cat; aggregate: total is amt.sum() }
}
`,
      });
      expect(names).not.toContain("s__by_cat__view");
      expect(
         refused.filter((r) => r.message.includes("its upstream 'a'")),
      ).toHaveLength(1);
   });
});

describe("a database rollup over a persisted base it reads through a build copy", () => {
   it("is refused at plan when the base re-applies a given-reading term", async () => {
      const { refused } = await refusedOf({
         "m.malloy": `##! experimental { persistence givens }
given: REGION :: string is 'x'
source: raw is duckdb.sql("""
  select * from (values ('a', 'x', 1.5), ('b', 'y', 2.5)) as t(cat, region, amt)
""")

#(materialize)
source: a is raw extend {
  where: region = $REGION
  dimension: cat2 is cat
  #(rollup) grain=["cat", "region"]
  measure: total is amt.sum()
}
`,
      });
      const ours = refused.filter((r) =>
         r.message.includes("reads a given in a term it re-applies at read"),
      );
      expect(ours).toHaveLength(1);
      expect(ours[0].reason).toBe("inherited_where_with_given");
      expect(ours[0].message).toContain("upstream 'a'");
   });

   it("is refused at plan when its grain reads a private dimension", async () => {
      const { refused } = await refusedOf({
         "m.malloy": `##! experimental { persistence access_modifiers }
source: raw is duckdb.sql("""
  select * from (values ('a', 'x', 1.5), ('b', 'y', 2.5)) as t(cat, region, amt)
""")

#(materialize)
source: a is raw extend {
  private dimension: secret is region
  #(rollup) grain=["cat", "secret"]
  measure: total is amt.sum()
}
`,
      });
      const ours = refused.filter((r) => r.message.includes("keeps private"));
      expect(ours).toHaveLength(1);
      expect(ours[0].reason).toBe("hidden_field");
      expect(ours[0].message).toContain("'secret'");
   });

   it("is not refused over an unpersisted base", async () => {
      const { refused, names } = await refusedOf({
         "m.malloy": `##! experimental { persistence access_modifiers }
source: raw is duckdb.sql("""
  select * from (values ('a', 'x', 1.5), ('b', 'y', 2.5)) as t(cat, region, amt)
""")

source: a is raw extend {
  private dimension: secret is region
  #(rollup) grain=["cat", "secret"]
  measure: total is amt.sum()
}
`,
      });
      expect(
         refused.filter((r) => r.message.includes("keeps private")),
      ).toEqual([]);
      expect(names.length).toBe(1);
   });
});
