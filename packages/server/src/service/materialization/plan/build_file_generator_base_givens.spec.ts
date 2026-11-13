// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { addressSQL } from "./build_plan";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./build_plan_compile";
import {
   buildCopyText,
   keptBasesInText,
   type MaterializePlan,
} from "./build_file_generator";

/**
 * A base's given-reading `where:` and given-reading dimensions on the
 * `#(materialize)` build side: the common governed shape, a base scoped by a
 * given and extensions over it. The term is taken out of the base in the
 * extension's build copy and the dimension out of its `select:`, so the table
 * holds every caller's rows and no caller's value; the plan names the bases
 * whose blocks the served copy keeps (`keptBases`), which re-apply both at
 * read. Anything the served copy could not keep refuses the source.
 */

const HEAD = `##! experimental { givens access_modifiers }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5),
    (2, 2, 'b', 20.0),
    (3, 1, 'c', 5.25)
  ) as t(id, org_id, s, amount)
""")
`;

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

async function compiled(text: string | Record<string, string>) {
   const pkg = await packages.create(text);
   return { pkg, c: await compilePackageBuildPlan(pkg) };
}

type Compiled = Awaited<ReturnType<typeof compilePackageBuildPlan>>;

function planOf(c: Compiled, name: string): MaterializePlan | undefined {
   return Object.values(c.materializePlans ?? {}).find(
      (p) => p.sourceName === name,
   );
}

function notBuilt(c: Compiled, source: string): string | undefined {
   return (c.annotationFindings ?? []).find(
      (f) => f.code === "materialize-not-built" && f.subject === source,
   )?.message;
}

function names(plan: MaterializePlan | undefined): string[] | undefined {
   return plan?.keptBases?.map((b) => b.sourceID.split("@")[0]);
}

const SCOPED = `${HEAD}
source: scoped is raw extend {
  where: org_id = $ORG_ID
  dimension: mine is org_id = $ORG_ID
  dimension: big is amount > 10
}
`;

describe("an inherited given-reading where: is left out of the build", () => {
   it(
      "takes the base's term out of the build and names the base the served copy keeps",
      async () => {
         const { pkg, c } = await compiled(`${SCOPED}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
         const p = planOf(c, "orders");
         expect(p?.readTimeTerms?.map((t) => [t.kind, t.code])).toEqual([
            ["given", "org_id = $ORG_ID"],
         ]);
         expect(names(p)).toEqual(["scoped"]);
         expect(p?.givenDeclarations?.map((g) => g.name)).toEqual(["ORG_ID"]);
         expect(p?.leftOut?.map((d) => d.dimension)).toEqual(["mine"]);
         // The build reads no given: neither the term nor the dimension.
         const source = Object.values(c.sources).find(
            (s) => s.name === "orders",
         );
         const sql = addressSQL(source!, c.targets?.[p!.sourceID]);
         expect(sql).not.toMatch(/"org_id"\s*=\s*1/);
         expect(sql).not.toContain('"mine"');
         const outcome = await computePackageBuildPlan(pkg);
         expect(outcome.plan?.refusedSources?.[p!.sourceID]).toBeUndefined();
         expect(outcome.sourceEligibility?.eligible).toContain(p!.sourceID);
      },
      { timeout: 30000 },
   );

   it(
      "names every base on the way to the one that writes the term, nearest first",
      async () => {
         const { c } = await compiled(`${SCOPED}
source: mid is scoped extend { dimension: tag is 'x' }
source: upper_mid is mid extend { measure: total is amount.sum() }
#(materialize) storage="lake"
source: orders is upper_mid extend {
  where: amount > 0
  measure: n is count()
}
`);
         const p = planOf(c, "orders");
         expect(names(p)).toEqual(["upper_mid", "mid", "scoped"]);
         // The source's own fixed term stays in the build.
         expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
            "org_id = $ORG_ID",
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "takes out a base's term and the source's own together",
      async () => {
         const { c } = await compiled(`${HEAD.replace(
            "ORG_ID :: number is 1",
            "ORG_ID :: number is 1\n  MIN_AMT :: number is 0",
         )}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { where: amount >= $MIN_AMT }
`);
         const p = planOf(c, "orders");
         expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
            "org_id = $ORG_ID",
            "amount >= $MIN_AMT",
         ]);
         const source = Object.values(c.sources).find(
            (s) => s.name === "orders",
         );
         const sql = addressSQL(source!, c.targets?.[p!.sourceID]);
         expect(sql).not.toMatch(/"org_id"\s*=\s*1/);
         expect(sql).not.toMatch(/>=\s*0/);
      },
      { timeout: 30000 },
   );

   it("writes `true` in the base's term in the build copy, and leaves every other line as written", () => {
      const text = `source: scoped is raw extend {
  where: org_id = $ORG_ID
}
source: orders is scoped extend {
  measure: n is count()
}
`;
      const url = "file:///pkg/m.malloy";
      const plan: MaterializePlan = {
         sourceName: "orders",
         sourceID: `orders@${url}`,
         buildSourceName: "orders__mat",
         columns: [{ dimension: "org_id", column: "org_id", computed: false }],
         persist: { name: "orders", storage: "lake" },
         declaration: {
            url,
            range: {
               start: { line: 3, character: 8 },
               end: { line: 5, character: 1 },
            },
         },
         keptBases: [
            {
               sourceID: `scoped@${url}`,
               range: {
                  start: { line: 0, character: 8 },
                  end: { line: 2, character: 1 },
               },
            },
         ],
         readTimeTerms: [
            {
               kind: "given",
               code: "org_id = $ORG_ID",
               givens: ["ORG_ID"],
               columns: ["org_id"],
               at: [
                  {
                     url,
                     range: {
                        start: { line: 1, character: 9 },
                        end: { line: 1, character: 15 },
                     },
                  },
                  {
                     url,
                     range: {
                        start: { line: 1, character: 18 },
                        end: { line: 1, character: 25 },
                     },
                  },
               ],
            },
         ],
      };
      const result = buildCopyText(text, plan);
      expect(result).toEqual({
         copy: text.replace("where: org_id = $ORG_ID", "where: true"),
      });
      // A base the text does not name as the next declaration's whole base
      // is not kept by the served copy, so nothing is taken out.
      const moved = text.replace("orders is scoped", "orders is (scoped)");
      expect(keptBasesInText(moved, plan)).toHaveProperty("reason");
      expect(buildCopyText(moved, plan)).toHaveProperty("reason");
   });
});

describe("what the served copy could not keep refuses the source", () => {
   it(
      "refuses a base's term when the source renames and redefines the field it reads",
      async () => {
         const { c } = await compiled(`${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend {
  rename: tenant is org_id
  dimension: org_id is 1
}
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(
            "on a base whose fields this source renames, redefines or drops",
         );
      },
      { timeout: 30000 },
   );

   it(
      "refuses a base's term when the source drops a field of the base",
      async () => {
         const { c } = await compiled(`${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { except: s }
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain("renames, redefines or drops");
      },
      { timeout: 30000 },
   );

   it(
      "refuses a base's term when a join of the source reads that base",
      async () => {
         // The build copy takes the term out of the base for every reader in
         // the copy, so the join would read every caller's rows in the build.
         const { c } = await compiled(`${HEAD}
source: scoped is raw extend {
  where: org_id = $ORG_ID
  primary_key: id
}
#(materialize) storage="lake"
source: orders is scoped extend {
  join_one: twin is scoped on twin.id = id
  where: twin.amount > 0
}
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(
            "on a base a join of this source also reads",
         );
      },
      { timeout: 30000 },
   );

   it(
      "leaves a term that reads its given through a dimension to the eligibility gate, which refuses the build",
      async () => {
         const { pkg, c } = await compiled(`${HEAD}
source: scoped is raw extend {
  dimension: is_mine is org_id = $ORG_ID
  where: is_mine
}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
         const p = planOf(c, "orders");
         const outcome = await computePackageBuildPlan(pkg);
         expect(outcome.sourceEligibility?.eligible ?? []).not.toContain(
            p?.sourceID,
         );
         expect(outcome.plan?.refusedSources?.[p!.sourceID]?.reason).toBe(
            "given_in_persisted_query",
         );
      },
      { timeout: 30000 },
   );
});

describe("an extension table over a parent that left a base's dimension out", () => {
   it(
      "builds over the raw chain when it stores that dimension itself, and a sibling still chains",
      async () => {
         // `parent` leaves `base_hour` (declared on `base`) out and computes it
         // at read in base's kept block; `child` renames a base field, so it
         // cannot keep base's block and stores `base_hour` as built. The
         // parent's table does not hold it, and its re-root on its build
         // source carries no definition of it, so `child` builds over the
         // raw chain.
         const { c } = await compiled(`${HEAD}
source: timed is duckdb.sql("select 1 as id, 'a' as s, timestamp '2026-01-01 03:00:00' as ts")
source: base is timed extend { dimension: base_hour is hour(ts) }
#(materialize) storage="lake"
source: parent is base extend { dimension: tag is 'x' }
#(materialize) storage="lake"
source: child is parent extend { rename: label is s }
#(materialize) storage="lake"
source: sibling is parent extend { dimension: tag2 is 'y' }
`);
         const parent = planOf(c, "parent");
         expect(parent?.leftOut?.map((d) => d.dimension)).toEqual([
            "base_hour",
         ]);
         const child = planOf(c, "child");
         expect(child?.buildsOver).toBeUndefined();
         // A sibling that stores nothing the parent left out still builds
         // over the parent's table: the child's raw chain costs it nothing.
         expect(planOf(c, "sibling")?.buildsOver?.sourceName).toBe("parent");
      },
      { timeout: 30000 },
   );
});
