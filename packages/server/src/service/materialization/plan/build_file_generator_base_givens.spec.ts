// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   materializePlansIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { addressSQL } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { buildCopyText } from "./materialize_build_copy";
import { type MaterializePlan } from "../domain/table_plans";

/**
 * A base's given-reading `where:` and given-reading dimensions on the
 * `#(materialize)` build side. Only a materialized source's own declaration
 * is left out of the build and re-applied at read: the served copy keeps that
 * declaration's trailing `extend` and no base's text. So a term the source
 * inherits from any base (one or several `extend` or rename hops up, or a
 * materialized parent) refuses the source at plan, as does a given-reading
 * dimension a base declares; the source is not built and answers live. A
 * zone-dependent dimension a base declares is stored as built.
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

/** What every refusal of an inherited given-reading `where:` says to do. */
const MOVE_WHERE =
   "move the given-reading `where:` onto the materialized source itself";
/** And of a given-reading dimension a base declares. */
const MOVE_DIMENSION =
   "declare the given-reading dimension on the materialized source itself";

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
   return Object.values(materializePlansIn(c) ?? {}).find(
      (p) => p.sourceName === name,
   );
}

function notBuilt(c: Compiled, source: string): string | undefined {
   return (c.annotationFindings ?? []).find(
      (f) => f.code === "materialize-not-built" && f.subject === source,
   )?.message;
}

function sourceNamed(c: Compiled, name: string) {
   return Object.values(sourcesIn(c)).find((s) => s.name === name);
}

const SCOPED = `${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
`;

describe("a given-reading where: inherited from a base refuses the source", () => {
   for (const [shape, text] of [
      [
         "one extend up",
         `${SCOPED}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`,
      ],
      [
         "several extend levels up",
         `${SCOPED}
source: mid is scoped extend { dimension: tag is 'x' }
source: upper_mid is mid extend { measure: total is amount.sum() }
#(materialize) storage="lake"
source: orders is upper_mid extend { measure: n is count() }
`,
      ],
      [
         "through a bare rename",
         `${SCOPED}
source: scoped2 is scoped
#(materialize) storage="lake"
source: orders is scoped2 extend { measure: n is count() }
`,
      ],
      [
         "with a term of the source's own beside it",
         `${HEAD.replace(
            "ORG_ID :: number is 1",
            "ORG_ID :: number is 1\n  MIN_AMT :: number is 0",
         )}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { where: amount >= $MIN_AMT }
`,
      ],
      [
         "colocated",
         `${SCOPED}
#(materialize) name="orders"
source: orders is scoped extend { measure: n is count() }
`,
      ],
      [
         "from a base in another file",
         {
            "base.malloy": SCOPED,
            "m.malloy": `##! experimental.givens
import "base.malloy"
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`,
         },
      ],
   ] as const) {
      it(
         `is not planned or built: ${shape}`,
         async () => {
            const { c } = await compiled(text);
            expect(planOf(c, "orders")).toBeUndefined();
            expect(sourceNamed(c, "orders")).toBeUndefined();
            expect(notBuilt(c, "orders")).toContain(MOVE_WHERE);
            expect(notBuilt(c, "orders")).toContain(
               "`where: org_id = $ORG_ID`",
            );
         },
         { timeout: 30000 },
      );
   }

   it(
      "refuses an extension of a materialized parent whose term it inherits, and plans the parent",
      async () => {
         const { c } = await compiled(`${HEAD}
#(materialize) storage="lake"
source: parent is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: child is parent extend { dimension: tag is 'x' }
`);
         expect(planOf(c, "child")).toBeUndefined();
         expect(notBuilt(c, "child")).toContain(MOVE_WHERE);
         const parent = planOf(c, "parent");
         expect(parent?.readTimeTerms?.map((t) => t.code)).toEqual([
            "org_id = $ORG_ID",
         ]);
      },
      { timeout: 30000 },
   );

   it(
      "still plans the term on the source itself, out of the build, whatever the given's default",
      async () => {
         const text = (org: number) => `${HEAD.replace(
            "ORG_ID :: number is 1",
            `ORG_ID :: number is ${org}`,
         )}
#(materialize) storage="lake"
source: orders is raw extend {
  where: org_id = $ORG_ID
  dimension: mine is org_id = $ORG_ID
  measure: n is count()
}
`;
         const sqls: string[] = [];
         for (const org of [1, 7]) {
            const { c } = await compiled(text(org));
            const p = planOf(c, "orders");
            expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
               "org_id = $ORG_ID",
            ]);
            expect(p?.leftOut?.map((d) => d.dimension)).toEqual(["mine"]);
            const sql = addressSQL(
               sourceNamed(c, "orders")!,
               targetsIn(c)?.[p!.sourceID],
            );
            expect(sql).not.toMatch(new RegExp(`=\\s*${org}\\b`));
            expect(sql).not.toContain('"mine"');
            sqls.push(sql);
         }
         expect(sqls[1]).toBe(sqls[0]);
      },
      { timeout: 30000 },
   );
});

describe("a caller-dependent dimension a base declares", () => {
   it(
      "refuses the source when it reads a given",
      async () => {
         const { c } = await compiled(`${HEAD}
source: scoped is raw extend { dimension: mine is org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(MOVE_DIMENSION);
      },
      { timeout: 30000 },
   );

   it(
      "refuses a term that reads its given through a base's dimension",
      async () => {
         const { c } = await compiled(`${HEAD}
source: scoped is raw extend {
  dimension: is_mine is org_id = $ORG_ID
  where: is_mine
}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
         expect(planOf(c, "orders")).toBeUndefined();
         expect(notBuilt(c, "orders")).toContain(MOVE_DIMENSION);
      },
      { timeout: 30000 },
   );

   it(
      "stores a zone-dependent one as built, so the table is not marked zone-free for it",
      async () => {
         const { c } = await compiled(`${HEAD}
source: timed is duckdb.sql("select 1 as id, timestamp '2026-01-01 03:00:00' as ts")
source: base is timed extend { dimension: ts_day is ts.day }
#(materialize) storage="lake"
source: orders is base extend { measure: n is count() }
`);
         const p = planOf(c, "orders");
         expect(p?.leftOut ?? []).toEqual([]);
         const day = p?.columns.find((col) => col.dimension === "ts_day");
         expect(day?.computed).toBe(true);
         expect(day?.zoneFree).toBeUndefined();
      },
      { timeout: 30000 },
   );
});

describe("the build copy takes out only a term the source's own declaration writes", () => {
   it("refuses a plan whose term lies in a base's declaration", () => {
      // Defense in depth: were the planner to place an inherited term on a
      // plan, the build copy would not find it in the source's declaration
      // and refuses, so the term is never written \`true\` in a base.
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
      expect(buildCopyText(text, plan)).toHaveProperty("reason");
   });
});
