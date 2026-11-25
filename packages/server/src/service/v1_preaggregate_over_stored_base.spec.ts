// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A v1 `#@ preaggregate` over a `#@ persist storage=` base is planned as the
 * `#(rollup)` it reads as, by the rollup leg, over the author's rows: the
 * same build source, address, grain, measures and placement as the
 * `#(rollup)` spelling, and the same refusals for the same reasons. Its
 * table takes the rollup's readable name, `<pkg>__<file>__<base>_by_<grain>`,
 * never the base's. A grain in the warehouse (no `storage=` of its own) is
 * refused at plan as `upstream_table_unusable`, in both spellings: its base's
 * table lives in a storage destination a build in the warehouse cannot read,
 * and strict upstreams forbid recomputing the base from raw. (Answers equal
 * to live: r4/v1_preaggregate_over_stored_base.)
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { installSettings } from "../test_helpers/settings_env";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import {
   derivePackagePlan,
   type FinishedBuildPlan,
} from "./materialization/plan/build_plan";
import { deriveSelfInstructions } from "./materialization/run/run_instructions";

const packages = tempBuildPlanPackages();
let restore: (() => void) | undefined;
let csvDir: string;
let csvPath: string;
beforeAll(async () => {
   csvDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-v1-rollup-"));
   csvPath = path.join(csvDir, "t.csv");
   await fs.writeFile(
      csvPath,
      "amount,category,region\n10,A,east\n20,B,west\n",
   );
});
afterAll(async () => {
   await fs.rm(csvDir, { recursive: true, force: true });
});
afterEach(async () => {
   restore?.();
   restore = undefined;
   await packages.cleanup();
});

/**
 * Compile `text` on this thread, with `#(rollup)` read (v2) or not (the
 * default, where `#@ preaggregate` is the spelling that builds).
 */
async function compile(
   text: string,
   v2: boolean,
): Promise<{ compiled: FinishedBuildPlan; packagePath: string }> {
   restore?.();
   restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: v2 ? "true" : "false",
      },
   }).restore;
   // One file for both spellings: its path is in the build's SQL.
   const pkg = await packages.create({
      // Forward slashes: a backslash in a Malloy string is an escape.
      "m.malloy": text.replace(
         "'t.csv'",
         `'${csvPath.split(path.sep).join("/")}'`,
      ),
   });
   return {
      compiled: await compilePackageBuildPlan(pkg),
      packagePath: pkg.getPackagePath(),
   };
}

/** Each planned rollup, by name: what its build and its table are. */
type PlannedSource = NonNullable<
   ReturnType<typeof derivePackagePlan>["plan"]
>["sources"][string];

interface PlannedRollup {
   sourceEntityId: PlannedSource["sourceEntityId"];
   sql: PlannedSource["sql"];
   annotationFields: PlannedSource["annotationFields"];
   preaggregate: PlannedSource["preaggregate"];
}

function rollupsOf(compiled: FinishedBuildPlan): Record<string, PlannedRollup> {
   const { plan } = derivePackagePlan(compiled, null);
   const out: Record<string, PlannedRollup> = {};
   for (const s of Object.values(plan?.sources ?? {})) {
      if (s.origin !== "rollup") continue;
      out[s.name] = {
         sourceEntityId: s.sourceEntityId,
         sql: s.sql,
         annotationFields: s.annotationFields,
         preaggregate: s.preaggregate,
      };
   }
   return out;
}

/** Each rollup finding's refusal reason, or its code when it refuses nothing. */
function codesOf(compiled: FinishedBuildPlan): string[] {
   return (compiled.annotationFindings ?? [])
      .filter((f) => /^(preaggregate|rollup)-/.test(f.code))
      .map((f) => f.refused?.reason ?? f.code)
      .sort();
}

/** `template` with each `ROLLUP(grain | placement)` written in one spelling. */
function spelled(template: string, v2: boolean): string {
   return template.replace(/ROLLUP\(([^)]*)\)/g, (_, args: string) => {
      const [grain, place] = args.split("|").map((a) => a.trim());
      const placement = place ? ` ${place}` : "";
      return v2
         ? `#(rollup) grain=["${grain}"]${placement}`
         : `#@ preaggregate grain="${grain}"${placement}`;
   });
}

const QUERY_SHAPED = `##! experimental.persistence
#@ persist storage="lake"
source: daily is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A', 'east'), (20, 'B', 'west')) AS t(amount, category, region)
""") -> {
  group_by: category, region
  aggregate: amount_sum is amount.sum(), n is count()
} extend {
  ROLLUP(category | storage="lake")
  ROLLUP(region)
  measure: total is amount_sum.sum()
  ROLLUP(category | storage="lake")
  measure: order_count is n.sum()
}
`;

const TABLE_EXTENDED = `##! experimental.persistence
#@ persist storage="lake"
source: daily is duckdb.table('t.csv') extend {
  ROLLUP(category | storage="lake")
  ROLLUP(region | namespace="analytics")
  measure: total is amount.sum()
}
`;

describe("a v1 #@ preaggregate over a storage= base", () => {
   // A query-shaped base builds a stored table, so its warehouse grain is
   // refused; a table extended with measures builds none
   // (`persist-not-materializable`), so its warehouse grain reads no stored
   // upstream and builds as planned.
   for (const [shape, template, stored] of [
      ["a query-shaped base", QUERY_SHAPED, true],
      ["a table extended with measures", TABLE_EXTENDED, false],
   ] as const) {
      it(`plans exactly the #(rollup) it reads as, on ${shape}`, async () => {
         const v1 = await compile(spelled(template, false), false);
         const v1Rollups = rollupsOf(v1.compiled);
         const v2 = await compile(spelled(template, true), true);
         const v2Rollups = rollupsOf(v2.compiled);
         expect(Object.keys(v1Rollups).sort()).toEqual([
            expect.stringMatching(/^daily__rollup__category__[0-9a-f]{8}$/),
            ...(stored
               ? []
               : [
                    expect.stringMatching(
                       /^daily__rollup__region__[0-9a-f]{8}$/,
                    ),
                 ]),
         ]);
         // The same build source, at the same content address.
         expect(v1Rollups).toEqual(v2Rollups);
         // Over a stored base the warehouse grain is refused, in both
         // spellings, and nothing else is.
         const codes = stored ? ["upstream_table_unusable"] : [];
         expect(codesOf(v1.compiled)).toEqual(codes);
         expect(codesOf(v2.compiled)).toEqual(codes);
      });
   }

   it("plans the storage= grain into the destination, and refuses the one in the warehouse, naming the base's destination", async () => {
      const { compiled } = await compile(spelled(QUERY_SHAPED, false), false);
      const refusal = (compiled.annotationFindings ?? []).find(
         (f) => f.refused?.reason === "upstream_table_unusable",
      );
      expect(refusal?.message).toContain(
         "its upstream 'daily' is stored in the storage= destination 'lake'",
      );
      const placed = Object.fromEntries(
         Object.entries(rollupsOf(compiled)).map(([name, r]) => [
            name.replace(/__[0-9a-f]{8}$/, ""),
            [r.annotationFields?.storage, r.preaggregate?.measures],
         ]),
      );
      expect(placed).toEqual({
         daily__rollup__category: ["lake", ["order_count", "total"]],
      });
   });

   it("names each rollup's table <pkg>__<file>__<base>_by_<grain>, never the base's", async () => {
      const { compiled, packagePath } = await compile(
         spelled(QUERY_SHAPED, false),
         false,
      );
      const { instructions } = deriveSelfInstructions(
         compiled,
         undefined,
         {},
         undefined,
         { environmentName: "e1", packageName: "pa", packagePath },
      );
      const tables = instructions.map((i) => i.physicalTableName).sort();
      // The warehouse grain is refused, so it has no table.
      expect(tables).toEqual([
         expect.stringMatching(/^pa__m__daily__p[0-9a-f]{8}$/),
         expect.stringMatching(/^pa__m__daily_by_category__p[0-9a-f]{8}$/),
      ]);
      expect(new Set(instructions.map((i) => i.sourceEntityId)).size).toBe(2);
   });

   it("refuses a database grain over a storage= base's name=, so it takes no container from it", async () => {
      const { compiled, packagePath } = await compile(
         `##! experimental.persistence
#@ persist storage="lake" name="analytics.lake_daily"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'east' AS region") -> {
  group_by: category, region
  aggregate: amount_sum is amount.sum()
} extend {
  #@ preaggregate grain="region"
  measure: total is amount_sum.sum()
}
`,
         false,
      );
      const { instructions } = deriveSelfInstructions(
         compiled,
         undefined,
         {},
         undefined,
         { environmentName: "e1", packageName: "pa", packagePath },
      );
      expect(instructions.map((i) => i.physicalTableName).sort()).toEqual([
         "analytics.lake_daily",
      ]);
      expect(codesOf(compiled)).toEqual(["upstream_table_unusable"]);
   });

   it("plans the v1 spelling the same with PUBLISHER_MATERIALIZE on", async () => {
      const off = await compile(spelled(QUERY_SHAPED, false), false);
      const on = await compile(spelled(QUERY_SHAPED, false), true);
      expect(rollupsOf(on.compiled)).toEqual(rollupsOf(off.compiled));
      expect(Object.keys(rollupsOf(on.compiled))).toHaveLength(1);
      // The same refusal of the warehouse grain either way.
      expect(codesOf(on.compiled)).toEqual(codesOf(off.compiled));
      expect(codesOf(on.compiled)).toEqual(["upstream_table_unusable"]);
   });

   it("takes no prefix and no container from the base's name=, and refuses a grain in the warehouse whatever its namespace=", async () => {
      const { compiled, packagePath } = await compile(
         `##! experimental.persistence
#@ persist storage="lake" name="lake_daily"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'east' AS region") -> {
  group_by: category, region
  aggregate: amount_sum is amount.sum()
} extend {
  #@ preaggregate grain="category" storage="lake"
  #@ preaggregate grain="region" namespace="analytics"
  measure: total is amount_sum.sum()
}
`,
         false,
      );
      const { instructions } = deriveSelfInstructions(
         compiled,
         undefined,
         {},
         undefined,
         { environmentName: "e1", packageName: "pa", packagePath },
      );
      expect(instructions.map((i) => i.physicalTableName).sort()).toEqual([
         // The base's own name=, as the author wrote it.
         "lake_daily",
         expect.stringMatching(/^pa__m__daily_by_category__p[0-9a-f]{8}$/),
      ]);
      expect(codesOf(compiled)).toEqual(["upstream_table_unusable"]);
   });

   describe("is refused as the #(rollup) would be", () => {
      const cases: [string, string, string[]][] = [
         [
            "a base whose rows read the current time",
            `##! experimental.persistence
#@ persist storage="lake"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category, TIMESTAMP '2026-01-01' AS ts") -> {
  where: ts < now
  group_by: category
  aggregate: amount_sum is amount.sum()
} extend {
  ROLLUP(category | storage="lake")
  measure: total is amount_sum.sum()
}
`,
            ["current_time"],
         ],
         [
            "a gated base",
            `##! experimental { persistence givens }
given: REGION :: string is 'east'
#(access_filter) region = $REGION
#@ persist storage="lake"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'east' AS region") -> {
  group_by: category, region
  aggregate: amount_sum is amount.sum()
} extend {
  ROLLUP(category | storage="lake")
  measure: total is amount_sum.sum()
}
`,
            ["access_filter"],
         ],
         [
            "a base whose rows read a given",
            `##! experimental { persistence givens }
given: REGION :: string is 'east'
#@ persist storage="lake"
source: daily is duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'east' AS region") -> {
  where: region = $REGION
  group_by: category
  aggregate: amount_sum is amount.sum()
} extend {
  ROLLUP(category | storage="lake")
  measure: total is amount_sum.sum()
}
`,
            ["reads_given"],
         ],
      ];
      for (const [label, template, codes] of cases) {
         it(label, async () => {
            const v1 = await compile(spelled(template, false), false);
            const v2 = await compile(spelled(template, true), true);
            expect(rollupsOf(v1.compiled)).toEqual({});
            expect(rollupsOf(v2.compiled)).toEqual({});
            expect(codesOf(v1.compiled)).toEqual(codes);
            expect(codesOf(v2.compiled)).toEqual(codes);
         });
      }
   });
});
