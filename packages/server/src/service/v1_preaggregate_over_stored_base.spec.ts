// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A v1 `#@ preaggregate` over a `#@ persist storage=` base is planned as the
 * `#(preaggregate)` it reads as, by the rollup leg, over the author's rows: the
 * same build source, address, grain, measures and placement as the
 * `#(preaggregate)` spelling, and the same refusals for the same reasons. Its
 * table takes the rollup's readable name, `<pkg>__<file>__<base>_by_<grain>`,
 * never the base's, and nothing is left out of the plan for its base being
 * stored. (Answers equal to live: r4/v1_preaggregate_over_stored_base.)
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { installSettings } from "../test_helpers/settings_env";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import {
   deriveBuildPlanOutcome,
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
 * Compile `text` on this thread, with `#(preaggregate)` read (v2) or not (the
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
   ReturnType<typeof deriveBuildPlanOutcome>["plan"]
>["sources"][string];

interface PlannedRollup {
   sourceEntityId: PlannedSource["sourceEntityId"];
   sql: PlannedSource["sql"];
   annotationFields: PlannedSource["annotationFields"];
   preaggregate: PlannedSource["preaggregate"];
}

function rollupsOf(compiled: FinishedBuildPlan): Record<string, PlannedRollup> {
   const { plan } = deriveBuildPlanOutcome(compiled, null);
   const out: Record<string, PlannedRollup> = {};
   for (const s of Object.values(plan?.sources ?? {})) {
      if (s.origin !== "preaggregate") continue;
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
      .filter((f) => f.code.startsWith("preaggregate-"))
      .map((f) => f.refused?.reason ?? f.code)
      .sort();
}

/** `template` with each `ROLLUP(grain | placement)` written in one spelling. */
function spelled(template: string, v2: boolean): string {
   return template.replace(/ROLLUP\(([^)]*)\)/g, (_, args: string) => {
      const [grain, place] = args.split("|").map((a) => a.trim());
      const placement = place ? ` ${place}` : "";
      return v2
         ? `#(preaggregate) grain=["${grain}"]${placement}`
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
   for (const [shape, template] of [
      ["a query-shaped base", QUERY_SHAPED],
      ["a table extended with measures", TABLE_EXTENDED],
   ] as const) {
      it(`plans exactly the #(preaggregate) it reads as, on ${shape}`, async () => {
         const v1 = await compile(spelled(template, false), false);
         const v1Rollups = rollupsOf(v1.compiled);
         const v2 = await compile(spelled(template, true), true);
         const v2Rollups = rollupsOf(v2.compiled);
         expect(Object.keys(v1Rollups).sort()).toEqual([
            expect.stringMatching(/^daily__rollup__category__[0-9a-f]{8}$/),
            expect.stringMatching(/^daily__rollup__region__[0-9a-f]{8}$/),
         ]);
         // The same build source, at the same content address.
         expect(v1Rollups).toEqual(v2Rollups);
         // Nothing is refused or left out for the base being stored.
         expect(codesOf(v1.compiled)).toEqual([]);
      });
   }

   it("plans the storage= grain into the destination and the other beside the base", async () => {
      const { compiled } = await compile(spelled(QUERY_SHAPED, false), false);
      const placed = Object.fromEntries(
         Object.entries(rollupsOf(compiled)).map(([name, r]) => [
            name.replace(/__[0-9a-f]{8}$/, ""),
            [r.annotationFields?.storage, r.preaggregate?.measures],
         ]),
      );
      expect(placed).toEqual({
         daily__rollup__category: ["lake", ["order_count", "total"]],
         daily__rollup__region: [undefined, ["total"]],
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
      expect(tables).toEqual([
         expect.stringMatching(/^pa__m__daily__p[0-9a-f]{8}$/),
         expect.stringMatching(/^pa__m__daily_by_category__p[0-9a-f]{8}$/),
         expect.stringMatching(/^pa__m__daily_by_region__p[0-9a-f]{8}$/),
      ]);
      expect(new Set(instructions.map((i) => i.sourceEntityId)).size).toBe(3);
   });

   it("gives a colocated grain with no namespace= no container from a storage= base's name=", async () => {
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
         expect.stringMatching(/^pa__m__daily_by_region__p[0-9a-f]{8}$/),
      ]);
   });

   it("plans the v1 spelling the same with PUBLISHER_MATERIALIZE on", async () => {
      const off = await compile(spelled(QUERY_SHAPED, false), false);
      const on = await compile(spelled(QUERY_SHAPED, false), true);
      expect(rollupsOf(on.compiled)).toEqual(rollupsOf(off.compiled));
      expect(Object.keys(rollupsOf(on.compiled))).toHaveLength(2);
      // Only the deprecation of the spelling is added.
      expect(
         (on.compiled.annotationFindings ?? [])
            .map((f) => f.code)
            .filter((c) => c !== "deprecated-preaggregate-annotation")
            .filter((c) => c.startsWith("preaggregate-")),
      ).toEqual([]);
   });

   it("takes no prefix and no container from the base's name=, and its own namespace= as a container", async () => {
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
         expect.stringMatching(
            /^analytics\.pa__m__daily_by_region__p[0-9a-f]{8}$/,
         ),
         // The base's own name=, as the author wrote it.
         "lake_daily",
         expect.stringMatching(/^pa__m__daily_by_category__p[0-9a-f]{8}$/),
      ]);
   });

   describe("is refused as the #(preaggregate) would be", () => {
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
            ["given_in_persisted_query"],
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
