// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One placement key for every table: a rollup's `name=` places it. Its
 * container (everything before the last dot) is the schema or dataset the
 * table is created in, and its leaf stays Publisher's readable name
 * (`<pkg>__<file>__<base>_by_<grain>__p<hash>`), on `#(preaggregate)` and
 * `#@ preaggregate` alike. `namespace=`, the older spelling of the container,
 * is read for one more release with a `deprecated-preaggregate-namespace`
 * warning, and places the table exactly as the `name=` that replaces it. A
 * rollup with no placement of its own inherits its base's colocated `name=`
 * container, from `#@ persist` or `#(materialize)`, so the two spellings land
 * in one schema. A rollup's table never takes its base's table, whatever the
 * two `name=`s say (#1794's distinct-name rules).
 */

import { afterEach, describe, expect, it } from "bun:test";
import { installSettings } from "../test_helpers/settings_env";
import { tempBuildPlanPackages } from "../test_helpers/incremental_test_harness";
import { compilePackageBuildPlan } from "./materialization/plan/build_plan_compile";
import type { FinishedBuildPlan } from "./materialization/plan/build_plan";
import { deriveSelfInstructions } from "./materialization/run/run_instructions";

const packages = tempBuildPlanPackages();
let restore: (() => void) | undefined;
afterEach(async () => {
   restore?.();
   restore = undefined;
   await packages.cleanup();
});

const ROWS = `duckdb.sql("SELECT 10 AS amount, 'A' AS category, 'east' AS region")`;

async function compile(
   text: string,
): Promise<{ compiled: FinishedBuildPlan; packagePath: string }> {
   restore?.();
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
   const pkg = await packages.create({ "m.malloy": text });
   return {
      compiled: await compilePackageBuildPlan(pkg),
      packagePath: pkg.getPackagePath(),
   };
}

/** Each table a self-instructed run writes, with its address. */
function tablesOf(
   compiled: FinishedBuildPlan,
   packagePath: string,
): { table: string; address: string }[] {
   const { instructions } = deriveSelfInstructions(
      compiled,
      undefined,
      {},
      undefined,
      { environmentName: "e1", packageName: "pa", packagePath },
   );
   return instructions
      .map((i) => ({ table: i.physicalTableName, address: i.sourceEntityId }))
      .sort((a, b) => a.table.localeCompare(b.table));
}

/** The rollup findings, as [code, subject]. */
function findingsOf(compiled: FinishedBuildPlan): [string, string][] {
   return (compiled.annotationFindings ?? [])
      .filter((f) => f.route === "preaggregate")
      .map((f) => [f.code, f.subject ?? ""] as [string, string])
      .sort();
}

const readable = (container: string, label: string) =>
   new RegExp(
      `^${container ? `${container.replace(".", "\\.")}\\.` : ""}pa__m__${label}__p[0-9a-f]{8}$`,
   );

describe("a rollup's name= places its table", () => {
   for (const [spelling, line] of [
      [
         "#(preaggregate)",
         '#(preaggregate) grain=["category"] name="analytics.anything"',
      ],
      [
         "#@ preaggregate",
         '#@ preaggregate grain="category" name="analytics.anything"',
      ],
   ] as const) {
      it(`places a ${spelling} table in name='s container, under Publisher's own leaf`, async () => {
         const { compiled, packagePath } = await compile(
            `source: orders is ${ROWS} extend {
  ${line}
  measure: total is amount.sum()
}
`,
         );
         const tables = tablesOf(compiled, packagePath);
         expect(tables.map((t) => t.table)).toEqual([
            expect.stringMatching(readable("analytics", "orders_by_category")),
         ]);
         expect(
            findingsOf(compiled).filter(
               ([c]) => c !== "deprecated-preaggregate-annotation",
            ),
         ).toEqual([]);
      });
   }

   it("reads namespace= as name='s container, with a deprecation warning, at the same table", async () => {
      const as = async (placement: string, v1: boolean) =>
         compile(
            `source: orders is ${ROWS} extend {
  ${v1 ? `#@ preaggregate grain="category" ${placement}` : `#(preaggregate) grain=["category"] ${placement}`}
  measure: total is amount.sum()
}
`,
         );
      for (const v1 of [false, true]) {
         const named = await as('name="analytics.t"', v1);
         const spaced = await as('namespace="analytics"', v1);
         expect(tablesOf(spaced.compiled, spaced.packagePath)).toEqual(
            tablesOf(named.compiled, named.packagePath),
         );
         expect(
            findingsOf(spaced.compiled).filter(
               ([c]) => c === "deprecated-preaggregate-namespace",
            ),
         ).toEqual([["deprecated-preaggregate-namespace", "orders.total"]]);
         const warning = (spaced.compiled.annotationFindings ?? []).find(
            (f) => f.code === "deprecated-preaggregate-namespace",
         );
         expect(warning?.tier).toBe("deprecated");
         expect(warning?.message).toContain('name="analytics.<table>"');
         expect(
            findingsOf(named.compiled).filter(
               ([c]) => c === "deprecated-preaggregate-namespace",
            ),
         ).toEqual([]);
      }
   });

   for (const [label, placement] of [
      ["a name= with no container", 'name="t"'],
      ["a name= whose container needs quotes", 'name="my schema.t"'],
      ["both name= and namespace=", 'name="a.t" namespace="a"'],
      ["both name= and storage=", 'name="a.t" storage="lake"'],
   ] as const) {
      it(`fails the load for ${label}`, async () => {
         const { compiled } = await compile(
            `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"] ${placement}
  measure: total is amount.sum()
}
`,
         );
         const invalid = (compiled.annotationFindings ?? []).filter(
            (f) => f.code === "preaggregate-invalid",
         );
         expect(invalid.map((f) => f.tier)).toEqual(["malformed"]);
      });
   }
});

describe("a rollup with no placement of its own takes its base's name= container", () => {
   for (const [label, declaration] of [
      [
         "#@ persist",
         '##! experimental.persistence\n#@ persist name="analytics.orders_t"',
      ],
      ["#(materialize)", '#(materialize) name="analytics.orders_t"'],
   ] as const) {
      it(`from a colocated ${label} base, on both spellings, into one table`, async () => {
         const { compiled, packagePath } = await compile(
            `${declaration}
source: orders is ${ROWS} -> { select: * } extend {
  #(preaggregate) grain=["category"]
  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #(preaggregate) grain=["region"] name="other.t"
  measure: n is count()
}
`,
         );
         const tables = tablesOf(compiled, packagePath)
            .map((t) => t.table)
            .sort();
         expect(tables).toEqual([
            "analytics.orders_t",
            expect.stringMatching(readable("analytics", "orders_by_category")),
            expect.stringMatching(readable("other", "orders_by_region")),
         ]);
      });
   }

   it("takes nothing from a storage= base's name=", async () => {
      const { compiled, packagePath } = await compile(
         `#(materialize) name="analytics.orders_t" storage="lake"
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"]
  measure: total is amount.sum()
}
`,
      );
      const rollups = tablesOf(compiled, packagePath)
         .map((t) => t.table)
         .filter((t) => t.includes("_by_"));
      expect(rollups).toEqual([
         expect.stringMatching(readable("", "orders_by_category")),
      ]);
   });
});

describe("a rollup's name= never takes another table (#1794)", () => {
   it("keeps a rollup off its base's table when both name= say the same", async () => {
      const { compiled, packagePath } = await compile(
         `#(materialize) name="analytics.orders_t"
source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"] name="analytics.orders_t"
  measure: total is amount.sum()
}
`,
      );
      const entries = tablesOf(compiled, packagePath);
      expect(entries.map((t) => t.table).sort()).toEqual([
         "analytics.orders_t",
         expect.stringMatching(readable("analytics", "orders_by_category")),
      ]);
      expect(new Set(entries.map((t) => t.address)).size).toBe(2);
      expect(
         (compiled.annotationFindings ?? []).filter(
            (f) => f.code === "materialization-table-collision",
         ),
      ).toEqual([]);
   });

   it("gives one grain placed in two containers two tables at two addresses", async () => {
      const { compiled, packagePath } = await compile(
         `source: orders is ${ROWS} extend {
  #(preaggregate) grain=["category"] name="a.t"
  #(preaggregate) grain=["category"] name="b.t"
  measure: total is amount.sum()
}
`,
      );
      const entries = tablesOf(compiled, packagePath);
      expect(entries.map((t) => t.table).sort()).toEqual([
         expect.stringMatching(readable("a", "orders_by_category")),
         expect.stringMatching(readable("b", "orders_by_category")),
      ]);
      expect(new Set(entries.map((t) => t.address)).size).toBe(2);
   });
});
