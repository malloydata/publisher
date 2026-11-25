// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One placement key for every table: a rollup's `name=` places and names it.
 * Its container (everything before the last dot) is the schema or dataset the
 * table is created in, and its leaf takes the place of the readable prefix
 * (`<leaf>__p<hash>`, where it would read `<pkg>__<file>__<base>_by_<grain>`),
 * as a `name=` does under `scope: "version"`; the hash, and so uniqueness,
 * stays Publisher's. On `#(rollup)` and `#@ preaggregate` alike.
 * `namespace=`, the older spelling of the container, is read for one more
 * release with a `deprecated-rollup-namespace` warning, in the same
 * container under the readable prefix. A
 * rollup with no placement of its own inherits its base's database `name=`
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
      .filter((f) => f.route === "rollup")
      .map((f) => [f.code, f.subject ?? ""] as [string, string])
      .sort();
}

const readable = (container: string, label: string) =>
   new RegExp(
      `^${container ? `${container.replace(".", "\\.")}\\.` : ""}pa__m__${label}__p[0-9a-f]{8}$`,
   );
/** A rollup's name under its `name=` leaf: `[container.]<leaf>__p<hash>`. */
const prefixed = (container: string, leaf: string) =>
   new RegExp(
      `^${container ? `${container.replace(".", "\\.")}\\.` : ""}${leaf}__p[0-9a-f]{8}$`,
   );

describe("a rollup's name= places its table", () => {
   for (const [spelling, line] of [
      ["#(rollup)", '#(rollup) grain=["category"] name="analytics.anything"'],
      [
         "#@ preaggregate",
         '#@ preaggregate grain="category" name="analytics.anything"',
      ],
   ] as const) {
      it(`places a ${spelling} table in name='s container, its leaf the readable prefix`, async () => {
         const { compiled, packagePath } = await compile(
            `source: orders is ${ROWS} extend {
  ${line}
  measure: total is amount.sum()
}
`,
         );
         const tables = tablesOf(compiled, packagePath);
         expect(tables.map((t) => t.table)).toEqual([
            expect.stringMatching(prefixed("analytics", "anything")),
         ]);
         expect(
            findingsOf(compiled).filter(
               ([c]) => c !== "deprecated-preaggregate-annotation",
            ),
         ).toEqual([]);
      });
   }

   it("reads namespace= as name='s container, with a deprecation warning, at the same address", async () => {
      const as = async (placement: string, v1: boolean) =>
         compile(
            `source: orders is ${ROWS} extend {
  ${v1 ? `#@ preaggregate grain="category" ${placement}` : `#(rollup) grain=["category"] ${placement}`}
  measure: total is amount.sum()
}
`,
         );
      for (const v1 of [false, true]) {
         const named = await as('name="analytics.t"', v1);
         const spaced = await as('namespace="analytics"', v1);
         // Each alone keeps the plain address (#1794, option A) and the
         // container; the name= leaf renames it. Beside each other in one
         // plan they are two tables (below).
         const [n] = tablesOf(named.compiled, named.packagePath);
         const [d] = tablesOf(spaced.compiled, spaced.packagePath);
         expect(d?.address).toBe(n?.address);
         expect(n?.table).toMatch(prefixed("analytics", "t"));
         expect(d?.table).toMatch(readable("analytics", "orders_by_category"));
         expect(
            findingsOf(spaced.compiled).filter(
               ([c]) => c === "deprecated-rollup-namespace",
            ),
         ).toEqual([["deprecated-rollup-namespace", "orders.total"]]);
         const warning = (spaced.compiled.annotationFindings ?? []).find(
            (f) => f.code === "deprecated-rollup-namespace",
         );
         expect(warning?.tier).toBe("deprecated");
         expect(warning?.message).toContain('name="analytics.<table>"');
         expect(
            findingsOf(named.compiled).filter(
               ([c]) => c === "deprecated-rollup-namespace",
            ),
         ).toEqual([]);
      }
   });

   for (const [label, placement] of [
      ["a name= with an empty leaf", 'name="analytics."'],
      ["a leaf with a space", 'name="analytics.Orders Rollup"'],
      ["a leaf starting with a digit", 'name="1st_rollup"'],
      ["a leaf of punctuation", 'name="a.--"'],
      ["a leaf with a double underscore", 'name="a.orders__rollup"'],
      ["a name= whose container needs quotes", 'name="my schema.t"'],
      ["a name= ending as Publisher's own names do", 'name="a.t__p0123abcd"'],
      [
         "two leaves at one grain",
         'name="a.t"\n  #(rollup) grain=["category"] name="a.u"',
      ],
      ["both name= and namespace=", 'name="a.t" namespace="a"'],
      ["both name= and storage=", 'name="a.t" storage="lake"'],
   ] as const) {
      it(`fails the load for ${label}`, async () => {
         const { compiled } = await compile(
            `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"] ${placement}
  measure: total is amount.sum()
}
`,
         );
         const invalid = (compiled.annotationFindings ?? []).filter(
            (f) => f.code === "rollup-invalid",
         );
         expect(invalid.map((f) => f.tier)).toEqual(["malformed"]);
      });
   }
});

describe("a rollup's name= leaf with no container", () => {
   it("is lowercased in the table's name, and otherwise written as it reads", async () => {
      const { compiled, packagePath } = await compile(
         `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"] name="OrdersRollup"
  measure: total is amount.sum()
}
`,
      );
      expect(tablesOf(compiled, packagePath).map((t) => t.table)).toEqual([
         expect.stringMatching(prefixed("", "ordersrollup")),
      ]);
   });

   it("names its table in the connection's default schema", async () => {
      const { compiled, packagePath } = await compile(
         `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"] name="orders_rollup"
  measure: total is amount.sum()
}
`,
      );
      expect(tablesOf(compiled, packagePath).map((t) => t.table)).toEqual([
         expect.stringMatching(prefixed("", "orders_rollup")),
      ]);
   });
});

describe("a rollup with no placement of its own takes its base's name= container", () => {
   for (const [label, declaration] of [
      [
         "#@ persist",
         '##! experimental.persistence\n#@ persist name="analytics.orders_t"',
      ],
      ["#(materialize)", '#(materialize) name="analytics.orders_t"'],
   ] as const) {
      it(`from a database ${label} base, on both spellings, into one table`, async () => {
         const { compiled, packagePath } = await compile(
            `${declaration}
source: orders is ${ROWS} -> { select: * } extend {
  #(rollup) grain=["category"]
  #@ preaggregate grain="category"
  measure: total is amount.sum()
  #(rollup) grain=["region"] name="other.t"
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
            expect.stringMatching(prefixed("other", "t")),
         ]);
      });
   }

   it("refuses a database rollup over a storage= base, which takes nothing from its name=", async () => {
      const { compiled, packagePath } = await compile(
         `#(materialize) name="analytics.orders_t" storage="lake"
source: orders is ${ROWS} extend {
  #(rollup) grain=["category"]
  measure: total is amount.sum()
}
`,
      );
      const rollups = tablesOf(compiled, packagePath)
         .map((t) => t.table)
         .filter((t) => t.includes("_by_"));
      // Refused at plan: its base's table lives in a storage destination a
      // build in the warehouse cannot read.
      expect(rollups).toEqual([]);
      expect(
         (compiled.annotationFindings ?? []).map((f) => f.refused?.reason),
      ).toContain("upstream_table_unusable");
   });
});

describe("a rollup's name= never takes another table (#1794)", () => {
   it("keeps a rollup off its base's table when both name= say the same", async () => {
      const { compiled, packagePath } = await compile(
         `#(materialize) name="analytics.orders_t"
source: orders is ${ROWS} extend {
  #(rollup) grain=["category"] name="analytics.orders_t"
  measure: total is amount.sum()
}
`,
      );
      const entries = tablesOf(compiled, packagePath);
      expect(entries.map((t) => t.table).sort()).toEqual([
         "analytics.orders_t",
         expect.stringMatching(prefixed("analytics", "orders_t")),
      ]);
      expect(new Set(entries.map((t) => t.address)).size).toBe(2);
      expect(
         (compiled.annotationFindings ?? []).filter(
            (f) => f.code === "materialization-table-collision",
         ),
      ).toEqual([]);
   });

   it("gives identical rollups named by different leaves two tables at two addresses, whatever the file order", async () => {
      const model = (
         first: string,
         second: string,
      ) => `source: a is ${ROWS} extend {
  #(rollup) grain=["category"] ${first}
  measure: total is amount.sum()
}
source: b is ${ROWS} extend {
  #(rollup) grain=["category"] ${second}
  measure: total is amount.sum()
}
`;
      for (const [first, second] of [
         ['name="x"', 'name="y"'],
         ['name="y"', 'name="x"'],
      ] as const) {
         const { compiled, packagePath } = await compile(model(first, second));
         const entries = tablesOf(compiled, packagePath);
         expect(entries.map((t) => t.table).sort()).toEqual([
            expect.stringMatching(prefixed("", "x")),
            expect.stringMatching(prefixed("", "y")),
         ]);
         expect(new Set(entries.map((t) => t.address)).size).toBe(2);
      }
      // A leaf beside none (the readable name): two tables too.
      const mixed = await compile(model("", 'name="x"'));
      expect(
         new Set(
            tablesOf(mixed.compiled, mixed.packagePath).map((t) => t.address),
         ).size,
      ).toBe(2);
      // The same leaf: one table.
      const same = await compile(model('name="x"', 'name="x"'));
      expect(tablesOf(same.compiled, same.packagePath)).toHaveLength(1);
   });

   it("gives one grain placed in two containers two tables at two addresses", async () => {
      const { compiled, packagePath } = await compile(
         `source: orders is ${ROWS} extend {
  #(rollup) grain=["category"] name="a.t"
  #(rollup) grain=["category"] name="b.t"
  measure: total is amount.sum()
}
`,
      );
      const entries = tablesOf(compiled, packagePath);
      expect(entries.map((t) => t.table).sort()).toEqual([
         expect.stringMatching(prefixed("a", "t")),
         expect.stringMatching(prefixed("b", "t")),
      ]);
      expect(new Set(entries.map((t) => t.address)).size).toBe(2);
   });
});
