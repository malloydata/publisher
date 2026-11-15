// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import {
   packageTableSuffix,
   versionTableSuffix,
} from "../../versions/materialization_scope";
import type { CompiledBuildPlan } from "../plan/build_plan";
import type { ManifestEntry } from "../domain/types";
import {
   deriveSelfInstructions,
   type SelfTableNaming,
} from "./run_instructions";

/**
 * Auto-run's table names, which must never let two packages, or two
 * environments, on one schema build one table: every name the author did not
 * choose is Publisher's readable one, `<pkg>__<file>__<source>[__v<ver>]__p<8
 * hex>` (domain/table_name.ts), an explicit `name=` outside `scope: version`
 * keeps its name and is checked for ownership at build
 * (table_ownership.spec.ts), and a table an earlier run built under an older
 * name is rebuilt, never carried.
 */

const PKG: SelfTableNaming = { environmentName: "e1", packageName: "pa" };
const OTHER: SelfTableNaming = { environmentName: "e2", packageName: "pa" };
const V120: SelfTableNaming = { ...PKG, ownedVersion: "1.2.0" };

/** The suffixes Publisher gave the same tables before readable names. */
const LEGACY_PKG = packageTableSuffix({
   environmentName: "e1",
   packageName: "pa",
});
const LEGACY_V120 = versionTableSuffix({
   environmentName: "e1",
   packageName: "pa",
   version: "1.2.0",
});

const READABLE = /__p[0-9a-f]{8}$/;

const compiled = (): CompiledBuildPlan => ({
   ...compiledWith(
      {
         summary: fakeSource({ name: "summary", sourceEntityId: "a1" }),
         named: fakeSource({
            name: "named",
            sourceEntityId: "a2",
            annotationFields: { name: "named_t" },
         }),
         qualified: fakeSource({
            name: "qualified",
            sourceEntityId: "a3",
            annotationFields: { name: "analytics.q_t" },
         }),
         rollup: fakeSource({
            name: "orders__preagg__category__1a2b3c4d",
            sourceEntityId: "a4",
            annotationFields: {
               name: "ns.orders__preagg__category__1a2b3c4d",
            },
         }),
      },
      [["summary", "named", "qualified", "rollup"]],
   ),
   sourceModelPaths: {
      summary: "models/sales.malloy",
      named: "models/sales.malloy",
      qualified: "models/sales.malloy",
      orders__preagg__category__1a2b3c4d: "models/sales.malloy",
   },
});

const withRollup = (c: CompiledBuildPlan): CompiledBuildPlan => ({
   ...c,
   preaggregatePlans: {
      [c.sources.rollup.sourceID]: {
         baseSourceName: "orders",
         grainDimensions: ["category"],
      } as never,
   },
});

const names = (r: ReturnType<typeof deriveSelfInstructions>) =>
   Object.fromEntries(
      r.instructions.map((i) => [i.sourceEntityId, i.physicalTableName]),
   );

const prior = (table: string, sourceEntityId: string): ManifestEntry =>
   ({
      sourceEntityId,
      physicalTableName: table,
      connectionName: "duckdb",
   }) as ManifestEntry;

describe("auto-run's table names", () => {
   it("give every name the author did not choose its readable name", () => {
      const r = deriveSelfInstructions(
         withRollup(compiled()),
         undefined,
         {},
         undefined,
         PKG,
      );
      expect(names(r)).toEqual({
         a1: expect.stringMatching(/^pa__sales__summary__p[0-9a-f]{8}$/),
         a2: "named_t",
         a3: "analytics.q_t",
         // A rollup's namespace is a container; its table reads base_by_grain.
         a4: expect.stringMatching(
            /^ns\.pa__sales__orders_by_category__p[0-9a-f]{8}$/,
         ),
      });
      // Another environment's package of the same name builds other tables.
      const other = names(
         deriveSelfInstructions(
            withRollup(compiled()),
            undefined,
            {},
            undefined,
            OTHER,
         ),
      );
      expect(other.a1).toMatch(/^pa__sales__summary__p[0-9a-f]{8}$/);
      expect(other.a1).not.toBe(names(r).a1);
      expect(other.a2).toBe("named_t");
   });

   it("under scope: version, show the version and keep only a name='s container", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {},
         undefined,
         V120,
      );
      expect(names(r).a1).toMatch(/^pa__sales__summary__v1_2_0__p[0-9a-f]{8}$/);
      expect(names(r).a2).toMatch(/^pa__sales__named__v1_2_0__p[0-9a-f]{8}$/);
      expect(names(r).a3).toMatch(
         /^analytics\.pa__sales__qualified__v1_2_0__p[0-9a-f]{8}$/,
      );
   });

   it("fit the dialect's budget", () => {
      const c = compiledWith(
         {
            s: fakeSource({
               name: "x".repeat(70),
               sourceEntityId: "a1",
               dialectName: "postgres",
            }),
         },
         [["s"]],
      );
      const [instruction] = deriveSelfInstructions(
         c,
         undefined,
         {},
         undefined,
         PKG,
      ).instructions;
      expect(instruction.physicalTableName.length).toBeLessThanOrEqual(50);
      expect(instruction.physicalTableName).toMatch(READABLE);
   });

   it("name a table several sources share after the smallest model path, then source", () => {
      const shared = (order: string[]) =>
         names(
            deriveSelfInstructions(
               {
                  ...compiledWith(
                     {
                        zeta: fakeSource({
                           name: "zeta",
                           sourceEntityId: "a1",
                        }),
                        alpha: fakeSource({
                           name: "alpha",
                           sourceEntityId: "a1",
                        }),
                     },
                     [order],
                  ),
                  sourceModelPaths: { zeta: "a.malloy", alpha: "b.malloy" },
               },
               undefined,
               {},
               undefined,
               PKG,
            ),
         ).a1;
      expect(shared(["zeta", "alpha"])).toMatch(/^pa__a__zeta__p/);
      expect(shared(["alpha", "zeta"])).toBe(shared(["zeta", "alpha"]));
   });

   it("rebuild, never carry, a table built under an older name", () => {
      const now = names(
         deriveSelfInstructions(compiled(), undefined, {}, undefined, PKG),
      );
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            a1: prior("summary", "a1"),
            a2: prior("named_t", "a2"),
            a3: prior("analytics.q_t", "a3"),
         },
         undefined,
         PKG,
      );
      // The bare `summary` another package may have written since: rebuilt,
      // and never reclaimed (nothing proves it is this package's).
      expect(names(r).a1).toBe(now.a1);
      expect(r.carried.a1).toBeUndefined();
      expect(r.renamed).toEqual([]);
      // An author's name= is carried, but only while its mark says it is
      // this package's (the service asks, uncarryUnownedTables).
      expect(Object.keys(r.carried).sort()).toEqual(["a2", "a3"]);
      expect(Object.keys(r.recheck).sort()).toEqual(["a2", "a3"]);
      expect(r.recheck.a2.physicalTableName).toBe("named_t");
      // Its own readable table is carried as before, with nothing to recheck.
      const again = deriveSelfInstructions(
         compiled(),
         undefined,
         { a1: prior(now.a1, "a1") },
         undefined,
         PKG,
      );
      expect(again.carried.a1).toBeDefined();
      expect(again.recheck.a1).toBeUndefined();
   });

   it("leave names as written without naming (a test seam)", () => {
      const r = deriveSelfInstructions(compiled(), undefined, {});
      expect(names(r).a1).toBe("summary");
      expect(r.recheck).toEqual({});
   });
});

describe("a #(materialize) table's name", () => {
   // The generator writes the declared `name=` on the build source's persist
   // line, or the source's own name when there is none (`nameDefaulted`).
   const withMaterialize = (
      c: CompiledBuildPlan,
      defaulted: boolean,
   ): CompiledBuildPlan => ({
      ...c,
      materializePlans: {
         [c.sources.named.sourceID]: {
            persist: { name: "named_t" },
            ...(defaulted ? { nameDefaulted: true } : {}),
         } as never,
      },
   });

   it("keeps a declared name= as written, and names a defaulted one readably", () => {
      const named = deriveSelfInstructions(
         withMaterialize(compiled(), false),
         undefined,
         {},
         undefined,
         PKG,
      );
      expect(names(named).a2).toBe("named_t");
      const defaulted = deriveSelfInstructions(
         withMaterialize(compiled(), true),
         undefined,
         {},
         undefined,
         PKG,
      );
      expect(names(defaulted).a2).toMatch(/^pa__sales__named__p[0-9a-f]{8}$/);
      // A scope: version run keeps name='s container, as for a v1 name=.
      const versioned = deriveSelfInstructions(
         withMaterialize(compiled(), false),
         undefined,
         {},
         undefined,
         V120,
      );
      expect(names(versioned).a2).toMatch(
         /^pa__sales__named__v1_2_0__p[0-9a-f]{8}$/,
      );
      expect(names(versioned).a3).toMatch(
         /^analytics\.pa__sales__qualified__v1_2_0__p/,
      );
   });
});

describe("a table whose name changed", () => {
   it("is rebuilt under its name now, never carried at the old one", () => {
      // The content address does not encode the name: a name= added,
      // removed or changed leaves the address as it was.
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            // name= removed: the table was at the author's name.
            a1: prior("summary_old", "a1"),
            // name= changed.
            a2: prior("named_old", "a2"),
            // name= added: the table was the package's default.
            a3: prior("pa__sales__qualified__p00000000", "a3"),
         },
         undefined,
         PKG,
      );
      expect(r.carried).toEqual({});
      expect(names(r)).toMatchObject({
         a1: expect.stringMatching(READABLE),
         a2: "named_t",
         a3: "analytics.q_t",
      });
      // The same name, spelled in another case on a case-folding schema, is
      // the same table, and carried.
      const again = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior("NAMED_T", "a2") },
         undefined,
         PKG,
      );
      expect(again.carried.a2).toBeDefined();
      // On a warehouse that keeps case (Postgres), a case-only rename is
      // another table: rebuilt, and checked again under its new name.
      const kept = deriveSelfInstructions(
         compiledWith(
            {
               named: fakeSource({
                  name: "named",
                  sourceEntityId: "a2",
                  annotationFields: { name: "named_t" },
                  dialectName: "postgres",
               }),
            },
            [["named"]],
         ),
         undefined,
         { a2: prior("NAMED_T", "a2") },
         undefined,
         PKG,
      );
      expect(kept.carried).toEqual({});
      expect(names(kept).a2).toBe("named_t");
      // A name= moved to another schema is another table.
      const moved = deriveSelfInstructions(
         compiled(),
         undefined,
         { a3: prior("staging.q_t", "a3") },
         undefined,
         PKG,
      );
      expect(moved.carried).toEqual({});
      expect(names(moved).a3).toBe("analytics.q_t");
   });
});

describe("a table named before readable names", () => {
   it("is rebuilt under its readable name and offered for reclaim", () => {
      const r = deriveSelfInstructions(
         withRollup(compiled()),
         undefined,
         {
            a1: prior(`summary${LEGACY_PKG}`, "a1"),
            a4: prior(
               `ns.orders__preagg__category__1a2b3c4d${LEGACY_PKG}`,
               "a4",
            ),
         },
         undefined,
         PKG,
      );
      expect(r.carried).toEqual({});
      expect(r.renamed).toEqual([
         {
            sourceEntityId: "a1",
            previousName: `summary${LEGACY_PKG}`,
            physicalTableName: names(r).a1,
            connectionName: "duckdb",
         },
         {
            sourceEntityId: "a4",
            previousName: `ns.orders__preagg__category__1a2b3c4d${LEGACY_PKG}`,
            physicalTableName: names(r).a4,
            connectionName: "duckdb",
         },
      ]);
   });

   it("under scope: version, a name= too, since every name took the version's suffix", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior(`named_t${LEGACY_V120}`, "a2") },
         undefined,
         V120,
      );
      expect(r.renamed.map((t) => t.previousName)).toEqual([
         `named_t${LEGACY_V120}`,
      ]);
   });

   it("is not offered unless it is exactly the name Publisher gave it, in the same place", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            // Another environment's package of the same name.
            a1: prior(
               `summary${packageTableSuffix({ environmentName: "e2", packageName: "pa" })}`,
               "a1",
            ),
            // In a storage destination, where this run builds it colocated.
            a3: {
               ...prior(`qualified${LEGACY_PKG}`, "a3"),
               storageDestinationName: "lake",
            } as ManifestEntry,
         },
         undefined,
         PKG,
      );
      expect(r.renamed).toEqual([]);
      // An author's name= outside scope: version had no suffix to re-derive.
      const named = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior(`named_t${LEGACY_PKG}`, "a2") },
         undefined,
         PKG,
      );
      expect(named.renamed).toEqual([]);
   });

   describe("an incremental source", () => {
      const DECLARATION = {
         refresh: "incremental",
         incremental: true,
         declaredWatermark: true,
         declaredMergeKey: false,
         watermark: {
            name: "order_date",
            kind: "dimension" as const,
            malloyType: "date",
         },
         watermarkOrderable: true,
         mergeKeys: [],
         watermarkInMergeKeys: false,
         strategy: "range_replace" as const,
         malformed: [],
         unknownKeys: [],
         calculateFields: [],
         outputColumns: ["order_date", "revenue"],
      };
      const context = (forceRefresh: boolean) =>
         ({
            environmentId: "env-1",
            packageName: "pa",
            materializationId: "run-1",
            forceRefresh,
            now: new Date("2024-07-01T00:00:00Z"),
            declarations: { s1: DECLARATION },
            ledger: {},
         }) as never;
      const c = compiledWith(
         {
            s1: fakeSource({
               name: "s1",
               sourceEntityId: "b1aaaaaaaaaaaaaa",
               connectionName: "wh",
               dialectName: "postgres",
               annotationFields: {
                  refresh: "incremental",
                  watermark: "order_date",
               },
            }),
         },
         [["s1"]],
      );
      const legacy = `s1${LEGACY_PKG}`;
      const priorEntries = {
         b1aaaaaaaaaaaaaa: {
            sourceEntityId: "b1aaaaaaaaaaaaaa",
            physicalTableName: legacy,
            connectionName: "wh",
         } as ManifestEntry,
      };

      it("keeps its old name until it is reseeded, so it is not re-seeded for a rename", () => {
         const kept = deriveSelfInstructions(
            c,
            undefined,
            priorEntries,
            context(false),
            PKG,
         );
         expect(kept.instructions[0].physicalTableName).toBe(legacy);
         expect(kept.renamed).toEqual([]);
      });

      it("moves to its readable name when reseeded, and its old table is offered", () => {
         const moved = deriveSelfInstructions(
            c,
            undefined,
            priorEntries,
            context(true),
            PKG,
         );
         expect(moved.instructions[0].physicalTableName).toMatch(READABLE);
         expect(moved.renamed.map((t) => t.previousName)).toEqual([legacy]);
      });
   });
});

describe("two sources whose readable names meet", () => {
   it("builds the first, and never the second over it", () => {
      // A source named like a persisted view's build source, beside that
      // view, in one file: one identity, so one name. The package's own
      // collision check refuses this before a run; the run never writes one
      // table for two sources even so.
      const c: CompiledBuildPlan = {
         ...compiledWith(
            {
               first: fakeSource({ name: "orders", sourceEntityId: "a1" }),
               second: fakeSource({ name: "orders", sourceEntityId: "a2" }),
            },
            [["first", "second"]],
         ),
         sourceModelPaths: { orders: "m.malloy" },
      };
      const r = deriveSelfInstructions(c, undefined, {}, undefined, PKG);
      expect(r.instructions.map((i) => i.sourceEntityId)).toEqual(["a1"]);
      expect(r.carried).toEqual({});
   });
});
