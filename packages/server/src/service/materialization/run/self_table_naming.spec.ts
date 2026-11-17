// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import path from "path";
import { pathToFileURL } from "url";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
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
 * `c` with its tables keyed by each source's sourceID, as a plan keys them,
 * and each declaring model path `modelPaths` gives, by sourceID.
 */
function bySourceID(
   c: CompiledBuildPlan,
   modelPaths: Record<string, string> = {},
): CompiledBuildPlan {
   const rename = new Map<string, string>();
   const tables: CompiledBuildPlan["tables"] = {};
   for (const [key, table] of Object.entries(c.tables)) {
      const id = table.source.sourceID;
      rename.set(key, id);
      tables[id] = {
         ...table,
         ...(modelPaths[id] ? { modelPath: modelPaths[id] } : {}),
      };
   }
   return {
      ...c,
      tables,
      builds: c.builds.map((b) => ({
         ...b,
         order: b.order.map((key) => rename.get(key) ?? key),
      })),
   };
}

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

const compiled = (): CompiledBuildPlan =>
   bySourceID(
      compiledWith(
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
      {
         summary: "models/sales.malloy",
         named: "models/sales.malloy",
         qualified: "models/sales.malloy",
         orders__preagg__category__1a2b3c4d: "models/sales.malloy",
      },
   );

const ROLLUP = "orders__preagg__category__1a2b3c4d";
const withRollup = (
   c: CompiledBuildPlan,
   prefix?: string,
): CompiledBuildPlan => ({
   ...c,
   tables: {
      ...c.tables,
      [ROLLUP]: {
         ...c.tables[ROLLUP],
         kind: "rollup",
         rollup: {
            baseSourceName: "orders",
            grainDimensions: ["category"],
            namespace: "ns",
            ...(prefix !== undefined ? { prefix } : {}),
         } as never,
         materialize: undefined,
      },
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

   it("under scope: version, show the version, and let a name= set the prefix, schema included", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {},
         undefined,
         V120,
      );
      expect(names(r).a1).toMatch(/^pa__sales__summary__v1_2_0__p[0-9a-f]{8}$/);
      // name="named_t": the prefix, then the tail.
      expect(names(r).a2).toMatch(/^named_t__v1_2_0__p[0-9a-f]{8}$/);
      // name="analytics.q_t": the schema, the prefix, then the tail.
      expect(names(r).a3).toMatch(/^analytics\.q_t__v1_2_0__p[0-9a-f]{8}$/);
   });

   it("unversioned and under scope: package, a name= is the exact name, schema included", () => {
      // A scope: package version's run names its tables as an unversioned
      // package's does: no owned version.
      const r = names(
         deriveSelfInstructions(compiled(), undefined, {}, undefined, PKG),
      );
      expect(r.a2).toBe("named_t");
      expect(r.a3).toBe("analytics.q_t");
   });

   it("sanitize a name= prefix and cut it, never the tail, to fit the budget", () => {
      const c = compiledWith(
         {
            s: fakeSource({
               name: "s",
               sourceEntityId: "a1",
               dialectName: "postgres",
               annotationFields: { name: `analytics.Big-${"x".repeat(60)}` },
            }),
         },
         [["s"]],
      );
      const name = (n: SelfTableNaming) =>
         deriveSelfInstructions(c, undefined, {}, undefined, n).instructions[0]
            .physicalTableName;
      const versioned = name(V120);
      expect(versioned).toMatch(/^analytics\.big_x+__v1_2_0__p[0-9a-f]{8}$/);
      expect(versioned.slice("analytics.".length)).toHaveLength(50);
      // Deterministic.
      expect(name(V120)).toBe(versioned);
      // Unversioned, the name= stands as written.
      expect(name(PKG)).toBe(`analytics.Big-${"x".repeat(60)}`);
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
               bySourceID(
                  compiledWith(
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
                  { zeta: "a.malloy", alpha: "b.malloy" },
               ),
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
      tables: {
         ...c.tables,
         named: {
            ...c.tables.named,
            kind: "materialize",
            materialize: {
               persist: { name: "named_t" },
               ...(defaulted ? { nameDefaulted: true } : {}),
            } as never,
            rollup: undefined,
         },
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
      expect(names(versioned).a2).toMatch(/^named_t__v1_2_0__p[0-9a-f]{8}$/);
      expect(names(versioned).a3).toMatch(/^analytics\.q_t__v1_2_0__p/);
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
            sourceName: "summary",
            previousName: `summary${LEGACY_PKG}`,
            physicalTableName: names(r).a1,
            connectionName: "duckdb",
         },
         {
            sourceEntityId: "a4",
            sourceName: ROLLUP,
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

   it("is offered wherever this package's records put it on the source's connection: the reclaim's guards decide", () => {
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
      // Another environment's suffix reads as a readable name of this
      // package's, kept where it is; the table in another destination moves,
      // and the old one is offered from its own place.
      expect(r.carried.a1).toBeDefined();
      expect(r.renamed.map((t) => [t.previousName, t.previousPlace])).toEqual([
         [`qualified${LEGACY_PKG}`, { storageDestinationName: "lake" }],
      ]);
      // On another connection: another content address, never offered.
      const elsewhere = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            a1: {
               ...prior(`summary${LEGACY_PKG}`, "a1"),
               connectionName: "other",
            } as ManifestEntry,
         },
         undefined,
         PKG,
      );
      expect(elsewhere.renamed).toEqual([]);
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

      it("keeps its old name on a scheduled fire (forceRefresh), so its ledger is not lost", () => {
         const scheduled = deriveSelfInstructions(
            c,
            undefined,
            priorEntries,
            context(false),
            PKG,
            { forceRefresh: true },
         );
         expect(scheduled.instructions[0].physicalTableName).toBe(legacy);
         expect(scheduled.renamed).toEqual([]);
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
      // Two identities, as a plan has them (one key per sourceID).
      const first = Object.assign(
         fakeSource({ name: "orders", sourceEntityId: "a1" }),
         { sourceID: "orders@first" },
      );
      const second = Object.assign(
         fakeSource({ name: "orders", sourceEntityId: "a2" }),
         { sourceID: "orders@second" },
      );
      const c: CompiledBuildPlan = bySourceID(
         compiledWith({ first, second }, [["first", "second"]]),
         { "orders@first": "m.malloy", "orders@second": "m.malloy" },
      );
      const r = deriveSelfInstructions(c, undefined, {}, undefined, PKG);
      expect(r.instructions.map((i) => i.sourceEntityId)).toEqual(["a1"]);
      expect(r.carried).toEqual({});
      // Refused, with its own reason, so the run's record says why.
      expect(r.refused["orders@second"]).toMatchObject({
         reason: "table_name_collision",
         tier: "colocated",
         modelPath: "m.malloy",
      });
   });
});

/**
 * A package root that is absolute on every OS (on Windows, on the current
 * drive), and the file URL Publisher gives a model under it: pathToFileURL, as
 * model_runtime builds it. A hand-spelled `file:///pkg/…` has no drive, so
 * Windows cannot read it as a path at all.
 */
const PKG_ROOT = path.resolve("/pkg");
const fileURL = (rel: string) => pathToFileURL(path.join(PKG_ROOT, rel)).href;

describe("a source's file", () => {
   /** `name`, declared in package-relative `file`, under PKG_ROOT. */
   const declared = (name: string, file: string, sourceEntityId: string) =>
      Object.assign(fakeSource({ name, sourceEntityId }), {
         sourceID: `${name}@${fileURL(file)}`,
      });
   const naming: SelfTableNaming = { ...PKG, packagePath: PKG_ROOT };

   it("is the model that declares it, whatever model imports it or in what order", () => {
      const build = (lastImporter: string, order: string[]) => {
         const orders = declared("orders", "models/a.malloy", "a1");
         return deriveSelfInstructions(
            bySourceID(compiledWith({ orders }, [order]), {
               [orders.sourceID]: lastImporter,
            }),
            undefined,
            {},
            undefined,
            naming,
         ).instructions[0].physicalTableName;
      };
      const name = build("models/z_imports_a.malloy", ["orders"]);
      expect(name).toMatch(/^pa__a__orders__p[0-9a-f]{8}$/);
      expect(build("models/b_imports_a.malloy", ["orders"])).toBe(name);
      expect(build("models/a.malloy", ["orders"])).toBe(name);
   });

   it("reads a percent-encoded path, and a generated companion as its author's file", () => {
      // pathToFileURL percent-encodes the spaces: `my%20models/b%20c.malloy`.
      const spaced = declared("orders", "my models/b c.malloy", "a1");
      const view = declared(
         "orders__by_month__view",
         "m.malloy.persist-view.malloy",
         "a2",
      );
      const r = names(
         deriveSelfInstructions(
            compiledWith({ spaced, view }, [["spaced", "view"]]),
            undefined,
            {},
            undefined,
            naming,
         ),
      );
      expect(r.a1).toMatch(/^pa__b_c__orders__p[0-9a-f]{8}$/);
      expect(r.a2).toMatch(/^pa__m__orders_by_month__p[0-9a-f]{8}$/);
   });

   it("names a shared table by the declaring paths, not the importers'", () => {
      const zeta = declared("zeta", "a.malloy", "a1");
      const alpha = declared("alpha", "b.malloy", "a1");
      const r = deriveSelfInstructions(
         bySourceID(compiledWith({ zeta, alpha }, [["alpha", "zeta"]]), {
            [zeta.sourceID]: "z.malloy",
            [alpha.sourceID]: "a.malloy",
         }),
         undefined,
         {},
         undefined,
         naming,
      );
      expect(names(r).a1).toMatch(/^pa__a__zeta__p/);
   });
});

describe("a table built under a readable name", () => {
   it("keeps it when its file moves or a source with a smaller name comes to share it", () => {
      const now = names(
         deriveSelfInstructions(compiled(), undefined, {}, undefined, PKG),
      );
      // Built when the source lived in another file.
      const recorded = "pa__old_file__summary__p0123abcd";
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a1: prior(recorded, "a1") },
         undefined,
         PKG,
      );
      expect(r.carried.a1.physicalTableName).toBe(recorded);
      expect(now.a1).not.toBe(recorded);
   });

   it("takes the new name when its container or its name= prefix changes", () => {
      // A rollup's namespace moved.
      const moved = deriveSelfInstructions(
         withRollup(compiled()),
         undefined,
         { a4: prior("old_ns.pa__sales__orders_by_category__p0123abcd", "a4") },
         undefined,
         PKG,
      );
      expect(names(moved).a4).toMatch(/^ns\.pa__sales__orders_by_category__p/);
      // Under scope: version, a name= changed from `old_t` to `named_t`.
      const renamedPrefix = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior("old_t__v1_2_0__p0123abcd", "a2") },
         undefined,
         V120,
      );
      expect(names(renamedPrefix).a2).toMatch(/^named_t__v1_2_0__p/);
      // The same prefix, its file moved: kept.
      const kept = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior("named_t__v1_2_0__p0123abcd", "a2") },
         undefined,
         V120,
      );
      expect(kept.carried.a2.physicalTableName).toBe(
         "named_t__v1_2_0__p0123abcd",
      );
   });
});

describe("a rollup's name= leaf, the prefix of its readable name", () => {
   const run = (prefix: string | undefined, recorded: string) =>
      deriveSelfInstructions(
         withRollup(compiled(), prefix),
         undefined,
         { a4: prior(recorded, "a4") },
         undefined,
         PKG,
      );

   it("names the table after the leaf, its container kept", () => {
      const r = deriveSelfInstructions(
         withRollup(compiled(), "orders_rollup"),
         undefined,
         {},
         undefined,
         PKG,
      );
      expect(names(r).a4).toMatch(/^ns\.orders_rollup__p[0-9a-f]{8}$/);
   });

   it("keeps the table's name while the leaf is the same, its file moved", () => {
      const recorded = "ns.orders_rollup__p0123abcd";
      const r = run("orders_rollup", recorded);
      expect(r.carried.a4.physicalTableName).toBe(recorded);
      expect(r.renamed).toEqual([]);
   });

   for (const [label, prefix, recorded, now] of [
      [
         "the leaf changed",
         "orders_rollup",
         "ns.old_rollup__p0123abcd",
         /^ns\.orders_rollup__p/,
      ],
      [
         "the leaf removed",
         undefined,
         "ns.orders_rollup__p0123abcd",
         /^ns\.pa__sales__orders_by_category__p/,
      ],
      [
         "namespace= changed to name=",
         "orders_rollup",
         "ns.pa__sales__orders_by_category__p0123abcd",
         /^ns\.orders_rollup__p/,
      ],
   ] as const) {
      it(`renames the table, rebuilt once, and offers the old one for reclaim: ${label}`, () => {
         const r = run(prefix, recorded);
         expect(names(r).a4).toMatch(now);
         expect(r.carried.a4).toBeUndefined();
         expect(r.renamed.map((t) => t.previousName)).toEqual([recorded]);
      });
   }
});

describe("a forced refresh (every scheduled fire)", () => {
   it("carries nothing, and still names each table from the run before", () => {
      const now = names(
         deriveSelfInstructions(compiled(), undefined, {}, undefined, PKG),
      );
      const recorded = "pa__old_file__summary__p0123abcd";
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            a1: prior(recorded, "a1"),
            a3: prior(`qualified${LEGACY_PKG}`, "a3"),
         },
         undefined,
         PKG,
         { forceRefresh: true },
      );
      expect(r.carried).toEqual({});
      // Rebuilt in place under its recorded readable name.
      expect(names(r).a1).toBe(recorded);
      expect(now.a1).not.toBe(recorded);
   });

   it("offers a table named before readable names for reclaim", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a1: prior(`summary${LEGACY_PKG}`, "a1") },
         undefined,
         PKG,
         { forceRefresh: true },
      );
      expect(r.renamed.map((t) => t.previousName)).toEqual([
         `summary${LEGACY_PKG}`,
      ]);
   });
});

describe("a recorded readable name another source now takes fresh", () => {
   const legacy = Object.assign(
      fakeSource({ name: "orders_legacy", sourceEntityId: "aaa" }),
      { sourceID: `orders_legacy@${fileURL("m.malloy")}` },
   );
   const fresh = Object.assign(
      fakeSource({ name: "orders", sourceEntityId: "ccc" }),
      { sourceID: `orders@${fileURL("m.malloy")}` },
   );
   const naming: SelfTableNaming = { ...PKG, packagePath: PKG_ROOT };
   // The table `orders` built at address `aaa`, before it was renamed.
   const ordersTable = deriveSelfInstructions(
      compiledWith({ fresh }, [["fresh"]]),
      undefined,
      {},
      undefined,
      naming,
   ).instructions[0].physicalTableName;

   for (const order of [
      ["legacy", "fresh"],
      ["fresh", "legacy"],
   ]) {
      it(`goes to that source, and the renamed one moves (${order.join(" then ")})`, () => {
         // `orders` renamed to `orders_legacy` (same SQL, same address), and
         // a new `orders` beside it.
         const r = deriveSelfInstructions(
            compiledWith({ legacy, fresh }, [order]),
            undefined,
            { aaa: prior(ordersTable, "aaa") },
            undefined,
            naming,
         );
         expect(r.refused).toEqual({});
         expect(names(r)).toEqual({
            ccc: ordersTable,
            aaa: expect.stringMatching(/^pa__m__orders_legacy__p[0-9a-f]{8}$/),
         });
      });
   }
});

describe("removing a name= under scope: version", () => {
   it("changes the prefix, so the table takes its readable name", () => {
      const c = compiledWith(
         { named: fakeSource({ name: "named", sourceEntityId: "a2" }) },
         [["named"]],
      );
      const r = deriveSelfInstructions(
         bySourceID(c, { named: "models/sales.malloy" }),
         undefined,
         { a2: prior("named_t__v1_2_0__p0123abcd", "a2") },
         undefined,
         V120,
      );
      expect(names(r).a2).toMatch(/^pa__sales__named__v1_2_0__p[0-9a-f]{8}$/);
   });
});

describe("the one rename rule: what an auto-run offers to the reclaim", () => {
   /** What `r` offers: each old table, the new one, and where it was. */
   const offered = (r: ReturnType<typeof deriveSelfInstructions>) =>
      r.renamed.map((t) => ({
         previousName: t.previousName,
         physicalTableName: t.physicalTableName,
         ...(t.previousPlace ? { previousPlace: t.previousPlace } : {}),
      }));

   it("under scope: version, a name= prefix changed", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a2: prior("old_t__v1_2_0__p0123abcd", "a2") },
         undefined,
         V120,
      );
      expect(offered(r)).toEqual([
         {
            previousName: "old_t__v1_2_0__p0123abcd",
            physicalTableName: expect.stringMatching(/^named_t__v1_2_0__p/),
         },
      ]);
   });

   it("under scope: version, a name= container changed", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a3: prior("staging.q_t__v1_2_0__p0123abcd", "a3") },
         undefined,
         V120,
      );
      expect(offered(r)).toEqual([
         {
            previousName: "staging.q_t__v1_2_0__p0123abcd",
            physicalTableName: expect.stringMatching(
               /^analytics\.q_t__v1_2_0__p/,
            ),
         },
      ]);
   });

   it("a readable-name table, when the author adds name=", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         { a3: prior("pa__sales__qualified__p0123abcd", "a3") },
         undefined,
         PKG,
      );
      expect(offered(r)).toEqual([
         {
            previousName: "pa__sales__qualified__p0123abcd",
            physicalTableName: "analytics.q_t",
         },
      ]);
   });

   it("a rollup moved to another container", () => {
      const r = deriveSelfInstructions(
         withRollup(compiled()),
         undefined,
         { a4: prior("old_ns.pa__sales__orders_by_category__p0123abcd", "a4") },
         undefined,
         PKG,
      );
      expect(offered(r)).toEqual([
         {
            previousName: "old_ns.pa__sales__orders_by_category__p0123abcd",
            physicalTableName: expect.stringMatching(
               /^ns\.pa__sales__orders_by_category__p/,
            ),
         },
      ]);
   });

   it("an exact name= changed or removed (the reclaim keeps and lists it)", () => {
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            a1: prior("summary_t", "a1"),
            a2: prior("named_old", "a2"),
         },
         undefined,
         PKG,
      );
      expect(offered(r)).toEqual([
         {
            previousName: "summary_t",
            physicalTableName: expect.stringMatching(READABLE),
         },
         { previousName: "named_old", physicalTableName: "named_t" },
      ]);
   });

   describe("a storage= destination changed", () => {
      let restore: () => void;
      beforeEach(() => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
      });
      afterEach(() => restore());

      const stored = (storage: string | undefined) =>
         compiledWith(
            {
               s: fakeSource({
                  name: "s",
                  sourceEntityId: "a1",
                  ...(storage ? { annotationFields: { storage } } : {}),
               }),
            },
            [["s"]],
         );

      for (const [from, to] of [
         ["lake", "lake2"],
         ["lake", undefined],
         [undefined, "lake"],
      ] as const) {
         it(`from ${from ?? "the source's own database"} to ${to ?? "the source's own database"}: offered from its old place`, () => {
            const name = "pa__s__s__p0123abcd";
            const r = deriveSelfInstructions(
               stored(to),
               undefined,
               {
                  a1: {
                     ...prior(name, "a1"),
                     ...(from ? { storageDestinationName: from } : {}),
                  } as ManifestEntry,
               },
               undefined,
               PKG,
            );
            expect(r.carried).toEqual({});
            expect(r.renamed).toHaveLength(1);
            expect(r.renamed[0]).toMatchObject({
               previousName: name,
               previousPlace: from ? { storageDestinationName: from } : {},
               ...(to ? { storageDestinationName: to } : {}),
            });
         });
      }
   });

   it("nothing for a table carried, an edit to the SQL, or a name another source now takes", () => {
      const now = names(
         deriveSelfInstructions(compiled(), undefined, {}, undefined, PKG),
      );
      const r = deriveSelfInstructions(
         compiled(),
         undefined,
         {
            // Carried as it is.
            a1: prior(now.a1, "a1"),
            // Another content address (its SQL changed): no prior for a3.
            zz: prior("pa__sales__qualified__p0123abcd", "zz"),
            // Its old name is now a2's own name=.
            a3: prior("named_t", "a3"),
         },
         undefined,
         PKG,
      );
      expect(r.carried.a1).toBeDefined();
      expect(r.renamed).toEqual([]);
   });
});

describe("a persisted named query's table", () => {
   const COMPANION = "file:///pkg/models/sales.malloy.persist-view.malloy";
   /** `q`'s persisted query, a persisted view, and a source named `top`. */
   const plan = (): CompiledBuildPlan => {
      const ofCompanion = (name: string, sourceEntityId: string) => {
         const source = fakeSource({ name, sourceEntityId });
         (source as { sourceID: string }).sourceID = `${name}@${COMPANION}`;
         return source;
      };
      return bySourceID(
         compiledWith(
            {
               top__query: ofCompanion("top__query", "q1"),
               orders__top__view: ofCompanion("orders__top__view", "v1"),
               orders__top__query: ofCompanion("orders__top__query", "q2"),
               top_query__query: ofCompanion("top_query__query", "q3"),
               top: fakeSource({ name: "top", sourceEntityId: "s1" }),
               // A plain source whose own name ends `__query` keeps it.
               mine__query: fakeSource({
                  name: "mine__query",
                  sourceEntityId: "s2",
               }),
            },
            [
               [
                  "top__query",
                  "orders__top__view",
                  "orders__top__query",
                  "top_query__query",
                  "top",
                  "mine__query",
               ],
            ],
         ),
         {
            [`top__query@${COMPANION}`]: "models/sales.malloy",
            [`orders__top__view@${COMPANION}`]: "models/sales.malloy",
            [`orders__top__query@${COMPANION}`]: "models/sales.malloy",
            [`top_query__query@${COMPANION}`]: "models/sales.malloy",
            top: "models/sales.malloy",
            mine__query: "models/sales.malloy",
         },
      );
   };
   const run = (priors: Record<string, ManifestEntry> = {}) =>
      deriveSelfInstructions(plan(), undefined, priors, undefined, PKG);

   it("reads as the query, with no `_query`, as a persisted view's reads with no `_view`", () => {
      const n = names(run());
      expect(n.q1).toMatch(/^pa__sales__top__p[0-9a-f]{8}$/);
      expect(n.v1).toMatch(/^pa__sales__orders_top__p[0-9a-f]{8}$/);
      // A query whose own name ends `_query` keeps it.
      expect(n.q3).toMatch(/^pa__sales__top_query__p[0-9a-f]{8}$/);
      expect(n.s2).toMatch(/^pa__sales__mine_query__p[0-9a-f]{8}$/);
   });

   it("is never another's table when a source, a view and a query read alike: the hash is over the build source's name", () => {
      const n = names(run());
      // `top` (a source) and `top` (a query) read the same; so do the view
      // `top` of `orders` and a query named `orders__top`.
      expect(n.q1.replace(/__p[0-9a-f]{8}$/, "")).toBe(
         n.s1.replace(/__p[0-9a-f]{8}$/, ""),
      );
      expect(n.q2.replace(/__p[0-9a-f]{8}$/, "")).toBe(
         n.v1.replace(/__p[0-9a-f]{8}$/, ""),
      );
      expect(new Set(Object.values(n)).size).toBe(Object.keys(n).length);
      const r = run();
      expect(r.refused).toEqual({});
      expect(r.instructions.map((i) => i.sourceEntityId).sort()).toEqual(
         ["q1", "q2", "q3", "s1", "s2", "v1"].sort(),
      );
   });

   it("built under its `_query` name before, is renamed, rebuilt once, and the old one offered for reclaim", () => {
      const legacy = "pa__sales__top_query__p0123abcd";
      const r = run({ q1: prior(legacy, "q1") });
      expect(names(r).q1).toMatch(/^pa__sales__top__p[0-9a-f]{8}$/);
      expect(r.carried.q1).toBeUndefined();
      expect(r.renamed.map((t) => t.previousName)).toEqual([legacy]);
      // Once: the next run carries the new name.
      const next = run({ q1: prior(names(r).q1, "q1") });
      expect(next.carried.q1).toBeDefined();
      expect(next.renamed).toEqual([]);
   });

   it("keeps a name that ends `_query` because the query's own name does, and a source's", () => {
      const r = run({
         q3: prior("pa__old__top_query__p0123abcd", "q3"),
         s2: prior("pa__old__mine_query__p0123abcd", "s2"),
      });
      expect(r.carried.q3?.physicalTableName).toBe(
         "pa__old__top_query__p0123abcd",
      );
      expect(r.carried.s2?.physicalTableName).toBe(
         "pa__old__mine_query__p0123abcd",
      );
      expect(r.renamed).toEqual([]);
   });
});
