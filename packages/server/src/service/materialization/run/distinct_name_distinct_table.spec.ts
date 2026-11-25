// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Two sources that compile to one build share one content address. An
// author's `name=` is part of what identifies a table, and so is the set of
// columns a `storage=` build stores: sources of one build that differ in
// either get a table each (foldDistinctTableNames), whatever the build order.
// Sources that agree (the same `name=`, none, the same projection) share one
// table, at the address they always had.

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import { derivePackagePlan, sharedTables } from "../plan/build_plan";
import { loadGateText } from "../plan/load_gate_text";
import type { BuildInstruction } from "../domain/types";
import { computeSourceEntityId } from "../plan/addressing";
import {
   foldDistinctTableNames,
   planAddresses,
   storedColumnNames,
} from "../plan/build_plan";
import {
   assertNoTableCollisions,
   indexInstructions,
} from "./materialization_instructed_run";
import { deriveSelfInstructions } from "./run_instructions";

const ADDRESS = "shared";
const CONNECTIONS = new Map([["duckdb", {} as unknown as MalloyConnection]]);
const NAMING = { environmentName: "env", packageName: "pkg" };

type Spec = { annotationFields?: Record<string, string>; columns?: string[] };

/** `a` and `b` on one build, derived in `order`. */
function derive(
   a: Spec,
   b: Spec,
   order: string[][],
   naming: Parameters<typeof deriveSelfInstructions>[4] = NAMING,
): BuildInstruction[] {
   const sources = {
      a: fakeSource({ name: "a", sourceEntityId: ADDRESS, ...a }),
      b: fakeSource({ name: "b", sourceEntityId: ADDRESS, ...b }),
   };
   return deriveSelfInstructions(
      compiledWith(sources, order, CONNECTIONS),
      undefined,
      {},
      undefined,
      naming,
   ).instructions;
}

const ORDERS = [
   [["a"], ["b"]],
   [["b"], ["a"]],
];

/** The instructions as (address, table), sorted, so order does not show. */
function tables(instructions: BuildInstruction[]): [string, string][] {
   return instructions
      .map((i): [string, string] => [i.sourceEntityId, i.physicalTableName])
      .sort((x, y) => x[1].localeCompare(y[1]) || x[0].localeCompare(y[0]));
}

describe("one table per name=", () => {
   it("builds a table under each of two name=s on one build, in either order", () => {
      const results = ORDERS.map((order) =>
         tables(
            derive(
               { annotationFields: { name: "a_tbl" } },
               { annotationFields: { name: "b_tbl" } },
               order,
            ),
         ),
      );
      expect(results[0].map(([, t]) => t)).toEqual(["a_tbl", "b_tbl"]);
      // Two addresses, neither the shared one, and the same in both orders.
      expect(new Set(results[0].map(([a]) => a)).size).toBe(2);
      expect(results[0].map(([a]) => a)).not.toContain(ADDRESS);
      expect(results[1]).toEqual(results[0]);
   });

   it("names each table by its own name= under scope: version, in either order", () => {
      const results = ORDERS.map((order) =>
         tables(
            derive(
               { annotationFields: { name: "a_tbl" } },
               { annotationFields: { name: "b_tbl" } },
               order,
               { ...NAMING, ownedVersion: "1.0.0" },
            ),
         ),
      );
      expect(results[0]).toHaveLength(2);
      expect(results[0][0][1]).toStartWith("a_tbl__");
      expect(results[0][1][1]).toStartWith("b_tbl__");
      expect(results[1]).toEqual(results[0]);
   });

   it("keeps sources of one name= on one table, at the address they had", () => {
      for (const order of ORDERS) {
         expect(
            tables(
               derive(
                  { annotationFields: { name: "t" } },
                  { annotationFields: { name: "t" } },
                  order,
               ),
            ),
         ).toEqual([[ADDRESS, "t"]]);
      }
   });

   it("keeps two name=s a warehouse may read as one table on one table", () => {
      // Folded where the destination folds case (DuckDB), and a name with
      // fewer containers meets one that ends the same way (tableNamesMeet).
      for (const [x, y] of [
         ["T", "t"],
         ['"T"', '"t"'],
         ["main.t", "t"],
      ]) {
         expect(
            tables(
               derive(
                  { annotationFields: { name: x } },
                  { annotationFields: { name: y } },
                  [["a"], ["b"]],
               ),
            ).map(([a]) => a),
         ).toEqual([ADDRESS]);
      }
   });

   it("splits two name=s in different schemas, and quoted names in two cases where the warehouse keeps case", () => {
      for (const [x, y, dialectName] of [
         ["s1.t", "s2.t", "duckdb"],
         ['"T"', '"t"', "postgres"],
      ]) {
         const sources = {
            a: fakeSource({
               name: "a",
               sourceEntityId: ADDRESS,
               dialectName,
               annotationFields: { name: x },
            }),
            b: fakeSource({
               name: "b",
               sourceEntityId: ADDRESS,
               dialectName,
               annotationFields: { name: y },
            }),
         };
         for (const order of ORDERS) {
            const built = tables(
               deriveSelfInstructions(
                  compiledWith(sources, order, CONNECTIONS),
                  undefined,
                  {},
                  undefined,
                  NAMING,
               ).instructions,
            );
            expect(built.map(([, t]) => t)).toEqual(
               [x, y].sort((p, q) => p.localeCompare(q)),
            );
            expect(new Set(built.map(([a]) => a)).size).toBe(2);
         }
      }
   });

   it("keeps sources with no name= on one table, at the address they had", () => {
      for (const order of ORDERS) {
         const built = tables(derive({}, {}, order));
         expect(built).toHaveLength(1);
         expect(built[0][0]).toBe(ADDRESS);
         // Named after the first in model path, then source, order: `a`.
         expect(built[0][1]).toStartWith("pkg__");
         expect(built[0][1]).toContain("__a__");
      }
   });

   it("gives a name= its own table beside a source with none, which keeps the address", () => {
      const results = ORDERS.map((order) =>
         tables(derive({ annotationFields: { name: "a_tbl" } }, {}, order)),
      );
      expect(results[0]).toHaveLength(2);
      expect(results[0][0][1]).toBe("a_tbl");
      expect(results[0][0][0]).not.toBe(ADDRESS);
      // `b`'s readable table, at the address it always had.
      expect(results[0][1][0]).toBe(ADDRESS);
      expect(results[0][1][1]).toContain("__b__");
      expect(results[1]).toEqual(results[0]);
   });
});

describe("one table per stored projection", () => {
   const STORED = { storage: "lake" };

   function withStorage<T>(fn: () => T): T {
      const { restore } = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      });
      try {
         return fn();
      } finally {
         restore();
      }
   }

   it("stores two projections of one build in a table each, in either order", () => {
      const results = withStorage(() =>
         ORDERS.map((order) =>
            tables(
               derive(
                  {
                     annotationFields: STORED,
                     columns: ["id", "region", "status"],
                  },
                  { annotationFields: STORED, columns: ["id", "region"] },
                  order,
               ),
            ),
         ),
      );
      expect(results[0]).toHaveLength(2);
      expect(results[0].map(([a]) => a)).not.toContain(ADDRESS);
      expect(results[0][0][1]).toContain("__a__");
      expect(results[0][1][1]).toContain("__b__");
      expect(results[1]).toEqual(results[0]);
   });

   it("splits one name= stored as two projections, in either order: a collision an auto-run builds neither of, and a host run refuses, naming the columns", () => {
      const results = withStorage(() =>
         ORDERS.map((order) => {
            const a = {
               annotationFields: { ...STORED, name: "t" },
               columns: ["id", "region", "status"],
            };
            const b = {
               annotationFields: { ...STORED, name: "t" },
               columns: ["id", "region"],
            };
            let autoRun: string | undefined;
            try {
               derive(a, b, order);
            } catch (err) {
               autoRun = (err as Error).message;
            }
            const compiled = compiledWith(
               {
                  a: fakeSource({ name: "a", sourceEntityId: ADDRESS, ...a }),
                  b: fakeSource({ name: "b", sourceEntityId: ADDRESS, ...b }),
               },
               order,
               CONNECTIONS,
            );
            const tableFolds = foldDistinctTableNames(
               compiled,
               storedColumnNames,
            );
            const addresses = Object.values(compiled.tables).map(
               ({ source: s }) =>
                  computeSourceEntityId(
                     s,
                     compiled.connectionDigests,
                     undefined,
                     tableFolds,
                  ),
            );
            let hostRun: string | undefined;
            try {
               assertNoTableCollisions(
                  compiled.builds,
                  compiled.tables,
                  compiled.connectionDigests,
                  indexInstructions(
                     addresses.map((sourceEntityId, i) => ({
                        sourceEntityId,
                        materializedTableId: `m${i}`,
                        physicalTableName: "t",
                        realization: "COPY" as const,
                        destination: "lake",
                     })),
                  ),
                  tableFolds,
               );
            } catch (err) {
               hostRun = (err as Error).message;
            }
            return { addresses: [...addresses].sort(), autoRun, hostRun };
         }),
      );
      for (const { addresses, autoRun, hostRun } of results) {
         // Two addresses, one table: the auto-run builds neither, so neither
         // projection is written over the other's.
         expect(new Set(addresses).size).toBe(2);
         expect(addresses).not.toContain(ADDRESS);
         expect(autoRun).toContain('all write the table "t"');
         expect(hostRun).toContain(
            "compile to the same query but store different columns, yet both materialize into table 't'",
         );
      }
      expect(results[1].addresses).toEqual(results[0].addresses);
   });

   it("leaves a database source of the build on the address it had", () => {
      const built = withStorage(() =>
         tables(
            derive(
               { annotationFields: STORED, columns: ["id", "region"] },
               { columns: ["id", "region", "status"] },
               [["a"], ["b"]],
            ),
         ),
      );
      // Twins in two tiers (P2-21): the stored one moves to an address of its
      // own, the database `b` keeps the one it had.
      expect(built).toHaveLength(2);
      const database = built.find(([, t]) => t.includes("__b__"));
      const stored = built.find(([, t]) => t.includes("__a__"));
      expect(database?.[0]).toBe(ADDRESS);
      expect(stored?.[0]).not.toBe(ADDRESS);
   });

   it("keeps one projection on one table, at the address it had", () => {
      const built = withStorage(() =>
         tables(
            derive(
               { annotationFields: STORED, columns: ["id", "region"] },
               { annotationFields: STORED, columns: ["region", "id"] },
               [["b"], ["a"]],
            ),
         ),
      );
      expect(built.map(([a]) => a)).toEqual([ADDRESS]);
   });
});

describe("one table per stored projection, over the real compiler", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   const RAW = `source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', 'open'), (2, 'west', 'closed')) AS t(id, region, status)
""")`;
   const HIDES = `#@ persist name="proj_t" storage="lake"
source: b_hides is raw -> { select: id, region, status } include { public: id, region; private: status }`;
   const DROPS = `#@ persist name="proj_t" storage="lake"
source: a_drops is raw -> { select: id, region, status } extend { except: status }`;

   async function addressesOf(model: string) {
      const { restore } = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      });
      try {
         const compiled = await compilePackageBuildPlan(
            await packages.create(model),
         );
         return Object.fromEntries(
            Object.values(compiled.tables).map(({ source: s, target }) => [
               s.name,
               computeSourceEntityId(
                  s,
                  compiled.connectionDigests,
                  target,
                  compiled.tableFolds,
               ),
            ]),
         );
      } finally {
         restore();
      }
   }

   it("addresses one name= stored as two projections apart, in either order", async () => {
      const header = "##! experimental { persistence access_modifiers }";
      const one = await addressesOf([header, RAW, HIDES, DROPS].join("\n"));
      const two = await addressesOf([header, RAW, DROPS, HIDES].join("\n"));
      expect(one.a_drops).not.toBe(one.b_hides);
      // The same two addresses whichever is declared first.
      expect(two).toEqual(one);
   });

   it("keeps an extension on its base's table, whatever it renames or adds", async () => {
      const addresses = await addressesOf(
         [
            "##! experimental.persistence",
            RAW,
            '#@ persist name="base_t" storage="lake"',
            "source: base is raw -> { group_by: area is region, status } extend { rename: state is status }",
            "source: ext is base extend { rename: zone is area; dimension: one is 1 }",
         ].join("\n"),
      );
      expect(addresses.ext).toBe(addresses.base);
   });
});

describe("a bare name between two schemas", () => {
   const NAMES: Record<string, string> = { a: "s1.t", b: "s2.t", c: "t" };
   /** Every declaration order of the three. */
   const ORDERS3 = [
      ["a", "b", "c"],
      ["a", "c", "b"],
      ["b", "a", "c"],
      ["b", "c", "a"],
      ["c", "a", "b"],
      ["c", "b", "a"],
   ];

   function trio(order: string[], storage: boolean) {
      const sources = Object.fromEntries(
         order.map((id) => [
            id,
            fakeSource({
               name: id,
               sourceEntityId: ADDRESS,
               annotationFields: {
                  name: NAMES[id],
                  ...(storage ? { storage: "lake" } : {}),
               },
            }),
         ]),
      );
      const compiled = compiledWith(
         sources,
         order.map((id) => [id]),
         CONNECTIONS,
      );
      return {
         ...compiled,
         tableFolds: foldDistinctTableNames(compiled, storedColumnNames),
      };
   }

   for (const storage of [false, true]) {
      it(`gives each of s1.t and s2.t its own address, and t neither's, in every order (${storage ? "storage=" : "database"})`, () => {
         const results = withStorage(() =>
            ORDERS3.map((order) => {
               const compiled = trio(order, storage);
               const addressOf = (id: string) =>
                  computeSourceEntityId(
                     compiled.tables[id].source,
                     compiled.connectionDigests,
                     undefined,
                     compiled.tableFolds,
                  );
               return {
                  addresses: ["a", "b", "c"].map(addressOf),
                  shared: [
                     ...sharedTables({
                        ...compiled,
                        addresses: planAddresses(compiled),
                     }).keys(),
                  ].sort(),
               };
            }),
         );
         for (const { addresses, shared } of results) {
            // Three addresses: neither schema's table is stood in for by t.
            expect(new Set(addresses).size).toBe(3);
            // t may be either table, so it collides with both, and each of
            // the three is refused rather than one written for all.
            expect(shared).toEqual(
               ["a", "b", "c"]
                  .map(
                     (id) =>
                        trio(["a", "b", "c"], storage).tables[id].source
                           .sourceID,
                  )
                  .sort(),
            );
         }
         for (const r of results.slice(1)) {
            expect(r.addresses).toEqual(results[0].addresses);
         }
      });
   }

   it("refuses each of the three alone in an auto-run, in every order", () => {
      for (const order of ORDERS3) {
         const compiled = trio(order, false);
         let error: string | undefined;
         try {
            deriveSelfInstructions(compiled, undefined, {}, undefined, NAMING);
         } catch (err) {
            error = (err as Error).message;
         }
         // Nothing else to build: every source refused for the collision.
         expect(error).toContain('all write the table "');
      }
   });
});

function withStorage<T>(fn: () => T): T {
   const { restore } = installSettings({ env: { PERSIST_STORAGE_MODE: "on" } });
   try {
      return fn();
   } finally {
      restore();
   }
}

describe("a bare name between two schemas, over the real compiler", () => {
   const packages = tempBuildPlanPackages();
   afterEach(() => packages.cleanup());

   const RAW = `source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east'), (2, 'west')) AS t(id, region)
""")`;

   for (const storage of [false, true]) {
      for (const order of [
         ["s1.t", "s2.t", "t"],
         ["t", "s2.t", "s1.t"],
         ["s2.t", "t", "s1.t"],
      ]) {
         it(`fails the load with materialization-table-collision (${storage ? "storage=" : "database"}, ${order.join(" ")})`, async () => {
            const model = [
               "##! experimental.persistence",
               RAW,
               ...order.map(
                  (name, i) =>
                     `#@ persist name="${name}"${storage ? ' storage="lake"' : ""}\nsource: src${i} is raw -> { select: id, region }`,
               ),
            ].join("\n");
            const { restore } = installSettings({
               env: { PERSIST_STORAGE_MODE: "on" },
            });
            try {
               const compiled = await compilePackageBuildPlan(
                  await packages.create(model),
               );
               const outcome = derivePackagePlan(compiled, null);
               const text = loadGateText(
                  {
                     plan: outcome.plan,
                     annotationFindings: outcome.annotationFindings,
                     incrementalDeclarations: outcome.incrementalDeclarations,
                     ...(outcome.materializePlans
                        ? { materializePlans: outcome.materializePlans }
                        : {}),
                  },
                  { enforceCollisions: true, versionScoped: false },
               );
               expect(text).toContain("[materialization-table-collision]");
               for (const i of [0, 1, 2]) expect(text).toContain(`"src${i}"`);
            } finally {
               restore();
            }
         });
      }
   }
});
