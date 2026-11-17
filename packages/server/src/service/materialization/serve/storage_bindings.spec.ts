// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ModelDef } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   persistedResultFixture,
   type PersistedResultFixture,
} from "../../../test_helpers/persisted_result_fixture";
import type { Binding } from "../domain/binding";
import type { BuildPlan } from "../domain/types";
import type { MaterializePlan } from "../domain/table_plans";
import {
   boundTableTypeFindings,
   databaseDriftFindings,
   modelStorageTables,
   ownBindings,
} from "./storage_bindings";

/**
 * What one model reads of a snapshot's `storage=` tables: the bindings of its
 * own sources (a same-named source declared elsewhere is not its own), and
 * each stored column whose type does not serve as declared, read through a
 * cast or answered live.
 */

const MODEL = `source: orders is duckdb.sql("""
  select 1 as id, 'east' as region, DATE '2024-01-01' as day
""")
`;

function binding(over: Partial<Binding> = {}): Binding {
   return {
      kind: "persist",
      tier: "storage",
      sourceName: "orders",
      destinationName: "lake",
      virtualHandle: "vh",
      tablePath: "main.orders_t",
      schema: [
         { name: "id", type: "INTEGER" },
         { name: "region", type: "VARCHAR" },
         { name: "day", type: "DATE" },
      ],
      ...over,
   };
}

describe("modelStorageTables", () => {
   let f: PersistedResultFixture;
   let modelDef: ModelDef;
   let ownID: string;
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL, { noBuildPlan: true });
      modelDef = f.modelDef;
      ownID = (modelDef.contents.orders as unknown as { sourceID: string })
         .sourceID;
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("reads nothing when no binding is a `#@ persist` storage table", () => {
      expect(
         modelStorageTables(
            [
               binding({ kind: "materialize" }),
               binding({ tier: "database" }),
               binding({ kind: "view" }),
            ],
            modelDef,
            f.packagePath,
         ),
      ).toEqual({ kept: [], shadowed: [], mismatches: [], findings: [] });
   });

   it("keeps a binding of this model's own source, and shadows one of a same-named source elsewhere", () => {
      const own = binding({ sourceIDs: [ownID] });
      const other = binding({
         sourceIDs: ["orders@file:///elsewhere.malloy"],
      });
      const out = modelStorageTables([own, other], modelDef, f.packagePath);
      expect(out.kept).toEqual([own]);
      expect(out.shadowed).toEqual(["orders"]);
      expect(out.mismatches).toEqual([]);
      expect(out.checkFailed).toBeUndefined();
   });

   it("reports each mismatched column, with a cast where one is faithful and live otherwise", () => {
      const own = binding({
         sourceIDs: [ownID],
         schema: [
            { name: "id", type: "VARCHAR" },
            { name: "region", type: "VARCHAR" },
            { name: "day", type: "TIMESTAMP" },
         ],
      });
      const out = modelStorageTables([own], modelDef, f.packagePath);
      expect(out.mismatches).toEqual([
         {
            sourceName: "orders",
            sourceID: ownID,
            mismatches: [
               {
                  column: "id",
                  storedType: "VARCHAR",
                  declaredType: "number",
                  handling: "live",
               },
               {
                  column: "day",
                  storedType: "TIMESTAMP",
                  declaredType: "date",
                  handling: "cast",
               },
            ],
         },
      ]);
      expect(out.findings).toHaveLength(1);
      expect(out.findings[0].binding).toBe(own);
      expect(out.findings[0].casts).toEqual([
         { column: "day", castTo: "DATE" },
      ]);
      expect(out.findings[0].unservable.map((m) => m.column)).toEqual(["id"]);
   });

   it("reports no mismatch for a binding whose name the model does not define", () => {
      const stranger = binding({
         sourceName: "nowhere",
         schema: [{ name: "id", type: "VARCHAR" }],
      });
      const out = modelStorageTables([stranger], modelDef, f.packagePath);
      expect(out.kept).toEqual([stranger]);
      expect(out.mismatches).toEqual([]);
   });

   it("reports why when the type check cannot run, and keeps the bindings it found", () => {
      const broken = binding({
         sourceIDs: [ownID],
         schema: undefined as never,
      });
      const out = modelStorageTables([broken], modelDef, f.packagePath);
      expect(out.kept).toEqual([broken]);
      expect(out.mismatches).toEqual([]);
      expect(out.findings).toEqual([]);
      expect(typeof out.checkFailed).toBe("string");
   });

   it("keeps every binding with no model to read, the name alone deciding", () => {
      const own = binding({ sourceIDs: [ownID] });
      expect(modelStorageTables([own], undefined).kept).toEqual([own]);
   });
});

describe("ownBindings", () => {
   let f: PersistedResultFixture;
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL, { noBuildPlan: true });
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("keeps every binding when none names its sourceIDs, without reading the model", () => {
      const a = binding();
      const b = binding({ sourceName: "other", kind: "rollup" });
      expect(ownBindings([a, b], undefined)).toEqual({
         kept: [a, b],
         shadowed: [],
      });
   });

   it("splits bindings into this model's own and a same-named source's elsewhere", () => {
      const ownID = (
         f.modelDef.contents.orders as unknown as { sourceID: string }
      ).sourceID;
      const own = binding({ sourceIDs: [ownID] });
      const other = binding({ sourceIDs: ["orders@file:///x.malloy"] });
      const rollup = binding({
         kind: "rollup",
         sourceIDs: ["orders@file:///x.malloy"],
      });
      expect(
         ownBindings([own, other, rollup], f.modelDef, f.packagePath),
      ).toEqual({ kept: [own, rollup], shadowed: ["orders"] });
   });
});

describe("boundTableTypeFindings", () => {
   const plan = {
      sources: {
         "orders@m": {
            name: "orders",
            sourceEntityId: "vh",
            modelPath: "m.malloy",
            columns: [
               { name: "id", type: "number" },
               { name: "day", type: "date" },
            ],
         },
         "r@m": {
            name: "orders__rollup",
            sourceEntityId: "vr",
            modelPath: "m.malloy",
            preaggregate: { baseSourceName: "orders", grainDimensions: ["k"] },
            columns: [
               { name: "k", type: "number" },
               { name: "s__partial", type: "number" },
            ],
         },
      },
   } as unknown as BuildPlan;
   const stored = [
      { name: "id", type: "VARCHAR" },
      { name: "day", type: "TIMESTAMP" },
   ];

   it("reads each kind's mismatches against its build source's columns", () => {
      const findings = boundTableTypeFindings(
         [
            binding({ kind: "view", sourceIDs: ["orders@m"], schema: stored }),
            binding({
               kind: "rollup",
               virtualHandle: "vr",
               schema: [
                  { name: "k", type: "VARCHAR" },
                  { name: "s__partial", type: "VARCHAR" },
               ],
            }),
            binding({ kind: "materialize", schema: stored }),
         ],
         plan,
      );
      expect(
         findings.map((f) => ({
            subject: f.subject,
            kind: f.binding.kind,
            handling: f.mismatches.map((m) => [m.column, m.handling]),
         })),
      ).toEqual([
         // A persisted result casts what a faithful cast reads.
         {
            subject: "orders",
            kind: "view",
            handling: [
               ["id", "live"],
               ["day", "cast"],
            ],
         },
         // A rollup and a `#(materialize)` table follow the same rule:
         // neither reads a column as its stored type.
         {
            subject: "orders",
            kind: "rollup",
            handling: [
               ["k", "live"],
               ["s__partial", "live"],
            ],
         },
         {
            subject: "orders",
            kind: "materialize",
            handling: [
               ["id", "live"],
               ["day", "cast"],
            ],
         },
      ]);
   });

   it("reports a castable column live where its reader cannot cast it: in a record, or under a query time zone", () => {
      const tzPlan = {
         sources: {
            "orders@m": {
               name: "orders",
               sourceEntityId: "vh",
               modelPath: "m.malloy",
               columns: [
                  { name: "day", type: "date" },
                  { name: "c__on", type: "date" },
                  { name: "flag", type: "boolean" },
               ],
            },
         },
      } as unknown as BuildPlan;
      const schema = [
         { name: "day", type: "TIMESTAMP WITH TIME ZONE" },
         { name: "c__on", type: "TIMESTAMP" },
         { name: "flag", type: "INTEGER" },
      ];
      const handling = (zone?: string) =>
         boundTableTypeFindings(
            [binding({ kind: "materialize", sourceIDs: ["orders@m"], schema })],
            tzPlan,
            {
               queryTimezone: () => zone,
               materializePlans: {
                  "orders@m": {
                     columns: [
                        { column: "day", dimension: "day" },
                        { column: "c__on", dimension: "on", join: "c" },
                        { column: "flag", dimension: "flag" },
                     ],
                  } as unknown as MaterializePlan,
               },
            },
         )[0].mismatches.map((m) => [m.column, m.handling]);
      expect(handling()).toEqual([
         ["day", "cast"],
         ["c__on", "live"],
         ["flag", "cast"],
      ]);
      expect(handling("America/New_York")).toEqual([
         ["day", "live"],
         ["c__on", "live"],
         ["flag", "cast"],
      ]);
   });

   it("leaves a `#@ persist` table to its model's check, and a colocated one alone", () => {
      expect(
         boundTableTypeFindings(
            [
               binding({ schema: stored }),
               binding({
                  kind: "materialize",
                  tier: "database",
                  schema: stored,
               }),
            ],
            plan,
         ),
      ).toEqual([]);
   });
});

describe("databaseDriftFindings", () => {
   const plan = {
      sources: {
         "orders@m": {
            name: "orders",
            sourceEntityId: "vh",
            modelPath: "m.malloy",
            columns: [
               { name: "id", type: "number" },
               { name: "flag", type: "boolean" },
               { name: "amt", type: "number" },
               { name: "day", type: "date" },
            ],
         },
         "r@m": {
            name: "orders__rollup",
            sourceEntityId: "vr",
            modelPath: "m.malloy",
            preaggregate: { baseSourceName: "orders", grainDimensions: ["k"] },
            columns: [{ name: "k", type: "number" }],
         },
      },
   } as unknown as BuildPlan;

   it("handles each moved column as the bind does: cast, left out, or the whole table live", () => {
      const findings = databaseDriftFindings(
         [
            {
               sourceEntityId: "vh",
               columns: [
                  // A faithful cast reads it.
                  {
                     column: "flag",
                     builtType: "BOOLEAN",
                     observedType: "INTEGER",
                  },
                  // None does: left out.
                  {
                     column: "amt",
                     builtType: "DOUBLE",
                     observedType: "VARCHAR",
                  },
                  // Gone: left out.
                  {
                     column: "day",
                     builtType: "DATE",
                     gone: true,
                     leftOut: true,
                  },
                  // A zone gained off DuckDB: the bind leaves it out, though
                  // the type alone would serve as declared.
                  {
                     column: "ts",
                     builtType: "TIMESTAMP",
                     observedType: "TIMESTAMP WITH TIME ZONE",
                     leftOut: true,
                  },
                  // Now a type Malloy reads as no basic type: left out.
                  { column: "m", builtType: "VARCHAR", leftOut: true },
                  // Still a number: served as is, so not reported.
                  {
                     column: "id",
                     builtType: "VARCHAR",
                     observedType: "BIGINT",
                  },
               ],
            },
            {
               sourceEntityId: "vr",
               columns: [
                  { column: "k", builtType: "BIGINT", observedType: "VARCHAR" },
               ],
            },
            // Not in the plan: nothing to name it by.
            {
               sourceEntityId: "other",
               columns: [{ column: "x", builtType: "BIGINT" }],
            },
         ],
         plan,
         { servedCopy: (id) => id === "vh" },
      );
      expect(findings).toEqual([
         {
            sourceID: "orders@m",
            sourceEntityId: "vh",
            modelPath: "m.malloy",
            subject: "orders",
            mismatches: [
               {
                  column: "flag",
                  declaredType: "boolean",
                  storedType: "INTEGER",
                  handling: "cast",
               },
               {
                  column: "amt",
                  declaredType: "number",
                  storedType: "VARCHAR",
                  handling: "live",
               },
               {
                  column: "day",
                  declaredType: "date",
                  gone: true,
                  handling: "live",
               },
               {
                  column: "ts",
                  declaredType: "timestamp",
                  storedType: "TIMESTAMP WITH TIME ZONE",
                  handling: "live",
               },
               { column: "m", declaredType: "string", handling: "live" },
            ],
         },
         {
            sourceID: "r@m",
            sourceEntityId: "vr",
            modelPath: "m.malloy",
            subject: "orders",
            mismatches: [
               {
                  column: "k",
                  declaredType: "number",
                  storedType: "VARCHAR",
                  handling: "table",
               },
            ],
         },
      ]);
   });
});
