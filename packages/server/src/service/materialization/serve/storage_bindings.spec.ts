// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { ModelDef } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   persistedResultFixture,
   type PersistedResultFixture,
} from "../../../test_helpers/persisted_result_fixture";
import type { Binding } from "../domain/binding";
import { modelStorageTables, ownBindings } from "./storage_bindings";

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
               binding({ tier: "colocated" }),
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
