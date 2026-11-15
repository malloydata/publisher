// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   arrayQueryJoins,
   forEachFieldPath,
   governsAccess,
} from "./array_query";

const MODEL = `##! experimental.array_queries
source: other is duckdb.sql("select 'a' as k, 1 as v")
source: p is duckdb.sql("""
  select 1 as id, 'r' as price,
    [{'sku': 'a', 'price': 10, 'tags': [{'t': 'x', 'w': 1}]}] as items
""") extend {
  dimension: total is items -> { aggregate: t is price.sum() }
  dimension: nested is items -> { select: sku, tw is tags -> { aggregate: w2 is w.sum() } }
  dimension: declared is items -> { extend: { dimension: big is price > 5 } where: big; aggregate: c is count() }
  dimension: later is items -> { select: sku, price } -> { where: price > 5; aggregate: c is count() }
  dimension: joined is items -> { extend: { join_one: o is other on sku = o.k } aggregate: s is o.v.sum() }
  dimension: nested_join is items -> { select: tw is tags -> { extend: { join_one: o is other on t = o.k } aggregate: s is o.v.sum() } }
}
`;

let duckdb: DuckDBConnection;
let fields: Record<string, { e?: unknown }>;
let modelDef: unknown;

beforeAll(async () => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   const runtime = new SingleConnectionRuntime({ connection: duckdb });
   const model = await runtime.loadModel(MODEL).getModel();
   modelDef = (model as unknown as { _modelDef: unknown })._modelDef;
   const p = (
      modelDef as {
         contents: Record<string, { fields: { name: string; e?: unknown }[] }>;
      }
   ).contents.p;
   fields = Object.fromEntries(p.fields.map((f) => [f.name, f]));
});

afterAll(async () => {
   await duckdb.close();
});

function pathsOf(name: string): string[] {
   const out: string[] = [];
   forEachFieldPath(fields[name].e, (path) => out.push(path.join(".")), {
      skipKeys: new Set(["at", "location"]),
      fieldrefs: true,
   });
   return out;
}

describe("forEachFieldPath", () => {
   it("re-roots an element's field under its array, never reading the row's field of that name", () => {
      expect(pathsOf("total")).toEqual(["items", "items.price"]);
   });

   it("re-roots a nested array query under its own array", () => {
      expect(pathsOf("nested")).toEqual([
         "items",
         "items.sku",
         "items.tags",
         "items.tags.w",
      ]);
   });

   it("reads an element dimension the query declares by its definition", () => {
      expect(pathsOf("declared")).toEqual(["items", "items.price"]);
   });

   it("reads a later stage as the stage before it, not the array", () => {
      expect(pathsOf("later")).toEqual(["items", "items.sku", "items.price"]);
   });

   it("leaves a read through a join inside the array query under the array, where it resolves to nothing", () => {
      expect(pathsOf("joined")).toContain("items.o.v");
   });
});

describe("arrayQueryJoins", () => {
   it("finds a join in an array query's stage and in one nested in it, and nothing else", () => {
      expect(arrayQueryJoins(fields.joined).map((j) => j.alias)).toEqual(["o"]);
      expect(arrayQueryJoins(fields.nested_join).map((j) => j.alias)).toEqual([
         "o",
      ]);
      for (const name of ["total", "nested", "declared", "later"]) {
         expect(arrayQueryJoins(fields[name])).toEqual([]);
      }
      expect(arrayQueryJoins(modelDef).length).toBe(2);
   });
});

describe("governsAccess", () => {
   it("reads the governing routes only", async () => {
      expect(governsAccess(modelDef)).toBe(false);
      const runtime = new SingleConnectionRuntime({ connection: duckdb });
      for (const [note, governs] of [
         [`#(authorize) false`, true],
         [`#(carries) ["none"]`, true],
         [`#(doc) Not a gate.`, false],
      ] as const) {
         const model = await runtime
            .loadModel(`${note}\nsource: s is duckdb.sql("select 1 as x")`)
            .getModel();
         expect(
            governsAccess(
               (model as unknown as { _modelDef: unknown })._modelDef,
            ),
         ).toBe(governs);
      }
   });
});
