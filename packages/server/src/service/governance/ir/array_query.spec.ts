// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { isSourceDef } from "@malloydata/malloy";
import {
   arrayQueryJoinRefusal,
   arrayQueryJoins,
   forEachFieldPath,
   governsAccess,
   type ArrayQueryJoin,
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

/**
 * The walk as malloydata/publisher#1744 shipped it, memo and all (a fresh
 * memo per call, which is what a freshly hydrated ModelDef saw): the oracle
 * for the single-visit walk, which must find the same joins in the same order.
 */
function shippedArrayQueryJoins(root: unknown): ArrayQueryJoin[] {
   type O = Record<string, unknown>;
   const isObj = (v: unknown): v is O => v !== null && typeof v === "object";
   const NOT_IR = new Set(["at", "location", "annotations"]);
   const memo = new WeakMap<object, ArrayQueryJoin[]>();
   const locationOf = (f: O): ArrayQueryJoin["at"] => {
      const loc = isObj(f.location) ? f.location : undefined;
      if (!loc) return undefined;
      const range = isObj(loc.range) ? loc.range : undefined;
      const start = range && isObj(range.start) ? range.start : undefined;
      return {
         ...(typeof loc.url === "string" ? { url: loc.url } : {}),
         ...(typeof start?.line === "number" ? { line: start.line + 1 } : {}),
      };
   };
   const joinsInside = (arrayQuery: O): ArrayQueryJoin[] => {
      const found: ArrayQueryJoin[] = [];
      const seen = new Set<object>();
      const walk = (v: unknown): void => {
         if (!isObj(v) || seen.has(v)) return;
         seen.add(v);
         if (Array.isArray(v.extendSource)) {
            for (const f of v.extendSource) {
               if (isObj(f) && f.join && isSourceDef(f as never)) {
                  const alias = typeof f.as === "string" ? f.as : f.name;
                  const at = locationOf(f);
                  found.push({
                     alias: typeof alias === "string" ? alias : "",
                     ...(at ? { at } : {}),
                  });
               }
            }
         }
         for (const [k, child] of Object.entries(v)) {
            if (!NOT_IR.has(k)) walk(child);
         }
      };
      walk(arrayQuery);
      return found;
   };
   const inProgress = new Set<object>();
   const walk = (v: unknown): ArrayQueryJoin[] => {
      if (!isObj(v)) return [];
      const hit = memo.get(v);
      if (hit) return hit;
      if (inProgress.has(v)) return [];
      inProgress.add(v);
      let found: ArrayQueryJoin[] = [];
      if (v.node === "arrayQuery") {
         found = joinsInside(v);
      } else {
         for (const [k, child] of Object.entries(v)) {
            if (NOT_IR.has(k)) continue;
            const under = walk(child);
            if (under.length > 0) found = [...found, ...under];
         }
      }
      inProgress.delete(v);
      memo.set(v, found);
      return found;
   };
   return walk(root);
}

/** Each join once, in first-reached order, compared by value. */
function distinct(joins: ArrayQueryJoin[]): string[] {
   return Array.from(new Set(joins.map((j) => JSON.stringify(j))));
}

describe("arrayQueryJoins against the walk it replaced", () => {
   /** Joins reached by many paths: views, a query and an extending source. */
   const SHARED = `${MODEL}
source: q is p extend {
  view: v1 is { group_by: joined, nested_join }
  view: v2 is { group_by: joined; nest: v1 }
}
query: r is q -> v2
source: s is q extend { dimension: again is items -> { extend: { join_one: o2 is other on sku = o2.k } aggregate: s2 is o2.v.sum() } }
`;

   async function compiled(text: string): Promise<unknown> {
      const runtime = new SingleConnectionRuntime({ connection: duckdb });
      const model = await runtime.loadModel(text).getModel();
      return (model as unknown as { _modelDef: unknown })._modelDef;
   }

   it("finds the same joins in the same order, governed or not, compiled or structured-cloned", async () => {
      let sawSharing = false;
      for (const text of [
         MODEL,
         SHARED,
         `${SHARED}\n#(authorize) false\nsource: g is duckdb.sql("select 1 as x")\n`,
         `${SHARED}\n#(carries) ["none"]\nsource: g is duckdb.sql("select 1 as x")\n`,
      ]) {
         const def = await compiled(text);
         // A structured clone keeps the aliasing, as the load worker's does.
         for (const root of [def, structuredClone(def)]) {
            const shipped = shippedArrayQueryJoins(root);
            const now = arrayQueryJoins(root);
            expect(now.length).toBeGreaterThan(0);
            expect(now.map((j) => JSON.stringify(j))).toEqual(
               distinct(shipped),
            );
            expect(arrayQueryJoinRefusal("Model m", now)).toBe(
               arrayQueryJoinRefusal("Model m", shipped) as string,
            );
            if (shipped.length > now.length) sawSharing = true;
         }
      }
      // The fixture reaches some join by two paths, so "once" was exercised.
      expect(sawSharing).toBe(true);
   });

   it("finds nothing where the model holds no array query join", async () => {
      const def = await compiled(
         `source: plain is duckdb.sql("select 1 as x") extend { view: v is { group_by: x } }`,
      );
      expect(arrayQueryJoins(def)).toEqual([]);
      expect(shippedArrayQueryJoins(def)).toEqual([]);
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
