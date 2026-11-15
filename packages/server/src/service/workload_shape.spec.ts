// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelMaterializer,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { readPreparedQuery } from "./governance/ir/prepared_query";
import { canonicalExpression, workloadShapeOf } from "./workload_shape";

const MODEL = `##! experimental { sql_functions }
source: cust is duckdb.table('c') extend { primary_key: id }
source: orders is duckdb.table('o') extend {
  join_one: cust with cid
  measure: total is amt.sum()
  dimension: big is amt > 2
  view: by_region is { group_by: region; aggregate: total }
}
`;

describe("workloadShapeOf", () => {
   const duckdb = new DuckDBConnection("duckdb");
   let model: ModelMaterializer;

   beforeAll(async () => {
      await duckdb.runSQL(
         "CREATE TABLE o AS SELECT 1 AS id, 'west' AS region, DATE '2026-01-01' AS dt, 2.5 AS amt, 1 AS cid",
      );
      await duckdb.runSQL("CREATE TABLE c AS SELECT 1 AS id, 'x' AS name");
      model = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///m.malloy", MODEL]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      }).loadModel(new URL("file:///m.malloy"));
   });

   afterAll(async () => {
      await duckdb.close();
   });

   async function shapeOf(query: string, view?: string) {
      const prepared = await readPreparedQuery(model.loadQuery(query));
      return workloadShapeOf({
         query: prepared.query,
         modelDef: prepared.modelDef,
         source: "orders",
         view,
      });
   }

   it("records dimensions, measures, filters and the time zone, and no value", async () => {
      const shape = await shapeOf(
         `run: orders -> {
            timezone: 'America/New_York'
            where: region = 'west' and cust.name ~ 'x%'
            group_by: dt.month, cust.name, big
            aggregate: total, n is count(), a is amt.avg()
            having: total > 100
            limit: 5
         }`,
      );
      expect(shape).toEqual({
         key: expect.stringMatching(/^[0-9a-f]{16}$/),
         source: "orders",
         kind: "reduce",
         stages: 1,
         dimensions: ["big", "cust.name", "trunc:month(dt)"],
         measures: ["aggregate:avg(amt)", "aggregate:count()", "total"],
         filteredDimensions: ["cust.name", "region"],
         filters: [">(total,?)", "and(=(region,?),like(cust.name,?))"],
         timezone: "America/New_York",
         complete: true,
      });
      const text = JSON.stringify(shape);
      for (const literal of ["west", "x%", "100", "'5'"]) {
         expect(text).not.toContain(literal);
      }
   });

   it("gives one key to runs that differ only in their values, aliases, order or limit", async () => {
      const a = await shapeOf(
         "run: orders -> { where: region = 'west'; group_by: region; aggregate: n is count(), total; limit: 5 }",
      );
      const b = await shapeOf(
         "run: orders -> { where: region = 'east'; group_by: region; aggregate: total, rows is count(); order_by: region; limit: 50 }",
      );
      expect(a?.key).toBe(b?.key);
   });

   it("gives another key to another question", async () => {
      const base = await shapeOf(
         "run: orders -> { group_by: region; aggregate: total }",
      );
      for (const other of [
         "run: orders -> { group_by: dt.month; aggregate: total }",
         "run: orders -> { group_by: region; aggregate: n is count() }",
         "run: orders -> { where: big; group_by: region; aggregate: total }",
         "run: orders -> { timezone: 'UTC'; group_by: region; aggregate: total }",
         "run: orders -> { group_by: region; aggregate: total } -> { aggregate: s is total.sum() }",
      ]) {
         expect((await shapeOf(other))?.key).not.toBe(base?.key);
      }
   });

   it("names a view the request named, and keys it with the same fields asked directly", async () => {
      const viaQueryName = await shapeOf(
         "run: orders -> by_region",
         "by_region",
      );
      const viaRun = await shapeOf("run: orders -> by_region");
      const direct = await shapeOf(
         "run: orders -> { group_by: region; aggregate: total }",
      );
      expect(viaQueryName).toMatchObject({
         view: "by_region",
         dimensions: ["region"],
         measures: ["total"],
      });
      // However the request was phrased, one compiled stage is one shape.
      expect(viaQueryName?.key).toBe(viaRun?.key);
      expect(viaQueryName?.key).toBe(direct?.key);
   });

   it("gives one key to a multi-select however many values it has", async () => {
      const two = await shapeOf(
         "run: orders -> { where: region ? 'a' | 'b'; aggregate: total }",
      );
      const three = await shapeOf(
         "run: orders -> { where: region ? 'a' | 'b' | 'c'; aggregate: total }",
      );
      expect(two?.key).toBe(three?.key);
   });

   it("flags a nested named view, and does not count it as a dimension", async () => {
      const shape = await shapeOf(
         "run: orders -> { group_by: dt; nest: by_region }",
      );
      expect(shape).toMatchObject({
         dimensions: ["dt"],
         nested: true,
         complete: true,
      });
   });

   it("flags a nest, and keeps only the outer stage", async () => {
      const shape = await shapeOf(
         "run: orders -> { group_by: region; nest: by_month is { group_by: dt.month; aggregate: total } }",
      );
      expect(shape).toMatchObject({ dimensions: ["region"], nested: true });
   });

   it("records a select as a project stage", async () => {
      const shape = await shapeOf("run: orders -> { select: id, region }");
      expect(shape).toMatchObject({
         kind: "project",
         dimensions: ["id", "region"],
         measures: [],
      });
   });

   it("is undefined without a pipeline", () => {
      expect(
         workloadShapeOf({ query: { pipeline: [] }, modelDef: undefined }),
      ).toBeUndefined();
   });
});

describe("canonicalExpression", () => {
   it("replaces every value, and keeps structure, functions and units", () => {
      expect(
         canonicalExpression({
            node: "and",
            kids: {
               left: {
                  node: "=",
                  kids: {
                     left: { node: "field", path: ["region"] },
                     right: { node: "stringLiteral", literal: "west" },
                  },
               },
               right: {
                  node: "trunc",
                  units: "month",
                  e: { node: "field", path: ["dt"] },
               },
            },
         }),
      ).toBe("and(=(region,?),trunc:month(dt))");
   });

   it("never keeps a string that is not structure, such as SQL text", () => {
      const text = canonicalExpression({
         node: "genericSQLExpr",
         src: ["CASE WHEN x = 'secret' THEN 1 END"],
         kids: { args: [{ node: "field", path: ["x"] }] },
      });
      expect(text).not.toContain("secret");
      expect(text).toContain("x");
   });

   it("collapses a list of values to one ?, and keeps `not in` apart from `in`", () => {
      const inList = (n: number, not = false) =>
         canonicalExpression({
            node: "in",
            not,
            kids: {
               e: { node: "field", path: ["region"] },
               oneOf: Array.from({ length: n }, (_, i) => ({
                  node: "stringLiteral",
                  literal: `v${i}`,
               })),
            },
         });
      expect(inList(2)).toBe(inList(5));
      expect(inList(2)).toBe("in(region,[?])");
      expect(inList(2, true)).toBe("in:not(region,[?])");
   });

   it("renders a function call by its name and arguments, never its overload, and keeps its partition", () => {
      const call = (dialectSql: string) =>
         canonicalExpression({
            node: "function_call",
            name: "rank",
            overload: {
               returnType: { type: "number" },
               dialect: {
                  duckdb: { e: { node: "genericSQLExpr", src: [dialectSql] } },
               },
            },
            kids: { args: [{ node: "field", path: ["amt"] }] },
            partitionBy: ["region"],
            structPath: ["orders"],
         });
      expect(call("RANK()")).toBe(call("RANK() OVER ()"));
      expect(call("RANK()")).toContain("rank");
      expect(call("RANK()")).toContain("partitionBy=[region]");
      expect(call("RANK()")).not.toContain("RANK");
   });

   it("renders a node the compiler shares between branches in both", () => {
      const x = { node: "field", path: ["x"] };
      expect(
         canonicalExpression({
            node: "and",
            kids: {
               left: {
                  node: ">=",
                  kids: {
                     left: x,
                     right: { node: "numberLiteral", literal: "1" },
                  },
               },
               right: {
                  node: "<",
                  kids: {
                     left: x,
                     right: { node: "numberLiteral", literal: "5" },
                  },
               },
            },
         }),
      ).toBe("and(>=(x,?),<(x,?))");
   });

   it("still ends on a true cycle", () => {
      const loop: Record<string, unknown> = { node: "+", kids: {} };
      (loop.kids as Record<string, unknown>).left = loop;
      expect(() => canonicalExpression(loop)).not.toThrow();
   });
});
