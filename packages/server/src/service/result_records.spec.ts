// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A record whose row omits a key its type declares, as a Snowflake OBJECT
// does (Malloy infers its keys by sampling; OBJECT_CONSTRUCT drops NULL
// keys). DuckDB's structs always carry every key, so each test runs a real
// query and then removes keys from the rows, as a Snowflake row arrives.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime, type Result } from "@malloydata/malloy";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { afterAll, describe, expect, it, spyOn } from "bun:test";
import { internalErrorToHttpError, ResultShapeError } from "../errors";
import {
   loadAuthorizeModel,
   type LoadedAuthorizeModel,
   queryRows,
} from "../test_helpers/authorize_fixture";
import { mapRunError, serializeWithinCaps } from "./query_execution";

const MODEL = `
source: t is duckdb.sql("""
  select
    {'n': 1, 's': 'a', 'inner': {'x': 2, 'y': 3}} as obj,
    [{'k': 4, 'm': 5}] as arr,
    [[{'k': 6, 'm': 7}]] as nested_arr
""")
`;

const connection = new DuckDBConnection("duckdb", ":memory:");
const model = new SingleConnectionRuntime({ connection } as never).loadModel(
   MODEL,
);
afterAll(() => connection.close());

const CAPS = {
   maxRows: 100,
   maxBytes: 1_000_000,
   source: "model_query" as const,
};

async function run(): Promise<{
   result: Result;
   row: Record<string, unknown>;
}> {
   const result = await model.loadQuery("run: t -> { select: * }").run();
   return {
      result,
      row: result.data.rawData[0] as unknown as Record<string, unknown>,
   };
}

describe("a record whose row omits a declared key", () => {
   it("reads the key as null, at the top level and nested", async () => {
      const { result, row } = await run();
      const obj = row.obj as Record<string, unknown>;
      delete obj.n;
      delete (obj.inner as Record<string, unknown>).y;
      const { wrapped } = serializeWithinCaps(result, CAPS);
      const rows = wrapped.data as Malloy.CellWithArrayCell;
      const objIndex = wrapped.schema.fields.findIndex((f) => f.name === "obj");
      const objCell = (rows.array_value[0] as Malloy.CellWithRecordCell)
         .record_value[objIndex] as Malloy.CellWithRecordCell;
      expect(objCell.record_value[0]).toEqual({ kind: "null_cell" });
      expect(
         (objCell.record_value[2] as Malloy.CellWithRecordCell).record_value[1],
      ).toEqual({ kind: "null_cell" });
      expect(obj).toEqual({ n: null, s: "a", inner: { x: 2, y: null } });
   });

   it("reads the key as null in a record inside an array, and an array of arrays", async () => {
      const { result, row } = await run();
      delete (row.arr as Record<string, unknown>[])[0].m;
      delete (row.nested_arr as Record<string, unknown>[][])[0][0].k;
      serializeWithinCaps(result, CAPS);
      expect(row.arr).toEqual([{ k: 4, m: null }]);
      expect(row.nested_arr).toEqual([[{ k: null, m: 7 }]]);
   });

   it("carries the key into a compact answer too", async () => {
      const { result, row } = await run();
      delete (row.obj as Record<string, unknown>).s;
      const { serialized } = serializeWithinCaps(result, {
         ...CAPS,
         compact: true,
      });
      expect(JSON.parse(serialized)[0].obj).toEqual({
         n: 1,
         s: null,
         inner: { x: 2, y: 3 },
      });
   });
});

describe("a row the result cannot hold", () => {
   it("is a 502 with a server-authored message, through mapRunError", async () => {
      const { result, row } = await run();
      // A string field holding a number: Malloy's wrapper throws on it.
      (row.obj as Record<string, unknown>).s = 5;
      let thrown: unknown;
      try {
         serializeWithinCaps(result, CAPS);
      } catch (err) {
         thrown = err;
      }
      expect(thrown).toBeInstanceOf(ResultShapeError);
      let mapped: unknown;
      try {
         mapRunError(thrown, {
            packageName: "p",
            modelPath: "m.malloy",
            failurePrefix: "Query execution failed",
            passThrough: [],
            logFailure: () => {},
         });
      } catch (err) {
         mapped = err;
      }
      expect(mapped).toBe(thrown);
      const answer = internalErrorToHttpError(mapped as Error, { log: false });
      expect(answer.status).toBe(502);
      expect(answer.json.message).not.toContain("Invalid string");
      expect(answer.json.message).toContain("does not fit the result's schema");
   });
});

describe("a model query whose database row does not fit", () => {
   /** The model, with each row the database returns passed through `edit`. */
   async function withEditedRows<T>(
      edit: (row: Record<string, Record<string, unknown>>) => void,
      body: (loaded: LoadedAuthorizeModel) => Promise<T>,
   ): Promise<T> {
      const loaded = await loadAuthorizeModel(
         `source: t is duckdb.sql("""select {'n': 1, 's': 'a'} as obj""")`,
      );
      const runSQL = loaded.duckdb.runSQL.bind(loaded.duckdb);
      const spy = spyOn(loaded.duckdb, "runSQL").mockImplementation(
         async (...args: Parameters<typeof runSQL>) => {
            const data = await runSQL(...args);
            for (const row of data.rows) edit(row as never);
            return data;
         },
      );
      try {
         expect(loaded.error).toBeUndefined();
         return await body(loaded);
      } finally {
         spy.mockRestore();
         await loaded.cleanup();
      }
   }

   it("answers with the omitted key as null", async () => {
      const rows = await withEditedRows(
         (row) => delete row.obj.n,
         ({ model }) => queryRows(model, "run: t -> { select: obj }"),
      );
      expect(rows).toEqual([{ obj: { n: null, s: "a" } }]);
   });

   it("is a 502, not an unclassified 500, when a value has the wrong type", async () => {
      let thrown: unknown;
      await withEditedRows(
         (row) => {
            row.obj.s = 5;
         },
         async ({ model }) => {
            try {
               await queryRows(model, "run: t -> { select: obj }");
            } catch (err) {
               thrown = err;
            }
         },
      );
      expect(thrown).toBeInstanceOf(ResultShapeError);
      expect(
         internalErrorToHttpError(thrown as Error, { log: false }).status,
      ).toBe(502);
   });
});
