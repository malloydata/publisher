// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler contract for the column origins a `storage=` build judges each
// column's `gateExact` by: the IR shapes `columnOriginsOf` reads (a `.table()`
// source's fields, one `select:` stage's field references, a rename's `as`)
// are pinned against the compiler, so a Malloy change to any of them fails
// here rather than record a computed column as a copy.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   columnOriginsOf,
   upstreamTableKey,
   upstreamTypesOf,
} from "./upstream_schemas";

const ROOT = "file:///origins/";
let connections: FixedConnectionMap;

beforeAll(async () => {
   const db = new DuckDBConnection("db");
   await db.runSQL(
      "CREATE OR REPLACE TABLE origins_t AS SELECT 1::BIGINT AS id, 1.5::DOUBLE AS amt, 'a' AS cat",
   );
   connections = new FixedConnectionMap(new Map([["db", db]]), "db");
});

/** Each source the model declares, by name, as its compiled `SourceDef`. */
async function defs(model: string): Promise<Record<string, unknown>> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const compiled = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   return (
      compiled as unknown as {
         _modelDef: { contents: Record<string, unknown> };
      }
   )._modelDef.contents;
}

const origin = (column: string) => ({
   connectionName: "db",
   table: "origins_t",
   column,
});

describe("columnOriginsOf against the real compiler", () => {
   it("names every column of a .table() source, by its own name through a rename", async () => {
      const d = await defs(`
source: raw is db.table('origins_t') extend {
  rename: amount is amt
  dimension: twice is amount * 2
  measure: total is amount.sum()
}`);
      expect(
         [...columnOriginsOf(d.raw)].sort(([a], [b]) => a.localeCompare(b)),
      ).toEqual([
         ["amt", origin("amt")],
         ["cat", origin("cat")],
         ["id", origin("id")],
      ]);
   });

   it("follows one select: stage of plain columns, as a #(materialize) build source reads its source", async () => {
      const d = await defs(`
source: raw is db.table('origins_t') extend {
  rename: amount is amt
  dimension: twice is amount * 2
}
source: picked is raw -> { select: id, amount, twice }
source: again is picked -> { select: amount }
source: grouped is raw -> { group_by: amount }
source: named is raw -> { select: id, a2 is amount }`);
      // `twice` is computed: no origin.
      expect([...columnOriginsOf(d.picked)]).toEqual([
         ["id", origin("id")],
         ["amount", origin("amt")],
      ]);
      expect([...columnOriginsOf(d.again)]).toEqual([
         ["amount", origin("amt")],
      ]);
      // A grouping stage, and a field the stage computes, copy nothing.
      expect([...columnOriginsOf(d.grouped)]).toEqual([]);
      expect([...columnOriginsOf(d.named)]).toEqual([["id", origin("id")]]);
   });

   it("names no column of a .sql() source", async () => {
      const d = await defs(`source: s is db.sql("SELECT * FROM origins_t")`);
      expect(columnOriginsOf(d.s).size).toBe(0);
   });
});

describe("upstreamTypesOf", () => {
   const readings = {
      [upstreamTableKey({ connectionName: "db", table: "origins_t" })]: {
         dialect: "duckdb",
         columns: [
            { name: "ID", nativeType: "BIGINT" },
            { name: "amt", nativeType: "DOUBLE" },
         ],
      },
   };

   it("looks each origin up in its table's reading, exactly or case-blind", () => {
      const types = upstreamTypesOf(
         new Map([
            ["id", origin("id")],
            ["amount", origin("amt")],
            ["cat", origin("cat")],
         ]),
         readings,
      );
      expect(types && [...types]).toEqual([
         ["id", { dialect: "duckdb", nativeType: "BIGINT" }],
         ["amount", { dialect: "duckdb", nativeType: "DOUBLE" }],
      ]);
   });

   it("knows nothing of an unread table or one with no dialect", () => {
      const key = upstreamTableKey({
         connectionName: "db",
         table: "origins_t",
      });
      const origins = new Map([["amt", origin("amt")]]);
      expect(
         upstreamTypesOf(origins, { [key]: { unread: "read_failed" } }),
      ).toBeUndefined();
      expect(
         upstreamTypesOf(origins, {
            [key]: { columns: [{ name: "amt", nativeType: "DOUBLE" }] },
         }),
      ).toBeUndefined();
      expect(upstreamTypesOf(origins, undefined)).toBeUndefined();
   });
});
