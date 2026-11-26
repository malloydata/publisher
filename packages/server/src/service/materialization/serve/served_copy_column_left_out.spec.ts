// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * N-11: a query whose compile on the served copy fails for a stored column a
 * re-root leaves out is declined `MISSING_FIELD` with a detail naming why.
 * Several sources in one model may each leave out a column of the same name
 * (each with its own types): the detail names only the queried source's, and
 * a joined source's by its join path. Compiled by Malloy over DuckDB.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import type { Binding } from "../domain/binding";
import {
   columnLeftOutDecline,
   ServedCopyServe,
   sourceIDsReadThrough,
} from "./served_copy_serve";

/** The author's model: three sources, each with its own `v`. */
const AUTHOR = `
source: s_geo is duckdb.sql("SELECT 1 AS start_station_id, 1.5::DOUBLE AS v") extend {
  primary_key: start_station_id
}
source: s_names is duckdb.sql("SELECT 1 AS id, 'x' AS v")
source: s_days is duckdb.sql("SELECT 1 AS id, DATE '2026-01-01' AS v")
source: s_trips is duckdb.sql("SELECT 1 AS trip_id, 1 AS station") extend {
  join_one: geo is s_geo with station
}
`;

/** The served copy as it compiles: each `v` left out of its table's type. */
const COPY = `
source: s_geo is duckdb.sql("SELECT 1 AS start_station_id") extend {
  primary_key: start_station_id
}
source: s_names is duckdb.sql("SELECT 1 AS id")
source: s_days is duckdb.sql("SELECT 1 AS id")
source: s_trips is duckdb.sql("SELECT 1 AS trip_id, 1 AS station") extend {
  join_one: geo is s_geo with station
}
`;

const WHY: Record<string, string> = {
   s_geo: "column `v` (declared `number`, stored as VARCHAR) has no declaration that reads it back as live answers it",
   s_names:
      "column `v` (declared `string`, stored as BLOB) has no declaration that reads it back as live answers it",
   s_days: "column `v` (declared `date`) is not in the stored table",
};

describe("a decline for a stored column the served copy leaves out", () => {
   let connection: DuckDBConnection;
   let runtime: SingleConnectionRuntime;
   // One materializer each, so every compile of a model shares its sourceIDs.
   let author: ReturnType<SingleConnectionRuntime["loadModel"]>;
   let copy: ReturnType<SingleConnectionRuntime["loadModel"]>;
   let authorDef: { contents: Record<string, { sourceID?: string }> };
   let idOf: (name: string) => string;
   let compiled: Parameters<typeof columnLeftOutDecline>[0];

   /** The copy's compile failure for `query`. */
   async function copyError(query: string): Promise<unknown> {
      return copy
         .loadQuery(query)
         .getPreparedQuery()
         .then(
            () => {
               throw new Error("the copy compiled the query");
            },
            (e: unknown) => e,
         );
   }

   async function authorPrepared(query: string): Promise<unknown> {
      return author.loadQuery(query).getPreparedQuery();
   }

   beforeAll(async () => {
      connection = new DuckDBConnection("duckdb", ":memory:");
      runtime = new SingleConnectionRuntime({ connection });
      author = runtime.loadModel(AUTHOR);
      copy = runtime.loadModel(COPY);
      const model = await author.getModel();
      authorDef = (model as unknown as { _modelDef: typeof authorDef })
         ._modelDef;
      idOf = (name) => {
         const id = authorDef.contents[name]?.sourceID;
         if (!id) throw new Error(`no sourceID for ${name}`);
         return id;
      };
      const tables = ["s_geo", "s_names", "s_days"];
      compiled = {
         leftOutColumns: new Map(tables.map((t) => [idOf(t), ["v"]])),
         leftOutWhy: new Map(
            tables.map((t) => [idOf(t), new Map([["v", WHY[t]]])]),
         ),
         aliased: tables.map(
            (t) =>
               ({
                  sourceIDs: [idOf(t)],
                  virtualHandle: `${t}_table`,
               }) as unknown as Binding,
         ),
      };
   });

   afterAll(async () => {
      await connection.close();
   });

   it("names only the queried source's column, not every source's of that name", async () => {
      const query = "run: s_geo -> { select: start_station_id, v }";
      const { struct, modelDef } = await structOf(await authorPrepared(query));
      const decline = columnLeftOutDecline(
         compiled,
         await copyError(query),
         query,
         sourceIDsReadThrough(struct, modelDef),
      );
      expect(decline).toEqual({
         detail: `the stored table cannot answer with live's columns: ${WHY.s_geo}`,
         tableIds: ["s_geo_table"],
      });
   });

   it("names a joined source's column by its join path", async () => {
      const query = "run: s_trips -> { select: trip_id, geo.v }";
      const { struct, modelDef } = await structOf(await authorPrepared(query));
      const readIDs = sourceIDsReadThrough(struct, modelDef);
      expect(readIDs.get(idOf("s_trips"))).toBe("");
      expect(readIDs.get(idOf("s_geo"))).toBe("geo");
      expect(readIDs.has(idOf("s_names"))).toBe(false);
      const decline = columnLeftOutDecline(
         compiled,
         await copyError(query),
         query,
         readIDs,
      );
      expect(decline?.detail).toBe(
         "the stored table cannot answer with live's columns: " +
            "column `geo.v` (declared `number`, stored as VARCHAR) has no declaration that reads it back as live answers it",
      );
      expect(decline?.tableIds).toEqual(["s_geo_table"]);
   });

   it("finds the queried source through the author's compile", async () => {
      const query = "run: s_geo -> { select: start_station_id, v }";
      const serve = new ServedCopyServe({
         authorPreparedQuery: authorPrepared,
         modelDef: () => authorDef,
      } as never);
      const readIDs = await (
         serve as unknown as {
            queriedSourceIDs(
               c: unknown,
               q: string,
            ): Promise<ReadonlyMap<string, string> | undefined>;
         }
      ).queriedSourceIDs(compiled, query);
      expect(readIDs?.get(idOf("s_geo"))).toBe("");
      expect(readIDs?.has(idOf("s_names"))).toBe(false);
   });

   it("names every source that leaves the column out when the queried one is unknown", async () => {
      const query = "run: s_geo -> { select: start_station_id, v }";
      const decline = columnLeftOutDecline(
         compiled,
         await copyError(query),
         query,
      );
      expect(decline?.tableIds).toEqual([
         "s_geo_table",
         "s_names_table",
         "s_days_table",
      ]);
   });

   it("is not a left-out decline when the copy's failure reads no left-out column", async () => {
      const query = "run: s_geo -> { select: start_station_id, nope }";
      expect(
         columnLeftOutDecline(compiled, await copyError(query), query),
      ).toBeUndefined();
   });
});

/** The struct a prepared query runs against, and the model it resolves in. */
async function structOf(
   prepared: unknown,
): Promise<{ struct: unknown; modelDef: unknown }> {
   const { preparedQueryView, resolveStructRefSource } = await import(
      "../../governance/ir/prepared_query"
   );
   const view = preparedQueryView(prepared);
   return {
      struct: resolveStructRefSource(view.structRef, view.modelDef),
      modelDef: view.modelDef,
   };
}
