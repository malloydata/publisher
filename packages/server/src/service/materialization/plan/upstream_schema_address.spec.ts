// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// R-13: the content address folds in the native column types of the tables
// a source's build reads, so a column retyped upstream (whose SQL is
// unchanged) moves the address and the table is rebuilt rather than carried
// or reused. A source whose tables cannot be known keeps its SQL's address.

import { openTempDuckDB } from "../../../test_helpers/temp_storage";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   computeSourceEntityId,
   plannedSources,
   upstreamSchemasOfSource,
   upstreamTablesOf,
} from "./addressing";
import {
   type UpstreamSchemaReadings,
   upstreamTableKey,
} from "../domain/upstream_schemas";

const MODEL = `##! experimental.persistence
source: t is duckdb.table('t')

#@ persist name="by_cat"
source: by_cat is t -> { group_by: cat; aggregate: s is amt.sum() }

#@ persist name="from_sql"
source: from_sql is duckdb.sql("SELECT cat, amt FROM t") -> { group_by: cat; aggregate: s is amt.sum() }
`;

const temp = openTempDuckDB("upstream-address-", "duckdb", {
   file: "duckdb.db",
});
let sources: Record<string, PersistSource>;
let digests: Record<string, string>;

beforeAll(async () => {
   await temp.connection.runSQL(
      "CREATE TABLE t AS SELECT * FROM (VALUES ('a', 1), ('b', 2)) AS x(cat, amt)",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([["file:///m.malloy", MODEL]])),
      connections: new FixedConnectionMap(
         new Map([["duckdb", temp.connection]]),
         "duckdb",
      ),
   });
   const model = await runtime
      .loadModel(new URL("file:///m.malloy"))
      .getModel();
   sources = Object.fromEntries(
      plannedSources(await runtime.getBuildTargets(model)).map((s) => [
         s.name,
         s,
      ]),
   );
   digests = { duckdb: await temp.connection.getDigest() };
});

afterAll(async () => {
   await temp.dispose();
});

const T = upstreamTableKey({ connectionName: "duckdb", table: "t" });

/** Readings of `t` with `amt` as `amtType`. */
function readingsWith(amtType: string): UpstreamSchemaReadings {
   return {
      [T]: {
         columns: [
            { name: "cat", nativeType: "VARCHAR" },
            { name: "amt", nativeType: amtType },
         ],
      },
   };
}

const address = (name: string, readings?: UpstreamSchemaReadings) =>
   computeSourceEntityId(
      sources[name],
      digests,
      undefined,
      undefined,
      readings,
   );

describe("R-13: the upstream column types in the content address", () => {
   it("reads the table the source's build reads", () => {
      expect(upstreamTablesOf(sources.by_cat)).toEqual([
         { connectionName: "duckdb", table: "t" },
      ]);
   });

   it("moves the address when a column is retyped, the SQL unchanged", () => {
      expect(address("by_cat", readingsWith("INTEGER"))).not.toBe(
         address("by_cat", readingsWith("DOUBLE")),
      );
   });

   it("moves it on a precision change Malloy's own types do not see", () => {
      expect(address("by_cat", readingsWith("DECIMAL(10,2)"))).not.toBe(
         address("by_cat", readingsWith("DECIMAL(12,4)")),
      );
   });

   it("keeps it for the same columns, whatever their order or the type's spacing and case", () => {
      const reordered: UpstreamSchemaReadings = {
         [T]: {
            columns: [
               { name: "amt", nativeType: "decimal( 10, 2 )" },
               { name: "cat", nativeType: "varchar" },
            ],
         },
      };
      expect(address("by_cat", reordered)).toBe(
         address("by_cat", readingsWith("DECIMAL(10,2)")),
      );
   });

   it("keeps it when a column its SQL does not name is added, dropped or retyped", () => {
      const base = readingsWith("INTEGER");
      const withExtra: UpstreamSchemaReadings = {
         [T]: {
            columns: [
               ...("columns" in base[T] ? base[T].columns : []),
               { name: "note", nativeType: "VARCHAR" },
            ],
         },
      };
      const retypedExtra: UpstreamSchemaReadings = {
         [T]: {
            columns: [
               ...("columns" in base[T] ? base[T].columns : []),
               { name: "note", nativeType: "INTEGER" },
            ],
         },
      };
      expect(address("by_cat", withExtra)).toBe(address("by_cat", base));
      expect(address("by_cat", retypedExtra)).toBe(address("by_cat", base));
      // Dropping a column it names moves it.
      const dropped: UpstreamSchemaReadings = {
         [T]: { columns: [{ name: "cat", nativeType: "VARCHAR" }] },
      };
      expect(address("by_cat", dropped)).not.toBe(address("by_cat", base));
   });

   it("folds a table that could not be read as a stable value, apart from any read", () => {
      const failed: UpstreamSchemaReadings = { [T]: { unread: "read_failed" } };
      expect(address("by_cat", failed)).toBe(address("by_cat", failed));
      expect(address("by_cat", failed)).not.toBe(
         address("by_cat", readingsWith("INTEGER")),
      );
      expect(address("by_cat", failed)).not.toBe(address("by_cat"));
   });

   it("keeps the SQL's address with no readings (a plan read none)", () => {
      expect(address("by_cat", {})).toBe(address("by_cat"));
      expect(address("by_cat", undefined)).toBe(address("by_cat"));
   });

   it("keeps the SQL's address for a source whose tables cannot be known (`.sql()`)", () => {
      expect(upstreamTablesOf(sources.from_sql)).toBeUndefined();
      expect(address("from_sql", readingsWith("DOUBLE"))).toBe(
         address("from_sql"),
      );
      expect(
         upstreamSchemasOfSource(sources.from_sql, readingsWith("DOUBLE")),
      ).toBeUndefined();
   });

   it("records the readings it folded, as a manifest entry carries them", () => {
      expect(
         upstreamSchemasOfSource(sources.by_cat, readingsWith("DOUBLE")),
      ).toEqual([
         {
            connectionName: "duckdb",
            table: "t",
            columns: [
               { name: "cat", nativeType: "VARCHAR" },
               { name: "amt", nativeType: "DOUBLE" },
            ],
         },
      ]);
   });
});
