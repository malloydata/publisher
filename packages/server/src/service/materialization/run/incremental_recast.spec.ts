// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { IsolatedSessionConnection } from "./materialization_build_session";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { openTempDuckDB } from "../../../test_helpers/temp_storage";
import { logger } from "../../../logger";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import { plannedSources } from "../plan/build_plan";
import type { BuildInstruction } from "../domain/types";
import type { IncrementalLineage } from "./incremental_apply";
import {
   columnsARebuildWouldRecast,
   indexCallerLedger,
   storageRefreshFor,
   type IncrementalRunContext,
} from "./incremental_build";

// ms2data/service#6642: a delta INSERTs into the stored table's columns, so a
// table an older build typed is only corrected by a re-seed's CTAS. The
// refresh asks for one when a rebuild would cast a stored column.
describe("columnsARebuildWouldRecast", () => {
   // File-backed: db-duckdb shares one in-memory instance between connections,
   // where another file's `ev` may already exist.
   const temp = openTempDuckDB("recast-", "duckdb", { file: "recast.duckdb" });
   const duckdb = temp.connection;
   let source: Awaited<ReturnType<typeof compile>>;

   async function compile(model: string) {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([["file:///m.malloy", model]]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      const compiled = await runtime
         .loadModel(new URL("file:///m.malloy"))
         .getModel();
      const [only] = plannedSources(await runtime.getBuildTargets(compiled));
      return only;
   }

   beforeAll(async () => {
      await duckdb.runSQL(
         "CREATE TABLE ev AS SELECT 1 AS id, DATE '2026-01-31' AS d, 1 AS flag_int",
      );
      source = await compile(`##! experimental { persistence sql_functions }
#@ persist name="t" storage="lake"
source: t is duckdb.table('ev') -> {
  select: id, d_next is d + 1 month, flag is sql_boolean("\${flag_int} = 1")
}
`);
   });

   afterAll(() => temp.dispose());

   it("names the columns an older build stored as another type", async () => {
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE old_t AS SELECT 1 AS id, TIMESTAMP '2026-02-28 00:00:00' AS d_next, 1 AS flag",
      );
      const recast = await columnsARebuildWouldRecast(duckdb, "old_t", source);
      expect(recast.map((m) => [m.column, m.storedType])).toEqual([
         ["d_next", "TIMESTAMP"],
         ["flag", "INTEGER"],
      ]);
   });

   it("names none for a table that stores the declared types", async () => {
      await duckdb.runSQL(
         "CREATE OR REPLACE TABLE new_t AS SELECT 1 AS id, DATE '2026-02-28' AS d_next, true AS flag",
      );
      expect(await columnsARebuildWouldRecast(duckdb, "new_t", source)).toEqual(
         [],
      );
   });

   it("names none when the table cannot be described", async () => {
      expect(
         await columnsARebuildWouldRecast(duckdb, "no_such_table", source),
      ).toEqual([]);
   });
});

// The refresh's decision, end to end through storageRefreshFor's plan: DESCRIBE
// runs against a real DuckDB table, the planner's other probes are answered.
describe("storageRefreshFor re-seeds a table only a rebuild can retype", () => {
   // File-backed: db-duckdb shares one in-memory instance between
   // connections, so an attach here would outlive this file.
   const temp = openTempDuckDB("recast-refresh-", "recast_probe", {
      file: "probe.duckdb",
   });
   const destination = temp.connection;
   const TABLE = '"lake"."orders__g000__abc"';
   const LINEAGE: IncrementalLineage = {
      physicalTableName: "orders__g000__abc",
      connectionName: "orders_pg",
      storageDestinationName: "lake",
      sourceEntityId: "addr-1",
      watermarkName: "order_date",
      watermarkType: "date",
      mergeKeys: [],
      scopeColumns: [],
      strategy: "range_replace",
   };
   const SQL = "SELECT order_date, flag, is_open FROM base";

   beforeAll(async () => {
      await destination.runSQL("ATTACH ':memory:' AS lake");
   });
   afterAll(async () => {
      await destination.runSQL("DETACH lake");
      await temp.dispose();
   });

   /**
    * A source declaring a date watermark and two booleans; with `renamed`,
    * its `flag` field is a `rename: flag_r is flag`, so its column is still
    * `flag` in the build SQL and the stored table.
    */
   function source(renamed = false) {
      const s = fakeSource({
         name: "orders",
         sourceEntityId: "addr-1",
         sql: SQL,
         connectionName: "orders_pg",
         dialectName: "postgres",
      });
      const fields: [string, string][] = [
         ["order_date", "date"],
         [renamed ? "flag_r" : "flag", "boolean"],
         ["is_open", "boolean"],
      ];
      (
         s as unknown as { _explore: { intrinsicFields: unknown[] } }
      )._explore.intrinsicFields = fields.map(([name, type]) => ({
         name,
         type,
         isAtomicField: () => true,
      }));
      if (renamed) {
         (s as unknown as { _sourceDef: unknown })._sourceDef = {
            fields: [
               { name: "order_date", type: "date" },
               { name: "flag", as: "flag_r", type: "boolean" },
               { name: "is_open", type: "boolean" },
            ],
         };
      }
      return s;
   }

   /**
    * DESCRIBE goes to the real table; the planner's column, emptiness and
    * frontier probes, and the delta's DML, are answered and recorded.
    */
   function session() {
      const issued: string[] = [];
      const s = {
         runSQL: async (sql: string) => {
            // The warehouse's own column types: none read as DOUBLE.
            if (sql.startsWith("DESCRIBE") && sql.includes("postgres_query")) {
               return { rows: [] };
            }
            if (sql.startsWith("DESCRIBE")) return destination.runSQL(sql);
            issued.push(sql);
            if (sql.includes("duckdb_columns()")) {
               return {
                  rows: ["order_date", "flag", "is_open"].map(
                     (column_name) => ({
                        column_name,
                     }),
                  ),
               };
            }
            if (sql.includes("LIMIT 1")) return { rows: [{ present: 1 }] };
            if (sql.includes("MAX(")) {
               return { rows: [{ watermark_max: "2024-06-10" }] };
            }
            return { rows: [] };
         },
      };
      return { session: s as unknown as IsolatedSessionConnection, issued };
   }

   async function refresh(
      storedAs: string,
      opts: {
         reseed?: boolean;
         forceRefresh?: boolean;
         renamed?: boolean;
      } = {},
   ) {
      await destination.runSQL(
         `CREATE OR REPLACE TABLE ${TABLE} AS SELECT ${storedAs}`,
      );
      const persistSource = source(opts.renamed);
      const instruction = {
         sourceEntityId: "addr-1",
         ...(opts.reseed ? { reseed: true } : {}),
      } as unknown as BuildInstruction;
      const context: IncrementalRunContext = {
         environmentId: "env",
         packageName: "pkg",
         materializationId: "m1",
         forceRefresh: opts.forceRefresh ?? false,
         now: new Date("2024-06-15T00:00:00Z"),
         declarations: {},
         ledger: {} as IncrementalRunContext["ledger"],
         callerLedger: indexCallerLedger([
            {
               sourceEntityId: "addr-1",
               coveredThrough: "2024-06-01",
               coveredThroughType: "date",
               watermark: "order_date",
               mergeKeys: [],
               strategy: "range_replace",
               physicalTableName: "orders__g000__abc",
               connectionName: "orders_pg",
               storageDestinationName: "lake",
            } as unknown as Parameters<typeof indexCallerLedger>[0][number],
         ]),
      };
      const warn = spyOn(logger, "warn");
      const info = spyOn(logger, "info");
      const { session: s, issued } = session();
      try {
         const result = await storageRefreshFor({
            context,
            lineage: LINEAGE,
            persistSource,
            instruction,
            destinationName: "lake",
            physicalTableName: "orders__g000__abc",
            buildSQL: SQL,
         }).plan({
            session: s,
            sourceType: "postgres",
            handle: "orders_pg_db",
            quotedTablePath: TABLE,
         });
         const seed = (warn.mock.calls as unknown[][]).find(
            ([message]) =>
               message === "Rebuilding an incremental source in full",
         );
         const recast = (info.mock.calls as unknown[][]).find(
            ([message]) =>
               message === "Re-seeding to store declared column types",
         );
         return {
            result,
            issued,
            seed: seed?.[1] as
               | { reasonCode?: string; reason?: string }
               | undefined,
            recast: recast?.[1] as
               | { columns?: { column: string; storedType: string }[] }
               | undefined,
         };
      } finally {
         warn.mockRestore();
         info.mockRestore();
      }
   }

   const DECLARED =
      "DATE '2024-06-05' AS order_date, true AS flag, false AS is_open";

   it("advances by delta over a table that stores the declared types", async () => {
      const { result, seed } = await refresh(DECLARED);
      expect(seed).toBeUndefined();
      expect(result?.refresh).toBe("delta");
   });

   // A `storage=` table stores a renamed field's column under the name the
   // build SQL emits, so the delta's DML names it the same way and applies,
   // rather than seeding on a shape mismatch every refresh.
   it("advances by delta over a source with a rename:, naming the stored column", async () => {
      const { result, seed, issued } = await refresh(DECLARED, {
         renamed: true,
      });
      expect(seed).toBeUndefined();
      expect(result?.refresh).toBe("delta");
      const insert = issued.find((sql) => sql.includes("INSERT INTO"));
      expect(insert).toContain('"flag"');
      expect(insert).not.toContain("flag_r");
   });

   it("re-seeds a table an older build typed, saying why", async () => {
      const { result, seed, recast } = await refresh(
         "TIMESTAMP '2024-06-05 00:00:00' AS order_date, 1 AS flag, false AS is_open",
      );
      // undefined: the caller rebuilds with a CTAS, which casts.
      expect(result).toBeUndefined();
      expect(seed?.reasonCode).toBe("forced");
      // The seed's log carries the code alone; the columns a rebuild retypes
      // are on the re-seed's own log.
      expect(seed).not.toHaveProperty("reason");
      expect(recast?.columns).toEqual(
         expect.arrayContaining([
            expect.objectContaining({
               column: "order_date",
               storedType: "TIMESTAMP",
            }),
            expect.objectContaining({ column: "flag", storedType: "INTEGER" }),
         ]),
      );
   });

   it("does not report a retype for a re-seed the caller forced", async () => {
      for (const opts of [{ reseed: true }, { forceRefresh: true }]) {
         const { result, seed, recast } = await refresh(
            "TIMESTAMP '2024-06-05 00:00:00' AS order_date, 1 AS flag, false AS is_open",
            opts,
         );
         expect(result).toBeUndefined();
         expect(seed?.reasonCode).toBe("forced");
         expect(recast).toBeUndefined();
      }
   });

   // What a build leaves when a cast could not convert a value: every column
   // as the warehouse returned it. A re-seed would fail the same cast.
   it("does not re-seed a table whose cast could not convert a value, run after run", async () => {
      for (let run = 0; run < 2; run++) {
         const { result, seed } = await refresh(
            "TIMESTAMP '2024-06-05 00:00:00' AS order_date, 1 AS flag, 'open' AS is_open",
         );
         expect(seed).toBeUndefined();
         expect(result?.refresh).toBe("delta");
      }
   });
});
