// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// R-13: a load's read of its upstream tables' native column types. A table
// read once keeps its reading until an explicit refresh; a failed read keeps
// the last reading, so a transient failure never moves an address; a table
// never read reads `read_failed`, stable until a read succeeds.

import { openTempDuckDB } from "../../../test_helpers/temp_storage";
import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { upstreamTableKey } from "../domain/upstream_schemas";
import { readUpstreamSchemas } from "./upstream_schema_read";

const temp = openTempDuckDB("upstream-read-", "duckdb", {
   file: "duckdb.db",
   workingDirectory: true,
});
const duckdb = temp.connection;

beforeAll(async () => {
   await duckdb.runSQL(
      "CREATE TABLE t_dec AS SELECT CAST(1.5 AS DECIMAL(10,2)) AS amt, 'a' AS cat",
   );
   await duckdb.runSQL("CREATE TABLE t_flaky AS SELECT 1 AS n");
   await duckdb.runSQL("CREATE TABLE t_cold AS SELECT 1 AS n");
});

afterAll(async () => {
   await temp.dispose();
});

const key = (table: string) =>
   upstreamTableKey({ connectionName: "duckdb", table });

/** `duckdb`, whose catalog reads fail while `failing.on`. */
function flaky(failing: { on: boolean }): Connection {
   return new Proxy(duckdb, {
      get(target, prop, receiver) {
         if (prop === "runSQL") {
            return (...args: Parameters<Connection["runSQL"]>) =>
               failing.on
                  ? Promise.reject(new Error("catalog unavailable"))
                  : target.runSQL(...args);
         }
         const value = Reflect.get(target, prop, receiver);
         return typeof value === "function" ? value.bind(target) : value;
      },
   });
}

const read = (tables: string[], connection: Connection, refresh: boolean) =>
   readUpstreamSchemas({
      tables: tables.map((table) => ({ connectionName: "duckdb", table })),
      connection: async () => connection,
      refresh,
      timeoutMs: 5_000,
   });

describe("R-13: reading the upstream tables' column types", () => {
   it("reads each column's native type, precision and scale included", async () => {
      const readings = await read(["t_dec"], duckdb, true);
      expect(readings[key("t_dec")]).toEqual({
         columns: [
            { name: "amt", nativeType: "DECIMAL(10,2)" },
            { name: "cat", nativeType: "VARCHAR" },
         ],
         dialect: "duckdb",
      });
   });

   it("keeps a table's reading until a refresh reads it again", async () => {
      await read(["t_dec"], duckdb, true);
      await duckdb.runSQL("ALTER TABLE t_dec ALTER amt TYPE DECIMAL(12,4)");
      expect(
         (await read(["t_dec"], duckdb, false))[key("t_dec")],
      ).toMatchObject({
         columns: [{ name: "amt", nativeType: "DECIMAL(10,2)" }, {}],
      });
      expect((await read(["t_dec"], duckdb, true))[key("t_dec")]).toMatchObject(
         {
            columns: [{ name: "amt", nativeType: "DECIMAL(12,4)" }, {}],
         },
      );
   });

   it("keeps the last reading when a refresh's read fails, so the address does not move", async () => {
      const failing = { on: false };
      const connection = flaky(failing);
      const first = await read(["t_flaky"], connection, true);
      failing.on = true;
      const during = await read(["t_flaky"], connection, true);
      expect(during[key("t_flaky")]).toEqual(first[key("t_flaky")]);
      expect(during[key("t_flaky")]).toEqual({
         columns: [{ name: "n", nativeType: "INTEGER" }],
         dialect: "duckdb",
      });
   });

   it("reads a failed read as `read_failed`, never its last reading, for a caller that asks", async () => {
      const failing = { on: false };
      const connection = flaky(failing);
      await read(["t_flaky"], connection, true);
      failing.on = true;
      const during = await readUpstreamSchemas({
         tables: [{ connectionName: "duckdb", table: "t_flaky" }],
         connection: async () => connection,
         refresh: true,
         timeoutMs: 5_000,
         keepLastOnFailure: false,
      });
      expect(during[key("t_flaky")]).toEqual({ unread: "read_failed" });
      // The last reading is still there for a caller that keeps it.
      expect(
         (await read(["t_flaky"], connection, true))[key("t_flaky")],
      ).toEqual({
         columns: [{ name: "n", nativeType: "INTEGER" }],
         dialect: "duckdb",
      });
   });

   it("reads a table never read as `read_failed` while its read fails, then its columns", async () => {
      const failing = { on: true };
      const connection = flaky(failing);
      expect(
         (await read(["t_cold"], connection, false))[key("t_cold")],
      ).toEqual({ unread: "read_failed" });
      // Not kept: the next load tries again.
      failing.on = false;
      expect(
         (await read(["t_cold"], connection, false))[key("t_cold")],
      ).toEqual({
         columns: [{ name: "n", nativeType: "INTEGER" }],
         dialect: "duckdb",
      });
   });

   it("retries a failed read within the load, so a passing failure reads the table", async () => {
      await duckdb.runSQL("CREATE TABLE t_retry AS SELECT 1 AS n");
      let failures = 1;
      const connection = new Proxy(duckdb, {
         get(target, prop, receiver) {
            if (prop === "runSQL") {
               return (...args: Parameters<Connection["runSQL"]>) =>
                  failures-- > 0
                     ? Promise.reject(new Error("catalog unavailable"))
                     : target.runSQL(...args);
            }
            const value = Reflect.get(target, prop, receiver);
            return typeof value === "function" ? value.bind(target) : value;
         },
      });
      expect(
         (await read(["t_retry"], connection, true))[key("t_retry")],
      ).toEqual({
         columns: [{ name: "n", nativeType: "INTEGER" }],
         dialect: "duckdb",
      });
   });

   it("reads a file as Malloy reads its schema", async () => {
      fs.writeFileSync(path.join(temp.dir, "f.csv"), "cat,amt\na,1.5\n");
      const file = path.join(temp.dir, "f.csv");
      const readings = await read([`'${file}'`], duckdb, true);
      expect(readings[key(`'${file}'`)]).toEqual({
         columns: [
            { name: "cat", nativeType: "VARCHAR" },
            { name: "amt", nativeType: "DOUBLE" },
         ],
         dialect: "duckdb",
      });
   });

   it("reads a table whose connection does not resolve as `read_failed`", async () => {
      const readings = await readUpstreamSchemas({
         tables: [{ connectionName: "gone", table: "t" }],
         connection: async () => {
            throw new Error("no such connection");
         },
         refresh: true,
      });
      expect(
         readings[upstreamTableKey({ connectionName: "gone", table: "t" })],
      ).toEqual({ unread: "read_failed" });
   });
});

describe("R-13: an upstream table's path, resolved as the warehouse resolves SQL", () => {
   /** The catalog SQL a read of `table` on `dialect` sends. */
   async function catalogSQL(dialect: string, table: string): Promise<string> {
      const sent: string[] = [];
      const connection = {
         dialectName: dialect,
         runSQL: async (sql: string) => {
            sent.push(sql);
            return { rows: [], totalRows: 0 };
         },
      } as unknown as Connection;
      // Each call a new connection object, so no reading is kept.
      await readUpstreamSchemas({
         tables: [{ connectionName: "w", table }],
         connection: async () => connection,
         refresh: true,
         timeoutMs: 5_000,
      });
      expect(sent.length).toBe(1);
      return sent[0];
   }

   it("lower-cases an unquoted Postgres part and keeps a quoted one", async () => {
      expect(await catalogSQL("postgres", "Public.Orders")).toContain(
         `to_regclass('"public"."orders"')`,
      );
      expect(await catalogSQL("postgres", 'public."Orders"')).toContain(
         `to_regclass('"public"."Orders"')`,
      );
   });

   it("upper-cases an unquoted Snowflake part and keeps a quoted one", async () => {
      const unquoted = await catalogSQL("snowflake", "analytics.Sales.orders");
      expect(unquoted).toContain(`"ANALYTICS".information_schema.columns`);
      expect(unquoted).toContain("'SALES'");
      expect(unquoted).toContain("'ORDERS'");
      const quoted = await catalogSQL(
         "snowflake",
         '"analytics"."Sales"."orders"',
      );
      expect(quoted).toContain(`"analytics".information_schema.columns`);
      expect(quoted).toContain("'Sales'");
      expect(quoted).toContain("'orders'");
   });

   it("reads a BigQuery path as written", async () => {
      const sql = await catalogSQL("standardsql", "Proj.Sales.Orders");
      expect(sql).toContain("`Proj`.`Sales`.INFORMATION_SCHEMA.COLUMNS");
      expect(sql).toContain("'Orders'");
   });
});

describe("R-13: what the catalog holds, read as a stable reading", () => {
   /** One table read on a fake `dialect` connection answering `rows`, or failing. */
   async function readingOf(
      dialect: string,
      table: string,
      rows: Record<string, unknown>[] | Error,
   ) {
      let calls = 0;
      const connection = {
         dialectName: dialect,
         runSQL: async () => {
            calls++;
            if (rows instanceof Error) throw rows;
            return { rows, totalRows: rows.length };
         },
      } as unknown as Connection;
      const readings = await readUpstreamSchemas({
         tables: [{ connectionName: "w", table }],
         connection: async () => connection,
         refresh: true,
         timeoutMs: 5_000,
      });
      return {
         reading: readings[upstreamTableKey({ connectionName: "w", table })],
         calls,
      };
   }

   it("reads a BigQuery wildcard table the catalog has no columns for as not_found, read once", async () => {
      const { reading, calls } = await readingOf(
         "standardsql",
         "proj.ds.events_*",
         [],
      );
      expect(reading).toEqual({ unread: "not_found" });
      expect(calls).toBe(1);
   });

   it("reads an unqualified Snowflake name held in two schemas as ambiguous", async () => {
      const row = (schema: string) => ({
         table_catalog: "DB",
         table_schema: schema,
         table_name: "ORDERS",
         column_name: "ID",
         data_type: "NUMBER",
      });
      const { reading } = await readingOf("snowflake", "orders", [
         row("A"),
         row("B"),
      ]);
      expect(reading).toEqual({ unread: "ambiguous" });
   });

   it("still reads a failed catalog query as read_failed, after retrying", async () => {
      const { reading, calls } = await readingOf(
         "standardsql",
         "proj.ds.events",
         new Error("catalog unavailable"),
      );
      expect(reading).toEqual({ unread: "read_failed" });
      expect(calls).toBeGreaterThan(1);
   });
});

describe("R-13: the read's one budget", () => {
   it("reads a DuckDB relation that does not exist as not_found, without retrying", async () => {
      let calls = 0;
      const connection = new Proxy(duckdb, {
         get(target, prop, receiver) {
            if (prop === "runSQL") {
               return (...args: Parameters<Connection["runSQL"]>) => {
                  calls++;
                  return target.runSQL(...args);
               };
            }
            const value = Reflect.get(target, prop, receiver);
            return typeof value === "function" ? value.bind(target) : value;
         },
      });
      expect(
         (await read(["no_such_relation"], connection, true))[
            key("no_such_relation")
         ],
      ).toEqual({ unread: "not_found" });
      expect(calls).toBe(1);
   });

   it("bounds the whole read, retries included, by one timeout", async () => {
      let calls = 0;
      const connection = {
         dialectName: "standardsql",
         runSQL: () => {
            calls++;
            // The first attempt fails, the retry never answers. The failure
            // comes well inside the budget so a loaded runner still retries.
            return calls === 1
               ? new Promise((_, reject) =>
                    setTimeout(() => reject(new Error("unavailable")), 50),
                 )
               : new Promise(() => undefined);
         },
      } as unknown as Connection;
      const started = Date.now();
      const readings = await readUpstreamSchemas({
         tables: [{ connectionName: "w", table: "p.ds.t" }],
         connection: async () => connection,
         refresh: true,
         timeoutMs: 2_000,
      });
      expect(calls).toBe(2);
      expect(
         readings[upstreamTableKey({ connectionName: "w", table: "p.ds.t" })],
      ).toEqual({ unread: "read_failed" });
      // Bounded by the one budget, retries included (with slack for a loaded runner).
      expect(Date.now() - started).toBeLessThan(4_000);
   });
});
