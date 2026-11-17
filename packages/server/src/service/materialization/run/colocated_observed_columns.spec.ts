// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   capturedColocatedSchema,
   observedColocatedColumns,
} from "./colocated_build_strategy";

/**
 * The bind's read of each colocated table's columns from its warehouse: one
 * schema read per table, never per query, spelled as the build's capture is,
 * and fail safe (`unread`) when a table cannot be read.
 */
describe("observedColocatedColumns", () => {
   let connection: DuckDBConnection;
   /** Every statement DuckDB ran, counted. */
   let statements: string[];
   beforeAll(async () => {
      connection = new DuckDBConnection(
         "observed_columns",
         path.join(
            fs.mkdtempSync(path.join(os.tmpdir(), "observed-")),
            "db.duckdb",
         ),
      );
      await connection.runSQL(
         `CREATE TABLE a AS SELECT 'east' AS region, 1.5::DECIMAL(10,1) AS amt, DATE '2026-01-01' AS d`,
      );
      await connection.runSQL(`CREATE TABLE b AS SELECT 1 AS n`);
      await connection.runSQL(`CREATE TABLE c AS SELECT true AS flag`);
      // The connection's one statement runner, counted.
      const spied = connection as unknown as {
         runDuckDBQuery: (sql: string) => Promise<unknown>;
      };
      const run = spied.runDuckDBQuery.bind(connection);
      statements = [];
      spied.runDuckDBQuery = (sql: string) => {
         statements.push(sql);
         return run(sql);
      };
   });
   afterAll(async () => {
      await connection.close();
   });

   it("reads one schema per table, spelled as the build's capture", async () => {
      const captured = await capturedColocatedSchema({
         connection,
         physicalTableName: "a",
         dialect: "duckdb",
         sourceName: "a",
      });
      statements.length = 0;
      const observed = await observedColocatedColumns({
         entries: {
            ida: { tableName: "a" },
            idb: { tableName: "b" },
            idc: { tableName: "c" },
         },
         connectionNameOf: () => "duckdb",
         connection: async () => connection,
      });
      // The cost of a bind: one query per bound table.
      expect(statements).toHaveLength(3);
      expect(observed.get("ida")).toEqual({ columns: captured! });
      expect(observed.get("idb")).toEqual({
         columns: [{ name: "n", type: "BIGINT" }],
      });
   });

   it("reads the table as it is now, past the connection's schema cache", async () => {
      const read = () =>
         observedColocatedColumns({
            entries: { idc: { tableName: "c" } },
            connectionNameOf: () => "duckdb",
            connection: async () => connection,
         });
      expect((await read()).get("idc")).toEqual({
         columns: [{ name: "flag", type: "BOOLEAN" }],
      });
      await connection.runSQL(`ALTER TABLE c ALTER flag TYPE INTEGER`);
      expect((await read()).get("idc")).toEqual({
         columns: [{ name: "flag", type: "BIGINT" }],
      });
   });

   it("a table that cannot be read, or whose connection is unknown, is unread", async () => {
      const observed = await observedColocatedColumns({
         entries: {
            gone: { tableName: "no_such_table" },
            nowhere: { tableName: "a" },
            broken: { tableName: "a", connectionName: "broken" },
         },
         connectionNameOf: (id) =>
            id === "nowhere"
               ? undefined
               : id === "broken"
                 ? "broken"
                 : "duckdb",
         connection: async (name) => {
            if (name === "broken") throw new Error("no such connection");
            return connection;
         },
      });
      expect(Object.keys(observed.get("gone")!)).toEqual(["unread"]);
      expect(observed.get("nowhere")).toEqual({
         unread: "the table's connection is not known",
      });
      expect(observed.get("broken")).toEqual({ unread: "no such connection" });
   });
});
