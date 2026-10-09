// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import { storageDestinationRoot } from "./connection_config";
import {
   cleanupStorageFiles,
   dropStorageTable,
   listStorageTables,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

const LAKE: ApiConnection = { name: "lake", type: "duckdb" } as ApiConnection;

/**
 * A plain DuckDB destination: the same list/drop code path a DuckLake one takes
 * once attached, with a database file standing in for the catalog. DuckLake's
 * own maintenance calls need a real catalog and are exercised end to end by the
 * hammer scenario instead.
 */
describe("storage destination tables (plain DuckDB destination)", () => {
   let envPath: string;
   let dbPath: string;

   beforeEach(() => {
      envPath = mkdtempSync(join(tmpdir(), "dest-tables-"));
      mkdirSync(storageDestinationRoot(envPath), { recursive: true });
      dbPath = join(storageDestinationRoot(envPath), "lake.duckdb");
   });
   afterEach(() => rmSync(envPath, { recursive: true, force: true }));

   async function seed(sql: string): Promise<void> {
      const instance = await DuckDBInstance.create(dbPath);
      const connection = await instance.connect();
      try {
         await connection.run(sql);
      } finally {
         connection.closeSync();
         instance.closeSync();
      }
   }

   const list = (schemaName = "main") =>
      listStorageTables({
         destinationName: "lake",
         destinationConnection: LAKE,
         schemaName,
         environmentPath: envPath,
      });
   const drop = (physicalTableName: string) =>
      dropStorageTable({
         destinationName: "lake",
         destinationConnection: LAKE,
         physicalTableName,
         environmentPath: envPath,
      });

   it("reports a destination that was never written as empty, without creating it", async () => {
      expect(await list()).toEqual([]);
      expect(await Bun.file(dbPath).exists()).toBe(false);
   });

   it("lists one schema's tables in name order", async () => {
      await seed(
         "CREATE TABLE b_daily__g001 AS SELECT 1 AS x; " +
            "CREATE TABLE a_daily__g001 AS SELECT 1 AS x; " +
            "CREATE SCHEMA analytics; " +
            "CREATE TABLE analytics.other AS SELECT 1 AS x;",
      );
      expect(await list()).toEqual(["a_daily__g001", "b_daily__g001"]);
      expect(await list("analytics")).toEqual(["other"]);
      expect(await list("absent")).toEqual([]);
   });

   it("drops exactly the named table, and a repeated drop is a no-op", async () => {
      await seed(
         "CREATE TABLE daily__g001 AS SELECT 1 AS x; " +
            "CREATE TABLE daily__g002 AS SELECT 2 AS x;",
      );
      await drop("daily__g001");
      expect(await list()).toEqual(["daily__g002"]);
      await drop("daily__g001");
      expect(await list()).toEqual(["daily__g002"]);
   });

   it("drops a table in a named schema by its qualified name", async () => {
      await seed(
         "CREATE SCHEMA analytics; " +
            "CREATE TABLE analytics.daily AS SELECT 1 AS x; " +
            "CREATE TABLE daily AS SELECT 1 AS x;",
      );
      await drop("analytics.daily");
      expect(await list("analytics")).toEqual([]);
      expect(await list()).toEqual(["daily"]);
   });

   it("refuses a file cleanup for a destination that is not DuckLake", async () => {
      const cleanup = cleanupStorageFiles({
         destinationName: "lake",
         destinationConnection: LAKE,
         snapshotsOlderThan: new Date(),
         filesOlderThan: new Date(),
      });
      await expect(cleanup).rejects.toBeInstanceOf(BadRequestError);
      await expect(cleanup).rejects.toThrow(/only to 'ducklake'/);
   });
});
