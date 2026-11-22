// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "path";
import type { components } from "../../../api";
import { installSettings } from "../../../test_helpers/settings_env";
import * as connection from "../../connection";
import { storageDestinationRoot } from "../../connection_config";
import {
   assertSupportedDestination,
   buildSourceIntoStorage,
   requireDuckLakeConfig,
} from "./materialization_build_session";
import { cleanupStorageFiles, listStorageTables } from "./storage_admin";
import { storageSourceConnection } from "./storage_build_strategy";
import { copyStorageTable, loadStorageTable } from "./storage_table_load";

/**
 * A storage destination's handling by its connection type (whether it is
 * one, how it is attached for a build, a listing, a file cleanup, a load and
 * a copy, the settings a build session runs with, and which source a
 * destination builds from), run for every connection type and for types
 * nothing names, and compared with what the code produced before those
 * branches read the registry's `destination`, recorded in
 * storage_destination.golden.json: each outcome or refusal, the catalog
 * attaches each made (DuckLake's, stood in for by an in-memory database),
 * and the statements a build session sent on one.
 *
 * Re-record only for a reviewed behaviour change:
 * `RECORD_STORAGE_DESTINATION=1 bun test <this file>`.
 */

type ApiConnection = components["schemas"]["Connection"];

const GOLDEN = path.join(__dirname, "storage_destination.golden.json");

const TYPES = [
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
   "constructor",
   "nope",
];

/** A local data path, fixed so the statements naming it are comparable. */
const DATA_PATH = path.join(tmpdir(), "storage-destination-conservation/");

const DUCKLAKE_CONFIG = {
   catalog: {
      postgresConnection: { host: "pg", port: 5432, databaseName: "cat" },
   },
   storage: { bucketUrl: DATA_PATH },
};

/** A connection of `type`, with (or without) a DuckLake catalog config. */
function connectionOf(type: string, withConfig = true): ApiConnection {
   return {
      name: "lake",
      type,
      ...(withConfig ? { ducklakeConnection: DUCKLAKE_CONFIG } : {}),
   } as unknown as ApiConnection;
}

const normalize = (text: string) =>
   text
      .split(DATA_PATH)
      .join("<data>/")
      .replace(/(\/private)?\/(var|tmp)\/[^'"\s)\\]*/g, "<tmp>");

async function outcome(run: () => unknown): Promise<unknown> {
   try {
      const value = await run();
      return { value: value === undefined ? "undefined" : value };
   } catch (error) {
      return {
         error: normalize(
            error instanceof Error ? error.message : String(error),
         ),
         name: error instanceof Error ? error.constructor.name : typeof error,
      };
   }
}

/** Each catalog attach, and every statement a session sent after one. */
let attaches: string[] = [];
let statements: string[] = [];
let dir: string;
let spies: { mockRestore(): void }[] = [];

/** DuckLake's attach, as an in-memory database that records what follows. */
async function standIn(
   session: DuckDBConnection,
   dbName: string,
   label: string,
): Promise<void> {
   attaches.push(`${label} ${dbName}`);
   await session.runSQL(`ATTACH ':memory:' AS "${dbName}"`);
   const real = session.runSQL.bind(session);
   session.runSQL = (async (sql: string, ...rest: unknown[]) => {
      statements.push(normalize(sql));
      // The DuckLake settings the stand-in has no extension for.
      if (/^SET ducklake_/.test(sql) || /ducklake_/.test(sql)) {
         return { rows: [] };
      }
      return (real as (...a: unknown[]) => Promise<unknown>)(sql, ...rest);
   }) as typeof session.runSQL;
}

let restoreSettings: () => void;
beforeAll(() => {
   restoreSettings = installSettings().restore;
   mkdirSync(DATA_PATH, { recursive: true });
   fs.writeFileSync(path.join(DATA_PATH, "in.csv"), "x\n1\n");
});
afterAll(() => {
   restoreSettings();
   rmSync(DATA_PATH, { recursive: true, force: true });
});

beforeEach(() => {
   dir = mkdtempSync(path.join(tmpdir(), "storage-destination-"));
   mkdirSync(storageDestinationRoot(dir), { recursive: true });
   attaches = [];
   statements = [];
   spies = [
      spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
         async (session, dbName) =>
            standIn(session as DuckDBConnection, dbName, "read-write"),
      ),
      spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
         async (session, dbName) =>
            standIn(session as DuckDBConnection, dbName, "read-only"),
      ),
      spyOn(connection, "attachDuckLakeReadOnlyIfPresent").mockImplementation(
         async (session, dbName) => {
            await standIn(session as DuckDBConnection, dbName, "read-only");
            return true;
         },
      ),
   ];
});

afterEach(() => {
   for (const spy of spies) spy.mockRestore();
   rmSync(dir, { recursive: true, force: true });
});

/** The attaches and statements since the last call, then cleared. */
function drained(): { attaches: string[]; statements: string[] } {
   const out = { attaches, statements };
   attaches = [];
   statements = [];
   return out;
}

async function observe(type: string): Promise<Record<string, unknown>> {
   const out: Record<string, unknown> = {};
   for (const withConfig of [true, false]) {
      const dest = connectionOf(type, withConfig);
      const key = withConfig ? "" : " (no config)";
      out[`supported${key}`] = await outcome(() =>
         assertSupportedDestination("lake", dest),
      );
      out[`config${key}`] = await outcome(() =>
         requireDuckLakeConfig("lake", dest),
      );
      out[`list${key}`] = {
         ...((await outcome(() =>
            listStorageTables({
               destinationName: "lake",
               destinationConnection: dest,
               schemaName: "main",
               environmentPath: dir,
            }),
         )) as object),
         ...drained(),
      };
      out[`cleanup${key}`] = {
         ...((await outcome(() =>
            cleanupStorageFiles({
               destinationName: "lake",
               destinationConnection: dest,
               snapshotsOlderThan: new Date("2026-01-01T00:00:00Z"),
               filesOlderThan: new Date("2026-01-02T00:00:00Z"),
            }),
         )) as object),
         ...drained(),
      };
      out[`load${key}`] = {
         ...((await outcome(() =>
            loadStorageTable({
               destinationName: "lake",
               destinationConnection: dest,
               schemaName: "main",
               tableName: "t",
               uri: path.join(DATA_PATH, "in.csv"),
               format: "csv",
               mode: "create",
            }),
         )) as object),
         ...drained(),
      };
      for (const sourceType of ["ducklake", type]) {
         out[`copy from ${sourceType}${key}`] = {
            ...((await outcome(() =>
               copyStorageTable({
                  destinationName: "lake",
                  destinationConnection: dest,
                  schemaName: "main",
                  tableName: "t",
                  source: {
                     catalogName: "old",
                     connection: connectionOf(sourceType, withConfig),
                     schemaName: "main",
                     tableName: "t",
                  },
                  mode: "create",
               }),
            )) as object),
            ...drained(),
         };
      }
      out[`build${key}`] = {
         ...((await outcome(async () => {
            const result = await buildSourceIntoStorage({
               destinationName: "lake",
               destinationConnection: dest,
               sourceConnection: { name: "wh", type: "postgres" },
               buildSQL: "SELECT 1 AS x",
               physicalTableName: "t",
               environmentPath: dir,
               deps: {
                  federate: async () => ({
                     handle: "wh",
                     sourceType: "postgres",
                  }),
                  read: async () => ({
                     selectSQL: "SELECT 1 AS x",
                     jobId: null,
                     cost: null,
                  }),
                  numericColumns: async () => ({ columns: [], numeric: [] }),
               },
            });
            return result.schema.map((c) => `${c.name} ${c.type}`);
         })) as object),
         ...drained(),
      };
      out[`same-catalog build${key}`] = {
         ...((await outcome(async () => {
            const result = await buildSourceIntoStorage({
               destinationName: "lake",
               destinationConnection: dest,
               sourceConnection: connectionOf(type, withConfig),
               sourceCarriesAuthorSql: false,
               buildSQL: "SELECT 1 AS x",
               physicalTableName: "t",
               environmentPath: dir,
            });
            return result.schema.map((c) => `${c.name} ${c.type}`);
         })) as object),
         ...drained(),
      };
   }
   // A source of this type, building into a DuckLake destination: the same
   // catalog, and another one.
   for (const [name, destination] of [
      ["same catalog", connectionOf("ducklake")],
      [
         "other catalog",
         {
            ...connectionOf("ducklake"),
            ducklakeConnection: {
               ...DUCKLAKE_CONFIG,
               catalog: {
                  postgresConnection: {
                     host: "other",
                     port: 5432,
                     databaseName: "cat",
                  },
               },
            },
         } as ApiConnection,
      ],
   ] as const) {
      out[`storage source, ${name}`] = await outcome(
         () =>
            storageSourceConnection(
               { getApiConnection: () => connectionOf(type) },
               {
                  connectionName: "lake",
                  name: "src",
               } as unknown as PersistSource,
               destination,
            ).type,
      );
   }
   return JSON.parse(normalize(JSON.stringify(out)));
}

describe("a storage destination is handled by its type as before the move", () => {
   it("for every type", async () => {
      const actual: Record<string, unknown> = {};
      for (const type of TYPES) actual[type] = await observe(type);
      if (process.env.RECORD_STORAGE_DESTINATION === "1") {
         fs.writeFileSync(GOLDEN, JSON.stringify(actual, null, 2) + "\n");
      }
      expect(actual).toEqual(JSON.parse(fs.readFileSync(GOLDEN, "utf8")));
   }, 120_000);
});
