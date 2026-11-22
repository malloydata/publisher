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
 * A storage destination's handling by its connection type: whether it is
 * one, which source a destination builds from, and how each destination
 * class (DuckLake, DuckDB, and a warehouse type standing for every type that
 * is not a destination) is attached for a listing, a file cleanup, a load,
 * a copy and a build, with the settings a build session runs with. DuckLake's
 * attach is stood in for by an in-memory database, and the statements a
 * session sent after its lockdown are recorded; the lockdown itself is
 * pinned in duckdb_lockdown.spec.ts.
 */

type ApiConnection = components["schemas"]["Connection"];

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

/** The source types a `storage=` build passes through. */
const PASSTHROUGH = new Set(["postgres", "bigquery", "snowflake"]);

/**
 * A local data path, fixed so the statements naming it are comparable. Spelled
 * with `/` on every platform: the load's containment check reads a data path
 * as `/`-separated, and DuckDB reads either separator on Windows.
 */
const DATA_PATH = `${path
   .join(tmpdir(), "storage-destination-conservation")
   .replaceAll("\\", "/")}/`;

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
   fs.writeFileSync(`${DATA_PATH}in.csv`, "x\n1\n");
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

/** The statements after the session lockdown's last one. */
function afterLockdown(sent: string[]): string[] {
   return sent.slice(sent.lastIndexOf("SET enable_external_access=false") + 1);
}

/** The settings a session ran before its lockdown began. */
function openingSettings(sent: string[]): string[] {
   const lockdown = sent.findIndex((q) =>
      q.startsWith("SELECT current_setting"),
   );
   return lockdown < 0 ? sent : sent.slice(0, lockdown);
}

/** What `run` returned or refused with, and the attaches and statements. */
async function traced(run: () => unknown): Promise<Record<string, unknown>> {
   const result = (await outcome(run)) as Record<string, unknown>;
   const { attaches: attached, statements: sent } = drained();
   return JSON.parse(
      normalize(
         JSON.stringify({
            ...result,
            attaches: attached,
            statements: afterLockdown(sent),
         }),
      ),
   );
}

const list = (dest: ApiConnection) =>
   traced(() =>
      listStorageTables({
         destinationName: "lake",
         destinationConnection: dest,
         schemaName: "main",
         environmentPath: dir,
      }),
   );
const cleanup = (dest: ApiConnection) =>
   traced(() =>
      cleanupStorageFiles({
         destinationName: "lake",
         destinationConnection: dest,
         snapshotsOlderThan: new Date("2026-01-01T00:00:00Z"),
         filesOlderThan: new Date("2026-01-02T00:00:00Z"),
      }),
   );
const load = (dest: ApiConnection) =>
   traced(() =>
      loadStorageTable({
         destinationName: "lake",
         destinationConnection: dest,
         schemaName: "main",
         tableName: "t",
         uri: `${DATA_PATH}in.csv`,
         format: "csv",
         mode: "create",
      }),
   );
const copy = (dest: ApiConnection, source: ApiConnection) =>
   traced(() =>
      copyStorageTable({
         destinationName: "lake",
         destinationConnection: dest,
         schemaName: "main",
         tableName: "t",
         source: {
            catalogName: "old",
            connection: source,
            schemaName: "main",
            tableName: "t",
         },
         mode: "create",
      }),
   );
/** A build from a stubbed Postgres warehouse; the session's opening SETs too. */
async function build(dest: ApiConnection): Promise<Record<string, unknown>> {
   const result = await outcome(async () => {
      const built = await buildSourceIntoStorage({
         destinationName: "lake",
         destinationConnection: dest,
         sourceConnection: { name: "wh", type: "postgres" },
         buildSQL: "SELECT 1 AS x",
         physicalTableName: "t",
         environmentPath: dir,
         deps: {
            federate: async () => ({ handle: "wh", sourceType: "postgres" }),
            read: async () => ({
               selectSQL: "SELECT 1 AS x",
               jobId: null,
               cost: null,
            }),
            numericColumns: async () => ({ columns: [], numeric: [] }),
         },
      });
      return built.schema.map((c) => `${c.name} ${c.type}`);
   });
   const { attaches: attached, statements: sent } = drained();
   return {
      ...(result as object),
      attaches: attached,
      opening: openingSettings(sent),
      statements: afterLockdown(sent),
   };
}
/** A build whose source is the destination's own connection. */
const sameCatalogBuild = (dest: ApiConnection) =>
   traced(async () => {
      const built = await buildSourceIntoStorage({
         destinationName: "lake",
         destinationConnection: dest,
         sourceConnection: dest,
         sourceCarriesAuthorSql: false,
         buildSQL: "SELECT 1 AS x",
         physicalTableName: "t",
         environmentPath: dir,
      });
      return built.schema.map((c) => `${c.name} ${c.type}`);
   });

const BUILT = [
   "BEGIN TRANSACTION",
   'CREATE OR REPLACE TABLE "lake"."t" AS (SELECT 1 AS x)',
   'DESCRIBE "lake"."t"',
   "COMMIT",
   'SELECT count(*) AS n FROM "lake"."t"',
];
const NONE = { attaches: [], statements: [] };

describe("a storage destination is handled by its type", () => {
   it("takes only DuckLake and DuckDB as destinations", async () => {
      for (const type of TYPES) {
         const refused =
            type === "ducklake" || type === "duckdb"
               ? { value: "undefined" }
               : {
                    error: `Storage destination 'lake' is type '${type}', but 'storage=' destinations must be one of ducklake, duckdb.`,
                    name: "BadRequestError",
                 };
         expect([
            type,
            await outcome(() =>
               assertSupportedDestination("lake", connectionOf(type)),
            ),
         ]).toEqual([type, refused]);
      }
   });

   it("reads the DuckLake config of any type, and refuses its absence", async () => {
      expect(
         await outcome(() =>
            requireDuckLakeConfig("lake", connectionOf("postgres")),
         ),
      ).toEqual({ value: DUCKLAKE_CONFIG });
      expect(
         await outcome(() =>
            requireDuckLakeConfig("lake", connectionOf("ducklake", false)),
         ),
      ).toEqual({
         error: "Storage destination 'lake' is type 'ducklake' but has no ducklakeConnection config.",
         name: "BadRequestError",
      });
   });

   it("builds a source of each type from the connection its type allows", async () => {
      const other = {
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
      } as ApiConnection;
      const sourceOf = (type: string, destination: ApiConnection) =>
         outcome(
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
      for (const type of TYPES) {
         const same = await sourceOf(type, connectionOf("ducklake"));
         const apart = await sourceOf(type, other);
         if (PASSTHROUGH.has(type)) {
            expect([type, same, apart]).toEqual([
               type,
               { value: type },
               { value: type },
            ]);
         } else if (type === "ducklake") {
            expect(same).toEqual({ value: "ducklake" });
            expect(apart).toMatchObject({
               error: expect.stringContaining(
                  "its connection 'lake' is a DuckLake catalog other than the destination's",
               ),
               name: "MaterializationEligibilityError",
            });
         } else {
            const words =
               type === "publisher"
                  ? "cannot be materialized into a storage destination on this server: its connection 'lake' is a 'publisher' connection"
                  : `its connection 'lake' is a '${type}' connection, which cannot be passed through`;
            for (const refused of [same, apart]) {
               expect([type, refused]).toMatchObject([
                  type,
                  {
                     error: expect.stringContaining(words),
                     name: "MaterializationEligibilityError",
                  },
               ]);
            }
         }
      }
   });

   it("lists, cleans, loads, copies into and builds into a DuckLake catalog", async () => {
      const lake = connectionOf("ducklake");
      expect(await list(lake)).toEqual({
         value: [],
         attaches: ["read-only lake"],
         statements: [
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' AND schema_name = 'main' ORDER BY table_name",
         ],
      });
      expect(await cleanup(lake)).toEqual({
         value: { expiredSnapshots: 0, deletedFiles: 0 },
         attaches: ["read-write lake"],
         statements: [
            "CALL ducklake_expire_snapshots('lake', older_than => TIMESTAMPTZ '2026-01-01T00:00:00.000Z')",
            "CALL ducklake_cleanup_old_files('lake', older_than => TIMESTAMPTZ '2026-01-02T00:00:00.000Z')",
         ],
      });
      expect(await load(lake)).toEqual({
         value: { rowCount: 1, columns: [{ name: "x", type: "BIGINT" }] },
         attaches: ["read-write lake"],
         statements: [
            "SET ducklake_default_data_inlining_row_limit = 0",
            `CREATE TABLE "lake"."main"."t" AS SELECT * FROM read_csv('<data>/in.csv')`,
            "CALL ducklake_flush_inlined_data('lake', schema_name => 'main', table_name => 't')",
            'SELECT count(*) AS n FROM "lake"."main"."t"',
            'DESCRIBE "lake"."main"."t"',
         ],
      });
      // The stand-in source has no table, so the copy fails at its CREATE,
      // after attaching both catalogs.
      expect(await copy(lake, connectionOf("ducklake"))).toMatchObject({
         name: "Error",
         attaches: ["read-write lake", "read-only __copy_source"],
         statements: expect.arrayContaining([
            'CREATE TABLE "lake"."main"."t" AS SELECT * FROM "__copy_source"."main"."t"',
         ]),
      });
      expect(await build(lake)).toEqual({
         value: ["x INTEGER"],
         attaches: ["read-write lake"],
         opening: [
            "SET ducklake_default_data_inlining_row_limit=0",
            "SET TimeZone='UTC'",
         ],
         statements: BUILT,
      });
      expect(await sameCatalogBuild(lake)).toEqual({
         value: ["x INTEGER"],
         attaches: ["read-write lake"],
         statements: [
            `SET search_path = '"lake".main'`,
            "DESCRIBE (SELECT 1 AS x)",
            ...BUILT,
         ],
      });
   });

   it("refuses a DuckLake destination with no config at its attach", async () => {
      const lake = connectionOf("ducklake", false);
      const noConfig = {
         error: "Storage destination 'lake' is type 'ducklake' but has no ducklakeConnection config.",
         name: "BadRequestError",
         ...NONE,
      };
      expect(await list(lake)).toEqual(noConfig);
      expect(await cleanup(lake)).toEqual(noConfig);
      expect(await sameCatalogBuild(lake)).toMatchObject({
         error: expect.stringContaining(
            "Cannot materialize a 'ducklake' source into a storage destination that is a different catalog.",
         ),
         ...NONE,
      });
   });

   it("lists and builds into a DuckDB destination without attaching", async () => {
      const duck = connectionOf("duckdb");
      expect(await list(duck)).toEqual({ value: [], ...NONE });
      expect(await build(duck)).toEqual({
         value: ["x INTEGER"],
         ...NONE,
         opening: [],
      });
      expect(await cleanup(duck)).toEqual({
         error: "Storage destination 'lake' is type 'duckdb'; file cleanup applies only to 'ducklake' destinations.",
         name: "BadRequestError",
         ...NONE,
      });
      expect(await load(duck)).toEqual({
         error: "Storage destination 'lake' is not DuckLake; only a DuckLake catalog takes a load.",
         name: "BadRequestError",
         ...NONE,
      });
      expect(await copy(duck, connectionOf("ducklake"))).toEqual({
         error: "Only a DuckLake catalog is a copy's source or destination.",
         name: "BadRequestError",
         ...NONE,
      });
      expect(await sameCatalogBuild(duck)).toMatchObject({
         error: expect.stringContaining(
            "Cannot materialize a 'duckdb' source into a storage destination: the native query-passthrough build supports",
         ),
         ...NONE,
      });
   });

   it("refuses every operation on a type that is not a destination", async () => {
      for (const type of ["postgres", "constructor"]) {
         const dest = connectionOf(type);
         const unsupported = {
            error: `Storage destination 'lake' is type '${type}', but 'storage=' destinations must be one of ducklake, duckdb.`,
            name: "BadRequestError",
            ...NONE,
         };
         expect(await list(dest)).toEqual(unsupported);
         expect(await build(dest)).toEqual({ ...unsupported, opening: [] });
         expect(await sameCatalogBuild(dest)).toEqual(unsupported);
         expect(await cleanup(dest)).toEqual({
            error: `Storage destination 'lake' is type '${type}'; file cleanup applies only to 'ducklake' destinations.`,
            name: "BadRequestError",
            ...NONE,
         });
         expect(await load(dest)).toEqual({
            error: "Storage destination 'lake' is not DuckLake; only a DuckLake catalog takes a load.",
            name: "BadRequestError",
            ...NONE,
         });
         expect(await copy(dest, connectionOf("ducklake"))).toEqual({
            error: "Only a DuckLake catalog is a copy's source or destination.",
            name: "BadRequestError",
            ...NONE,
         });
      }
   });
});
