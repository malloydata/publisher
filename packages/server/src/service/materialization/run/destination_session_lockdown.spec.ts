// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every session on a storage destination is confined to that destination by
 * default (duckdb_lockdown.ts): external access off, so its work reads and
 * writes only the destination's own paths. A caller that must set up more
 * first (a build that federates its source) opts out and locks it itself.
 * Listing a destination's tables and the DuckLake file cleanup lock their own
 * sessions, after the attach and before their work.
 */

import { afterAll, afterEach, describe, expect, it, spyOn } from "bun:test";
import {
   existsSync,
   mkdirSync,
   mkdtempSync,
   rmSync,
   writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../../api";
import * as connection from "../../connection";
import { storageDestinationRoot } from "../../connection_config";
import * as lockdown from "../../duckdb_lockdown";
import {
   cleanupStorageFiles,
   createIsolatedBuildSession,
   createLocalDataPaths,
   dropStorageTable,
   listStorageTables,
   withDestinationSession,
} from "./materialization_build_session";

type ApiConnection = components["schemas"]["Connection"];

const environmentPath = mkdtempSync(join(tmpdir(), "dest-lockdown-"));
const outsideDir = mkdtempSync(join(tmpdir(), "outside-"));
afterAll(() => {
   rmSync(environmentPath, { recursive: true, force: true });
   rmSync(outsideDir, { recursive: true, force: true });
});

const destination = {
   destinationName: "lake",
   destinationConnection: { name: "lake", type: "duckdb" } as never,
   environmentPath,
};

async function externalAccess(session: DuckDBConnection): Promise<unknown> {
   const result = (await session.runSQL(
      "SELECT current_setting('enable_external_access') AS on",
   )) as unknown as { rows: { on: unknown }[] };
   return result.rows[0]?.on;
}

/**
 * Watch the sessions `lockDownDuckDBSession` confines: each lock's external
 * access just after it, and every statement its session runs from then on.
 */
function watchLocks(): {
   locks: { externalAccess: unknown }[];
   afterLock: string[];
   restore(): void;
} {
   const locks: { externalAccess: unknown }[] = [];
   const afterLock: string[] = [];
   const original = lockdown.lockDownDuckDBSession;
   const spy = spyOn(lockdown, "lockDownDuckDBSession").mockImplementation(
      async (session, options) => {
         await original(session, options);
         locks.push({ externalAccess: await externalAccess(session) });
         const runSQL = session.runSQL.bind(session);
         session.runSQL = ((sql: string, ...rest: unknown[]) => {
            afterLock.push(sql);
            return (runSQL as (...args: unknown[]) => unknown)(sql, ...rest);
         }) as typeof session.runSQL;
      },
   );
   return { locks, afterLock, restore: () => spy.mockRestore() };
}

describe("a destination session", () => {
   it("is confined to its destination before its work runs", async () => {
      expect(
         await withDestinationSession(
            destination,
            { purpose: "view" },
            externalAccess,
         ),
      ).toBe(false);
   });

   it("reads no file outside its destination", async () => {
      const outside = join(outsideDir, "t.csv");
      writeFileSync(outside, "a\n1\n");
      const read = withDestinationSession(
         destination,
         { purpose: "view" },
         (session) => session.runSQL(`SELECT * FROM read_csv('${outside}')`),
      );
      await expect(read).rejects.toThrow(
         /Permission Error: .*disabled by configuration/,
      );
   });

   it("is left to its caller to confine, when the caller says so", async () => {
      expect(
         await withDestinationSession(
            destination,
            { purpose: "view", lockdown: "caller" },
            externalAccess,
         ),
      ).toBe(true);
   });
});

describe("listing a destination's tables", () => {
   let watch: ReturnType<typeof watchLocks> | undefined;
   afterEach(() => watch?.restore());

   it("is confined to the destination before it lists", async () => {
      // A plain DuckDB destination with one table, so the listing attaches.
      const root = storageDestinationRoot(environmentPath);
      mkdirSync(root, { recursive: true });
      const { session: seed, dispose } = createIsolatedBuildSession("seed");
      try {
         await seed.runSQL(`ATTACH '${join(root, "listed.duckdb")}' AS listed`);
         await seed.runSQL("CREATE TABLE listed.main.t AS SELECT 1 AS a");
         await seed.runSQL("DETACH listed");
      } finally {
         await dispose();
      }

      watch = watchLocks();
      const tables = await listStorageTables({
         destinationName: "listed",
         destinationConnection: { name: "listed", type: "duckdb" },
         schemaName: "main",
         environmentPath,
      });
      expect(tables).toEqual(["t"]);
      expect(watch.locks).toEqual([{ externalAccess: false }]);
      expect(
         watch.afterLock.some((sql) => sql.includes("duckdb_tables()")),
      ).toBe(true);
   });
});

describe("the DuckLake file cleanup", () => {
   let watch: ReturnType<typeof watchLocks> | undefined;
   let attach: { mockRestore(): void } | undefined;
   afterEach(() => {
      watch?.restore();
      attach?.mockRestore();
   });

   it("is confined to the catalog before it expires or deletes anything", async () => {
      // A file catalog in place of the Postgres one the real attach needs.
      const lakeDir = join(environmentPath, "cleanup");
      mkdirSync(join(lakeDir, "data"), { recursive: true });
      attach = spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
         async (session, alias) => {
            await session.runSQL("INSTALL ducklake");
            await session.runSQL("LOAD ducklake");
            await session.runSQL(
               `ATTACH 'ducklake:${join(lakeDir, "lake.ducklake")}' AS "${alias}" ` +
                  `(DATA_PATH '${join(lakeDir, "data")}/')`,
            );
            lockdown.allowForLockdown(session, join(lakeDir, "data"));
         },
      );

      watch = watchLocks();
      const now = new Date();
      await cleanupStorageFiles({
         destinationName: "lake",
         destinationConnection: {
            name: "lake",
            type: "ducklake",
            ducklakeConnection: {
               catalog: {
                  postgresConnection: {
                     host: "h",
                     databaseName: "d",
                     userName: "u",
                  },
               },
            },
         } as ApiConnection,
         snapshotsOlderThan: now,
         filesOlderThan: now,
      });
      expect(watch.locks).toEqual([{ externalAccess: false }]);
      const calls = watch.afterLock.filter((sql) => sql.startsWith("CALL"));
      expect(calls).toHaveLength(2);
      expect(calls[0]).toContain("ducklake_expire_snapshots");
      expect(calls[1]).toContain("ducklake_cleanup_old_files");
   });
});

describe("a destination's local data path", () => {
   let attach: { mockRestore(): void } | undefined;
   afterEach(() => attach?.mockRestore());

   const lake = {
      name: "lake",
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: { host: "h", databaseName: "d", userName: "u" },
         },
      },
   } as ApiConnection;

   /**
    * A file catalog in place of the Postgres one, whose attach records
    * `recorded` as its data path, as the real attach records the catalog's.
    */
   function fileCatalog(name: string, recorded: string): void {
      const lakeDir = join(environmentPath, name);
      mkdirSync(join(lakeDir, "data"), { recursive: true });
      attach?.mockRestore();
      attach = spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
         async (session, alias) => {
            await session.runSQL("INSTALL ducklake");
            await session.runSQL("LOAD ducklake");
            await session.runSQL(
               `ATTACH 'ducklake:${join(lakeDir, "lake.ducklake")}' AS "${alias}" ` +
                  `(DATA_PATH '${join(lakeDir, "data")}/')`,
            );
            lockdown.allowForLockdown(session, recorded);
         },
      );
   }

   const lakeDestination = {
      destinationName: "lake",
      destinationConnection: lake,
      environmentPath,
   };

   it("is created for a build, which writes table files there", async () => {
      const recorded = join(environmentPath, "fresh-build", "recorded");
      fileCatalog("fresh-build", recorded);
      await withDestinationSession(
         lakeDestination,
         { purpose: "build", build: true },
         async () => undefined,
      );
      expect(existsSync(recorded)).toBe(true);
   });

   it("is not created for a key check or a table mark, which write no file", async () => {
      const recorded = join(environmentPath, "fresh-view", "recorded");
      fileCatalog("fresh-view", recorded);
      await withDestinationSession(
         lakeDestination,
         { purpose: "view" },
         async () => undefined,
      );
      expect(existsSync(recorded)).toBe(false);
   });

   it("is not created by the file cleanup, which only deletes", async () => {
      const recorded = join(environmentPath, "fresh-cleanup", "recorded");
      fileCatalog("fresh-cleanup", recorded);
      const now = new Date();
      await cleanupStorageFiles({
         destinationName: "lake",
         destinationConnection: lake,
         snapshotsOlderThan: now,
         filesOlderThan: now,
      });
      expect(existsSync(recorded)).toBe(false);
   });

   it("that cannot be created on this host leaves a drop working", async () => {
      // Recorded under a regular file: no directory can be created there.
      const file = join(environmentPath, "not-a-directory");
      writeFileSync(file, "");
      fileCatalog("uncreatable-drop", join(file, "data"));
      await dropStorageTable({
         destinationName: "lake",
         destinationConnection: lake,
         physicalTableName: "gone",
         environmentPath,
      });
   });

   it("that cannot be created is skipped, not thrown, and the rest are created", async () => {
      const file = join(environmentPath, "also-not-a-directory");
      writeFileSync(file, "");
      const creatable = join(environmentPath, "creatable", "data");
      const { session, dispose } = createIsolatedBuildSession("paths");
      try {
         expect(() =>
            createLocalDataPaths(session, [
               join(file, "data"),
               creatable,
               "s3://bucket/prefix",
            ]),
         ).not.toThrow();
      } finally {
         await dispose();
      }
      expect(existsSync(creatable)).toBe(true);
   });
});
