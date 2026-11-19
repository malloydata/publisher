// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { existsSync, mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import type { components } from "../../../api";
import { BadRequestError } from "../../../errors";
import * as connection from "../../connection";
import { allowForLockdown } from "../../duckdb_lockdown";
import { createIsolatedBuildSession } from "./materialization_build_session";
import { copyStorageTable } from "./storage_table_load";

type ApiConnection = components["schemas"]["Connection"];

/**
 * Copying a stored table from one DuckLake catalog (or the same one) into a
 * destination: the refusals before anything is attached, and the copy itself,
 * which keeps the source's partition layout. The attaches are pointed at file
 * catalogs here, since the real ones need a Postgres catalog database.
 */

function lakeConnection(name: string): ApiConnection {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         storage: { bucketUrl: `gs://bucket/${name}` },
         catalog: {
            postgresConnection: { host: "h", databaseName: "d", userName: "u" },
         },
      },
   } as ApiConnection;
}

const base = {
   destinationName: "dest",
   destinationConnection: lakeConnection("dest"),
   schemaName: "main",
   tableName: "copied",
   source: {
      catalogName: "src",
      connection: lakeConnection("src"),
      schemaName: "main",
      tableName: "events",
   },
   mode: "create" as const,
};

describe("copyStorageTable refuses before attaching", () => {
   it("copies only between DuckLake catalogs, either way round", async () => {
      const postgres = { name: "pg", type: "postgres" } as ApiConnection;
      await expect(
         copyStorageTable({ ...base, destinationConnection: postgres }),
      ).rejects.toThrow(BadRequestError);
      await expect(
         copyStorageTable({
            ...base,
            source: { ...base.source, connection: postgres },
         }),
      ).rejects.toThrow("Only a DuckLake catalog");
   });

   it("takes plain identifiers only, on either side", async () => {
      await expect(
         copyStorageTable({ ...base, tableName: 'x"; DROP TABLE y; --' }),
      ).rejects.toThrow("plain identifiers");
      await expect(
         copyStorageTable({
            ...base,
            source: { ...base.source, schemaName: "a.b" },
         }),
      ).rejects.toThrow(BadRequestError);
   });
});

describe("copyStorageTable on file catalogs", () => {
   let dir: string;
   const spies: { mockRestore(): void }[] = [];

   /**
    * Attach `alias` to the file catalog named for the connection's catalog,
    * recording its data path for the session's lock as the real attach does
    * (unless `record` is false).
    */
   const attachFile =
      (readOnly: boolean, record = true) =>
      async (
         session: DuckDBConnection,
         alias: string,
         config: components["schemas"]["DucklakeConnection"],
      ) => {
         const name = config.storage?.bucketUrl?.split("/").pop() ?? alias;
         await session.runSQL("INSTALL ducklake");
         await session.runSQL("LOAD ducklake");
         await session.runSQL(
            `ATTACH 'ducklake:${join(dir, `${name}.ducklake`)}' AS "${alias}" ` +
               `(DATA_PATH '${join(dir, name)}/'${readOnly ? ", READ_ONLY" : ""})`,
         );
         if (record) allowForLockdown(session, join(dir, name));
      };

   // Each test has its own catalogs, and every session that attached one is
   // closed for good (a plain close keeps the instance, and its files, open
   // until it is collected), so a file a later attach or the cleanup needs is
   // never still held: Windows refuses to open or remove a file held open.
   beforeEach(async () => {
      dir = mkdtempSync(join(tmpdir(), "storage-table-copy-"));
      spies.push(
         spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
            attachFile(false),
         ),
         spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
            attachFile(true),
         ),
      );
      // The source catalog: a partitioned table of three rows.
      const { session: seed, dispose } = createIsolatedBuildSession("seed");
      try {
         await attachFile(false)(
            seed,
            "src",
            lakeConnection("src").ducklakeConnection!,
         );
         // As Parquet files, as a build writes them, not rows inlined in the
         // catalog.
         await seed.runSQL("SET ducklake_default_data_inlining_row_limit = 0");
         await seed.runSQL(
            "CREATE OR REPLACE TABLE src.main.events AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'a')) AS v(id, kind)",
         );
         await seed.runSQL(
            "ALTER TABLE src.main.events SET PARTITIONED BY (kind)",
         );
         await seed.runSQL("DETACH src");
      } finally {
         await dispose();
      }
   });
   afterEach(() => {
      for (const spy of spies.splice(0)) spy.mockRestore();
      rmSync(dir, { recursive: true, force: true });
   });

   it("copies a table across catalogs, with its rows, columns and partition layout", async () => {
      const result = await copyStorageTable(base);
      expect(result).toEqual({
         rowCount: 3,
         columns: [
            { name: "id", type: "INTEGER" },
            { name: "kind", type: "VARCHAR" },
         ],
         partitionedBy: [{ column: "kind", transform: "identity" }],
      });
   });

   it("copies into a fresh catalog whose local data path does not exist yet", async () => {
      // The session is locked before the write, and a locked session creates
      // no directory above an allowed path: the copy creates the data path
      // first.
      expect(existsSync(join(dir, "dest"))).toBe(false);
      expect((await copyStorageTable(base)).rowCount).toBe(3);
      expect(existsSync(join(dir, "dest"))).toBe(true);
   });

   it("creates only the destination's data path, never the read-only source's", async () => {
      // The source's attach also records a local path that does not exist.
      const elsewhere = join(dir, "src-elsewhere");
      const recordElsewhere = attachFile(true);
      spies.push(
         spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
            async (session, alias, config) => {
               await recordElsewhere(session, alias, config);
               allowForLockdown(session, elsewhere);
            },
         ),
      );
      expect((await copyStorageTable(base)).rowCount).toBe(3);
      expect(existsSync(join(dir, "dest"))).toBe(true);
      expect(existsSync(elsewhere)).toBe(false);
   });

   it("copies within one catalog to another name", async () => {
      const result = await copyStorageTable({
         ...base,
         destinationName: "src",
         destinationConnection: lakeConnection("src"),
         tableName: "events_copy",
      });
      expect(result.rowCount).toBe(3);
      expect(result.partitionedBy).toEqual([
         { column: "kind", transform: "identity" },
      ]);
   });

   it("refuses to copy a partitioned table onto itself, whatever its case", async () => {
      await expect(
         copyStorageTable({
            ...base,
            destinationName: "src",
            destinationConnection: lakeConnection("src"),
            tableName: "EVENTS",
            mode: "replace",
         }),
      ).rejects.toThrow("cannot be copied onto itself");
   });

   it("reads and writes only the catalogs' data paths: its session is locked before the copy runs", async () => {
      // A source attach that records no data path: once the session is
      // locked, the source's files are outside everything it may read.
      spies.push(
         spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
            attachFile(true, false),
         ),
      );
      await expect(copyStorageTable(base)).rejects.toThrow(
         /Permission Error: .*disabled by configuration/,
      );
   });
});
