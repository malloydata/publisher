// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { ENGINE_TABLES } from "../../retrieval/index/catalog_store";
import { DuckDBConnection } from "./DuckDBConnection";
import {
   deleteUnconfiguredRetrievalRows,
   initializeSchema,
   RETRIEVAL_CACHE_TABLES,
} from "./schema";

const TEST_DB_DIR = path.join(os.tmpdir(), "duckdb-init-keeps-retrieval");

async function seedRead(
   db: DuckDBConnection,
   environment: string,
   pkg: string,
): Promise<void> {
   await db.run(
      `INSERT INTO dimension_value_reads VALUES (?, ?, 's.d', '', 'k', FALSE, '[]', now())`,
      [environment, pkg],
   );
}

async function readPackages(db: DuckDBConnection): Promise<string[]> {
   const rows = await db.all<{ p: string }>(
      `SELECT environment_name || '/' || package_name AS p FROM dimension_value_reads ORDER BY p`,
   );
   return rows.map((r) => r.p);
}

describe("--init and the retrieval caches", () => {
   let db: DuckDBConnection;

   beforeEach(async () => {
      await fs.mkdir(TEST_DB_DIR, { recursive: true });
      db = new DuckDBConnection(
         path.join(TEST_DB_DIR, `${Date.now()}-${Math.random()}.db`),
      );
      await db.initialize();
      await initializeSchema(db);
   });

   afterEach(async () => {
      await db.close();
      await fs.rm(TEST_DB_DIR, { recursive: true, force: true });
   });

   it("keeps every table the engine writes", () => {
      for (const table of ENGINE_TABLES) {
         expect(RETRIEVAL_CACHE_TABLES).toContain(table);
      }
   });

   it("keeps a stored read through --init, and drops the configuration", async () => {
      await seedRead(db, "env", "pkg");
      await db.run(
         `INSERT INTO environments (id, name, path, created_at, updated_at) VALUES ('e1', 'env', '/x', now(), now())`,
      );
      await initializeSchema(db, true);
      expect(await readPackages(db)).toEqual(["env/pkg"]);
      expect(await db.all(`SELECT * FROM environments`)).toEqual([]);
   });

   it("deletes the rows of a package the config no longer serves", async () => {
      await seedRead(db, "env", "kept");
      await seedRead(db, "env", "removed");
      await seedRead(db, "gone", "kept");
      await deleteUnconfiguredRetrievalRows(db, [
         { environment: "env", packages: ["kept"] },
      ]);
      expect(await readPackages(db)).toEqual(["env/kept"]);
   });

   it("deletes every row when the config serves nothing", async () => {
      await seedRead(db, "env", "pkg");
      await deleteUnconfiguredRetrievalRows(db, []);
      expect(await readPackages(db)).toEqual([]);
   });
});
