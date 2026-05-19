/// <reference types="bun-types" />

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { PackageRepository } from "../../../src/storage/duckdb/PackageRepository";
import { initializeSchema } from "../../../src/storage/duckdb/schema";

const TEST_DB_DIR = path.join(os.tmpdir(), "duckdb-swap-package-dir-tests");

async function freshDb(): Promise<{
   db: DuckDBConnection;
   repo: PackageRepository;
   environmentId: string;
   cleanup: () => Promise<void>;
}> {
   await fs.mkdir(TEST_DB_DIR, { recursive: true });
   const dbPath = path.join(
      TEST_DB_DIR,
      `swap-${Date.now()}-${Math.random().toString(36).slice(2)}.duckdb`,
   );
   const db = new DuckDBConnection(dbPath);
   await db.initialize();
   await initializeSchema(db, false);

   // Seed an environment row so foreign keys are satisfied.
   const environmentId = "env-1";
   await db.run(
      `INSERT INTO environments (id, name, path, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?)`,
      [
         environmentId,
         "test-env",
         "/tmp/test-env",
         new Date().toISOString(),
         new Date().toISOString(),
      ],
   );

   const repo = new PackageRepository(db);

   return {
      db,
      repo,
      environmentId,
      cleanup: async () => {
         await db.close();
         await fs.rm(dbPath, { force: true });
      },
   };
}

describe("PackageRepository.swapPackageDirectory", () => {
   beforeEach(async () => {
      await fs.mkdir(TEST_DB_DIR, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(TEST_DB_DIR, { recursive: true, force: true });
   });

   it("inserts a new row and reports no previous directory", async () => {
      const { repo, environmentId, cleanup } = await freshDb();
      try {
         const result = await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-a",
            newDirectoryPath: "/var/data/pkg-a.versions/v1",
            description: "first version",
            metadata: { location: "https://example.com/pkg-a.git" },
         });

         expect(result.oldDirectoryPath).toBeNull();
         expect(result.id).toBeTruthy();

         const row = await repo.getPackageByName(environmentId, "pkg-a");
         expect(row?.manifestPath).toBe("/var/data/pkg-a.versions/v1");
         expect(row?.description).toBe("first version");
         expect(row?.metadata).toEqual({
            location: "https://example.com/pkg-a.git",
         });
      } finally {
         await cleanup();
      }
   });

   it("updates an existing row and reports the previous directory", async () => {
      const { repo, environmentId, cleanup } = await freshDb();
      try {
         await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-b",
            newDirectoryPath: "/var/data/pkg-b.versions/v1",
            description: "v1",
         });

         const swap = await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-b",
            newDirectoryPath: "/var/data/pkg-b.versions/v2",
            description: "v2",
         });

         expect(swap.oldDirectoryPath).toBe("/var/data/pkg-b.versions/v1");

         const row = await repo.getPackageByName(environmentId, "pkg-b");
         expect(row?.manifestPath).toBe("/var/data/pkg-b.versions/v2");
         expect(row?.description).toBe("v2");
      } finally {
         await cleanup();
      }
   });

   it("serializes concurrent swaps so the database always points at one of the new directories", async () => {
      const { repo, environmentId, cleanup } = await freshDb();
      try {
         await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-c",
            newDirectoryPath: "/var/data/pkg-c.versions/v0",
         });

         // Fire 10 concurrent swaps. Each one must observe a non-null
         // `oldDirectoryPath` (the row exists from v0 onward) and the
         // database must end up pointing at exactly one of the candidate
         // directories.
         const candidates = Array.from(
            { length: 10 },
            (_, i) => `/var/data/pkg-c.versions/v${i + 1}`,
         );

         const results = await Promise.all(
            candidates.map((newDirectoryPath) =>
               repo.swapPackageDirectory({
                  environmentId,
                  name: "pkg-c",
                  newDirectoryPath,
               }),
            ),
         );

         for (const result of results) {
            expect(result.oldDirectoryPath).not.toBeNull();
         }
         const oldsReported = results.map((r) => r.oldDirectoryPath as string);
         // Every old-path returned must be either v0 or one of the v1..v10
         // values (i.e. it was observed by some swap as the previous
         // value). No swap may report a path that was never written.
         const validOlds = new Set([
            "/var/data/pkg-c.versions/v0",
            ...candidates,
         ]);
         for (const old of oldsReported) {
            expect(validOlds.has(old)).toBe(true);
         }

         const row = await repo.getPackageByName(environmentId, "pkg-c");
         expect(candidates).toContain(row!.manifestPath);
      } finally {
         await cleanup();
      }
   });
});

describe("PackageRepository.listPackageDirectoryPaths", () => {
   it("returns every referenced directory and skips empty paths", async () => {
      const { repo, environmentId, cleanup } = await freshDb();
      try {
         await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-a",
            newDirectoryPath: "/var/data/pkg-a.versions/v1",
         });
         await repo.swapPackageDirectory({
            environmentId,
            name: "pkg-b",
            newDirectoryPath: "/var/data/pkg-b.versions/v1",
         });
         // Insert a legacy row with an empty manifest_path to confirm the
         // helper does not include the sentinel value.
         await repo.createPackage({
            environmentId,
            name: "legacy",
            manifestPath: "",
         });

         const paths = await repo.listPackageDirectoryPaths(environmentId);
         expect(paths.size).toBe(2);
         expect(paths.has("/var/data/pkg-a.versions/v1")).toBe(true);
         expect(paths.has("/var/data/pkg-b.versions/v1")).toBe(true);
      } finally {
         await cleanup();
      }
   });
});
