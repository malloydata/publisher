// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

// The registry half of package versions, against a REAL DuckDBConnection: the
// `package_versions` table, the `packages.latest_version` pointer, the hand-made
// delete cascades that keep both in step with their package and environment,
// and the upgrade of a store that predates them.

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import {
   DuplicatePackageVersionError,
   type PackageVersion,
} from "../../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../src/storage/duckdb/schema";

type NewVersion = Omit<PackageVersion, "id" | "createdAt" | "updatedAt">;

function newVersion(
   environmentId: string,
   packageName: string,
   version: string,
   overrides: Partial<NewVersion> = {},
): NewVersion {
   return {
      environmentId,
      packageName,
      version,
      dirName: version.replace(/\+/g, "_"),
      contentHash: `hash-of-${version}`,
      sourceLocation: `gs://bucket/${packageName}-${version}.zip`,
      manifestLocation: null,
      archiveStatus: "unarchive",
      archivedAt: null,
      description: null,
      gitCommitSha: null,
      gitRef: null,
      ...overrides,
   };
}

describe("package versions registry (real connection)", () => {
   let dbDir: string;
   let db: DuckDBConnection;
   let repo: DuckDBRepository;
   let environmentId: string;

   beforeEach(async () => {
      dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "duckdb-versions-"));
      db = new DuckDBConnection(path.join(dbDir, "test.db"));
      await db.initialize();
      await initializeSchema(db);
      repo = new DuckDBRepository(db);
      environmentId = (
         await repo.createEnvironment({ name: "env", path: "/tmp/env" })
      ).id;
   });

   afterEach(async () => {
      try {
         await db.close();
      } catch {
         // ignore
      }
      await fs.rm(dbDir, { recursive: true, force: true });
   });

   describe("versions", () => {
      it("creates a version and reads every field back", async () => {
         const created = await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.2.0-rc1+build.5", {
               manifestLocation: "gs://bucket/manifest.json",
               description: "first cut",
               gitCommitSha: "deadbeef",
               gitRef: "refs/heads/main",
            }),
         );
         expect(created.id).toBeTruthy();
         expect(created.createdAt).toBeInstanceOf(Date);

         const read = await repo.getPackageVersion(
            environmentId,
            "sales",
            "1.2.0-rc1+build.5",
         );
         expect(read).toEqual(created);
         expect(read?.dirName).toBe("1.2.0-rc1_build.5");
         expect(read?.manifestLocation).toBe("gs://bucket/manifest.json");
         expect(read?.archivedAt).toBeNull();
      });

      it("refuses a second publish of the same version", async () => {
         await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.0.0"),
         );
         await expect(
            repo.createPackageVersion(
               newVersion(environmentId, "sales", "1.0.0", {
                  contentHash: "something-else",
               }),
            ),
         ).rejects.toBeInstanceOf(DuplicatePackageVersionError);
      });

      it("keys a version on its package: one version number in two packages is two versions", async () => {
         await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.0.0"),
         );
         await repo.createPackageVersion(
            newVersion(environmentId, "marketing", "1.0.0"),
         );
         expect(
            await repo.listPackageVersions(environmentId, "sales"),
         ).toHaveLength(1);
         expect(
            (await repo.listPackageVersionsByEnvironment(environmentId)).map(
               (v) => `${v.packageName}@${v.version}`,
            ),
         ).toEqual(["marketing@1.0.0", "sales@1.0.0"]);
      });

      it("returns null for a version that does not exist", async () => {
         expect(
            await repo.getPackageVersion(environmentId, "sales", "9.9.9"),
         ).toBeNull();
      });

      it("updates only lifecycle state and the manifest binding", async () => {
         const created = await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.0.0"),
         );
         const archivedAt = new Date("2026-10-01T00:00:00Z");
         const archived = await repo.updatePackageVersion(created.id, {
            archiveStatus: "archive",
            archivedAt,
         });
         expect(archived.archiveStatus).toBe("archive");
         expect(archived.archivedAt?.getTime()).toBe(archivedAt.getTime());
         expect(archived.contentHash).toBe(created.contentHash);

         const rebound = await repo.updatePackageVersion(created.id, {
            manifestLocation: "gs://bucket/next.json",
         });
         expect(rebound.manifestLocation).toBe("gs://bucket/next.json");
         // Fields the update did not name are left as they were.
         expect(rebound.archiveStatus).toBe("archive");

         const cleared = await repo.updatePackageVersion(created.id, {
            manifestLocation: null,
            archiveStatus: "unarchive",
            archivedAt: null,
         });
         expect(cleared.manifestLocation).toBeNull();
         expect(cleared.archivedAt).toBeNull();
      });

      it("fails an update of a version that does not exist", async () => {
         await expect(
            repo.updatePackageVersion("no-such-id", {
               archiveStatus: "archive",
            }),
         ).rejects.toThrow("not found");
      });
   });

   describe("latest pointer", () => {
      beforeEach(async () => {
         await repo.createPackage({
            environmentId,
            name: "sales",
            manifestPath: "",
         });
      });

      async function latest(): Promise<string | null | undefined> {
         return (await repo.getPackageByName(environmentId, "sales"))
            ?.latestVersion;
      }

      it("starts with no latest", async () => {
         expect(await latest()).toBeNull();
      });

      it("moves only from the value the caller expected", async () => {
         expect(
            await repo.setPackageLatestVersion(
               environmentId,
               "sales",
               null,
               "1.0.0",
            ),
         ).toBe(true);
         expect(await latest()).toBe("1.0.0");

         // A caller that read "no latest" before the first move lost the race,
         // and must not overwrite it.
         expect(
            await repo.setPackageLatestVersion(
               environmentId,
               "sales",
               null,
               "2.0.0",
            ),
         ).toBe(false);
         expect(await latest()).toBe("1.0.0");

         expect(
            await repo.setPackageLatestVersion(
               environmentId,
               "sales",
               "1.0.0",
               "2.0.0",
            ),
         ).toBe(true);
         expect(await latest()).toBe("2.0.0");
      });

      it("reports false for a package with no row", async () => {
         expect(
            await repo.setPackageLatestVersion(
               environmentId,
               "nope",
               null,
               "1.0.0",
            ),
         ).toBe(false);
      });

      it("is not touched by the package upsert the boot sync performs", async () => {
         await repo.setPackageLatestVersion(
            environmentId,
            "sales",
            null,
            "1.0.0",
         );
         const row = await repo.getPackageByName(environmentId, "sales");
         await repo.updatePackage(row!.id, {
            description: "edited",
            manifestPath: "",
            metadata: {},
         });
         expect(await latest()).toBe("1.0.0");
      });

      it("admits exactly one of two concurrent moves from the same value", async () => {
         const results = await Promise.all([
            repo.setPackageLatestVersion(environmentId, "sales", null, "1.0.0"),
            repo.setPackageLatestVersion(environmentId, "sales", null, "1.1.0"),
         ]);
         expect(results.filter(Boolean)).toHaveLength(1);
         expect(["1.0.0", "1.1.0"]).toContain((await latest()) as string);
      });
   });

   describe("delete cascades", () => {
      it("deleting a package deletes its versions and no other package's", async () => {
         const sales = await repo.createPackage({
            environmentId,
            name: "sales",
            manifestPath: "",
         });
         await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.0.0"),
         );
         await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.1.0"),
         );
         await repo.createPackageVersion(
            newVersion(environmentId, "marketing", "1.0.0"),
         );

         await repo.deletePackage(sales.id);

         expect(await repo.listPackageVersions(environmentId, "sales")).toEqual(
            [],
         );
         expect(
            await repo.listPackageVersions(environmentId, "marketing"),
         ).toHaveLength(1);
      });

      it("deleting an environment deletes its versions before the environment row", async () => {
         await repo.createPackage({
            environmentId,
            name: "sales",
            manifestPath: "",
         });
         await repo.createPackageVersion(
            newVersion(environmentId, "sales", "1.0.0"),
         );

         // The versions table references environments(id), so this throws if
         // its rows are not removed first.
         await repo.deleteEnvironment(environmentId);

         expect(
            await repo.listPackageVersionsByEnvironment(environmentId),
         ).toEqual([]);
         expect(await repo.getEnvironmentById(environmentId)).toBeNull();
      });
   });
});

describe("package versions schema upgrade", () => {
   let dbDir: string;
   let db: DuckDBConnection;

   beforeEach(async () => {
      dbDir = await fs.mkdtemp(path.join(os.tmpdir(), "duckdb-versions-up-"));
      db = new DuckDBConnection(path.join(dbDir, "test.db"));
      await db.initialize();
   });

   afterEach(async () => {
      try {
         await db.close();
      } catch {
         // ignore
      }
      await fs.rm(dbDir, { recursive: true, force: true });
   });

   async function columns(table: string): Promise<string[]> {
      const rows = await db.all<{ column_name: string }>(
         "SELECT column_name FROM duckdb_columns() WHERE table_name = ? ORDER BY column_index",
         [table],
      );
      return rows.map((r) => r.column_name);
   }

   it("adds the versions table and the two new columns to a store that predates them, keeping its rows", async () => {
      // The tables as the build before package versions declared them, seeded
      // verbatim: DuckDB cannot drop a column from an indexed table, so the
      // older store cannot be derived from today's by subtraction.
      await db.run(`
         CREATE TABLE environments (
            id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL UNIQUE,
            path VARCHAR NOT NULL,
            description VARCHAR,
            metadata JSON,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
         )
      `);
      await db.run(`
         CREATE TABLE packages (
            id VARCHAR PRIMARY KEY,
            environment_id VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            description VARCHAR,
            manifest_path VARCHAR NOT NULL,
            metadata JSON,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (environment_id) REFERENCES environments(id),
            UNIQUE (environment_id, name)
         )
      `);
      await db.run(`
         CREATE TABLE materializations (
            id VARCHAR PRIMARY KEY,
            environment_id VARCHAR NOT NULL,
            package_name VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            active_key VARCHAR,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error TEXT,
            metadata JSON,
            manifest JSON,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (environment_id) REFERENCES environments(id)
         )
      `);
      // The indexes a real store already holds when this build first boots.
      // DuckDB refuses some ALTERs on an indexed table, so the upgrade has to
      // be proven with them in place, not on bare tables.
      await db.run(
         "CREATE INDEX idx_packages_environment_id ON packages(environment_id)",
      );
      await db.run(
         "CREATE INDEX idx_materializations_environment_package ON materializations(environment_id, package_name)",
      );
      await db.run(
         "CREATE UNIQUE INDEX idx_materializations_active_key ON materializations(active_key)",
      );
      await db.run(
         `INSERT INTO environments VALUES ('env-1', 'env', '/tmp/env', NULL, NULL,
            TIMESTAMP '2026-09-01 00:00:00', TIMESTAMP '2026-09-01 00:00:00')`,
      );
      await db.run(
         `INSERT INTO packages VALUES ('pkg-1', 'env-1', 'sales', NULL, '', NULL,
            TIMESTAMP '2026-09-01 00:00:00', TIMESTAMP '2026-09-01 00:00:00')`,
      );
      const repo = new DuckDBRepository(db);
      const env = { id: "env-1" };

      await initializeSchema(db);

      expect(await columns("packages")).toContain("latest_version");
      expect(await columns("materializations")).toContain("version");
      expect(await columns("package_versions")).toContain("content_hash");
      const kept = await repo.getPackageByName(env.id, "sales");
      expect(kept?.latestVersion).toBeNull();
      expect(
         await repo.setPackageLatestVersion(env.id, "sales", null, "1.0.0"),
      ).toBe(true);
   });
});
