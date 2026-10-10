// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import {
   PackageVersionError,
   type PackageVersionErrorReason,
} from "../../errors";
import { NewVersion } from "../DatabaseInterface";
import { DuckDBConnection, isUniqueViolation } from "./DuckDBConnection";
import { DuckDBRepository } from "./DuckDBRepository";
import { initializeSchema } from "./schema";

const ENV_ID = "env-1";
const PKG = "sales";

const dbs: DuckDBConnection[] = [];

afterEach(async () => {
   while (dbs.length) await dbs.pop()!.close();
});

async function freshDb(): Promise<DuckDBConnection> {
   const db = new DuckDBConnection(":memory:");
   dbs.push(db);
   await db.initialize();
   return db;
}

/** A registry with one environment and one package row, as a publish leaves it. */
async function freshRepo(): Promise<{
   db: DuckDBConnection;
   repo: DuckDBRepository;
   packageId: string;
}> {
   const db = await freshDb();
   await initializeSchema(db);
   const repo = new DuckDBRepository(db);
   await db.run(
      `INSERT INTO environments (id, name, path, created_at, updated_at)
       VALUES (?, 'env', '/env', now(), now())`,
      [ENV_ID],
   );
   const pkg = await repo.createPackage({
      environmentId: ENV_ID,
      name: PKG,
      manifestPath: "/env/sales",
   });
   return { db, repo, packageId: pkg.id };
}

function newVersion(versionId: string, extra: Partial<NewVersion> = {}) {
   return {
      versionId,
      environmentId: ENV_ID,
      packageName: PKG,
      dirName: versionId.replace(/\+/g, "_"),
      contentHash: `hash-${versionId}`,
      sourceLocation: `/src/${versionId}`,
      manifestPath: null,
      description: `release ${versionId}`,
      ...extra,
   };
}

const always = () => true;
const never = () => false;

async function latestOf(repo: DuckDBRepository): Promise<string | null> {
   const pkg = await repo.getPackageByName(ENV_ID, PKG);
   return pkg?.latestVersion ?? null;
}

async function expectReason(
   promise: Promise<unknown>,
   reason: PackageVersionErrorReason,
): Promise<void> {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(PackageVersionError);
   expect((error as PackageVersionError).reason).toBe(reason);
}

describe("DuckDBConnection.transaction", () => {
   it("commits every statement together", async () => {
      const db = await freshDb();
      await db.run("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      const result = await db.transaction(async (tx) => {
         await tx.run("INSERT INTO t VALUES (1)");
         await tx.run("INSERT INTO t VALUES (2)");
         return (await tx.all<{ id: number }>("SELECT id FROM t")).length;
      });
      expect(result).toBe(2);
      expect(await db.all("SELECT id FROM t")).toHaveLength(2);
   });

   it("rolls back everything when the callback throws, and rethrows it", async () => {
      const db = await freshDb();
      await db.run("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      const boom = new Error("boom");
      const error = await db
         .transaction(async (tx) => {
            await tx.run("INSERT INTO t VALUES (1)");
            throw boom;
         })
         .catch((err: unknown) => err);
      expect(error).toBe(boom);
      expect(await db.all("SELECT id FROM t")).toHaveLength(0);
   });

   it("rolls back when a statement fails, and the connection stays usable", async () => {
      const db = await freshDb();
      await db.run("CREATE TABLE t (id INTEGER PRIMARY KEY)");
      await expect(
         db.transaction(async (tx) => {
            await tx.run("INSERT INTO t VALUES (1)");
            await tx.run("INSERT INTO t VALUES (1)");
         }),
      ).rejects.toThrow(/Duplicate key/i);
      expect(await db.all("SELECT id FROM t")).toHaveLength(0);
      await db.run("INSERT INTO t VALUES (3)");
      expect(await db.all("SELECT id FROM t")).toHaveLength(1);
   });

   it("keeps other statements out until it ends", async () => {
      const db = await freshDb();
      await db.run("CREATE TABLE t (n INTEGER)");
      await db.run("INSERT INTO t VALUES (0)");
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const order: string[] = [];
      const tx = db.transaction(async (q) => {
         const row = await q.get<{ n: number }>("SELECT n FROM t");
         await gate;
         await q.run("UPDATE t SET n = ?", [Number(row!.n) + 1]);
         order.push("tx");
      });
      const outside = db
         .run("UPDATE t SET n = n + 10")
         .then(() => order.push("outside"));
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(order).toEqual([]);
      release();
      await Promise.all([tx, outside]);
      expect(order).toEqual(["tx", "outside"]);
      expect(Number((await db.get<{ n: number }>("SELECT n FROM t"))!.n)).toBe(
         11,
      );
   });
});

describe("VersionRepository", () => {
   it("records a version, gets it and lists it", async () => {
      const { repo } = await freshRepo();
      const { version, promoted } = await repo.commitPublish(
         newVersion("1.0.0", { manifestPath: "gs://m/1.json" }),
         always,
      );
      expect(promoted).toBe(true);
      expect(version).toMatchObject({
         versionId: "1.0.0",
         packageName: PKG,
         dirName: "1.0.0",
         contentHash: "hash-1.0.0",
         sourceLocation: "/src/1.0.0",
         manifestPath: "gs://m/1.json",
         archiveStatus: "unarchive",
         archivedAt: null,
         demotedAt: null,
         description: "release 1.0.0",
         gitCommitSha: null,
         gitRef: null,
      });
      expect(version.promotedAt).toBeInstanceOf(Date);
      expect(await repo.getVersion(ENV_ID, PKG, "1.0.0")).toEqual(version);
      expect(await repo.getVersion(ENV_ID, PKG, "9.9.9")).toBeNull();

      await repo.commitPublish(newVersion("1.1.0+build.5"), never);
      expect(
         (await repo.listVersions(ENV_ID, PKG)).map((v) => [
            v.versionId,
            v.dirName,
         ]),
      ).toEqual([
         ["1.0.0", "1.0.0"],
         ["1.1.0+build.5", "1.1.0_build.5"],
      ]);
      expect(await repo.listVersionsByEnvironment(ENV_ID)).toHaveLength(2);
   });

   it("moves latest only when the rule says so, and stamps both sides", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      const second = await repo.commitPublish(newVersion("2.0.0"), never);
      expect(second.promoted).toBe(false);
      expect(second.version.promotedAt).toBeNull();
      expect(await latestOf(repo)).toBe("1.0.0");

      const seen: (string | null)[] = [];
      await repo.commitPublish(newVersion("3.0.0"), (current) => {
         seen.push(current);
         return true;
      });
      expect(seen).toEqual(["1.0.0"]);
      expect(await latestOf(repo)).toBe("3.0.0");
      expect(
         (await repo.getVersion(ENV_ID, PKG, "1.0.0"))!.demotedAt,
      ).toBeInstanceOf(Date);
      expect(
         (await repo.getVersion(ENV_ID, PKG, "3.0.0"))!.promotedAt,
      ).toBeInstanceOf(Date);
   });

   it("refuses a version that already has a row, changing nothing", async () => {
      const { repo } = await freshRepo();
      const first = await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.commitPublish(newVersion("2.0.0"), never);
      await expectReason(
         repo.commitPublish(
            newVersion("2.0.0", { contentHash: "different" }),
            always,
         ),
         "VERSION_CONFLICT",
      );
      expect((await repo.getVersion(ENV_ID, PKG, "2.0.0"))!.contentHash).toBe(
         "hash-2.0.0",
      );
      expect(await latestOf(repo)).toBe("1.0.0");
      expect(await repo.getVersion(ENV_ID, PKG, "1.0.0")).toEqual(
         first.version,
      );
   });

   it("writes nothing when the package has no row", async () => {
      const { repo } = await freshRepo();
      await expect(
         repo.commitPublish(
            { ...newVersion("1.0.0"), packageName: "ghost" },
            always,
         ),
      ).rejects.toThrow(/no registry row/);
      expect(await repo.listVersions(ENV_ID, "ghost")).toEqual([]);
   });

   it("writes nothing when the promote rule throws", async () => {
      const { repo } = await freshRepo();
      await expect(
         repo.commitPublish(newVersion("1.0.0"), () => {
            throw new Error("rule failed");
         }),
      ).rejects.toThrow("rule failed");
      expect(await repo.listVersions(ENV_ID, PKG)).toEqual([]);
      expect(await latestOf(repo)).toBeNull();
   });

   it("serializes concurrent publishes, so the promote rule sees each one's result", async () => {
      const { repo } = await freshRepo();
      const higher = (candidate: string) => (current: string | null) =>
         current === null || Number(candidate[0]) > Number(current[0]);
      await Promise.all(
         ["3.0.0", "1.0.0", "5.0.0", "2.0.0", "4.0.0"].map((v) =>
            repo.commitPublish(newVersion(v), higher(v)),
         ),
      );
      expect(await latestOf(repo)).toBe("5.0.0");
      expect(await repo.listVersions(ENV_ID, PKG)).toHaveLength(5);
   });

   it("setLatestVersion moves the pointer to an existing, unarchived version", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.commitPublish(newVersion("2.0.0"), always);

      expect(await repo.setLatestVersion(ENV_ID, PKG, "1.0.0")).toBe(true);
      expect(await latestOf(repo)).toBe("1.0.0");
      expect(
         (await repo.getVersion(ENV_ID, PKG, "2.0.0"))!.demotedAt,
      ).toBeInstanceOf(Date);
      // Already latest: nothing moves.
      expect(await repo.setLatestVersion(ENV_ID, PKG, "1.0.0")).toBe(false);
      // The guard is evaluated against the current latest.
      expect(await repo.setLatestVersion(ENV_ID, PKG, "2.0.0", never)).toBe(
         false,
      );
      expect(await latestOf(repo)).toBe("1.0.0");

      await expectReason(
         repo.setLatestVersion(ENV_ID, PKG, "9.0.0"),
         "VERSION_NOT_FOUND",
      );
      await repo.setVersionArchiveStatus(ENV_ID, PKG, "2.0.0", "archive");
      await expectReason(
         repo.setLatestVersion(ENV_ID, PKG, "2.0.0"),
         "VERSION_ARCHIVED",
      );
      expect(await latestOf(repo)).toBe("1.0.0");
   });

   it("archives and unarchives, and repeating a state changes nothing", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.commitPublish(newVersion("2.0.0"), always);

      const archived = await repo.setVersionArchiveStatus(
         ENV_ID,
         PKG,
         "1.0.0",
         "archive",
      );
      expect(archived.archiveStatus).toBe("archive");
      expect(archived.archivedAt).toBeInstanceOf(Date);
      const again = await repo.setVersionArchiveStatus(
         ENV_ID,
         PKG,
         "1.0.0",
         "archive",
      );
      expect(again).toEqual(archived);

      const restored = await repo.setVersionArchiveStatus(
         ENV_ID,
         PKG,
         "1.0.0",
         "unarchive",
      );
      expect(restored.archiveStatus).toBe("unarchive");
      expect(restored.archivedAt).toBeNull();
   });

   it("never archives latest, or the last version in service", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), never);
      // Under explicit promotion there may be no latest at all; the last
      // version in service is still kept.
      await expectReason(
         repo.setVersionArchiveStatus(ENV_ID, PKG, "1.0.0", "archive"),
         "VERSION_IS_LAST_ACTIVE",
      );

      await repo.commitPublish(newVersion("2.0.0"), always);
      await expectReason(
         repo.setVersionArchiveStatus(ENV_ID, PKG, "2.0.0", "archive"),
         "VERSION_IS_LATEST",
      );
      await repo.setVersionArchiveStatus(ENV_ID, PKG, "1.0.0", "archive");
      expect((await repo.getVersion(ENV_ID, PKG, "2.0.0"))!.archiveStatus).toBe(
         "unarchive",
      );
      await expectReason(
         repo.setVersionArchiveStatus(ENV_ID, PKG, "9.0.0", "archive"),
         "VERSION_NOT_FOUND",
      );
   });

   it("binds and clears a version's manifest", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      expect(
         (
            await repo.setVersionManifestPath(
               ENV_ID,
               PKG,
               "1.0.0",
               "s3://m.json",
            )
         ).manifestPath,
      ).toBe("s3://m.json");
      expect(
         (await repo.setVersionManifestPath(ENV_ID, PKG, "1.0.0", null))
            .manifestPath,
      ).toBeNull();
      await expectReason(
         repo.setVersionManifestPath(ENV_ID, PKG, "9.0.0", null),
         "VERSION_NOT_FOUND",
      );
   });

   it("deletes a package's versions with the package, and an environment's with it", async () => {
      const { repo, packageId } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.deletePackage(packageId);
      expect(await repo.listVersions(ENV_ID, PKG)).toEqual([]);

      const again = await repo.createPackage({
         environmentId: ENV_ID,
         name: PKG,
         manifestPath: "/env/sales",
      });
      expect(again.latestVersion).toBeNull();
      await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.deleteEnvironment(ENV_ID);
      expect(await repo.listVersionsByEnvironment(ENV_ID)).toEqual([]);
   });

   it("deletes versions with the environment's packages", async () => {
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), always);
      await repo.deletePackagesByEnvironmentId(ENV_ID);
      expect(await repo.listVersionsByEnvironment(ENV_ID)).toEqual([]);
   });

   it("says whether a package has any version", async () => {
      const { repo } = await freshRepo();
      expect(await repo.hasVersions(ENV_ID, PKG)).toBe(false);
      await repo.commitPublish(newVersion("1.0.0"), never);
      expect(await repo.hasVersions(ENV_ID, PKG)).toBe(true);
      expect(await repo.hasVersions(ENV_ID, "other")).toBe(false);
   });

   it("lets only one of two concurrent archives take the second-to-last version", async () => {
      // No latest (explicit promotion), two versions in service: each archive
      // alone is allowed, both together would leave none. The guard and the
      // write share one transaction, so exactly one wins.
      const { repo } = await freshRepo();
      await repo.commitPublish(newVersion("1.0.0"), never);
      await repo.commitPublish(newVersion("2.0.0"), never);
      const outcomes = await Promise.allSettled([
         repo.setVersionArchiveStatus(ENV_ID, PKG, "1.0.0", "archive"),
         repo.setVersionArchiveStatus(ENV_ID, PKG, "2.0.0", "archive"),
      ]);
      expect(outcomes.filter((o) => o.status === "fulfilled")).toHaveLength(1);
      const refused = outcomes.find((o) => o.status === "rejected") as
         | PromiseRejectedResult
         | undefined;
      expect((refused!.reason as PackageVersionError).reason).toBe(
         "VERSION_IS_LAST_ACTIVE",
      );
      const active = (await repo.listVersions(ENV_ID, PKG)).filter(
         (v) => v.archiveStatus === "unarchive",
      );
      expect(active).toHaveLength(1);
   });

   it("tells a duplicate key from a foreign-key failure", async () => {
      // A version insert can only be a VERSION_CONFLICT when the key is taken;
      // a missing environment must not read as one.
      const db = await freshDb();
      await db.run("CREATE TABLE parent (id INTEGER PRIMARY KEY)");
      await db.run(
         "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
      );
      await db.run("INSERT INTO parent VALUES (1)");
      await db.run("INSERT INTO child VALUES (1, 1)");
      const duplicate = await db
         .run("INSERT INTO child VALUES (1, 1)")
         .catch((err: unknown) => err);
      const foreignKey = await db
         .run("INSERT INTO child VALUES (2, 99)")
         .catch((err: unknown) => err);
      expect(isUniqueViolation(duplicate)).toBe(true);
      expect(foreignKey).toBeInstanceOf(Error);
      expect(isUniqueViolation(foreignKey)).toBe(false);
   });
});

describe("versions schema upgrade", () => {
   it("adds the versions table and the two new columns to a store that predates them, keeping its rows", async () => {
      const db = await freshDb();
      // The shapes main shipped before versions, indexes included: ADD COLUMN
      // on an indexed table is the step most likely to fail.
      await db.run(`
         CREATE TABLE environments (
            id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL UNIQUE,
            path VARCHAR NOT NULL, description VARCHAR, metadata JSON,
            created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL)`);
      await db.run(`
         CREATE TABLE packages (
            id VARCHAR PRIMARY KEY, environment_id VARCHAR NOT NULL,
            name VARCHAR NOT NULL, description VARCHAR,
            manifest_path VARCHAR NOT NULL, metadata JSON,
            created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (environment_id) REFERENCES environments(id),
            UNIQUE (environment_id, name))`);
      await db.run(`
         CREATE TABLE materializations (
            id VARCHAR PRIMARY KEY, environment_id VARCHAR NOT NULL,
            package_name VARCHAR NOT NULL, status VARCHAR NOT NULL,
            active_key VARCHAR, started_at TIMESTAMP, completed_at TIMESTAMP,
            error TEXT, metadata JSON, manifest JSON,
            created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (environment_id) REFERENCES environments(id))`);
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
         "INSERT INTO environments VALUES ('env-1', 'env', '/env', NULL, NULL, now(), now())",
      );
      await db.run(
         "INSERT INTO packages VALUES ('p-1', 'env-1', 'sales', 'kept', '/env/sales', NULL, now(), now())",
      );
      await db.run(
         "INSERT INTO materializations (id, environment_id, package_name, status, created_at, updated_at) VALUES ('m-1', 'env-1', 'sales', 'SUCCESS', now(), now())",
      );

      await initializeSchema(db);

      const columns = async (table: string) =>
         (
            await db.all<{ column_name: string }>(
               "SELECT column_name FROM duckdb_columns() WHERE table_name = ?",
               [table],
            )
         ).map((row) => row.column_name);
      expect(await columns("packages")).toContain("latest_version");
      expect(await columns("materializations")).toContain("version");
      expect(await columns("versions")).toEqual([
         "version_id",
         "environment_id",
         "package_name",
         "dir_name",
         "content_hash",
         "source_location",
         "manifest_path",
         "archive_status",
         "archived_at",
         "promoted_at",
         "demoted_at",
         "description",
         "git_commit_sha",
         "git_ref",
         "created_at",
         "updated_at",
      ]);

      const repo = new DuckDBRepository(db);
      const pkg = await repo.getPackageByName("env-1", "sales");
      expect(pkg).toMatchObject({ id: "p-1", description: "kept" });
      expect(pkg!.latestVersion).toBeNull();
      expect(await repo.getMaterializationById("m-1")).not.toBeNull();

      // And the upgraded store takes versions.
      await repo.commitPublish(
         { ...newVersion("1.0.0"), environmentId: "env-1" },
         always,
      );
      expect(
         (await repo.getPackageByName("env-1", "sales"))!.latestVersion,
      ).toBe("1.0.0");
   });
});
