// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageVersionError,
   type PackageVersionErrorReason,
} from "../../errors";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { VersionHost, VersionService } from "./version_service";
import { VersionStore } from "./version_store";

// The lifecycle rules: list, get, archive/unarchive, manifest binding and the
// `latest` pointer, against a real registry and real version folders. The host
// is a fake that records what the rules asked of it.

const ENV_ID = "env-1";
const PKG = "sales";

interface Loaded {
   versionId: string;
   manifest: string | null;
}

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
let log: string[];
let building: Set<string>;
let failLoad: Set<string>;

function host(): VersionHost<Loaded> {
   const lock = new Mutex();
   return {
      loadVersion: async (_pkg, _path, version) => {
         if (failLoad.has(version.versionId)) {
            throw new Error(`cannot load ${version.versionId}`);
         }
         log.push(`load ${version.versionId}`);
         return {
            versionId: version.versionId,
            manifest: version.manifestPath,
         };
      },
      releaseVersion: (_pkg, versionId) => log.push(`release ${versionId}`),
      bindVersionManifest: async (loaded, manifestPath) => {
         loaded.manifest = manifestPath;
         log.push(`bind ${loaded.versionId} ${manifestPath}`);
      },
      downloaderFor: (_pkg, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      admit: () => {},
      withPackageLock: (_pkg, fn) => lock.runExclusive(fn),
      isWatchMounted: async () => false,
      retireUnversioned: () => {},
      ensurePackageRecord: async (packageName) => {
         if (!(await repo.getPackageByName(ENV_ID, packageName))) {
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               manifestPath: "",
            });
         }
      },
      onVersionLoaded: (_pkg, loaded, isLatest) =>
         log.push(`served ${loaded.versionId}${isLatest ? " latest" : ""}`),
      isVersionBuilding: (_pkg, versionId) => building.has(versionId),
      onVersionArchived: (_pkg, versionId) => log.push(`reclaim ${versionId}`),
   };
}

function service() {
   return new VersionService<Loaded>(
      repo,
      ENV_ID,
      new VersionStore(envPath),
      host(),
   );
}

async function publish(
   svc: VersionService<Loaded>,
   versionId: string,
   promotion: "on-publish" | "explicit" = "on-publish",
) {
   const dir = path.join(root, "src", versionId);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, version: versionId }),
   );
   fs.writeFileSync(path.join(dir, "model.malloy"), `// ${versionId}`);
   return svc.publish(
      PKG,
      async (staging) => {
         await fs.promises.cp(dir, staging, { recursive: true });
      },
      { sourceLocation: dir, promotion },
   );
}

async function expectReason(
   promise: Promise<unknown>,
   reason: PackageVersionErrorReason,
) {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(PackageVersionError);
   expect((error as PackageVersionError).reason).toBe(reason);
}

const latest = async () =>
   (await repo.getPackageByName(ENV_ID, PKG))?.latestVersion ?? null;

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-lifecycle-")),
   );
   envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'env', ?, now(), now())",
      [ENV_ID, envPath],
   );
   repo = new DuckDBRepository(db);
   log = [];
   building = new Set();
   failLoad = new Set();
});

afterEach(async () => {
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("listing and getting versions", () => {
   it("lists highest first, archived included, with latest", async () => {
      const svc = service();
      for (const v of ["1.0.0", "2.0.0-rc.1", "1.10.0", "2.0.0"]) {
         await publish(svc, v);
      }
      await svc.setArchiveStatus(PKG, "1.0.0", "archive");
      const listed = await svc.listVersions(PKG);
      expect(listed.latest).toBe("2.0.0");
      expect(listed.versions.map((v) => v.versionId)).toEqual([
         "2.0.0",
         "2.0.0-rc.1",
         "1.10.0",
         "1.0.0",
      ]);
      expect(listed.versions[3].archiveStatus).toBe("archive");
      expect((await svc.listVersions("plain")).versions).toEqual([]);
   });

   it("gets one version, and refuses a malformed or unknown one", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      expect((await svc.getVersion(PKG, "1.0.0")).versionId).toBe("1.0.0");
      await expectReason(svc.getVersion(PKG, "9.0.0"), "VERSION_NOT_FOUND");
      await expectReason(svc.getVersion(PKG, "one"), "VERSION_ID_INVALID");
      await expectReason(svc.getVersion(PKG, ""), "VERSION_ID_INVALID");
   });
});

describe("archive and unarchive", () => {
   it("archives a version: unloaded, reclaimed, refused on reads, files kept", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      expect(svc.cache.isLoaded(PKG, "1.0.0")).toBe(true);
      log = [];

      const archived = await svc.setArchiveStatus(PKG, "1.0.0", "archive");
      expect(archived.archiveStatus).toBe("archive");
      expect(svc.cache.isLoaded(PKG, "1.0.0")).toBe(false);
      expect(log).toEqual(["release 1.0.0", "reclaim 1.0.0"]);
      expect(fs.existsSync(path.join(envPath, PKG, "1.0.0"))).toBe(true);
      await expectReason(svc.getLoaded(PKG, "1.0.0"), "VERSION_ARCHIVED");

      // The same state again changes nothing.
      log = [];
      expect(
         (await svc.setArchiveStatus(PKG, "1.0.0", "archive")).archivedAt,
      ).toEqual(archived.archivedAt);
      expect(log).toEqual([]);
   });

   it("unarchives a version, which loads again on its next read", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      await svc.setArchiveStatus(PKG, "1.0.0", "archive");
      const back = await svc.setArchiveStatus(PKG, "1.0.0", "unarchive");
      expect(back.archiveStatus).toBe("unarchive");
      expect(back.archivedAt).toBeNull();
      expect(svc.cache.isLoaded(PKG, "1.0.0")).toBe(false);
      expect((await svc.getLoaded(PKG, "1.0.0"))!.loaded.versionId).toBe(
         "1.0.0",
      );
   });

   it("refuses to archive latest, the last version in service, and a version that is building", async () => {
      const svc = service();
      await publish(svc, "1.0.0", "explicit");
      await expectReason(
         svc.setArchiveStatus(PKG, "1.0.0", "archive"),
         "VERSION_IS_LAST_ACTIVE",
      );
      await publish(svc, "2.0.0");
      await expectReason(
         svc.setArchiveStatus(PKG, "2.0.0", "archive"),
         "VERSION_IS_LATEST",
      );
      // latest is refused as latest even while it builds.
      building.add("2.0.0");
      await expectReason(
         svc.setArchiveStatus(PKG, "2.0.0", "archive"),
         "VERSION_IS_LATEST",
      );
      building.add("1.0.0");
      await expectReason(
         svc.setArchiveStatus(PKG, "1.0.0", "archive"),
         "VERSION_BUILDING",
      );
      expect((await svc.getVersion(PKG, "1.0.0")).archiveStatus).toBe(
         "unarchive",
      );
      expect(svc.cache.isLoaded(PKG, "1.0.0")).toBe(true);
      await expectReason(
         svc.setArchiveStatus(PKG, "9.0.0", "archive"),
         "VERSION_NOT_FOUND",
      );
   });
});

describe("manifest binding", () => {
   it("binds a loaded version in place, and clears it with null", async () => {
      const svc = service();
      const { loaded } = await publish(svc, "1.0.0");
      expect(await svc.setManifest(PKG, "1.0.0", "gs://m/1.json")).toBe(loaded);
      expect(loaded.manifest).toBe("gs://m/1.json");
      expect((await svc.getVersion(PKG, "1.0.0")).manifestPath).toBe(
         "gs://m/1.json",
      );
      await svc.setManifest(PKG, "1.0.0", null);
      expect(loaded.manifest).toBeNull();
   });

   it("loads a version that is not loaded, bound from the row just written", async () => {
      await publish(service(), "1.0.0");
      const restarted = service();
      const loaded = await restarted.setManifest(PKG, "1.0.0", "s3://m.json");
      expect(loaded.manifest).toBe("s3://m.json");
      expect(log).not.toContain("bind 1.0.0 s3://m.json");
   });

   it("refuses an archived version before writing anything", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      await svc.setArchiveStatus(PKG, "1.0.0", "archive");
      await expectReason(
         svc.setManifest(PKG, "1.0.0", "gs://m/late.json"),
         "VERSION_ARCHIVED",
      );
      expect((await svc.getVersion(PKG, "1.0.0")).manifestPath).toBeNull();
   });
});

describe("moving latest", () => {
   it("moves latest to an existing version, loading it first, and stamps both sides", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      svc.cache.evict(PKG, "1.0.0");
      log = [];

      const moved = await svc.setLatest(PKG, "1.0.0");
      expect(moved.versionId).toBe("1.0.0");
      expect(moved.promotedAt).toBeInstanceOf(Date);
      expect(await latest()).toBe("1.0.0");
      expect((await svc.getVersion(PKG, "2.0.0")).demotedAt).toBeInstanceOf(
         Date,
      );
      expect(log).toEqual([
         "load 1.0.0",
         "served 1.0.0",
         "served 1.0.0 latest",
      ]);
   });

   it("leaves latest where it was when the target cannot load", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      svc.cache.evict(PKG, "1.0.0");
      failLoad.add("1.0.0");
      await expect(svc.setLatest(PKG, "1.0.0")).rejects.toThrow("cannot load");
      expect(await latest()).toBe("2.0.0");
   });

   it("refuses an unknown, archived or malformed target", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      await svc.setArchiveStatus(PKG, "1.0.0", "archive");
      await expectReason(svc.setLatest(PKG, "3.0.0"), "VERSION_NOT_FOUND");
      await expectReason(svc.setLatest(PKG, "1.0.0"), "VERSION_ARCHIVED");
      await expectReason(svc.setLatest(PKG, undefined), "VERSION_ID_INVALID");
      expect(await latest()).toBe("2.0.0");
   });

   it("is a pointer, not a pin: the next publish still advances it", async () => {
      const svc = service();
      await publish(svc, "1.0.0");
      await publish(svc, "2.0.0");
      await svc.setLatest(PKG, "1.0.0");
      await publish(svc, "3.0.0");
      expect(await latest()).toBe("3.0.0");
   });
});
