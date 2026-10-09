// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { VersionPromotionMode } from "../../config";
import {
   BadRequestError,
   PackageVersionError,
   type PackageVersionErrorReason,
} from "../../errors";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { VersionHost, VersionService } from "./version_service";
import { VersionStore } from "./version_store";

const ENV_ID = "env-1";
const PKG = "sales";

/** What the fake host "compiles": the folder it was handed and its binding. */
interface Loaded {
   versionId: string;
   path: string;
   manifest: string | null;
   model: string;
}

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
let store: VersionStore;
let log: string[];
let failLoadFor: Set<string>;
let watchMounted: boolean;
let descriptions: (string | undefined)[];
let packageLock: Mutex;

function host(): VersionHost<Loaded> {
   return {
      loadVersion: async (packageName, versionPath, version) => {
         if (failLoadFor.has(version.versionId)) {
            throw new Error(`compile failed for ${version.versionId}`);
         }
         log.push(`load ${packageName}@${version.versionId}`);
         return {
            versionId: version.versionId,
            path: versionPath,
            manifest: version.manifestPath,
            model: fs.readFileSync(
               path.join(versionPath, "model.malloy"),
               "utf8",
            ),
         };
      },
      releaseVersion: (packageName, versionId) =>
         log.push(`release ${packageName}@${versionId}`),
      bindVersionManifest: async (loaded, manifestPath) => {
         loaded.manifest = manifestPath;
         log.push(`bind ${loaded.versionId} ${manifestPath}`);
      },
      downloaderFor: (_packageName, location) => copyFrom(location),
      admit: () => {},
      withPackageLock: (_packageName, fn) => packageLock.runExclusive(fn),
      isWatchMounted: async () => watchMounted,
      retireUnversioned: (packageName) =>
         log.push(`retire unversioned ${packageName}`),
      ensurePackageRecord: async (packageName, description) => {
         descriptions.push(description);
         if (!(await repo.getPackageByName(ENV_ID, packageName))) {
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               manifestPath: "",
               description,
            });
         }
      },
   };
}

/** Write a package source tree to publish from, and return its location. */
function source(
   name: string,
   manifest: Record<string, unknown>,
   model = "source: s is x",
): string {
   const dir = path.join(sourcesPath, name);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   fs.writeFileSync(path.join(dir, "model.malloy"), model);
   return dir;
}

function copyFrom(location: string) {
   return async (stagingPath: string) => {
      await fs.promises.cp(location, stagingPath, { recursive: true });
   };
}

function service(): VersionService<Loaded> {
   return new VersionService<Loaded>(repo, ENV_ID, store, host());
}

async function publish(
   svc: VersionService<Loaded>,
   location: string,
   extra: {
      promotion?: VersionPromotionMode;
      manifestLocation?: string | null;
      description?: string;
      validate?: (loaded: Loaded) => string | undefined;
      packageName?: string;
   } = {},
) {
   return svc.publish(extra.packageName ?? PKG, copyFrom(location), {
      sourceLocation: location,
      promotion: extra.promotion ?? "on-publish",
      manifestLocation: extra.manifestLocation,
      description: extra.description,
      validate: extra.validate,
   });
}

async function latest(): Promise<string | null> {
   return (await repo.getPackageByName(ENV_ID, PKG))?.latestVersion ?? null;
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

const versionDirs = () =>
   fs.existsSync(path.join(envPath, PKG))
      ? fs.readdirSync(path.join(envPath, PKG)).sort()
      : [];

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-publish-")),
   );
   envPath = path.join(root, "env");
   sourcesPath = path.join(root, "sources");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'env', ?, now(), now())",
      [ENV_ID, envPath],
   );
   repo = new DuckDBRepository(db);
   store = new VersionStore(envPath);
   log = [];
   failLoadFor = new Set();
   watchMounted = false;
   descriptions = [];
   packageLock = new Mutex();
});

afterEach(async () => {
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("VersionService.publish", () => {
   it("places, compiles, records and promotes a new version", async () => {
      const svc = service();
      const result = await publish(
         svc,
         source("v1", { version: "1.0.0", description: "first cut" }),
         { manifestLocation: "gs://m/1.json", description: "Sales package" },
      );
      expect(result.created).toBe(true);
      expect(result.loaded).toMatchObject({
         versionId: "1.0.0",
         path: path.join(envPath, PKG, "1.0.0"),
         manifest: "gs://m/1.json",
      });
      expect(result.version).toMatchObject({
         versionId: "1.0.0",
         dirName: "1.0.0",
         description: "first cut",
         manifestPath: "gs://m/1.json",
         sourceLocation: path.join(sourcesPath, "v1"),
         archiveStatus: "unarchive",
      });
      expect(await latest()).toBe("1.0.0");
      expect(descriptions).toEqual(["Sales package"]);
      expect(versionDirs()).toEqual(["1.0.0"]);
      expect(svc.cache.peek(PKG, "1.0.0")).toBe(result.loaded);
      expect(log).toEqual(["load sales@1.0.0", "retire unversioned sales"]);
   });

   it("moves an unversioned tree aside for the first version, and drops it once committed", async () => {
      fs.mkdirSync(path.join(envPath, PKG));
      fs.writeFileSync(path.join(envPath, PKG, "publisher.json"), "{}");
      await publish(service(), source("v1", { version: "1.0.0" }));
      expect(versionDirs()).toEqual(["1.0.0"]);
      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
   });

   it("refuses a version the publish checks reject, leaving nothing behind and the unversioned tree back", async () => {
      fs.mkdirSync(path.join(envPath, PKG));
      fs.writeFileSync(path.join(envPath, PKG, "old.malloy"), "old");
      const error = await publish(
         service(),
         source("v1", { version: "1.0.0" }),
         {
            validate: () => "explores names a file that does not exist",
         },
      ).catch((err: unknown) => err);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(fs.readdirSync(path.join(envPath, PKG))).toEqual(["old.malloy"]);
      expect(await repo.listVersions(ENV_ID, PKG)).toEqual([]);
      expect(await latest()).toBeNull();
      expect(log).toEqual(["load sales@1.0.0", "release sales@1.0.0"]);
   });

   it("refuses a version that does not compile, and the versions already published keep serving", async () => {
      const svc = service();
      await publish(svc, source("v1", { version: "1.0.0" }));
      failLoadFor.add("2.0.0");
      await expect(
         publish(svc, source("v2", { version: "2.0.0" })),
      ).rejects.toThrow("compile failed");
      expect(versionDirs()).toEqual(["1.0.0"]);
      expect(await latest()).toBe("1.0.0");
      expect(await repo.getVersion(ENV_ID, PKG, "2.0.0")).toBeNull();
   });

   it("refuses a tree without a version, before anything is placed", async () => {
      await expectReason(
         publish(service(), source("nov", { name: "sales" })),
         "MANIFEST_VERSION_MISSING",
      );
      await expectReason(
         publish(service(), source("bad", { version: "one" })),
         "MANIFEST_VERSION_INVALID",
      );
      expect(versionDirs()).toEqual([]);
   });

   it("refuses a watch-mounted package", async () => {
      watchMounted = true;
      await expect(
         publish(service(), source("v1", { version: "1.0.0" })),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("advances latest unless a later version is already latest", async () => {
      const svc = service();
      await publish(svc, source("v2", { version: "2.0.0" }));
      await publish(svc, source("v15", { version: "1.5.0" }));
      expect(await latest()).toBe("2.0.0");
      await publish(svc, source("rc", { version: "2.1.0-rc.1" }));
      expect(await latest()).toBe("2.1.0-rc.1");
      // A tie by precedence (build metadata only): the newer publish wins.
      await publish(svc, source("b1", { version: "3.0.0+b1" }));
      await publish(svc, source("b2", { version: "3.0.0+b2" }));
      expect(await latest()).toBe("3.0.0+b2");
      expect(versionDirs()).toContain("3.0.0_b2");
   });

   it("never moves latest under explicit promotion", async () => {
      const svc = service();
      await publish(svc, source("v1", { version: "1.0.0" }), {
         promotion: "explicit",
      });
      expect(await latest()).toBeNull();
      expect(
         (await repo.getVersion(ENV_ID, PKG, "1.0.0"))!.promotedAt,
      ).toBeNull();
   });

   it("refuses a version differing from a published one only by case", async () => {
      const svc = service();
      await publish(svc, source("a", { version: "1.0.0-RC1" }));
      await expectReason(
         publish(svc, source("b", { version: "1.0.0-rc1" })),
         "VERSION_CONFLICT",
      );
   });

   it("publishes two versions in parallel", async () => {
      const svc = service();
      const [a, b] = await Promise.all([
         publish(svc, source("v1", { version: "1.0.0" })),
         publish(svc, source("v2", { version: "2.0.0" })),
      ]);
      expect([a.created, b.created]).toEqual([true, true]);
      expect(versionDirs()).toEqual(["1.0.0", "2.0.0"]);
      expect(await latest()).toBe("2.0.0");
      expect(log.filter((line) => line.startsWith("retire"))).toHaveLength(1);
   });
});

describe("VersionService.publish of a version already published", () => {
   it("is a no-op for the same content, and binds a new manifest", async () => {
      const svc = service();
      const location = source("v1", { version: "1.0.0" });
      const first = await publish(svc, location);
      const before = fs.statSync(
         path.join(envPath, PKG, "1.0.0", "model.malloy"),
      );

      const again = await publish(svc, location, {
         manifestLocation: "s3://m/after-build.json",
      });
      expect(again.created).toBe(false);
      expect(again.loaded).toBe(first.loaded);
      expect(again.loaded.manifest).toBe("s3://m/after-build.json");
      expect(again.version.manifestPath).toBe("s3://m/after-build.json");
      const after = fs.statSync(
         path.join(envPath, PKG, "1.0.0", "model.malloy"),
      );
      expect(after.mtimeMs).toBe(before.mtimeMs);
      expect(after.ino).toBe(before.ino);

      // A null manifest on a re-load leaves the binding alone.
      const third = await publish(svc, location, { manifestLocation: null });
      expect(third.version.manifestPath).toBe("s3://m/after-build.json");
   });

   it("refuses different content under the same version", async () => {
      const svc = service();
      await publish(svc, source("v1", { version: "1.0.0" }));
      await expectReason(
         publish(svc, source("v1b", { version: "1.0.0" }, "source: s is y")),
         "VERSION_CONFLICT",
      );
      expect(
         fs.readFileSync(
            path.join(envPath, PKG, "1.0.0", "model.malloy"),
            "utf8",
         ),
      ).toBe("source: s is x");
   });

   it("refuses an archived version", async () => {
      const svc = service();
      const v1 = source("v1", { version: "1.0.0" });
      await publish(svc, v1);
      await publish(svc, source("v2", { version: "2.0.0" }));
      await repo.setVersionArchiveStatus(ENV_ID, PKG, "1.0.0", "archive");
      await expectReason(publish(svc, v1), "VERSION_ARCHIVED");
   });

   it("puts a missing folder back from the staged copy", async () => {
      const svc = service();
      const location = source("v1", { version: "1.0.0" });
      await publish(svc, location);
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });
      await publish(svc, location);
      expect(versionDirs()).toEqual(["1.0.0"]);
   });

   it("moves latest only to a strictly higher version", async () => {
      const svc = service();
      const v1 = source("v1", { version: "1.0.0" });
      await publish(svc, v1);
      await publish(svc, source("v2", { version: "2.0.0" }));
      await publish(svc, v1);
      expect(await latest()).toBe("2.0.0");

      // Published under explicit promotion, then re-loaded under on-publish.
      const v3 = source("v3", { version: "3.0.0" });
      await publish(svc, v3, { promotion: "explicit" });
      expect(await latest()).toBe("2.0.0");
      await publish(svc, v3);
      expect(await latest()).toBe("3.0.0");
   });
});

describe("VersionService.getLoaded", () => {
   it("loads a published version on first use, and fetches a missing folder again", async () => {
      const location = source("v1", { version: "1.0.0" });
      await publish(service(), location);
      // A restarted server: an empty cache, and the files gone.
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });
      const fresh = service();
      const got = await fresh.getLoaded(PKG);
      expect(got!.version.versionId).toBe("1.0.0");
      expect(got!.loaded.model).toBe("source: s is x");
      expect(versionDirs()).toEqual(["1.0.0"]);
   });

   it("refuses to serve a missing folder whose location changed", async () => {
      const location = source("v1", { version: "1.0.0" });
      await publish(service(), location);
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });
      fs.writeFileSync(path.join(location, "model.malloy"), "edited");
      await expect(service().getLoaded(PKG, "1.0.0")).rejects.toThrow(
         /missing and could not be fetched again/,
      );
   });

   it("is null for a package with no versions", async () => {
      expect(await service().getLoaded("plain")).toBeNull();
   });
});
