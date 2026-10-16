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
let descriptions: (string | null | undefined)[];
let packageLock: Mutex;
/** Loads of these versions wait, inside their version's lock, until released. */
let heldLoads: Map<string, Promise<void>>;
let loadsWaiting: string[];

function host(): VersionHost<Loaded> {
   return {
      loadVersion: async (packageName, versionPath, version) => {
         const held = heldLoads.get(version.versionId);
         if (held) {
            loadsWaiting.push(version.versionId);
            await held;
         }
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
      holdingUnversionedReads: (_packageName, fn) => fn(),
      retireUnversioned: (packageName) =>
         log.push(`retire unversioned ${packageName}`),
      ensurePackageRecord: async (packageName, description) => {
         descriptions.push(description);
         const existing = await repo.getPackageByName(ENV_ID, packageName);
         if (!existing) {
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               manifestPath: "",
               description: description ?? undefined,
            });
            return true;
         }
         if (description !== undefined) {
            await repo.updatePackage(existing.id, { description });
         }
         return false;
      },
      removePackageRecord: async (packageName) => {
         log.push(`remove record ${packageName}`);
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
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
   heldLoads = new Map();
   loadsWaiting = [];
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
      // The row is made with no description, which is set once committed.
      expect(descriptions).toEqual([undefined, "Sales package"]);
      expect((await repo.getPackageByName(ENV_ID, PKG))!.description).toBe(
         "Sales package",
      );
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
      // The package row this publish created goes with it.
      expect(await repo.getPackageByName(ENV_ID, PKG)).toBeNull();
      expect(log).toEqual([
         "load sales@1.0.0",
         "release sales@1.0.0",
         "remove record sales",
      ]);
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

   it("publishes a version with leading zeros, and refuses the same version written without them, either way round", async () => {
      const svc = service();
      const padded = await publish(svc, source("a", { version: "01.0.0" }));
      expect(padded.version.versionId).toBe("01.0.0");
      await expectReason(
         publish(svc, source("b", { version: "1.0.0" })),
         "VERSION_CONFLICT",
      );
      await expectReason(
         publish(svc, source("c", { version: "1.00.0" })),
         "VERSION_CONFLICT",
      );
      await publish(svc, source("d", { version: "1.0.0-rc.1" }));
      await expectReason(
         publish(svc, source("e", { version: "01.0.0-rc.01" })),
         "VERSION_CONFLICT",
      );
      // Only the first of each was published, and build metadata's digits
      // still make a version of their own.
      await publish(svc, source("f", { version: "1.0.0+001" }));
      await publish(svc, source("g", { version: "1.0.0+1" }));
      expect(versionDirs()).toEqual([
         "01.0.0",
         "1.0.0-rc.1",
         "1.0.0_001",
         "1.0.0_1",
      ]);
   });

   it("lets only one of two spellings of a version publish when both arrive at once", async () => {
      const svc = service();
      // Not the package's first version, which takes the package's own lock:
      // here only the version's lock keeps the two apart.
      await publish(svc, source("earlier", { version: "0.1.0" }));
      // The first holds its compile, past its check for a twin and before
      // it commits.
      let release!: () => void;
      heldLoads.set(
         "1.0.0",
         new Promise<void>((resolve) => (release = resolve)),
      );
      const first = publish(svc, source("a", { version: "1.0.0" }));
      while (!loadsWaiting.includes("1.0.0")) {
         await new Promise((resolve) => setTimeout(resolve, 5));
      }
      // The second has time to check for a twin, and would commit first, if
      // the two spellings did not share one lock.
      const second = publish(svc, source("b", { version: "01.0.0" }));
      await new Promise((resolve) => setTimeout(resolve, 100));
      release();
      expect((await first).version.versionId).toBe("1.0.0");
      await expectReason(second, "VERSION_CONFLICT");
      expect(versionDirs()).toEqual(["0.1.0", "1.0.0"]);
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

   it("leaves latest where it is: an older or a newer version published again moves nothing", async () => {
      const svc = service();
      const v1 = source("v1", { version: "1.0.0" });
      const v2 = source("v2", { version: "2.0.0" });
      await publish(svc, v1);
      await publish(svc, v2);
      await publish(svc, v1);
      expect(await latest()).toBe("2.0.0");

      // Rolled back, then the newer version's publish replayed (a retry):
      // the rollback stands.
      await svc.setLatest(PKG, "1.0.0");
      await publish(svc, v2);
      expect(await latest()).toBe("1.0.0");

      // Published under explicit promotion, then placed again under
      // on-publish: still a placement, so latest stays.
      const v3 = source("v3", { version: "3.0.0" });
      await publish(svc, v3, { promotion: "explicit" });
      await publish(svc, v3);
      expect(await latest()).toBe("1.0.0");
   });

   it("gives a package with no latest the version placed again", async () => {
      // Published under explicit promotion, nothing set latest; placed again
      // under on-publish, it is the only candidate.
      const svc = service();
      const v1 = source("v1", { version: "1.0.0" });
      await publish(svc, v1, { promotion: "explicit" });
      expect(await latest()).toBeNull();
      await publish(svc, v1);
      expect(await latest()).toBe("1.0.0");
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
