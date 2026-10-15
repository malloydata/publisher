// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, PackageVersionError } from "../../errors";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { VersionHost, VersionService } from "./version_service";
import { VersionStore } from "./version_store";

// Two publishes of the SAME version id at once, against a real registry and
// real version folders. The same content is one publish and one placement of
// it; different content is one publish and one 409. The races between two
// different versions are covered in version_publish.spec.ts.

const ENV_ID = "env-1";
const PKG = "sales";

interface Loaded {
   versionId: string;
   model: string;
}

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
let log: string[];

function host(): VersionHost<Loaded> {
   const packageLock = new Mutex();
   return {
      loadVersion: async (packageName, versionPath, version) => {
         log.push(`load ${packageName}@${version.versionId}`);
         // A compile takes a while, so the other publish runs meanwhile.
         await new Promise((resolve) => setTimeout(resolve, 10));
         return {
            versionId: version.versionId,
            model: fs.readFileSync(
               path.join(versionPath, "model.malloy"),
               "utf8",
            ),
         };
      },
      releaseVersion: (packageName, versionId) =>
         log.push(`release ${packageName}@${versionId}`),
      bindVersionManifest: async () => {},
      downloaderFor: (_packageName, location) => copyFrom(location),
      admit: () => {},
      withPackageLock: (_packageName, fn) => packageLock.runExclusive(fn),
      isWatchMounted: async () => false,
      holdingUnversionedReads: (_packageName, fn) => fn(),
      retireUnversioned: () => {},
      ensurePackageRecord: async (packageName) => {
         if (await repo.getPackageByName(ENV_ID, packageName)) return false;
         await repo.createPackage({
            environmentId: ENV_ID,
            name: packageName,
            manifestPath: "",
         });
         return true;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   };
}

function copyFrom(location: string) {
   return async (stagingPath: string) => {
      await fs.promises.cp(location, stagingPath, { recursive: true });
   };
}

/** A package source tree declaring `version`, whose model is `model`. */
function source(name: string, version: string, model: string): string {
   const dir = path.join(sourcesPath, name);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, version }),
   );
   fs.writeFileSync(path.join(dir, "model.malloy"), model);
   return dir;
}

function publish(svc: VersionService<Loaded>, location: string) {
   return svc.publish(PKG, copyFrom(location), {
      sourceLocation: location,
      promotion: "on-publish",
   });
}

const versionDirs = () =>
   fs.existsSync(path.join(envPath, PKG))
      ? fs.readdirSync(path.join(envPath, PKG)).sort()
      : [];

const stagingLeft = () =>
   fs.existsSync(path.join(envPath, ".staging"))
      ? fs.readdirSync(path.join(envPath, ".staging"))
      : [];

const modelOnDisk = (dirName: string) =>
   fs.readFileSync(path.join(envPath, PKG, dirName, "model.malloy"), "utf8");

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-same-id-race-")),
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
   log = [];
});

afterEach(async () => {
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

// Both shapes: the package's first version (the publishes queue on the
// package lock) and a later one (they queue on the version's own lock).
const cases = [
   { when: "as the package's first version", priorVersion: false },
   { when: "on a package that already has versions", priorVersion: true },
];

describe("two publishes of the same version at once", () => {
   for (const { when, priorVersion } of cases) {
      it(`with the same content, both succeed and the version is published once, ${when}`, async () => {
         const svc = service();
         if (priorVersion) {
            await publish(svc, source("v1", "1.0.0", "// one"));
            log = [];
         }
         // Two locations holding the same tree: the same content hash.
         const a = source("a", "2.0.0", "// two");
         const b = source("b", "2.0.0", "// two");
         const [first, second] = await Promise.all([
            publish(svc, a),
            publish(svc, b),
         ]);

         // One publish created it; the other was a placement of it.
         expect([first.created, second.created].sort()).toEqual([false, true]);
         expect(first.loaded).toBe(second.loaded);
         expect(first.version.versionId).toBe("2.0.0");
         expect(second.version.versionId).toBe("2.0.0");
         // One row, one folder, one compile; nothing released or left staged.
         const rows = (await repo.listVersions(ENV_ID, PKG)).filter(
            (v) => v.versionId === "2.0.0",
         );
         expect(rows).toHaveLength(1);
         expect(versionDirs()).toEqual(
            priorVersion ? ["1.0.0", "2.0.0"] : ["2.0.0"],
         );
         expect(modelOnDisk("2.0.0")).toBe("// two");
         expect(log).toEqual([`load ${PKG}@2.0.0`]);
         expect(stagingLeft()).toEqual([]);
         expect((await repo.getPackageByName(ENV_ID, PKG))!.latestVersion).toBe(
            "2.0.0",
         );
      });

      it(`with different content, one succeeds and the other is 409 VERSION_CONFLICT, ${when}`, async () => {
         const svc = service();
         if (priorVersion) {
            await publish(svc, source("v1", "1.0.0", "// one"));
         }
         const outcomes = await Promise.allSettled([
            publish(svc, source("a", "2.0.0", "// two from a")),
            publish(svc, source("b", "2.0.0", "// two from b")),
         ]);

         const won = outcomes.filter((o) => o.status === "fulfilled");
         const lost = outcomes.filter((o) => o.status === "rejected");
         expect(won).toHaveLength(1);
         expect(lost).toHaveLength(1);
         const error = (lost[0] as PromiseRejectedResult).reason;
         expect(error).toBeInstanceOf(PackageVersionError);
         expect((error as PackageVersionError).reason).toBe("VERSION_CONFLICT");
         expect(
            internalErrorToHttpError(error as Error, { log: false }).status,
         ).toBe(409);

         // The winner's files are the ones recorded and served.
         const winner = (won[0] as PromiseFulfilledResult<{ loaded: Loaded }>)
            .value.loaded;
         const rows = (await repo.listVersions(ENV_ID, PKG)).filter(
            (v) => v.versionId === "2.0.0",
         );
         expect(rows).toHaveLength(1);
         expect(modelOnDisk("2.0.0")).toBe(winner.model);
         expect(svc.cache.peek(PKG, "2.0.0")).toBe(winner);
         expect(versionDirs()).toEqual(
            priorVersion ? ["1.0.0", "2.0.0"] : ["2.0.0"],
         );
         expect(stagingLeft()).toEqual([]);
      });
   }
});

describe("two registry commits of the same version at once", () => {
   it("records one, and refuses the other as VERSION_CONFLICT on the duplicate key", async () => {
      await repo.createPackage({
         environmentId: ENV_ID,
         name: PKG,
         manifestPath: "",
      });
      const commit = (contentHash: string) =>
         repo.commitPublish(
            {
               versionId: "1.0.0",
               environmentId: ENV_ID,
               packageName: PKG,
               dirName: "1.0.0",
               contentHash,
               sourceLocation: null,
               manifestPath: null,
               description: null,
            },
            () => true,
         );
      const outcomes = await Promise.allSettled([
         commit("hash-a"),
         commit("hash-b"),
      ]);
      expect(outcomes.map((o) => o.status).sort()).toEqual([
         "fulfilled",
         "rejected",
      ]);
      const refused = (
         outcomes.find((o) => o.status === "rejected") as PromiseRejectedResult
      ).reason;
      expect(refused).toBeInstanceOf(PackageVersionError);
      expect((refused as PackageVersionError).reason).toBe("VERSION_CONFLICT");
      expect((refused as Error).message).toMatch(/is already published/);

      // The row is the committed one's, whole: the refused commit rolled back.
      const committed = (
         outcomes.find(
            (o) => o.status === "fulfilled",
         ) as PromiseFulfilledResult<Awaited<ReturnType<typeof commit>>>
      ).value;
      const rows = await repo.listVersions(ENV_ID, PKG);
      expect(rows).toHaveLength(1);
      expect(rows[0].contentHash).toBe(committed.version.contentHash);
      expect((await repo.getPackageByName(ENV_ID, PKG))!.latestVersion).toBe(
         "1.0.0",
      );
   });
});

function service(): VersionService<Loaded> {
   return new VersionService<Loaded>(
      repo,
      ENV_ID,
      new VersionStore(envPath),
      host(),
   );
}
