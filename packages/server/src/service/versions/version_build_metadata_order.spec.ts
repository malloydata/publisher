// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { VersionHost, VersionService } from "./version_service";
import { VersionStore } from "./version_store";

// Two versions that differ only in build metadata (`1.0.0+a`, `1.0.0+b`) share
// semver precedence, so neither is higher: the later PUBLISH is ordered first
// and, under on-publish promotion, becomes `latest`. Each case publishes the
// lexically higher build first, so "later publish" is told apart from an
// ordering by the build string.

const ENV_ID = "env-1";
const PKG = "sales";

interface Loaded {
   versionId: string;
}

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;

function host(): VersionHost<Loaded> {
   const lock = new Mutex();
   return {
      loadVersion: async (_pkg, _path, version) => ({
         versionId: version.versionId,
      }),
      releaseVersion: () => {},
      bindVersionManifest: async () => {},
      downloaderFor: (_pkg, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      admit: () => {},
      withPackageLock: (_pkg, fn) => lock.runExclusive(fn),
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
   const result = await svc.publish(
      PKG,
      async (staging) => {
         await fs.promises.cp(dir, staging, { recursive: true });
      },
      { sourceLocation: dir, promotion },
   );
   // Each publish gets a creation time of its own.
   await new Promise((resolve) => setTimeout(resolve, 5));
   return result;
}

const latest = async () =>
   (await repo.getPackageByName(ENV_ID, PKG))?.latestVersion ?? null;

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "version-build-metadata-")),
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
});

afterEach(async () => {
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("versions equal in precedence, differing only in build metadata", () => {
   it("are listed later publish first", async () => {
      const svc = service();
      await publish(svc, "1.0.0+b");
      await publish(svc, "1.0.0+a");
      const listed = await svc.listVersions(PKG);
      expect(listed.versions.map((v) => v.versionId)).toEqual([
         "1.0.0+a",
         "1.0.0+b",
      ]);
      // Each in its own folder, `+` written as `_`.
      expect(listed.versions.map((v) => v.dirName)).toEqual([
         "1.0.0_a",
         "1.0.0_b",
      ]);
   });

   it("are listed later publish first among higher and lower versions, whichever is latest", async () => {
      const svc = service();
      // Explicit promotion, so `latest` says nothing about the order.
      for (const v of ["1.0.0+z", "2.0.0", "1.0.0+m", "0.9.0", "1.0.0+a"]) {
         await publish(svc, v, "explicit");
      }
      expect(
         (await svc.listVersions(PKG)).versions.map((v) => v.versionId),
      ).toEqual(["2.0.0", "1.0.0+a", "1.0.0+m", "1.0.0+z", "0.9.0"]);
   });

   it("under on-publish promotion, make the later publish latest", async () => {
      const svc = service();
      await publish(svc, "1.0.0+b");
      expect(await latest()).toBe("1.0.0+b");
      await publish(svc, "1.0.0+a");
      expect(await latest()).toBe("1.0.0+a");
      expect((await svc.listVersions(PKG)).latest).toBe("1.0.0+a");
      // The one it replaced is stamped as demoted.
      expect((await svc.getVersion(PKG, "1.0.0+b")).demotedAt).toBeInstanceOf(
         Date,
      );
      // And a request naming no version is served the later publish.
      expect((await svc.resolve(PKG))!.versionId).toBe("1.0.0+a");
   });
});
