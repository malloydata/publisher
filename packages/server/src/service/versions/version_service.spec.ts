// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import {
   PackageVersionError,
   type PackageVersionErrorReason,
} from "../../errors";
import { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../storage/duckdb/schema";
import { requestedVersionId, VersionService } from "./version_service";

const ENV_ID = "env-1";

const dbs: DuckDBConnection[] = [];

afterEach(async () => {
   while (dbs.length) await dbs.pop()!.close();
});

async function freshRegistry(): Promise<DuckDBRepository> {
   const db = new DuckDBConnection(":memory:");
   dbs.push(db);
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'env', '/env', now(), now())",
      [ENV_ID],
   );
   return new DuckDBRepository(db);
}

async function addPackage(repo: DuckDBRepository, name: string) {
   await repo.createPackage({
      environmentId: ENV_ID,
      name,
      manifestPath: `/env/${name}`,
   });
}

async function publish(
   repo: DuckDBRepository,
   name: string,
   versionId: string,
   promote: boolean,
) {
   await repo.commitPublish(
      {
         versionId,
         environmentId: ENV_ID,
         packageName: name,
         dirName: versionId.replace(/\+/g, "_"),
         contentHash: `h-${versionId}`,
         sourceLocation: null,
         manifestPath: null,
         description: null,
      },
      () => promote,
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

describe("requestedVersionId", () => {
   it("reads absent, empty and null as no version", () => {
      expect(requestedVersionId(undefined)).toBeUndefined();
      expect(requestedVersionId(null)).toBeUndefined();
      expect(requestedVersionId("")).toBeUndefined();
   });

   it("passes a semantic version through, and refuses anything else with 400", () => {
      expect(requestedVersionId("1.2.0-rc.1+b.5")).toBe("1.2.0-rc.1+b.5");
      for (const bad of ["latest", "1.2", "v1.2.0", " 1.2.0", ["1.0.0"], 1]) {
         expect(() => requestedVersionId(bad)).toThrow(PackageVersionError);
         try {
            requestedVersionId(bad);
         } catch (err) {
            expect((err as PackageVersionError).reason).toBe(
               "VERSION_ID_INVALID",
            );
         }
      }
   });
});

describe("VersionService.resolve", () => {
   it("resolves a package with no versions to null, and refuses a named version on it", async () => {
      const repo = await freshRegistry();
      await addPackage(repo, "plain");
      const service = new VersionService(repo, ENV_ID);
      expect(await service.resolve("plain")).toBeNull();
      expect(await service.resolve("plain", "")).toBeNull();
      await expectReason(
         service.resolve("plain", "1.0.0"),
         "VERSION_NOT_FOUND",
      );
      // A package the registry has never heard of behaves the same way.
      expect(await service.resolve("unknown")).toBeNull();
   });

   it("serves latest when no version is named, and the named one otherwise", async () => {
      const repo = await freshRegistry();
      await addPackage(repo, "sales");
      await publish(repo, "sales", "1.0.0", true);
      await publish(repo, "sales", "2.0.0", true);
      const service = new VersionService(repo, ENV_ID);
      expect((await service.resolve("sales"))!.versionId).toBe("2.0.0");
      expect((await service.resolve("sales", null))!.versionId).toBe("2.0.0");
      expect((await service.resolve("sales", "1.0.0"))!.versionId).toBe(
         "1.0.0",
      );
   });

   it("refuses an unknown, an archived and a malformed version", async () => {
      const repo = await freshRegistry();
      await addPackage(repo, "sales");
      await publish(repo, "sales", "1.0.0", true);
      await publish(repo, "sales", "2.0.0", true);
      await repo.setVersionArchiveStatus(ENV_ID, "sales", "1.0.0", "archive");
      const service = new VersionService(repo, ENV_ID);
      await expectReason(
         service.resolve("sales", "3.0.0"),
         "VERSION_NOT_FOUND",
      );
      await expectReason(service.resolve("sales", "1.0.0"), "VERSION_ARCHIVED");
      await expectReason(service.resolve("sales", "two"), "VERSION_ID_INVALID");
   });

   it("is 404 for a versioned package with no latest yet", async () => {
      const repo = await freshRegistry();
      await addPackage(repo, "sales");
      await publish(repo, "sales", "1.0.0", false);
      const service = new VersionService(repo, ENV_ID);
      await expectReason(service.resolve("sales"), "VERSION_NOT_FOUND");
      expect((await service.resolve("sales", "1.0.0"))!.versionId).toBe(
         "1.0.0",
      );
      expect(await service.isVersioned("sales")).toBe(true);
   });
});
