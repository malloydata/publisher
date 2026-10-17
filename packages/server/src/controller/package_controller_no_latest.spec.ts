// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, PackageVersionError } from "../errors";
import { Environment } from "../service/environment";
import type { EnvironmentStore } from "../service/environment_store";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import type { ResourceRepository } from "../storage/DatabaseInterface";
import { initializeSchema } from "../storage/duckdb/schema";
import { PackageController } from "./package.controller";

// A versioned package with no `latest` yet: published under explicit
// promotion, and no version made latest. Through the package routes'
// controller, over a real Environment, registry and compiled versions.
//
//  - A request that names no version has nothing to be served (404
//    VERSION_NOT_FOUND, "no latest version yet"); one that names a version is
//    served it.
//  - The deprecated PATCH of the package has no version to apply to (409
//    PACKAGE_IS_VERSIONED), and changes nothing.

const ENV_NAME = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let env: Environment;
let controller: PackageController;

function source(version: string, answer: number): string {
   const dir = path.join(root, "sources", version);
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, version }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer")\n`,
   );
   return dir;
}

async function publishExplicit(version: string, answer: number) {
   const location = source(version, answer);
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion: "explicit" },
   );
}

/** The HTTP answer a refused call gets: its status, reason and message. */
async function answered(promise: Promise<unknown>) {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(PackageVersionError);
   const { status, json } = internalErrorToHttpError(error as Error, {
      log: false,
   });
   return {
      status,
      reason: (json as { reason?: string }).reason,
      message: (json as { message: string }).message,
   };
}

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "package-no-latest-")),
   );
   envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV_NAME, envPath],
   );
   repo = createDuckDBRepository(db);
   env = await Environment.create(ENV_NAME, envPath, []);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      ensurePackageRecord: async (packageName, description) => {
         const existing = await repo.getPackageByName(ENV_ID, packageName);
         if (!existing) {
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description: description ?? undefined,
               manifestPath: "",
            });
            return true;
         }
         if (description !== undefined) {
            await repo.updatePackage(existing.id, { description });
         }
         return false;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   });
   controller = new PackageController({
      publisherConfigIsFrozen: false,
      serverRootPath: root,
      getEnvironment: async () => env,
   } as unknown as EnvironmentStore);

   await publishExplicit("1.0.0", 1);
   await publishExplicit("2.0.0", 2);
});

afterEach(async () => {
   await env.closeAllConnections();
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("a versioned package with no latest yet", () => {
   it("is published, with no latest", async () => {
      expect(await env.getVersionService()!.latestOf(PKG)).toBeNull();
      expect(await env.getVersionService()!.isVersioned(PKG)).toBe(true);
   });

   it("answers a request that names no version with 404 VERSION_NOT_FOUND", async () => {
      for (const versionId of [undefined, null, ""]) {
         const refused = await answered(
            controller.getPackage(ENV_NAME, PKG, false, versionId),
         );
         expect(refused).toMatchObject({
            status: 404,
            reason: "VERSION_NOT_FOUND",
         });
         expect(refused.message).toMatch(
            /Package sales has no latest version yet; name a version, or make one latest\./,
         );
      }
      // A reload, and the environment read every model and query route takes.
      expect(
         await answered(controller.getPackage(ENV_NAME, PKG, true)),
      ).toMatchObject({ status: 404, reason: "VERSION_NOT_FOUND" });
      expect(await answered(env.getPackage(PKG))).toMatchObject({
         status: 404,
         reason: "VERSION_NOT_FOUND",
      });
   });

   it("serves a request that names a version", async () => {
      const named = await controller.getPackage(ENV_NAME, PKG, false, "1.0.0");
      expect(named).toMatchObject({ versionId: "1.0.0", latestVersion: null });
      const other = await env.getPackage(PKG, false, { versionId: "2.0.0" });
      expect(other.getPackageMetadata().versionId).toBe("2.0.0");
   });

   it("serves a request that names no version once a version is made latest", async () => {
      await env.getVersionService()!.setLatest(PKG, "1.0.0");
      const served = await controller.getPackage(ENV_NAME, PKG, false);
      expect(served).toMatchObject({
         versionId: "1.0.0",
         latestVersion: "1.0.0",
      });
   });

   it("refuses the deprecated PATCH of the package with 409 PACKAGE_IS_VERSIONED, changing nothing", async () => {
      const bodies = [
         { manifestLocation: "gs://bucket/sales.manifest.json" },
         { description: "Sales, renamed" },
      ];
      for (const body of bodies) {
         const refused = await answered(
            controller.updatePackage(ENV_NAME, PKG, body),
         );
         expect(refused).toMatchObject({
            status: 409,
            reason: "PACKAGE_IS_VERSIONED",
         });
         expect(refused.message).toMatch(
            /Package sales has published versions, which are immutable: it has no latest version to apply this to\./,
         );
      }
      for (const version of await repo.listVersions(ENV_ID, PKG)) {
         expect(version.manifestPath).toBeNull();
      }
      expect((await repo.getPackageByName(ENV_ID, PKG))!.description).toBe(
         null,
      );
      expect(await env.getVersionService()!.latestOf(PKG)).toBeNull();
   });

   it("applies the same PATCH once a version is made latest", async () => {
      // So the 409 above is the missing latest, not the body.
      await env.getVersionService()!.setLatest(PKG, "2.0.0");
      const patched = await controller.updatePackage(ENV_NAME, PKG, {
         description: "Sales, renamed",
      });
      expect(patched).toMatchObject({
         versionId: "2.0.0",
         description: "Sales, renamed",
      });
   });
});
