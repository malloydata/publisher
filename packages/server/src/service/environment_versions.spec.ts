// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { internalErrorToHttpError, PackageVersionError } from "../errors";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { DuckDBRepository } from "../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../storage/duckdb/schema";
import { Environment } from "./environment";
import { Package } from "./package";

// Loading published versions through a real Environment: real packages
// compiled from real folders, a real registry, and real queries, so what each
// read answers proves which version's files it came from.

const ENV_ID = "env-1";
const PKG = "sales";

let root: string;
let envPath: string;
let sourcesPath: string;
let db: DuckDBConnection;
let repo: DuckDBRepository;
const savedFlag = process.env.PUBLISHER_PACKAGE_VERSIONING;

/** A package tree whose one view answers `answer`, so a query names its version. */
function writePackage(
   dir: string,
   manifest: Record<string, unknown>,
   answer: number,
): string {
   fs.mkdirSync(dir, { recursive: true });
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: PKG, ...manifest }),
   );
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {\n  view: which_version is { select: answer }\n}\n`,
   );
   return dir;
}

function source(version: string, answer: number): string {
   return writePackage(
      path.join(sourcesPath, version),
      { version, description: `release ${version}` },
      answer,
   );
}

async function answerOf(pkg: Package): Promise<number> {
   const model = pkg.getModel("model.malloy");
   expect(model).toBeDefined();
   const result = await model!.getQueryResults(
      undefined,
      undefined,
      "run: numbers -> which_version",
   );
   const rows = (result as unknown as { compactResult?: { answer: number }[] })
      .compactResult;
   return Number(rows![0].answer);
}

async function newEnvironment(): Promise<Environment> {
   const env = await Environment.create("testEnv", envPath, []);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      ensurePackageRecord: async (packageName, description) => {
         if (!(await repo.getPackageByName(ENV_ID, packageName))) {
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description,
               manifestPath: "",
            });
         }
      },
   });
   return env;
}

async function publish(env: Environment, location: string) {
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion: "on-publish" },
   );
}

async function refusal(promise: Promise<unknown>) {
   const error = await promise.then(
      () => undefined,
      (err: unknown) => err,
   );
   expect(error).toBeInstanceOf(PackageVersionError);
   const { status, json } = internalErrorToHttpError(error as Error, {
      log: false,
   });
   return { status, reason: (json as { reason?: string }).reason };
}

beforeEach(async () => {
   process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "environment-versions-")),
   );
   envPath = path.join(root, "env");
   sourcesPath = path.join(root, "sources");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, 'testEnv', ?, now(), now())",
      [ENV_ID, envPath],
   );
   repo = new DuckDBRepository(db);
});

afterEach(async () => {
   if (savedFlag === undefined) delete process.env.PUBLISHER_PACKAGE_VERSIONING;
   else process.env.PUBLISHER_PACKAGE_VERSIONING = savedFlag;
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

describe("Environment loading of published versions", () => {
   it("serves each version from its own folder, and latest when none is named", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0", 1));
      await publish(env, source("2.0.0", 2));

      const latest = await env.getPackage(PKG);
      expect(await answerOf(latest)).toBe(2);
      expect(latest.getPackagePath()).toBe(path.join(envPath, PKG, "2.0.0"));
      expect(latest.getPackageMetadata().versionId).toBe("2.0.0");

      const first = await env.getPackage(PKG, false, { versionId: "1.0.0" });
      expect(await answerOf(first)).toBe(1);
      expect(first.getPackagePath()).toBe(path.join(envPath, PKG, "1.0.0"));
      expect(first.getPackageMetadata().versionId).toBe("1.0.0");
      expect(first.getPackageMetadata().location).toBe(
         path.join(sourcesPath, "1.0.0"),
      );

      // An empty versionId is no version: latest.
      expect(await env.getPackage(PKG, false, { versionId: "" })).toBe(latest);
   });

   it("keeps a version loaded once it stops being latest, and never reloads one", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0", 1))).loaded;
      await publish(env, source("2.0.0", 2));
      const create = spyOn(Package, "create");
      try {
         expect(await env.getPackage(PKG, false, { versionId: "1.0.0" })).toBe(
            v1,
         );
         // reload=true on a published version returns it as it is.
         expect(await env.getPackage(PKG, true, { versionId: "1.0.0" })).toBe(
            v1,
         );
         expect(create).not.toHaveBeenCalled();
      } finally {
         create.mockRestore();
      }
   });

   it("after a restart, loads a version on first use, once, however many reads race", async () => {
      await publish(await newEnvironment(), source("1.0.0", 1));
      await publish(await newEnvironment(), source("2.0.0", 2));

      const restarted = await newEnvironment();
      const create = spyOn(Package, "create");
      try {
         const reads = await Promise.all([
            restarted.getPackage(PKG),
            restarted.getPackage(PKG),
            restarted.getPackage(PKG, false, { versionId: "2.0.0" }),
            restarted.getPackage(PKG, false, { versionId: "1.0.0" }),
            restarted.getPackage(PKG, false, { versionId: "1.0.0" }),
         ]);
         expect(reads[0]).toBe(reads[1]);
         expect(reads[0]).toBe(reads[2]);
         expect(reads[3]).toBe(reads[4]);
         expect(reads[0]).not.toBe(reads[3]);
         expect(await answerOf(reads[0])).toBe(2);
         expect(await answerOf(reads[3])).toBe(1);
         // One load per version, in parallel.
         expect(create).toHaveBeenCalledTimes(2);
      } finally {
         create.mockRestore();
      }
   });

   it("fetches a missing version folder again from where it was published", async () => {
      await publish(await newEnvironment(), source("1.0.0", 1));
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });

      const restarted = await newEnvironment();
      expect(await answerOf(await restarted.getPackage(PKG))).toBe(1);
      expect(
         fs.existsSync(path.join(envPath, PKG, "1.0.0", "model.malloy")),
      ).toBe(true);
   });

   it("does not serve a missing folder whose location now holds other content", async () => {
      const location = source("1.0.0", 1);
      await publish(await newEnvironment(), location);
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });
      writePackage(location, { version: "1.0.0" }, 99);

      const restarted = await newEnvironment();
      await expect(restarted.getPackage(PKG)).rejects.toThrow(
         /missing and could not be fetched again/,
      );
      expect(fs.existsSync(path.join(envPath, PKG, "1.0.0"))).toBe(false);
   });

   it("refuses an unknown, an archived and a malformed version", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0", 1));
      await publish(env, source("2.0.0", 2));
      await repo.setVersionArchiveStatus(ENV_ID, PKG, "1.0.0", "archive");

      expect(
         await refusal(env.getPackage(PKG, false, { versionId: "3.0.0" })),
      ).toEqual({ status: 404, reason: "VERSION_NOT_FOUND" });
      expect(
         await refusal(env.getPackage(PKG, false, { versionId: "1.0.0" })),
      ).toEqual({ status: 410, reason: "VERSION_ARCHIVED" });
      expect(
         await refusal(env.getPackage(PKG, false, { versionId: "two" })),
      ).toEqual({ status: 400, reason: "VERSION_ID_INVALID" });
      expect(
         await refusal(
            env.getPackage(PKG, false, { versionId: ["1.0.0", "2.0.0"] }),
         ),
      ).toEqual({ status: 400, reason: "VERSION_ID_INVALID" });
   });

   it("turns a served unversioned package into a versioned one at its first version", async () => {
      writePackage(path.join(envPath, PKG), {}, 0);
      const env = await newEnvironment();
      await env.addPackage(PKG);
      expect(await answerOf(await env.getPackage(PKG))).toBe(0);
      // An unversioned package has no version to name.
      expect(
         await refusal(env.getPackage(PKG, false, { versionId: "1.0.0" })),
      ).toEqual({ status: 404, reason: "VERSION_NOT_FOUND" });

      await publish(env, source("1.0.0", 1));

      expect(env.peekPackage(PKG)).toBeUndefined();
      expect(await answerOf(await env.getPackage(PKG))).toBe(1);
      expect(fs.readdirSync(path.join(envPath, PKG))).toEqual(["1.0.0"]);
      expect(fs.readdirSync(path.join(envPath, ".legacy"))).toEqual([]);
   });

   it("serves an unversioned package with no registry lookups once it is loaded", async () => {
      writePackage(path.join(envPath, "plain"), {}, 7);
      const env = await newEnvironment();
      await env.addPackage("plain");
      const lookups = spyOn(repo, "getVersion");
      const probes = spyOn(repo, "hasVersions");
      try {
         expect(await answerOf(await env.getPackage("plain"))).toBe(7);
         expect(lookups).not.toHaveBeenCalled();
         expect(probes).not.toHaveBeenCalled();
      } finally {
         lookups.mockRestore();
         probes.mockRestore();
      }
   });

   it("deletes every version of a package, loaded or not, with its folder", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0", 1));
      await publish(env, source("2.0.0", 2));
      await env.getPackage(PKG, false, { versionId: "1.0.0" });

      await env.deletePackage(PKG);

      expect(env.getVersionService()!.cache.entries()).toEqual([]);
      expect(fs.existsSync(path.join(envPath, PKG))).toBe(false);
   });
});

describe("Environment loading with versioning off", () => {
   it("serves the single slot as before and never consults the registry", async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "off";
      writePackage(path.join(envPath, "plain"), {}, 5);
      const env = await newEnvironment();
      const lookups = spyOn(repo, "getVersion");
      const probes = spyOn(repo, "hasVersions");
      try {
         const pkg = await env.getPackage("plain", false, {
            versionId: "1.0.0",
         });
         expect(await answerOf(pkg)).toBe(5);
         expect(lookups).not.toHaveBeenCalled();
         expect(probes).not.toHaveBeenCalled();
      } finally {
         lookups.mockRestore();
         probes.mockRestore();
      }
   });
});
