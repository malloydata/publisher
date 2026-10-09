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
/** How many times a version's files were fetched again from their location. */
let restoreDownloads = 0;

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
         restoreDownloads += 1;
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      ensurePackageRecord: async (packageName, description) => {
         if (await repo.getPackageByName(ENV_ID, packageName)) return false;
         await repo.createPackage({
            environmentId: ENV_ID,
            name: packageName,
            description,
            manifestPath: "",
         });
         return true;
      },
      removePackageRecord: async (packageName) => {
         const row = await repo.getPackageByName(ENV_ID, packageName);
         if (row) await repo.deletePackage(row.id);
      },
   });
   return env;
}

async function publish(
   env: Environment,
   location: string,
   promotion: "on-publish" | "explicit" = "on-publish",
) {
   return env.getVersionService()!.publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(location, stagingPath, { recursive: true });
      },
      { sourceLocation: location, promotion },
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

      // No unversioned copy is left; by name, the package is now its latest.
      expect(env.peekPackage(PKG)?.getPackageMetadata().versionId).toBe(
         "1.0.0",
      );
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

      // As the controller does it: the rows first, so nothing can resolve a
      // version while its files go, then the environment's own state.
      const row = await repo.getPackageByName(ENV_ID, PKG);
      await repo.deletePackage(row!.id);
      await env.deletePackage(PKG, { versioned: true });

      expect(env.getVersionService()!.cache.entries()).toEqual([]);
      expect(fs.existsSync(path.join(envPath, PKG))).toBe(false);
      expect(await repo.listVersions(ENV_ID, PKG)).toEqual([]);
      // Nothing brings it back: no version resolves, and there is no package
      // folder to load as one.
      await expect(env.getPackage(PKG)).rejects.toThrow();
      expect(fs.existsSync(path.join(envPath, PKG))).toBe(false);
      expect(await env.listPackages()).toEqual([]);
   });

   it("keeps the folder of a package that is not versioned and failed to load", async () => {
      fs.mkdirSync(path.join(envPath, "broken"));
      fs.writeFileSync(path.join(envPath, "broken", "notes.txt"), "keep me");
      const env = await newEnvironment();
      await env.deletePackage("broken");
      expect(fs.existsSync(path.join(envPath, "broken", "notes.txt"))).toBe(
         true,
      );
   });
});

describe("Environment loading: races, listings and bookkeeping", () => {
   it("a read racing a package's first publish waits for it, then serves the version", async () => {
      const env = await newEnvironment();
      // Hold the publish's compile, with the package lock held.
      let release!: () => void;
      const gate = new Promise<void>((resolve) => (release = resolve));
      const create = Package.create.bind(Package);
      const spy = spyOn(Package, "create").mockImplementationOnce(
         async (...args: Parameters<typeof Package.create>) => {
            await gate;
            return create(...args);
         },
      );
      try {
         const publishing = publish(env, source("1.0.0", 1));
         await new Promise((resolve) => setTimeout(resolve, 20));
         // The package has no version yet, and no folder of its own to load.
         const reading = env.getPackage(PKG);
         await new Promise((resolve) => setTimeout(resolve, 20));
         release();
         await publishing;
         const read = await reading;
         expect(await answerOf(read)).toBe(1);
      } finally {
         spy.mockRestore();
      }
      // Still listed and serving: the read did not drop its status.
      expect((await env.listPackages()).map((p) => p.name)).toEqual([PKG]);
      expect(env.describePackageStatus(PKG).serving).toBe(true);
      expect(env.hasLoadedPackage(PKG)).toBe(true);
      expect(env.getFailedPackages().size).toBe(0);
   });

   it("lists a package with no latest yet without reporting a failure", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0", 1), "explicit");
      const listed = await env.listPackages();
      expect(listed).toHaveLength(1);
      expect(listed[0]).toMatchObject({
         name: PKG,
         versionId: null,
         latestVersion: null,
      });
      expect(env.getFailedPackages().size).toBe(0);
   });

   it("does not fetch an unrestorable version's location again on every read", async () => {
      const location = source("1.0.0", 1);
      await publish(await newEnvironment(), location);
      fs.rmSync(path.join(envPath, PKG, "1.0.0"), { recursive: true });
      writePackage(location, { version: "1.0.0" }, 99);
      const restarted = await newEnvironment();
      restoreDownloads = 0;
      for (let i = 0; i < 3; i++) {
         await expect(restarted.getPackage(PKG)).rejects.toThrow(
            /missing and could not be fetched again/,
         );
      }
      expect(restoreDownloads).toBe(1);
   });

   it("closing the environment releases every loaded version's connections", async () => {
      const env = await newEnvironment();
      const v1 = (await publish(env, source("1.0.0", 1))).loaded;
      const v2 = (await publish(env, source("2.0.0", 2))).loaded;
      const shutdowns = [v1, v2].map((pkg) =>
         spyOn(pkg.getMalloyConfig(), "shutdown"),
      );
      await env.closeAllConnections();
      expect(env.getVersionService()!.cache.entries()).toEqual([]);
      for (const shutdown of shutdowns) {
         expect(shutdown).toHaveBeenCalled();
      }
   });

   it("refuses to add or install a versioned package as a single slot, keeping its versions", async () => {
      const env = await newEnvironment();
      await publish(env, source("1.0.0", 1));
      expect(await refusal(env.addPackage(PKG))).toEqual({
         status: 409,
         reason: "PACKAGE_IS_VERSIONED",
      });
      const other = writePackage(path.join(root, "unversioned"), {}, 7);
      expect(
         await refusal(
            env.installPackage(PKG, async (stagingPath) => {
               await fs.promises.cp(other, stagingPath, { recursive: true });
            }),
         ),
      ).toEqual({ status: 409, reason: "PACKAGE_IS_VERSIONED" });
      expect(fs.readdirSync(path.join(envPath, PKG))).toEqual(["1.0.0"]);
      expect(await answerOf(await env.getPackage(PKG))).toBe(1);
   });

   it("publishes only one of two versions that differ only by case, even at once", async () => {
      const env = await newEnvironment();
      const upper = writePackage(
         path.join(root, "upper"),
         { version: "1.0.0-RC1" },
         1,
      );
      const lower = writePackage(
         path.join(root, "lower"),
         { version: "1.0.0-rc1" },
         2,
      );
      const outcomes = await Promise.allSettled([
         publish(env, upper),
         publish(env, lower),
      ]);
      expect(outcomes.filter((o) => o.status === "fulfilled")).toHaveLength(1);
      const refused = outcomes.find(
         (o) => o.status === "rejected",
      ) as PromiseRejectedResult;
      expect((refused.reason as PackageVersionError).reason).toBe(
         "VERSION_CONFLICT",
      );
      const [only] = await repo.listVersions(ENV_ID, PKG);
      const served = await env.getPackage(PKG);
      // The version recorded is the one whose files are served.
      expect(await answerOf(served)).toBe(
         only.versionId === "1.0.0-RC1" ? 1 : 2,
      );
   });
});
