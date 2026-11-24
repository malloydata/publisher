// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A host binding a build manifest to a published version, through the real
 * `PUT …/versions/{version}/manifest` path (VersionService.setManifest), on
 * DuckDB.
 *
 * - A package holding an empty and a markdown-only notebook binds: neither
 *   has a model definition, and reading the primary keys a bound entry's
 *   contract names once dereferenced it, so the whole bind failed and every
 *   query answered live (0.17.3).
 * - A bind that fails answers 500 `MANIFEST_BIND_FAILED` and applies
 *   nothing: the version keeps its `manifestLocation`, its status and the
 *   tables it served before, even when the failure comes after the models
 *   were recompiled over the new manifest.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   internalErrorToHttpError,
   ManifestBindFailedError,
} from "../../src/errors";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName, VersionRef } from "../../src/service/identity";
import * as manifestLoader from "../../src/service/materialization/run/manifest_loader";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   ManifestEntry,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { withNativeSchema } from "../../src/test_helpers/host_manifest";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "pkg";

const MODEL = `##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS a")

#@ persist name="summary"
source: summary is base -> {
  group_by: a
  aggregate: n is count()
}
`;

/** Each summary table's columns, as its build records them. */
const SUMMARY_TYPES = [
   { name: "a", type: "INTEGER" },
   { name: "n", type: "BIGINT" },
];

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let env: Environment;

beforeEach(async () => {
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "notebook-manifest-bind-")),
   );
   envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   repo = createDuckDBRepository(db);
   env = await Environment.create(ENV, envPath, []);
   env.bindVersions(repo, ENV_ID, {
      downloaderFor: (_packageName, location) => async (stagingPath) => {
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
   env.setStorageBindingResolver(async () => ({}));
});

afterEach(async () => {
   await env.closeAllConnections().catch(() => undefined);
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
});

const versions = () => env.getVersionService()!;

/** Publish 1.0.0, live, and return it with `summary`'s source entity id. */
async function publishLive(): Promise<{ pkg: Package; id: string }> {
   const dir = path.join(root, "sources", "1.0.0");
   writePackageFiles(
      dir,
      {
         "m.malloy": MODEL,
         "empty.malloynb": "",
         "notes.malloynb": ">>>markdown\n# Notes\nProse only, no Malloy.\n",
      },
      { manifest: { name: PKG, version: "1.0.0" } },
   );
   await versions().publish(
      PKG,
      async (stagingPath) => {
         await fs.promises.cp(dir, stagingPath, { recursive: true });
      },
      { sourceLocation: dir, promotion: "on-publish" },
   );
   const pkg = await env.getPackage(parsePackageName(PKG), false, {
      versionId: VersionRef.parse("1.0.0"),
   });
   // Both notebooks loaded, with no model definition between them.
   expect(pkg.getModel("empty.malloynb")?.getModelDef()).toBeUndefined();
   expect(pkg.getModel("notes.malloynb")?.getModelDef()).toBeUndefined();
   const id = Object.values(pkg.getBuildPlan()?.sources ?? {})[0]
      ?.sourceEntityId;
   expect(id).toBeString();
   return { pkg, id: id! };
}

/** A table `name` in the version's own DuckDB whose `summary` total is `n`. */
async function makeTable(pkg: Package, name: string, n: number) {
   const duckdb = await pkg
      .getMalloyConfig()
      .connections.lookupConnection("duckdb");
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE ${name} AS SELECT 1 AS a, ${n}::BIGINT AS n`,
   );
}

/** A host manifest binding `summary` to `table`, written to `file`; every entry carries its contract. */
function writeManifest(file: string, id: string, table: string): string {
   const location = path.join(root, file);
   const entry = withNativeSchema(
      {
         sourceEntityId: id,
         sourceName: "summary",
         physicalTableName: table,
         connectionName: "duckdb",
      },
      SUMMARY_TYPES,
   ) as ManifestEntry;
   expect(entry.contract).toBeDefined();
   fs.writeFileSync(
      location,
      JSON.stringify({
         builtAt: new Date().toISOString(),
         strict: false,
         entries: { [id]: entry },
      }),
   );
   return location;
}

/** `summary`'s total `n` (1 live), and what served it. */
async function total(
   pkg: Package,
): Promise<{ n: number; servedBy: string | undefined }> {
   const res = await pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query: "run: summary -> { aggregate: t is n.sum() }" },
         QueryPrivileges.of({ bypassFilters: true }),
      );
   return {
      n: Number((res.compactResult as unknown as { t: number }[])[0].t),
      servedBy: res.path?.servedBy.kind,
   };
}

/**
 * Manifests a caller named that hold no usable manifest, each with the
 * answer it gets, and one the server could not read, which stays a 500.
 */
const REFUSALS: {
   what: string;
   location: () => string;
   status: number;
   reason: string;
}[] = [
   {
      what: "nothing at the location",
      location: () => path.join(root, "missing.json"),
      status: 400,
      reason: "MANIFEST_NOT_FOUND",
   },
   {
      what: "a file that does not parse",
      location: () => {
         const file = path.join(root, "torn.json");
         fs.writeFileSync(file, '{ "entries": {');
         return file;
      },
      status: 422,
      reason: "MANIFEST_INVALID",
   },
   {
      what: "JSON that is not a manifest",
      location: () => {
         const file = path.join(root, "list.json");
         fs.writeFileSync(file, "[1, 2]");
         return file;
      },
      status: 422,
      reason: "MANIFEST_INVALID",
   },
   {
      what: "a location the server cannot read",
      location: () => path.join(root, "unreadable.json"),
      status: 500,
      reason: "MANIFEST_BIND_FAILED",
   },
];

/** Run `fn` with `unreadable.json`'s fetch failing as a refused read does. */
async function withUnreadable<T>(fn: () => Promise<T>): Promise<T> {
   const original = manifestLoader.fetchManifestEntries;
   const fetches = spyOn(
      manifestLoader,
      "fetchManifestEntries",
   ).mockImplementation(async (uri: string) => {
      if (uri.endsWith("unreadable.json")) {
         throw Object.assign(new Error("permission denied"), {
            code: "EACCES",
         });
      }
      return original(uri);
   });
   try {
      return await fn();
   } finally {
      fetches.mockRestore();
   }
}

describe("a manifest bound to a version holding empty and markdown-only notebooks", () => {
   it("binds, and its contract-bearing entry serves materialized", async () => {
      const { pkg, id } = await publishLive();
      await makeTable(pkg, "summary_host", 42);
      const host = writeManifest("host.json", id, "summary_host");

      const bound = await versions().setManifest(PKG, "1.0.0", host);
      expect(bound).toBe(pkg);
      expect(pkg.materialization().bindingState()).toMatchObject({
         manifestBindingStatus: "bound",
         boundManifestUri: host,
      });
      const served = await total(pkg);
      expect(served.n).toBe(42);
      expect(served.servedBy).toBeDefined();
      expect(served.servedBy).not.toBe("live");
   });
});

describe("a manifest whose bind fails", () => {
   it("answers 500 MANIFEST_BIND_FAILED, and the version serves the manifest it had bound", async () => {
      const { pkg, id } = await publishLive();
      await makeTable(pkg, "summary_host", 42);
      await makeTable(pkg, "summary_next", 9);
      const host = writeManifest("host.json", id, "summary_host");
      await versions().setManifest(PKG, "1.0.0", host);
      expect((await total(pkg)).n).toBe(42);

      // The new manifest fetches, the models recompile over it, and then its
      // bind fails: the failure comes after what the version serves moved.
      const next = writeManifest("next.json", id, "summary_next");
      const mz = pkg.materialization();
      const bindCompiled = spyOn(mz, "bindCompiled").mockImplementationOnce(
         async () => {
            throw new Error("bind exploded");
         },
      );
      const refused = await versions()
         .setManifest(PKG, "1.0.0", next)
         .catch((error: unknown) => error);
      bindCompiled.mockRestore();

      expect(refused).toBeInstanceOf(ManifestBindFailedError);
      expect((refused as ManifestBindFailedError).applied).toBe(true);
      const answer = internalErrorToHttpError(refused as Error, { log: false });
      expect(answer.status).toBe(500);
      expect(answer.json).toMatchObject({
         code: 500,
         reason: "MANIFEST_BIND_FAILED",
      });
      expect(answer.json.message).toContain("bind exploded");

      // Nothing of it applied: the row, the metadata, the status and the
      // tables are the previous manifest's.
      expect((await versions().getVersion(PKG, "1.0.0")).manifestPath).toBe(
         host,
      );
      expect(pkg.getManifestLocation()).toBe(host);
      expect(mz.bindingState()).toMatchObject({
         manifestBindingStatus: "bound",
         boundManifestUri: host,
      });
      const served = await total(pkg);
      expect(served.n).toBe(42);
      expect(served.servedBy).not.toBe("live");
   });

   it("answers 400 for a manifest that is not there, and a version bound to none stays live", async () => {
      const { pkg } = await publishLive();
      const refused = await versions()
         .setManifest(PKG, "1.0.0", path.join(root, "missing.json"))
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(ManifestBindFailedError);
      expect((refused as ManifestBindFailedError).applied).toBe(false);
      expect(
         internalErrorToHttpError(refused as Error, { log: false }).json,
      ).toMatchObject({ code: 400, reason: "MANIFEST_NOT_FOUND" });
      expect((await versions().getVersion(PKG, "1.0.0")).manifestPath).toBe(
         null,
      );
      expect(pkg.materialization().bindingState()).toMatchObject({
         manifestBindingStatus: "unbound",
         boundManifestUri: null,
      });
      expect(await total(pkg)).toEqual({ n: 1, servedBy: "live" });
   });
});

describe("an unversioned package's manifest PATCH", () => {
   /** An unversioned package `plain` with both notebooks, in its own environment. */
   async function plainPackage(): Promise<{
      plainEnv: Environment;
      pkg: Package;
      id: string;
      publisherJson: () => string;
   }> {
      const plainPath = path.join(root, "plain-env");
      writePackageFiles(
         path.join(plainPath, "plain"),
         {
            "m.malloy": MODEL,
            "empty.malloynb": "",
            "notes.malloynb": ">>>markdown\n# Notes\nProse only, no Malloy.\n",
         },
         { manifest: { name: "plain", description: "unversioned" } },
      );
      const plainEnv = await Environment.create("plainEnv", plainPath, []);
      await plainEnv.addPackage("plain");
      const pkg = await plainEnv.getPackage(parsePackageName("plain"), false);
      const id = Object.values(pkg.getBuildPlan()?.sources ?? {})[0]
         ?.sourceEntityId;
      expect(id).toBeString();
      const file = path.join(plainPath, "plain", "publisher.json");
      return {
         plainEnv,
         pkg,
         id: id!,
         publisherJson: () => fs.readFileSync(file, "utf-8"),
      };
   }

   it("binds past the notebooks, and still answers 200 when a table it names is missing", async () => {
      const { plainEnv, pkg, id } = await plainPackage();
      try {
         await makeTable(pkg, "summary_host", 42);
         const host = writeManifest("host.json", id, "summary_host");
         await plainEnv.updatePackage("plain", {
            name: "plain",
            manifestLocation: host,
         });
         expect(pkg.materialization().bindingState()).toMatchObject({
            manifestBindingStatus: "bound",
            boundManifestUri: host,
         });
         const served = await total(pkg);
         expect(served.n).toBe(42);
         expect(served.servedBy).not.toBe("live");

         // A table the warehouse does not hold is declined on its own, at
         // bind: the manifest binds, and that source answers live.
         const gone = writeManifest("gone.json", id, "summary_gone");
         const patched = await plainEnv.updatePackage("plain", {
            name: "plain",
            manifestLocation: gone,
         });
         expect(patched.manifestLocation).toBe(gone);
         expect(pkg.materialization().bindingState()).toMatchObject({
            manifestBindingStatus: "bound",
            boundManifestUri: gone,
         });
         expect(await total(pkg)).toEqual({ n: 1, servedBy: "live" });
      } finally {
         await plainEnv.closeAllConnections().catch(() => undefined);
      }
   });

   it("answers 500 MANIFEST_BIND_FAILED for a bind that fails, and undoes the PATCH", async () => {
      const { plainEnv, pkg, id, publisherJson } = await plainPackage();
      try {
         await makeTable(pkg, "summary_host", 42);
         await makeTable(pkg, "summary_next", 9);
         const host = writeManifest("host.json", id, "summary_host");
         await plainEnv.updatePackage("plain", {
            name: "plain",
            manifestLocation: host,
         });
         expect((await total(pkg)).n).toBe(42);
         const fileBefore = publisherJson();

         // Fetched, the models recompiled over it, then its bind fails.
         const next = writeManifest("next.json", id, "summary_next");
         const mz = pkg.materialization();
         const bindCompiled = spyOn(mz, "bindCompiled").mockImplementationOnce(
            async () => {
               throw new Error("bind exploded");
            },
         );
         const refused = await plainEnv
            .updatePackage("plain", {
               name: "plain",
               description: "changed with it",
               manifestLocation: next,
            })
            .catch((error: unknown) => error);
         bindCompiled.mockRestore();

         expect(refused).toBeInstanceOf(ManifestBindFailedError);
         expect((refused as ManifestBindFailedError).applied).toBe(true);
         expect(
            internalErrorToHttpError(refused as Error, { log: false }).json,
         ).toMatchObject({ code: 500, reason: "MANIFEST_BIND_FAILED" });

         // Nothing of the PATCH applied: the file, the metadata, the status
         // and the tables served are all as they were.
         expect(publisherJson()).toBe(fileBefore);
         expect(pkg.getPackageMetadata()).toMatchObject({
            manifestLocation: host,
         });
         expect(pkg.getPackageMetadata().description).not.toBe(
            "changed with it",
         );
         expect(mz.bindingState()).toMatchObject({
            manifestBindingStatus: "bound",
            boundManifestUri: host,
         });
         const served = await total(pkg);
         expect(served.n).toBe(42);
         expect(served.servedBy).not.toBe("live");
      } finally {
         await plainEnv.closeAllConnections().catch(() => undefined);
      }
   });

   for (const refusal of REFUSALS) {
      it(`answers ${refusal.status} ${refusal.reason} for ${refusal.what}, and undoes the PATCH`, async () => {
         const { plainEnv, pkg, id, publisherJson } = await plainPackage();
         try {
            await makeTable(pkg, "summary_host", 42);
            const host = writeManifest("host.json", id, "summary_host");
            await plainEnv.updatePackage("plain", {
               name: "plain",
               manifestLocation: host,
            });
            const fileBefore = publisherJson();

            const refused = await withUnreadable(() =>
               plainEnv
                  .updatePackage("plain", {
                     name: "plain",
                     manifestLocation: refusal.location(),
                  })
                  .catch((error: unknown) => error),
            );
            expect(
               internalErrorToHttpError(refused as Error, { log: false }).json,
            ).toMatchObject({ code: refusal.status, reason: refusal.reason });

            expect(publisherJson()).toBe(fileBefore);
            expect(pkg.getPackageMetadata()).toMatchObject({
               manifestLocation: host,
            });
            expect(pkg.materialization().bindingState()).toMatchObject({
               manifestBindingStatus: "bound",
               boundManifestUri: host,
            });
            expect((await total(pkg)).n).toBe(42);
         } finally {
            await plainEnv.closeAllConnections().catch(() => undefined);
         }
      });
   }
});

describe("a manifest that cannot be fetched", () => {
   /** Every manifest fetch fails, as an unreachable bucket does; counts them. */
   function unreachable() {
      return spyOn(manifestLoader, "fetchManifestEntries").mockImplementation(
         async () => {
            throw new Error("the bucket did not answer");
         },
      );
   }

   it("is fetched once by a PUT to a version that is not loaded", async () => {
      await publishLive();
      versions().cache.evict(PKG, "1.0.0");
      const fetches = unreachable();
      try {
         const refused = await versions()
            .setManifest(PKG, "1.0.0", "gs://unreachable/manifest.json")
            .catch((error: unknown) => error);
         expect(refused).toBeInstanceOf(ManifestBindFailedError);
         // The load's bind and the refusing bind read it once between them.
         expect(fetches).toHaveBeenCalledTimes(1);
      } finally {
         fetches.mockRestore();
      }
      expect((await versions().getVersion(PKG, "1.0.0")).manifestPath).toBe(
         null,
      );
   });

   it("is fetched once by a publish again of a loaded version that names it", async () => {
      const { pkg } = await publishLive();
      const fetches = unreachable();
      try {
         const dir = path.join(root, "sources", "1.0.0");
         await versions().publish(
            PKG,
            async (stagingPath) => {
               await fs.promises.cp(dir, stagingPath, { recursive: true });
            },
            {
               sourceLocation: dir,
               manifestLocation: "gs://unreachable/manifest.json",
               promotion: "on-publish",
            },
         );
         expect(fetches).toHaveBeenCalledTimes(1);
      } finally {
         fetches.mockRestore();
      }
      // A publish reports the failure and serves live.
      expect(pkg.materialization().bindingState().manifestBindingStatus).toBe(
         "bind_failed",
      );
   });

   it("is fetched again by the next request", async () => {
      await publishLive();
      const fetches = unreachable();
      try {
         for (let i = 0; i < 2; i++) {
            await versions()
               .setManifest(PKG, "1.0.0", "gs://unreachable/manifest.json")
               .catch(() => undefined);
         }
         expect(fetches).toHaveBeenCalledTimes(2);
      } finally {
         fetches.mockRestore();
      }
   });
});

describe("a refused PUT to a version that was not loaded", () => {
   it("leaves the copy it loaded serving a request that joined the load", async () => {
      await publishLive();
      versions().cache.evict(PKG, "1.0.0");
      const retired = (
         env as unknown as { retiredConnectionGenerations: Set<unknown> }
      ).retiredConnectionGenerations;
      const retiredBefore = retired.size;
      let fail!: (error: Error) => void;
      const fetched = new Promise<never>((_, reject) => {
         fail = reject;
      });
      fetched.catch(() => undefined);
      const fetches = spyOn(
         manifestLoader,
         "fetchManifestEntries",
      ).mockImplementation(() => fetched);
      try {
         const put = versions()
            .setManifest(PKG, "1.0.0", "gs://unreachable/manifest.json")
            .catch((error: unknown) => error);
         // The PUT's load is reading the manifest: a query arrives and
         // joins that load.
         while (fetches.mock.calls.length === 0) await Bun.sleep(5);
         const joined = env.getPackage(parsePackageName(PKG), false, {
            versionId: VersionRef.parse("1.0.0"),
         });
         fail(new Error("the bucket did not answer"));
         expect(await put).toBeInstanceOf(ManifestBindFailedError);
         const pkg = await joined;

         // Still the copy served, never released, and on the row put back:
         // nothing bound, live.
         expect(retired.size).toBe(retiredBefore);
         expect(versions().cache.peek(PKG, "1.0.0")).toBe(pkg);
         expect(pkg.getManifestLocation() ?? null).toBeNull();
         expect(pkg.materialization().bindingState()).toMatchObject({
            manifestBindingStatus: "unbound",
            boundManifestUri: null,
         });
         expect(await total(pkg)).toEqual({ n: 1, servedBy: "live" });
      } finally {
         fetches.mockRestore();
      }
   });
});

describe("a manifest PUT naming a location that holds no usable manifest", () => {
   for (const refusal of REFUSALS) {
      it(`answers ${refusal.status} ${refusal.reason} for ${refusal.what}, and keeps the manifest bound before`, async () => {
         const { pkg, id } = await publishLive();
         await makeTable(pkg, "summary_host", 42);
         const host = writeManifest("host.json", id, "summary_host");
         await versions().setManifest(PKG, "1.0.0", host);

         const refused = await withUnreadable(() =>
            versions()
               .setManifest(PKG, "1.0.0", refusal.location())
               .catch((error: unknown) => error),
         );
         expect(refused).toBeInstanceOf(ManifestBindFailedError);
         const answer = internalErrorToHttpError(refused as Error, {
            log: false,
         });
         expect(answer.status).toBe(refusal.status);
         expect(answer.json).toMatchObject({
            code: refusal.status,
            reason: refusal.reason,
         });

         expect((await versions().getVersion(PKG, "1.0.0")).manifestPath).toBe(
            host,
         );
         expect(pkg.getManifestLocation()).toBe(host);
         expect(pkg.materialization().bindingState()).toMatchObject({
            manifestBindingStatus: "bound",
            boundManifestUri: host,
         });
         const served = await total(pkg);
         expect(served.n).toBe(42);
         expect(served.servedBy).not.toBe("live");
      });
   }
});
