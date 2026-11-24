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

   it("answers 500 for a manifest that cannot be fetched, and a version bound to none stays live", async () => {
      const { pkg } = await publishLive();
      const refused = await versions()
         .setManifest(PKG, "1.0.0", path.join(root, "missing.json"))
         .catch((error: unknown) => error);
      expect(refused).toBeInstanceOf(ManifestBindFailedError);
      expect((refused as ManifestBindFailedError).applied).toBe(false);
      expect(
         internalErrorToHttpError(refused as Error, { log: false }).status,
      ).toBe(500);
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
});
