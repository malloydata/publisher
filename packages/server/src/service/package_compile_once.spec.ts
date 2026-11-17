// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package compiles once per thing a compile reads, on the main thread's
 * behalf (ms2data/service#7802):
 *
 * - a published version bound to a manifest compiles once: its models are
 *   hydrated over the manifest's database entries, so the bind that follows
 *   does not reload them, and it serves what the load-then-reload served;
 * - a rebind whose entries differ only in what a compile does not read
 *   (freshness, schemas) binds them without a package compile;
 * - whether a model governs access is answered by the load worker, and
 *   shipped with the model, so the main thread does not walk it.
 *
 * Driven on real Packages in a real Environment, counting package compiles
 * at the load pool.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { PackageLoadPool } from "../package_load/package_load_pool";
import {
   duckdbMalloyConfig,
   writePackageFiles,
} from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import type { FreshnessManifest } from "./materialization/domain/types";
import { QueryPrivileges } from "./model_query_request";
import { governsAccess } from "./governance/ir/array_query";
import { Package } from "./package";

const MODEL = `##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS a")

#@ persist name="summary"
source: summary is base -> {
  group_by: a
  aggregate: n is count()
}
`;

const GOVERNED = `##! experimental.givens
#(secure)
given: ROLES :: string[]

#(authorize) 'admin' in $ROLES
source: gated is duckdb.sql("SELECT 1 AS a")
`;

type VersionLoader = {
   loadVersionPackage(
      packageName: string,
      versionPath: string,
      version: {
         versionId: string;
         manifestPath: string | null;
         sourceLocation: string | null;
      },
   ): Promise<Package>;
};

/** `summary`'s total `n`: 1 live, 42 from the stored table. */
async function total(pkg: Package): Promise<number> {
   const res = await pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query: "run: summary -> { aggregate: t is n.sum() }" },
         QueryPrivileges.of({ bypassFilters: true }),
      );
   return Number((res.compactResult as unknown as { t: number }[])[0].t);
}

async function storeTable(pkg: Package): Promise<void> {
   const duckdb = await pkg
      .getMalloyConfig()
      .connections.lookupConnection("duckdb");
   await duckdb.runSQL(
      `CREATE OR REPLACE TABLE summary_a AS SELECT 1 AS a, 42::BIGINT AS n`,
   );
}

describe("a package compiles once per thing a compile reads", () => {
   let rootDir: string;
   let env: Environment;
   let versionPath: string;
   let compiles: ReturnType<typeof spyOn>;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-once-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      versionPath = path.join(rootDir, "versions", "pkg-1.0.0");
      writePackageFiles(versionPath, { "m.malloy": MODEL });
      compiles = spyOn(PackageLoadPool.prototype, "loadPackage");
   });

   afterEach(async () => {
      compiles.mockRestore();
      // Release every loaded package: a table a rebind could not read keeps
      // a retry timer that would otherwise fire into a later spec file.
      await env.closeAllConnections().catch(() => {});
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const loadVersion = (manifestPath: string | null) =>
      (env as unknown as VersionLoader).loadVersionPackage("pkg", versionPath, {
         versionId: "1.0.0",
         manifestPath,
         sourceLocation: null,
      });

   /** A host manifest binding `summary` to `summary_a`, and its database entries. */
   async function writeManifest(
      pkg: Package,
   ): Promise<{ file: string; entries: FreshnessManifest }> {
      const id = Object.values(pkg.getBuildPlan()?.sources ?? {})[0]
         ?.sourceEntityId;
      expect(id).toBeString();
      const file = path.join(rootDir, "manifest.json");
      await fs.writeFile(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {
               [id!]: {
                  sourceEntityId: id,
                  sourceName: "summary",
                  physicalTableName: "summary_a",
                  connectionName: "duckdb",
               },
            },
         }),
      );
      return {
         file,
         entries: {
            [id!]: { tableName: "summary_a", connectionName: "duckdb" },
         },
      };
   }

   it("compiles a version bound to a manifest once, and serves what a load and reload served", async () => {
      // As before: a load, then a reload against the manifest's entries.
      const before = await loadVersion(null);
      const { file, entries } = await writeManifest(before);
      await storeTable(before);
      compiles.mockClear();
      expect(await before.reloadAllModels(entries)).toBe(true);
      expect(compiles).toHaveBeenCalledTimes(1);

      compiles.mockClear();
      const once = await loadVersion(file);
      expect(compiles).toHaveBeenCalledTimes(1);
      await storeTable(once);

      expect(once.getOperationalFields().manifestBindingStatus).toBe("bound");
      // Each keeps its registry row's manifest: none, and the manifest.
      expect(before.getManifestLocation()).toBeNull();
      expect(once.getManifestLocation()).toBe(file);
      expect(once.materialization().getBuildManifestEntries()).toEqual(
         before.materialization().getBuildManifestEntries(),
      );
      await once.getModel("m.malloy")!.prepareServedCopy();
      await before.getModel("m.malloy")!.prepareServedCopy();
      expect(once.getModel("m.malloy")!.servedCopySourceIDs()).toEqual(
         before.getModel("m.malloy")!.servedCopySourceIDs(),
      );
      expect(await total(before)).toBe(42);
      expect(await total(once)).toBe(42);
      expect(once.getPackageMetadata().warnings).toEqual(
         before.getPackageMetadata().warnings,
      );
      // Binding the same manifest again compiles nothing either.
      await (
         env as unknown as {
            bindManifest(pkg: Package, uri: string): Promise<void>;
         }
      ).bindManifest(once, file);
      expect(compiles).toHaveBeenCalledTimes(1);
   });

   it("loads live, and binds nothing, when the manifest cannot be read", async () => {
      const missing = path.join(rootDir, "missing.json");
      const pkg = await loadVersion(missing);
      expect(compiles).toHaveBeenCalledTimes(1);
      expect(pkg.getOperationalFields().manifestBindingStatus).toBe(
         "live_fallback",
      );
      expect(pkg.materialization().getBuildManifestEntries()).toBeUndefined();
      expect(await total(pkg)).toBe(1);
   });

   it("binds new freshness or schemas without a package compile, and recompiles new tables", async () => {
      const pkg = await loadVersion(null);
      const { entries } = await writeManifest(pkg);
      const [id] = Object.keys(entries);
      await storeTable(pkg);
      const at = (dataAsOf: string, extra = {}): FreshnessManifest => ({
         [id]: { ...entries[id], dataAsOf, ...extra },
      });
      compiles.mockClear();
      expect(
         await pkg.reloadAllModels(at("2026-11-01T00:00:00Z"), {
            ifChanged: true,
         }),
      ).toBe(true);
      expect(compiles).toHaveBeenCalledTimes(1);
      const model = pkg.getModel("m.malloy");

      // Only the freshness moved: bound, not compiled.
      expect(
         await pkg.reloadAllModels(at("2026-11-02T00:00:00Z"), {
            ifChanged: true,
         }),
      ).toBe(false);
      // Nor a captured schema.
      expect(
         await pkg.reloadAllModels(
            at("2026-11-02T00:00:00Z", {
               schema: [
                  { name: "a", type: "INTEGER" },
                  { name: "n", type: "BIGINT" },
               ],
            }),
            { ifChanged: true },
         ),
      ).toBe(false);
      expect(compiles).toHaveBeenCalledTimes(1);
      expect(pkg.getModel("m.malloy")).toBe(model);
      const bound = (
         pkg.materialization() as unknown as {
            boundDatabaseEntries(): FreshnessManifest;
         }
      ).boundDatabaseEntries();
      expect(bound[id]).toMatchObject({
         dataAsOf: "2026-11-02T00:00:00Z",
         schema: [
            { name: "a", type: "INTEGER" },
            { name: "n", type: "BIGINT" },
         ],
      });
      expect(await total(pkg)).toBe(42);

      // Another table is what a compile reads: recompiled.
      expect(
         await pkg.reloadAllModels(
            { [id]: { ...entries[id], tableName: "summary_b" } },
            { ifChanged: true },
         ),
      ).toBe(true);
      expect(compiles).toHaveBeenCalledTimes(2);
      expect(pkg.getManifestLocation()).toBeNull();
   });

   it("cancels an unread table's retry when the package is released, and schedules none after", async () => {
      const probe = await loadVersion(null);
      const { entries } = await writeManifest(probe);
      const [id] = Object.keys(entries);
      const pkg = await loadVersion(null);
      // `summary_b` does not exist: the bind cannot read it, and retries.
      await pkg.reloadAllModels(
         { [id]: { ...entries[id], tableName: "summary_b" } },
         { ifChanged: true },
      );
      const unread = () =>
         (pkg.getPackageMetadata().warnings ?? []).find(
            (w) => w.code === "database-table-unread",
         );
      expect(pkg.materialization().hasUnreadDatabaseTables()).toBe(true);
      expect(unread()?.message).toContain("A retry is pending.");
      await pkg.releaseConnections();
      expect(unread()?.message).not.toContain("A retry is pending.");
   });

   it("keeps a version's stored manifestLocation through a load and every rebind", async () => {
      const probe = await loadVersion(null);
      const { file, entries } = await writeManifest(probe);
      const [id] = Object.keys(entries);
      const pkg = await loadVersion(file);
      expect(pkg.getManifestLocation()).toBe(file);
      // Another table: a reload, which never reads a version's binding from
      // its publisher.json.
      const moved = { [id]: { ...entries[id], tableName: "summary_b" } };
      expect(await pkg.reloadAllModels(moved, { ifChanged: true })).toBe(true);
      expect(pkg.getManifestLocation()).toBe(file);
      // Only the freshness: bound in place of that reload.
      const fresher = {
         [id]: { ...moved[id], dataAsOf: "2026-11-02T00:00:00Z" },
      };
      expect(await pkg.reloadAllModels(fresher, { ifChanged: true })).toBe(
         false,
      );
      expect(pkg.getManifestLocation()).toBe(file);
      // The identical entries: nothing to do.
      expect(await pkg.reloadAllModels(fresher, { ifChanged: true })).toBe(
         false,
      );
      expect(pkg.getManifestLocation()).toBe(file);
   });

   it("re-reads an unversioned package's manifestLocation from publisher.json on a reload", async () => {
      const unversionedPath = path.join(rootDir, "unversioned");
      const declared = path.join(rootDir, "declared.json");
      writePackageFiles(
         unversionedPath,
         { "m.malloy": MODEL },
         { manifest: { name: "pkg", manifestLocation: declared } },
      );
      const pkg = await Package.create(
         "testEnv",
         "pkg",
         unversionedPath,
         () => duckdbMalloyConfig().malloyConfig,
      );
      expect(pkg.getVersionId()).toBeUndefined();
      expect(pkg.getManifestLocation()).toBe(declared);
      const { entries } = await writeManifest(pkg);
      const [id] = Object.keys(entries);

      // A stored value a reload replaces with publisher.json's, as before.
      pkg.updateStoredMetadata({ manifestLocation: "elsewhere.json" });
      expect(await pkg.reloadAllModels(entries)).toBe(true);
      expect(pkg.getManifestLocation()).toBe(declared);

      // An edit to publisher.json is picked up by the next reload.
      const edited = path.join(rootDir, "edited.json");
      writePackageFiles(
         unversionedPath,
         { "m.malloy": MODEL },
         { manifest: { name: "pkg", manifestLocation: edited } },
      );
      expect(
         await pkg.reloadAllModels({
            [id]: { ...entries[id], tableName: "summary_b" },
         }),
      ).toBe(true);
      expect(pkg.getManifestLocation()).toBe(edited);
      await pkg.releaseConnections();
   });

   it("ships whether each model governs access with the model", async () => {
      writePackageFiles(versionPath, {
         "m.malloy": MODEL,
         "gated.malloy": GOVERNED,
      });
      const pkg = await loadVersion(null);
      const outcome = (await compiles.mock.results[0].value) as {
         models: { modelPath: string; governsAccess?: boolean }[];
      };
      const shipped = Object.fromEntries(
         outcome.models.map((m) => [m.modelPath, m.governsAccess]),
      );
      expect(shipped).toEqual({ "m.malloy": false, "gated.malloy": true });
      // Noted on the hydrated model: the lookup answers what the worker did.
      expect(governsAccess(pkg.getModel("gated.malloy")!.getModelDef())).toBe(
         true,
      );
      expect(governsAccess(pkg.getModel("m.malloy")!.getModelDef())).toBe(
         false,
      );
   });
});
