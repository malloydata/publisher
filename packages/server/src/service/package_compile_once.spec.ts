// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package compiles once per thing a compile reads, on the main thread's
 * behalf (ms2data/service#7802):
 *
 * - a published version bound to a manifest compiles once: its models are
 *   hydrated over the manifest's colocated entries, so the bind that follows
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
import { writePackageFiles } from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import type { FreshnessManifest } from "./materialization/domain/types";
import { QueryPrivileges } from "./model_query_request";
import { governsAccess } from "./governance/ir/array_query";
import type { Package } from "./package";

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
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const loadVersion = (manifestPath: string | null) =>
      (env as unknown as VersionLoader).loadVersionPackage("pkg", versionPath, {
         versionId: "1.0.0",
         manifestPath,
         sourceLocation: null,
      });

   /** A host manifest binding `summary` to `summary_a`, and its colocated entries. */
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
      // The reload a bind ran before re-read `manifestLocation` from
      // publisher.json (none here), over the registry row's path; the bind
      // that stands in for it does the same, so `boundManifestOf` and the
      // local-store layering read what they read before.
      expect(before.getManifestLocation()).toBeNull();
      expect(once.getManifestLocation()).toBeNull();
      expect(once.getBuildManifestEntries()).toEqual(
         before.getBuildManifestEntries(),
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
      expect(pkg.getBuildManifestEntries()).toBeUndefined();
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
         pkg as unknown as {
            materialization: { boundColocatedEntries(): FreshnessManifest };
         }
      ).materialization.boundColocatedEntries();
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

   it("leaves manifestLocation as a reload left it, after a version load and a table-changing rebind", async () => {
      const probe = await loadVersion(null);
      const { file, entries } = await writeManifest(probe);
      const [id] = Object.keys(entries);
      const pkg = await loadVersion(file);
      expect(pkg.getManifestLocation()).toBeNull();
      // The PATCH-style path: the stored location is set to the manifest,
      // then a rebind to other tables reloads, which reads publisher.json.
      pkg.updateStoredMetadata({ manifestLocation: file });
      expect(
         await pkg.reloadAllModels(
            { [id]: { ...entries[id], tableName: "summary_b" } },
            { ifChanged: true },
         ),
      ).toBe(true);
      expect(pkg.getManifestLocation()).toBeNull();
      // And a freshness-only rebind, which binds in place of that reload.
      pkg.updateStoredMetadata({ manifestLocation: file });
      expect(
         await pkg.reloadAllModels(
            {
               [id]: {
                  ...entries[id],
                  tableName: "summary_b",
                  dataAsOf: "2026-11-02T00:00:00Z",
               },
            },
            { ifChanged: true },
         ),
      ).toBe(false);
      expect(pkg.getManifestLocation()).toBeNull();
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
