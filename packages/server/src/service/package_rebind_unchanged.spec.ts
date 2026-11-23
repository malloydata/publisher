// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Binding a package to the manifest it is already compiled against costs no
 * compile (ms2data/service#7795: a dev worker re-bound the same manifest over
 * and over, and each bind recompiled every model in the load pool and every
 * served copy on the main thread). And a reload that must recompile compiles
 * each served copy once, not once and again after an invalidation.
 *
 * Driven on a real Package in a real Environment, counting package compiles
 * at the load pool and served-copy compiles at `GeneratedFiles.loadModel`.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { PackageLoadPool } from "../package_load/package_load_pool";
import { writePackageFiles } from "../test_helpers/temp_package";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import type { FreshnessManifest } from "./materialization/domain/types";
import { GeneratedFiles } from "./materialization/text/generated_files";
import type { Package } from "./package";
import { contractFor } from "../test_helpers/contract_fixture";

const MODEL = `##! experimental.persistence
source: base is duckdb.sql("SELECT 1 AS a")

#@ persist name="summary"
source: summary is base -> {
  group_by: a
  aggregate: n is count()
}
`;

describe("a rebind of the same manifest", () => {
   let rootDir: string;
   let env: Environment;
   let pkg: Package;
   let reloads: ReturnType<typeof spyOn>;
   let copies: ReturnType<typeof spyOn>;
   let offThread: ReturnType<typeof spyOn>;
   /** The served copy's compiles: inline, and those a load worker took. */
   const copyCount = () =>
      copies.mock.calls.length +
      offThread.mock.results.filter(
         (r: { value: unknown }) => r.value !== undefined,
      ).length;
   /** The database entry for `summary`, on `table`. */
   let manifest: (table: string) => FreshnessManifest;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-rebind-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) =>
         writePackageFiles(stagingPath, { "m.malloy": MODEL }),
      );
      pkg = await env.getPackage(parsePackageName("pkg"), false);
      const id = Object.values(pkg.getBuildPlan()?.sources ?? {})[0]
         ?.sourceEntityId;
      expect(id).toBeString();
      const duckdb = await pkg
         .getMalloyConfig()
         .connections.lookupConnection("duckdb");
      for (const table of ["summary_a", "summary_b"]) {
         await duckdb.runSQL(
            `CREATE OR REPLACE TABLE ${table} AS SELECT 1 AS a, 1::BIGINT AS n`,
         );
      }
      manifest = (table) => ({
         [id!]: {
            tableName: table,
            contract: contractFor(),
            connectionName: "duckdb",
            schema: [
               { name: "a", type: "INTEGER" },
               { name: "n", type: "BIGINT" },
            ],
         },
      });
      reloads = spyOn(PackageLoadPool.prototype, "loadPackage");
      copies = spyOn(GeneratedFiles.prototype, "loadModel");
      offThread = spyOn(PackageLoadPool.prototype, "compileServedCopy");
   });

   afterEach(async () => {
      reloads.mockRestore();
      copies.mockRestore();
      offThread.mockRestore();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** Let a served-copy compile a reload started finish. */
   const settled = async () => {
      await pkg.getModel("m.malloy")!.prepareServedCopy();
   };

   it("recompiles nothing, and still serves the table", async () => {
      expect(
         await pkg.reloadAllModels(manifest("summary_a"), { ifChanged: true }),
      ).toBe(true);
      await settled();
      const model = pkg.getModel("m.malloy")!;
      expect(model.servedCopySourceIDs()).toEqual(
         new Set([expect.stringContaining("summary@")]),
      );
      const [packageCompiles, copyCompiles] = [
         reloads.mock.calls.length,
         copyCount(),
      ];
      expect(packageCompiles).toBe(1);
      // One served-copy compile for the reload: not one started, invalidated
      // by the destinations and the bind that followed, and started again.
      expect(copyCompiles).toBe(1);

      for (let i = 0; i < 5; i++) {
         expect(
            await pkg.reloadAllModels(manifest("summary_a"), {
               ifChanged: true,
            }),
         ).toBe(false);
         await settled();
      }
      expect(reloads.mock.calls.length).toBe(packageCompiles);
      expect(copyCount()).toBe(copyCompiles);
      expect(pkg.getModel("m.malloy")).toBe(model);
      expect(pkg.materialization().getBuildManifestEntries()).toEqual({
         [Object.keys(manifest("x"))[0]]: expect.objectContaining({
            tableName: expect.stringContaining("summary_a"),
         }),
      });

      // A real change over a bound package: one package compile, and one
      // served-copy compile (not one over the old tables, discarded, then
      // one over the new).
      expect(
         await pkg.reloadAllModels(manifest("summary_b"), { ifChanged: true }),
      ).toBe(true);
      await settled();
      expect(reloads.mock.calls.length).toBe(packageCompiles + 1);
      expect(copyCount()).toBe(copyCompiles + 1);
      expect(pkg.getModel("m.malloy")!.servedCopySourceIDs()).toEqual(
         new Set([expect.stringContaining("summary@")]),
      );
   });

   it("binds a host's manifest URI again with no recompile, unless a PATCH asks", async () => {
      const [id] = Object.keys(manifest("x"));
      const file = path.join(rootDir, "manifest.json");
      await fs.writeFile(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {
               [id]: {
                  sourceEntityId: id,
                  sourceName: "summary",
                  physicalTableName: "summary_a",
                  connectionName: "duckdb",
               },
            },
         }),
      );
      const bind = (recompile?: "always") =>
         (
            env as unknown as {
               bindManifest(
                  pkg: Package,
                  uri: string,
                  recompile?: "always",
               ): Promise<void>;
            }
         ).bindManifest(pkg, file, recompile);
      const status = () =>
         (pkg.getOperationalFields() as { manifestBindingStatus?: string })
            .manifestBindingStatus;

      await bind();
      expect(reloads).toHaveBeenCalledTimes(1);
      expect(status()).toBe("bound");
      await bind();
      await bind();
      expect(reloads).toHaveBeenCalledTimes(1);
      expect(status()).toBe("bound");
      await bind("always");
      expect(reloads).toHaveBeenCalledTimes(2);
   });

   it("recompiles a changed manifest, a reload since, and a bind since", async () => {
      await pkg.reloadAllModels(manifest("summary_a"), { ifChanged: true });
      let seen = reloads.mock.calls.length;
      const recompiled = async (entries: FreshnessManifest) => {
         const reloaded = await pkg.reloadAllModels(entries, {
            ifChanged: true,
         });
         const counted = reloads.mock.calls.length > seen;
         seen = reloads.mock.calls.length;
         expect(reloaded).toBe(counted);
         return reloaded;
      };

      // Another table.
      expect(await recompiled(manifest("summary_b"))).toBe(true);
      // Back to live, and bound again.
      expect(await recompiled({})).toBe(true);
      expect(await recompiled(manifest("summary_b"))).toBe(true);
      // A reload that did not ask (a reload from disk) moves the models.
      await pkg.reloadAllModels(manifest("summary_b"));
      seen = reloads.mock.calls.length;
      expect(await recompiled(manifest("summary_b"))).toBe(false);
      await pkg.reloadAllModels({});
      seen = reloads.mock.calls.length;
      expect(await recompiled(manifest("summary_b"))).toBe(true);
      // A database bind with no reload moves what is bound.
      pkg.materialization().bind({ database: manifest("summary_a") });
      expect(await recompiled(manifest("summary_b"))).toBe(true);
   });

   it("recompiles while a connection edit has the plan stale, and after it", async () => {
      const again = () =>
         pkg.reloadAllModels(manifest("summary_a"), { ifChanged: true });
      expect(await again()).toBe(true);
      expect(await again()).toBe(false);
      // A connection edit marks the plan stale and queues its re-derivation:
      // a rebind meanwhile recompiles, as it did before.
      pkg.markBuildPlanStale(["duckdb"]);
      expect(pkg.materialization().isBuildPlanStale()).toBe(true);
      expect(await again()).toBe(true);
      // Still stale: the reload just made read the stale plan too.
      expect(await again()).toBe(true);
      // Re-derived: the plan that reload read is replaced, so recompile.
      expect(await pkg.refreshBuildPlan()).toBe(true);
      expect(pkg.materialization().isBuildPlanStale()).toBe(false);
      expect(await again()).toBe(true);
      expect(await again()).toBe(false);
   });

   it("recompiles after a connection edit even when the plan re-derives to the same object", async () => {
      const again = () =>
         pkg.reloadAllModels(manifest("summary_a"), { ifChanged: true });
      expect(await again()).toBe(true);
      // The edit is recorded; with the plan reported current and the same
      // object, only the edit itself tells the rebind to recompile.
      pkg.markBuildPlanStale(["duckdb"]);
      const materialization = pkg.materialization() as unknown as {
         isBuildPlanStale(): boolean;
      };
      const stale = spyOn(materialization, "isBuildPlanStale").mockReturnValue(
         false,
      );
      try {
         expect(await again()).toBe(true);
         expect(await again()).toBe(false);
      } finally {
         stale.mockRestore();
      }
   });

   it("recompiles when the models moved without a reload, or one is a placeholder", async () => {
      const again = () =>
         pkg.reloadAllModels(manifest("summary_a"), { ifChanged: true });
      expect(await again()).toBe(true);
      // The models map replaced by anything but this reload.
      const holder = pkg as unknown as { models: Map<string, unknown> };
      holder.models = new Map(holder.models);
      expect(await again()).toBe(true);
      expect(await again()).toBe(false);
      // A model the last reload left as a placeholder may compile now.
      const model = pkg.getModel("m.malloy")!;
      const failed = spyOn(model, "getCompilationError").mockReturnValue(
         new Error("transient"),
      );
      try {
         expect(await again()).toBe(true);
      } finally {
         failed.mockRestore();
      }
      expect(await again()).toBe(false);
   });
});
