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
   /** The colocated entry for `summary`, on `table`. */
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
            connectionName: "duckdb",
            schema: [
               { name: "a", type: "INTEGER" },
               { name: "n", type: "BIGINT" },
            ],
         },
      });
      reloads = spyOn(PackageLoadPool.prototype, "loadPackage");
      copies = spyOn(GeneratedFiles.prototype, "loadModel");
   });

   afterEach(async () => {
      reloads.mockRestore();
      copies.mockRestore();
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
         copies.mock.calls.length,
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
      expect(copies.mock.calls.length).toBe(copyCompiles);
      expect(pkg.getModel("m.malloy")).toBe(model);
      expect(pkg.getBuildManifestEntries()).toEqual({
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
      expect(copies.mock.calls.length).toBe(copyCompiles + 1);
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
      // A colocated bind with no reload moves what is bound.
      pkg.bindColocatedServeManifest(manifest("summary_a"));
      expect(await recompiled(manifest("summary_b"))).toBe(true);
   });
});
