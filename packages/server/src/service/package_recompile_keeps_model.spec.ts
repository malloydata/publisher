// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A reload whose recompile fails for a file that compiled before keeps that
 * file's last good model when neither it nor anything it imports changed:
 * dropping the table one source reads must not take every source in the file
 * to a 424 (the model-wide compile error) on a worker that rebinds after the
 * drop. An author's edit that fails is never kept: its error replaces the
 * model, as it always did. The recompile's failure is the load
 * pool's outcome, set here, since this process still holds the schema it
 * read at load; the end-to-end drop is
 * tests/component/upstream_table_dropped_rebind.component.spec.ts.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { PackageLoadPool } from "../package_load/package_load_pool";
import { writePackageFiles } from "../test_helpers/temp_package";
import { unrecordedAudit } from "./access_record";
import { Environment } from "./environment";
import { parsePackageName } from "./identity";
import { MODEL_RECOMPILE_FAILED, type Package } from "./package";

const MODEL = `##! experimental.persistence
#@ persist name="gone_summary"
source: gone is duckdb.table('gone.csv') -> {
  group_by: a
  aggregate: n is count()
}

#@ persist name="kept_summary"
source: kept is duckdb.table('kept.csv') -> {
  group_by: b
  aggregate: n is count()
}
`;

const DROPPED =
   "Error(s) compiling model: FILE: m.malloy line 3: Not found: Table gone.csv";

describe("a reload whose recompile fails for a file that compiled before", () => {
   let rootDir: string;
   let env: Environment;
   let pkg: Package;
   let loads: ReturnType<typeof spyOn> | undefined;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-keep-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) =>
         writePackageFiles(stagingPath, {
            "m.malloy": MODEL,
            "gone.csv": "a\n1\n2\n",
            "kept.csv": "b\nx\ny\ny\n",
            "base.malloy": "source: base is duckdb.table('kept.csv')\n",
            "user.malloy":
               'import "base.malloy"\nsource: user is base extend { measure: c is count() }\n',
         }),
      );
      pkg = await env.getPackage(parsePackageName("pkg"), false);
   });

   afterEach(async () => {
      loads?.mockRestore();
      loads = undefined;
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   /** Every recompile from now on fails `modelPath` with `message`. */
   const failRecompileOf = (modelPath: string, message: string) => {
      const original = PackageLoadPool.prototype.loadPackage;
      loads = spyOn(
         PackageLoadPool.prototype,
         "loadPackage",
      ).mockImplementation(async function (this: PackageLoadPool, request) {
         const outcome = await original.call(this, request);
         return {
            ...outcome,
            models: outcome.models.map((m) =>
               m.modelPath === modelPath
                  ? { ...m, compilationError: { name: "Error", message } }
                  : m,
            ),
         };
      });
   };

   const recompileWarnings = () =>
      (pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === MODEL_RECOMPILE_FAILED,
      );

   const run = (query: string, modelPath = "m.malloy") =>
      pkg.getModel(modelPath)!.runQuery(unrecordedAudit(), {
         query,
         filterParams: {},
         givens: {},
         responseShape: "compact",
      });

   it("keeps serving the last good model, with a coded warning naming the file", async () => {
      const before = pkg.getModel("m.malloy")!;
      expect(before.getCompilationError()).toBeUndefined();
      await fs.rm(path.join(pkg.getPackagePath(), "gone.csv"));
      failRecompileOf("m.malloy", DROPPED);

      expect(await pkg.reloadAllModels({})).toBe(true);

      const after = pkg.getModel("m.malloy")!;
      expect(after).toBe(before);
      expect(after.getCompilationError()).toBeUndefined();
      const [warning, ...rest] = recompileWarnings();
      expect(rest).toEqual([]);
      expect(warning).toMatchObject({
         model: "m.malloy",
         reason: "compile_failed",
         severity: "warn",
      });
      expect(warning.message).toContain("its sources (gone, kept)");
      expect(warning.message).toContain("Not found: Table gone.csv");
      // Reported stale, as a failed package reload is.
      const stale = env.getStaleCompileErrors().get("pkg");
      expect(stale?.message).toContain("m.malloy did not recompile");
      expect(stale?.failedAt).toBeString();
      // But not a refused reload: its files are the kept model's.
      expect(env.getRefusedReloads().has("pkg")).toBe(false);
      // The kept model answers the source whose table is intact.
      const result = await run("run: kept -> { select: b, n; order_by: b }");
      expect(result.compactResult).toEqual([
         { b: "x", n: 1 },
         { b: "y", n: 2 },
      ]);
      // The dropped one fails alone, as its table now stands.
      await expect(run("run: gone -> { select: * }")).rejects.toThrow();
   });

   it("installs the new model, and clears the warning, once the file compiles again", async () => {
      failRecompileOf("m.malloy", DROPPED);
      await pkg.reloadAllModels({});
      expect(recompileWarnings()).toHaveLength(1);
      const kept = pkg.getModel("m.malloy");
      loads!.mockRestore();
      loads = undefined;

      await pkg.reloadAllModels({});
      expect(recompileWarnings()).toEqual([]);
      expect(env.getStaleCompileErrors().has("pkg")).toBe(false);
      expect(pkg.getModel("m.malloy")).not.toBe(kept);
      expect(pkg.getModel("m.malloy")!.getCompilationError()).toBeUndefined();
   });

   it("keeps a file that did not compile before as its compile error", async () => {
      await fs.writeFile(
         path.join(pkg.getPackagePath(), "broken.malloy"),
         "source: broken is duckdb.table('nope.csv')\n",
      );
      await pkg.reloadAllModels({});
      expect(
         pkg.getModel("broken.malloy")?.getCompilationError(),
      ).toBeDefined();
      expect(recompileWarnings()).toEqual([]);
   });
   it("never keeps a model whose file the author edited, whatever the error", async () => {
      // The same error a dropped table gives, after an edit: the edit is
      // what failed, so its error replaces the model, as before.
      failRecompileOf("m.malloy", DROPPED);
      const file = path.join(pkg.getPackagePath(), "m.malloy");
      await fs.writeFile(file, `${MODEL}\n-- an edit\n`);

      await pkg.reloadAllModels({});

      const after = pkg.getModel("m.malloy")!;
      expect(after.getCompilationError()?.message).toContain(
         "Not found: Table gone.csv",
      );
      expect(recompileWarnings()).toEqual([]);
      expect(env.getStaleCompileErrors().has("pkg")).toBe(false);
      await expect(run("run: kept -> { select: b }")).rejects.toThrow(
         /Not found: Table gone\.csv/,
      );
   });

   it("never keeps a model whose import the author edited", async () => {
      await fs.writeFile(
         path.join(pkg.getPackagePath(), "base.malloy"),
         "source: base is duckdb.table('nope.csv')\n",
      );

      await pkg.reloadAllModels({});

      expect(pkg.getModel("base.malloy")!.getCompilationError()).toBeDefined();
      expect(pkg.getModel("user.malloy")!.getCompilationError()).toBeDefined();
      expect(recompileWarnings()).toEqual([]);
      await expect(
         run("run: user -> { aggregate: c }", "user.malloy"),
      ).rejects.toThrow();
   });

   it("never answers from the old model when a broken edit tightens a gate", async () => {
      const before = await run("run: kept -> { select: b; order_by: b }");
      expect(before.compactResult).toHaveLength(2);
      // The edit locks `kept` and, in the same save, breaks the file.
      await fs.writeFile(
         path.join(pkg.getPackagePath(), "m.malloy"),
         MODEL.replace(
            '#@ persist name="kept_summary"',
            '#(authorize) false\n#@ persist name="kept_summary"',
         ) + "\nsource: typo is duckdb.table('nope.csv')\n",
      );

      await pkg.reloadAllModels({});

      expect(pkg.getModel("m.malloy")!.getCompilationError()).toBeDefined();
      expect(recompileWarnings()).toEqual([]);
      await expect(
         run("run: kept -> { select: b; order_by: b }"),
      ).rejects.toThrow();
   });
});
