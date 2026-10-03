// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import sinon from "sinon";
import {
   BadRequestError,
   CompileRefusedError,
   FrozenConfigError,
   WriteConflictError,
   WriteRolledBackError,
} from "../errors";
import { resetDashboardWriteMetricsForTest } from "../dashboard_write_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import type { EnvironmentStore } from "../service/environment_store";
import { contentHashOf, DashboardController } from "./dashboard.controller";

/**
 * The write path, against a stubbed environment: what is refused before
 * anything touches disk, the order of compile → checked write → reload, and
 * the restore when the reload does not take the new file.
 *
 * Both callbacks are run by the service under the package lock, so the stub
 * for `writeModelFileTransactional` runs them here too: the `check` callback
 * IS the 409 and the `verify` callback IS the rollback trigger, and a stub
 * that ignored either would pass every test while the endpoint overwrote
 * whatever it liked. That the restore itself happens under the same lock is
 * the service's contract, covered in `environment_write_model_file.spec`.
 */
const PATH = "dashboards/overview.malloy";
const BEFORE = '## artifact { title="Before" tiles=["a -> x"] }';
const AFTER = '## artifact { title="After" tiles=["a -> x"] }';
const NOTEBOOK_PATH = "notebooks/tour.malloy";
const NOTEBOOK = "## artifact { kind=notebook }\n##(markdown) Hello\n";

function harness(
   options: {
      frozen?: boolean;
      current?: string;
      problems?: Array<{ severity: string; message: string }>;
      reloadCompiles?: boolean;
      /** Whether the reloaded package serves the file as the document it was written as. */
      served?: boolean;
      /** The compiled model the package held for the path before the write, and the file holding the slug. */
      loaded?: { note: boolean; holder?: string };
      /** The `##` notes the reloaded model carries, as dashboard facts. */
      reloadedNotes?: string[];
   } = {},
) {
   const model = {
      getModel:
         options.reloadCompiles === false
            ? sinon.stub().rejects(new Error("Cannot redefine 'x'"))
            : sinon.stub().resolves({}),
      getDashboardModelFacts: () =>
         options.reloadedNotes && {
            modelAnnotations: options.reloadedNotes,
            queries: [],
         },
   };
   let written = "";
   const served = options.served ?? true;
   const pkg = {
      getModel: sinon.stub().returns(model),
      getDashboard: () => (served ? { path: written } : undefined),
      isServedNotebook: () => served,
   };
   const loaded = options.loaded && {
      getModel: () => ({
         getModelDef: () => ({}),
         carriesNotebookArtifactNote: () => options.loaded?.note,
      }),
      getDashboard: () =>
         options.loaded?.holder ? { path: options.loaded.holder } : undefined,
   };
   const environment = {
      getPackage: sinon.stub().resolves(pkg),
      compileSource: sinon
         .stub()
         .resolves({ problems: options.problems ?? [] }),
      writeModelFileTransactional: sinon
         .stub()
         .callsFake(
            async (
               _pkg: string,
               path: string,
               _source: string,
               check: (current: string | undefined, loaded: unknown) => void,
               verify: (reloaded: unknown) => Promise<unknown>,
            ) => {
               written = path;
               check(options.current, loaded);
               try {
                  return {
                     previous: options.current,
                     verified: await verify(pkg),
                  };
               } catch {
                  throw new WriteRolledBackError(
                     "The package did not reload with the new file, so the " +
                        "previous text was put back and nothing changed.",
                  );
               }
            },
         ),
   };
   const store = {
      publisherConfigIsFrozen: options.frozen ?? false,
      getEnvironment: sinon.stub().resolves(environment),
   } as unknown as EnvironmentStore;
   return {
      controller: new DashboardController(store),
      environment,
      pkg,
      model,
   };
}

describe("DashboardController.putDashboardSource", () => {
   afterEach(() => sinon.restore());

   it("compiles the text as the file, writes it atomically, and reloads the package in place", async () => {
      const { controller, environment, pkg, model } = harness({
         current: BEFORE,
      });
      const result = await controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
         expectedHash: contentHashOf(BEFORE),
      });
      expect(result).toEqual({
         resource: `/api/v0/environments/env/packages/pkg/models/${PATH}`,
         path: PATH,
         contentHash: contentHashOf(AFTER),
         created: false,
      });
      const compile = environment.compileSource.firstCall.args;
      expect(compile.slice(0, 3)).toEqual(["pkg", PATH, AFTER]);
      expect(compile[5]).toBe("file");
      expect(environment.writeModelFileTransactional.calledOnce).toBe(true);
      expect(
         environment.writeModelFileTransactional.firstCall.args.slice(0, 3),
      ).toEqual(["pkg", PATH, AFTER]);
      // The reload now happens inside the transaction, so `getPackage` is
      // called once — to find the package before compiling.
      expect(environment.getPackage.callCount).toBe(1);
      expect(
         environment.compileSource.calledBefore(
            environment.writeModelFileTransactional,
         ),
      ).toBe(true);
      // `verify` asked the reloaded package for the file it just wrote and
      // compiled it; that is what a rollback is triggered by.
      expect(pkg.getModel.calledWith(PATH)).toBe(true);
      expect(model.getModel.called).toBe(true);
   });

   it("creates a file that did not exist, and says so", async () => {
      const { controller } = harness();
      const result = await controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
      });
      expect(result.created).toBe(true);
   });

   it("refuses under frozenConfig before reading anything", async () => {
      const { controller, environment } = harness({ frozen: true });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, { source: AFTER }),
      ).rejects.toBeInstanceOf(FrozenConfigError);
      expect(environment.getPackage.called).toBe(false);
   });

   it("writes only a dashboard file at the top of dashboards/", async () => {
      const { controller, environment } = harness();
      for (const bad of [
         "storefront.malloy",
         "dashboards/deep/x.malloy",
         "dashboards/x.malloynb",
         "../dashboards/x.malloy",
      ]) {
         await expect(
            controller.putDashboardSource("env", "pkg", bad, { source: AFTER }),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("writes a tagged notebook at the top of notebooks/ through the same compile-first flow", async () => {
      const { controller, environment } = harness();
      const result = await controller.putDashboardSource(
         "env",
         "pkg",
         NOTEBOOK_PATH,
         { source: NOTEBOOK },
      );
      expect(result.path).toBe(NOTEBOOK_PATH);
      expect(environment.compileSource.firstCall.args[1]).toBe(NOTEBOOK_PATH);
      expect(environment.compileSource.firstCall.args[5]).toBe("file");
      expect(environment.writeModelFileTransactional.calledOnce).toBe(true);
   });

   it("takes the kind from the tag, so a notebook can be written into dashboards/ and a dashboard into notebooks/", async () => {
      const { controller, environment } = harness();
      await controller.putDashboardSource("env", "pkg", PATH, {
         source: NOTEBOOK,
      });
      await controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
         source: "## artifact { kind=dashboard }\n",
      });
      expect(environment.writeModelFileTransactional.calledTwice).toBe(true);
   });

   it("serves a written file as the kind its tag names: a notebook in dashboards/ is checked as a notebook", async () => {
      const { controller, pkg } = harness();
      pkg.getDashboard = () => undefined;
      pkg.isServedNotebook = () => true;
      await controller.putDashboardSource("env", "pkg", PATH, {
         source: NOTEBOOK,
      });
      pkg.isServedNotebook = () => false;
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: NOTEBOOK,
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("still refuses an untagged notebooks/ file whatever the kind would have been", async () => {
      const { controller, environment } = harness();
      await expect(
         controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: "source: a is duckdb.sql('select 1')\n",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("refuses an untagged notebooks/ file, which is a shared include, before compiling", async () => {
      const { controller, environment } = harness();
      const error = await controller
         .putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: "##(markdown) Hello\n",
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("dashboards/<slug>.malloy");
      expect(error.message).toContain("notebooks/<slug>.malloy");
      expect(environment.compileSource.called).toBe(false);
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("refuses an untagged dashboards/ file with 400 before compiling", async () => {
      const { controller, environment } = harness();
      const error = await controller
         .putDashboardSource("env", "pkg", PATH, {
            source: "source: a is duckdb.sql('select 1')\n",
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("not a dashboard");
      expect(environment.compileSource.called).toBe(false);
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("lets a dashboards/ file tagged at the query level through the pre-write check", async () => {
      const { controller, environment } = harness();
      await controller.putDashboardSource("env", "pkg", PATH, {
         source: '# artifact { title="T" }\nrun: a -> x\n',
      });
      expect(environment.compileSource.called).toBe(true);
   });

   // Each of these compiles to a served dashboard: `artifact` need not be the line's first property.
   it.each([
      [
         "a model-level line",
         '## dashboard { columns=2 } artifact { tiles=["s -> v"] }\n',
      ],
      [
         "a query-level line",
         '# dashboard { columns=2 } artifact { title="T" }\nquery: q is s -> v\n',
      ],
      [
         "a block",
         '##| dashboard { columns=2 }\nartifact { tiles=["s -> v"] }\n|##\n',
      ],
   ])(
      "lets artifact after another property on %s through the pre-write check",
      async (_name, source) => {
         const { controller, environment } = harness();
         await controller.putDashboardSource("env", "pkg", PATH, { source });
         expect(environment.compileSource.called).toBe(true);
      },
   );

   it.each([
      ["a line comment", "// ## artifact { tiles=[] }\n"],
      ["a string", '## dashboard { title="artifact { }" }\n'],
      ["a doc route", '##" The artifact { } page\n'],
      ["a markdown route", "##(markdown) artifact { }\n"],
      [
         'a ##|" block',
         '##|"\nThe "page" is an artifact { } of\n|##\nsource: a is duckdb.sql("select 1")\n',
      ],
      ["a property path", "## dashboard.artifact { }\n"],
   ])(
      "refuses a dashboards/ file whose only artifact is in %s with 400",
      async (_name, source) => {
         const { controller, environment } = harness();
         const error = await controller
            .putDashboardSource("env", "pkg", PATH, { source })
            .catch((e) => e);
         expect(error).toBeInstanceOf(BadRequestError);
         expect(environment.compileSource.called).toBe(false);
      },
   );

   it("refuses a tagged write over an existing untagged file, a shared include", async () => {
      const include = "##(markdown) shared\n";
      const { controller, environment } = harness({ current: include });
      const error = await controller
         .putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: NOTEBOOK,
            expectedHash: contentHashOf(include),
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("shared include");
      expect(environment.writeModelFileTransactional.calledOnce).toBe(true);
   });

   it("refuses a tagged write over an include whose only tag is inside a block comment, as discovery reads it", async () => {
      const include =
         "/*\n## artifact { kind=notebook }\n*/\nsource: s is duckdb.sql('select 1')\n";
      const { controller } = harness({
         current: include,
         loaded: { note: false },
      });
      const error = await controller
         .putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: NOTEBOOK,
            expectedHash: contentHashOf(include),
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("shared include");
   });

   it("replaces an existing notebook the loaded package serves", async () => {
      const { controller } = harness({
         current: NOTEBOOK,
         loaded: { note: true },
      });
      const result = await controller.putDashboardSource(
         "env",
         "pkg",
         NOTEBOOK_PATH,
         {
            source: NOTEBOOK.replace("Hello", "Hi"),
            expectedHash: contentHashOf(NOTEBOOK),
         },
      );
      expect(result.created).toBe(false);
   });

   it("rolls back a notebook write whose tag is commented out, so the package does not serve it", async () => {
      const { controller } = harness({ served: false });
      await expect(
         controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source:
               "/*\n## artifact { kind=notebook }\n*/\n##(markdown) Hello\n",
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("rolls back a dashboard write the reloaded package does not serve", async () => {
      const { controller } = harness({ served: false });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, { source: AFTER }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("accepts a tile-less dashboard, which has no manifest, when the reloaded model carries the artifact tag", async () => {
      const { controller, pkg } = harness({
         reloadedNotes: ["## artifact { kind=dashboard tiles=[] }"],
      });
      pkg.getDashboard = () => undefined;
      const result = await controller.putDashboardSource("env", "pkg", PATH, {
         source: "## artifact { kind=dashboard tiles=[] }\n",
      });
      expect(result.created).toBe(true);
   });

   it("rolls back a tile-less dashboard whose reloaded model carries no artifact tag", async () => {
      const { controller, pkg } = harness({ reloadedNotes: [] });
      pkg.getDashboard = () => undefined;
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: "/*\n## artifact { kind=dashboard tiles=[] }\n*/\n",
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("rolls back a tagged tile-less dashboard whose slug another file holds", async () => {
      const { controller, pkg } = harness({
         reloadedNotes: ["## artifact { kind=dashboard tiles=[] }"],
      });
      pkg.getDashboard = () => ({ path: "dashboards/other.malloy" });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: "## artifact { kind=dashboard tiles=[] }\n",
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("refuses a dashboard whose slug another file already holds, before writing", async () => {
      const { controller, environment } = harness({
         current: NOTEBOOK,
         loaded: { note: true, holder: "dashboards/tour.malloy" },
      });
      const error = await controller
         .putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: "## artifact { kind=dashboard }\n",
            expectedHash: contentHashOf(NOTEBOOK),
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(WriteConflictError);
      expect(error.message).toContain("dashboards/tour.malloy");
      expect(error.message).toContain("already holds the dashboard name");
      expect(environment.writeModelFileTransactional.calledOnce).toBe(true);
   });

   it("lets the file that holds the slug be re-saved, and a notebook share a slug with a dashboard", async () => {
      const held = { note: true, holder: "dashboards/tour.malloy" };
      const resave = await harness({
         current: BEFORE,
         loaded: held,
      }).controller.putDashboardSource("env", "pkg", "dashboards/tour.malloy", {
         source: AFTER,
         expectedHash: contentHashOf(BEFORE),
      });
      expect(resave.created).toBe(false);
      // A notebook is keyed by path, so it never contends for the dashboard name.
      const notebook = await harness({
         current: NOTEBOOK,
         loaded: held,
      }).controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
         source: NOTEBOOK.replace("Hello", "Hi"),
         expectedHash: contentHashOf(NOTEBOOK),
      });
      expect(notebook.created).toBe(false);
   });

   it("answers a missing path with a BadRequestError, not a TypeError", async () => {
      const { controller } = harness();
      await expect(
         controller.putDashboardSource("env", "pkg", undefined as never, {
            source: AFTER,
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("refuses a nested notebook path and a .malloynb", async () => {
      const { controller, environment } = harness();
      for (const bad of ["notebooks/nested/x.malloy", "notebooks/x.malloynb"]) {
         await expect(
            controller.putDashboardSource("env", "pkg", bad, {
               source: NOTEBOOK,
            }),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("refuses, without merging, when the file changed since it was opened", async () => {
      const { controller } = harness({ current: "changed" });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteConflictError);
   });

   it("refuses to overwrite an existing file when no expectedHash is sent", async () => {
      const { controller } = harness({ current: BEFORE });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, { source: AFTER }),
      ).rejects.toBeInstanceOf(WriteConflictError);
   });

   it("refuses text that does not compile, and writes nothing", async () => {
      const { controller, environment } = harness({
         current: BEFORE,
         problems: [
            { severity: "warn", message: "unused import" },
            { severity: "error", message: "'x' is not defined" },
         ],
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toThrow(/does not compile.*'x' is not defined/);
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("restores the previous text and reloads again when the reloaded package does not compile the file", async () => {
      const { controller, model } = harness({
         current: BEFORE,
         reloadCompiles: false,
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
      // The controller's part is refusing the write when the reloaded package
      // will not compile the file; putting the text back is the service's,
      // under the lock it still holds.
      expect(model.getModel.called).toBe(true);
   });
});

/**
 * Every exit from the endpoint records exactly one outcome, and records the
 * one that matches what happened. The classifier reads the error's TYPE, so
 * these are what stops a reworded message — or a branch added later — from
 * quietly recounting a compile failure as a malformed request.
 */
describe("putDashboardSource: what it reports", () => {
   let metrics: MetricsHarness;

   beforeEach(async () => {
      metrics = await startMetricsHarness();
      resetDashboardWriteMetricsForTest();
   });

   afterEach(async () => {
      resetDashboardWriteMetricsForTest();
      await metrics.shutdown();
      sinon.restore();
   });

   const outcomeCount = (outcome: string) =>
      metrics.collectCounter("publisher_dashboard_writes_total", { outcome });

   it("reports a create and a replace apart", async () => {
      await harness().controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
      });
      expect(await outcomeCount("created")).toBe(1);

      await harness({ current: BEFORE }).controller.putDashboardSource(
         "env",
         "pkg",
         PATH,
         { source: AFTER, expectedHash: contentHashOf(BEFORE) },
      );
      expect(await outcomeCount("replaced")).toBe(1);
   });

   it("labels a notebook write with its own kind", async () => {
      await harness().controller.putDashboardSource(
         "env",
         "pkg",
         NOTEBOOK_PATH,
         { source: NOTEBOOK },
      );
      await harness().controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
      });
      const count = (kind: string) =>
         metrics.collectCounter("publisher_dashboard_writes_total", {
            outcome: "created",
            kind,
         });
      expect(await count("notebook")).toBe(1);
      expect(await count("dashboard")).toBe(1);

      await expect(
         harness().controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: "no tag",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(
         await metrics.collectCounter("publisher_dashboard_writes_total", {
            outcome: "refused",
            kind: "notebook",
         }),
      ).toBe(1);
   });

   it("reports a missing path as refused, not a crash", async () => {
      await expect(
         harness().controller.putDashboardSource(
            "env",
            "pkg",
            undefined as never,
            { source: AFTER },
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(await outcomeCount("refused")).toBe(1);
   });

   it("reports a stale hash as a conflict", async () => {
      const { controller } = harness({ current: "changed" });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteConflictError);
      expect(await outcomeCount("conflict")).toBe(1);
   });

   it("reports source that does not compile apart from a malformed request", async () => {
      const { controller } = harness({
         current: BEFORE,
         problems: [{ severity: "error", message: "'x' is not defined" }],
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(CompileRefusedError);
      expect(await outcomeCount("compile_failed")).toBe(1);
      expect(await outcomeCount("refused")).toBe(0);

      // A body with no source is the other kind: the caller got the API
      // wrong, not Malloy. Counting the two together would make the compile
      // gate impossible to watch.
      await expect(
         harness().controller.putDashboardSource(
            "env",
            "pkg",
            PATH,
            {} as never,
         ),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(await outcomeCount("refused")).toBe(1);
   });

   it("reports a rollback under its own outcome", async () => {
      const { controller } = harness({
         current: BEFORE,
         reloadCompiles: false,
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
      expect(await outcomeCount("rolled_back")).toBe(1);
      expect(await outcomeCount("refused")).toBe(0);
   });

   it("reports a frozen config as refused, without touching the package", async () => {
      const { controller, environment } = harness({ frozen: true });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, { source: AFTER }),
      ).rejects.toBeInstanceOf(FrozenConfigError);
      expect(await outcomeCount("refused")).toBe(1);
      expect(environment.getPackage.called).toBe(false);
   });
});
