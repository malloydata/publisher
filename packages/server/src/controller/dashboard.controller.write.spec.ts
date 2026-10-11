// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import sinon from "sinon";
import {
   BadRequestError,
   CompileRefusedError,
   FrozenConfigError,
   InvalidModelPathError,
   PackageNotFoundError,
   WriteConflictError,
   WriteRollbackFailedError,
   WriteRolledBackError,
} from "../errors";
import { resetReportMetricsForTest } from "../report_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import type { EnvironmentStore } from "../service/environment_store";
import { readReportTag } from "../service/report";
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
const NOTEBOOK = "## artifact { layout=linear }\n##(markdown) Hello\n";

function harness(
   options: {
      frozen?: boolean;
      current?: string;
      problems?: Array<{ severity: string; message: string }>;
      reloadCompiles?: boolean;
      /** Whether putting the previous text back fails too, after a failed reload. */
      restoreFails?: boolean;
      /** Whether the package does not exist. */
      noPackage?: boolean;
      /** Whether the reloaded package serves the written file as a report. */
      served?: boolean;
      /**
       * Whether the package loaded before the write reads the file now at the
       * path as a report (`Package.isWritableReport`). Defaults to true: a
       * replace in these tests is over a report unless the test says not.
       */
      loadedReport?: boolean;
      /** The `##` notes the reloaded model carries, as dashboard facts. */
      reloadedNotes?: string[];
      /** A refusal the environment raises once the lock is held (a link leading outside). */
      writeRefusal?: Error;
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
   const pkg = {
      getModel: sinon.stub().returns(model),
      // A report, served or (a grid one with no tiles yet) not.
      isReport: () =>
         (options.served ?? true) ||
         (options.reloadedNotes !== undefined &&
            readReportTag(options.reloadedNotes).status === "report"),
   };
   const loaded = {
      isWritableReport: () => options.loadedReport ?? true,
   };
   const environment = {
      getPackage: options.noPackage
         ? sinon.stub().rejects(new PackageNotFoundError("no package"))
         : sinon.stub().resolves(pkg),
      compileSource: sinon
         .stub()
         .resolves({ problems: options.problems ?? [] }),
      writeModelFileTransactional: sinon
         .stub()
         .callsFake(
            async (
               _pkg: string,
               _path: string,
               _source: string,
               check: (current: string | undefined, loaded: unknown) => void,
               verify: (reloaded: unknown) => Promise<unknown>,
            ) => {
               if (options.writeRefusal) throw options.writeRefusal;
               check(options.current, loaded);
               try {
                  return {
                     previous: options.current,
                     verified: await verify(pkg),
                  };
               } catch {
                  if (options.restoreFails)
                     throw new WriteRollbackFailedError(
                        "The previous text could not be put back.",
                     );
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

   // Ported from "writes only a dashboard file at the top of dashboards/": a
   // report may be created anywhere (`docs/reports-design.md` §7), at the root
   // and at any depth, so those are acceptances now.
   it.each([
      "storefront.malloy",
      "q3.malloy",
      "dashboards/deep/x.malloy",
      "notebooks/nested/x.malloy",
      "sales/2026/q3.malloy",
      "sales/index.malloy",
   ])("creates a report at %s", async (good) => {
      const { controller, environment } = harness();
      const result = await controller.putDashboardSource("env", "pkg", good, {
         source: AFTER,
      });
      expect(result).toMatchObject({ path: good, created: true });
      expect(environment.writeModelFileTransactional.firstCall.args[1]).toBe(
         good,
      );
   });

   it.each([
      ["a .malloynb", "dashboards/x.malloynb"],
      ["a path that climbs out", "../dashboards/x.malloy"],
      ["a dotfile directory", ".hidden/x.malloy"],
      ["public/", "public/x.malloy"],
      ["a nested public/ path", "public/sub/x.malloy"],
      ["the root index.malloy", "index.malloy"],
      ["the root index.malloy in another case", "Index.malloy"],
      ["a file that is not Malloy", "x.json"],
   ])("refuses %s before compiling", async (_name, bad) => {
      const { controller, environment } = harness();
      await expect(
         controller.putDashboardSource("env", "pkg", bad, { source: AFTER }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(environment.compileSource.called).toBe(false);
      expect(environment.writeModelFileTransactional.called).toBe(false);
   });

   it("writes a linear report through the same compile-first flow", async () => {
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

   it("takes the layout from the tag, so either layout can be written into either folder", async () => {
      const { controller, environment } = harness();
      await controller.putDashboardSource("env", "pkg", PATH, {
         source: NOTEBOOK,
      });
      await controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
         source: '## artifact { layout=grid tiles=["a -> x"] }\n',
      });
      expect(environment.writeModelFileTransactional.calledTwice).toBe(true);
   });

   it("switches a report's layout with a replace, which is a one-property edit", async () => {
      const grid = '## artifact { layout=grid tiles=["a -> x"] }\n';
      const linear = grid.replace("layout=grid", "layout=linear");
      const { controller } = harness({ current: grid });
      const result = await controller.putDashboardSource(
         "env",
         "pkg",
         "q3.malloy",
         {
            source: linear,
            expectedHash: contentHashOf(grid),
         },
      );
      expect(result.created).toBe(false);
   });

   it("rolls back a written file the reloaded package does not serve as a report", async () => {
      const { controller } = harness({ served: false });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: NOTEBOOK,
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   it("lets a file whose artifact follows another property through the pre-write check", async () => {
      const { controller, environment } = harness();
      await controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
         source:
            '## dashboard { columns=2 } artifact { layout=linear tiles=["s -> v"] }\n',
      });
      expect(environment.compileSource.called).toBe(true);
   });

   it("refuses a file whose only artifact is a string value, before compiling", async () => {
      const { controller, environment } = harness();
      await expect(
         controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: '## dashboard { title="artifact" }\n',
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(environment.compileSource.called).toBe(false);
   });

   it.each([
      ["notebooks/", NOTEBOOK_PATH],
      ["dashboards/", PATH],
      ["the root", "q3.malloy"],
      ["a nested directory", "sales/q3.malloy"],
   ])(
      "refuses an untagged file in %s, which is a model, before compiling",
      async (_where, at) => {
         const { controller, environment } = harness();
         const error = await controller
            .putDashboardSource("env", "pkg", at, {
               source: "source: a is duckdb.sql('select 1')\n",
            })
            .catch((e) => e);
         expect(error).toBeInstanceOf(BadRequestError);
         expect(error.message).toContain("not a report");
         expect(environment.compileSource.called).toBe(false);
         expect(environment.writeModelFileTransactional.called).toBe(false);
      },
   );

   it("refuses a file tagged only at the query level, which is no report", async () => {
      const { controller, environment } = harness();
      const error = await controller
         .putDashboardSource("env", "pkg", PATH, {
            source: '# artifact { title="T" }\nquery: q is a -> x\n',
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("not a report");
      expect(environment.compileSource.called).toBe(false);
   });

   // Each of these compiles to a served report: `artifact` need not be the line's first property.
   it.each([
      [
         "a model-level line",
         '## dashboard { columns=2 } artifact { tiles=["s -> v"] }\n',
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
      "refuses a file whose only artifact is in %s with 400",
      async (_name, source) => {
         const { controller, environment } = harness();
         const error = await controller
            .putDashboardSource("env", "pkg", PATH, { source })
            .catch((e) => e);
         expect(error).toBeInstanceOf(BadRequestError);
         expect(environment.compileSource.called).toBe(false);
      },
   );

   // Replace only what the package reads as a report, wherever it lives.
   it.each([
      ["a model", "source: a is duckdb.sql('select 1')\n", "orders.malloy"],
      [
         "an untagged file in notebooks/",
         "##(markdown) shared\n",
         NOTEBOOK_PATH,
      ],
      [
         "an untagged file in dashboards/",
         "source: s is duckdb.sql('select 1')\n",
         PATH,
      ],
   ])("refuses a tagged write over %s", async (_name, existing, at) => {
      const { controller, environment } = harness({
         current: existing,
         loadedReport: false,
      });
      const error = await controller
         .putDashboardSource("env", "pkg", at, {
            source: NOTEBOOK,
            expectedHash: contentHashOf(existing),
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("is not a report");
      expect(environment.writeModelFileTransactional.calledOnce).toBe(true);
   });

   it("refuses a tagged write over a file whose only tag is inside a block comment, as discovery reads it", async () => {
      const include =
         "/*\n## artifact { layout=linear }\n*/\nsource: s is duckdb.sql('select 1')\n";
      const { controller } = harness({
         current: include,
         loadedReport: false,
      });
      const error = await controller
         .putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: NOTEBOOK,
            expectedHash: contentHashOf(include),
         })
         .catch((e) => e);
      expect(error).toBeInstanceOf(BadRequestError);
      expect(error.message).toContain("is not a report");
   });

   it("refuses a write over a report whose file lost its tag since the load", async () => {
      const untagged = "source: s is duckdb.sql('select 1')\n";
      const { controller } = harness({ current: untagged });
      await expect(
         controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: NOTEBOOK,
            expectedHash: contentHashOf(untagged),
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("replaces an existing report the loaded package serves, of either layout, anywhere", async () => {
      for (const at of [NOTEBOOK_PATH, PATH, "q3.malloy", "a/b/c.malloy"]) {
         const { controller } = harness({ current: NOTEBOOK });
         const result = await controller.putDashboardSource("env", "pkg", at, {
            source: NOTEBOOK.replace("Hello", "Hi"),
            expectedHash: contentHashOf(NOTEBOOK),
         });
         expect(result.created).toBe(false);
      }
   });

   it("refuses a write whose tag is commented out before compiling it, as the compiler reads it", async () => {
      const { controller, environment } = harness({ served: false });
      await expect(
         controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source:
               "/*\n## artifact { layout=linear }\n*/\n##(markdown) Hello\n",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(environment.compileSource.called).toBe(false);
   });

   it("accepts a tile-less grid report, which has no manifest, when the reloaded model carries the artifact tag", async () => {
      const { controller } = harness({
         served: false,
         reloadedNotes: ["## artifact { layout=grid tiles=[] }"],
      });
      const result = await controller.putDashboardSource("env", "pkg", PATH, {
         source: "## artifact { layout=grid tiles=[] }\n",
      });
      expect(result.created).toBe(true);
   });

   it("rolls back a tile-less grid report whose reloaded model carries no artifact tag", async () => {
      const { controller } = harness({ served: false, reloadedNotes: [] });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: "## artifact { layout=grid tiles=[] }\n",
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
   });

   // Ported from the name-collision 409: identity is the path, so a report may
   // share a basename with another, and nothing is refused for it.
   it("writes a report whose basename another report already has", async () => {
      const resave = await harness({
         current: BEFORE,
      }).controller.putDashboardSource("env", "pkg", "dashboards/tour.malloy", {
         source: AFTER,
         expectedHash: contentHashOf(BEFORE),
      });
      expect(resave.created).toBe(false);
      const created = await harness().controller.putDashboardSource(
         "env",
         "pkg",
         "sales/tour.malloy",
         { source: AFTER },
      );
      expect(created.created).toBe(true);
   });

   it("answers a missing path with a BadRequestError, not a TypeError", async () => {
      const { controller } = harness();
      await expect(
         controller.putDashboardSource("env", "pkg", undefined as never, {
            source: AFTER,
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("refuses, without merging, when the file changed since it was opened", async () => {
      const { controller } = harness({ current: "## artifact { } changed" });
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
      resetReportMetricsForTest();
   });

   afterEach(async () => {
      resetReportMetricsForTest();
      await metrics.shutdown();
      sinon.restore();
   });

   const outcomeCount = (outcome: string) =>
      metrics.collectCounter("publisher_report_writes_total", { outcome });

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

   it("labels each write with the layout written", async () => {
      await harness().controller.putDashboardSource(
         "env",
         "pkg",
         NOTEBOOK_PATH,
         { source: NOTEBOOK },
      );
      await harness().controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
      });
      const count = (layout: string) =>
         metrics.collectCounter("publisher_report_writes_total", {
            outcome: "created",
            layout,
         });
      expect(await count("linear")).toBe(1);
      expect(await count("grid")).toBe(1);

      await expect(
         harness().controller.putDashboardSource("env", "pkg", NOTEBOOK_PATH, {
            source: "no tag",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      expect(
         await metrics.collectCounter("publisher_report_writes_total", {
            outcome: "refused",
            layout: "grid",
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
      const { controller } = harness({ current: "## artifact { } changed" });
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

   it("reports a rollback that could not be undone apart from one that was", async () => {
      const { controller } = harness({
         current: BEFORE,
         reloadCompiles: false,
         restoreFails: true,
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteRollbackFailedError);
      expect(await outcomeCount("rollback_failed")).toBe(1);
      expect(await outcomeCount("rolled_back")).toBe(0);
      expect(
         await metrics.collectCounter("publisher_report_writes_total", {
            outcome: "rollback_failed",
            reason: "reload_failed",
         }),
      ).toBe(1);
   });

   /**
    * `refused` is the alert; `reason` is what the person answering it reads
    * next. Each refusal the write path raises is counted under its own reason,
    * so a spike of `untagged` (a client sending models) reads differently
    * from one of `not_a_report` (a client aiming at the wrong file).
    */
   it("labels each refusal with why it was refused", async () => {
      const reasonCount = (outcome: string, reason: string) =>
         metrics.collectCounter("publisher_report_writes_total", {
            outcome,
            reason,
         });

      await harness({ frozen: true })
         .controller.putDashboardSource("env", "pkg", PATH, { source: AFTER })
         .catch(() => undefined);
      await harness()
         .controller.putDashboardSource("env", "pkg", "../escape.malloy", {
            source: AFTER,
         })
         .catch(() => undefined);
      await harness()
         .controller.putDashboardSource("env", "pkg", "index.malloy", {
            source: AFTER,
         })
         .catch(() => undefined);
      await harness()
         .controller.putDashboardSource("env", "pkg", PATH, {} as never)
         .catch(() => undefined);
      await harness()
         .controller.putDashboardSource("env", "pkg", PATH, {
            source: "source: s is t",
         })
         .catch(() => undefined);
      await harness({ current: "source: s is t", loadedReport: false })
         .controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf("source: s is t"),
         })
         .catch(() => undefined);
      await harness({ noPackage: true })
         .controller.putDashboardSource("env", "pkg", PATH, { source: AFTER })
         .catch(() => undefined);
      await harness({ current: BEFORE })
         .controller.putDashboardSource("env", "pkg", PATH, { source: AFTER })
         .catch(() => undefined);
      await harness({
         problems: [{ severity: "error", message: "'x' is not defined" }],
      })
         .controller.putDashboardSource("env", "pkg", PATH, { source: AFTER })
         .catch(() => undefined);

      expect(await reasonCount("refused", "frozen_config")).toBe(1);
      expect(await reasonCount("refused", "invalid_path")).toBe(2);
      expect(await reasonCount("refused", "missing_source")).toBe(1);
      expect(await reasonCount("refused", "untagged")).toBe(1);
      expect(await reasonCount("refused", "not_a_report")).toBe(1);
      expect(await reasonCount("refused", "not_found")).toBe(1);
      expect(await reasonCount("conflict", "conflict")).toBe(1);
      expect(await reasonCount("compile_failed", "compile_failed")).toBe(1);
      expect(await outcomeCount("refused")).toBe(7);
   });

   it("labels a path the environment refuses as invalid_path, not bad_request", async () => {
      await expect(
         harness({
            writeRefusal: new InvalidModelPathError(
               "Invalid model path: it resolves outside the package.",
            ),
         }).controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
      const reasonCount = (reason: string) =>
         metrics.collectCounter("publisher_report_writes_total", {
            outcome: "refused",
            reason,
         });
      expect(await reasonCount("invalid_path")).toBe(1);
      expect(await reasonCount("bad_request")).toBe(0);
   });

   it("labels a write that landed with no reason", async () => {
      await harness().controller.putDashboardSource("env", "pkg", PATH, {
         source: AFTER,
      });
      expect(
         await metrics.collectCounter("publisher_report_writes_total", {
            outcome: "created",
            reason: "none",
         }),
      ).toBe(1);
   });
});
