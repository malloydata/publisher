// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import sinon from "sinon";
import {
   BadRequestError,
   FrozenConfigError,
   WriteConflictError,
   WriteRolledBackError,
} from "../errors";
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

function harness(
   options: {
      frozen?: boolean;
      current?: string;
      problems?: Array<{ severity: string; message: string }>;
      reloadCompiles?: boolean;
   } = {},
) {
   const model = {
      getModel:
         options.reloadCompiles === false
            ? sinon.stub().rejects(new Error("Cannot redefine 'x'"))
            : sinon.stub().resolves({}),
   };
   const pkg = { getModel: sinon.stub().returns(model) };
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
               _path: string,
               _source: string,
               check: (current: string | undefined) => void,
               verify: (reloaded: unknown) => Promise<unknown>,
            ) => {
               check(options.current);
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
