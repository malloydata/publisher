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
 * The precondition is checked by a callback the service runs under the package
 * lock, so the stub for `writeModelFileChecked` runs it here too — that
 * callback IS the 409, and a stub that ignored it would pass every test while
 * the endpoint overwrote whatever it liked.
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
      writeModelFileChecked: sinon
         .stub()
         .callsFake(
            async (
               _pkg: string,
               _path: string,
               _source: string,
               check: (current: string | undefined) => void,
            ) => {
               check(options.current);
               return { previous: options.current };
            },
         ),
      restoreModelFile: sinon.stub().resolves(undefined),
   };
   const store = {
      publisherConfigIsFrozen: options.frozen ?? false,
      getEnvironment: sinon.stub().resolves(environment),
   } as unknown as EnvironmentStore;
   return { controller: new DashboardController(store), environment };
}

describe("DashboardController.putDashboardSource", () => {
   afterEach(() => sinon.restore());

   it("compiles the text as the file, writes it atomically, and reloads the package in place", async () => {
      const { controller, environment } = harness({ current: BEFORE });
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
      expect(environment.writeModelFileChecked.calledOnce).toBe(true);
      expect(
         environment.writeModelFileChecked.firstCall.args.slice(0, 3),
      ).toEqual(["pkg", PATH, AFTER]);
      // The reload is the second getPackage: the first found the package.
      expect(environment.getPackage.secondCall.args).toEqual(["pkg", true]);
      expect(
         environment.compileSource.calledBefore(
            environment.writeModelFileChecked,
         ),
      ).toBe(true);
      expect(
         environment.getPackage.secondCall.calledAfter(
            environment.writeModelFileChecked.firstCall,
         ),
      ).toBe(true);
      expect(environment.restoreModelFile.called).toBe(false);
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
      expect(environment.writeModelFileChecked.called).toBe(false);
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
      expect(environment.writeModelFileChecked.called).toBe(false);
   });

   it("restores the previous text and reloads again when the reloaded package does not compile the file", async () => {
      const { controller, environment } = harness({
         current: BEFORE,
         reloadCompiles: false,
      });
      await expect(
         controller.putDashboardSource("env", "pkg", PATH, {
            source: AFTER,
            expectedHash: contentHashOf(BEFORE),
         }),
      ).rejects.toBeInstanceOf(WriteRolledBackError);
      expect(
         environment.restoreModelFile.calledOnceWith("pkg", PATH, BEFORE),
      ).toBe(true);
      // Reload, restore, reload: the package is left serving what it did.
      expect(environment.getPackage.callCount).toBe(3);
      expect(environment.getPackage.thirdCall.args).toEqual(["pkg", true]);
   });
});
