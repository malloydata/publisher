// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import sinon from "sinon";
import type { components } from "../api";
import { DashboardNotFoundError, PackageNotFoundError } from "../errors";
import { VersionRef } from "../service/identity";
import { ReportController } from "./report.controller";
import { fakeOf } from "../test_helpers/store_fakes";
import type {
   ConfigFreezeReader,
   PackageLookup,
} from "../service/environment_ports";

/**
 * The two report read routes, against a stubbed package: each serves what the
 * package computed at load, by path, and a path that names no report is a 404.
 * The write route is `dashboard.controller.write.spec`'s.
 */
const REPORT: components["schemas"]["ReportManifest"] = {
   resource: "/api/v0/environments/env/packages/pkg/reports/sales/q3.malloy",
   packageName: "pkg",
   path: "sales/q3.malloy",
   name: "q3",
   layout: "grid",
   title: "Q3",
   editable: true,
};

function harness(options: { noPackage?: boolean } = {}) {
   const pkg = {
      listReports: sinon.stub().returns([REPORT]),
      getReport: sinon
         .stub()
         .callsFake(async (reportPath: string) =>
            reportPath === REPORT.path ? REPORT : undefined,
         ),
   };
   const environment = {
      getPackage: options.noPackage
         ? sinon.stub().rejects(new PackageNotFoundError("no package"))
         : sinon.stub().resolves(pkg),
   };
   const store = fakeOf<PackageLookup & ConfigFreezeReader>({
      getEnvironment: sinon.stub().resolves(environment),
   });
   return {
      controller: new ReportController(store),
      store,
      environment,
      pkg,
   };
}

describe("ReportController read routes", () => {
   it("lists the package's reports as it computed them, without reloading it", async () => {
      const { controller, store, environment } = harness();
      expect(await controller.listReports("env", "pkg")).toEqual([REPORT]);
      expect((store.getEnvironment as sinon.SinonStub).firstCall.args).toEqual([
         "env",
         false,
      ]);
      expect(environment.getPackage.firstCall.args).toEqual([
         "pkg",
         false,
         { versionId: VersionRef.LATEST },
      ]);
   });

   it("serves one report by its path", async () => {
      const { controller, pkg } = harness();
      expect(await controller.getReport("env", "pkg", REPORT.path)).toBe(
         REPORT,
      );
      expect(pkg.getReport.firstCall.args[0]).toBe(REPORT.path);
   });

   it("answers a path that names no report with a 404", async () => {
      const { controller } = harness();
      const error = await controller
         .getReport("env", "pkg", "orders.malloy")
         .catch((e) => e);
      expect(error).toBeInstanceOf(DashboardNotFoundError);
      expect(error.message).toBe(
         "Report orders.malloy does not exist in package pkg",
      );
   });

   it.each([
      ["an empty path", ""],
      ["no path", undefined],
   ])("answers %s with a 404 without asking the package", async (_name, at) => {
      const { controller, pkg } = harness();
      await expect(
         controller.getReport("env", "pkg", at as unknown as string),
      ).rejects.toBeInstanceOf(DashboardNotFoundError);
      expect(pkg.getReport.called).toBe(false);
   });

   it("lets a missing package's 404 through on both routes", async () => {
      const { controller } = harness({ noPackage: true });
      await expect(
         controller.listReports("env", "nope"),
      ).rejects.toBeInstanceOf(PackageNotFoundError);
      await expect(
         controller.getReport("env", "nope", REPORT.path),
      ).rejects.toBeInstanceOf(PackageNotFoundError);
   });
});
