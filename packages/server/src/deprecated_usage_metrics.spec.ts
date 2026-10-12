// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `publisher_deprecated_usage_total`, through each seam that counts it: the
 * legacy routes, the Console alias, the `#(filter)` request fields, the
 * deprecated publisher.json keys and the deprecated formats. Each is driven
 * through the seam's own entry point, so a seam that stops counting fails
 * here rather than reading as "nobody uses it" on the day it is deleted.
 * A package load's counts are pinned on a real package in
 * `service/report_discovery_telemetry.spec.ts`.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import express from "express";
import request from "supertest";
import { resetDeprecatedUsageMetricsForTest } from "./deprecated_usage_metrics";
import { EnvironmentNotFoundError, ServiceUnavailableError } from "./errors";
import {
   recordDeprecatedFilterUsage,
   setFilterDeprecationHeaders,
} from "./filter_deprecation";
import { logger } from "./logger";
import { registerDeprecatedRoutes } from "./routes/deprecated";
import { recordDeprecatedFormatUsage } from "./service/deprecated/lints";
import {
   declaredDeprecatedManifestKeys,
   recordDeprecatedManifestKeys,
} from "./service/deprecated/manifest_keys";
import type { EnvironmentStore } from "./service/environment_store";
import {
   isLegacyReportAlias,
   recordConsoleAliasUse,
} from "./service/legacy_report_views";
import type { Model } from "./service/model";
import type { Package } from "./service/package";
import { linesMentioning } from "./test_helpers/log_lines";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";

describe("publisher_deprecated_usage_total", () => {
   let harness: MetricsHarness;
   const used = (feature: string) =>
      harness.collectCounter("publisher_deprecated_usage_total", { feature });

   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetDeprecatedUsageMetricsForTest();
   });

   afterEach(async () => {
      resetDeprecatedUsageMetricsForTest();
      await harness.shutdown();
   });

   describe("the legacy routes", () => {
      /** The routes over a store with no environments: by default every call is a 404. */
      const app = (
         fail: (name: string) => Error = (name) =>
            new EnvironmentNotFoundError(`Environment ${name} does not exist`),
      ) => {
         const store = {
            getEnvironment: async (name: string) => {
               throw fail(name);
            },
         } as unknown as EnvironmentStore;
         const server = express();
         registerDeprecatedRoutes(server, "/api/v0", store);
         return server;
      };
      const pkg = "/api/v0/environments/env/packages/pkg";

      it("counts each request by route family, whatever it answers", async () => {
         const server = app();
         await request(server).get(`${pkg}/dashboards`);
         await request(server).get(`${pkg}/dashboards/sales`);
         await request(server).get(`${pkg}/dashboards/sales`);
         await request(server).get(`${pkg}/notebooks`);
         await request(server).get(`${pkg}/notebooks/tour.malloynb`);
         await request(server).get(`${pkg}/notebooks/tour.malloynb/cells/0`);

         expect(await used("dashboards_list")).toBe(1);
         expect(await used("dashboard_get")).toBe(2);
         expect(await used("notebooks_list")).toBe(1);
         expect(await used("notebook_get")).toBe(1);
         expect(await used("notebook_cell")).toBe(1);
      });

      it("logs a 404 once, at warn, never at error", async () => {
         // `bun test` runs many spec files in one process, and their async
         // work logs through this same logger while the spies are active
         // (another file's "Environment env does not exist", say). Only lines
         // naming this request's own environment are this request's.
         const probe = "deprecated-404-probe";
         const path = `/api/v0/environments/${probe}/packages/pkg/dashboards/sales`;
         const warn = spyOn(logger, "warn");
         const error = spyOn(logger, "error");
         try {
            const res = await request(app()).get(path);
            expect(res.status).toBe(404);
            expect(linesMentioning(warn, probe)).toEqual([
               [
                  "Request refused",
                  {
                     path,
                     packageName: "pkg",
                     status: 404,
                     error: `Environment ${probe} does not exist`,
                  },
               ],
            ]);
            expect(linesMentioning(error, probe)).toEqual([]);
         } finally {
            warn.mockRestore();
            error.mockRestore();
         }
      });

      it("logs a 5xx once, at error, whether or not the mapping logs that class", async () => {
         const error = spyOn(logger, "error");
         try {
            // 503: a class the mapping answers without logging.
            const busy = await request(
               app(() => new ServiceUnavailableError("busy-503-marker")),
            ).get(`${pkg}/notebooks/tour.malloynb/cells/0`);
            expect(busy.status).toBe(503);
            // 500: a class the mapping logs itself, with its stack.
            const fault = await request(
               app(() => new Error("fault-500-marker")),
            ).get(`${pkg}/dashboards/sales`);
            expect(fault.status).toBe(500);

            expect(
               linesMentioning(error, "busy-503-marker").map(([text]) => text),
            ).toEqual(["Request failed"]);
            expect(linesMentioning(error, "fault-500-marker")).toHaveLength(1);
         } finally {
            error.mockRestore();
         }
      });
   });

   describe("the Console alias", () => {
      const paths = [
         "deep/sales.malloy",
         "dashboards/q3.malloy",
         "a/my q.malloy",
      ];
      const pkg = {
         servedReportPaths: () => paths.values(),
         isServedReport: (path: string) => paths.includes(path),
      } as unknown as Package;
      const packageOf = (environmentName: string, packageName: string) =>
         environmentName === "env" && packageName === "pkg" ? pkg : undefined;

      it("counts each app route that is the alias of a report, once", async () => {
         for (const route of [
            "/env/pkg/dashboards/sales",
            "/env/pkg/notebooks/sales",
            "/env/pkg/dashboards/my%20q",
         ])
            recordConsoleAliasUse(route, packageOf);
         expect(await used("console_alias")).toBe(3);
      });

      it("does not count a route that is no alias in use", async () => {
         for (const route of [
            // No report has the name.
            "/env/pkg/dashboards/missing",
            // A report's own route, not an alias of it.
            "/env/pkg/dashboards/q3",
            // Not the alias shape.
            "/env/pkg/sales",
            "/env/pkg/dashboards/deep/sales",
            "/env/pkg",
            // A package that is not loaded.
            "/env/other/dashboards/sales",
         ])
            recordConsoleAliasUse(route, packageOf);
         expect(await used("console_alias")).toBe(0);
      });

      it("leaves the counting to the route: asking whether a path is an alias counts nothing", async () => {
         expect(isLegacyReportAlias(pkg, "dashboards/sales")).toBe(true);
         expect(isLegacyReportAlias(pkg, "dashboards/q3")).toBe(false);
         expect(await used("console_alias")).toBe(0);
      });
   });

   describe("the #(filter) request fields", () => {
      const res = () => ({ setHeader: () => undefined });

      it("counts filter values and a bypass, on REST and MCP alike", async () => {
         setFilterDeprecationHeaders(res(), { filterParams: { region: "EU" } });
         setFilterDeprecationHeaders(res(), { bypassFilters: false });
         recordDeprecatedFilterUsage({ filterParams: { region: ["EU"] } });

         expect(await used("filter_params")).toBe(2);
         expect(await used("bypass_filters")).toBe(1);
      });

      it("does not count an empty or absent value, which is no use", async () => {
         setFilterDeprecationHeaders(res(), { filterParams: {} });
         setFilterDeprecationHeaders(res(), {});
         recordDeprecatedFilterUsage({ filterParams: undefined });

         expect(await used("filter_params")).toBe(0);
         expect(await used("bypass_filters")).toBe(0);
      });
   });

   it("counts each deprecated publisher.json key a package declares, at any value", async () => {
      expect(
         declaredDeprecatedManifestKeys({
            explores: [],
            queryableSources: "all",
         }),
      ).toEqual(["explores", "queryableSources"]);
      expect(declaredDeprecatedManifestKeys({})).toEqual([]);

      recordDeprecatedManifestKeys(["explores", "queryableSources"]);
      recordDeprecatedManifestKeys(["explores"]);
      recordDeprecatedManifestKeys(undefined);
      expect(await used("explores")).toBe(2);
      expect(await used("queryable_sources")).toBe(1);
   });

   it("counts each .malloynb file and each source declaring #(filter)", async () => {
      const model = (filtered: string[]) =>
         ({ declaredFilterSourceNames: () => filtered }) as unknown as Model;
      recordDeprecatedFormatUsage(
         new Map([
            ["a.malloynb", model([])],
            ["b.malloynb", model([])],
            ["base.malloy", model(["orders", "items"])],
            ["other.malloy", model([])],
         ]),
      );
      expect(await used("malloynb")).toBe(2);
      expect(await used("filter_annotation")).toBe(2);
   });
});
