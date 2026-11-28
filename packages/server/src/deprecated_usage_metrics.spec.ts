// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `publisher_deprecated_usage_total`, through each seam that counts it: the
 * Console alias, the `#(filter)` request fields, the
 * deprecated publisher.json keys and the deprecated formats. Each is driven
 * through the seam's own entry point, so a seam that stops counting fails
 * here rather than reading as "nobody uses it" on the day it is deleted.
 * A package load's counts are pinned on a real package in
 * `service/report_discovery_telemetry.spec.ts`.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import request from "supertest";
import { PackageController } from "./controller/package.controller";
import { resetDeprecatedUsageMetricsForTest } from "./deprecated_usage_metrics";
import { EnvironmentNotFoundError, PackageNotFoundError } from "./errors";
import {
   recordDeprecatedFilterUsage,
   setFilterDeprecationHeaders,
} from "./filter_deprecation";
import { logger } from "./logger";
import { recordDeprecatedFormatUsage } from "./service/deprecated/lints";
import type { DeprecatedPersistAnnotation } from "./service/deprecated/persist_annotations";
import {
   declaredDeprecatedManifestKeys,
   recordDeprecatedManifestKeys,
   recordSkillsDirNotRead,
} from "./service/deprecated/manifest_keys";
import {
   isLegacyReportAlias,
   recordConsoleAliasUse,
} from "./service/legacy_report_views";
import type { Model } from "./service/model";
import type { Package } from "./service/package";
import { linesMentioning } from "./test_helpers/log_lines";
import { buildRouteApp } from "./test_helpers/route_app";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "./test_helpers/metrics_harness";
import { fakeOf } from "./test_helpers/store_fakes";
import type {
   EnvironmentAdmin,
   PackageAdmin,
} from "./service/environment_ports";
import { testRetrieval } from "./test_helpers/test_retrieval";

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

   describe("the package PATCH", () => {
      /** Both PATCH paths over a real PackageController; every environment is missing. */
      const app = () => {
         const store = fakeOf<EnvironmentAdmin & PackageAdmin>({
            publisherConfigIsFrozen: false,
            getEnvironment: async (name: string) => {
               throw new EnvironmentNotFoundError(
                  `Environment ${name} does not exist`,
               );
            },
         });
         return buildRouteApp({
            environmentStore: store,
            packageController: new PackageController(
               store,
               testRetrieval(store),
            ),
         });
      };

      it("counts each request on both paths, whatever it answers, and marks it deprecated", async () => {
         const server = app();
         const answers = [
            await request(server)
               .patch("/api/v0/environments/env/packages/pkg")
               .send({ description: "d" }),
            await request(server)
               .patch("/api/v0/projects/env/packages/pkg")
               .send({ description: "d" }),
         ];

         for (const res of answers) {
            expect(res.status).toBe(404);
            expect(res.headers["deprecation"]).toBe("true");
            expect(res.headers["link"]).toContain(
               'docs/package-versions.md>; rel="deprecation"',
            );
         }
         expect(await used("package_patch")).toBe(2);
      });

      it("warns once per package with published versions, naming the route that replaces it, and never for a name that is not one", async () => {
         const probe = "package-patch-probe";
         // `versioned` has published versions (and no latest, so the PATCH is
         // refused after the warning); `plain` has none.
         const environment = {
            getVersionService: () => ({
               isVersioned: async (name: string) => name === "versioned",
               latestOf: async () => null,
            }),
            awaitPackageLoads: async () => undefined,
            peekPackage: () => undefined,
            updatePackage: async (name: string) => {
               throw new PackageNotFoundError(`Package ${name} not found`);
            },
         };
         const store = fakeOf<EnvironmentAdmin & PackageAdmin>({
            publisherConfigIsFrozen: false,
            getEnvironment: async (name: string) => {
               if (name === probe) return environment;
               throw new EnvironmentNotFoundError(
                  `Environment ${name} does not exist`,
               );
            },
         });
         const server = buildRouteApp({
            environmentStore: store,
            packageController: new PackageController(
               store,
               testRetrieval(store),
            ),
         });
         const warn = spyOn(logger, "warn");
         try {
            for (const [env, name] of [
               [probe, "versioned"],
               [probe, "versioned"],
               [probe, "plain"],
               // Names that resolve to no package must not be remembered.
               [probe, "missing-1"],
               [`${probe}-gone`, "versioned"],
            ]) {
               await request(server)
                  .patch(`/api/v0/environments/${env}/packages/${name}`)
                  .send({});
            }
            const deprecations = linesMentioning(warn, probe).filter(
               ([message]) => String(message).includes("is deprecated"),
            );
            expect(deprecations.map(([, fields]) => fields)).toEqual([
               { environmentName: probe, packageName: "versioned" },
            ]);
            expect(String(deprecations[0][0])).toContain(
               "PUT .../packages/versioned/versions/{version}/manifest",
            );
         } finally {
            warn.mockRestore();
         }
         // Every request is counted, whatever it answered.
         expect(await used("package_patch")).toBe(5);
      });
   });

   describe("GET /environments/{env}?reload=true", () => {
      it("marks a reload deprecated and counts it; a plain read is neither", async () => {
         const seen: boolean[] = [];
         const app = buildRouteApp({
            environmentStore: {
               getEnvironment: async (_name: unknown, reload?: boolean) => {
                  seen.push(reload === true);
                  return { serialize: () => ({ name: "env" }) } as never;
               },
            } as never,
         });
         const reload = await request(app).get(
            "/api/v0/environments/env?reload=true",
         );
         expect(reload.status).toBe(200);
         expect(reload.headers["deprecation"]).toBe("true");

         const plain = await request(app).get("/api/v0/environments/env");
         expect(plain.status).toBe(200);
         expect(plain.headers["deprecation"]).toBeUndefined();

         expect(seen).toEqual([true, false]);
         expect(await used("environment_reload")).toBe(1);
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
            agents: {},
         }),
      ).toEqual(["explores", "queryableSources", "agents"]);
      expect(declaredDeprecatedManifestKeys({})).toEqual([]);

      recordDeprecatedManifestKeys(["explores", "queryableSources", "agents"]);
      recordDeprecatedManifestKeys(["explores"]);
      recordDeprecatedManifestKeys(undefined);
      expect(await used("explores")).toBe(2);
      expect(await used("queryable_sources")).toBe(1);
      expect(await used("agents")).toBe(1);
   });

   it("counts a package whose root skills/ directory no declared skill set is at or under, once per call", async () => {
      recordSkillsDirNotRead();
      recordSkillsDirNotRead();
      expect(await used("skills_dir")).toBe(2);
   });

   it("counts each .malloynb file, each source declaring #(filter), and each measure declaring #@ preaggregate", async () => {
      const model = (
         filtered: string[],
         persist: DeprecatedPersistAnnotation[] = [],
      ) =>
         ({
            declaredFilterSourceNames: () => filtered,
            declaredDeprecatedPersistAnnotations: () => persist,
         }) as unknown as Model;
      // Counted with materialization v2 off (the default), where the
      // #@ preaggregate warning is not shown: the count is the evidence for
      // when to show it.
      recordDeprecatedFormatUsage(
         new Map([
            ["a.malloynb", model([])],
            ["b.malloynb", model([])],
            [
               "base.malloy",
               model(
                  ["orders", "items"],
                  [
                     { kind: "preaggregate", source: "orders", measure: "a" },
                     { kind: "preaggregate", source: "orders", measure: "b" },
                  ],
               ),
            ],
            ["other.malloy", model([])],
         ]),
      );
      expect(await used("malloynb")).toBe(2);
      expect(await used("filter_annotation")).toBe(2);
      expect(await used("preaggregate_annotation")).toBe(2);
   });
});
