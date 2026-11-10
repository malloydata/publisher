// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   registerReloadPackageTool,
   VERSION_RELOAD_UNCHANGED,
} from "./reload_package_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   EnvironmentNotFoundError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../../errors";
import { PackageController } from "../../controller/package.controller";
import { testRetrieval } from "../../test_helpers/test_retrieval";

// Capture the handler registerReloadPackageTool passes to McpServer.registerTool, so it
// can be exercised against a mocked EnvironmentStore. The tool calls the
// PackageController it is handed, which asks the environment's version service
// whether the package is a published version and then calls
// environment.getPackage(pkg, reload), so the mock needs getEnvironment ->
// { getVersionService, getPackage } (and describePackage for a version).
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource: { text: string } }>;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      registerTool: (_name: string, _config: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerReloadPackageTool(
      fakeServer as never,
      new PackageController(
         store as EnvironmentStore,
         testRetrieval(store as EnvironmentStore),
      ),
   );
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

// A store whose package reload returns `metadata`. It has no version service,
// so the package is not a published version and PackageController recompiles
// it in place. The optional `calls` array records the `reload` flag of every
// getPackage call.
// The reload receipt (`servedRevision`, `sourceContentSha`) is the package's
// serving state, so the stub serves it from the same record.
function operationalOf(metadata: Record<string, unknown>) {
   return {
      servedRevision: metadata.servedRevision,
      sourceContentSha: metadata.sourceContentSha,
   };
}

function storeReturning(
   metadata: Record<string, unknown>,
   calls?: boolean[],
): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            // No registry: this package has no published versions.
            getVersionService: () => null,
            getPackage: async (_pkg: string, reload: boolean) => {
               calls?.push(reload);
               return {
                  getPackageMetadata: () => metadata,
                  getOperationalFields: () => operationalOf(metadata),
               } as never;
            },
         }) as never,
   };
}

// A store whose package is a published version: the version service resolves
// it, so PackageController answers it as it is and recompiles nothing. The
// `calls` array records the `reload` flag of every getPackage call.
function storeServingVersion(
   metadata: Record<string, unknown>,
   calls: boolean[],
): Partial<EnvironmentStore> {
   const version = {
      getPackageMetadata: () => metadata,
      getOperationalFields: () => operationalOf(metadata),
   };
   return {
      getEnvironment: async () =>
         ({
            getVersionService: () => ({
               resolve: async () => ({ versionId: "1.0.0" }),
            }),
            getPackage: async (_pkg: string, reload: boolean) => {
               calls.push(reload);
               return version as never;
            },
            describePackage: async () => metadata,
         }) as never,
   };
}

const args = { environmentName: "malloy-samples", packageName: "ecommerce" };

describe("reload_package tool", () => {
   it("returns status reloaded with the package name", async () => {
      const handler = captureHandler(storeReturning({ name: "ecommerce" }));
      const result = await handler(args);
      expect(result.isError).toBe(false);
      expect(parse(result)).toEqual({
         status: "reloaded",
         mode: "in-place",
         name: "ecommerce",
      });
   });

   it("reports sourceContentSha and servedRevision, the reload receipt", async () => {
      const handler = captureHandler(
         storeReturning({
            name: "ecommerce",
            sourceContentSha: "a".repeat(64),
            servedRevision: "rev-1",
         }),
      );
      expect(parse(await handler(args))).toEqual({
         status: "reloaded",
         mode: "in-place",
         name: "ecommerce",
         sourceContentSha: "a".repeat(64),
         servedRevision: "rev-1",
      });
   });

   it("includes description and non-empty warnings", async () => {
      const warnings = [
         {
            model: "ecommerce.malloy",
            subject: "top_categories",
            severity: "error",
         },
      ];
      const handler = captureHandler(
         storeReturning({ name: "ecommerce", description: "shop", warnings }),
      );
      const parsed = parse(await handler(args));
      expect(parsed).toEqual({
         status: "reloaded",
         mode: "in-place",
         name: "ecommerce",
         description: "shop",
         warnings,
      });
   });

   it("omits warnings when the reloaded package has none", async () => {
      const handler = captureHandler(
         storeReturning({ name: "ecommerce", warnings: [] }),
      );
      const parsed = parse(await handler(args));
      expect(parsed.warnings).toBeUndefined();
      expect(parsed.status).toBe("reloaded");
   });

   it("forwards reload=true (recompiles, not just reads)", async () => {
      const calls: boolean[] = [];
      const handler = captureHandler(
         storeReturning({ name: "ecommerce" }, calls),
      );
      await handler(args);
      // A package mounted in place is recompiled, not read from the cache.
      expect(calls).toEqual([true]);
   });

   it("surfaces a thrown error as a tool error, not a transport fault", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new Error("Environment 'nope' could not be resolved");
         },
      });
      const result = await handler({ ...args, environmentName: "nope" });
      expect(result.isError).toBe(true);
      expect(parse(result).error).toBeDefined();
   });

   it("names an unknown environment and the ones that exist", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new EnvironmentNotFoundError(
               'Environment "analytics" could not be resolved to a path.',
               {
                  environmentName: "analytics",
                  availableEnvironments: ["default"],
               },
            );
         },
      });
      const parsed = parse(
         await handler({ ...args, environmentName: "analytics" }),
      );
      expect(parsed.error).toBe(
         "Environment 'analytics' not found. Available environments: default. Use a name from list_packages.",
      );
   });

   it("reports a missing package as not-found, not as a Malloy syntax problem", async () => {
      // Pin the remediation text, not just that an error came back. Asserting
      // only that `error` is defined is what let every error class funnel
      // through the Malloy helper unnoticed: a typo'd package name told the
      // caller to go check its Malloy syntax.
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new PackageNotFoundError("Package 'nope' not found");
         },
      });
      const parsed = parse(await handler({ ...args, packageName: "nope" }));
      expect(parsed.error).toContain("Resource not found");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("reports back-pressure as retryable, not as a Malloy syntax problem", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new ServiceUnavailableError("Memory limit reached");
         },
      });
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("Memory limit reached");
      expect(JSON.stringify(parsed.suggestions)).toContain("Retry");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("surfaces a hard compile failure on reload as a tool error", async () => {
      // A Package.create compile failure propagates out of environment.getPackage
      // (the reload site), not out of getEnvironment.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               getVersionService: () => null,
               getPackage: async () => {
                  throw new Error("'nonexistent_status_field' is not defined");
               },
            }) as never,
      });
      const result = await handler(args);
      expect(result.isError).toBe(true);
      expect(parse(result).error).toBeDefined();
   });

   it("reports mode in-place for a package that is not a published version", async () => {
      const handler = captureHandler(storeReturning({ name: "ecommerce" }));
      expect(parse(await handler(args)).mode).toBe("in-place");
   });

   it("reports mode unchanged for a published version, recompiling nothing", async () => {
      // A published version is immutable, so a reload answers it as it is,
      // and still as a success: the mode and the message are the only things
      // that tell an agent its saved edit went nowhere. (The in-place cases
      // above pin the message's absence.)
      const calls: boolean[] = [];
      const handler = captureHandler(
         storeServingVersion({ name: "ecommerce", versionId: "1.0.0" }, calls),
      );
      const result = await handler(args);
      expect(result.isError).toBe(false);
      expect(parse(result)).toEqual({
         status: "reloaded",
         mode: "unchanged",
         message: VERSION_RELOAD_UNCHANGED,
         name: "ecommerce",
      });
      expect(calls).not.toContain(true);
   });

   it("never echoes the package location back to the caller", async () => {
      // The payload hand-picks fields; REST spreads the whole ApiPackage. Pin
      // that the allowlist holds, since location is the one field here that is
      // deployment detail rather than something an agent needs. A published
      // version always carries the location it was published from.
      for (const store of [
         storeReturning({
            name: "ecommerce",
            location: "https://github.com/example/pkg",
         }),
         storeServingVersion(
            {
               name: "ecommerce",
               location: "https://github.com/example/pkg",
            },
            [],
         ),
      ]) {
         const parsed = parse(await captureHandler(store)(args));
         expect(parsed.location).toBeUndefined();
      }
   });

   // DEPRECATED FEATURE COVERAGE — delete together with `explores` (see
   // RELEASE_NOTES deprecations).
   describe("deprecated: explores", () => {
      it("surfaces exploresWarnings (a mistyped explores entry is not swallowed)", async () => {
         const exploresWarnings = [
            "explore 'ordrs' does not resolve to a model in this package",
         ];
         const handler = captureHandler(
            storeReturning({ name: "ecommerce", exploresWarnings }),
         );
         const parsed = parse(await handler(args));
         expect(parsed.status).toBe("reloaded");
         expect(parsed.exploresWarnings).toEqual(exploresWarnings);
      });
   });
});
