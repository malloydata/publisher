import { describe, expect, it } from "bun:test";
import { registerReloadPackageTool } from "./reload_package_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import { PackageNotFoundError, ServiceUnavailableError } from "../../errors";

// Capture the handler registerReloadPackageTool passes to McpServer.tool, so it
// can be exercised against a mocked EnvironmentStore. The tool builds a
// PackageController internally, which calls environment.getPackage(pkg, reload),
// so the mock only needs getEnvironment -> { getPackage }.
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource: { text: string } }>;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerReloadPackageTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

// A store whose package reload returns `metadata`. `location` is left undefined
// so PackageController takes the in-place-reload branch (no re-download). The
// optional `calls` array records the `reload` flag of every getPackage call.
function storeReturning(
   metadata: Record<string, unknown>,
   calls?: boolean[],
): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            getPackage: async (_pkg: string, reload: boolean) => {
               calls?.push(reload);
               return { getPackageMetadata: () => metadata } as never;
            },
         }) as never,
   };
}

// A store whose cached package metadata carries `location`, so PackageController
// takes the re-fetch branch: installPackage instead of an in-place recompile.
// This is the only remaining path that overwrites on-disk edits, so it is worth
// pinning that the routing and the reported mode both match.
function storeWithInstallLocation(installed: Record<string, unknown>): {
   store: Partial<EnvironmentStore>;
   installCalls: string[];
} {
   const installCalls: string[] = [];
   return {
      installCalls,
      store: {
         getEnvironment: async () =>
            ({
               getPackage: async () =>
                  ({
                     getPackageMetadata: () => ({
                        name: "ecommerce",
                        location: "https://github.com/example/pkg",
                     }),
                  }) as never,
               installPackage: async (pkg: string) => {
                  installCalls.push(pkg);
                  return { getPackageMetadata: () => installed } as never;
               },
            }) as never,
      },
   };
}

const args = { environmentName: "malloy-samples", packageName: "ecommerce" };

describe("malloy_reloadPackage tool", () => {
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

   it("includes description and non-empty warnings", async () => {
      const warnings = [
         {
            model: "ecommerce.malloy",
            target: "top_categories",
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
      // The controller probes cached metadata (reload=false) then reloads
      // (reload=true); the tool must trigger the recompile.
      expect(calls).toContain(true);
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
      // (the reload site), not out of getEnvironment. The controller swallows the
      // first probe call and re-throws on the reload call.
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               getPackage: async () => {
                  throw new Error("'nonexistent_status_field' is not defined");
               },
            }) as never,
      });
      const result = await handler(args);
      expect(result.isError).toBe(true);
      expect(parse(result).error).toBeDefined();
   });

   it("reports mode in-place for a package with no install location", async () => {
      const handler = captureHandler(storeReturning({ name: "ecommerce" }));
      expect(parse(await handler(args)).mode).toBe("in-place");
   });

   it("re-fetches and reports mode reinstalled when the package carries an install location", async () => {
      // The one path that still overwrites on-disk edits. It must route through
      // installPackage, and the payload must say so, since an agent cannot
      // otherwise tell that its saved edit was just re-fetched over.
      const { store, installCalls } = storeWithInstallLocation({
         name: "ecommerce",
         location: "https://github.com/example/pkg",
      });
      const handler = captureHandler(store);
      const result = await handler(args);
      expect(result.isError).toBe(false);
      expect(installCalls).toEqual(["ecommerce"]);
      expect(parse(result).mode).toBe("reinstalled");
   });

   it("never echoes the package location back to the caller", async () => {
      // The payload hand-picks fields; REST spreads the whole ApiPackage. Pin
      // that the allowlist holds, since location is the one field here that is
      // deployment detail rather than something an agent needs.
      const { store } = storeWithInstallLocation({
         name: "ecommerce",
         location: "https://github.com/example/pkg",
      });
      const parsed = parse(await captureHandler(store)(args));
      expect(parsed.location).toBeUndefined();
   });

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
