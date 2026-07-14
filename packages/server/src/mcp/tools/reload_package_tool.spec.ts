import { describe, expect, it } from "bun:test";
import { registerReloadPackageTool } from "./reload_package_tool";
import type { EnvironmentStore } from "../../service/environment_store";

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

const args = { environmentName: "malloy-samples", packageName: "ecommerce" };

describe("malloy_reloadPackage tool", () => {
   it("returns status reloaded with the package name", async () => {
      const handler = captureHandler(storeReturning({ name: "ecommerce" }));
      const result = await handler(args);
      expect(result.isError).toBe(false);
      expect(parse(result)).toEqual({ status: "reloaded", name: "ecommerce" });
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
