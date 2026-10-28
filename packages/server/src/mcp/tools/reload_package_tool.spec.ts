// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { registerReloadPackageTool } from "./reload_package_tool";
import { Package } from "../../service/package";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   EnvironmentNotFoundError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../../errors";

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
   timings?: Record<string, number>,
): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            getPackage: async (_pkg: string, reload: boolean) => {
               calls?.push(reload);
               return {
                  getPackageMetadata: () => metadata,
                  getLoadTimings: () => timings,
               } as never;
            },
         }) as never,
   };
}

// A store whose cached package metadata carries `location`, so PackageController
// takes the re-fetch branch: installPackage instead of an in-place recompile.
// This is the only remaining path that overwrites on-disk edits, so it is worth
// pinning that the routing and the reported mode both match.
function storeWithInstallLocation(
   installed: Record<string, unknown>,
   timings?: Record<string, number>,
): {
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
                  return {
                     getPackageMetadata: () => installed,
                     getLoadTimings: () => timings,
                  } as never;
               },
            }) as never,
      },
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

   describe("ifChanged and timings", () => {
      const timings = {
         compileDurationMs: 12,
         schemaFetchDurationMs: 3,
         schemaFetchCount: 2,
         schemaCacheHits: 5,
         compileCacheHits: 7,
         totalMs: 40,
      };

      /** A resident package over a real tree, served at the tree's current sha. */
      async function withTree(
         run: (
            tree: string,
            handler: Handler,
            calls: boolean[],
         ) => Promise<void>,
      ) {
         const tree = fs.mkdtempSync(path.join(os.tmpdir(), "reload-tool-"));
         try {
            fs.writeFileSync(path.join(tree, "publisher.json"), "{}");
            fs.writeFileSync(path.join(tree, "a.malloy"), "source: a is x\n");
            const served = await Package.currentContentSha(tree);
            const calls: boolean[] = [];
            const resident = {
               getPackagePath: () => tree,
               getSourceContentSha: () => served,
               getLoadTimings: () => timings,
               getPackageMetadata: () => ({
                  name: "ecommerce",
                  sourceContentSha: served,
                  servedRevision: "rev-1",
               }),
            };
            const handler = captureHandler({
               getEnvironment: async () =>
                  ({
                     getPackage: async (_pkg: string, reload: boolean) => {
                        calls.push(reload);
                        return resident as never;
                     },
                  }) as never,
            });
            await run(tree, handler, calls);
         } finally {
            fs.rmSync(tree, { recursive: true, force: true });
         }
      }

      it("answers unchanged, with the sha and no timings, and loads nothing", async () => {
         await withTree(async (_tree, handler, calls) => {
            const parsed = parse(await handler({ ...args, ifChanged: true }));
            expect(parsed.status).toBe("unchanged");
            expect(parsed.sourceContentSha).toMatch(/^[0-9a-f]{64}$/);
            expect(parsed.timings).toBeUndefined();
            expect(calls).not.toContain(true);
         });
      });

      it.each([
         [
            "a model file was added",
            (t: string) =>
               fs.writeFileSync(path.join(t, "b.malloy"), "source: b is x\n"),
         ],
         [
            "a model file was removed",
            (t: string) => fs.rmSync(path.join(t, "a.malloy")),
         ],
         [
            "a model file changed",
            (t: string) =>
               fs.writeFileSync(path.join(t, "a.malloy"), "source: a is y\n"),
         ],
         [
            "a skill file was added",
            (t: string) => {
               fs.mkdirSync(path.join(t, "skills", "s"), { recursive: true });
               fs.writeFileSync(
                  path.join(t, "skills", "s", "SKILL.md"),
                  "---\nname: s\ndescription: d\n---\nbody\n",
               );
            },
         ],
      ])("reloads, with timings, when %s", async (_name, change) => {
         await withTree(async (tree, handler, calls) => {
            change(tree);
            const parsed = parse(await handler({ ...args, ifChanged: true }));
            expect(parsed.status).toBe("reloaded");
            expect(parsed.timings).toEqual(timings);
            expect(calls).toContain(true);
         });
      });

      it("reloads without ifChanged even when nothing changed", async () => {
         await withTree(async (_tree, handler, calls) => {
            const parsed = parse(await handler(args));
            expect(parsed.status).toBe("reloaded");
            expect(parsed.timings).toEqual(timings);
            expect(calls).toContain(true);
         });
      });

      it("reloads an unchanged tree when refreshSchema is set", async () => {
         await withTree(async (_tree, handler, calls) => {
            const parsed = parse(
               await handler({ ...args, ifChanged: true, refreshSchema: true }),
            );
            expect(parsed.status).toBe("reloaded");
            expect(calls).toContain(true);
         });
      });

      it("reloads when publisher.json cannot be parsed, so the load reports why", async () => {
         await withTree(async (tree, handler, calls) => {
            fs.writeFileSync(path.join(tree, "publisher.json"), "{");
            const parsed = parse(await handler({ ...args, ifChanged: true }));
            expect(parsed.status).toBe("reloaded");
            expect(calls).toContain(true);
         });
      });

      it("always reloads a package installed from a location", async () => {
         const { store, installCalls } = storeWithInstallLocation(
            { name: "ecommerce" },
            timings,
         );
         const parsed = parse(
            await captureHandler(store)({ ...args, ifChanged: true }),
         );
         expect(parsed.status).toBe("reloaded");
         expect(parsed.mode).toBe("reinstalled");
         expect(parsed.timings).toEqual(timings);
         expect(installCalls).toEqual(["ecommerce"]);
      });
   });

   it("documents the timings fields in api-doc.yaml", () => {
      const doc = fs.readFileSync(
         path.join(__dirname, "../../../../../api-doc.yaml"),
         "utf8",
      );
      const block = doc.slice(doc.indexOf("    PackageLoadTimings:"));
      for (const field of [
         "compileDurationMs",
         "schemaFetchDurationMs",
         "schemaFetchCount",
         "schemaCacheHits",
         "compileCacheHits",
         "totalMs",
      ]) {
         expect(block.slice(0, block.indexOf("    Package:"))).toContain(
            `        ${field}:`,
         );
      }
   });
});
