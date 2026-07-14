import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { PackageController } from "../../controller/package.controller";
import { getMalloyErrorDetails, type ErrorDetails } from "../error_messages";
import { buildMalloyUri } from "../handler_utils";

// Zod shape for malloy_reloadPackage. environmentName/packageName mirror the
// other tools and point the agent at malloy_getContext for name discovery.
const reloadShape = {
   environmentName: z
      .string()
      .describe(
         "Environment name. Call malloy_getContext with no arguments to list the available environments.",
      ),
   packageName: z
      .string()
      .describe(
         "Package to reload. Call malloy_getContext with just environmentName to list its packages.",
      ),
};

const RELOAD_DESCRIPTION = `Reload a package so edits to its model files on disk are picked up, making newly added or changed sources, views, and named queries resolvable by malloy_executeQuery WITHOUT restarting the server. Publisher compiles a package on first use and serves that cached model, so a source or view you add afterwards is not queryable by name until the package is reloaded. Use this to close the edit -> run loop after saving a model change.

Validate the change with malloy_compile FIRST, because a reload is destructive when the models do not compile: it removes the package's on-disk copy under publisher_data/ and drops the cached package, so a second reload cannot recover it. Recovery is a server restart with --init, which re-copies or re-downloads the package from publisher.config.json; a package that exists only under publisher_data/ cannot be recovered at all. A watch-mode symlink mount is exempt. A clean compile is the safety gate here, not just a speed-up.

## Parameters
- environmentName, packageName (required): the package to recompile. Use the names malloy_getContext returns.

## Behavior
Recompiles the package from its current on-disk content under publisher_data/, so your saved edits are picked up. A package whose metadata carries an install location (only set by an API addPackage or a PATCH that supplies one, never by publisher.config.json) is re-fetched from that source instead, which overwrites on-disk edits. Packages loaded from publisher.config.json always recompile in place.

## Response
A JSON object with status "reloaded", the package name, any render-tag warnings, and any exploresWarnings (curated-discovery entries that did not resolve to a model). A reload that hits a hard compile error returns an error payload instead.`;

/**
 * Registers the malloy_reloadPackage MCP tool: recompiles a package from its
 * on-disk content so a saved model edit becomes queryable by name without a
 * server restart. Wraps the same PackageController.getPackage(reload=true) path
 * the REST GET /environments/:env/packages/:pkg?reload=true endpoint uses, so it
 * adds no capability beyond that already-exposed endpoint (SECURITY: parity with
 * REST. The MCP surface is unauthenticated, and so is that endpoint; a reload
 * mutates the shared in-memory package but returns only names and warnings,
 * never row data or the package's location).
 */
export function registerReloadPackageTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const packageController = new PackageController(environmentStore);

   mcpServer.tool(
      "malloy_reloadPackage",
      RELOAD_DESCRIPTION,
      reloadShape,
      async (params) => {
         const { environmentName, packageName } = params;

         logger.info("[MCP Tool reloadPackage] Reloading package", {
            environmentName,
            packageName,
         });

         const uri = buildMalloyUri(
            { environment: environmentName, package: packageName },
            "reloadPackage",
         );

         try {
            const pkg = await packageController.getPackage(
               environmentName,
               packageName,
               true,
            );

            const payload = {
               status: "reloaded" as const,
               name: pkg.name,
               ...(pkg.description !== undefined && {
                  description: pkg.description,
               }),
               ...(pkg.warnings !== undefined &&
                  pkg.warnings.length > 0 && { warnings: pkg.warnings }),
               // exploresWarnings is a distinct signal from render-tag warnings:
               // it flags a curated-discovery (explores) entry that did not
               // resolve to a model, so a mistyped explores edit is not silently
               // swallowed on reload.
               ...(pkg.exploresWarnings !== undefined &&
                  pkg.exploresWarnings.length > 0 && {
                     exploresWarnings: pkg.exploresWarnings,
                  }),
            };

            return {
               isError: false,
               content: [
                  {
                     type: "resource" as const,
                     resource: {
                        type: "application/json",
                        uri,
                        text: JSON.stringify(payload),
                     },
                  },
               ],
            };
         } catch (error) {
            // Unknown environment/package, or a compile error in the reloaded
            // package: surface as a clean isError payload rather than a
            // transport fault.
            logger.warn("[MCP Tool reloadPackage] reload failed", {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = getMalloyErrorDetails(
               "reloadPackage",
               `${environmentName}/${packageName}`,
               error,
            );
            return {
               isError: true,
               content: [
                  {
                     type: "resource" as const,
                     resource: {
                        type: "application/json",
                        uri,
                        text: JSON.stringify({
                           error: errorDetails.message,
                           suggestions: errorDetails.suggestions,
                        }),
                     },
                  },
               ],
            };
         }
      },
   );
}
