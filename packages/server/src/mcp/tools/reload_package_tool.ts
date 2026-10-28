// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { PackageController } from "../../controller/package.controller";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

const reloadShape = {
   environmentName: z
      .string()
      .describe("Environment name, as list_packages returns it."),
   packageName: z
      .string()
      .describe("Package to reload, as list_packages returns it."),
   ifChanged: z
      .boolean()
      .optional()
      .describe(
         'Skip the reload when the package\'s files on disk hash to what is already served; answers status "unchanged" with the sha. Ignored for a package re-fetched from a location, whose source can change without the tree changing.',
      ),
   refreshSchema: z
      .boolean()
      .optional()
      .describe(
         "Re-read warehouse table schemas instead of reusing the ones cached from earlier loads. Set it after a column was added or changed in the warehouse. Overrides ifChanged.",
      ),
};

/**
 * What a failed reload does, shared with MCP_INSTRUCTIONS so the two cannot
 * drift.
 *
 * SECURITY: interpolated into MCP_INSTRUCTIONS, delivered pre-authorization.
 * Keep it a static literal; never derive it from config or request data.
 */
export const RELOAD_FAILURE_IS_SAFE =
   "A reload that fails to compile leaves your files on disk alone and keeps serving the previously compiled model, returning the compile errors.";

const RELOAD_DESCRIPTION = `Reload a package so edits to its model files on disk are picked up, making new or changed sources, views, and named queries resolvable by execute_query WITHOUT a server restart. A source or view you add is not queryable by name until the package is reloaded; use this to close the edit -> run loop.

Receipt: Publisher compiles a COPY under publisher_data/ unless the environment is watch-mounted, so a reload succeeds whether or not it saw your edit. sourceContentSha hashes the model, skill and agent files (and the manifest's agents): unchanged after an edit means the edit never reached the server. Data and other publisher.json keys are not hashed.

${RELOAD_FAILURE_IS_SAFE} compile_model at scope "package" first is the faster way to see diagnostics. It reports the compile, render-tag and dashboard findings a reload would; every other package warning appears only here.

## Parameters
- environmentName, packageName (required): use the names get_context returns.
- ifChanged: skip the reload when the files on disk are already what is served; an added or removed file counts as a change. Not applied to a package re-fetched from a location.
- refreshSchema: also re-read warehouse table schemas, which are otherwise cached across reloads; set it after a column changed in the warehouse. Overrides ifChanged.

## Behavior
Recompiles the package from its on-disk content under publisher_data/. A package the server installed from a location is re-fetched from it instead, which overwrites on-disk edits.

## Response
A JSON object with status "reloaded", a mode of "in-place" or "reinstalled", the package name, sourceContentSha and servedRevision, timings (compileDurationMs, schemaFetchDurationMs, schemaFetchCount, schemaCacheHits, compileCacheHits, totalMs), any render-tag warnings, and any exploresWarnings. "in-place" recompiled your on-disk edits; "reinstalled" re-fetched over them. With ifChanged and no change, status is "unchanged" and there are no timings. A hard compile error returns an error payload instead.`;

/**
 * Registers reload_package: recompiles a package from disk so a saved edit is
 * queryable by name without a restart.
 *
 * SECURITY: parity with the unauthenticated REST `?reload=true` path it wraps
 * (PackageController.getPackage). It returns only names and warnings, never row
 * data or the package's location.
 */
export function registerReloadPackageTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const packageController = new PackageController(environmentStore);

   mcpServer.tool(
      "reload_package",
      RELOAD_DESCRIPTION,
      reloadShape,
      async (params) => {
         const { environmentName, packageName, ifChanged, refreshSchema } =
            params;

         logger.debug("[MCP Tool reloadPackage] Reloading package", {
            environmentName,
            packageName,
         });

         const uri = buildMalloyUri(
            { environment: environmentName, package: packageName },
            "reloadPackage",
         );

         try {
            const {
               metadata: pkg,
               mode,
               unchanged,
               timings,
            } = await packageController.reloadPackage(
               environmentName,
               packageName,
               "mcp",
               { ifChanged, refreshSchema },
            );

            const payload = {
               status: unchanged
                  ? ("unchanged" as const)
                  : ("reloaded" as const),
               // "in-place" recompiled the tree on disk; "reinstalled"
               // re-fetched it, so on-disk edits are gone. Only this says which.
               mode,
               name: pkg.name,
               // A reload succeeds whether or not it saw the caller's edit; an unchanged sha on a hashed file is the signal.
               ...(pkg.sourceContentSha !== undefined && {
                  sourceContentSha: pkg.sourceContentSha,
               }),
               ...(pkg.servedRevision !== undefined && {
                  servedRevision: pkg.servedRevision,
               }),
               ...(timings !== undefined && { timings }),
               ...(pkg.description !== undefined && {
                  description: pkg.description,
               }),
               ...(pkg.warnings !== undefined &&
                  pkg.warnings.length > 0 && { warnings: pkg.warnings }),
               // An explores entry that did not resolve to a model, so a
               // mistyped explores edit is not silently swallowed.
               ...(pkg.exploresWarnings !== undefined &&
                  pkg.exploresWarnings.length > 0 && {
                     exploresWarnings: pkg.exploresWarnings,
                  }),
            };

            return jsonResource(uri, payload);
         } catch (error) {
            // A clean isError payload rather than a transport fault.
            logger.warn("[MCP Tool reloadPackage] reload failed", {
               environmentName,
               packageName,
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = classifyToolError(
               "reloadPackage",
               `${environmentName}/${packageName}`,
               error,
            );
            return jsonToolError(uri, errorDetails);
         }
      },
   );
}
