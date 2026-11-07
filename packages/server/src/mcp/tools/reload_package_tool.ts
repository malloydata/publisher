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

const RELOAD_DESCRIPTION = `Reload a package so edits to its model files on disk are picked up, making newly added or changed sources, views, and named queries resolvable by execute_query WITHOUT restarting the server. It recompiles only a package a watched environment (--watch-env) mounts from its source directory: every other package is a published version, which is immutable, so a reload answers it unchanged and new content ships as a new version. Use this to close the edit -> run loop after saving a model change in a watched environment.

Receipt: a reload succeeds whether or not it saw your edit. sourceContentSha hashes the model, skill and agent files (and the manifest's agents), so if you edited one of those and it is unchanged, the edit never reached the server. Data, other publisher.json keys and retrieval settings are not hashed.

${RELOAD_FAILURE_IS_SAFE} compile_model at scope "package" first is the faster way to see diagnostics. It reports the compile, render-tag and dashboard findings a reload would; every other package warning (storage, persist, materialization, explores, surface, discovery, deprecated .malloynb or #(filter), among others) appears only here.

## Parameters
- environmentName, packageName (required): the package to recompile. Use the names get_context returns.

## Behavior
Recompiles a watch-mounted package from its source directory. A published version is answered as it is: nothing is recompiled.

## Response
A JSON object with status "reloaded", a mode, the package name, sourceContentSha and servedRevision, any render-tag warnings, and any exploresWarnings (curated-discovery entries that did not resolve to a model). Mode "in-place" recompiled your on-disk edits; "unchanged": an immutable published version, so nothing was recompiled. A hard compile error returns an error payload instead.`;

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
         const { environmentName, packageName } = params;

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
               operational,
               mode,
            } = await packageController.reloadPackage(
               environmentName,
               packageName,
               "mcp",
            );

            const payload = {
               status: "reloaded" as const,
               // "in-place" recompiled the tree on disk; "unchanged" is a
               // published version, which no reload changes. Only this says which.
               mode,
               name: pkg.name,
               // A reload succeeds whether or not it saw the caller's edit; an unchanged sha on a hashed file is the signal.
               ...(operational.sourceContentSha !== undefined && {
                  sourceContentSha: operational.sourceContentSha,
               }),
               ...(operational.servedRevision !== undefined && {
                  servedRevision: operational.servedRevision,
               }),
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
