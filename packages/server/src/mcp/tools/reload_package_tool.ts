// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { PackageController } from "../../controller/package.controller";
import { buildMalloyUri } from "../handler_utils";
import { registerGuardedTool } from "../register_tool";
import { jsonResource } from "../tool_response";

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

const RELOAD_DESCRIPTION = `Reload a package so edits to its model files on disk are picked up, making newly added or changed sources, views, and named queries resolvable by execute_query WITHOUT restarting the server. Publisher compiles each configured package at boot and serves that cached model, so a source or view you add afterwards is not queryable by name until the package is reloaded. Use this to close the edit -> run loop after saving a model change.

Receipt: Publisher compiles a COPY under publisher_data/ unless the environment is watch-mounted, so a reload succeeds whether or not it saw your edit. sourceContentSha hashes the model, skill and agent files (and the manifest's agents), so if you edited one of those and it is unchanged, the edit never reached the server. Data, other publisher.json keys and retrieval settings are not hashed.

${RELOAD_FAILURE_IS_SAFE} compile_model at scope "package" first is the faster way to see diagnostics. It reports the compile, render-tag and dashboard findings a reload would; every other package warning (storage, persist, materialization, explores, surface, discovery, deprecated .malloynb or #(filter), among others) appears only here.

## Parameters
- environmentName, packageName (required): the package to recompile. Use the names get_context returns.

## Behavior
Recompiles the package from its on-disk content under publisher_data/, the path every package from publisher.config.json takes. A package the server installed from a location (a publish or an update that supplied one) is re-fetched from that location instead, keeping its manifest binding, which overwrites on-disk edits.

## Response
A JSON object with status "reloaded", a mode, the package name, sourceContentSha and servedRevision, any render-tag warnings, and any exploresWarnings (curated-discovery entries that did not resolve to a model). Mode "in-place" recompiled your on-disk edits; "reinstalled" re-fetched over them; "unchanged": an immutable published version. A hard compile error returns an error payload instead.`;

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

   registerGuardedTool(mcpServer, {
      name: "reload_package",
      description: RELOAD_DESCRIPTION,
      inputSchema: reloadShape,
      op: "reloadPackage",
      failed: "reload failed",
      uri: ({ environmentName, packageName }) =>
         buildMalloyUri(
            { environment: environmentName, package: packageName },
            "reloadPackage",
         ),
      identifier: ({ environmentName, packageName }) =>
         `${environmentName}/${packageName}`,
      logFields: ({ environmentName, packageName }) => ({
         environmentName,
         packageName,
      }),
      run: async ({ environmentName, packageName }, uri) => {
         logger.debug("[MCP Tool reloadPackage] Reloading package", {
            environmentName,
            packageName,
         });
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
            // "in-place" recompiled the tree on disk; "reinstalled"
            // re-fetched it, so on-disk edits are gone. Only this says which.
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
      },
   });
}
