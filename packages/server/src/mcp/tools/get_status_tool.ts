// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

const GET_STATUS_DESCRIPTION = `Report the server's health: its operational state and every configured package or environment that failed to load or is serving a stale model. This is the only MCP surface where a load failure is visible, so call it before concluding a package is empty or missing, and after a model edit whose reload you did not run yourself (e.g. you rely on watch mode).

## Response
A JSON object with:
- operationalState: "initializing" | "serving" | "throttled" | "draining".
- initialized: whether startup finished.
- version: this server's release version.
- environments: each environment's name with the packages serving there, each carrying a status of {serving, loading}; a package reloading while it serves reports both. A package loading here for the first time is not listed (the REST endpoint lists it on request, with includeLoading=true).
- emptyReason (only present when the server found no config at startup, or the --config path was missing): why environments is empty, and the path it checked. The server still reports serving in that state.
- initError (only present when startup failed): why. The server stays at "initializing" and never serves; the cause is usually a server root it cannot write (publisher.db), or a \${VAR} in the config file's environments that is not set. A package or environment that failed to load is under loadErrors instead.
- loadErrors (only present when something failed): entries of {environment, package?, message, stale?, failedAt?}. An entry WITHOUT stale means the package (or whole environment, when package is absent) did not load and is missing from environments; that includes a package add that failed on the server's side. An entry WITH stale: true means the package IS serving, but its most recent reload failed to compile, so the model answering queries is OLDER than the files on disk; the message says why. Fix the file and reload (reload_package) to clear it.

No loadErrors key means everything configured loaded and nothing is stale.`;

/**
 * Registers get_status: GET /api/v0/status reduced to what an agent needs to
 * judge health. Without it an agent cannot tell an empty package from one that
 * failed to load, and a failed watch-mode recompile is invisible over MCP.
 *
 * SECURITY: parity with the unauthenticated REST /status, minus detail: names,
 * states and already-redacted load errors, never connection attributes or rows.
 */
export function registerGetStatusTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool("get_status", GET_STATUS_DESCRIPTION, {}, async () => {
      const uri = buildMalloyUri({}, "getStatus");
      try {
         const status = await environmentStore.getStatus();
         const payload = {
            operationalState: status.operationalState,
            initialized: status.initialized,
            version: status.version,
            environments: status.environments.map((environment) => ({
               name: environment.name,
               // Optional in the schema but always set by listPackages; filtered
               // rather than emitted as a null.
               packages: (environment.packages ?? [])
                  .map((pkg) => pkg.name)
                  .filter((name): name is string => name !== undefined),
            })),
            ...(status.emptyReason !== undefined && {
               emptyReason: status.emptyReason,
            }),
            ...(status.initError !== undefined && {
               initError: status.initError,
            }),
            ...(status.loadErrors !== undefined && {
               loadErrors: status.loadErrors,
            }),
         };
         return jsonResource(uri, payload);
      } catch (error) {
         logger.warn("[MCP Tool getStatus] status failed", {
            error: error instanceof Error ? error.message : String(error),
         });
         const errorDetails: ErrorDetails = classifyToolError(
            "getStatus",
            "server",
            error,
         );
         return jsonToolError(uri, errorDetails);
      }
   });
}
