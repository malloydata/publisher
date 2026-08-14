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
- environments: each environment's name with its loaded package names.
- loadErrors (only present when something failed): entries of {environment, package?, message, stale?, failedAt?}. An entry WITHOUT stale means the package (or whole environment, when package is absent) did not load and is missing from environments. An entry WITH stale: true means the package IS serving, but its most recent reload failed to compile, so the model answering queries is OLDER than the files on disk; the message says why. Fix the file and reload (malloy_reloadPackage) to clear it.

No loadErrors key means everything configured loaded and nothing is stale.`;

/**
 * Registers the malloy_getStatus MCP tool: the MCP analog of GET /api/v0/status,
 * reduced to what an agent needs to judge health (state, package names, load
 * errors, staleness). Without it an agent cannot distinguish "empty package"
 * from "package that failed to load", and a failed watch-mode recompile is
 * invisible over MCP entirely.
 *
 * SECURITY: parity with the unauthenticated REST /status endpoint, minus
 * detail. Emits names, states, and (already-redacted) load-error messages;
 * never connection attributes, locations, or row data.
 */
export function registerGetStatusTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool("malloy_getStatus", GET_STATUS_DESCRIPTION, {}, async () => {
      const uri = buildMalloyUri({}, "getStatus");
      try {
         const status = await environmentStore.getStatus();
         const payload = {
            operationalState: status.operationalState,
            initialized: status.initialized,
            environments: status.environments.map((environment) => ({
               name: environment.name,
               // Name is optional in the API schema but always set by
               // listPackages; filtered rather than emitted as a null an agent
               // would have to reason about.
               packages: (environment.packages ?? [])
                  .map((pkg) => pkg.name)
                  .filter((name): name is string => name !== undefined),
            })),
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
