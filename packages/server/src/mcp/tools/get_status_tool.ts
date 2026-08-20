import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";
import { describeWorkspaceSetup } from "./workspace_setup";

const GET_STATUS_DESCRIPTION = `Report the server's health: its operational state, every configured package or environment that failed to load or is serving a stale model, and, when nothing is being served at all, why the directory is not set up. This is the only MCP surface where a load failure is visible, so call it before concluding a package is empty or missing, and after a model edit whose reload you did not run yourself (e.g. you rely on watch mode).

## Contract rules
- A setup key means NO package is being served. Do setup.nextAction first; further query or context calls cannot succeed.
- Fix files at setup.unservedPackages[].sourcePath, never under publisher_data/: that is server-managed state, and it is the path loadErrors names.

## Response
A JSON object with:
- operationalState: "initializing" | "serving" | "throttled" | "draining".
- initialized: whether startup finished.
- environments: each environment's name with its loaded package names.
- loadErrors (only present when something failed): entries of {environment, package?, message, stale?, failedAt?}. An entry WITHOUT stale means the package (or whole environment, when package is absent) did not load and is missing from environments. An entry WITH stale: true means the package IS serving, but its most recent reload failed to compile, so the model answering queries is OLDER than the files on disk; the message says why. Fix the file and reload (malloy_reloadPackage) to clear it.
- setup (only present when the server is up and serving no package at all): {problem, nextAction, serverRoot, configFile, unservedPackages?, unservedPackagesTruncated?, configuredConnections?, unclaimedModelFiles?}. Read problem and nextAction; the rest is supporting detail. unservedPackagesTruncated true means the list is capped and does NOT name every package.

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
 * never connection attributes or row data. The `setup` block is the one
 * place locations are emitted (server root, package source paths, and the
 * names of stray .malloy files at the root). Two things bound it, and note
 * what does NOT: in the un-set-up case there are no load errors at all, so
 * this block is the only disclosure rather than a repeat of one. What holds
 * is that it is returned ONLY when the server is up and serving no package,
 * so a healthy deployment never produces it, and that the unauthenticated
 * REST /status already returns each environment's absolute location.
 * Connection NAMES only, never attributes.
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
         // Only when nothing is served: a healthy deployment never pays for
         // this, and never emits the paths it carries. Diagnosis is additive,
         // so its own failure must never turn a healthy status call into a
         // tool error; it degrades to the status payload without it.
         let setup;
         try {
            setup = await describeWorkspaceSetup(
               environmentStore.serverRootPath,
               payload,
            );
         } catch (setupError) {
            logger.warn("[MCP Tool getStatus] setup diagnosis failed", {
               error:
                  setupError instanceof Error
                     ? setupError.message
                     : String(setupError),
            });
         }
         return jsonResource(uri, { ...payload, ...(setup && { setup }) });
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
