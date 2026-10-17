// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../../service/environment_store";
import { logger } from "../../logger";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

/** list_packages' counterpart: the error envelope matches its success shape. */
function listPackagesError(uri: string, error: unknown) {
   return jsonToolError(
      uri,
      classifyToolError("listPackages", "environments", error),
      { environments: [] },
   );
}

const LIST_ENVIRONMENTS_DESCRIPTION = `List what this Publisher serves: every environment, and the packages in each.

Call this when you do not already know an environment and package name. get_context requires one of each in its \`scopes\`, and those names come from here; never guess them.

Each package reports \`name\`, its \`description\` where it has one, and two health facts that are otherwise invisible:
- \`error\` with no \`stale\`: the package FAILED to load. It is not queryable, and it would otherwise simply be missing, which reads as "does not exist".
- \`error\` with \`stale: true\`: the package IS serving and answering, but its most recent reload failed to compile, so its names and any numbers you get from it come from the model compiled BEFORE that save. Fix the model and call reload_package.

An environment carries \`error\` on the same terms: its packages could not be listed at all (an unreachable store, expired credentials), so its \`packages\` is empty because the listing FAILED, not because the environment is empty. Without that field the two are the same payload. Report the error rather than telling the user there is nothing there.

Takes no arguments.`;

/**
 * The environment and package catalog, which supplies the names `get_context`'s
 * required `scopes` need.
 *
 * A package that failed to load is absent from a plain listing, which reads as
 * "does not exist"; reporting it with its error is the only place outside
 * get_status that shows it is broken.
 */
export function registerListPackagesTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "list_packages",
      LIST_ENVIRONMENTS_DESCRIPTION,
      {},
      async () => {
         const uri = buildMalloyUri({}, "list-environments");
         try {
            const environments = await environmentStore.listEnvironments();
            const results = [];
            for (const env of environments) {
               const name = env.name;
               if (!name) continue;
               let packages: unknown[] = [];
               let environmentError: string | undefined;
               try {
                  const environment = await environmentStore.getEnvironment(
                     name,
                     false,
                  );
                  const staleErrors = environment.getStaleCompileErrors();
                  packages = (await environment.listPackages()).map((pkg) => {
                     const stale = pkg.name
                        ? staleErrors.get(pkg.name)
                        : undefined;
                     return {
                        name: pkg.name,
                        ...(pkg.description
                           ? { description: pkg.description }
                           : {}),
                        ...(stale && { error: stale.message, stale: true }),
                     };
                  });
                  for (const [
                     failed,
                     message,
                  ] of environment.getFailedPackages()) {
                     packages.push({ name: failed, error: message });
                  }
               } catch (error) {
                  // One unreachable environment must not hide the others, but
                  // must be reported too: an empty list would read as a
                  // genuinely empty environment.
                  environmentError =
                     error instanceof Error ? error.message : String(error);
                  logger.warn(
                     "[MCP Tool listPackages] listing packages failed",
                     {
                        environmentName: name,
                        error: environmentError,
                     },
                  );
                  packages = [];
               }
               results.push({
                  name,
                  ...(environmentError ? { error: environmentError } : {}),
                  packages,
               });
            }
            return jsonResource(uri, { environments: results });
         } catch (error) {
            logger.warn("[MCP Tool listPackages] listing failed", {
               error: error instanceof Error ? error.message : String(error),
            });
            return listPackagesError(uri, error);
         }
      },
   );
}
