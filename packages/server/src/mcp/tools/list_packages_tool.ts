// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../../service/environment_store";
import { readPackageCatalog } from "../../retrieval/model/package_catalog";
import { buildMalloyUri } from "../handler_utils";
import { registerGuardedTool } from "../register_tool";
import { jsonResource } from "../tool_response";

const LIST_ENVIRONMENTS_DESCRIPTION = `List what this Publisher serves: every environment, and the packages in each.

Call this when you do not already know an environment and package name, or to see a package that failed to load. get_context with no arguments lists the same names; never guess them.

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
   registerGuardedTool(mcpServer, {
      name: "list_packages",
      description: LIST_ENVIRONMENTS_DESCRIPTION,
      inputSchema: {},
      op: "listPackages",
      failed: "listing failed",
      uri: () => buildMalloyUri({}, "list-environments"),
      identifier: () => "environments",
      errorPayload: { environments: [] },
      run: async (_params, uri) =>
         jsonResource(uri, {
            environments: await readPackageCatalog(environmentStore),
         }),
   });
}
