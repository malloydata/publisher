import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { logger } from "../../logger";
import { ProjectStore } from "../../service/project_store";
import { getMalloyErrorDetails, type ErrorDetails } from "../error_messages";
import { buildMalloyUri, getModelForQuery } from "../handler_utils";
import { MCP_ERROR_MESSAGES } from "../mcp_constants";

// Zod shape defining required/optional params for executeQuery
const executeQueryShape = {
   // projectName is required; other fields mirror SDK expectations
   projectName: z
      .string()
      .describe(
         "Project name. Project names are listed in the malloy resource list.",
      ),
   packageName: z
      .string()
      .describe(
         "Package containing the model. Package names are listed in the malloy resource list.",
      ),
   modelPath: z.string().describe("Path to the .malloy model file"),
   query: z.string().optional().describe("Ad-hoc Malloy query code"),
   sourceName: z.string().optional().describe("Source name for a view"),
   queryName: z.string().optional().describe("Named query or view"),
};

// Type inference is handled automatically by the MCP server based on the executeQueryShape

/**
 * Registers the malloy_executeQuery tool with the MCP server.
 */
export function registerExecuteQueryTool(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   mcpServer.tool(
      "malloy_executeQuery",
      "Executes a Malloy query (either ad-hoc or a named query/view defined in a model) against the specified model and returns the results as JSON.",
      executeQueryShape,
      /** Handles requests for the malloy_executeQuery tool */
      async (params) => {
         // Destructure projectName as well
         const {
            projectName,
            packageName,
            modelPath,
            query,
            sourceName,
            queryName,
         } = params;

         logger.info("[MCP Tool executeQuery] Received params:", { params });

         const hasAdhocQuery = !!query;
         const hasNamedQuery = !!queryName;

         if (!hasAdhocQuery && !hasNamedQuery) {
            throw new McpError(
               ErrorCode.InvalidParams,
               MCP_ERROR_MESSAGES.MISSING_REQUIRED_PARAMS,
            );
         }
         if (hasAdhocQuery && hasNamedQuery) {
            throw new McpError(
               ErrorCode.InvalidParams,
               MCP_ERROR_MESSAGES.MUTUALLY_EXCLUSIVE_PARAMS,
            );
         }
         // Zod/SDK handles missing required fields (packageName, modelPath) based on the shape

         // --- Get Package and Model ---
         logger.info(
            `[MCP Tool executeQuery] Calling getModelForQuery for ${projectName}/${packageName}/${modelPath}`,
         );
         const modelResult = await getModelForQuery(
            projectStore,
            projectName,
            packageName,
            modelPath,
         );

         // Handle errors during package/model access (e.g., not found, initial compilation)
         if ("error" in modelResult) {
            // Format error details as structured JSON
            const errorJson = JSON.stringify(
               {
                  error: modelResult.error.message,
                  suggestions: modelResult.error.suggestions,
               },
               null,
               2,
            );
            return {
               isError: true,
               // Return as application/json nested inside a 'resource' type
               content: [
                  {
                     type: "resource", // Use 'resource' type
                     resource: {
                        type: "application/json", // Actual content type
                        uri: "error://executeQuery/modelAccess", // Placeholder URI
                        text: errorJson,
                     },
                  },
               ],
            };
         }

         // --- Execute Query ---
         const { model } = modelResult;
         logger.info(
            `[MCP Tool executeQuery] Model found. Proceeding to execute query.`,
         );
         try {
            // If ad-hoc query is provided, use it directly in the 3rd arg
            if (query) {
               const { result } = await model.getQueryResults(
                  undefined,
                  undefined,
                  query,
               );

               // --- Format Success Response (Duplicated for now, could refactor) ---
               const baseUriComponents = {
                  project: projectName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               };
               const resultUri = buildMalloyUri(baseUriComponents, "result");
               const resultString = JSON.stringify(result, null, 2);
               return {
                  isError: false,
                  content: [
                     {
                        type: "resource",
                        resource: {
                           type: "application/json",
                           uri: resultUri,
                           text: resultString,
                        },
                     },
                  ],
               };
            } else if (queryName) {
               // Otherwise, use sourceName/queryName in 1st/2nd args
               const { result } = await model.getQueryResults(
                  sourceName,
                  queryName,
                  undefined,
               );

               // --- Format Success Response ---
               // Use the helper function to build valid URIs
               const baseUriComponents = {
                  project: projectName,
                  package: packageName,
                  resourceType: "models" as const,
                  resourceName: modelPath,
               };
               const resultUri = buildMalloyUri(baseUriComponents, "result");

               const resultString = JSON.stringify(result, null, 2);

               return {
                  isError: false,
                  content: [
                     {
                        type: "resource",
                        resource: {
                           type: "application/json",
                           uri: resultUri,
                           text: resultString,
                        },
                     },
                  ],
               };
            }

            // If execution reaches this point, something has gone wrong with
            // the earlier parameter validation logic. Throw an explicit error
            // so the return type is never 'undefined' from the compiler's
            // perspective.
            throw new McpError(
               ErrorCode.InternalError,
               "Unreachable executeQuery code path – parameters were not validated correctly.",
            );
         } catch (queryError) {
            // Handle query execution errors (syntax errors, invalid queries, etc.)
            logger.error(
               `[MCP Server Error] Error executing query in ${projectName}/${packageName}/${modelPath}:`,
               { error: queryError },
            );
            const errorDetails: ErrorDetails = getMalloyErrorDetails(
               "executeQuery",
               `${projectName}/${packageName}/${modelPath}`, // Include project
               queryError,
            );

            // Format error details as structured JSON
            const errorJson = JSON.stringify(
               {
                  error: errorDetails.message,
                  suggestions: errorDetails.suggestions,
               },
               null,
               2,
            );
            return {
               isError: true,
               // Return as application/json nested inside a 'resource' type
               content: [
                  {
                     type: "resource", // Use 'resource' type
                     resource: {
                        type: "application/json", // Actual content type
                        uri: "error://executeQuery/queryExecution", // Placeholder URI
                        text: errorJson,
                     },
                  },
               ],
            };
         }
      },
   );
}
