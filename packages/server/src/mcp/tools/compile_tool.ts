import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { GivenValue } from "@malloydata/malloy";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { CompileController } from "../../controller/compile.controller";
import { getMalloyErrorDetails, type ErrorDetails } from "../error_messages";
import { buildMalloyUri } from "../handler_utils";

// Zod shape for malloy_compile. environmentName/packageName mirror the other
// tools and point the agent at malloy_getContext for name discovery.
const compileShape = {
   environmentName: z
      .string()
      .describe(
         "Environment name. Call malloy_getContext with no arguments to list the available environments.",
      ),
   packageName: z
      .string()
      .describe(
         "Package containing the model. Call malloy_getContext with just environmentName to list its packages.",
      ),
   modelPath: z
      .string()
      .describe(
         "Path to the .malloy model whose namespace the source compiles against. The source is appended to this model, so its imports, sources, and queries are in scope.",
      ),
   source: z
      .string()
      .describe(
         "The Malloy source to validate. Compiled in the context of modelPath and not executed.",
      ),
   includeSql: z
      .boolean()
      .optional()
      .describe(
         "When true and the source ends in a runnable query, also return the generated SQL for inspection. The query is still not run.",
      ),
   givens: z
      .record(z.unknown())
      .optional()
      .describe(
         "Given values for the model's given: block. Also required to satisfy any #(authorize) gate on the target model, whether or not includeSql is set.",
      ),
};

const COMPILE_DESCRIPTION = `Compile-check Malloy source against a model and return structured diagnostics WITHOUT running a query. Use this to validate a model or a change while authoring, instead of firing a throwaway malloy_executeQuery just to see whether it parses.

## Parameters
- environmentName, packageName, modelPath (required): the model whose namespace the source compiles against. The source is appended to that model, so its imports, sources, and queries are in scope, and modelPath is real context, not a label.
- source (required): the Malloy text to validate.
- includeSql (optional): also return the generated SQL when the source ends in a runnable query. The query is still not executed and no data is scanned.

## Response
A JSON object with status ("success" or "error") and diagnostics: an array of { severity ("error" / "warn" / "debug"), message, code, line, character, endLine, endCharacter, replacement }. Positions are 0-based (line and character start at 0) and relative to the model file with your source appended to it, so a diagnostic in your submitted source lands after the model's own line count, and a diagnostic may point at pre-existing content in the model rather than at your source. A clean compile can still return warnings; status is "error" only when at least one diagnostic has error severity.`;

/**
 * Registers the malloy_compile MCP tool: validates Malloy source against a model
 * and returns structured diagnostics without executing a query. Wraps the same
 * in-process compile path the REST /compile endpoint uses (CompileController).
 */
export function registerCompileTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const compileController = new CompileController(environmentStore);

   mcpServer.tool(
      "malloy_compile",
      COMPILE_DESCRIPTION,
      compileShape,
      async (params) => {
         const {
            environmentName,
            packageName,
            modelPath,
            source,
            includeSql,
            givens,
         } = params;

         logger.info("[MCP Tool compile] Compiling source", {
            environmentName,
            packageName,
            modelPath,
            includeSql: !!includeSql,
         });

         const uri = buildMalloyUri(
            {
               environment: environmentName,
               package: packageName,
               resourceType: "models",
               resourceName: modelPath,
            },
            "compile",
         );

         try {
            const result = await compileController.compile(
               environmentName,
               packageName,
               modelPath,
               source,
               includeSql ?? false,
               givens as Record<string, GivenValue> | undefined,
            );

            // Flatten each LogMessage's nested at.range into line/character so
            // agents do not have to walk it. Positions are 0-based (LSP-style).
            const diagnostics = result.problems.map((p) => ({
               severity: p.severity,
               message: p.message,
               code: p.code,
               line: p.at?.range.start.line,
               character: p.at?.range.start.character,
               endLine: p.at?.range.end.line,
               endCharacter: p.at?.range.end.character,
               replacement: p.replacement,
            }));

            const payload = {
               status: result.status,
               diagnostics,
               ...(result.sql !== undefined && { sql: result.sql }),
            };

            return {
               isError: result.status === "error",
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
            // Unknown environment/package, a notebook (.malloynb) rejected up
            // front, an authorize denial, or a system error: surface as a clean
            // isError payload rather than a transport fault. A missing modelPath
            // does NOT error here; compileSource compiles the source against an
            // empty namespace, so a typo in modelPath yields a normal result.
            logger.warn("[MCP Tool compile] compile failed", {
               environmentName,
               packageName,
               modelPath,
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = getMalloyErrorDetails(
               "compile",
               `${environmentName}/${packageName}/${modelPath}`,
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
