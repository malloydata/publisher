import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { GivenValue } from "@malloydata/malloy";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { CompileController } from "../../controller/compile.controller";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

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

## Checking part of a source
Your source is APPENDED to the model, so it has to stand on its own as top-level Malloy. Two things follow, and both have error messages that name the symptom rather than the cause:
- A bare \`view:\`, \`dimension:\`, or \`measure:\` is not a top-level statement. Submitted alone it fails with "no viable alternative at input 'view:'". To check a view's body, send it as a top-level query instead: \`query: check is <source> -> { ... }\`. To check a field or view in the namespace it will actually live in, wrap it in a throwaway extension: \`source: check is <source> extend { <your fragment> }\`. Both compile against the real source, so inherited measures resolve and \`private:\` fields stay hidden exactly as they would in place.
- Resubmitting the whole source you are editing fails with "Cannot redefine '<name>'", because the model already declares that name. Use one of the two forms above instead. Note an extension ADDS to a source's namespace rather than overriding it, so a fragment reusing an existing view name reports the same error; rename it for the check.

## Response
A JSON object with status ("success" or "error") and diagnostics: an array of { severity ("error" / "warn" / "debug"), message, code, line, character, endLine, endCharacter, replacement }. Positions are 0-based (line and character start at 0) and relative to the model file with your source appended to it, so a diagnostic in your submitted source lands after the model's own line count, and a diagnostic may point at pre-existing content in the model rather than at your source. Any wrapper you add counts toward that offset too. A clean compile can still return warnings; status is "error" only when at least one diagnostic has error severity. When status is "error" the response also states the error diagnostics in a plain text block alongside the JSON, so the failure is legible without parsing the payload.`;

/** The flattened diagnostic shape this tool returns. */
type CompileDiagnostic = {
   severity: string;
   message: string;
   code?: string;
   line?: number;
   character?: number;
};

/**
 * Renders error diagnostics as the prose a text-only client will show.
 *
 * A failed compile is an isError result whose diagnostics live in the resource
 * block, so a client that renders only text blocks on isError sees nothing —
 * which is how a real compile error gets reported as a bare "Unknown error".
 * The resource block stays the parseable channel; this is the legible one.
 * Positions are labelled 0-based to match the payload rather than introduce a
 * second convention for the same numbers.
 *
 * Only error-severity diagnostics are rendered: status is "error" because of
 * those, and they are what the caller has to fix. Falling back to every
 * diagnostic keeps the text non-empty if that invariant ever breaks.
 *
 * Exported so a spec can pin the rendering rather than merely its presence.
 */
export function formatDiagnosticsText(
   diagnostics: CompileDiagnostic[],
): string {
   const errors = diagnostics.filter((d) => d.severity === "error");
   const shown = errors.length > 0 ? errors : diagnostics;
   const lines = shown.map((d) => {
      const at =
         d.line !== undefined
            ? ` (line ${d.line}, character ${d.character ?? 0})`
            : "";
      const code = d.code ? ` [${d.code}]` : "";
      return `- ${d.message}${at}${code}`;
   });
   const noun = shown.length === 1 ? "error" : "errors";
   return `Compile failed with ${shown.length} ${noun} (positions are 0-based):\n\n${lines.join("\n")}`;
}

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

            // A compile that returns error diagnostics is still a successful
            // tool call in the transport sense, but isError tells the agent not
            // to treat the model as valid. This is the most common way the tool
            // fails, so it carries a text block for the same reason the catch
            // below does: the diagnostics are only the message to a client that
            // parses the resource. The payload keeps its documented
            // {status, diagnostics} shape rather than the {error, suggestions}
            // of jsonToolError, so parsing clients are unaffected.
            const isError = result.status === "error";
            return jsonResource(uri, payload, {
               isError,
               ...(isError && { text: formatDiagnosticsText(diagnostics) }),
            });
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
            const errorDetails: ErrorDetails = classifyToolError(
               "compile",
               `${environmentName}/${packageName}/${modelPath}`,
               error,
            );
            return jsonToolError(uri, errorDetails);
         }
      },
   );
}
