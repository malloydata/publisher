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
         "Path to the .malloy model the check targets: the namespace the source is appended to (scope append), or the file it replaces (scope file / package-with-source).",
      ),
   source: z
      .string()
      .optional()
      .describe(
         "The Malloy source to validate; never executed. Required at scope append/file; optional at scope package (a what-if replacement for modelPath).",
      ),
   scope: z
      .enum(["append", "file", "package"])
      .optional()
      .describe(
         'What source means. "append" (default): appended to modelPath, for validating NEW definitions (an edit collides with "Cannot redefine"). "file": compiled AS modelPath, replacing its content — validates an edit pre-save at true file coordinates. "package": dry-run every .malloy file as saved (reload\'s reach, none of its effects); with source, importers compile against the edit.',
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

const COMPILE_DESCRIPTION = `Compile-check Malloy source against a package and return structured diagnostics WITHOUT running anything. Use this to validate a model or a change while authoring, instead of a throwaway malloy_executeQuery.

## Scopes (the scope parameter)
- "append" (default): the source is appended to modelPath and compiled in its namespace. For NEW definitions and queries. Resubmitting a definition the model already declares reports "Cannot redefine" — use scope "file" for edits — and diagnostics are positioned in the CONCATENATED file, so a line in your source lands after the model's own line count.
- "file": the source is compiled AS modelPath, replacing its on-disk content for this check. This is how to validate an EDIT before saving; diagnostics land at true file coordinates.
- "package": a dry-run of every .malloy file in the package as saved — reload's reach (imports across files) with none of its effects on the served model. An optional source is a what-if replacement for modelPath, so importers compile against the edit. includeSql is not available here. A clean dry-run still requires malloy_reloadPackage after saving for the edit to serve.

## Parameters
- environmentName, packageName, modelPath (required). source: required at append/file, optional at package. includeSql (append/file): also return generated SQL when the source ends in a runnable query; never executed, no data scanned. givens: values for the model's given: block and any #(authorize) gate. An #(authorize) annotation in the source itself is rejected — gates come only from package files.

## Response
{ status: "success"|"error", diagnostics: [{ severity, message, code, model, line, character, endLine, endCharacter, replacement }], sql? }. Positions are 0-based; model is the package-relative file the diagnostic points at (which can be pre-existing content, not your source). status is "error" only when an error-severity diagnostic exists; errors are also stated in a plain text block.`;

/** The flattened diagnostic shape this tool returns. */
type CompileDiagnostic = {
   severity: string;
   message: string;
   code?: string;
   model?: string;
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
      // The file tag matters most at scope "package", where one list carries
      // every model's problems.
      const model = d.model ? `${d.model}: ` : "";
      return `- ${model}${d.message}${at}${code}`;
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
            scope,
         } = params;

         logger.info("[MCP Tool compile] Compiling source", {
            environmentName,
            packageName,
            modelPath,
            includeSql: !!includeSql,
            scope: scope ?? "append",
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
               scope ?? "append",
            );

            // Flatten each LogMessage's nested at.range into line/character so
            // agents do not have to walk it. Positions are 0-based (LSP-style).
            // `model` is the package-relative file a diagnostic points at —
            // load-bearing at scope "package", clarifying elsewhere (an
            // append-scope diagnostic can point at pre-existing content).
            const diagnostics = result.problems.map((p) => ({
               severity: p.severity,
               message: p.message,
               code: p.code,
               model: p.model,
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
