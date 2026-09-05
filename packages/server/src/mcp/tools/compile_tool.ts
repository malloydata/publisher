// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { GivenValue } from "@malloydata/malloy";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { CompileController } from "../../controller/compile.controller";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import {
   type QuerySlotHandle,
   tryAcquireQuerySlot,
} from "../../query_concurrency";
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
         'What source means. "append" (default): append to modelPath. "file": compile AS modelPath to validate an edit. "package": run reload\'s worker compiler over all .malloy/.malloynb files without serving the result; an optional source replaces modelPath.',
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

const COMPILE_DESCRIPTION = `Compile-check Malloy without running a query. Use this while authoring instead of a throwaway malloy_executeQuery.

## Scopes (the scope parameter)
- "append" (default): append source to modelPath. Use for NEW definitions; existing definitions report "Cannot redefine". Positions refer to the concatenated file.
- "file": compile source AS modelPath. Use to validate an EDIT before saving; positions match the submitted file.
- "package": run reload's worker compiler over all .malloy/.malloynb files without changing the served package. Optional source replaces modelPath so importers see the edit. Diagnostics may name files hidden from discovery; no rows or SQL are returned, and #(authorize) still gates caller text. A missing exact path is warned and treated as a new file. Save and call malloy_reloadPackage to serve a clean edit.

## Parameters
- environmentName, packageName, modelPath: required. source: required at append/file, optional at package. includeSql: append/file only. givens: model givens and #(authorize) values. Caller source may not declare #(authorize).

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

         // Compile resolves source schemas against the connection, so it draws on
         // the same warehouse work the query cap exists to bound. Gated here for
         // the same reason the HTTP compile route is: leaving one surface ungated
         // relocates the bypass rather than closing it. A refusal throws
         // ServiceUnavailableError, which the catch below turns into the standard
         // MCP error payload.
         let querySlot: QuerySlotHandle | null = null;
         try {
            querySlot = tryAcquireQuerySlot("mcp:compile");
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
         } finally {
            querySlot?.release();
         }
      },
   );
}
