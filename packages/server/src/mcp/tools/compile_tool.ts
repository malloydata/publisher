// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
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

const compileShape = {
   environmentName: z
      .string()
      .describe("Environment name, as list_packages returns it."),
   packageName: z
      .string()
      .describe("Package containing the model, as list_packages returns it."),
   modelPath: z
      .string()
      .describe(
         "Path to the .malloy model the check targets: the namespace the source is appended to (scope append), or the file it replaces (scope file / package-with-source).",
      ),
   source: z
      .string()
      .optional()
      .describe(
         "The Malloy source to validate; never executed. Required at scope append/file; optional at scope package (a what-if replacement for modelPath). Text with a model-level ## artifact tag is a report, wherever it lives (never public/ or the root index.malloy); its layout is the tag's layout=grid|linear, else the deprecated kind= (dashboard is grid, notebook linear), else grid when it lists tiles or has a dashboard { … } tag, else linear.",
      ),
   scope: z
      .enum(["append", "file", "package"])
      .optional()
      .describe(
         'What source means. "append" (default): append to modelPath. "file": compile AS modelPath to validate an edit. "package": dry-run a reload over all .malloy/.malloynb files without serving the result, including its render-tag and report checks; an optional source replaces modelPath.',
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
         "Given values for the model's given: block. Also required to satisfy any authorize gate on the target model, whether or not includeSql is set.",
      ),
};

const COMPILE_DESCRIPTION = `Compile-check Malloy without running a query. Use this while authoring instead of a throwaway execute_query.

## Scopes (the scope parameter)
- "append" (default): append source to modelPath. Use for NEW definitions; existing definitions report "Cannot redefine". Positions refer to the concatenated file (a ## artifact document reads only your text, and no document is returned). Refused here, with a 400: import, connection.table(...), connection.sql(...), raw-SQL functions, given: declarations (reading the model's givens as $NAME is fine), ##! flags, and text that does not stand alone as top-level Malloy — a bare view:/dimension:/measure:, or anything parsing only as a continuation of the model's last statement. A ## artifact document also refuses # image, # link and @env.; plain text keeps them. Wrap a view body in a top-level query:, a field in a throwaway source: check is <source> extend { … }; use "file" or "package" for a model that declares data roots.
- "file": compile source AS modelPath. Use to validate an EDIT before saving; positions match the submitted file.
- "package": dry-run a reload of every .malloy/.malloynb file without serving the result, with its render-tag and report checks (no position) but not its other warnings. A source replaces modelPath so importers see the edit. Diagnostics may name hidden files; no rows or SQL are returned, and authorize gates still apply to caller text. An unmatched path is warned and treated as new. Save, then reload_package to serve it.

## Parameters
- environmentName, packageName, modelPath: required. source: required at append/file, optional at package. Caller source may not declare an access-control gate outside prose — neither #(authorize) nor #(access_filter).

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
 * Renders error diagnostics as the prose a text-only client shows (see
 * tool_response.ts). Positions stay 0-based, matching the payload. Only
 * error-severity diagnostics are rendered; falling back to all of them keeps
 * the text non-empty if that invariant ever breaks.
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
 * Registers compile_model: structured diagnostics without running a query, via
 * the same compile path as the REST /compile endpoint (CompileController).
 */
export function registerCompileTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   const compileController = new CompileController(environmentStore);

   mcpServer.tool(
      "compile_model",
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

         logger.debug("[MCP Tool compile] Compiling source", {
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

         // Compile resolves schemas against the warehouse, so it takes a query
         // slot like the HTTP compile route; one ungated surface would relocate
         // the bypass. A refusal throws into the catch below.
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

            // Flattened to 0-based line/character (LSP-style). `model` is the
            // package-relative file, essential at scope "package".
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

            // isError so the agent does not treat the model as valid, with a
            // text block for text-only clients. The payload keeps its documented
            // {status, diagnostics} shape rather than jsonToolError's.
            const isError = result.status === "error";
            return jsonResource(uri, payload, {
               isError,
               ...(isError && { text: formatDiagnosticsText(diagnostics) }),
            });
         } catch (error) {
            // Not found, a notebook, an authorize denial, an unloadable model
            // (a modelPath typo lands here at append scope), or a system error:
            // a clean isError payload rather than a transport fault.
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
