// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { formatDiagnosticsText, registerCompileTool } from "./compile_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import { PackageNotFoundError, ServiceUnavailableError } from "../../errors";

// Capture the handler registerCompileTool passes to McpServer.tool, so it can
// be exercised against a mocked EnvironmentStore. The tool builds a
// CompileController internally, which calls environment.compileSource, so the
// mock only needs getEnvironment -> { compileSource }.
type Content = Array<{
   type?: string;
   text?: string;
   resource?: { text: string };
}>;

type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Content;
}>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerCompileTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Content }) {
   return JSON.parse(result.content[0].resource!.text);
}

function textBlock(result: { content: Content }) {
   return result.content.find((b) => b.type === "text")?.text;
}

function storeReturning(
   problems: Array<Record<string, unknown>>,
   sql?: string,
): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            compileSource: async () => ({ problems, sql }),
         }) as never,
   };
}

const args = {
   environmentName: "malloy-samples",
   packageName: "ecommerce",
   modelPath: "ecommerce.malloy",
   source: "run: order_items -> { aggregate: c is count() }",
};

describe("compile_model tool", () => {
   it("returns status success and empty diagnostics for a clean compile", async () => {
      const handler = captureHandler(storeReturning([]));
      const result = await handler(args);
      expect(result.isError).toBe(false);
      expect(parse(result)).toEqual({ status: "success", diagnostics: [] });
   });

   it("flattens an error diagnostic and sets isError", async () => {
      const handler = captureHandler(
         storeReturning([
            {
               severity: "error",
               message: "'nope' is not defined",
               code: "field-not-found",
               at: {
                  range: {
                     start: { line: 2, character: 3 },
                     end: { line: 2, character: 7 },
                  },
               },
               replacement: undefined,
            },
         ]),
      );
      const result = await handler(args);
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.status).toBe("error");
      expect(parsed.diagnostics).toEqual([
         {
            severity: "error",
            message: "'nope' is not defined",
            code: "field-not-found",
            line: 2,
            character: 3,
            endLine: 2,
            endCharacter: 7,
         },
      ]);
   });

   it("also states the diagnostics in a text block", async () => {
      // The regression pin for the isError path that matters most: a bad Malloy
      // snippet. A client that renders only text blocks on isError sees nothing
      // without this, and reports a bare "Unknown error" while the diagnostics
      // sit unread in the resource block.
      const handler = captureHandler(
         storeReturning([
            {
               severity: "error",
               message: "'nope' is not defined",
               code: "field-not-found",
               at: {
                  range: {
                     start: { line: 2, character: 3 },
                     end: { line: 2, character: 7 },
                  },
               },
            },
         ]),
      );
      const result = await handler(args);
      expect(textBlock(result)).toBe(
         "Compile failed with 1 error (positions are 0-based):\n\n" +
            "- 'nope' is not defined (line 2, character 3) [field-not-found]",
      );
   });

   it("adds no text block to a clean compile", async () => {
      // Success stays resource-only: the payload is the answer, and duplicating
      // it as prose would double every response for no gain.
      const handler = captureHandler(storeReturning([]));
      expect(textBlock(await handler(args))).toBeUndefined();
   });

   it("treats a warning as a clean compile (status success, isError false)", async () => {
      const handler = captureHandler(
         storeReturning([
            {
               severity: "warn",
               message: "deprecated syntax",
               code: "deprecated",
               at: {
                  range: {
                     start: { line: 0, character: 0 },
                     end: { line: 0, character: 4 },
                  },
               },
            },
         ]),
      );
      const result = await handler(args);
      expect(result.isError).toBe(false);
      const parsed = parse(result);
      expect(parsed.status).toBe("success");
      expect(parsed.diagnostics[0].severity).toBe("warn");
   });

   it("passes scope through to compileSource, defaulting to append", async () => {
      const scopes: unknown[] = [];
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               compileSource: async (
                  ..._args: unknown[]
               ): Promise<{ problems: unknown[] }> => {
                  // Positional: packageName, modelName, source, includeSql,
                  // givens, scope.
                  scopes.push(_args[5]);
                  return { problems: [] };
               },
            }) as never,
      });
      await handler(args);
      await handler({ ...args, scope: "package", source: undefined });
      expect(scopes).toEqual(["append", "package"]);
   });

   it("carries the model tag through diagnostics and the text block", async () => {
      // `model` is what keeps a package-scope result legible: one array holds
      // every file's problems, and without the tag two files' identical
      // messages are indistinguishable.
      const handler = captureHandler(
         storeReturning([
            {
               severity: "error",
               message: "Reference to undefined value nope",
               model: "tracks.malloy",
               at: {
                  url: "file:///pkg/tracks.malloy",
                  range: {
                     start: { line: 2, character: 20 },
                     end: { line: 2, character: 24 },
                  },
               },
            },
         ]),
      );
      const result = await handler({ ...args, scope: "package" });
      const parsed = parse(result);
      expect(parsed.diagnostics[0].model).toBe("tracks.malloy");
      expect(textBlock(result)).toContain(
         "tracks.malloy: Reference to undefined value nope",
      );
   });

   it("includes sql when the controller returns it", async () => {
      const handler = captureHandler(storeReturning([], "SELECT 1"));
      const parsed = parse(await handler({ ...args, includeSql: true }));
      expect(parsed.sql).toBe("SELECT 1");
   });

   it("surfaces a thrown error as a tool error, not a transport fault", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new Error("Environment 'nope' could not be resolved");
         },
      });
      const result = await handler({ ...args, environmentName: "nope" });
      expect(result.isError).toBe(true);
      const parsed = parse(result);
      expect(parsed.error).toBeDefined();
   });

   it("reports a missing package as not-found, not as a Malloy syntax problem", async () => {
      // Pin the remediation text, not just that an error came back. Asserting
      // only that `error` is defined passes even when every class funnels
      // through the Malloy helper, which is the bug classifyToolError exists to
      // fix: a typo'd package name told the caller to check its Malloy syntax.
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new PackageNotFoundError("Package 'nope' not found");
         },
      });
      const parsed = parse(await handler({ ...args, packageName: "nope" }));
      expect(parsed.error).toContain("Resource not found");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("reports back-pressure as retryable, not as a Malloy syntax problem", async () => {
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new ServiceUnavailableError("Memory limit reached");
         },
      });
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("Memory limit reached");
      expect(JSON.stringify(parsed.suggestions)).toContain("Retry");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("reports an unexpected internal error without blaming the Malloy", async () => {
      // The catch-all used to hand a TypeError to the Malloy helper, so a bug
      // in our own code told the agent to go edit a model that was fine.
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new TypeError("cannot read properties of undefined");
         },
      });
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("unexpected internal error");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("omits position fields when a diagnostic has no location", async () => {
      const handler = captureHandler(
         storeReturning([
            { severity: "error", message: "no location", code: "syntax" },
         ]),
      );
      const parsed = parse(await handler(args));
      expect(parsed.diagnostics[0]).toEqual({
         severity: "error",
         message: "no location",
         code: "syntax",
      });
   });

   describe("formatDiagnosticsText", () => {
      it("renders only the error-severity diagnostics", () => {
         const text = formatDiagnosticsText([
            { severity: "warn", message: "deprecated syntax", code: "dep" },
            { severity: "error", message: "first", line: 1, character: 0 },
            { severity: "error", message: "second", line: 4, character: 2 },
         ]);
         expect(text).toBe(
            "Compile failed with 2 errors (positions are 0-based):\n\n" +
               "- first (line 1, character 0)\n" +
               "- second (line 4, character 2)",
         );
         expect(text).not.toContain("deprecated syntax");
      });

      it("omits the position when a diagnostic has no location", () => {
         expect(
            formatDiagnosticsText([
               { severity: "error", message: "no location", code: "syntax" },
            ]),
         ).toBe(
            "Compile failed with 1 error (positions are 0-based):\n\n" +
               "- no location [syntax]",
         );
      });

      it("renders line 0 / character 0 rather than dropping them", () => {
         // 0 is a real position, not a missing one; a falsy check here would
         // silently swallow the first line of the file.
         expect(
            formatDiagnosticsText([
               {
                  severity: "error",
                  message: "at the top",
                  line: 0,
                  character: 0,
               },
            ]),
         ).toContain("- at the top (line 0, character 0)");
      });

      it("defaults character to 0 when only the line is known", () => {
         expect(
            formatDiagnosticsText([
               { severity: "error", message: "line only", line: 7 },
            ]),
         ).toContain("- line only (line 7, character 0)");
      });

      it("falls back to every diagnostic when none has error severity", () => {
         // Defensive: status is "error" only when an error-severity diagnostic
         // exists, so this should be unreachable. It must not render an empty
         // list if that ever changes.
         expect(
            formatDiagnosticsText([
               { severity: "warn", message: "only a warn" },
            ]),
         ).toBe(
            "Compile failed with 1 error (positions are 0-based):\n\n" +
               "- only a warn",
         );
      });
   });

   it("forwards givens to compileSource", async () => {
      let captured: unknown;
      const handler = captureHandler({
         getEnvironment: async () =>
            ({
               compileSource: async (
                  _pkg: string,
                  _model: string,
                  _source: string,
                  _includeSql: boolean,
                  givens: unknown,
               ) => {
                  captured = givens;
                  return { problems: [] };
               },
            }) as never,
      });
      await handler({ ...args, givens: { region: "west" } });
      expect(captured).toEqual({ region: "west" });
   });
});
