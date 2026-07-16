import { describe, expect, it } from "bun:test";
import { registerCompileTool } from "./compile_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import { PackageNotFoundError, ServiceUnavailableError } from "../../errors";

// Capture the handler registerCompileTool passes to McpServer.tool, so it can
// be exercised against a mocked EnvironmentStore. The tool builds a
// CompileController internally, which calls environment.compileSource, so the
// mock only needs getEnvironment -> { compileSource }.
type Handler = (params: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource: { text: string } }>;
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

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
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

describe("malloy_compile tool", () => {
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
