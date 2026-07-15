import { describe, expect, it } from "bun:test";
import { MalloyError } from "@malloydata/malloy";
import { registerExecuteQueryTool } from "./execute_query_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import {
   NotQueryableError,
   QueryTimeoutError,
   ServiceUnavailableError,
} from "../../errors";

// Capture the handler registerExecuteQueryTool passes to McpServer.tool, so it
// can be exercised against a mocked EnvironmentStore. Mirrors the pattern in
// compile_tool.spec.ts and reload_package_tool.spec.ts.
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
   registerExecuteQueryTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Array<{ resource: { text: string } }> }) {
   return JSON.parse(result.content[0].resource.text);
}

/**
 * A store whose model loads cleanly and whose QUERY throws.
 *
 * The throw has to originate inside the tool's own try block to pin anything.
 * getModelForQuery already homes a back-pressure error from
 * `environment.assertCanAdmitQuery()`, which runs outside it, so a test that
 * trips that path passes even with the catch's classifyToolError call reverted.
 * Failing at getQueryResults puts the error where tryAcquireQuerySlot's
 * ServiceUnavailableError lands, which is the case that was reported.
 */
function storeWhoseQueryThrows(error: unknown): Partial<EnvironmentStore> {
   return {
      getEnvironment: async () =>
         ({
            assertCanAdmitQuery: () => undefined,
            getPackage: async () => ({
               getModel: () => ({
                  getModelType: () => "model",
                  getModel: async () => ({}),
                  getQueryResults: async () => {
                     throw error;
                  },
               }),
            }),
         }) as never,
   };
}

const args = {
   environmentName: "env",
   packageName: "pkg",
   modelPath: "m.malloy",
   query: "run: a -> { aggregate: c is count() }",
};

describe("malloy_executeQuery error classification", () => {
   it("tells an at-capacity caller to retry, not to check its Malloy", async () => {
      // The reported bug. tryAcquireQuerySlot runs inside the tool's try, so at
      // the concurrency cap its ServiceUnavailableError landed in a catch that
      // funnelled everything through the Malloy helper: a 503 told the agent to
      // go verify its syntax. This is the assertion that fails if the catch
      // stops routing through classifyToolError.
      const handler = captureHandler(
         storeWhoseQueryThrows(
            new ServiceUnavailableError("Memory limit reached"),
         ),
      );
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("Memory limit reached");
      expect(JSON.stringify(parsed.suggestions)).toContain("Retry");
      expect(JSON.stringify(parsed.suggestions)).not.toContain("Malloy file");
   });

   it("keeps Malloy advice for a real query error", async () => {
      // The other half: a bad query throws a raw MalloyError, so homing by
      // class must not send it to the internal branch.
      const handler = captureHandler(
         storeWhoseQueryThrows(new MalloyError("unexpected '@'", [])),
      );
      const parsed = parse(await handler(args));
      expect(parsed.error).not.toContain("unexpected internal error");
      expect(JSON.stringify(parsed.suggestions)).toContain("Malloy");
   });

   it("keeps the reload hint on an undefined name", async () => {
      const handler = captureHandler(
         storeWhoseQueryThrows(
            new MalloyError("Reference to undefined object 'orders'", []),
         ),
      );
      const parsed = parse(await handler(args));
      expect(JSON.stringify(parsed.suggestions)).toContain(
         "malloy_reloadPackage",
      );
   });

   it("does not tell a timed-out query to try again later", async () => {
      // Retrying an identical too-slow query fails the same way. The class
      // exists to be distinguishable from the retryable 503, so the one thing
      // it must not say is the internal branch's "try the request again later".
      const handler = captureHandler(
         storeWhoseQueryThrows(
            new QueryTimeoutError("Query exceeded PUBLISHER_QUERY_TIMEOUT_MS"),
         ),
      );
      const parsed = parse(await handler(args));
      expect(JSON.stringify(parsed.suggestions)).toContain("not transient");
      expect(JSON.stringify(parsed.suggestions)).not.toContain(
         "Try the request again later",
      );
   });

   it("reports a query-boundary denial as not-found, not as a server fault", async () => {
      // NotQueryableError is a deliberate 404: a hidden source should look like
      // a missing one. Reporting it as an internal error tells the caller to
      // contact support about a boundary that is working correctly.
      const handler = captureHandler(
         storeWhoseQueryThrows(
            new NotQueryableError('No queryable source "salaries".'),
         ),
      );
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("Resource not found");
      expect(parsed.error).not.toContain("unexpected internal error");
      // The class exists so a hidden target is indistinguishable from a missing
      // one; echoing the name back would undo that.
      expect(parsed.error).not.toContain("salaries");
   });
});
