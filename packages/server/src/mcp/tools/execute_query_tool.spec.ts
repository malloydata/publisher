// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { MalloyError } from "@malloydata/malloy";
import { registerExecuteQueryTool } from "./execute_query_tool";
import type { EnvironmentStore } from "../../service/environment_store";
import type { ModelQueryMetadataInput } from "../../service/model";
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
   content: Array<{
      type?: string;
      text?: string;
      resource?: { text: string };
   }>;
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

function parse(result: { content: Array<{ resource?: { text: string } }> }) {
   return JSON.parse(result.content[0].resource!.text);
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
               // The tool reads the package's own declared bag as the
               // least-specific author layer.
               getDeclaredQueryMetadata: () => null,
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

/**
 * A store whose query SUCCEEDS, capturing the per-query metadata input the tool
 * built. `connections` becomes the environment's connection config, so a test
 * can pin what the connection layers resolved to.
 */
function storeCapturingMetadata(
   connections: Record<
      string,
      {
         queryMetadata?: Record<string, string>;
         queryMetadataEnforced?: Record<string, string>;
      }
   > = {},
): {
   store: Partial<EnvironmentStore>;
   captured: () => ModelQueryMetadataInput | undefined;
   capturedShape: () => string | undefined;
} {
   let captured: ModelQueryMetadataInput | undefined;
   let capturedShape: string | undefined;
   const store: Partial<EnvironmentStore> = {
      getEnvironment: async () =>
         ({
            assertCanAdmitQuery: () => undefined,
            getApiConnection: (name: string) => {
               const connection = connections[name];
               if (!connection) throw new Error(`no connection ${name}`);
               return connection;
            },
            getPackage: async () => ({
               // The tool reads the package's own declared bag as the
               // least-specific author layer.
               getDeclaredQueryMetadata: () => null,
               getModel: () => ({
                  getModelType: () => "model",
                  getModel: async () => ({}),
                  getQueryResults: async (..._args: unknown[]) => {
                     // Positional: sourceName, queryName, query, filterParams,
                     // bypassFilters, givens, abortSignal, metadata input,
                     // responseShape. Indexed rather than destructured off the
                     // end, because the argument list has grown twice and a
                     // trailing-element type went stale both times.
                     captured = _args[7] as ModelQueryMetadataInput | undefined;
                     capturedShape = _args[8] as string | undefined;
                     return {
                        result: {
                           schema: { fields: [] },
                           connection_name: "warehouse",
                        },
                        compactResult: [{ c: 1 }],
                        rowLimit: 1000,
                        rowLimitSource: "server_default",
                        queryCorrelationId: "corr-1",
                     };
                  },
               }),
            }),
         }) as never,
   };
   return {
      store,
      captured: () => captured,
      capturedShape: () => capturedShape,
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

   it("returns only the restricted cause, with the model-file loop, on a restricted rejection", async () => {
      // One forbidden construct cascades into not-defined noise; the payload
      // must carry the cause alone and route the caller to a model file, not
      // to the generic syntax advice (QA field notes F5).
      const restricted = new MalloyError(
         "`duckdb.sql(...)` cannot be used in a restricted query\n'x' is not defined",
         [],
      );
      (restricted as MalloyError & { problems: unknown }).problems = [
         {
            severity: "error",
            code: "restricted-construct-forbidden",
            message: "`duckdb.sql(...)` cannot be used in a restricted query",
         },
         {
            severity: "error",
            code: "not-found",
            message: "'x' is not defined",
         },
      ];
      const handler = captureHandler(storeWhoseQueryThrows(restricted));
      const parsed = parse(await handler(args));
      expect(parsed.error).toContain("cannot be used in a restricted query");
      expect(parsed.error).not.toContain("'x' is not defined");
      expect(JSON.stringify(parsed.suggestions)).toContain("model file");
      expect(JSON.stringify(parsed.suggestions)).toContain(
         "malloy_reloadPackage",
      );
      expect(JSON.stringify(parsed.suggestions)).not.toContain(
         "Verify the structure and syntax",
      );
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

   it("also states the error in a text block", async () => {
      // The structured payload rides in an embedded resource block. A client
      // that renders only text blocks on an isError result shows nothing at
      // all for it, which is how a real diagnostic surfaces to the agent as a
      // bare "Unknown error". Every error must say it in plain text too.
      const handler = captureHandler(
         storeWhoseQueryThrows(new MalloyError("unexpected '@'", [])),
      );
      const result = await handler(args);
      const parsed = parse(result);

      const textBlock = result.content.find((b) => b.type === "text");
      expect(textBlock).toBeDefined();
      expect(textBlock!.text).toContain(parsed.error);
   });
});

describe("malloy_executeQuery per-query metadata", () => {
   it("resolves the connection's enforced layer, which an agent must not be able to shed", async () => {
      // The reason this path matters: `queryMetadataEnforced` is the property a
      // host is billed or audited by, and MCP is this server's primary agent
      // interface. A tool that omits the metadata input issues every one of its
      // statements without the tenant label, on a connection whose config says
      // it is applied to everything the connection sends.
      const { store, captured } = storeCapturingMetadata({
         warehouse: {
            queryMetadata: { team: "finance" },
            queryMetadataEnforced: { tenant: "acme" },
         },
      });
      const handler = captureHandler(store);
      await handler(args);

      const layers = captured()?.connectionMetadata?.("warehouse");
      expect(layers).toEqual({
         default: { team: "finance" },
         enforced: { tenant: "acme" },
      });
   });

   it("passes the environment and mints a correlation id", async () => {
      // Both arrive through the input object; neither is derivable inside Model.
      const { store, captured } = storeCapturingMetadata();
      const handler = captureHandler(store);
      await handler(args);

      expect(captured()?.environment).toBe("env");
      expect(captured()?.correlationId).toMatch(/^[0-9a-f-]{36}$/);
   });

   it("asks the model for the compact shape, which its envelope is built from", async () => {
      // Left at the default, the byte cap measured the full wrapped result and the
      // string was discarded, so a query could be refused on bytes the agent would
      // never receive: this envelope is built from the compact rows and truncated
      // to MAX_RESULT_CHARS regardless. Reverting the argument breaks nothing else,
      // so this is the only thing holding it.
      const { store, capturedShape } = storeCapturingMetadata();
      const handler = captureHandler(store);
      await handler(args);
      expect(capturedShape()).toBe("compact");
   });

   it("asks for the compact shape on the named-view path too", async () => {
      const { store, capturedShape } = storeCapturingMetadata();
      const handler = captureHandler(store);
      await handler({
         ...args,
         query: undefined,
         sourceName: "orders",
         queryName: "summary",
      });
      expect(capturedShape()).toBe("compact");
   });

   it("returns the id the statements carried, so an agent can find its query", async () => {
      const { store } = storeCapturingMetadata();
      const handler = captureHandler(store);
      const parsed = parse(await handler(args));
      expect(parsed._query_id).toBe("corr-1");
   });

   it("fails open when the connection config cannot be read", async () => {
      // A tag must never be the reason a query fails, so an unresolvable
      // connection costs the layers rather than the statement.
      const { store, captured } = storeCapturingMetadata();
      const handler = captureHandler(store);
      await handler(args);
      expect(captured()?.connectionMetadata?.("missing")).toBeNull();
   });

   it("builds the same input for a named view as for ad-hoc Malloy", async () => {
      // Two call sites, one input: the enforced layer cannot depend on which
      // shape of query the agent happened to send.
      const { store, captured } = storeCapturingMetadata({
         warehouse: { queryMetadataEnforced: { tenant: "acme" } },
      });
      const handler = captureHandler(store);
      await handler({
         environmentName: "env",
         packageName: "pkg",
         modelPath: "m.malloy",
         sourceName: "orders",
         queryName: "by_month",
      });

      expect(captured()?.environment).toBe("env");
      expect(captured()?.connectionMetadata?.("warehouse")).toEqual({
         default: undefined,
         enforced: { tenant: "acme" },
      });
   });
});
