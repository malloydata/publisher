// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { MalloyError } from "@malloydata/malloy";
import { registerExecuteQueryTool } from "./execute_query_tool";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../../package_load/package_load_pool";
import type { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import type { ModelQueryMetadataInput } from "../../service/model";
import {
   EnvironmentNotFoundError,
   NotQueryableError,
   OffSurfaceError,
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
   capturedArgs: () => unknown[];
} {
   let captured: ModelQueryMetadataInput | undefined;
   let capturedShape: string | undefined;
   let capturedArgs: unknown[] = [];
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
                     capturedArgs = _args;
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
      capturedArgs: () => capturedArgs,
   };
}

const args = {
   environmentName: "env",
   packageName: "pkg",
   modelPath: "m.malloy",
   query: "run: a -> { aggregate: c is count() }",
};

describe("execute_query error classification", () => {
   it("names an unknown environment and the ones that exist", async () => {
      // Goes through getModelForQuery's own catch, not the tool's, so it is
      // pinned separately from the other tools.
      const handler = captureHandler({
         getEnvironment: async () => {
            throw new EnvironmentNotFoundError(
               'Environment "analytics" could not be resolved to a path.',
               {
                  environmentName: "analytics",
                  availableEnvironments: ["default"],
               },
            );
         },
      });
      const parsed = parse(
         await handler({ ...args, environmentName: "analytics" }),
      );
      expect(parsed.error).toBe(
         "Environment 'analytics' not found. Available environments: default. Use a name from list_packages.",
      );
   });

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
      expect(JSON.stringify(parsed.suggestions)).toContain("reload_package");
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
      expect(JSON.stringify(parsed.suggestions)).toContain("reload_package");
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

   it("passes an off-surface refusal through with its reason, not as a typo", async () => {
      // OffSurfaceError is only built where nothing is gated, and its message
      // is the fix. Collapsing it to "Resource not found ... check the
      // spelling" sends an agent hunting a typo for a name that is real.
      const message =
         'No queryable model "users.malloy". It is not on this package\'s published surface, "index.malloy".';
      const handler = captureHandler(
         storeWhoseQueryThrows(new OffSurfaceError(message)),
      );
      const parsed = parse(await handler(args));
      expect(parsed.error).toBe(message);
      expect(JSON.stringify(parsed.suggestions)).toContain("not a typo");
      expect(JSON.stringify(parsed.suggestions)).not.toContain(
         "spelled correctly",
      );
      // The author's way to test it without publishing it.
      expect(JSON.stringify(parsed.suggestions)).toContain(
         "includeHiddenFilesAndSources: true",
      );
   });

   it("does not offer includeHiddenFilesAndSources on a plain not-found", async () => {
      // A gated hidden target answers the generic refusal so it reads like a
      // missing one. Offering the option there would hint that it exists.
      const handler = captureHandler(
         storeWhoseQueryThrows(
            new NotQueryableError('No queryable source "salaries".'),
         ),
      );
      const parsed = parse(await handler(args));
      expect(JSON.stringify(parsed.suggestions)).not.toContain(
         "includeHiddenFilesAndSources",
      );
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

describe("execute_query per-query metadata", () => {
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

describe("execute_query includeHiddenFilesAndSources", () => {
   // getQueryResults positions: 9 is bypassAuthorize, 10 is
   // includeHiddenFilesAndSources.
   it("passes true through on both call paths, and never a bypass", async () => {
      for (const call of [
         args,
         { ...args, query: undefined, sourceName: "orders", queryName: "v" },
      ]) {
         const { store, capturedArgs } = storeCapturingMetadata();
         await captureHandler(store)({
            ...call,
            includeHiddenFilesAndSources: true,
         });
         expect(capturedArgs()[9]).toBe(false);
         expect(capturedArgs()[10]).toBe(true);
      }
   });

   it("sends false when the argument is omitted or false", async () => {
      for (const extra of [{}, { includeHiddenFilesAndSources: false }]) {
         const { store, capturedArgs } = storeCapturingMetadata();
         await captureHandler(store)({ ...args, ...extra });
         expect(capturedArgs()[9]).toBe(false);
         expect(capturedArgs()[10]).toBe(false);
      }
   });
});

/**
 * The same cases as explore_visibility.spec.ts pins for the model, through the
 * real tool handler and a real loaded Package with a root index.malloy.
 */
describe("execute_query includeHiddenFilesAndSources on a real package", () => {
   const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;
   let tempDir: string;
   let duckdb: { close: () => Promise<void> };
   let handler: Handler;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      await __setPackageLoadPoolForTests(new PackageLoadPool(1));
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-mcp-hidden-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "locked.malloy"),
         `#(authorize) false
source: locked is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: helper is duckdb.sql("select 1 as id")
source: customers is duckdb.sql("select 1 as id")
export { customers }`,
      );

      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const connection = new DuckDBConnection("duckdb", ":memory:");
      duckdb = connection;
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () =>
            new FixedConnectionMap(new Map([["duckdb", connection]]), "duckdb"),
      );
      const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
      handler = captureHandler({
         getEnvironment: async () =>
            ({
               assertCanAdmitQuery: () => undefined,
               getApiConnection: () => {
                  throw new Error("no connection config in this test");
               },
               getPackage: async () => pkg,
            }) as never,
      });
   });

   afterAll(async () => {
      await duckdb?.close();
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const run = (
      modelPath: string,
      query: string,
      includeHiddenFilesAndSources?: boolean,
   ) =>
      handler({
         environmentName: "env",
         packageName: "pkg",
         modelPath,
         query,
         includeHiddenFilesAndSources,
      });

   it("refuses a hidden file and a hidden source without it, and runs both with it", async () => {
      for (const [modelPath, query] of [
         ["base.malloy", "run: base_source -> { select: * }"],
         ["index.malloy", "run: helper -> { select: * }"],
      ]) {
         const refused = await run(modelPath, query);
         expect(refused.isError).toBe(true);
         expect(parse(refused).error).toContain("published surface");

         const ran = await run(modelPath, query, true);
         expect(ran.isError).not.toBe(true);
         expect(parse(ran).rows).toEqual([{ id: 1 }]);
      }
   });

   it("still refuses an #(authorize) false source with it", async () => {
      const result = await run(
         "locked.malloy",
         "run: locked -> { select: * }",
         true,
      );
      expect(result.isError).toBe(true);
      expect(parse(result).error).toContain(
         'Access denied for source "locked"',
      );
   });
});
