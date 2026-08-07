// @ts-expect-error Bun test types are not recognized by ESLint
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";

import {
   cleanupE2ETestEnvironment,
   McpE2ETestEnvironment,
   setupE2ETestEnvironment,
} from "../../harness/mcp_test_setup";

/**
 * End-to-end coverage for malloy_searchDatabaseSchema against a real server and
 * a real DuckDB connection.
 *
 * The unit spec mocks the first two tiers because they need only an
 * EnvironmentStore. Everything below them runs real per-dialect introspection,
 * which is exactly the part worth exercising for real: it is the code this
 * feature depends on and does not own.
 */
describe.serial("malloy_searchDatabaseSchema (E2E Integration)", () => {
   let env: McpE2ETestEnvironment | null = null;
   let mcpClient: Client;

   const ENVIRONMENT_NAME = "examples";
   const PACKAGE_NAME = "storefront";
   const CONNECTION_NAME = "duckdb";

   function payloadOf(result: unknown) {
      const typed = result as {
         content: Array<{ resource?: { text?: string } }>;
      };
      const text = typed.content?.[0]?.resource?.text;
      if (!text) throw new Error("tool returned no resource payload");
      return JSON.parse(text);
   }

   beforeAll(async () => {
      env = await setupE2ETestEnvironment();
      mcpClient = env.mcpClient;
   });

   afterAll(async () => {
      await cleanupE2ETestEnvironment(env);
      env = null;
   });

   it("is advertised in the tool list", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const { tools } = await mcpClient.listTools();
      const names = tools.map((t: { name: string }) => t.name);
      expect(names).toContain("malloy_searchDatabaseSchema");
   });

   it("lists environments and their connections with no arguments", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {},
      });
      expect(result.isError).not.toBe(true);
      const payload = payloadOf(result);
      const names = payload.environments.map((e: { name: string }) => e.name);
      expect(names).toContain(ENVIRONMENT_NAME);
   });

   it("lists a connection's schemas", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
         },
      });
      expect(result.isError).not.toBe(true);
      const payload = payloadOf(result);
      expect(Array.isArray(payload.schemas)).toBe(true);
      // DuckDB qualifies schemas as "catalog.schema"; every dialect returns at
      // least one addressable schema for a working connection.
      expect(payload.schemas.length).toBeGreaterThan(0);
   });

   it("returns a well-formed table envelope for a real schema", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const schemasResult = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
         },
      });
      const schemas = payloadOf(schemasResult).schemas as {
         name: string;
         isHidden: boolean;
      }[];
      const visible = schemas.find((s) => !s.isHidden) ?? schemas[0];

      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
            schemaName: visible.name,
         },
      });
      expect(result.isError).not.toBe(true);
      const payload = payloadOf(result);
      expect(Array.isArray(payload.tables)).toBe(true);
      expect(payload.returned).toBe(payload.tables.length);
      // totalAvailable counts everything in scope, so it is >= what this page
      // returned; equal only when the whole schema fits in one page.
      expect(payload.totalAvailable).toBeGreaterThanOrEqual(payload.returned);
      // Every returned table must carry a usable source line, whatever the
      // dialect's resource shape.
      for (const table of payload.tables) {
         expect(table.malloySource).toContain(
            `${CONNECTION_NAME}.table('${table.tablePath}')`,
         );
      }
   });

   /**
    * The bundled packages address their data as files (`data/orders.parquet`),
    * so nothing is registered in the sandbox's information_schema and this
    * schema is genuinely empty. That is the case worth pinning: an agent that
    * gets a bare `[]` here concludes there is no data and stops, so the tool
    * must say which of "empty" and "not registered as tables" happened.
    *
    * Ranking quality over a populated schema is measured separately, by
    * src/mcp/tools/schema_search_eval.ts, which builds a fixture schema through
    * the REST port this MCP-only harness does not start.
    */
   it("explains an empty schema rather than returning a bare empty list", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
            schemaName: "memory.main",
         },
      });
      expect(result.isError).not.toBe(true);
      const payload = payloadOf(result);
      expect(payload.tables).toEqual([]);
      expect(payload.warnings?.join(" ")).toContain("not listed in a schema");
   });

   it("advertises the per-package duckdb sandbox so the drill-down is navigable", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: { environmentName: ENVIRONMENT_NAME },
      });
      const payload = payloadOf(result);
      const sandbox = payload.connections.find(
         (c: { name: string }) => c.name === CONNECTION_NAME,
      );
      expect(sandbox?.scope).toBe("package");
      expect(sandbox?.packages).toContain(PACKAGE_NAME);
   });

   it("ranks lexically when no embedding provider is configured", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const schemasResult = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
         },
      });
      const schemas = payloadOf(schemasResult).schemas as {
         name: string;
         isHidden: boolean;
      }[];
      const visible = schemas.find((s) => !s.isHidden) ?? schemas[0];

      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: CONNECTION_NAME,
            packageName: PACKAGE_NAME,
            schemaName: visible.name,
            searchQuery: "customer orders",
         },
      });
      expect(result.isError).not.toBe(true);
      const payload = payloadOf(result);
      // The integration environment sets no EMBEDDING_API_KEY, so the tool must
      // rank lexically rather than failing or reporting semantic.
      expect(payload.ranking).toBe("lexical");
      expect(Array.isArray(payload.tables)).toBe(true);
   });

   it("returns a structured error for an unknown connection", async () => {
      if (!env) throw new Error("Test environment not initialized");
      const result = await mcpClient.callTool({
         name: "malloy_searchDatabaseSchema",
         arguments: {
            environmentName: ENVIRONMENT_NAME,
            connectionName: "no-such-connection",
         },
      });
      expect(result.isError).toBe(true);
      const payload = payloadOf(result);
      expect(payload.tables).toEqual([]);
      expect(typeof payload.error).toBe("string");
   });
});
