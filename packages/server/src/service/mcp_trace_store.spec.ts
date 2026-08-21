import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import { McpTraceStore } from "./mcp_trace_store";

describe("McpTraceStore", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   let store: McpTraceStore;

   beforeEach(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "mcp-trace-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await initializeSchema(db, true);
      store = new McpTraceStore(db);
      delete process.env.PUBLISHER_MCP_TRACE;
      delete process.env.PUBLISHER_MCP_TRACE_RETENTION;
   });

   afterEach(async () => {
      delete process.env.PUBLISHER_MCP_TRACE;
      delete process.env.PUBLISHER_MCP_TRACE_RETENTION;
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("persists nothing when tracing is off", async () => {
      const id = await store.record({
         toolName: "malloy_getContext",
         request: { query: "secret" },
         response: { results: [] },
      });
      expect(id).toBeUndefined();
      const rows = await db.all(`SELECT * FROM mcp_traces`);
      expect(rows).toHaveLength(0);
   });

   it("stores hashes only in metadata mode", async () => {
      process.env.PUBLISHER_MCP_TRACE = "metadata";
      const id = await store.record({
         toolName: "malloy_getContext",
         request: { query: "secret" },
         response: { results: [{ name: "orders" }] },
         resultCount: 1,
      });
      expect(id).toBeDefined();
      const row = await store.get(id!);
      expect(row?.request).toBeUndefined();
      expect(row?.response).toBeUndefined();
      expect(row?.requestHash).toBeDefined();
      expect(row?.resultCount).toBe(1);
   });

   it("stores exact payloads in retrieval mode", async () => {
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      const id = await store.record({
         toolName: "malloy_getContext",
         request: { query: "orders" },
         response: { results: [{ kind: "source", name: "orders" }] },
         rankedSummary: { entityIds: ["source:orders"], ranks: [1] },
         resultCount: 1,
      });
      const row = await store.get(id!);
      expect(row?.request).toEqual({ query: "orders" });
      expect(row?.rankedSummary).toEqual({
         entityIds: ["source:orders"],
         ranks: [1],
      });
   });

   it("evicts the oldest trace past the retention cap", async () => {
      process.env.PUBLISHER_MCP_TRACE = "retrieval";
      process.env.PUBLISHER_MCP_TRACE_RETENTION = "1";
      const first = await store.record({
         toolName: "malloy_getContext",
         request: { n: 1 },
         response: {},
      });
      const second = await store.record({
         toolName: "malloy_getContext",
         request: { n: 2 },
         response: {},
      });
      expect(await store.get(first!)).toBeNull();
      expect(await store.get(second!)).not.toBeNull();
   });
});
