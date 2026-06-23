import { beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { initializeAgentMcpServer } from "./agent_server";
import type { EnvironmentStore } from "../service/environment_store";

/**
 * End-to-end coverage of the agent MCP server over the real MCP protocol, using
 * the SDK's in-memory transport (no HTTP, no network, no DuckDB). Exercises tool
 * registration, the dual-channel prompts, a real tool call (searchDocs over the
 * bundled index), and the getContext error path.
 */
describe("agent MCP server over the MCP protocol (in-memory)", () => {
   let client: Client;

   beforeAll(async () => {
      // searchDocs does not touch the store; getContext's error path only needs
      // getEnvironment to reject, so a throwing stub is sufficient here.
      const stubStore = {
         getEnvironment: async (name: string): Promise<never> => {
            throw new Error(`Environment not found: ${name}`);
         },
      } as unknown as EnvironmentStore;

      const server = initializeAgentMcpServer(stubStore);
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "agent-protocol-test", version: "0.0.0" });
      await client.connect(clientTransport);
   });

   it("exposes exactly the two agent tools", async () => {
      const { tools } = await client.listTools();
      expect(new Set(tools.map((t) => t.name))).toEqual(
         new Set(["malloy_getContext", "malloy_searchDocs"]),
      );
   });

   it("exposes the skill set as dual-channel prompts", async () => {
      const { prompts } = await client.listPrompts();
      expect(prompts.length).toBeGreaterThanOrEqual(24);
      expect(prompts.some((p) => p.name === "malloy-analysis")).toBe(true);
   });

   it("malloy_searchDocs returns relevant docs over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_searchDocs",
         arguments: { query: "window functions" },
      });
      const content = res.content as Array<{ resource?: { text?: string } }>;
      const results = JSON.parse(
         content?.[0]?.resource?.text ?? "[]",
      ) as Array<{
         url: string;
      }>;
      expect(results.length).toBeGreaterThan(0);
      expect(results[0].url.length).toBeGreaterThan(0);
   });

   it("malloy_getContext fails gracefully (isError) for an unknown environment", async () => {
      const res = await client.callTool({
         name: "malloy_getContext",
         arguments: {
            environmentName: "nope",
            packageName: "nope",
            query: "x",
         },
      });
      expect(res.isError).toBe(true);
   });

   it("a skill prompt returns its body", async () => {
      const res = await client.getPrompt({ name: "malloy-analysis" });
      const content = res.messages[0].content as { text?: string };
      expect((content.text ?? "").length).toBeGreaterThan(200);
   });
});
