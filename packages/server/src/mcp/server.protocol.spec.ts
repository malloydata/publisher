import { beforeAll, describe, expect, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { initializeMcpServer } from "./server";
import { RELOAD_FAILURE_IS_SAFE } from "./tools/reload_package_tool";
import type { EnvironmentStore } from "../service/environment_store";

/**
 * End-to-end coverage of the unified MCP server over the real MCP protocol,
 * using the SDK's in-memory transport (no HTTP, no network, no DuckDB).
 * Exercises tool registration (including the agent retrieval tools that now
 * live on the same server), the dual-channel skill prompts, a real tool call
 * (searchDocs over the bundled index), and the getContext error path.
 */
describe("MCP server over the MCP protocol (in-memory)", () => {
   let client: Client;

   beforeAll(async () => {
      // searchDocs does not touch the store; getContext's error path only needs
      // getEnvironment to reject, so a throwing stub is sufficient here.
      const stubStore = {
         getEnvironment: async (name: string): Promise<never> => {
            throw new Error(`Environment not found: ${name}`);
         },
      } as unknown as EnvironmentStore;

      const server = initializeMcpServer(stubStore);
      const [clientTransport, serverTransport] =
         InMemoryTransport.createLinkedPair();
      await server.connect(serverTransport);
      client = new Client({ name: "mcp-protocol-test", version: "0.0.0" });
      await client.connect(clientTransport);
   });

   it("exposes the agent retrieval tools alongside the core tools", async () => {
      const { tools } = await client.listTools();
      const names = new Set(tools.map((t) => t.name));
      expect(names.has("malloy_getContext")).toBe(true);
      expect(names.has("malloy_searchDocs")).toBe(true);
      expect(names.has("malloy_executeQuery")).toBe(true);
      expect(names.has("malloy_compile")).toBe(true);
      expect(names.has("malloy_reloadPackage")).toBe(true);
   });

   /**
    * Tool descriptions are truncated by some clients. `malloy_getContext`'s was
    * observed arriving cut off mid-sentence at 2271 characters, and what a tail
    * cut removes is whatever the description put last.
    *
    * Two defenses, and the ordering one matters more. No single number is
    * portable, since the cap belongs to the client and is not published; this
    * budget only stops silent regrowth past the one length we have watched fail.
    * The real rule is that contract rules come BEFORE the reference material, so
    * a cut costs an agent the worked example rather than the invariants it
    * cannot self-correct without. See docs/agent-skills/tool-description-template.md.
    */
   it("keeps every tool description under the truncation budget", async () => {
      const BUDGET = 2000;
      const { tools } = await client.listTools();
      const oversized = tools
         .filter((t) => (t.description ?? "").length > BUDGET)
         .map((t) => `${t.name} (${t.description?.length})`);
      expect(oversized).toEqual([]);
   });

   it("puts contract rules ahead of the reference sections", async () => {
      // The section a tail-truncating client drops must never be the invariants.
      const { tools } = await client.listTools();
      for (const tool of tools) {
         const description = tool.description ?? "";
         const contract = description.indexOf("## Contract rules");
         if (contract === -1) continue;
         for (const later of ["## Response", "## Worked example"]) {
            const index = description.indexOf(later);
            if (index !== -1) {
               expect(contract).toBeLessThan(index);
            }
         }
      }
   });

   it("exposes the skill set as dual-channel prompts", async () => {
      const { prompts } = await client.listPrompts();
      expect(prompts.length).toBeGreaterThanOrEqual(24);
      expect(prompts.some((p) => p.name === "malloy-analysis")).toBe(true);
   });

   it("delivers orientation instructions to the connecting client", () => {
      const instructions = client.getInstructions();
      expect(instructions).toBeDefined();
      expect(instructions).toContain("malloy_getContext");
      // Pin the shared fragment, not just that instructions exist. The whole
      // point of exporting it is that this surface and the tool description
      // cannot drift, and they have drifted twice. Without this, deleting the
      // interpolation leaves the suite green and the drift comes back.
      expect(instructions).toContain(RELOAD_FAILURE_IS_SAFE);
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

   /**
    * Assert the tool's OWN error payload, not just isError. The SDK sets
    * isError for any uncaught throw, so `expect(res.isError).toBe(true)` passes
    * even with the tool's catch deleted entirely. Only these tools emit a
    * resource block carrying structured JSON; a raw throw yields the SDK's text
    * content instead, so this shape is what actually pins the catch.
    */
   function expectToolErrorPayload(result: unknown): {
      error: string;
      suggestions: string[];
   } {
      const res = result as {
         isError?: boolean;
         content?: Array<{ type?: string; resource?: { text?: string } }>;
      };
      expect(res.isError).toBe(true);
      expect(res.content?.[0]?.type).toBe("resource");
      const payload = JSON.parse(res.content?.[0]?.resource?.text ?? "{}");
      expect(typeof payload.error).toBe("string");
      expect(Array.isArray(payload.suggestions)).toBe(true);
      return payload;
   }

   it("malloy_compile returns its own error payload over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_compile",
         arguments: {
            environmentName: "nope",
            packageName: "nope",
            modelPath: "x.malloy",
            source: "run: x -> { aggregate: c is count() }",
         },
      });
      expect(expectToolErrorPayload(res).error).toContain("nope");
   });

   it("malloy_reloadPackage returns its own error payload over the protocol", async () => {
      const res = await client.callTool({
         name: "malloy_reloadPackage",
         arguments: { environmentName: "nope", packageName: "nope" },
      });
      expect(expectToolErrorPayload(res).error).toContain("nope");
   });

   it("a skill prompt returns its body", async () => {
      const res = await client.getPrompt({ name: "malloy-analysis" });
      const content = res.messages[0].content as { text?: string };
      expect((content.text ?? "").length).toBeGreaterThan(200);
   });
});
