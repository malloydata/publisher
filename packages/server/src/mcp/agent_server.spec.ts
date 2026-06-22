import { describe, expect, it } from "bun:test";
import { agentServerInfo, initializeAgentMcpServer } from "./agent_server";
import type { EnvironmentStore } from "../service/environment_store";

describe("agent MCP server", () => {
   it("advertises a distinct server name from the core MCP server", () => {
      expect(agentServerInfo.name).toBe("malloy-publisher-agent-mcp-server");
   });

   it("initializes and registers its tools and skill prompts without throwing", () => {
      // The store is captured for later handler calls, not used during
      // registration, so a placeholder is sufficient for this smoke test.
      const server = initializeAgentMcpServer(
         {} as unknown as EnvironmentStore,
      );
      expect(server).toBeTruthy();
   });
});
