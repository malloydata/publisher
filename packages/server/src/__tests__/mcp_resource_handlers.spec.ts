// @ts-ignore - bun:test types
import { test, expect, describe, beforeAll, afterAll, it } from "bun:test";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ErrorCode, Request, Notification, Result } from "@modelcontextprotocol/sdk/types.js";

// --- Import E2E Test Setup ---
import { McpE2ETestEnvironment, setupE2ETestEnvironment, cleanupE2ETestEnvironment } from "./mcp_test_setup";

// --- Test Suite ---
describe("MCP Resource Handler Tests (E2E Integration)", () => {
  let env: McpE2ETestEnvironment | null = null;
  let mcpClient: Client<Request, Notification, Result>; // Convenience variable

  // Setup/Teardown using E2E Scaffolding
  beforeAll(async () => {
    env = await setupE2ETestEnvironment();
    mcpClient = env.mcpClient;
  });

  afterAll(async () => {
    await cleanupE2ETestEnvironment(env);
    env = null;
  });

  // Define constants used in tests
  const homeUri = "malloy://project/home";
  const nonExistentProjectUri = "malloy://project/non_existent";
  const validModelUri = "malloy://project/home/package/faa/flights.malloy"; // Assuming faa/flights.malloy exists
  const nonExistentModelUri = "malloy://project/home/package/faa/non_existent.malloy";
  const invalidUri = "invalid://uri";

  it('should list resources for a valid project', async () => {
    if (!env) throw new Error("Test environment not initialized");
    // Note: The list implementation for project URI might return packages
    const result = await mcpClient.listResources({ uri: homeUri });
    // Check for successful response structure
    expect(result).toBeDefined();
    expect(result.resources).toBeDefined();
    expect(Array.isArray(result.resources)).toBe(true);
    // It should contain at least the 'faa' package if listing packages
    expect(result.resources).toEqual(expect.arrayContaining([
      expect.objectContaining({ name: 'faa' })
    ]));
  });

  it('should reject for non-existent project', async () => {
     if (!env) throw new Error("Test environment not initialized");
    // Expect REJECTION because the project itself doesn't exist in the template definition
    await expect(mcpClient.listResources({ uri: nonExistentProjectUri }))
      .rejects.toMatchObject({
        code: ErrorCode.InvalidParams, // Or potentially MethodNotFound if no template matches
        message: expect.stringContaining("not found") // General not found message
      });
  });

  it('should read resource details for a valid model', async () => {
    if (!env) throw new Error("Test environment not initialized");
    const result: any = await mcpClient.readResource({ uri: validModelUri });
    expect(result).toBeDefined();
    // Check for success based on content
    expect(result.contents).toBeDefined();
    expect(Array.isArray(result.contents)).toBe(true);
    expect(result.contents.length).toBeGreaterThan(0);
    expect(result.contents[0].type).toBe('text');
    // Check content based on actual server implementation (might be model details or just name)
    expect(result.contents[0].text).toMatch(/Model:.*flights.malloy/i); 
  });

  it('should reject for non-existent resource URI', async () => {
    if (!env) throw new Error("Test environment not initialized");
     // Expect REJECTION for a non-existent model URI
    await expect(mcpClient.readResource({ uri: nonExistentModelUri }))
      .rejects.toMatchObject({
          code: ErrorCode.InvalidParams, // Expect InvalidParams from MCP framework
          message: expect.stringContaining("not found") // Check for message indicating resource not found
      });
  });

  it('should reject for invalid URI format', async () => {
    if (!env) throw new Error("Test environment not initialized");
    // Expect REJECTION for invalid URI format
    await expect(mcpClient.readResource({ uri: invalidUri }))
       .rejects.toMatchObject({
          code: ErrorCode.InvalidParams,
          message: expect.stringContaining("Invalid URI format")
       });
  });
}); 