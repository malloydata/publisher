# MCP SSE Endpoint Implementation Plan

## Goal

Add a Model Context Protocol (MCP) endpoint to the Malloy Publisher server (`packages/server`) adhering to the MCP specification and using the official TypeScript SDK. This endpoint will allow MCP clients (like AI agents) to:

1.  Discover Malloy resources (projects, packages, models) via MCP Resource requests.
2.  Execute existing and ad-hoc Malloy queries via an MCP Tool.
3.  Receive query results in JSON format.

The implementation should reuse existing logic from the REST API where possible and use the same authentication mechanisms. The endpoint must provide verbose documentation and user-friendly errors suitable for AI agent consumption.

## Development Approach

*   **Test-Driven Development (TDD):** Functional/integration tests will be written *before* implementing corresponding functionality.
*   **Testing Framework:** The project uses Bun's built-in test runner (`bun test`) with its Jest-compatible API (`bun:test` module for `describe`, `it`, `expect`). `bun-types` has been added to `devDependencies` for TypeScript support.
*   **HTTP Testing:** `supertest` is available for integration testing HTTP endpoints.
*   **Existing Test Suite (Submodule):** A test suite exists within the `malloy-samples` submodule (`packages/server/malloy-samples/tests`) using the `*.spec.ts` convention. These tests require specific environment setup (database connections) and are **expected to fail** in the default server development environment. **These submodule tests will be ignored.**
*   **Our Test Strategy:**
    *   New tests for the MCP endpoint will be located in `packages/server/src/__tests__/`.
    *   We will use the `*.spec.ts` naming convention (e.g., `mcp_server_init.spec.ts`, `mcp_transport.integration.spec.ts`).
    *   To execute only our tests and avoid noise from the submodule tests, **tests must be run by explicitly targeting the directory or file on the command line:**
        *   `bun test ./src/__tests__/` (to run all MCP tests)
        *   `bun test ./src/__tests__/your_test_file.spec.ts` (to run a specific file)
*   **Code Quality:** Adhere to existing ESLint and Prettier configurations.

## MCP Overview

*   **Standard:** Model Context Protocol ([https://modelcontextprotocol.io/](https://modelcontextprotocol.io/))
*   **SDK:** `@modelcontextprotocol/sdk` (TypeScript)
*   **Communication:** JSON-RPC 2.0
*   **Transport:** HTTP POST (Client -> Server requests) & Server-Sent Events (SSE) (Server -> Client messages/responses)

## Initial Analysis (Completed)

1.  **Project Structure:** Monorepo (`packages/sdk`, `packages/server`, `packages/app`) using `bun`. Server logic in `packages/server`.
2.  **Existing API:** Comprehensive REST API (`api-doc.yaml`) for resource discovery and query execution.

## Refined Implementation Plan

1.  **Setup & Dependencies:** (Completed)
    *   Added `@modelcontextprotocol/sdk`.
    *   Confirmed framework is **Express**.
    *   Confirmed **no existing authentication**.
    *   Added `bun-types` for test definitions.

2.  **MCP Server Initialization:** (Implementation Complete)
    *   Created `packages/server/src/mcp/`.
    *   Implemented `initializeMcpServer` in `src/mcp/server.ts`.
    *   Integrated initialization into `src/server.ts` startup.

**[TDD Step]** 2a. **Write Initialization Tests:** (Completed)
    *   Created `packages/server/src/__tests__/mcp_server_init.spec.ts`.
    *   Verified test runner setup and configuration.
    *   Added test confirming `initializeMcpServer` returns a `Server` instance.
    *   Tests passed using `bun test src/__tests__/mcp_server_init.spec.ts`.

**[TDD Step]** 3a. **Write Transport Layer Tests:**
    *   Create `packages/server/src/__tests__/mcp_transport.integration.spec.ts`.
    *   Set up `supertest` to target the Express app (may require exporting app from `server.ts`).
    *   Write tests for `POST /api/v0/mcp`:
        *   Accepts valid JSON-RPC request.
        *   Rejects non-JSON body with HTTP 400.
        *   Rejects non-JSON-RPC object with JSON-RPC Parse Error response.
        *   Returns MCP `MethodNotFound` error for unknown methods (initially).
        *   *(No auth tests initially)*
    *   Write tests for `GET /api/v0/mcp`:
        *   Returns HTTP 200.
        *   Returns correct SSE headers (`Content-Type: text/event-stream`, etc.).
        *   Keeps connection open.
        *   *(No auth tests initially)*
    *   Ensure tests adhere to linting/formatting standards.

3b. **HTTP/SSE Transport Implementation:**
    *   Implement MCP transport adapter within Express.
    *   Implement `POST /api/v0/mcp` route handler.
    *   Implement `GET /api/v0/mcp` route handler for SSE.
    *   Connect the `mcpServer` instance to this transport.
    *   Run tests from Step 3a to verify implementation.

**[TDD Step]** 4a. **Write Resource Handler Tests:**
    *   Add tests to `mcp.integration.test.ts` for resource discovery:
        *   Test `mcp/ListResources` request returns expected list of projects/packages/models.
        *   Test `mcp/GetResource` request for a valid resource URI returns correct details.
        *   Test `mcp/GetResource` for an invalid URI returns appropriate MCP error.

4b. **Expose Malloy Resources via MCP:**
    *   Define MCP resource schemas (Project, Package, Model) using URIs like `malloy://project/{projectName}`, `malloy://package/{projectName}/{packageName}`, etc.
    *   Implement MCP request handlers (`mcp/ListResources`, `mcp/GetResource`) reusing logic from REST API handlers (`list-projects`, `list-packages`, `get-model`, etc.).
    *   Format fetched data as MCP resources.
    *   **Resource descriptions within MCP must be verbose, detailing the structure of the resource (fields, types) and providing example MCP URIs.**
    *   Run tests written in Step 4a.

**[TDD Step]** 5a. **Write Tool Handler Tests:**
    *   Add tests to `mcp.integration.test.ts` for the query tool:
        *   Test `malloy/executeQuery` tool call with valid parameters returns expected JSON `QueryResult`.
        *   Test tool call with invalid parameters returns appropriate MCP error.
        *   Test tool call leading to Malloy error returns correctly formatted, user-friendly MCP error (as per Step 6).

5b. **Expose Malloy Query Execution as MCP Tool:**
    *   Define MCP tool schema `malloy/executeQuery`.
    *   Parameters: `projectName`, `packageName`, `modelPath`, `query` (optional), `sourceName` (optional), `queryName` (optional).
    *   Implement the tool handler reusing logic from the REST `execute-query` endpoint.
    *   Return JSON `QueryResult` according to the tool schema.
    *   **The `malloy/executeQuery` tool description within MCP must be verbose, including:**
        *   **Clear documentation of all parameters (name, type, description, required/optional).**
        *   **Detailed structure of the expected JSON `QueryResult`.**
        *   **Example MCP tool call demonstrating usage with both ad-hoc and named queries.**
    *   *(Optional Consideration)* Implement MCP progress reporting (`$/progress` notifications) if queries can be long-running.
    *   Run tests written in Step 5a.

6.  **Error Handling Refinement:**
    *   **Identify common error types thrown by existing services (e.g., `PackageNotFoundError`, `MalloyCompilationError`, `QueryExecutionError`) and plan their specific mapping to user-friendly MCP error codes and messages.**
    *   **Distinguish between Protocol and Application Errors:**
        *   **Protocol Errors:** These occur when the client sends an invalid request according to the MCP specification or the tool's parameter schema *before* the main application logic runs. Examples include invalid JSON-RPC format, missing required parameters, or providing mutually exclusive parameters (like both `query` and `queryName` for `malloy/executeQuery`).
            *   **Client Handling:** The MCP client's promise (e.g., from `client.callTool()`) will be **rejected**. The rejection error will typically be a `JSONRPCError` object containing an appropriate MCP `ErrorCode` (often `InvalidParams`) and a descriptive message.
        *   **Application Errors:** These occur *during* the execution of the tool's requested operation, after the initial parameters have been validated. Examples include the requested package or model not being found, errors during query compilation or execution against the database, or internal server issues.
            *   **Client Handling:** The MCP client's promise will be **resolved successfully**. However, the resolved `CallToolResult` object will indicate the error internally: `{ isError: true, content: [{ type: 'text', text: 'Specific application error message...' }] }`. The client must check the `isError` flag in the resolved object to detect application-level failures.
    *   **Implement the mapping of identified API/Malloy errors to MCP error responses. These errors must:**
        *   **Use clear, plain English descriptions of the problem.**
        *   **Avoid technical details like stack traces.**
        *   **Provide actionable suggestions or recommendations for how the AI agent might fix the issue (e.g., 'Check if the model path is correct', 'Ensure the query syntax is valid Malloy', 'Verify the sourceName exists in the specified model').**
    *   Enhance tests in Step 5a to cover specific error mapping scenarios.

7.  **Testing:**
    *   Review overall test coverage in `src/__tests__/`.
    *   Add any missing unit/integration tests.

8.  **Documentation:**
    *   Update this document (`specs.md`).
    *   Briefly document the MCP endpoint in `README.md` or `api-doc.yaml`, highlighting its purpose for AI interaction.

## Decisions & Considerations

*   Use official MCP standard and TypeScript SDK.
*   Implement HTTP POST / SSE transport using **Express**.
*   **Use a separate Express app instance for MCP endpoints to avoid interference from global middleware.** This is critical because the MCP SDK requires raw access to the request/response objects without interference from middleware like logging or body parsing that might consume the request stream.
*   Focus on JSON output initially.
*   Reuse existing REST API logic.
*   **Initial MCP endpoint will have no authentication, matching the current REST API. Authentication should be addressed holistically later if needed.**
*   Prioritize verbose, AI-friendly documentation and error messages within the MCP layer.
*   *(Optional Consideration)* Add an environment variable (e.g., `MCP_ENDPOINT_ENABLED`) to optionally enable/disable the MCP endpoint.
*   *(Optional Consideration)* Implement progress reporting for long-running queries.

## Implementation Challenges and Solutions

During implementation, we encountered the following challenges and solutions:

1. **Middleware Interference:** 
   - **Problem:** Global Express middleware (particularly morgan logging middleware) was interfering with the SSE connection and request handling required by the MCP SDK. 
   - **Impact:** This caused connection issues where the client couldn't properly establish SSE connections or the server couldn't parse incoming JSON-RPC requests.
   - **Solution:** Create a separate Express app instance dedicated to MCP endpoints that doesn't share middleware with the main API. This isolated app is then mounted at the `/api/v0/mcp` path.

2. **Transport Configuration:**
   - **Approach:** Let the MCP SDK handle all transport configuration, including setting headers and parsing requests. 
   - **Implementation:** Provide the SDK with raw request and response objects without prior processing by middleware.

## Next Steps

1.  Implement the separate Express app approach for MCP endpoints.
2.  Proceed with **[TDD Step] 3a: Write Transport Layer Tests** after the change. 