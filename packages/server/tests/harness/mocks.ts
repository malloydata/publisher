import sinon from "sinon";
import { ProjectStore } from "../../src/service/project_store";

/** Return a stubbed ProjectStore where every lookup throws or returns minimal objects. */
export function fakeProjectStore(): sinon.SinonStubbedInstance<ProjectStore> {
   const ps = sinon.createStubInstance(ProjectStore);
   // For now just have getProject reject; suites can stub more.
   ps.getProject.rejects(new Error("fakeProjectStore: getProject not stubbed"));
   return ps;
}

// The runtime implementation of `McpServer` lives in the MCP SDK package, which
// is **not** required for unit/integration tests that only need a stubbed
// instance. Importing it at runtime would fail when the dependency is not
// present in the local workspace. Instead we define a minimal dummy class that
// satisfies Sinon while keeping the public type surface the same for tests.
//
// NOTE: If we later add the real SDK as a dependency we can simply replace this
// dummy class with:
//   import { McpServer } from "@modelcontextprotocol/sdk";
class DummyMcpServer {}

// Re-export the symbol so downstream test files can continue to refer to the
// name `McpServer` without changes.
export type McpServer = DummyMcpServer;

/** Convenience helper mimicking the old mocks used in integration specs. */
export function createMalloyServiceMocks() {
   return {
      projectStore: fakeProjectStore(),
   } as const;
}

/** Create a Sinon spy wrapper around a new DummyMcpServer instance. */
export function spyMcpServer(): sinon.SinonStubbedInstance<DummyMcpServer> {
   return sinon.createStubInstance(DummyMcpServer);
}
