import sinon from "sinon";
import { EnvironmentStore } from "../../src/service/environment_store";

/** Return a stubbed EnvironmentStore where every lookup throws or returns minimal objects. */
export function fakeEnvironmentStore(): sinon.SinonStubbedInstance<EnvironmentStore> {
   const es = sinon.createStubInstance(EnvironmentStore);
   // For now just have getEnvironment reject; suites can stub more.
   es.getEnvironment.rejects(
      new Error("fakeEnvironmentStore: getEnvironment not stubbed"),
   );
   return es;
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
      environmentStore: fakeEnvironmentStore(),
   } as const;
}

/** Create a Sinon spy wrapper around a new DummyMcpServer instance. */
export function spyMcpServer(): sinon.SinonStubbedInstance<DummyMcpServer> {
   return sinon.createStubInstance(DummyMcpServer);
}
