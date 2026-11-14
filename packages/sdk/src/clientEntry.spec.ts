// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";

describe("@malloy-publisher/sdk/client", () => {
   // A host mounts `ServerProvider` and follows the theme mode at its root;
   // both have to come from the light entry, or the main entry's dashboard,
   // explorer and renderer code lands on every page's critical path.
   it("carries what a host needs at its root", async () => {
      const client = await import("./client-entry");
      expect(typeof client.ServerProvider).toBe("function");
      expect(typeof client.useServer).toBe("function");
      expect(typeof client.usePublisherTheme).toBe("function");
      expect(client.globalQueryClient).toBeDefined();
   });
});
