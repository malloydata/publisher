// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { buildInstructions } from "./server";

/**
 * The instructions are delivered pre-authorization to every connecting client,
 * so what they claim has to be true of THIS server.
 *
 * Everything else about MCP Apps is gated both ways on the widget bundle being
 * present: no extension declared, no resource registered, no `_meta` on the tool.
 * The instruction sentence was the one place that was not, and on a server with
 * no bundle it described a capability that does not exist and told the agent to
 * call `resources/list` to check, which on exactly that server answers
 * `-32601 Method not found`. It sent an agent to make a call that errors, on the
 * server that deliberately advertises nothing.
 */
describe("buildInstructions", () => {
   it("describes the widget when this server serves one", () => {
      const withWidget = buildInstructions(true);
      expect(withWidget).toContain("MCP Apps widget");
      expect(withWidget).toContain("expanded=true");
   });

   it("says nothing about widgets when this server serves none", () => {
      const without = buildInstructions(false);
      expect(without).not.toContain("MCP Apps");
      expect(without).not.toContain("expanded=true");
      expect(without).not.toContain("inline");
   });

   it("never tells an agent to call resources/list, which 404s without a bundle", () => {
      // The specific defect. `resources/list` answers -32601 on a bundle-less
      // server, as docs/ai-agents.md documents, so advising it in a string every
      // client receives is advice that errors on half the deployments.
      for (const servesWidget of [true, false]) {
         expect(buildInstructions(servesWidget)).not.toContain(
            "resources/list",
         );
      }
   });

   it("keeps the orientation itself in both branches", () => {
      // Only the widget sentence is conditional. Losing the rest would break
      // discovery guidance for every client on a server with no bundle.
      for (const servesWidget of [true, false]) {
         const text = buildInstructions(servesWidget);
         expect(text).toContain("malloy_getContext");
         expect(text).toContain("malloy_executeQuery");
         expect(text).toContain("malloy_searchDatabaseSchema");
      }
   });

   it("adds only the widget sentence, changing nothing else", () => {
      // Pins that the with-widget text is the without-widget text plus a suffix,
      // so a future edit cannot quietly reword the shared part in one branch.
      expect(buildInstructions(true).startsWith(buildInstructions(false))).toBe(
         true,
      );
   });
});
