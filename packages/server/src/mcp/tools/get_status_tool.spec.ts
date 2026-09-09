// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { registerGetStatusTool } from "./get_status_tool";
import type { EnvironmentStore } from "../../service/environment_store";

type Content = Array<{
   type?: string;
   text?: string;
   resource?: { text: string };
}>;

type Handler = () => Promise<{ isError?: boolean; content: Content }>;

function captureHandler(store: Partial<EnvironmentStore>): Handler {
   let handler: Handler | undefined;
   const fakeServer = {
      tool: (_name: string, _desc: string, _shape: unknown, h: Handler) => {
         handler = h;
      },
   };
   registerGetStatusTool(fakeServer as never, store as EnvironmentStore);
   if (!handler) throw new Error("handler was not registered");
   return handler;
}

function parse(result: { content: Content }) {
   return JSON.parse(result.content[0].resource!.text);
}

describe("get_status", () => {
   it("reduces status to state, package names, and load errors", async () => {
      const handler = captureHandler({
         getStatus: async () =>
            ({
               timestamp: 123,
               initialized: true,
               frozenConfig: false,
               operationalState: "serving",
               environments: [
                  {
                     name: "local",
                     packages: [{ name: "spotify", description: "music" }],
                     connections: [{ name: "secret-conn" }],
                  },
               ],
               loadErrors: [
                  {
                     environment: "local",
                     package: "spotify",
                     message: "broken.malloy: parse error",
                     stale: true,
                     failedAt: "2026-08-13T00:00:00.000Z",
                  },
               ],
            }) as never,
      });
      const payload = parse(await handler());
      expect(payload).toEqual({
         operationalState: "serving",
         initialized: true,
         environments: [{ name: "local", packages: ["spotify"] }],
         loadErrors: [
            {
               environment: "local",
               package: "spotify",
               message: "broken.malloy: parse error",
               stale: true,
               failedAt: "2026-08-13T00:00:00.000Z",
            },
         ],
      });
      // Reduction is the security property: no connections, no locations,
      // no theme, no package descriptions.
      expect(JSON.stringify(payload)).not.toContain("secret-conn");
   });

   it("omits loadErrors when the status has none", async () => {
      const handler = captureHandler({
         getStatus: async () =>
            ({
               timestamp: 1,
               initialized: true,
               frozenConfig: false,
               operationalState: "serving",
               environments: [],
            }) as never,
      });
      const payload = parse(await handler());
      expect("loadErrors" in payload).toBe(false);
   });

   it("surfaces a store failure as a tool error payload", async () => {
      const handler = captureHandler({
         getStatus: async () => {
            throw new Error("store not initialized");
         },
      });
      const result = await handler();
      expect(result.isError).toBe(true);
      const payload = parse(result);
      expect(payload.error).toContain("store not initialized");
   });
});
