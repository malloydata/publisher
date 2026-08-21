import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { registerGetStatusTool } from "./get_status_tool";
import type { EnvironmentStore } from "../../service/environment_store";

/**
 * Both env vars steer resolvePublisherConfigPath, which the setup block reads.
 * A developer (or another spec in the same Bun process) with either one set
 * would otherwise fail the setup assertions below for reasons unrelated to the
 * code under test.
 */
const savedEnv = {
   configPath: process.env.PUBLISHER_CONFIG_PATH,
   bundled: process.env.PUBLISHER_USE_BUNDLED_DEFAULT,
};

beforeEach(() => {
   delete process.env.PUBLISHER_CONFIG_PATH;
   delete process.env.PUBLISHER_USE_BUNDLED_DEFAULT;
});

afterEach(() => {
   if (savedEnv.configPath === undefined)
      delete process.env.PUBLISHER_CONFIG_PATH;
   else process.env.PUBLISHER_CONFIG_PATH = savedEnv.configPath;
   if (savedEnv.bundled === undefined)
      delete process.env.PUBLISHER_USE_BUNDLED_DEFAULT;
   else process.env.PUBLISHER_USE_BUNDLED_DEFAULT = savedEnv.bundled;
});

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

describe("malloy_getStatus", () => {
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

   it("explains why nothing is served when no package is loaded", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "getstatus-"));
      try {
         const handler = captureHandler({
            serverRootPath: root,
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
         // The measured dead end: healthy-looking status, zero signal. The
         // setup block is the signal.
         expect(payload.setup).toBeDefined();
         expect(payload.setup.configFile).toBeNull();
         expect(payload.setup.nextAction.length).toBeGreaterThan(0);
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   });

   it("omits setup when a package is being served", async () => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "getstatus-"));
      try {
         const handler = captureHandler({
            serverRootPath: root,
            getStatus: async () =>
               ({
                  timestamp: 1,
                  initialized: true,
                  frozenConfig: false,
                  operationalState: "serving",
                  environments: [{ name: "local", packages: [{ name: "p" }] }],
               }) as never,
         });
         expect("setup" in parse(await handler())).toBe(false);
      } finally {
         fs.rmSync(root, { recursive: true, force: true });
      }
   });

   it("still answers when the setup diagnosis itself fails", async () => {
      // Diagnosis is additive. A server with no serverRootPath must still get
      // a status payload rather than a tool error.
      const handler = captureHandler({
         serverRootPath: undefined as never,
         getStatus: async () =>
            ({
               timestamp: 1,
               initialized: true,
               frozenConfig: false,
               operationalState: "serving",
               environments: [],
            }) as never,
      });
      const result = await handler();
      expect(result.isError).toBeFalsy();
      const payload = parse(result);
      expect(payload.operationalState).toBe("serving");
      expect("setup" in payload).toBe(false);
   });
});
