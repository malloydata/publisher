// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";

/**
 * Step 2 of the configuration design through the real entry point: the
 * derived names configure a server from a variable, a flag or the file, with
 * one precedence; today's names still work, each with a deprecation warning
 * and a count in publisher_deprecated_usage_total; and an old and a new name
 * that disagree stop it starting.
 */

setDefaultTimeout(150_000);

/** The harness's own variables, by today's names, which a case replaces. */
const TODAYS_NAMES = {
   SERVER_ROOT: undefined,
   MCP_PORT: undefined,
   PUBLISHER_NO_MCP_CONFIG: undefined,
};

describe("settings by their derived names", () => {
   const servers: SpawnedServer[] = [];
   const cleanups: Array<() => void> = [];
   afterEach(async () => {
      while (servers.length) await servers.pop()!.stop();
      while (cleanups.length) cleanups.pop()!();
   });

   async function boot(
      options: Parameters<typeof spawnServer>[0] = {},
   ): Promise<SpawnedServer> {
      const server = await spawnServer(options);
      servers.push(server);
      const ready = () => server.stderr().includes("PUBLISHER_READY");
      await pollUntil(() => ready() || server.exited(), 120_000, 100);
      if (!ready()) throw new Error(`did not start:\n${server.output()}`);
      return server;
   }

   async function refused(
      options: Parameters<typeof spawnServer>[0],
   ): Promise<string> {
      const server = await spawnServer(options);
      servers.push(server);
      expect(await server.waitForExit()).not.toBe(0);
      return server.stderr();
   }

   const root = () => {
      const dir = tempDir("settings-names-root-");
      cleanups.push(dir.cleanup);
      return dir.dir;
   };

   it("starts from the derived variables alone, with no deprecation warning", async () => {
      const serverRoot = root();
      const mcpPort = await getFreePort();
      const server = await boot({
         serverRoot,
         mcpPort,
         env: {
            ...TODAYS_NAMES,
            PUBLISHER_ROOT: serverRoot,
            PUBLISHER_MCP_PORT: String(mcpPort),
            PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false",
         },
      });
      expect(server.stderr()).toContain(`mcp=http://127.0.0.1:${mcpPort} `);
      expect(server.output()).not.toContain("is deprecated");
   });

   it("takes the derived flags", async () => {
      const serverRoot = root();
      const mcpPort = await getFreePort();
      const server = await boot({
         serverRoot,
         env: { ...TODAYS_NAMES, PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false" },
         args: [
            "--root",
            serverRoot,
            "--mcp-port",
            String(mcpPort),
            "--frame-ancestors",
            "https://flag.example",
         ],
      });
      expect(server.stderr()).toContain(`mcp=http://127.0.0.1:${mcpPort} `);
      const res = await fetch(`${server.baseUrl}/api/v0/status`);
      expect(res.headers.get("content-security-policy")).toBe(
         "frame-ancestors https://flag.example",
      );
   });

   it("reads any setting from its key in the file, under a variable that is given", async () => {
      const fromFile = await boot({
         config: { frameAncestors: "https://file.example", environments: [] },
      });
      expect(
         (await fetch(`${fromFile.baseUrl}/api/v0/status`)).headers.get(
            "content-security-policy",
         ),
      ).toBe("frame-ancestors https://file.example");

      const fromVariable = await boot({
         env: { PUBLISHER_FRAME_ANCESTORS: "https://env.example" },
         config: { frameAncestors: "https://file.example", environments: [] },
      });
      expect(
         (await fetch(`${fromVariable.baseUrl}/api/v0/status`)).headers.get(
            "content-security-policy",
         ),
      ).toBe("frame-ancestors https://env.example");
   });

   it("still reads today's names, warns about each and counts it", async () => {
      // The harness starts servers by today's names (MCP_PORT, SERVER_ROOT).
      const server = await boot();
      expect(server.output()).toContain(
         "MCP_PORT is deprecated: set PUBLISHER_MCP_PORT instead (mcp.port). A later release stops reading it.",
      );
      const metrics = await (await fetch(`${server.baseUrl}/metrics`)).text();
      expect(metrics).toMatch(
         /publisher_deprecated_usage_total\{[^}]*feature="config:MCP_PORT"[^}]*\} 1/,
      );
   });

   it("refuses to start when an old and a new name disagree", async () => {
      const stderr = await refused({
         env: { MCP_PORT: "4141", PUBLISHER_MCP_PORT: "4242" },
      });
      expect(stderr).toContain(
         "mcp.port is given different values under its old and new names: MCP_PORT (4141) and PUBLISHER_MCP_PORT (4242). Remove the old one.",
      );
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
   });

   it("refuses to start on a secret written into the file", async () => {
      const stderr = await refused({
         config: { authorize: { bypassSecret: "s3cret" }, environments: [] },
      });
      expect(stderr).toContain(
         "a secret in the file must be a ${VAR} reference, not the value itself",
      );
      expect(stderr).not.toContain("s3cret");
   });

   it("ignores a launch setting in the file, and says so", async () => {
      const server = await boot({
         config: { root: "/srv/elsewhere", environments: [] },
      });
      expect(server.output()).toContain(
         "is a variable or a flag only (PUBLISHER_ROOT, --root).",
      );
   });
});
