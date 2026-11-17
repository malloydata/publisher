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
 * The configuration design's names through the real entry point: the derived
 * names configure a server from a variable, a flag or the file, with one
 * precedence, and the names step 3 removed are read by nothing: a removed
 * variable is ignored and a removed flag stops it starting.
 */

setDefaultTimeout(150_000);

/** The harness's own variables, which a case replaces. */
const HARNESS_NAMES = {
   PUBLISHER_ROOT: undefined,
   PUBLISHER_MCP_PORT: undefined,
   PUBLISHER_MCP_CONFIG_FILE_ENABLED: undefined,
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

   it("starts from the derived variables alone", async () => {
      const serverRoot = root();
      const mcpPort = await getFreePort();
      const server = await boot({
         serverRoot,
         mcpPort,
         env: {
            ...HARNESS_NAMES,
            PUBLISHER_ROOT: serverRoot,
            PUBLISHER_MCP_PORT: String(mcpPort),
            PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false",
         },
      });
      expect(server.stderr()).toContain(`mcp=http://127.0.0.1:${mcpPort} `);
   });

   it("takes the derived flags", async () => {
      const serverRoot = root();
      const mcpPort = await getFreePort();
      const server = await boot({
         serverRoot,
         env: { ...HARNESS_NAMES, PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false" },
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

   it("ignores a removed variable with a warning, and refuses a removed flag", async () => {
      // Read, MCP_PORT=1 would bind a port this process cannot open.
      const server = await boot({ env: { MCP_PORT: "1" } });
      expect(server.stderr()).not.toContain("mcp=http://127.0.0.1:1 ");
      expect(server.output()).toContain(
         "MCP_PORT is set, but this release no longer reads it: set PUBLISHER_MCP_PORT (mcp.port) instead.",
      );

      const stderr = await refused({ args: ["--mcp_port", "4141"] });
      expect(stderr).toContain(
         "Unknown flag --mcp_port. Did you mean --mcp-port?",
      );
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
   });

   it("refuses to start on a secret written into the file", async () => {
      const stderr = await refused({
         config: { authorize: { bypassSecret: "s3cret" }, environments: [] },
      });
      expect(stderr).toContain(
         "a secret in the file must be one ${VAR} reference and nothing else",
      );
      expect(stderr).not.toContain("s3cret");
   });

   it("refuses a launch setting in the file, and says what to set instead", async () => {
      const stderr = await refused({
         config: { root: "/srv/elsewhere", environments: [] },
      });
      expect(stderr).toContain(
         "cannot be set there: it decides which file is read, or must not come from one, so it is a variable or a flag only (PUBLISHER_ROOT, --root).",
      );
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
   });
});
