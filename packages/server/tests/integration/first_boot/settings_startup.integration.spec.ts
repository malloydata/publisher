// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   type SpawnedServer,
} from "../../harness/spawn_server";

/**
 * The settings rows only a running server can show: that the flags
 * `server.ts` parses reach the listeners, ahead of their variables, what it
 * says about a flag it does not know, and that a bad value stops it before it
 * binds. The per-setting cases are in src/settings/contract.spec.ts.
 */

setDefaultTimeout(150_000);

describe("settings at startup", () => {
   let server: SpawnedServer | undefined;
   afterEach(async () => {
      await server?.stop();
      server = undefined;
   });

   const readyLine = () =>
      server
         ?.stderr()
         .split("\n")
         .find((line) => line.startsWith("PUBLISHER_READY"));

   async function waitForReady(): Promise<string> {
      await pollUntil(
         () => readyLine() !== undefined || server!.exited(),
         120_000,
         100,
      );
      const line = readyLine();
      if (!line) {
         throw new Error(`no PUBLISHER_READY line:\n${server!.output()}`);
      }
      return line;
   }

   it("binds the ports the flags name, over their variables", async () => {
      const flagPort = await getFreePort();
      let flagMcpPort = await getFreePort();
      while (flagMcpPort === flagPort) flagMcpPort = await getFreePort();
      server = await spawnServer({
         args: ["--port", String(flagPort), "--mcp_port", String(flagMcpPort)],
      });

      const line = await waitForReady();
      expect(line).toContain(`url=http://127.0.0.1:${flagPort} `);
      expect(line).toContain(`mcp=http://127.0.0.1:${flagMcpPort} `);
      const status = await fetch(`http://127.0.0.1:${flagPort}/api/v0/status`);
      expect(status.ok).toBe(true);
   });

   it("warns about a flag it does not know and a flag missing its value, and starts", async () => {
      server = await spawnServer({
         // The design's derived name, not read before step 2, then a known
         // flag with nothing after it.
         args: ["--mcp-port", "1", "--admin_port"],
      });

      const line = await waitForReady();
      expect(line).toContain(`mcp=http://127.0.0.1:${server.mcpPort} `);
      await server.waitUntilServing();
      expect(server.output()).toContain(
         "--mcp-port is not read before a later release, so it is ignored; until then use --mcp_port.",
      );
      expect(server.output()).toContain(
         "--admin_port has no value after it, so it is ignored.",
      );
   });

   it("fails startup on bad values, naming every one, before anything binds", async () => {
      server = await spawnServer({
         env: { MCP_PORT: "40a0", PUBLISHER_MAX_QUERY_ROWS: "-1" },
      });

      expect(await server.waitForExit()).not.toBe(0);
      const stderr = server.stderr();
      expect(stderr).toContain(
         'mcp.port from MCP_PORT: expected an integer from 0 to 65535, got "40a0"',
      );
      expect(stderr).toContain(
         'query.maxRows from PUBLISHER_MAX_QUERY_ROWS: expected an integer of at least 0, got "-1"',
      );
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
      expect(stderr).not.toContain("PUBLISHER_READY");
   });
});
