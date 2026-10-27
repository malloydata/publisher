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
 * `server.ts` parses reach the listeners, ahead of their variables, and what
 * the parser does with a flag it does not know. The per-setting cases are in
 * src/settings/contract.spec.ts.
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

   it("ignores a flag it does not know and a flag missing its value", async () => {
      server = await spawnServer({
         // The design's derived name, not read before step 2, then a known
         // flag with nothing after it.
         args: ["--mcp-port", "1", "--admin_port"],
      });

      const line = await waitForReady();
      expect(line).toContain(`mcp=http://127.0.0.1:${server.mcpPort} `);
      await server.waitUntilServing();
   });
});
