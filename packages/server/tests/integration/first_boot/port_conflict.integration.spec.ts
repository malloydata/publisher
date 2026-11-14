// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import fs from "fs";
import net from "net";
import path from "path";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   type SpawnedServer,
   tempDir,
} from "../../harness/spawn_server";

/**
 * End-to-end coverage for a boot whose REST or MCP port is already taken.
 *
 * The old behavior was an uncaught 'error' event: a ~40-line crash dump for
 * what is usually just a busy port, and, worse, the MCP listener could win the
 * race, write `.mcp.json`, and then die with the REST failure, leaving a file
 * pointing at a dead port that silently breaks the NEXT agent session (the
 * successful retry on other ports never rewrites it). The contract now: one
 * actionable line naming the flag to pass, a nonzero exit, and no `.mcp.json`
 * left behind. Spans process boundaries, so it needs real subprocesses.
 */

setDefaultTimeout(120_000);

/** Bind and hold a fresh port so the server under test collides with it. */
async function occupyPort(): Promise<{ port: number; release: () => void }> {
   return new Promise((resolve, reject) => {
      const srv = net.createServer();
      srv.on("error", reject);
      srv.listen(0, "127.0.0.1", () => {
         const addr = srv.address();
         const port = typeof addr === "object" && addr ? addr.port : 0;
         if (!port) return reject(new Error("no port to occupy"));
         resolve({ port, release: () => srv.close() });
      });
   });
}

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
   while (cleanups.length) await cleanups.pop()?.();
});

/**
 * Boot a real server on the given ports, in a fresh non-git temp cwd so the
 * `.mcp.json` write is NOT suppressed by the git-worktree guard: if the boot
 * wrongly writes one, the caller sees it at `<workDir>/.mcp.json`.
 */
async function boot(
   restPort: number,
   mcpPort: number,
): Promise<{ server: SpawnedServer; mcpJson: string }> {
   const work = tempDir("port-conflict-cwd-");
   const workDir = fs.realpathSync(work.dir);
   const server = await spawnServer({
      cwd: workDir,
      restPort,
      mcpPort,
      config: { frozenConfig: false, environments: [] },
      env: { PUBLISHER_NO_MCP_CONFIG: undefined },
   });
   cleanups.push(work.cleanup, () => server.stop());
   return { server, mcpJson: path.join(workDir, ".mcp.json") };
}

describe("boot with both ports free", () => {
   it("writes .mcp.json naming the bound MCP endpoint once both listeners are up", async () => {
      // The write is deferred until BOTH listeners bind; this is the first
      // test of the real boot-time call site (mcp_config.spec.ts covers the
      // function, not the wiring).
      const restPort = await getFreePort();
      let mcpPort = await getFreePort();
      while (mcpPort === restPort) mcpPort = await getFreePort();

      const { server, mcpJson } = await boot(restPort, mcpPort);
      const written = await pollUntil(
         () => server.exited() || fs.existsSync(mcpJson),
         90_000,
      );
      expect(written && !server.exited()).toBe(true);
      expect(JSON.parse(fs.readFileSync(mcpJson, "utf8"))).toEqual({
         mcpServers: {
            malloy: {
               type: "http",
               url: `http://127.0.0.1:${mcpPort}/mcp`,
            },
         },
      });
   });
});

describe("boot with a busy port", () => {
   it("REST port busy: one actionable line, nonzero exit, and no .mcp.json left behind", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);

      const { server, mcpJson } = await boot(blocker.port, await getFreePort());
      expect(await server.waitForExit()).not.toBe(0);
      expect(server.output()).toContain(
         `Port ${blocker.port} in use; pass --port <n>`,
      );
      // The old failure shape: MCP wins the bind race, writes the file,
      // REST dies. The file must not exist for a boot that failed.
      expect(fs.existsSync(mcpJson)).toBe(false);
   });

   it("MCP port busy: names --mcp_port, nonzero exit, and no .mcp.json left behind", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);

      const { server, mcpJson } = await boot(await getFreePort(), blocker.port);
      expect(await server.waitForExit()).not.toBe(0);
      expect(server.output()).toContain(
         `Port ${blocker.port} in use; pass --mcp_port <n>`,
      );
      expect(fs.existsSync(mcpJson)).toBe(false);
   });
});
