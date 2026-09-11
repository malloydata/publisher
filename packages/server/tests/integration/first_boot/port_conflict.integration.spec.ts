// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it } from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

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

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// tests/integration/first_boot -> packages/server
const SERVER_DIR = path.resolve(__dirname, "../../..");

/** Allocate an OS-assigned free TCP port (avoids fixed-port collisions). */
async function getFreePort(): Promise<number> {
   return new Promise<number>((resolve, reject) => {
      const srv = net.createServer();
      srv.on("error", reject);
      srv.listen(0, "127.0.0.1", () => {
         const addr = srv.address();
         const found = typeof addr === "object" && addr ? addr.port : 0;
         srv.close(() =>
            found ? resolve(found) : reject(new Error("no free port")),
         );
      });
   });
}

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

interface BootResult {
   exitCode: number | null;
   log: string;
   /** The cwd the server booted in: where .mcp.json would land. */
   workDir: string;
}

const cleanups: Array<() => void> = [];
afterEach(() => {
   while (cleanups.length) cleanups.pop()?.();
});

/**
 * Boot a real server against the given ports and wait for it to exit. The cwd
 * is a fresh non-git temp dir, so the `.mcp.json` write is NOT suppressed by
 * the git-worktree guard: if the boot wrongly writes one, this harness sees it.
 */
async function bootExpectingExit(
   publisherPort: number,
   mcpPort: number,
): Promise<BootResult> {
   const workDir = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "port-conflict-cwd-")),
   );
   const serverRoot = fs.mkdtempSync(
      path.join(os.tmpdir(), "port-conflict-root-"),
   );
   fs.writeFileSync(
      path.join(serverRoot, "publisher.config.json"),
      JSON.stringify({ frozenConfig: false, environments: [] }),
   );
   cleanups.push(() => {
      for (const dir of [workDir, serverRoot]) {
         try {
            fs.rmSync(dir, { recursive: true, force: true });
         } catch {
            // best-effort cleanup
         }
      }
   });

   let log = "";
   const proc: ChildProcess = spawn(
      "bun",
      [path.join(SERVER_DIR, "src", "server.ts")],
      {
         cwd: workDir,
         env: {
            ...process.env,
            SERVER_ROOT: serverRoot,
            PUBLISHER_HOST: "127.0.0.1",
            PUBLISHER_PORT: String(publisherPort),
            MCP_PORT: String(mcpPort),
         },
         stdio: ["ignore", "pipe", "pipe"],
      },
   );
   proc.stdout?.on("data", (d: Buffer) => {
      log = (log + d.toString()).slice(-16000);
   });
   proc.stderr?.on("data", (d: Buffer) => {
      log = (log + d.toString()).slice(-16000);
   });

   const exitCode = await new Promise<number | null>((resolve, reject) => {
      const timeout = setTimeout(() => {
         proc.kill("SIGKILL");
         reject(
            new Error(
               `server did not exit within 90s\n--- log tail ---\n${log}`,
            ),
         );
      }, 90_000);
      proc.on("exit", (code) => {
         clearTimeout(timeout);
         resolve(code);
      });
   });
   return { exitCode, log, workDir };
}

describe("boot with both ports free", () => {
   it("writes .mcp.json naming the bound MCP endpoint once both listeners are up", async () => {
      // The write is deferred until BOTH listeners bind; this is the first
      // test of the real boot-time call site (mcp_config.spec.ts covers the
      // function, not the wiring).
      const publisherPort = await getFreePort();
      let mcpPort = await getFreePort();
      while (mcpPort === publisherPort) mcpPort = await getFreePort();

      const workDir = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "port-happy-cwd-")),
      );
      const serverRoot = fs.mkdtempSync(
         path.join(os.tmpdir(), "port-happy-root-"),
      );
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         JSON.stringify({ frozenConfig: false, environments: [] }),
      );

      const proc: ChildProcess = spawn(
         "bun",
         [path.join(SERVER_DIR, "src", "server.ts")],
         {
            cwd: workDir,
            env: {
               ...process.env,
               SERVER_ROOT: serverRoot,
               PUBLISHER_HOST: "127.0.0.1",
               PUBLISHER_PORT: String(publisherPort),
               MCP_PORT: String(mcpPort),
            },
            stdio: ["ignore", "ignore", "pipe"],
         },
      );
      let exited = false;
      proc.on("exit", () => {
         exited = true;
      });
      cleanups.push(() => {
         if (!exited) proc.kill("SIGKILL");
         for (const dir of [workDir, serverRoot]) {
            try {
               fs.rmSync(dir, { recursive: true, force: true });
            } catch {
               // best-effort cleanup
            }
         }
      });

      const file = path.join(workDir, ".mcp.json");
      const written = await (async () => {
         const deadline = Date.now() + 90_000;
         while (Date.now() < deadline) {
            if (exited) return false;
            if (fs.existsSync(file)) return true;
            await new Promise((r) => setTimeout(r, 200));
         }
         return false;
      })();
      expect(written).toBe(true);
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toEqual({
         mcpServers: {
            malloy: {
               type: "http",
               url: `http://127.0.0.1:${mcpPort}/mcp`,
            },
         },
      });
   }, 120_000);
});

describe("boot with a busy port", () => {
   it("REST port busy: one actionable line, nonzero exit, and no .mcp.json left behind", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);
      const mcpPort = await getFreePort();

      const result = await bootExpectingExit(blocker.port, mcpPort);
      expect(result.exitCode).not.toBe(0);
      expect(result.log).toContain(
         `Port ${blocker.port} in use; pass --port <n>`,
      );
      // The old failure shape: MCP wins the bind race, writes the file,
      // REST dies. The file must not exist for a boot that failed.
      expect(fs.existsSync(path.join(result.workDir, ".mcp.json"))).toBe(false);
   }, 120_000);

   it("MCP port busy: names --mcp_port, nonzero exit, and no .mcp.json left behind", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);
      const publisherPort = await getFreePort();

      const result = await bootExpectingExit(publisherPort, blocker.port);
      expect(result.exitCode).not.toBe(0);
      expect(result.log).toContain(
         `Port ${blocker.port} in use; pass --mcp_port <n>`,
      );
      expect(fs.existsSync(path.join(result.workDir, ".mcp.json"))).toBe(false);
   }, 120_000);
});
