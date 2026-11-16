// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import fs from "fs";
import net from "net";
import {
   getFreePort,
   spawnServer,
   type SpawnedServer,
   tempDir,
} from "../../harness/spawn_server";

/**
 * `spawnServer` picks a port by binding and releasing it, so another process
 * can take the port before the server binds it. The harness then boots again
 * on fresh ports. These specs lose the race on purpose: an injected picker
 * hands out a port this file holds.
 */
setDefaultTimeout(200_000);

/** Bind and hold a fresh loopback port. */
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

/** A picker whose `n`th calls (1-based) in `taken` return `port`. */
function pickerTaking(port: number, taken: (call: number) => boolean) {
   let calls = 0;
   return async () => (taken(++calls) ? port : getFreePort());
}

const config = { frozenConfig: false, environments: [] };

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
   while (cleanups.length) await cleanups.pop()?.();
});

/**
 * Each attempt is gone: process exited, the root the harness made removed.
 * Windows can hold a just-exited process's directories busy past the
 * harness's removal retries, so the root is checked elsewhere only.
 */
function expectGone(attempts: SpawnedServer[]): void {
   for (const attempt of attempts) {
      expect(attempt.exited()).toBe(true);
      if (process.platform !== "win32") {
         expect(fs.existsSync(attempt.serverRoot)).toBe(false);
      }
   }
}

describe("spawnServer on a port taken before the server bound it", () => {
   for (const [label, firstCall] of [
      ["REST", 1],
      ["MCP", 2],
   ] as const) {
      it(`boots again on fresh ports when the ${label} port was taken`, async () => {
         const blocker = await occupyPort();
         cleanups.push(blocker.release);
         const attempts: SpawnedServer[] = [];
         const server = await spawnServer({
            config,
            pickPort: pickerTaking(blocker.port, (n) => n === firstCall),
            onAttempt: (a) => attempts.push(a),
         });
         cleanups.push(() => server.stop());

         await server.waitUntilServing();
         expect(attempts).toHaveLength(2);
         expect(attempts[0].output()).toContain(`Port ${blocker.port} in use`);
         expect([server.restPort, server.mcpPort]).not.toContain(blocker.port);
         expectGone(attempts.slice(0, -1));
      });
   }

   it("fails with the last output after three lost races, leaving nothing behind", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);
      const attempts: SpawnedServer[] = [];
      // The REST port is picked first on every boot, so every boot loses it.
      const boot = spawnServer({
         config,
         pickPort: pickerTaking(blocker.port, (n) => n % 2 === 1),
         onAttempt: (a) => attempts.push(a),
      });

      await expect(boot).rejects.toThrow(
         `Port ${blocker.port} in use; pass --port <n>`,
      );
      expect(attempts).toHaveLength(3);
      expectGone(attempts);
   });

   it("returns the exited server when the caller pinned the taken port", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);
      const attempts: SpawnedServer[] = [];
      const server = await spawnServer({
         config,
         restPort: blocker.port,
         onAttempt: (a) => attempts.push(a),
      });
      cleanups.push(() => server.stop());

      expect(await server.waitForExit()).not.toBe(0);
      expect(server.output()).toContain(
         `Port ${blocker.port} in use; pass --port <n>`,
      );
      expect(attempts).toHaveLength(1);
   });

   it("does not retry a boot on a caller's server root", async () => {
      const blocker = await occupyPort();
      cleanups.push(blocker.release);
      const root = tempDir("port-retry-root-");
      cleanups.push(root.cleanup);
      const attempts: SpawnedServer[] = [];
      const server = await spawnServer({
         config,
         serverRoot: root.dir,
         pickPort: pickerTaking(blocker.port, (n) => n === 1),
         onAttempt: (a) => attempts.push(a),
      });
      cleanups.push(() => server.stop());

      expect(await server.waitForExit()).not.toBe(0);
      expect(server.output()).toContain(`Port ${blocker.port} in use`);
      expect(attempts).toHaveLength(1);
   });

   it("stops the boot when onAttempt throws", async () => {
      const attempts: SpawnedServer[] = [];
      const boot = spawnServer({
         config,
         onAttempt: (a) => {
            attempts.push(a);
            throw new Error("hook failed");
         },
      });

      await expect(boot).rejects.toThrow("hook failed");
      expectGone(attempts);
   });
});

describe("spawnServer under a quiet LOG_LEVEL", () => {
   it("returns once the server holds its ports, without the listening lines", async () => {
      const started = Date.now();
      const server = await spawnServer({
         config,
         env: { LOG_LEVEL: "error" },
      });
      cleanups.push(() => server.stop());
      const elapsed = Date.now() - started;

      expect(server.exited(), server.output().slice(-2000)).toBe(false);
      expect(server.output()).not.toContain("listening at");
      // The settle wait gives up at 90 s; a boot takes a few.
      expect(elapsed).toBeLessThan(45_000);
      await server.waitUntilServing();
   });
});
