// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A publisher.config.json the server cannot parse must leave the server up,
 * with /status naming the cause and PUBLISHER_INIT_FAILED on stderr. The
 * Docker smoke script checks the unreadable-file case (S7); this runs the real
 * entry point with a malformed file so the same contract is held without Docker.
 *
 * The `retrieval` block is read at the top of server.ts, before the
 * environment store starts. If that read throws, the process dies and nothing
 * answers /status.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { createServer } from "net";
import { tmpdir } from "os";
import { join } from "path";

const SERVER_ENTRY = join(import.meta.dir, "server.ts");

async function freePort(): Promise<number> {
   return new Promise((resolve, reject) => {
      const probe = createServer();
      probe.once("error", reject);
      probe.listen(0, "127.0.0.1", () => {
         const { port } = probe.address() as { port: number };
         probe.close(() => resolve(port));
      });
   });
}

describe("server with an unusable publisher.config.json", () => {
   const cleanups: Array<() => void> = [];
   afterEach(() => {
      while (cleanups.length) cleanups.pop()!();
   });

   async function boot(configText: string) {
      const root = mkdtempSync(join(tmpdir(), "publisher-bad-config-"));
      writeFileSync(join(root, "publisher.config.json"), configText);
      const port = await freePort();
      const mcpPort = await freePort();
      const child = Bun.spawn(["bun", SERVER_ENTRY], {
         env: {
            ...process.env,
            SERVER_ROOT: root,
            PUBLISHER_PORT: String(port),
            MCP_PORT: String(mcpPort),
         },
         stdout: "ignore",
         stderr: "pipe",
      });
      cleanups.push(() => {
         child.kill();
         rmSync(root, { recursive: true, force: true });
      });
      return { child, port };
   }

   async function statusWithin(port: number, ms: number) {
      const deadline = Date.now() + ms;
      let last: unknown;
      while (Date.now() < deadline) {
         try {
            const res = await fetch(`http://127.0.0.1:${port}/api/v0/status`);
            const body = (await res.json()) as { initError?: string };
            if (body.initError) return body;
            last = body;
         } catch (error) {
            last = error;
         }
         await Bun.sleep(250);
      }
      throw new Error(
         `/status never reported an initError within ${ms}ms; last: ${String(
            last instanceof Error ? last.message : JSON.stringify(last),
         )}`,
      );
   }

   it("stays up, prints PUBLISHER_INIT_FAILED, and /status names the cause", async () => {
      const { child, port } = await boot('{"environments":[],}');
      const status = await statusWithin(port, 60_000);
      expect(status.initError).toContain("publisher.config.json");
      expect(status.initError).toContain("JSON Parse error");
      expect(child.exitCode).toBeNull();
      child.kill();
      await child.exited;
      const stderr = await new Response(child.stderr).text();
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
   }, 90_000);

   it("stays up when the retrieval block itself is invalid", async () => {
      const { child, port } = await boot(
         '{"environments":[],"retrieval":{"indexing":{"maxEntities":"lots"}}}',
      );
      const status = await statusWithin(port, 60_000);
      expect(status.initError).toContain("retrieval.indexing.maxEntities");
      expect(child.exitCode).toBeNull();
   }, 90_000);
});
