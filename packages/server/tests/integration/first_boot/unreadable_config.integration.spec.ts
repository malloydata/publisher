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
import { writeFileSync } from "fs";
import { join } from "path";
import {
   pollUntil,
   spawnServer,
   type SpawnedServer,
   tempDir,
} from "../../harness/spawn_server";

describe("server with an unusable publisher.config.json", () => {
   const cleanups: Array<() => unknown> = [];
   afterEach(async () => {
      while (cleanups.length) await cleanups.pop()?.();
   });

   /** /status once it names an initError, or undefined if the server exits first. */
   async function statusWithin(server: SpawnedServer, ms: number) {
      let status: { initError?: string } | undefined;
      let last: unknown;
      await pollUntil(async () => {
         if (server.exited()) return true;
         try {
            const res = await fetch(`${server.baseUrl}/api/v0/status`);
            const body = (await res.json()) as { initError?: string };
            if (body.initError) status = body;
            last = body;
         } catch (error) {
            last = error;
         }
         return status !== undefined;
      }, ms);
      if (status || server.exited()) return status;
      throw new Error(
         `/status never reported an initError within ${ms}ms; last: ${String(
            last instanceof Error ? last.message : JSON.stringify(last),
         )}\n--- server log tail ---\n${server.output().slice(-4000)}`,
      );
   }

   /**
    * Boot on `configText` and wait for /status to name the init error. The
    * ports are probed free and only bound seconds later by the child, so a
    * suite running alongside can take one in between; the server then exits
    * with "Port N in use", and that boot is retried on new ports. Any other
    * exit fails with the server's output.
    */
   async function bootUntilInitError(configText: string) {
      const root = tempDir("publisher-bad-config-");
      cleanups.push(root.cleanup);
      writeFileSync(join(root.dir, "publisher.config.json"), configText);
      for (let attempt = 1; ; attempt++) {
         const server = await spawnServer({ serverRoot: root.dir });
         cleanups.push(() => server.stop());
         const status = await statusWithin(server, 60_000);
         if (status) return { server, status };
         if (attempt < 3 && server.output().includes(" in use; pass --")) {
            continue;
         }
         throw new Error(
            `server exited before /status answered\n--- server log tail ---\n${server
               .output()
               .slice(-4000)}`,
         );
      }
   }

   it("stays up, prints PUBLISHER_INIT_FAILED, and /status names the cause", async () => {
      const { server, status } = await bootUntilInitError(
         '{"environments":[],}',
      );
      expect(status.initError).toContain("publisher.config.json");
      expect(status.initError).toContain("JSON Parse error");
      expect(server.exited()).toBe(false);
      // stderr arrives on its own pipe, so it can trail the /status answer.
      expect(
         await pollUntil(
            () => server.stderr().includes("PUBLISHER_INIT_FAILED"),
            10_000,
         ),
      ).toBe(true);
   }, 200_000);

   it("stays up when the retrieval block itself is invalid", async () => {
      const { server, status } = await bootUntilInitError(
         '{"environments":[],"retrieval":{"indexing":{"maxEntities":"lots"}}}',
      );
      expect(status.initError).toContain("retrieval.indexing.maxEntities");
      expect(server.exited()).toBe(false);
   }, 200_000);
});
