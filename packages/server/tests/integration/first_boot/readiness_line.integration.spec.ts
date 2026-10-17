// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import {
   pollUntil,
   singlePackageConfig,
   spawnServer,
   type SpawnedServer,
   writeSourcePackage,
} from "../../harness/spawn_server";

/**
 * End-to-end coverage for the PUBLISHER_READY line: the contract scripts grep
 * for instead of polling /api/v0/status. It must appear on stderr exactly
 * once, only when the server is actually serving, and carry the environment,
 * package, and load-error counts. That contract spans process boundaries
 * (a real stderr stream from a real boot), so it needs a dedicated server
 * subprocess rather than the in-process REST harness.
 */

const ENV_NAME = "ready-env";
const PKG_NAME = "ready-pkg";

// Covers the `beforeAll` hook too, and holds when this file runs on its own.
setDefaultTimeout(150_000);

describe("first-boot readiness line", () => {
   let server: SpawnedServer;
   let src: { dir: string; cleanup: () => void };

   const readyLines = () =>
      server
         .stderr()
         .split("\n")
         .filter((line) => line.startsWith("PUBLISHER_READY"));

   beforeAll(async () => {
      src = writeSourcePackage(PKG_NAME);
      server = await spawnServer({
         config: singlePackageConfig(ENV_NAME, {
            name: PKG_NAME,
            location: src.dir,
         }),
      });
      await server.waitUntilServing();
      // The line is written in the same tick that flips the status to
      // serving, but the pipe delivery can trail the HTTP response.
      await pollUntil(() => readyLines().length > 0, 10_000, 100);
   });

   afterAll(async () => {
      await server?.stop();
      src?.cleanup();
   });

   it("prints one PUBLISHER_READY line on stderr once serving", () => {
      const lines = readyLines();
      expect(lines).toHaveLength(1);
      // The URL host/port are the ones the server was actually started with,
      // so a script can dial what the line says.
      expect(lines[0]).toMatch(
         new RegExp(
            `^PUBLISHER_READY url=http://127\\.0\\.0\\.1:${server.restPort} ` +
               `mcp=http://127\\.0\\.0\\.1:\\d+ ` +
               `environments=1 packages=1 load_errors=0$`,
         ),
      );
   });
});
