// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import http from "node:http";
import { tmpdir } from "os";
import path from "path";
import { HOST, PortAllocator, PortInUseError } from "./ports";
import { startServer } from "./server";

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
   while (cleanups.length) await cleanups.pop()?.();
});

/** A stand-in for Publisher: `bun <script>` run with the flags startServer passes. */
function fakeServer(body: string): { command: string[]; dir: string } {
   const dir = mkdtempSync(path.join(tmpdir(), "hammer-fake-server-"));
   cleanups.push(() => rmSync(dir, { recursive: true, force: true }));
   const script = path.join(dir, "server.mjs");
   writeFileSync(
      script,
      `const port = process.argv[process.argv.indexOf("--port") + 1];\n${body}\n`,
   );
   return { command: ["bun", script], dir };
}

/** Another process's Publisher, already serving on `port`. */
async function foreignPublisher(port: number): Promise<void> {
   const server = http.createServer((_req, res) => {
      res.setHeader("content-type", "application/json");
      res.end(JSON.stringify({ operationalState: "serving" }));
   });
   await new Promise<void>((resolve) => server.listen(port, HOST, resolve));
   cleanups.push(() => new Promise((r) => server.close(r)));
}

async function start(command: string[], dir: string, port: number) {
   const [mcpPort] = await new PortAllocator().allocate(1);
   return startServer({
      repoRoot: dir,
      serverRoot: dir,
      configPath: path.join(dir, "publisher.config.json"),
      port,
      mcpPort,
      mode: "on",
      command,
      cwd: dir,
      logFile: path.join(dir, "server.log"),
   });
}

describe("startServer", () => {
   // Two runs (or any two processes) can be handed the same free port: the
   // other one binds it first, and its Publisher answers /status while ours
   // is still starting. Taking that answer for ours sent a scenario's requests
   // to a server that had never heard of its environment (a 404 on its first
   // createMaterialization), while ours exited "in use" unnoticed.
   it("does not take another process's server on its port for its own", async () => {
      const [port] = await new PortAllocator().allocate(1);
      await foreignPublisher(port);
      const { command, dir } = fakeServer(
         `setTimeout(() => {
            console.error("Port " + port + " in use; pass --port <n>");
            process.exit(1);
         }, 1500);`,
      );
      const outcome = await start(command, dir, port).then(
         (server) => {
            cleanups.push(server.stop);
            return server;
         },
         (error: unknown) => error,
      );
      expect(outcome).toBeInstanceOf(PortInUseError);
      expect((outcome as PortInUseError).ports).toEqual([port]);
   });

   it("is ready when its own server says so", async () => {
      const [port] = await new PortAllocator().allocate(1);
      const { command, dir } = fakeServer(
         `console.error("PUBLISHER_READY url=http://127.0.0.1:" + port);
         setInterval(() => {}, 1000);`,
      );
      const server = await start(command, dir, port);
      cleanups.push(server.stop);
      expect(server.baseUrl).toBe(`http://${HOST}:${port}`);
   });
});
