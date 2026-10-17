// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

const SERVER_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../..",
);

/** An OS-assigned free TCP port. */
export async function getFreePort(): Promise<number> {
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

/** True once `predicate` is, or false if `timeoutMs` passes first. */
export async function pollUntil(
   predicate: () => Promise<boolean> | boolean,
   timeoutMs: number,
   intervalMs = 200,
): Promise<boolean> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      if (await predicate()) return true;
      await new Promise((r) => setTimeout(r, intervalMs));
   }
   return false;
}

export interface SpawnedServer {
   proc: ChildProcess;
   restPort: number;
   mcpPort: number;
   serverRoot: string;
   /** Everything written to stdout and stderr so far. */
   output(): string;
   stderr(): string;
   exited(): boolean;
   stop(): Promise<void>;
}

/**
 * `bun src/server.ts` as a real process on free ports, with an empty server
 * root unless `serverRoot` is given. `env` is merged over this process's.
 */
export async function spawnServer(
   options: {
      env?: Record<string, string | undefined>;
      serverRoot?: string;
      args?: string[];
   } = {},
): Promise<SpawnedServer> {
   const restPort = await getFreePort();
   let mcpPort = await getFreePort();
   while (mcpPort === restPort) mcpPort = await getFreePort();
   const serverRoot =
      options.serverRoot ??
      fs.mkdtempSync(path.join(os.tmpdir(), "spawned-root-"));
   const env: Record<string, string> = {};
   for (const [key, value] of Object.entries({
      ...process.env,
      SERVER_ROOT: serverRoot,
      PUBLISHER_PORT: String(restPort),
      MCP_PORT: String(mcpPort),
      PUBLISHER_NO_MCP_CONFIG: "true",
      ...options.env,
   })) {
      if (value !== undefined) env[key] = value;
   }
   const proc = spawn("bun", ["src/server.ts", ...(options.args ?? [])], {
      cwd: SERVER_DIR,
      env,
      stdio: ["ignore", "pipe", "pipe"],
   });
   let all = "";
   let err = "";
   let exited = false;
   proc.stdout?.on("data", (d: Buffer) => (all += d.toString()));
   proc.stderr?.on("data", (d: Buffer) => {
      all += d.toString();
      err += d.toString();
   });
   proc.on("exit", () => (exited = true));
   return {
      proc,
      restPort,
      mcpPort,
      serverRoot,
      output: () => all,
      stderr: () => err,
      exited: () => exited,
      stop: async () => {
         if (exited) return;
         proc.kill("SIGKILL");
         await pollUntil(() => exited, 10_000, 50);
      },
   };
}
