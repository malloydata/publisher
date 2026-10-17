// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { pollUntil as poll } from "../../src/test_helpers/poll";

/** `src/test_helpers/poll`'s pollUntil, at an interval suited to HTTP and process polls. */
export const pollUntil = (
   predicate: () => Promise<boolean> | boolean,
   timeoutMs: number,
   intervalMs = 200,
): Promise<boolean> => poll(predicate, timeoutMs, intervalMs);

const SERVER_DIR = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "../..",
);
const SERVER_ENTRY = path.join(SERVER_DIR, "src", "server.ts");

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

/** A fresh temp directory, removed by the returned `cleanup`. */
export function tempDir(prefix: string): { dir: string; cleanup: () => void } {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
   return {
      dir,
      cleanup: () => fs.rmSync(dir, { recursive: true, force: true }),
   };
}

/**
 * A package source directory: `publisher.json` naming `name`, plus `files`
 * (relative path → contents). Defaults to one DuckDB model, `first.malloy`.
 */
export function writeSourcePackage(
   name: string,
   files: Record<string, string> = {
      "first.malloy": 'source: first_source is duckdb.sql("SELECT 1 as n")\n',
   },
): { dir: string; cleanup: () => void } {
   const pkg = tempDir("spawned-src-");
   fs.writeFileSync(
      path.join(pkg.dir, "publisher.json"),
      JSON.stringify({ name }),
   );
   for (const [relative, contents] of Object.entries(files)) {
      fs.writeFileSync(path.join(pkg.dir, relative), contents);
   }
   return pkg;
}

/** A `publisher.config.json` body with one environment serving one package. */
export function singlePackageConfig(
   environment: string,
   pkg: { name: string; location: string },
   connections: unknown[] = [],
): Record<string, unknown> {
   return {
      frozenConfig: false,
      environments: [{ name: environment, packages: [pkg], connections }],
   };
}

export interface SpawnedServer {
   proc: ChildProcess;
   restPort: number;
   mcpPort: number;
   serverRoot: string;
   /** `http://127.0.0.1:<restPort>`. */
   baseUrl: string;
   /** Everything written to stdout and stderr so far. */
   output(): string;
   stderr(): string;
   exited(): boolean;
   /**
    * Resolves once `/api/v0/status` reports `serving`; throws, with the log
    * tail, if the process exits first or `timeoutMs` passes.
    */
   waitUntilServing(timeoutMs?: number): Promise<void>;
   /** The exit code once the process exits; throws if `timeoutMs` passes. */
   waitForExit(timeoutMs?: number): Promise<number | null>;
   /** SIGKILL, then remove the server root if this harness created it. */
   stop(): Promise<void>;
}

/**
 * `bun src/server.ts` as a real process on free ports (unless given), REST
 * bound to loopback, with an empty server root unless `serverRoot` is given.
 * `config` is written to the root's `publisher.config.json`. `env` is merged
 * over this process's; a key set to `undefined` is removed.
 */
export async function spawnServer(
   options: {
      env?: Record<string, string | undefined>;
      serverRoot?: string;
      config?: unknown;
      args?: string[];
      /** The process cwd, where `.mcp.json` would land. Defaults to packages/server. */
      cwd?: string;
      restPort?: number;
      mcpPort?: number;
   } = {},
): Promise<SpawnedServer> {
   const restPort = options.restPort ?? (await getFreePort());
   let mcpPort = options.mcpPort ?? (await getFreePort());
   while (options.mcpPort === undefined && mcpPort === restPort) {
      mcpPort = await getFreePort();
   }
   const ownRoot = options.serverRoot === undefined;
   const serverRoot =
      options.serverRoot ??
      fs.mkdtempSync(path.join(os.tmpdir(), "spawned-root-"));
   if (options.config !== undefined) {
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         JSON.stringify(options.config),
      );
   }
   const env: Record<string, string> = {};
   for (const [key, value] of Object.entries({
      ...process.env,
      SERVER_ROOT: serverRoot,
      PUBLISHER_HOST: "127.0.0.1",
      PUBLISHER_PORT: String(restPort),
      MCP_PORT: String(mcpPort),
      PUBLISHER_NO_MCP_CONFIG: "true",
      ...options.env,
   })) {
      if (value !== undefined) env[key] = value;
   }
   const proc = spawn("bun", [SERVER_ENTRY, ...(options.args ?? [])], {
      cwd: options.cwd ?? SERVER_DIR,
      env,
      stdio: ["ignore", "pipe", "pipe"],
   });
   let all = "";
   let err = "";
   let exitCode: number | null | undefined;
   proc.stdout?.on("data", (d: Buffer) => (all += d.toString()));
   proc.stderr?.on("data", (d: Buffer) => {
      all += d.toString();
      err += d.toString();
   });
   proc.on("exit", (code) => (exitCode = code));
   const exited = () => exitCode !== undefined;
   const baseUrl = `http://127.0.0.1:${restPort}`;
   const tail = () => `\n--- server log tail ---\n${all.slice(-4000)}`;

   return {
      proc,
      restPort,
      mcpPort,
      serverRoot,
      baseUrl,
      output: () => all,
      stderr: () => err,
      exited,
      waitUntilServing: async (timeoutMs = 120_000) => {
         const serving = await pollUntil(async () => {
            if (exited()) return true;
            try {
               const res = await fetch(`${baseUrl}/api/v0/status`);
               if (!res.ok) return false;
               const body = (await res.json()) as {
                  operationalState?: string;
               };
               return body.operationalState === "serving";
            } catch {
               return false;
            }
         }, timeoutMs);
         if (exited()) {
            throw new Error(`server exited before serving${tail()}`);
         }
         if (!serving) {
            throw new Error(
               `server not serving within ${timeoutMs}ms${tail()}`,
            );
         }
      },
      waitForExit: async (timeoutMs = 90_000) => {
         if (!(await pollUntil(exited, timeoutMs, 50))) {
            proc.kill("SIGKILL");
            throw new Error(
               `server did not exit within ${timeoutMs}ms${tail()}`,
            );
         }
         return exitCode ?? null;
      },
      stop: async () => {
         if (!exited()) {
            proc.kill("SIGKILL");
            await pollUntil(exited, 10_000, 50);
         }
         if (ownRoot) fs.rmSync(serverRoot, { recursive: true, force: true });
      },
   };
}
