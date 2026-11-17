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

/**
 * Remove `dir`, retrying while Windows reports it busy. A spawned server's cwd
 * and root can stay locked for a moment after its exit event, so the first rm
 * fails with EBUSY. A directory still busy after the retries is left behind: it
 * is in the OS temp directory and says nothing about the test's result.
 */
function removeDir(dir: string): void {
   for (let attempt = 1; ; attempt++) {
      try {
         fs.rmSync(dir, { recursive: true, force: true });
         return;
      } catch (error) {
         if ((error as NodeJS.ErrnoException).code !== "EBUSY") throw error;
         if (attempt === 10) return;
         Bun.sleepSync(100 * attempt);
      }
   }
}

/** A fresh temp directory, removed by the returned `cleanup`. */
export function tempDir(prefix: string): { dir: string; cleanup: () => void } {
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
   return {
      dir,
      cleanup: () => removeDir(dir),
   };
}

/**
 * A package source directory: `publisher.json` naming `name` and version
 * 1.0.0, plus `files` (relative path → contents). Defaults to one DuckDB
 * model, `first.malloy`.
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
      JSON.stringify({ name, version: "1.0.0" }),
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

/** How many boots `spawnServer` tries before a lost port race fails the test. */
const PORT_ATTEMPTS = 3;

/** The line `main_server.ts` logs before exiting on EADDRINUSE. */
const portInUseLine = (port: number, flag: string) =>
   `Port ${port} in use; pass ${flag} <n>`;

/**
 * Both listening lines `main_server.ts` logs once its listeners bind. They are
 * info-level, so a quieter PUBLISHER_LOGGING_LEVEL leaves them out; see `settled`.
 */
const bothListening = (log: string) =>
   log.includes("Publisher server listening at") &&
   log.includes("MCP server listening at");

/** Whether something accepts a TCP connection on 127.0.0.1:`port`. */
function accepts(port: number): Promise<boolean> {
   return new Promise((resolve) => {
      const socket = net.connect({ port, host: "127.0.0.1" });
      const done = (ok: boolean) => {
         socket.destroy();
         resolve(ok);
      };
      socket.setTimeout(1_000, () => done(false));
      socket.once("connect", () => done(true));
      socket.once("error", () => done(false));
   });
}

/**
 * Resolves once `server` has exited or holds both its ports. The listening
 * lines and PUBLISHER_READY say so at the default log level, and the lines
 * also cover ports moved through `args` or `env`; a connection to both ports
 * covers a quieter PUBLISHER_LOGGING_LEVEL. A port lost to another process accepts a
 * connection too, but the server exits within milliseconds of failing to
 * bind, so connections must succeed on two polls in a row with the process
 * still up.
 */
async function settled(server: SpawnedServer): Promise<void> {
   let connectedPolls = 0;
   await pollUntil(
      async () => {
         if (server.exited()) return true;
         if (bothListening(server.output())) return true;
         if (server.stderr().includes("PUBLISHER_READY")) return true;
         const both =
            (await accepts(server.restPort)) && (await accepts(server.mcpPort));
         connectedPolls = both && !server.exited() ? connectedPolls + 1 : 0;
         return connectedPolls >= 2;
      },
      90_000,
      100,
   );
}

interface SpawnServerOptions {
   env?: Record<string, string | undefined>;
   serverRoot?: string;
   /** Written as JSON to the root's `publisher.config.json`. */
   config?: unknown;
   /** Written verbatim to the root's `publisher.config.json`, for a file that must not parse. */
   configText?: string;
   args?: string[];
   /** The process cwd, where `.mcp.json` would land. Defaults to packages/server. */
   cwd?: string;
   restPort?: number;
   mcpPort?: number;
   /** Picks each unpinned port. Defaults to `getFreePort`; tests of the retry inject one. */
   pickPort?: () => Promise<number>;
   /** Called with each boot attempt, including ones retried away. For the harness's own spec. */
   onAttempt?: (attempt: SpawnedServer) => void;
}

/**
 * `bun src/server.ts` as a real process on free ports (unless given), REST
 * bound to loopback, with an empty server root unless `serverRoot` is given.
 * `config` (as JSON) or `configText` (verbatim) is written to the root's
 * `publisher.config.json`. `env` is merged over this process's; a key set to
 * `undefined` is removed.
 *
 * A free port is picked by binding and releasing it, so another process can
 * take it before the server binds. When a port this harness picked is lost
 * that way, the server exits with its port-in-use line and the harness boots
 * again on fresh ports and a fresh root, up to three times. It does not when
 * the caller pinned the port, or passed `serverRoot` (the losing boot may
 * have half-written storage there, so a second boot would not be a first
 * one): the exited server is returned, for the caller to assert on. When it
 * may retry, the call returns once the server holds both ports or has exited.
 */
export async function spawnServer(
   options: SpawnServerOptions = {},
): Promise<SpawnedServer> {
   const pickPort = options.pickPort ?? getFreePort;
   for (let attempt = 1; ; attempt++) {
      const restPort = options.restPort ?? (await pickPort());
      let mcpPort = options.mcpPort ?? (await pickPort());
      while (options.mcpPort === undefined && mcpPort === restPort) {
         mcpPort = await pickPort();
      }
      const { server, drained } = launch(options, restPort, mcpPort);
      const retryable =
         options.serverRoot === undefined
            ? [
                 options.restPort === undefined &&
                    portInUseLine(restPort, "--port"),
                 options.mcpPort === undefined &&
                    portInUseLine(mcpPort, "--mcp-port"),
              ].filter((line): line is string => line !== false)
            : [];
      try {
         options.onAttempt?.(server);
         if (retryable.length === 0) return server;
         await settled(server);
         // 'exit' can beat the last of the output, so read it once the pipes close.
         if (server.exited()) await pollUntil(drained, 5_000, 20);
      } catch (error) {
         // A throw here must not leave the process or its root behind.
         await server.stop();
         throw error;
      }
      const lostRace =
         server.exited() &&
         retryable.some((line) => server.output().includes(line));
      if (!lostRace) return server;
      await server.stop();
      if (attempt === PORT_ATTEMPTS) {
         throw new Error(
            `server lost a free port to another process on each of ${PORT_ATTEMPTS} boots\n--- last server log tail ---\n${server.output().slice(-4000)}`,
         );
      }
   }
}

/**
 * One boot of `spawnServer` on the given ports. `drained` turns true once the
 * process's output pipes have closed, so `output()` holds everything it wrote.
 */
function launch(
   options: SpawnServerOptions,
   restPort: number,
   mcpPort: number,
): { server: SpawnedServer; drained: () => boolean } {
   const ownRoot = options.serverRoot === undefined;
   const serverRoot =
      options.serverRoot ??
      fs.mkdtempSync(path.join(os.tmpdir(), "spawned-root-"));
   const configText =
      options.configText ??
      (options.config === undefined
         ? undefined
         : JSON.stringify(options.config));
   if (configText !== undefined) {
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         configText,
      );
   }
   const env: Record<string, string> = {};
   for (const [key, value] of Object.entries({
      ...process.env,
      PUBLISHER_ROOT: serverRoot,
      PUBLISHER_HOST: "127.0.0.1",
      PUBLISHER_PORT: String(restPort),
      PUBLISHER_MCP_PORT: String(mcpPort),
      PUBLISHER_MCP_CONFIG_FILE_ENABLED: "false",
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
   let closed = false;
   proc.on("close", () => (closed = true));
   // A spawn that fails (ENOENT: no `bun` on PATH) emits only 'error'; it is
   // the process's end, so record it as one rather than wait out a timeout.
   proc.on("error", (error) => {
      all += `spawn failed: ${error.message}\n`;
      err += `spawn failed: ${error.message}\n`;
      if (exitCode === undefined) exitCode = null;
      closed = true;
   });
   const exited = () => exitCode !== undefined;
   const baseUrl = `http://127.0.0.1:${restPort}`;
   const tail = () => `\n--- server log tail ---\n${all.slice(-4000)}`;

   const server: SpawnedServer = {
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
         if (ownRoot) removeDir(serverRoot);
      },
   };
   return { server, drained: () => closed };
}
