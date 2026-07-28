// Spawns the REAL Publisher server from source (`bun src/server.ts`) on isolated
// ports and an isolated server root, pointed at a generated config. Waits for
// operationalState === "serving". `PERSIST_STORAGE_MODE` is read at startup, so
// each mode needs its own server instance.

import path from "path";
import { existsSync } from "fs";
import type { Subprocess } from "bun";
import { log, run, runOrThrow, sleep, waitFor } from "./util";

/**
 * Build the server (server-only: API + baked DuckDB extensions, no SPA) so the
 * spawned `dist/server.mjs` has the ducklake/postgres_scanner extensions baked in
 * rather than autoinstalling them at first attach. Skipped when a dist already
 * exists unless `force`.
 */
export async function buildServerIfNeeded(
   repoRoot: string,
   force = false,
): Promise<void> {
   const serverDir = path.join(repoRoot, "packages", "server");
   const distEntry = path.join(serverDir, "dist", "server.mjs");
   if (!force && existsSync(distEntry)) {
      log.ok(`reusing existing server build (${distEntry})`);
      return;
   }
   log.step("building server (bun run build:server-only) — one-time, ~1-2 min");
   await runOrThrow(["bun", "run", "build:server-only"], { cwd: serverDir });
   if (!existsSync(distEntry)) {
      throw new Error(`build did not produce ${distEntry}`);
   }
   log.ok("server build complete");
}

export type PersistStorageMode = "off" | "write-only" | "on";

export interface ServerOptions {
   repoRoot: string;
   serverRoot: string;
   configPath: string;
   port: number;
   mcpPort: number;
   mode: PersistStorageMode;
   /**
    * Extra environment variables for the spawned server, beyond
    * `PERSIST_STORAGE_MODE` (which is set from `mode`). Lets a scenario boot a
    * publisher with a deployment flag like `PERSIST_COLLISION_ENFORCE=true` that,
    * like the mode, is fixed at process start.
    */
   extraEnv?: Record<string, string>;
   /** Wipe + re-sync storage from config on boot (default true). */
   init?: boolean;
   /** Pipe server stdout/stderr through (default false — captured to a log file). */
   inheritStdio?: boolean;
   logFile?: string;
}

export interface ServerHandle {
   baseUrl: string;
   mcpUrl: string;
   mode: PersistStorageMode;
   stop(): Promise<void>;
}

export async function startServer(opts: ServerOptions): Promise<ServerHandle> {
   const serverDir = path.join(opts.repoRoot, "packages", "server");
   const baseUrl = `http://127.0.0.1:${opts.port}`;
   const mcpUrl = `http://127.0.0.1:${opts.mcpPort}/mcp`;

   const args = [
      "bun",
      "run",
      "./dist/server.mjs",
      "--server_root",
      opts.serverRoot,
      "--config",
      opts.configPath,
      "--port",
      String(opts.port),
      "--mcp_port",
      String(opts.mcpPort),
   ];
   if (opts.init !== false) args.push("--init");

   log.step(
      `starting Publisher server (mode=${opts.mode}) on ${baseUrl} [mcp ${opts.mcpPort}]`,
   );

   const logSink = opts.logFile ? Bun.file(opts.logFile).writer() : null;
   const proc: Subprocess = Bun.spawn(args, {
      cwd: serverDir,
      env: {
         ...process.env,
         NODE_ENV: "production",
         PERSIST_STORAGE_MODE: opts.mode,
         PUBLISHER_HOST: "127.0.0.1",
         ...opts.extraEnv,
      },
      stdout: opts.inheritStdio ? "inherit" : "pipe",
      stderr: opts.inheritStdio ? "inherit" : "pipe",
   });

   // The server prints one PUBLISHER_READY line the moment it reaches `serving`
   // (PUBLISHER_INIT_FAILED instead if initialization failed). Watching for it is
   // exact, where polling /status can only resolve to its own interval — and since
   // every mode switch costs a boot, that interval was a real share of a run.
   let signalReady: (() => void) | undefined;
   let signalFailed: ((why: string) => void) | undefined;
   const readySignal = new Promise<void>((resolve, reject) => {
      signalReady = resolve;
      signalFailed = (why): void => reject(new Error(why));
   });

   // Drain captured output to the log file so a hang/crash is diagnosable.
   if (!opts.inheritStdio && logSink) {
      let tail = "";
      const pump = async (stream: ReadableStream): Promise<void> => {
         for await (const chunk of stream as unknown as AsyncIterable<Uint8Array>) {
            logSink.write(chunk);
            logSink.flush();
            // The marker can straddle a chunk boundary, so keep a short tail.
            tail = (tail + new TextDecoder().decode(chunk)).slice(-4096);
            if (tail.includes("PUBLISHER_READY")) signalReady?.();
            else if (tail.includes("PUBLISHER_INIT_FAILED")) {
               signalFailed?.(
                  `server reported PUBLISHER_INIT_FAILED (see ${opts.logFile ?? "stdio"})`,
               );
            }
         }
      };
      if (proc.stdout instanceof ReadableStream) void pump(proc.stdout);
      if (proc.stderr instanceof ReadableStream) void pump(proc.stderr);
   }

   let exited = false;
   void proc.exited.then(() => {
      exited = true;
   });

   const stop = async (): Promise<void> => {
      if (!exited) {
         proc.kill("SIGTERM");
         const killed = await Promise.race([
            proc.exited.then(() => true),
            sleep(8000).then(() => false),
         ]);
         if (!killed) proc.kill("SIGKILL");
      }
      await logSink?.end();
   };

   // The poll is the backstop: it covers inheritStdio (nothing to watch) and any
   // build predating the ready line. Whichever resolves first wins.
   const poll = waitFor(
      "server operationalState=serving",
      async () => {
         if (exited)
            throw new Error(
               `server process exited early (see ${opts.logFile ?? "stdio"})`,
            );
         const res = await fetch(`${baseUrl}/api/v0/status`);
         if (!res.ok) return false;
         const body = (await res.json()) as { operationalState?: string };
         return body.operationalState === "serving";
      },
      { timeoutMs: 180_000, intervalMs: 750 },
   );
   try {
      await Promise.race([readySignal, poll]);
   } catch (e) {
      await stop();
      throw e;
   }
   // Keep the loser from surfacing as an unhandled rejection after we've moved on.
   void poll.catch(() => undefined);

   log.ok(`server serving (mode=${opts.mode})`);
   return { baseUrl, mcpUrl, mode: opts.mode, stop };
}
