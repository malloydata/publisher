/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

/**
 * End-to-end coverage for watch mode's source-edit live reload, plus the
 * copy-mode gate it depends on.
 *
 * Watch mode only recompiles source edits when the server is started with
 * `--watch-env` (i.e. `PUBLISHER_WATCH=<env>`): that flag mounts local-dir
 * packages as in-place symlinks instead of copies, so an edit to the source
 * directory is visible to the chokidar watcher, which recompiles just that
 * package via `Environment.getPackage(name, reload=true)`. Without the flag
 * packages stay copies and source edits do not propagate, even if a watcher is
 * started over REST: production keeps the decoupled copy semantics.
 *
 * That mount decision is made in the `EnvironmentStore` constructor, before any
 * package loads, and `POST /watch-mode/start` resolves the env from the on-disk
 * `publisher.config.json`. Neither can be driven through the shared in-process
 * REST harness (one cached server + store for the whole file), so this suite
 * boots dedicated server subprocesses from source, each with its own
 * `SERVER_ROOT`, optional `PUBLISHER_WATCH`, and a seeded config pointing at a
 * writable temp source package that the tests then edit.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// tests/integration/watch-mode -> packages/server
const SERVER_DIR = path.resolve(__dirname, "../../..");

const ENV_NAME = "watch-it-env";
const PKG_NAME = "watch-it-pkg";

interface TestServer {
   baseUrl: string;
   /** publisher_data/<env>/<pkg>: a symlink under watch mode, a real dir otherwise. */
   mountedPkgPath: string;
   /** Writable source package dir the tests mutate to simulate source edits. */
   srcDir: string;
   stop(): Promise<void>;
}

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

/** Returns true once `predicate` does, or false if `timeoutMs` elapses first. */
async function poll(
   predicate: () => Promise<boolean>,
   timeoutMs: number,
   intervalMs = 300,
): Promise<boolean> {
   const deadline = Date.now() + timeoutMs;
   while (Date.now() < deadline) {
      if (await predicate()) return true;
      await new Promise((r) => setTimeout(r, intervalMs));
   }
   return false;
}

const apiUrl = (baseUrl: string, sub: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG_NAME}${sub}`;

const modelsText = async (baseUrl: string): Promise<string> => {
   const res = await fetch(apiUrl(baseUrl, "/models"));
   return res.ok ? await res.text() : "";
};

/**
 * Boot a dedicated Publisher server subprocess from source against a seeded
 * config. With `watch: true` the env is passed via `PUBLISHER_WATCH`, enabling
 * the in-place symlink mount and auto-starting the watcher after env load.
 */
async function startServer(opts: { watch: boolean }): Promise<TestServer> {
   const srcDir = fs.mkdtempSync(path.join(os.tmpdir(), "wm-src-"));
   fs.writeFileSync(
      path.join(srcDir, "publisher.json"),
      JSON.stringify({ name: PKG_NAME, version: "1.0.0" }),
   );
   fs.writeFileSync(
      path.join(srcDir, "first.malloy"),
      'source: first_source is duckdb.sql("SELECT 1 as n")\n',
   );

   const serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "wm-root-"));
   fs.writeFileSync(
      path.join(serverRoot, "publisher.config.json"),
      JSON.stringify({
         frozenConfig: false,
         environments: [
            {
               name: ENV_NAME,
               packages: [{ name: PKG_NAME, location: srcDir }],
               connections: [],
            },
         ],
      }),
   );

   const port = await getFreePort();
   let mcpPort = await getFreePort();
   while (mcpPort === port) mcpPort = await getFreePort();
   const baseUrl = `http://127.0.0.1:${port}`;

   const env: NodeJS.ProcessEnv = {
      ...process.env,
      SERVER_ROOT: serverRoot,
      PUBLISHER_HOST: "127.0.0.1",
      PUBLISHER_PORT: String(port),
      MCP_PORT: String(mcpPort),
   };
   if (opts.watch) env.PUBLISHER_WATCH = ENV_NAME;

   // No `--watch` on bun, so there is no stray file watcher to clean up beyond
   // the server itself.
   const proc: ChildProcess = spawn("bun", ["src/server.ts"], {
      cwd: SERVER_DIR,
      env,
      stdio: ["ignore", "pipe", "pipe"],
   });
   let exited = false;
   let serverLog = "";
   const capture = (d: Buffer) => {
      serverLog = (serverLog + d.toString()).slice(-4000);
   };
   proc.stdout?.on("data", capture);
   proc.stderr?.on("data", capture);
   proc.on("exit", () => {
      exited = true;
   });

   const cleanup = () => {
      for (const dir of [srcDir, serverRoot]) {
         try {
            fs.rmSync(dir, { recursive: true, force: true });
         } catch {
            // best-effort cleanup
         }
      }
   };

   try {
      const ready = await poll(async () => {
         if (exited) throw new Error("server exited before becoming ready");
         try {
            return (await fetch(apiUrl(baseUrl, "/models"))).ok;
         } catch {
            return false;
         }
      }, 120_000);
      if (!ready) throw new Error("server did not become ready within 120s");
   } catch (err) {
      proc.kill("SIGKILL");
      cleanup();
      const reason = err instanceof Error ? err.message : String(err);
      throw new Error(`${reason}\n--- server log tail ---\n${serverLog}`);
   }

   const stop = async (): Promise<void> => {
      if (!exited) {
         await new Promise<void>((resolve) => {
            // Backstop in case SIGTERM is ignored; cleared once the process exits.
            const backstop = setTimeout(() => proc.kill("SIGKILL"), 5_000);
            proc.on("exit", () => {
               clearTimeout(backstop);
               resolve();
            });
            proc.kill("SIGTERM");
         });
      }
      cleanup();
   };

   return {
      baseUrl,
      mountedPkgPath: path.join(
         serverRoot,
         "publisher_data",
         ENV_NAME,
         PKG_NAME,
      ),
      srcDir,
      stop,
   };
}

describe("Watch-mode source-edit live reload via --watch-env (E2E)", () => {
   let server: TestServer | null = null;
   let baseUrl = "";

   beforeAll(async () => {
      server = await startServer({ watch: true });
      baseUrl = server.baseUrl;
   });

   afterAll(async () => {
      await server?.stop();
      server = null;
   });

   // ── in-place mount ────────────────────────────────────────────────

   it("mounts a local-dir package in place (resolving to the source)", () => {
      // A copy would resolve to a distinct path under publisher_data; an
      // in-place mount resolves back to the source dir. realpath covers both a
      // POSIX symlink and a Windows directory junction.
      expect(fs.realpathSync(server!.mountedPkgPath)).toBe(
         fs.realpathSync(server!.srcDir),
      );
      if (process.platform !== "win32") {
         expect(fs.lstatSync(server!.mountedPkgPath).isSymbolicLink()).toBe(
            true,
         );
      }
   });

   // ── status lifecycle / validation ─────────────────────────────────

   it("auto-starts watching the configured env and reports enabled", async () => {
      const res = await fetch(`${baseUrl}/api/v0/watch-mode/status`);
      expect(res.status).toBe(200);
      const status = (await res.json()) as {
         enabled: boolean;
         environmentName: string;
         watchingPath: string;
      };
      expect(status.enabled).toBe(true);
      expect(status.environmentName).toBe(ENV_NAME);
      expect(status.watchingPath).toBe(path.dirname(server!.mountedPkgPath));
   });

   it("accepts an explicit start for the already-watched env and stays enabled", async () => {
      const res = await fetch(`${baseUrl}/api/v0/watch-mode/start`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ environmentName: ENV_NAME }),
      });
      expect(res.status).toBe(200);
      // ensureWatching is idempotent; confirm the watcher is still live for our env.
      const status = (await (
         await fetch(`${baseUrl}/api/v0/watch-mode/status`)
      ).json()) as { enabled: boolean; environmentName: string };
      expect(status.enabled).toBe(true);
      expect(status.environmentName).toBe(ENV_NAME);
   });

   it("rejects start for an unknown environment with 404", async () => {
      const res = await fetch(`${baseUrl}/api/v0/watch-mode/start`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ environmentName: "does-not-exist" }),
      });
      expect(res.status).toBe(404);
   });

   it("rejects start for an unsafe environment name with 400", async () => {
      const res = await fetch(`${baseUrl}/api/v0/watch-mode/start`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ environmentName: "../etc" }),
      });
      expect(res.status).toBe(400);
   });

   // ── recompile propagation (the actual fix) ────────────────────────

   it("serves a newly added model after a source edit (reload)", async () => {
      const before = await modelsText(baseUrl);
      expect(before).toContain("first.malloy");
      expect(before).not.toContain("second.malloy");

      fs.writeFileSync(
         path.join(server!.srcDir, "second.malloy"),
         'source: second_source is duckdb.sql("SELECT 2 as n")\n',
      );

      const appeared = await poll(
         async () => (await modelsText(baseUrl)).includes("second.malloy"),
         25_000,
      );
      expect(appeared).toBe(true);
   });

   it("reflects an edit to an existing model's contents (recompile)", async () => {
      // Rename the source inside first.malloy; the compiled model detail must
      // pick up the new name, proving the package recompiled (not just a
      // directory re-scan).
      fs.writeFileSync(
         path.join(server!.srcDir, "first.malloy"),
         'source: renamed_source is duckdb.sql("SELECT 1 as n")\n',
      );

      const recompiled = await poll(async () => {
         const res = await fetch(apiUrl(baseUrl, "/models/first.malloy"));
         if (!res.ok) return false;
         // `sources` is the parsed source list ({ name, views }); `sourceInfos`
         // is a sibling array of JSON-encoded strings, the wrong shape to read
         // `.name` off of.
         const model = (await res.json()) as {
            sources?: Array<{ name?: string }>;
         };
         return (model.sources ?? []).some((s) => s.name === "renamed_source");
      }, 25_000);
      expect(recompiled).toBe(true);
   });

   // ── stop ──────────────────────────────────────────────────────────

   it("stops watching and reports disabled", async () => {
      const stop = await fetch(`${baseUrl}/api/v0/watch-mode/stop`, {
         method: "POST",
      });
      expect(stop.status).toBe(200);

      const status = (await (
         await fetch(`${baseUrl}/api/v0/watch-mode/status`)
      ).json()) as { enabled: boolean; watchingPath: string };
      expect(status.enabled).toBe(false);
      expect(status.watchingPath).toBe("");
   });
});

describe("Watch-mode copy semantics without --watch-env (E2E)", () => {
   let server: TestServer | null = null;
   let baseUrl = "";

   beforeAll(async () => {
      server = await startServer({ watch: false });
      baseUrl = server.baseUrl;
   });

   afterAll(async () => {
      await server?.stop();
      server = null;
   });

   it("mounts a local-dir package as a copy (decoupled from the source)", () => {
      // No --watch-env: the package is copied into publisher_data, so it is a
      // real directory and does not resolve back to the source.
      expect(fs.lstatSync(server!.mountedPkgPath).isSymbolicLink()).toBe(false);
      expect(fs.realpathSync(server!.mountedPkgPath)).not.toBe(
         fs.realpathSync(server!.srcDir),
      );
   });

   it("does not propagate a source edit even with a watcher started", async () => {
      // Starting the watcher succeeds (the env is in the config), but it watches
      // the copy, so editing the source never reaches what the server serves.
      const start = await fetch(`${baseUrl}/api/v0/watch-mode/start`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ environmentName: ENV_NAME }),
      });
      expect(start.status).toBe(200);

      fs.writeFileSync(
         path.join(server!.srcDir, "second.malloy"),
         'source: second_source is duckdb.sql("SELECT 2 as n")\n',
      );

      // Copy mode never propagates, so this should time out without appearing.
      const appeared = await poll(
         async () => (await modelsText(baseUrl)).includes("second.malloy"),
         6_000,
      );
      expect(appeared).toBe(false);
      expect(await modelsText(baseUrl)).toContain("first.malloy");
   });
});
