/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

/**
 * End-to-end coverage for the PUBLISHER_READY line: the contract scripts grep
 * for instead of polling /api/v0/status. It must appear on stderr exactly
 * once, only when the server is actually serving, and carry the environment,
 * package, and load-error counts. That contract spans process boundaries
 * (a real stderr stream from a real boot), so it needs a dedicated server
 * subprocess rather than the in-process REST harness.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// tests/integration/first_boot -> packages/server
const SERVER_DIR = path.resolve(__dirname, "../../..");

const ENV_NAME = "ready-env";
const PKG_NAME = "ready-pkg";

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

describe("first-boot readiness line", () => {
   let proc: ChildProcess | undefined;
   let exited = false;
   let stderrLog = "";
   let combinedLog = "";
   let srcDir = "";
   let serverRoot = "";
   let port = 0;

   const readyLines = () =>
      stderrLog
         .split("\n")
         .filter((line) => line.startsWith("PUBLISHER_READY"));

   beforeAll(async () => {
      srcDir = fs.mkdtempSync(path.join(os.tmpdir(), "ready-src-"));
      fs.writeFileSync(
         path.join(srcDir, "publisher.json"),
         JSON.stringify({ name: PKG_NAME }),
      );
      fs.writeFileSync(
         path.join(srcDir, "first.malloy"),
         'source: first_source is duckdb.sql("SELECT 1 as n")\n',
      );

      serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "ready-root-"));
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

      port = await getFreePort();
      let mcpPort = await getFreePort();
      while (mcpPort === port) mcpPort = await getFreePort();

      proc = spawn("bun", ["src/server.ts"], {
         cwd: SERVER_DIR,
         env: {
            ...process.env,
            SERVER_ROOT: serverRoot,
            PUBLISHER_HOST: "127.0.0.1",
            PUBLISHER_PORT: String(port),
            MCP_PORT: String(mcpPort),
         },
         stdio: ["ignore", "pipe", "pipe"],
      });
      proc.stdout?.on("data", (d: Buffer) => {
         combinedLog = (combinedLog + d.toString()).slice(-8000);
      });
      proc.stderr?.on("data", (d: Buffer) => {
         stderrLog += d.toString();
         combinedLog = (combinedLog + d.toString()).slice(-8000);
      });
      proc.on("exit", () => {
         exited = true;
      });

      const serving = await poll(async () => {
         if (exited) throw new Error("server exited before becoming ready");
         try {
            const res = await fetch(`http://127.0.0.1:${port}/api/v0/status`);
            if (!res.ok) return false;
            const body = (await res.json()) as { operationalState?: string };
            return body.operationalState === "serving";
         } catch {
            return false;
         }
      }, 120_000);
      if (!serving) {
         throw new Error(
            `server did not reach serving within 120s\n--- log tail ---\n${combinedLog}`,
         );
      }
      // The line is written in the same tick that flips the status to
      // serving, but the pipe delivery can trail the HTTP response.
      await poll(async () => readyLines().length > 0, 10_000, 100);
   }, 150_000);

   afterAll(async () => {
      if (proc && !exited) {
         await new Promise<void>((resolve) => {
            // Backstop in case SIGTERM is ignored; cleared once the process exits.
            const backstop = setTimeout(() => proc?.kill("SIGKILL"), 5_000);
            proc?.on("exit", () => {
               clearTimeout(backstop);
               resolve();
            });
            proc?.kill("SIGTERM");
         });
      }
      for (const dir of [srcDir, serverRoot]) {
         try {
            fs.rmSync(dir, { recursive: true, force: true });
         } catch {
            // best-effort cleanup
         }
      }
   });

   it("prints one PUBLISHER_READY line on stderr once serving", () => {
      const lines = readyLines();
      expect(lines).toHaveLength(1);
      // The URL host/port are the ones the server was actually started with,
      // so a script can dial what the line says.
      expect(lines[0]).toMatch(
         new RegExp(
            `^PUBLISHER_READY url=http://127\\.0\\.0\\.1:${port} ` +
               `mcp=http://127\\.0\\.0\\.1:\\d+ ` +
               `environments=1 packages=1 load_errors=0$`,
         ),
      );
   });
});
