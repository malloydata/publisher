// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A connection type registered by a preloaded module, end to end: the server
 * boots with PUBLISHER_PRELOAD_MODULES naming a module that registers
 * `probe_duckdb` (tests/fixtures/preload/probe_duckdb.mjs), an environment
 * configures a connection of that type through `pluginConnection`, a package
 * queries it, and the connection's credential never comes back out.
 *
 * The server is a child process rather than the in-process app: the preload
 * has to happen before the server's own modules run, which only a fresh
 * process can show.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type ChildProcess, spawn } from "child_process";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SERVER_DIR = path.resolve(__dirname, "../../..");
const FIXTURE = path.resolve(
   SERVER_DIR,
   "tests/fixtures/preload/probe_duckdb.mjs",
);
const ENV_NAME = "plugin-env";
const PKG_NAME = "plugin-pkg";
const CONN_NAME = "probe";
const SECRET = "S3NT1NEL-plugin-token-do-not-return";

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

describe("a connection type registered by a preloaded module", () => {
   let proc: ChildProcess | undefined;
   let combinedLog = "";
   let port = 0;
   const url = (suffix: string) => `http://127.0.0.1:${port}${suffix}`;

   beforeAll(async () => {
      const srcDir = fs.mkdtempSync(path.join(os.tmpdir(), "plugin-src-"));
      fs.writeFileSync(
         path.join(srcDir, "publisher.json"),
         JSON.stringify({ name: PKG_NAME }),
      );
      fs.writeFileSync(
         path.join(srcDir, "index.malloy"),
         `source: nums is ${CONN_NAME}.sql("""SELECT 1 AS n UNION ALL SELECT 2""") extend {\n` +
            "  measure: total is n.sum()\n" +
            "}\n",
      );
      const serverRoot = fs.mkdtempSync(path.join(os.tmpdir(), "plugin-root-"));
      fs.writeFileSync(
         path.join(serverRoot, "publisher.config.json"),
         JSON.stringify({
            frozenConfig: false,
            environments: [
               {
                  name: ENV_NAME,
                  packages: [{ name: PKG_NAME, location: srcDir }],
                  connections: [
                     {
                        name: CONN_NAME,
                        type: "probe_duckdb",
                        pluginConnection: { secretToken: SECRET },
                     },
                  ],
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
            PUBLISHER_PRELOAD_MODULES: FIXTURE,
            PUBLISHER_NO_MCP_CONFIG: "1",
         },
         stdio: ["ignore", "pipe", "pipe"],
      });
      proc.stdout?.on("data", (d: Buffer) => {
         combinedLog += d.toString();
      });
      proc.stderr?.on("data", (d: Buffer) => {
         combinedLog += d.toString();
      });

      const serving = await poll(async () => {
         try {
            const res = await fetch(url("/api/v0/status"));
            return res.ok;
         } catch {
            return false;
         }
      }, 120_000);
      if (!serving) {
         throw new Error(`server never came up:\n${combinedLog}`);
      }
      const loaded = await poll(async () => {
         const res = await fetch(
            url(`/api/v0/environments/${ENV_NAME}/packages/${PKG_NAME}`),
         );
         return res.ok;
      }, 120_000);
      if (!loaded) {
         throw new Error(`package never loaded:\n${combinedLog}`);
      }
   }, 240_000);

   afterAll(async () => {
      if (proc && proc.exitCode === null) {
         proc.kill("SIGTERM");
         await poll(async () => proc?.exitCode !== null, 10_000);
         if (proc.exitCode === null) proc.kill("SIGKILL");
      }
   });

   it("reports the module and the type it added at boot", () => {
      expect(combinedLog).toContain("Preloaded module");
      expect(combinedLog).toContain("probe_duckdb");
   });

   it("answers a query through the connection", async () => {
      const res = await fetch(
         url(
            `/api/v0/environments/${ENV_NAME}/packages/${PKG_NAME}/models/index.malloy/query`,
         ),
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               query: "run: nums -> { aggregate: total }",
            }),
         },
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      const result = JSON.parse(body.result) as {
         data: {
            array_value: {
               record_value: { number_value: number }[];
            }[];
         };
      };
      expect(result.data.array_value[0].record_value[0].number_value).toBe(3);
   });

   it("lists the connection with its credential withheld", async () => {
      const res = await fetch(
         url(`/api/v0/environments/${ENV_NAME}/connections/${CONN_NAME}`),
      );
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).not.toContain(SECRET);
      const connection = JSON.parse(text) as {
         type: string;
         withheldFields?: string[];
      };
      expect(connection.type).toBe("probe_duckdb");
      expect(connection.withheldFields).toContain(
         "pluginConnection.secretToken",
      );
   });

   it("says why schema browsing is unavailable rather than failing anonymously", async () => {
      const res = await fetch(
         url(
            `/api/v0/environments/${ENV_NAME}/connections/${CONN_NAME}/schemas`,
         ),
      );
      expect(res.status).toBe(501);
      expect(await res.text()).toContain(
         "Schema browsing is not available for connection type 'probe_duckdb'",
      );
   });
});
