// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import fs from "fs";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";

/**
 * Each setting a running server shows from outside, through the real entry
 * point: what it does when given (the happy case) and what a bad value does
 * (the sad case). The per-value cases are in src/settings/contract.spec.ts;
 * these prove the value reaches the listener, the header or the file it is
 * for.
 */

setDefaultTimeout(150_000);

describe("settings in a running server", () => {
   const servers: SpawnedServer[] = [];
   const cleanups: Array<() => void> = [];
   afterEach(async () => {
      while (servers.length) await servers.pop()!.stop();
      while (cleanups.length) cleanups.pop()!();
   });

   /** Boot and wait for the readiness line, without polling a route. */
   async function boot(
      options: Parameters<typeof spawnServer>[0] = {},
   ): Promise<SpawnedServer> {
      const server = await spawnServer(options);
      servers.push(server);
      const ready = () => server.stderr().includes("PUBLISHER_READY");
      await pollUntil(() => ready() || server.exited(), 120_000, 100);
      if (!ready()) {
         throw new Error(`server did not start:\n${server.output()}`);
      }
      return server;
   }

   /** Boot a server that must refuse to start; its stderr. */
   async function refused(
      options: Parameters<typeof spawnServer>[0],
   ): Promise<string> {
      const server = await spawnServer(options);
      servers.push(server);
      expect(await server.waitForExit()).not.toBe(0);
      return server.stderr();
   }

   describe("rateLimit", () => {
      it("answers 429 past the limit", async () => {
         const server = await boot({ env: { PUBLISHER_RATE_LIMIT: "2" } });
         const statuses: number[] = [];
         for (let i = 0; i < 3; i++) {
            statuses.push(
               (await fetch(`${server.baseUrl}/api/v0/status`)).status,
            );
         }
         expect(statuses).toEqual([200, 200, 429]);
      });

      it("refuses to start on a limit that is not a number", async () => {
         expect(
            await refused({ env: { PUBLISHER_RATE_LIMIT: "lots" } }),
         ).toContain(
            'rateLimit from PUBLISHER_RATE_LIMIT: expected an integer of at least 0, got "lots"',
         );
      });
   });

   describe("frameAncestors", () => {
      it("sends the configured policy", async () => {
         const server = await boot({
            env: { PUBLISHER_FRAME_ANCESTORS: "https://a.example" },
         });
         const res = await fetch(`${server.baseUrl}/api/v0/status`);
         expect(res.headers.get("content-security-policy")).toBe(
            "frame-ancestors https://a.example",
         );
      });

      it("sends 'self' when the value is only whitespace", async () => {
         const server = await boot({
            env: { PUBLISHER_FRAME_ANCESTORS: "   " },
         });
         const res = await fetch(`${server.baseUrl}/api/v0/status`);
         expect(res.headers.get("content-security-policy")).toBe(
            "frame-ancestors 'self'",
         );
      });
   });

   describe("mcp.corsOrigins", () => {
      const preflight = (server: SpawnedServer, origin: string) =>
         fetch(`http://127.0.0.1:${server.mcpPort}/mcp`, {
            method: "OPTIONS",
            headers: {
               Origin: origin,
               "Access-Control-Request-Method": "POST",
            },
         });

      it("lets a listed origin read MCP responses", async () => {
         const server = await boot({
            env: { MCP_CORS_ORIGINS: "https://a.example" },
         });
         const res = await preflight(server, "https://a.example");
         expect(res.headers.get("access-control-allow-origin")).toBe(
            "https://a.example",
         );
      });

      it("lets no origin read them when the list is empty", async () => {
         const server = await boot({ env: { MCP_CORS_ORIGINS: "," } });
         const res = await preflight(server, "https://a.example");
         expect(res.headers.get("access-control-allow-origin")).toBeNull();
      });
   });

   describe("frozenConfig", () => {
      it("is reported on /status when the file sets it", async () => {
         const server = await boot({
            config: { frozenConfig: true, environments: [] },
         });
         const body = (await (
            await fetch(`${server.baseUrl}/api/v0/status`)
         ).json()) as { frozenConfig: boolean };
         expect(body.frozenConfig).toBe(true);
      });

      it('keeps the string "false" freezing the server, and warns', async () => {
         const server = await boot({
            config: { frozenConfig: "false", environments: [] },
         });
         const body = (await (
            await fetch(`${server.baseUrl}/api/v0/status`)
         ).json()) as { frozenConfig: boolean };
         expect(body.frozenConfig).toBe(true);
         expect(server.output()).toContain('"frozenConfig": "false"');
         expect(server.output()).toContain("a later release refuses");
      });
   });

   describe("admin.port", () => {
      it("starts the admin listener on the port", async () => {
         const adminPort = await getFreePort();
         await boot({ env: { PUBLISHER_ADMIN_PORT: String(adminPort) } });
         const res = await fetch(
            `http://127.0.0.1:${adminPort}/health/liveness`,
         );
         expect(res.status).toBe(200);
      });

      it("refuses to start on a port that is not one", async () => {
         expect(
            await refused({ env: { PUBLISHER_ADMIN_PORT: "abc" } }),
         ).toContain(
            'admin.port from PUBLISHER_ADMIN_PORT: expected an integer from 0 to 65535, got "abc"',
         );
      });
   });

   describe("logging.level", () => {
      it("leaves out what is below the level", async () => {
         const server = await boot({ env: { LOG_LEVEL: "warn" } });
         expect(server.output()).not.toContain("Publisher server listening at");
      });

      it("warns about a level it does not know, and logs everything", async () => {
         const server = await boot({ env: { LOG_LEVEL: "loud" } });
         expect(server.output()).toContain(
            'logging.level from LOG_LEVEL: expected one of "error", "warn", "info", "verbose", "debug" or "silly", got "loud"',
         );
         expect(server.output()).toContain("Publisher server listening at");
      });
   });

   describe("mcp.configFileEnabled", () => {
      it("writes .mcp.json into the working directory by default", async () => {
         const cwd = tempDir("settings-mcp-cwd-");
         cleanups.push(cwd.cleanup);
         await boot({
            cwd: cwd.dir,
            env: { PUBLISHER_NO_MCP_CONFIG: undefined },
         });
         expect(
            await pollUntil(
               () => fs.existsSync(`${cwd.dir}/.mcp.json`),
               10_000,
               100,
            ),
         ).toBe(true);
      });

      it("refuses to start on a value that is not a boolean", async () => {
         expect(
            await refused({ env: { PUBLISHER_NO_MCP_CONFIG: "maybe" } }),
         ).toContain("mcp.configFileEnabled from PUBLISHER_NO_MCP_CONFIG");
      });
   });

   describe("initializeStorage", () => {
      it("keeps INITIALIZE_STORAGE=1 off, and warns", async () => {
         const server = await boot({ env: { INITIALIZE_STORAGE: "1" } });
         expect(server.output()).toContain(
            "INITIALIZE_STORAGE=1 is read as off, as it always has been",
         );
      });
   });

   describe("the config file", () => {
      it("names the key a typo meant", async () => {
         const server = await boot({
            config: { adminApiEnable: false, environments: [] },
         });
         expect(server.output()).toContain(
            'which no setting reads, so it is ignored. Did you mean "adminApiEnabled"?',
         );
      });
   });
});
