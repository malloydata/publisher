// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import fs from "fs";
import path from "path";
import {
   getFreePort,
   pollUntil,
   singlePackageConfig,
   spawnServer,
   tempDir,
   writeSourcePackage,
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

   /** What /status reports about package versions. */
   async function versionStatus(server: SpawnedServer) {
      return (await (
         await fetch(`${server.baseUrl}/api/v0/status`)
      ).json()) as { versionPromotion?: string };
   }

   describe("versionPromotion", () => {
      it("is reported on /status from the file, and the variable wins over it", async () => {
         const config = { versionPromotion: "explicit", environments: [] };
         const fromFile = await boot({
            config,
            env: { PUBLISHER_VERSION_PROMOTION: undefined },
         });
         expect((await versionStatus(fromFile)).versionPromotion).toBe(
            "explicit",
         );
         const overridden = await boot({
            config,
            env: { PUBLISHER_VERSION_PROMOTION: "on-publish" },
         });
         expect((await versionStatus(overridden)).versionPromotion).toBe(
            "on-publish",
         );
      });

      it("refuses to start on a mode outside its set in the file", async () => {
         expect(
            await refused({
               config: { versionPromotion: "manual", environments: [] },
               env: { PUBLISHER_VERSION_PROMOTION: undefined },
            }),
         ).toContain(
            'expected one of "on-publish" or "explicit", got "manual"',
         );
      });
   });

   describe("packageInlineFields", () => {
      it("is logged at startup, in any case", async () => {
         const server = await boot({
            env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "Off" },
         });
         expect(server.output()).toContain(
            "Package inline fields (PUBLISHER_PACKAGE_INLINE_FIELDS): off",
         );
      });

      it("refuses to start on a value other than on or off", async () => {
         expect(
            await refused({
               env: { PUBLISHER_PACKAGE_INLINE_FIELDS: "false" },
            }),
         ).toContain(
            'packageInlineFields from PUBLISHER_PACKAGE_INLINE_FIELDS: expected one of "off" or "on", got "false"',
         );
      });
   });

   describe("modelPage", () => {
      it("is reported on /status, in any case", async () => {
         const server = await boot({ env: { PUBLISHER_MODEL_PAGE: "Viewer" } });
         const body = (await (
            await fetch(`${server.baseUrl}/api/v0/status`)
         ).json()) as { modelPage?: string };
         expect(body.modelPage).toBe("viewer");
      });

      it("refuses to start on a page outside its set", async () => {
         expect(
            await refused({ env: { PUBLISHER_MODEL_PAGE: "both" } }),
         ).toContain(
            'modelPage from PUBLISHER_MODEL_PAGE: expected one of "explorer" or "viewer", got "both"',
         );
      });
   });

   describe("callerHeader", () => {
      /** Boot serving one package, query it once with `headers`, and return the log. */
      async function queried(
         env: Record<string, string>,
         headers: Record<string, string>,
      ): Promise<string> {
         const pkg = writeSourcePackage("callers");
         cleanups.push(pkg.cleanup);
         // A configured package is published as the version its
         // publisher.json declares, so it needs one to load.
         fs.writeFileSync(
            path.join(pkg.dir, "publisher.json"),
            JSON.stringify({ name: "callers", version: "1.0.0" }),
         );
         const server = await boot({
            config: singlePackageConfig("e", {
               name: "callers",
               location: pkg.dir,
            }),
            env,
         });
         await server.waitUntilServing();
         const res = await fetch(
            `${server.baseUrl}/api/v0/environments/e/packages/callers/models/first.malloy/query`,
            {
               method: "POST",
               headers: { "content-type": "application/json", ...headers },
               body: JSON.stringify({
                  query: "run: first_source -> { select: n }",
               }),
            },
         );
         expect(res.status).toBe(200);
         await pollUntil(
            () => server.output().includes("Access record"),
            10_000,
            100,
         );
         return server.output();
      }

      it("copies the configured header into the access record", async () => {
         const log = await queried(
            { PUBLISHER_CALLER_HEADER: "X-Gateway-User" },
            { "x-gateway-user": "alice" },
         );
         expect(log).toContain('"caller":"alice"');
      });

      it("reads only the configured header, not the default one", async () => {
         const log = await queried(
            { PUBLISHER_CALLER_HEADER: "x-gateway-user" },
            { "x-publisher-caller": "bob" },
         );
         expect(log).toContain("Access record");
         expect(log).not.toContain('"caller":"bob"');
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
