// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `servingApiEnabled` and `adminApiEnabled` through the real entry point: with
 * the admin API off an admin setting refuses to start, and with serving off only
 * the admin listener starts, with no main API, no MCP and no watch mode, and a
 * config that declares environments refuses to start.
 */

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { createServer } from "net";
import { tmpdir } from "os";
import { join } from "path";

const SERVER_ENTRY = join(import.meta.dir, "../../../src/server.ts");

async function freePort(): Promise<number> {
   return new Promise((resolve, reject) => {
      const probe = createServer();
      probe.once("error", reject);
      probe.listen(0, "127.0.0.1", () => {
         const { port } = probe.address() as { port: number };
         probe.close(() => resolve(port));
      });
   });
}

async function answers(url: string): Promise<number | null> {
   try {
      return (await fetch(url)).status;
   } catch {
      return null;
   }
}

describe("servingApiEnabled and adminApiEnabled", () => {
   const cleanups: Array<() => void> = [];
   afterEach(() => {
      while (cleanups.length) cleanups.pop()!();
   });

   async function boot(env: Record<string, string>, configText?: string) {
      const root = mkdtempSync(join(tmpdir(), "publisher-apis-"));
      if (configText !== undefined) {
         writeFileSync(join(root, "publisher.config.json"), configText);
      }
      const ports = { main: await freePort(), mcp: await freePort() };
      const child = Bun.spawn(["bun", SERVER_ENTRY], {
         env: {
            ...process.env,
            SERVER_ROOT: root,
            PUBLISHER_PORT: String(ports.main),
            MCP_PORT: String(ports.mcp),
            PUBLISHER_NO_MCP_CONFIG: "true",
            ...env,
         },
         stdout: "pipe",
         stderr: "pipe",
      });
      cleanups.push(() => {
         child.kill();
         rmSync(root, { recursive: true, force: true });
      });
      return { child, ports };
   }

   // A refusal in server.ts reaches stderr; one after the logger loads is logged
   // as an uncaught exception, to stdout.
   async function exitOf(child: Bun.Subprocess<"ignore", "pipe", "pipe">) {
      const code = await child.exited;
      const out = await new Response(child.stdout).text();
      const err = await new Response(child.stderr).text();
      return { code, output: err + out };
   }

   it("refuses to start with the admin API off and its port set", async () => {
      const { child } = await boot(
         { PUBLISHER_ADMIN_PORT: String(await freePort()) },
         JSON.stringify({ adminApiEnabled: false, environments: [] }),
      );
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain(
         "adminApiEnabled is false, but PUBLISHER_ADMIN_PORT is set.",
      );
   });

   it("with serving off, serves only the admin listener", async () => {
      const port = await freePort();
      const { ports } = await boot(
         { PUBLISHER_ADMIN_PORT: String(port) },
         JSON.stringify({ servingApiEnabled: false }),
      );
      const admin = `http://127.0.0.1:${port}`;
      const deadline = Date.now() + 30_000;
      while ((await answers(`${admin}/health/readiness`)) !== 200) {
         if (Date.now() > deadline) throw new Error("admin never became ready");
         await Bun.sleep(250);
      }
      // No catalog database is configured, so the catalog routes say so.
      expect(await answers(`${admin}/api/v0/catalogs`)).toBe(503);
      // No environments to watch, so no watch-mode resource.
      expect(await answers(`${admin}/api/v0/watch-mode`)).toBe(404);
      expect(await answers(`http://127.0.0.1:${ports.main}/health`)).toBeNull();
      expect(await answers(`http://127.0.0.1:${ports.mcp}/mcp`)).toBeNull();
   });

   it("with serving off, refuses a config that declares environments", async () => {
      const { child } = await boot(
         { PUBLISHER_ADMIN_PORT: String(await freePort()) },
         JSON.stringify({
            servingApiEnabled: false,
            environments: [{ name: "sales", packages: [] }],
         }),
      );
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain(
         "servingApiEnabled is false, so no environments are loaded, but the config declares some.",
      );
   });

   it("with serving off, refuses to start without the admin port", async () => {
      const { child } = await boot(
         {},
         JSON.stringify({ servingApiEnabled: false }),
      );
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain("PUBLISHER_ADMIN_PORT is required.");
   });

   it("takes serving off from the variable, over a file that leaves it on", async () => {
      const port = await freePort();
      const { ports } = await boot(
         {
            PUBLISHER_ADMIN_PORT: String(port),
            PUBLISHER_SERVING_API_ENABLED: "false",
         },
         JSON.stringify({ servingApiEnabled: true }),
      );
      const deadline = Date.now() + 30_000;
      while (
         (await answers(`http://127.0.0.1:${port}/health/readiness`)) !== 200
      ) {
         if (Date.now() > deadline) throw new Error("admin never became ready");
         await Bun.sleep(250);
      }
      expect(await answers(`http://127.0.0.1:${ports.main}/health`)).toBeNull();
   });

   it("refuses a flag that is not a boolean", async () => {
      const { child } = await boot(
         {},
         JSON.stringify({ adminApiEnabled: "no", environments: [] }),
      );
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain("adminApiEnabled from ");
      expect(output).toContain(
         'expected a boolean (true/false, 1/0, yes/no or on/off), got "no"',
      );
   });
});
