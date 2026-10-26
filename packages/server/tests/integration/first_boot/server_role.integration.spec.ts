// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * PUBLISHER_ROLE through the real entry point: `serve` refuses to start with an
 * admin setting, and `admin` starts only the admin listener, with no main API,
 * no MCP and no watch mode, and refuses a config that declares environments.
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

describe("PUBLISHER_ROLE", () => {
   const cleanups: Array<() => void> = [];
   afterEach(() => {
      while (cleanups.length) cleanups.pop()!();
   });

   async function boot(env: Record<string, string>, configText?: string) {
      const root = mkdtempSync(join(tmpdir(), "publisher-role-"));
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

   it("serve refuses to start with the admin port set", async () => {
      const { child } = await boot({
         PUBLISHER_ROLE: "serve",
         PUBLISHER_ADMIN_PORT: String(await freePort()),
      });
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain(
         "PUBLISHER_ROLE=serve never runs the admin API, but PUBLISHER_ADMIN_PORT is set.",
      );
   });

   it("admin serves only the admin listener", async () => {
      const port = await freePort();
      const { ports } = await boot({
         PUBLISHER_ROLE: "admin",
         PUBLISHER_ADMIN_PORT: String(port),
      });
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

   it("admin refuses a config that declares environments", async () => {
      const port = await freePort();
      const { child } = await boot(
         { PUBLISHER_ROLE: "admin", PUBLISHER_ADMIN_PORT: String(port) },
         JSON.stringify({
            frozenConfig: true,
            environments: [{ name: "sales", packages: [] }],
         }),
      );
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain(
         "PUBLISHER_ROLE=admin loads no environments, but the publisher config declares some.",
      );
   });

   it("admin refuses to start without the admin port", async () => {
      const { child } = await boot({ PUBLISHER_ROLE: "admin" });
      const { code, output } = await exitOf(child);
      expect(code).not.toBe(0);
      expect(output).toContain("PUBLISHER_ADMIN_PORT is required.");
   });
});
