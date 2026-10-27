// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import path from "path";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   type SpawnedServer,
} from "../../harness/spawn_server";

/**
 * The settings rows only a running server can show: that the flags
 * `server.ts` parses reach the listeners, ahead of their variables, what it
 * says about a flag it does not know, and that a bad value stops it before it
 * binds. The per-setting cases are in src/settings/contract.spec.ts.
 */

setDefaultTimeout(150_000);

describe("settings at startup", () => {
   let server: SpawnedServer | undefined;
   afterEach(async () => {
      await server?.stop();
      server = undefined;
   });

   const readyLine = () =>
      server
         ?.stderr()
         .split("\n")
         .find((line) => line.startsWith("PUBLISHER_READY"));

   async function waitForReady(): Promise<string> {
      await pollUntil(
         () => readyLine() !== undefined || server!.exited(),
         120_000,
         100,
      );
      const line = readyLine();
      if (!line) {
         throw new Error(`no PUBLISHER_READY line:\n${server!.output()}`);
      }
      return line;
   }

   it("binds the ports the flags name, over their variables", async () => {
      const flagPort = await getFreePort();
      let flagMcpPort = await getFreePort();
      while (flagMcpPort === flagPort) flagMcpPort = await getFreePort();
      server = await spawnServer({
         args: ["--port", String(flagPort), "--mcp_port", String(flagMcpPort)],
      });

      const line = await waitForReady();
      expect(line).toContain(`url=http://127.0.0.1:${flagPort} `);
      expect(line).toContain(`mcp=http://127.0.0.1:${flagMcpPort} `);
      const status = await fetch(`http://127.0.0.1:${flagPort}/api/v0/status`);
      expect(status.ok).toBe(true);
      // The startup line names what was given and where it came from.
      expect(server.output()).toContain("Settings not at their defaults: ");
      expect(server.output()).toContain(`port=${flagPort} from --port`);
      expect(server.output()).toContain(
         `mcp.port=${flagMcpPort} from --mcp_port`,
      );
   });

   it("warns about a flag it does not know and a flag missing its value, and starts", async () => {
      server = await spawnServer({
         // A misspelled flag, then a known flag with nothing after it.
         args: ["--mcp_prot", "1", "--admin_port"],
      });

      const line = await waitForReady();
      expect(line).toContain(`mcp=http://127.0.0.1:${server.mcpPort} `);
      await server.waitUntilServing();
      expect(server.output()).toContain(
         "Unknown flag --mcp_prot, so it is ignored. Did you mean --mcp-port?",
      );
      expect(server.output()).toContain(
         "--admin_port has no value after it, so it is ignored.",
      );
   });

   it("fails startup on bad values, naming every one, before anything binds", async () => {
      server = await spawnServer({
         env: { MCP_PORT: "40a0", PUBLISHER_MAX_QUERY_ROWS: "-1" },
      });

      expect(await server.waitForExit()).not.toBe(0);
      const stderr = server.stderr();
      expect(stderr).toContain(
         'mcp.port from MCP_PORT: expected an integer from 0 to 65535, got "40a0"',
      );
      expect(stderr).toContain(
         'query.maxRows from PUBLISHER_MAX_QUERY_ROWS: expected an integer of at least 0, got "-1"',
      );
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
      expect(stderr).not.toContain("PUBLISHER_READY");
   });

   it("reports a bad value of a setting the logger reads with the others, not as a crash", async () => {
      // The logger reads logging.responseBodiesEnabled when it loads, so the
      // report has to come first.
      server = await spawnServer({
         env: {
            DISABLE_RESPONSE_LOGGING: "enabled",
            PUBLISHER_RATE_LIMIT: "abc",
         },
      });

      expect(await server.waitForExit()).not.toBe(0);
      // stderr arrives on its own pipe, so it can trail the exit.
      expect(
         await pollUntil(
            () => server!.stderr().includes("PUBLISHER_INIT_FAILED"),
            10_000,
         ),
      ).toBe(true);
      const stderr = server.stderr();
      expect(stderr).toContain(
         'logging.responseBodiesEnabled from DISABLE_RESPONSE_LOGGING: expected a boolean (true/false, 1/0, yes/no or on/off), got "enabled"',
      );
      expect(stderr).toContain(
         'rateLimit from PUBLISHER_RATE_LIMIT: expected an integer of at least 0, got "abc"',
      );
      expect(stderr).not.toContain("SettingError:");
   });
});

describe("--help from the real entry point", () => {
   const ENTRY = path.join(import.meta.dir, "../../../src/server.ts");

   for (const flag of ["--help", "-h"]) {
      it(`${flag} prints every flag and exits 0 without starting`, () => {
         const run = Bun.spawnSync(["bun", ENTRY, "--port", "1", flag], {
            env: { ...process.env, NODE_ENV: "test" },
         });
         expect(run.exitCode).toBe(0);
         const out = run.stdout.toString();
         expect(out).toContain("Usage: malloy-publisher [options]");
         expect(out).toContain(
            "--serving-api-enabled, --no-serving-api-enabled",
         );
         expect(out).toContain("--admin-port <n>");
         expect(out).toContain("(was --admin_port)");
         expect(out).not.toContain("PUBLISHER_READY");
      });
   }

   it("does not print help for a flag that merely starts like it", () => {
      const run = Bun.spawnSync(["bun", ENTRY, "--helpme", "--port", "40a0"], {
         env: { ...process.env, NODE_ENV: "test" },
      });
      expect(run.exitCode).not.toBe(0);
      expect(run.stdout.toString()).not.toContain("Usage: malloy-publisher");
      expect(run.stderr.toString()).toContain("port from --port");
   });
});
