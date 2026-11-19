// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `--check` end to end: a real `bun src/server.ts --check` process against a
 * real config, for the contract CI reads: the exit code (0 clean, 1 problems,
 * 2 could not check), stdout holding finding lines and nothing else, and the
 * summary line on stderr. Also that it reads the config as written, never
 * stored state or the bundled samples, and never writes the server root.
 */

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import fs from "fs";
import net from "net";
import os from "os";
import path from "path";
import {
   getFreePort,
   pollUntil,
   spawnServer,
   type SpawnedServer,
   tempDir,
   writeSourcePackage,
} from "../../harness/spawn_server";

setDefaultTimeout(150_000);

const ENV = "check-env";
const GOOD = 'source: good is duckdb.sql("SELECT 1 AS n")\n';
// Two models that each fail, so the report must not stop at the first.
const BROKEN = {
   "a.malloy":
      'source: a is duckdb.sql("SELECT 1 AS n") extend {\n  measure: c is count(\n}\n',
   "b.malloy":
      'source: b is duckdb.sql("SELECT 1 AS n") extend { dimension: y is nope + 1 }\n',
};

const config = (packages: { name: string; location: string }[]) => ({
   frozenConfig: false,
   environments: [{ name: ENV, packages, connections: [] }],
});

/** A finding line, or (under GitHub Actions) its workflow command. */
const FINDING = /^(\S.*: (error|warning): .+|::(error|warning)( [^:]*)?::.+)$/;

describe("check mode", () => {
   let server: SpawnedServer | undefined;
   const cleanups: (() => void)[] = [];

   afterEach(async () => {
      await server?.stop();
      server = undefined;
      while (cleanups.length > 0) cleanups.pop()!();
   });

   const source = (name: string, files: Record<string, string>) => {
      const pkg = writeSourcePackage(name, files);
      cleanups.push(pkg.cleanup);
      return pkg.dir;
   };

   const check = async (
      options: Parameters<typeof spawnServer>[0] = {},
   ): Promise<{ code: number | null; stdout: string; stderr: string }> => {
      await server?.stop();
      server = await spawnServer({
         ...options,
         // A CI runner sets it; these cases read the plain lines.
         env: { GITHUB_ACTIONS: undefined, ...options.env },
         args: ["--check", ...(options.args ?? [])],
      });
      const code = await server.waitForExit(120_000);
      return { code, stdout: server.stdout(), stderr: server.stderr() };
   };

   const summary = (stderr: string) =>
      stderr.split("\n").filter((line) => line.startsWith("PUBLISHER_CHECK"));

   /** stdout is findings and nothing else: no log line, no colour. */
   const findingsOnly = (stdout: string) => {
      const lines = stdout.split("\n").filter((line) => line.length > 0);
      for (const line of lines) expect(line).toMatch(FINDING);
      expect(stdout).not.toContain("\u001b[");
      return lines;
   };

   it("exits 1 and prints every model's errors with their locations", async () => {
      const good = source("good", { "good.malloy": GOOD });
      const broken = source("broken", BROKEN);
      const { code, stdout, stderr } = await check({
         config: config([
            { name: "good", location: good },
            { name: "broken", location: broken },
         ]),
      });

      expect(code).toBe(1);
      expect(findingsOnly(stdout)).toEqual([
         expect.stringMatching(
            new RegExp(
               `^${escape(path.join(broken, "a.malloy"))}:3:1: error: unexpected '}'.* \\[${ENV}/broken\\]$`,
            ),
         ),
         expect.stringMatching(
            new RegExp(
               `^${escape(path.join(broken, "b.malloy"))}:1:\\d+: error: 'nope' is not defined.* \\[${ENV}/broken\\]$`,
            ),
         ),
      ]);
      expect(summary(stderr)).toEqual([
         "PUBLISHER_CHECK_FAILED environments=1 packages=1 load_errors=1 errors=2 warnings=0",
      ]);
      // It checks, then exits: no readiness line, no listener.
      expect(stderr).not.toContain("PUBLISHER_READY");
      expect(server!.output()).not.toContain("listening at");
   });

   it("exits 0 when every package loads, and writes nothing into the server root", async () => {
      const good = source("good", { "good.malloy": GOOD });
      const { code, stdout, stderr } = await check({
         config: config([{ name: "good", location: good }]),
      });

      expect(code).toBe(0);
      expect(findingsOnly(stdout)).toEqual([]);
      expect(summary(stderr)).toEqual([
         "PUBLISHER_CHECK_OK environments=1 packages=1 load_errors=0 errors=0 warnings=0",
      ]);
      expect(fs.readdirSync(server!.serverRoot)).toEqual([
         "publisher.config.json",
      ]);
   });

   it("checks the config as written on every run, not what a root stored", async () => {
      const pkg = source("edited", { "m.malloy": GOOD });
      const root = tempDir("check-reused-root-");
      cleanups.push(root.cleanup);
      fs.writeFileSync(
         path.join(root.dir, "publisher.config.json"),
         JSON.stringify(config([{ name: "edited", location: pkg }])),
      );
      expect((await check({ serverRoot: root.dir })).code).toBe(0);

      // Break the model and add a package: a check that read stored state
      // would still pass.
      fs.writeFileSync(
         path.join(pkg, "m.malloy"),
         "source: s is duckdb.sql(\n",
      );
      const added = source("added", { "n.malloy": GOOD });
      fs.writeFileSync(
         path.join(root.dir, "publisher.config.json"),
         JSON.stringify(
            config([
               { name: "edited", location: pkg },
               { name: "added", location: added },
            ]),
         ),
      );
      const second = await check({ serverRoot: root.dir });
      expect(second.code).toBe(1);
      expect(summary(second.stderr)[0]).toStartWith(
         "PUBLISHER_CHECK_FAILED environments=1 packages=1 load_errors=1",
      );
   });

   it("prints a package warning without failing, unless warnings are fatal", async () => {
      const files = {
         "m.malloy":
            'source: s is duckdb.sql("SELECT 1 AS x") extend {\n  # subtitle="hello"\n  view: v is { group_by: x }\n}\n',
      };
      const warned = source("warned", files);
      const lenient = await check({
         config: config([{ name: "warned", location: warned }]),
      });
      expect(lenient.code).toBe(0);
      expect(findingsOnly(lenient.stdout)).toEqual([
         expect.stringMatching(
            /m\.malloy: warning: s -> v: Unknown render tag 'subtitle'/,
         ),
      ]);
      expect(summary(lenient.stderr)[0]).toStartWith(
         "PUBLISHER_CHECK_OK environments=1 packages=1 load_errors=0 errors=0 warnings=1",
      );

      const strict = await check({
         config: config([{ name: "warned", location: warned }]),
         args: ["--check-fail-on-warnings"],
      });
      expect(strict.code).toBe(1);
      expect(summary(strict.stderr)[0]).toStartWith("PUBLISHER_CHECK_FAILED");
   });

   it("follows each finding with its workflow command under GitHub Actions", async () => {
      const broken = source("broken", BROKEN);
      const { code, stdout } = await check({
         config: config([{ name: "broken", location: broken }]),
         env: { GITHUB_ACTIONS: "true" },
      });
      expect(code).toBe(1);
      const lines = findingsOnly(stdout);
      expect(lines).toContainEqual(
         `::error file=${path.join(broken, "a.malloy").replace(/:/g, "%3A").replace(/,/g, "%2C")},line=3,col=1,title=Malloy check%3A ${ENV}/broken::unexpected '}' (syntax-error)`,
      );
   });

   it("exits 2 when there is no config, and never falls back to the bundled samples", async () => {
      const { code, stdout, stderr } = await check({
         env: { PUBLISHER_USE_BUNDLED_DEFAULT: "true" },
      });
      expect(code).toBe(2);
      expect(findingsOnly(stdout)).toEqual([
         expect.stringContaining("publisher: error: No publisher.config.json"),
      ]);
      expect(summary(stderr)).toEqual([
         expect.stringMatching(
            /^PUBLISHER_CHECK_ERROR error="No publisher\.config\.json at /,
         ),
      ]);
      expect(fs.readdirSync(server!.serverRoot)).toEqual([]);
   });

   it("removes its temporary root and exits 143 on SIGTERM", async () => {
      // A Postgres connection to a port that accepts and never answers, so
      // the load blocks until the signal arrives, however fast the machine.
      const hole = net.createServer(() => {});
      await new Promise<void>((r) => hole.listen(0, "127.0.0.1", r));
      cleanups.push(() => hole.close());
      const port = (hole.address() as net.AddressInfo).port;
      const blocked = source("blocked", {
         "m.malloy": "source: s is pg.table('public.t')\n",
      });
      server = await spawnServer({
         // Pinned, so the harness returns at once instead of waiting for a
         // listener that check mode never opens.
         restPort: await getFreePort(),
         mcpPort: await getFreePort(),
         config: {
            environments: [
               {
                  name: ENV,
                  packages: [{ name: "blocked", location: blocked }],
                  connections: [
                     {
                        name: "pg",
                        type: "postgres",
                        postgresConnection: {
                           host: "127.0.0.1",
                           port,
                           databaseName: "x",
                           userName: "u",
                           password: "p",
                        },
                     },
                  ],
               },
            ],
         },
         env: { GITHUB_ACTIONS: undefined },
         args: ["--check"],
      });
      const pid = server.proc.pid!;
      const ours = () =>
         fs
            .readdirSync(os.tmpdir())
            .filter((name) => name.startsWith(`publisher-check-${pid}-`));
      await pollUntil(() => ours().length > 0 || server!.exited(), 60_000, 10);
      expect(server.exited()).toBe(false);
      server.proc.kill("SIGTERM");
      const code = await server.waitForExit(60_000);
      expect(code).toBe(143);
      expect(ours()).toEqual([]);
      expect(server.stderr()).toContain(
         'PUBLISHER_CHECK_ERROR error="Interrupted by SIGTERM."',
      );
   });

   it("prints ::error under GitHub Actions when a setting does not parse", async () => {
      const { code, stdout } = await check({
         env: {
            PUBLISHER_CHECK_FAIL_ON_WARNINGS: "maybe",
            GITHUB_ACTIONS: "true",
         },
      });
      expect(code).toBe(2);
      expect(stdout).toStartWith("::error title=Malloy check::");
   });

   it("exits 2 when --config names a missing file", async () => {
      const { code, stderr } = await check({
         args: ["--config", path.join(path.sep, "no", "such", "config.json")],
      });
      expect(code).toBe(2);
      expect(summary(stderr)[0]).toStartWith("PUBLISHER_CHECK_ERROR");
   });

   it("exits 2 when a setting does not parse", async () => {
      const { code, stderr } = await check({
         env: { PUBLISHER_CHECK_FAIL_ON_WARNINGS: "maybe" },
      });
      expect(code).toBe(2);
      expect(stderr).toContain("PUBLISHER_INIT_FAILED");
      expect(summary(stderr)[0]).toStartWith("PUBLISHER_CHECK_ERROR");
   });

   it("exits 2 when the config declares no environments", async () => {
      const { code, stderr } = await check({
         config: { environments: [] },
      });
      expect(code).toBe(2);
      expect(summary(stderr)[0]).toStartWith(
         'PUBLISHER_CHECK_ERROR error="Nothing to check',
      );
   });
});

function escape(text: string): string {
   return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
