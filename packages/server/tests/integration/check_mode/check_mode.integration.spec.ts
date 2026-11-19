// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `--check` end to end: a real `bun src/server.ts --check` process against a
 * real config, for the contract CI reads — the exit code, one line per
 * problem with its file, line and column, and the summary line on stderr —
 * and that it exits without binding either port.
 */

import { afterEach, describe, expect, it, setDefaultTimeout } from "bun:test";
import path from "path";
import {
   spawnServer,
   type SpawnedServer,
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
      server = await spawnServer({
         ...options,
         args: ["--check", ...(options.args ?? [])],
      });
      const code = await server.waitForExit(120_000);
      // The harness keeps stderr apart, and stdout only interleaved with it;
      // the findings are stdout's lines, and nothing on stderr looks like one.
      return { code, stdout: server.output(), stderr: server.stderr() };
   };

   const summary = (stderr: string) =>
      stderr.split("\n").filter((line) => line.startsWith("PUBLISHER_CHECK"));

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
      const lines = stdout
         .split("\n")
         .filter((line) => line.endsWith(`[${ENV}/broken]`));
      expect(lines).toEqual([
         expect.stringMatching(
            new RegExp(
               `${escape(path.join(broken, "a.malloy"))}:3:1: error: unexpected '}'`,
            ),
         ),
         expect.stringMatching(
            new RegExp(
               `${escape(path.join(broken, "b.malloy"))}:1:\\d+: error: 'nope' is not defined`,
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

   it("exits 0 when every package loads", async () => {
      const good = source("good", { "good.malloy": GOOD });
      const { code, stdout, stderr } = await check({
         config: config([{ name: "good", location: good }]),
      });

      expect(code).toBe(0);
      expect(stdout).not.toMatch(/: error: /);
      expect(summary(stderr)).toEqual([
         "PUBLISHER_CHECK_OK environments=1 packages=1 load_errors=0 errors=0 warnings=0",
      ]);
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
      expect(lenient.stdout).toMatch(
         /m\.malloy: warning: s -> v: Unknown render tag 'subtitle'/,
      );
      expect(summary(lenient.stderr)[0]).toStartWith(
         "PUBLISHER_CHECK_OK environments=1 packages=1 load_errors=0 errors=0 warnings=1",
      );
      await server?.stop();

      const strict = await check({
         config: config([{ name: "warned", location: warned }]),
         args: ["--check-fail-on-warnings"],
      });
      expect(strict.code).toBe(1);
      expect(summary(strict.stderr)[0]).toStartWith("PUBLISHER_CHECK_FAILED");
   });

   it("exits 1 when there is no config to check", async () => {
      const { code, stdout, stderr } = await check({
         args: ["--config", path.join(path.sep, "no", "such", "config.json")],
      });
      expect(code).toBe(1);
      expect(stdout).toContain("Serving with no environments");
      expect(summary(stderr)).toEqual([
         "PUBLISHER_CHECK_FAILED environments=0 packages=0 load_errors=0 errors=1 warnings=0",
      ]);
   });
});

function escape(text: string): string {
   return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
