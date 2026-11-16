// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover conformance runner as the standalone CLI it is documented
 * as (scripts/v1-cutover-conformance.ts, plan question 15): spawned with
 * `NODE_ENV` unset, as a CI job runs it, over a package with no `storage=`
 * source and one with. 0.14.0 exited 3 on every fixture here, because the
 * harness reached a test seam that throws outside `NODE_ENV=test`.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { DATA_FILES } from "./conformance/data";
import { renderFiles, SHAPES, type Variant } from "./conformance/shapes";

const SERVER_DIR = path.resolve(__dirname, "../../..");
const SCRIPT = path.join(SERVER_DIR, "scripts/v1-cutover-conformance.ts");
/** Each spawn is killed after this, and each test times out just after. */
const SPAWN_TIMEOUT_MS = 120_000;
const TEST_TIMEOUT_MS = SPAWN_TIMEOUT_MS + 10_000;

/**
 * The child's environment: this process's, without `NODE_ENV`. Each child
 * also runs with `--no-env-file`, so a local `.env` cannot set it back and
 * pass these tests vacuously.
 */
function childEnv(): Record<string, string> {
   const env: Record<string, string> = {};
   for (const [k, v] of Object.entries(process.env)) {
      if (v !== undefined && k !== "NODE_ENV") env[k] = v;
   }
   return env;
}

async function runScript(
   shapeName: string,
   variant: Variant,
): Promise<{ code: number; stdout: string; stderr: string }> {
   const shape = SHAPES.find((s) => s.name === shapeName)!;
   const root = fs.mkdtempSync(path.join(os.tmpdir(), "v1-cutover-script-"));
   try {
      const pkgDir = path.join(root, "pkg");
      writePackageFiles(pkgDir, {
         ...DATA_FILES,
         ...renderFiles(shape, variant),
      });
      const queriesFile = path.join(root, "queries.json");
      fs.writeFileSync(
         queriesFile,
         JSON.stringify(
            shape.queries.map((q) => ({
               name: q.name,
               model: q.model ?? "m.malloy",
               malloy: q.malloy,
            })),
         ),
      );
      const proc = Bun.spawn(
         [
            process.execPath,
            "--no-env-file",
            SCRIPT,
            "--package",
            pkgDir,
            "--queries",
            queriesFile,
            "--json",
         ],
         {
            cwd: SERVER_DIR,
            env: childEnv(),
            stdout: "pipe",
            stderr: "pipe",
            timeout: SPAWN_TIMEOUT_MS,
         },
      );
      const [stdout, stderr, code] = await Promise.all([
         new Response(proc.stdout).text(),
         new Response(proc.stderr).text(),
         proc.exited,
      ]);
      return { code, stdout, stderr };
   } finally {
      fs.rmSync(root, { recursive: true, force: true });
   }
}

describe("scripts/v1-cutover-conformance.ts without NODE_ENV", () => {
   it(
      "spawns its child with NODE_ENV unset",
      async () => {
         const proc = Bun.spawn(
            [
               process.execPath,
               "--no-env-file",
               "-e",
               'console.log(process.env.NODE_ENV ?? "<unset>")',
            ],
            {
               cwd: SERVER_DIR,
               env: childEnv(),
               stdout: "pipe",
               timeout: SPAWN_TIMEOUT_MS,
            },
         );
         expect((await new Response(proc.stdout).text()).trim()).toBe(
            "<unset>",
         );
         expect(await proc.exited).toBe(0);
      },
      TEST_TIMEOUT_MS,
   );

   it(
      "answers a package with no storage= source",
      async () => {
         const { code, stdout, stderr } = await runScript(
            "baseline",
            "colocated",
         );
         expect({ code, stderr: code === 0 ? "" : stderr }).toEqual({
            code: 0,
            stderr: "",
         });
         const rows = JSON.parse(stdout) as { verdict: string }[];
         expect(rows.length).toBeGreaterThan(0);
         expect(rows.every((r) => r.verdict === "served_equal")).toBe(true);
      },
      TEST_TIMEOUT_MS,
   );

   it(
      "answers a package with a storage= source, built from the sandbox",
      async () => {
         const { code, stdout, stderr } = await runScript(
            "v1 rollup on annotated measures",
            "storage",
         );
         expect({ code, stderr: code === 0 ? "" : stderr }).toEqual({
            code: 0,
            stderr: "",
         });
         const rows = JSON.parse(stdout) as {
            query: string;
            cutoverPath: string;
         }[];
         expect(rows.length).toBeGreaterThan(0);
         expect(
            rows.find((r) => r.query === "covered by the rollup")?.cutoverPath,
         ).toBe("rollup");
      },
      TEST_TIMEOUT_MS,
   );
});
