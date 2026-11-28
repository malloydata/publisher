// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Drop warehouse conformance scratch areas, for every warehouse whose
 * credentials are set (targets.ts):
 *
 *   bun tests/component/r4/conformance/warehouse/sweep.ts            # older than a day
 *   bun tests/component/r4/conformance/warehouse/sweep.ts --run-id X # run X's, any age
 *
 * The spec drops its own scratch in `afterAll` and sweeps day-old ones at
 * start; the workflow runs this with `--run-id` after the test step whatever
 * its outcome, so a cancelled or timed-out run leaves nothing behind either.
 * Exits non-zero if a drop failed.
 */

import type { Connection } from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../../../../src/service/environment";
import { discoverTargets, sweepScratch, WAREHOUSE_CONNECTION } from "./targets";

async function main(): Promise<number> {
   const at = process.argv.indexOf("--run-id");
   const runId = at >= 0 ? process.argv[at + 1] : undefined;
   if (at >= 0 && !runId) {
      console.error("--run-id needs a value");
      return 2;
   }
   const { targets, skipped } = discoverTargets();
   for (const s of skipped) console.log(`${s.dialect}: skipped, ${s.reason}`);
   let failed = 0;
   for (const target of targets) {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "whconf-sweep-"));
      const env = await Environment.create(
         `whc_sweep_${target.dialect}`,
         root,
         [target.connection()],
      );
      try {
         const connection = (await env.getMalloyConnection(
            WAREHOUSE_CONNECTION,
         )) as Connection;
         const result = await sweepScratch(
            connection,
            target.dialect,
            runId ? { runId } : {},
         );
         console.log(
            `${target.dialect}: dropped ${result.dropped.length}${
               result.dropped.length ? ` (${result.dropped.join(", ")})` : ""
            }`,
         );
         for (const f of result.failed)
            console.error(`${target.dialect}: ${f}`);
         failed += result.failed.length;
      } catch (error) {
         console.error(`${target.dialect}: sweep failed: ${String(error)}`);
         failed++;
      } finally {
         await env.closeAllConnections().catch(() => undefined);
         fs.rmSync(root, { recursive: true, force: true });
      }
   }
   return failed ? 1 : 0;
}

if (import.meta.main) {
   main().then(
      (code) => process.exit(code),
      (error) => {
         console.error(error);
         process.exit(1);
      },
   );
}
