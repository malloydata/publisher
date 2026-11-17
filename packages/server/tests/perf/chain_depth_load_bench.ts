// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Package-load time of a `#(materialize)` chain across files, by depth: file
 * `f<i>` imports `f<i-1>` and extends its source, each declaring its own
 * table, so every file's extension builds over its parent's (P10 across
 * files). Three loads per depth; `MZ=false` loads with materialization off.
 *
 *   bun tests/perf/chain_depth_load_bench.ts      (DEPTHS=2,4,6,8)
 */

import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";

installSettings({ env: { PUBLISHER_MATERIALIZE: process.env.MZ ?? "true" } });
async function load(depth: number): Promise<number> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "depth-")),
   );
   const files: Record<string, string> = {
      "f0.malloy": `source: raw is duckdb.sql("SELECT 1 AS a, 'x' AS b")\n#(materialize) name="t0"\nsource: s0 is raw extend { dimension: d0 is a + 0 }\n`,
   };
   for (let i = 1; i <= depth; i++) {
      files[`f${i}.malloy`] =
         `import "f${i - 1}.malloy"\n#(materialize) name="t${i}"\nsource: s${i} is s${i - 1} extend { dimension: d${i} is a + ${i} }\n`;
   }
   writePackageFiles(path.join(root, "pkg"), files, {
      manifest: { name: "pkg", description: "d" },
   });
   const env = await Environment.create("e", root, []);
   const t = performance.now();
   await env.addPackage("pkg");
   const ms = performance.now() - t;
   await env.closeAllConnections().catch(() => undefined);
   fs.rmSync(root, { recursive: true, force: true });
   return ms;
}
(async () => {
   await load(1);
   for (const d of (process.env.DEPTHS ?? "2,4,6,8").split(",").map(Number)) {
      const runs = [];
      for (let k = 0; k < 3; k++) runs.push(Math.round(await load(d)));
      console.log(`DEPTH ${d}: ${runs.join(", ")} ms`);
   }
   await __setPackageLoadPoolForTests(null);
   process.exit(0);
})();
