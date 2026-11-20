// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `malloy-publisher --compile-package <dir> [--bundle-out <file>]`: the
 * compile seam as a process. Loads the package through the worker pool exactly
 * as a serving load would and writes the outcome as a bundle, then exits. The
 * package's own DuckDB is the only connection; an environment's connections
 * are the compile pool's to supply.
 */
import * as path from "path";
import { MalloyConfig } from "@malloydata/malloy";
import { getPackageLoadPool } from "./package_load/package_load_pool";
import { Package } from "./service/package";
import { settings } from "./settings";

const packagePath = path.resolve(settings.get("compilePackage") ?? "");
const outPath = path.resolve(
   settings.get("bundleOut") ?? `${packagePath}.bundle`,
);
const packageName = path.basename(packagePath);
const started = performance.now();
try {
   const { stamp, bytes, workerMs } = await Package.compileToBundle(
      "compile",
      packageName,
      packagePath,
      () => new MalloyConfig({ connections: {} }),
      outPath,
   );
   console.log(
      JSON.stringify(
         {
            bundle: outPath,
            bytes,
            workerMs: Math.round(workerMs),
            totalMs: Math.round(performance.now() - started),
            stamp,
         },
         null,
         1,
      ),
   );
} finally {
   await getPackageLoadPool().shutdown();
}
