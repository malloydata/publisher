// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Temporarily load the profiling hook into the package-load workers. NOT a
// product change: it edits packages/server/dist (the build output, git-ignored),
// keeps the original as *.orig, and `restore` (or any `bun run build`) undoes it.
//
//   bun perf/profile/patch-dist.ts apply     # load hook.mjs in each worker
//   bun perf/profile/patch-dist.ts restore   # put the original back
//
// It prepends `import "<hook.mjs>"` to dist/package_load_worker.mjs, because a
// `--preload` does not reach worker_threads under Bun. The main thread gets
// the hook from ctl.ts's `--preload`. Nothing else in the bundle is touched:
// load-phase timings come from the server's own
// publisher_package_load_phase_duration_ms histogram (see ctl.ts `phases`), so
// no step here depends on the bundle's text.
import { copyFileSync, existsSync, readFileSync, writeFileSync } from "fs";
import path from "path";

const DIST = path.resolve(import.meta.dir, "..", "..", "..", "dist");
const WORKER = path.join(DIST, "package_load_worker.mjs");
const HOOK = path.join(import.meta.dir, "hook.mjs");
const IMPORT = `import ${JSON.stringify(HOOK)};\n`;

const mode = process.argv[2];
if (mode === "restore") {
   if (existsSync(`${WORKER}.orig`)) copyFileSync(`${WORKER}.orig`, WORKER);
   console.log("dist/ restored");
} else if (mode === "apply") {
   const current = readFileSync(WORKER, "utf8");
   // A file without the import is a fresh build: it becomes the new *.orig, so
   // a stale backup from an older build is never put back.
   if (!current.startsWith(IMPORT)) {
      copyFileSync(WORKER, `${WORKER}.orig`);
      writeFileSync(WORKER, IMPORT + current);
   }
   console.log("dist/ instrumented (worker hook)");
} else {
   console.error("usage: bun perf/profile/patch-dist.ts apply|restore");
   process.exit(2);
}
