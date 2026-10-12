// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Temporarily instrument the production build for profiling. NOT a product
// change: it edits packages/server/dist (the build output, git-ignored), keeps
// the originals as *.orig, and `restore` (or any `bun run build`) undoes it.
//
//   bun perf/profile/patch-dist.ts apply     # instrument dist/
//   bun perf/profile/patch-dist.ts restore   # put the originals back
//
// apply does two things:
//   1. Prepends `import "<hook.mjs>"` to dist/package_load_worker.mjs, so each
//      package-load worker runs the profiling hook (a preload does not reach
//      worker_threads under Bun).
//   2. Wraps the package-load phases in dist/server.mjs with timers that append
//      "<phase> <start epoch ms> <duration ms>" to $PERF_PROFILE_OUT/phases.log:
//      copy, create, worker, readDatabases, fromSerialized, validateRenderTags,
//      discoverReports, buildPlan, updateLocked, bindManifest, rebind,
//      notifyLoaded. Each wrapper matches the bundle text exactly once or the
//      script stops, so a refactor that moves a phase fails loudly here.
import { copyFileSync, existsSync, readFileSync, writeFileSync } from "fs";
import path from "path";

const DIST = path.resolve(import.meta.dir, "..", "..", "..", "dist");
const SERVER = path.join(DIST, "server.mjs");
const WORKER = path.join(DIST, "package_load_worker.mjs");
const HOOK = path.join(import.meta.dir, "hook.mjs");

const PHASES: Array<[string, string]> = [
   [
      `      await downloader(stagingPath);`,
      `      await __ph("copy", () => downloader(stagingPath));`,
   ],
   [
      `        newPackage = await Package.create(this.environmentName, packageName, canonicalPath, () => this.malloyConfig.malloyConfig, true);`,
      `        newPackage = await __ph("create", () => Package.create(this.environmentName, packageName, canonicalPath, () => this.malloyConfig.malloyConfig, true));`,
   ],
   [
      `        const _package = await Package.create(this.environmentName, packageName, packagePath, () => this.malloyConfig.malloyConfig);`,
      `        const _package = await __ph("create", () => Package.create(this.environmentName, packageName, packagePath, () => this.malloyConfig.malloyConfig));`,
   ],
   [
      `      workerOutcome,\n      Package.readDatabases(packagePath, malloyConfig)\n    ]);`,
      `      __ph("worker", () => workerOutcome),\n      __ph("readDatabases", () => Package.readDatabases(packagePath, malloyConfig))\n    ]);`,
   ],
   [
      `        model = Model.fromSerialized(packageName, packagePath, malloyConfig, sm, {\n          buildManifest,\n          skipAuthorizeWarningLog: ctx.compile\n        });`,
      `        model = __phs("fromSerialized", () => Model.fromSerialized(packageName, packagePath, malloyConfig, sm, {\n          buildManifest,\n          skipAuthorizeWarningLog: ctx.compile\n        }));`,
   ],
   [
      `        warnings = await model.validateRenderTags();`,
      `        warnings = await __ph("validateRenderTags", () => model.validateRenderTags());`,
   ],
   [
      `    const reportTimings = await pkg.discoverReports("load");`,
      `    const reportTimings = await __ph("discoverReports", () => pkg.discoverReports("load"));`,
   ],
   [
      `} = await computePackageBuildPlan(pkg, undefined, {\n        skipModel: (modelPath) => models.get(modelPath)?.mayContributeToBuildPlan() === false\n      });`,
      `} = await __ph("buildPlan", () => computePackageBuildPlan(pkg, undefined, {\n        skipModel: (modelPath) => models.get(modelPath)?.mayContributeToBuildPlan() === false\n      }));`,
   ],
   [
      `      if (options.update !== undefined) {\n        await this._updatePackageLocked(packageName, options.update, {\n          recordLocation: true\n        });\n      }`,
      `      if (options.update !== undefined) {\n        await __ph("updateLocked", () => this._updatePackageLocked(packageName, options.update, {\n          recordLocation: true\n        }));\n      }`,
   ],
   [
      `      await this.bindManifestIfConfigured(newPackage);\n      await this.rebindServeBindingsFromLocalStore(newPackage);\n      this.packages.set(packageName, newPackage);\n      this.notifyPackageLoaded(newPackage);`,
      `      await __ph("bindManifest", () => this.bindManifestIfConfigured(newPackage));\n      await __ph("rebind", () => this.rebindServeBindingsFromLocalStore(newPackage));\n      this.packages.set(packageName, newPackage);\n      __phs("notifyLoaded", () => this.notifyPackageLoaded(newPackage));`,
   ],
   [
      `        await this.bindManifestIfConfigured(_package);\n        await this.rebindServeBindingsFromLocalStore(_package);`,
      `        await __ph("bindManifest", () => this.bindManifestIfConfigured(_package));\n        await __ph("rebind", () => this.rebindServeBindingsFromLocalStore(_package));`,
   ],
   [
      `        this.notifyPackageLoaded(_package);`,
      `        __phs("notifyLoaded", () => this.notifyPackageLoaded(_package));`,
   ],
];

const TIMERS = `import { appendFileSync as __phAppend } from "node:fs";
globalThis.__phRec = (n, s, d) => { if (process.env.PERF_PROFILE_OUT) __phAppend(process.env.PERF_PROFILE_OUT + "/phases.log", n + " " + s + " " + d.toFixed(2) + "\\n"); };
globalThis.__ph = async (n, f) => { const t = performance.now(); const s = Date.now(); try { return await f(); } finally { globalThis.__phRec(n, s, performance.now() - t); } };
globalThis.__phs = (n, f) => { const t = performance.now(); const s = Date.now(); try { return f(); } finally { globalThis.__phRec(n, s, performance.now() - t); } };
`;

// The unpatched text. A file without the marker is a fresh build, so it
// becomes the new *.orig (a stale backup from an older build is never used).
function original(file: string, marker: string): string {
   if (!readFileSync(file, "utf8").includes(marker)) {
      copyFileSync(file, `${file}.orig`);
   }
   return readFileSync(`${file}.orig`, "utf8");
}

const mode = process.argv[2];
if (mode === "restore") {
   for (const f of [SERVER, WORKER]) {
      if (existsSync(`${f}.orig`)) copyFileSync(`${f}.orig`, f);
   }
   console.log("dist/ restored");
} else if (mode === "apply") {
   writeFileSync(
      WORKER,
      `import ${JSON.stringify(HOOK)};\n${original(WORKER, HOOK)}`,
   );
   let s = original(SERVER, "globalThis.__phRec");
   for (const [from, to] of PHASES) {
      const n = s.split(from).length - 1;
      if (n !== 1) {
         throw new Error(
            `expected exactly one match in server.mjs, found ${n}:\n${from}`,
         );
      }
      s = s.replace(from, to);
   }
   // After the shebang line, so the file still runs as an executable.
   const nl = s.indexOf("\n");
   writeFileSync(SERVER, s.slice(0, nl + 1) + TIMERS + s.slice(nl + 1));
   console.log(
      `dist/ instrumented (${PHASES.length} phase timers, worker hook)`,
   );
} else {
   console.error("usage: bun perf/profile/patch-dist.ts apply|restore");
   process.exit(2);
}
