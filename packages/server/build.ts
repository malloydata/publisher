import { build } from "bun";
import fs from "fs";

fs.rmSync("./dist", { recursive: true, force: true });
fs.mkdirSync("./dist");

await build({
   // compile_worker.ts is bundled as a SEPARATE entrypoint so it can
   // be loaded by `new Worker(...)` at runtime. It must NOT be
   // inlined into server.mjs (workers can't share module state with
   // the parent process — they get their own JS realm).
   entrypoints: [
      "./src/server.ts",
      "./src/instrumentation.ts",
      "./src/compile/compile_worker.ts",
   ],
   outdir: "./dist",
   target: "node",
   format: "esm",
   external: [
      "@malloydata/db-duckdb",
      "duckdb",
      "@malloydata/malloy",
      "@malloydata/malloy-sql",
      "@malloydata/render",
      "@malloydata/db-bigquery",
      "@malloydata/db-mysql",
      "@malloydata/db-postgres",
      "@malloydata/db-snowflake",
      "@malloydata/db-trino",
      "@malloydata/db-databricks",
      "@google-cloud/storage",
      "@azure/identity",
      "@azure/storage-blob",
   ],
});

fs.cpSync("../app/dist", "./dist/app", { recursive: true });

// Ship a default publisher.config.json inside the bundle so that
// `npx @malloy-publisher/server` works with zero args (uses
// DuckDB-only samples). config.ts looks for this file next to
// server.mjs.
//
// IMPORTANT: keep `packages/server/src/default-publisher.config.json`
// in sync with `packages/server/publisher.config.json`. The two files
// have intentionally identical content — the committed one drives the
// dev workflow (`bun run start`, integration tests), the bundled one
// is the fallback for npx users with no config of their own. A drift
// here surfaces as "npx users see different samples than dev users."
fs.copyFileSync(
   "./src/default-publisher.config.json",
   "./dist/default-publisher.config.json",
);

// Rename ESM outputs to .mjs so both Node and Bun can execute them
fs.renameSync("./dist/server.js", "./dist/server.mjs");
fs.renameSync("./dist/instrumentation.js", "./dist/instrumentation.mjs");
// Bun emits compile_worker into its source-relative subdir; flatten
// so compile_pool.ts's `resolveWorkerScript()` finds it as a sibling
// of server.mjs. The path layout match is intentional — keep these
// two in sync or the worker pool falls back to in-process compile.
if (fs.existsSync("./dist/compile/compile_worker.js")) {
   fs.renameSync(
      "./dist/compile/compile_worker.js",
      "./dist/compile_worker.mjs",
   );
   try {
      fs.rmdirSync("./dist/compile");
   } catch {
      /* directory may be non-empty if Bun produced sourcemaps; leave it */
   }
} else if (fs.existsSync("./dist/compile_worker.js")) {
   fs.renameSync("./dist/compile_worker.js", "./dist/compile_worker.mjs");
}

// Add shebang to server.mjs for npx/bunx compatibility
const serverJsPath = "./dist/server.mjs";
const serverJsContent = fs.readFileSync(serverJsPath, "utf8");
const shebangContent = "#!/usr/bin/env node\n" + serverJsContent;
fs.writeFileSync(serverJsPath, shebangContent);

// Make the file executable
fs.chmodSync(serverJsPath, "755");
