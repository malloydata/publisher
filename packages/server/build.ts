import { build } from "bun";
import fs from "fs";
import { createRequire } from "module";

fs.rmSync("./dist", { recursive: true, force: true });
fs.mkdirSync("./dist");

// Resolve the Malloy version at build time and inline it as a constant,
// avoiding a runtime `import ... from "package.json"` that Node ESM rejects.
const malloyVersion = createRequire(import.meta.url)(
   "@malloydata/malloy/package.json",
).version as string;

await build({
   entrypoints: ["./src/server.ts", "./src/instrumentation.ts"],
   outdir: "./dist",
   target: "node",
   format: "esm",
   define: {
      __MALLOY_VERSION__: JSON.stringify(malloyVersion),
   },
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
      "@google-cloud/storage",
      "@azure/identity",
      "@azure/storage-blob",
   ],
});

fs.cpSync("../app/dist", "./dist/app", { recursive: true });

// Rename ESM outputs to .mjs so both Node and Bun can execute them
fs.renameSync("./dist/server.js", "./dist/server.mjs");
fs.renameSync("./dist/instrumentation.js", "./dist/instrumentation.mjs");

// Add shebang to server.mjs for npx/bunx compatibility
const serverJsPath = "./dist/server.mjs";
const serverJsContent = fs.readFileSync(serverJsPath, "utf8");
const shebangContent = "#!/usr/bin/env node\n" + serverJsContent;
fs.writeFileSync(serverJsPath, shebangContent);

// Make the file executable
fs.chmodSync(serverJsPath, "755");
