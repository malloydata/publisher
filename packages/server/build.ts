import { build } from "bun";
import fs from "fs";

fs.rmSync("./dist", { recursive: true, force: true });
fs.mkdirSync("./dist");

await build({
   entrypoints: ["./src/server.ts", "./src/instrumentation.ts"],
   outdir: "./dist",
   target: "node",
   format: "cjs",
   minify: false,
   splitting: false,
   external: [
      "@malloydata/db-duckdb",
      "@malloydata/malloy",
      "@malloydata/malloy-sql",
      "@malloydata/render",
      "@malloydata/db-bigquery",
      "@malloydata/db-mysql",
      "@malloydata/db-postgres",
      "@malloydata/db-snowflake",
      "@malloydata/db-trino",
      "@google-cloud/storage",
   ],
});

fs.cpSync("../app/dist", "./dist/app", { recursive: true });

// Add shebang to server.js for npx compatibility and rename to .cjs for CommonJS
const serverJsPath = "./dist/server.js";
const serverCjsPath = "./dist/server.cjs";
let serverJsContent = fs.readFileSync(serverJsPath, "utf8");

// Replace import.meta.url with CommonJS equivalent
serverJsContent = serverJsContent.replace(
   /import\.meta\.url/g,
   "require('url').pathToFileURL(__filename).href",
);

const shebangContent = "#!/usr/bin/env node\n" + serverJsContent;
fs.writeFileSync(serverCjsPath, shebangContent);

// Remove the original .js file and make the .cjs file executable
fs.rmSync(serverJsPath);
fs.chmodSync(serverCjsPath, "755");
