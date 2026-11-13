// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's conformance runner, from the command line (plan question
 * 15): answers each query of a package snapshot live and served, and
 * prints one row per query.
 *
 *   bun scripts/v1-cutover-conformance.ts --package <dir> --queries <file.json> [--json]
 *
 * `<dir>` is a package whose connections are DuckDB (a data snapshot);
 * `<file.json>` is an array of `{ "name", "model", "malloy", "givens"? }`.
 * Exits 0 when no row's verdict is `bug`, 1 when one is (the cutover's answer
 * differs from live), 2 on a usage error or unreadable input, and 3 when the
 * run itself fails; a `live` row is printed for review, not failed. The
 * server's log lines go to stderr, so stdout holds
 * only the rows (`--json` parses as is).
 */

import * as fs from "fs";
import * as path from "path";
import winston from "winston";
import { logger } from "../src/logger";
import {
   type CutoverQuery,
   runV1CutoverConformance,
} from "../src/test_helpers/conformance/v1_cutover_runner";

// Every log line to stderr: stdout is the rows.
for (const transport of logger.transports) {
   if (transport instanceof winston.transports.Console) {
      logger.remove(transport);
      logger.add(
         new winston.transports.Console({
            format: transport.format,
            stderrLevels: Object.keys(logger.levels),
         }),
      );
   }
}

function arg(name: string): string | undefined {
   const at = process.argv.indexOf(`--${name}`);
   return at >= 0 ? process.argv[at + 1] : undefined;
}

const packageDir = arg("package");
const queriesFile = arg("queries");
if (!packageDir || !queriesFile) {
   console.error(
      "usage: bun scripts/v1-cutover-conformance.ts --package <dir> --queries <file.json> [--json]",
   );
   process.exit(2);
}
function usage(message: string): never {
   console.error(`v1-cutover-conformance: ${message}`);
   process.exit(2);
}
let queries: CutoverQuery[];
try {
   queries = JSON.parse(fs.readFileSync(queriesFile, "utf8"));
} catch (error) {
   usage(
      `cannot read the queries file: ${error instanceof Error ? error.message : String(error)}`,
   );
}
if (
   !Array.isArray(queries) ||
   !queries.every(
      (q) =>
         q &&
         typeof q.name === "string" &&
         typeof q.model === "string" &&
         typeof q.malloy === "string",
   )
) {
   usage('the queries file must be an array of { "name", "model", "malloy" }');
}
if (
   !fs
      .statSync(path.resolve(packageDir), { throwIfNoEntry: false })
      ?.isDirectory()
) {
   usage(`no package directory at ${packageDir}`);
}
let rows: Awaited<ReturnType<typeof runV1CutoverConformance>>;
try {
   rows = await runV1CutoverConformance({
      package: { packageDir: path.resolve(packageDir) },
      queries,
   });
} catch (error) {
   console.error(
      `v1-cutover-conformance: the run failed: ${error instanceof Error ? error.message : String(error)}`,
   );
   process.exit(3);
}
if (process.argv.includes("--json")) {
   console.log(JSON.stringify(rows, null, 2));
} else {
   console.log("| query | cutover | path | verdict | detail |");
   console.log("| --- | --- | --- | --- | --- |");
   for (const r of rows) {
      console.log(
         `| ${r.query} | ${r.cutover} | ${r.cutoverPath} | ${r.verdict} | ${r.detail ?? ""} |`,
      );
   }
}
process.exit(rows.some((r) => r.verdict === "bug") ? 1 : 0);
