// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 cutover's conformance runner, from the command line (plan question
 * 15): answers each query of a package snapshot live, today and after the
 * cutover, and prints one row per query.
 *
 *   bun scripts/v1-cutover-conformance.ts --package <dir> --queries <file.json> [--json]
 *
 * `<dir>` is a package whose connections are DuckDB (a data snapshot);
 * `<file.json>` is an array of `{ "name", "model", "malloy", "givens"? }`.
 * Exits 1 when any row's verdict is `bug` (the cutover's answer differs from
 * live) and 2 on a usage error; a `regression` (served today, live after) is
 * printed for review, not failed.
 */

import * as fs from "fs";
import * as path from "path";
import {
   type CutoverQuery,
   runV1CutoverConformance,
} from "../src/test_helpers/conformance/v1_cutover_runner";

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
const queries = JSON.parse(
   fs.readFileSync(queriesFile, "utf8"),
) as CutoverQuery[];
const rows = await runV1CutoverConformance({
   package: { packageDir: path.resolve(packageDir) },
   queries,
});
if (process.argv.includes("--json")) {
   console.log(JSON.stringify(rows, null, 2));
} else {
   console.log("| query | today | cutover | path | verdict | detail |");
   console.log("| --- | --- | --- | --- | --- | --- |");
   for (const r of rows) {
      console.log(
         `| ${r.query} | ${r.today} | ${r.cutover} | ${r.cutoverPath} | ${r.verdict} | ${r.detail ?? ""} |`,
      );
   }
}
process.exit(rows.some((r) => r.verdict === "bug") ? 1 : 0);
