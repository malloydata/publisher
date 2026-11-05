// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Integration test for the parameterized-source guard, compiling
 * a REAL Malloy model (experimental.parameters) against an in-process DuckDB
 * connection.
 *
 * Where the unit tests in {@link "./dim_index_discovery.spec"} exercise the
 * assert over synthetic candidates, this pins the part that can only be proven
 * against the real compiler: that a source's required (no-default) parameter
 * lands in ``structDef.parameters[name].value`` as null/undefined, so
 * ``collectDimIndexCandidates`` flags it and extraction refuses the package
 * with our message (an ``EntitiesValidationError``) — and that a *defaulted* parameter
 * (``value`` present) does NOT trip the guard, so the supported indexable-
 * parameterized-source pattern still compiles.
 *
 * No external services — DuckDB runs in-process over a CSV written to a tmpdir.
 */

import { afterEach, beforeEach, describe, test } from "bun:test";
import assert from "node:assert";
import path from "path";
import { writeWorkspace, type Workspace } from "./test_support";
import { packageCompilationService } from "./test_support";
import { ConnectionLookup } from "./test_support";
import { EntitiesValidationError } from "./extract";
import type { FilePointer } from "./test_support";

// `report_id::number` is the required (no-default) parameter; `channel` is an
// #(index) dimension on that parameterized source (as in
// `source: campaign_attribution(report_id::number)`).
const REQUIRED_PARAM_MODEL = `##! experimental.parameters
source: orders(report_id::number) is duckdb.table('orders.csv') extend {
  where: rep_id = report_id
  dimension:
    #(index)
    channel is channel_name
}
`;

// Same shape but the parameter carries a default, so the count query can run —
// the supported pattern. Must compile cleanly.
const DEFAULTED_PARAM_MODEL = `##! experimental.parameters
source: orders(report_id::number is 0) is duckdb.table('orders.csv') extend {
  where: rep_id = report_id
  dimension:
    #(index)
    channel is channel_name
}
`;

// An #(index) tag on a MEASURE (an aggregate has no value set to enumerate) —
// the dim-index placement guard must reject it rather than silently skip it.
const INDEX_ON_MEASURE_MODEL = `source: orders is duckdb.table('orders.csv') extend {
  measure:
    #(index)
    order_count is count()
}
`;

describe("dim-index publish-time placement guards (integration)", () => {
   let ws: Workspace;

   beforeEach(async () => {
      ws = await writeWorkspace(
         [
            {
               relpath: "orders.csv",
               contents: "rep_id,channel_name\n1,web\n2,store\n",
            },
         ],
         "malloy-param-source-",
      );
   });

   afterEach(async () => {
      await ws.cleanup();
   });

   async function compile(modelContents: string) {
      const { writeFile } = await import("node:fs/promises");
      const absolutePath = path.join(ws.dir, "model.malloy");
      await writeFile(absolutePath, modelContents);
      const lookup = await ConnectionLookup.getInstance([], ws.dir);
      const file: FilePointer = {
         packageRootPath: ws.dir,
         absolutePath,
         relativePath: "model.malloy",
      };
      return (
         await packageCompilationService.compilePackageModelFiles(
            { modelFiles: [file] },
            lookup,
         )
      )[0];
   }

   test("refuses an #(index) dimension on a source with a REQUIRED parameter", async () => {
      await assert.rejects(
         compile(REQUIRED_PARAM_MODEL),
         (err: unknown) =>
            err instanceof EntitiesValidationError &&
            /required parameter\(s\) \(report_id\)/.test(err.message),
      );
   }, 30_000);

   test("allows an #(index) dimension when the parameter is DEFAULTED", async () => {
      const result = await compile(DEFAULTED_PARAM_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS for a defaulted parameter; got ${result.compileStatus} (${result.errorMessage})`,
      );
   }, 30_000);

   test("refuses an #(index) tag on a measure (non-dimension)", async () => {
      await assert.rejects(
         compile(INDEX_ON_MEASURE_MODEL),
         (err: unknown) =>
            err instanceof EntitiesValidationError &&
            /is not a dimension/.test(err.message),
      );
   }, 30_000);
});
