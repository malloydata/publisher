// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Integration test for the non-public ``#(index)`` skip,
 * compiling REAL Malloy models against an in-process DuckDB connection.
 *
 * These pin the parts that can only be proven against the real compiler:
 *
 *  - an ``internal:`` ``#(index)`` dimension compiles SUCCESS (no publish
 *    failure), lands in ``dimIndexNonPublicSkips``, and produces NO entity
 *    (so an indexer never sees it);
 *  - a child source that internalizes an inherited indexed dimension via
 *    ``include { internal: ... }`` — the tag can't be stripped in Malloy, the
 *    a shape seen in real packages — records the skip on the child while
 *    the publish still succeeds;
 *  - an ``#(index)`` tag on an *internal measure* is skipped too, not
 *    rejected as ``not_a_dimension`` (the access check runs first: the tag is
 *    inert on any non-public field).
 *
 * No external services — DuckDB runs in-process over a CSV written to a tmpdir.
 */

import { afterEach, beforeEach, describe, test } from "bun:test";
import assert from "node:assert";
import path from "path";
import { writeWorkspace, type Workspace } from "./test_support";
import { packageCompilationService } from "./test_support";
import { ConnectionLookup } from "./test_support";
import type { FilePointer } from "./test_support";

// An #(index) dimension that is internal in its own source — the tag is inert
// (nothing outside the source can query the field) and must be skipped, not
// error the publish.
const INTERNAL_INDEXED_DIM_MODEL = `source: orders is duckdb.table('orders.csv') extend {
  internal dimension:
    #(index)
    channel is channel_name
}
`;

// Base source with a public #(index) dimension, plus a child that
// internalizes it — the inherited annotation cannot be stripped, so the child
// unavoidably carries #(index) on an internal field, as in
// \`overlap_discovery is overlap_reach_metrics include { internal: * ... }\`.
const INHERITED_INTERNALIZED_MODEL = `##! experimental.access_modifiers
source: orders is duckdb.table('orders.csv') extend {
  dimension:
    #(index)
    channel is channel_name
}

source: orders_discovery is orders include {
  internal: channel
}
`;

// An #(index) tag on an INTERNAL measure: the access check runs before the
// dimension check, so this is a skip (inert tag), not a not_a_dimension
// rejection — \`internal: *\` over an inherited tagged measure must never
// fail a publish either.
const INTERNAL_INDEXED_MEASURE_MODEL = `source: orders is duckdb.table('orders.csv') extend {
  internal measure:
    #(index)
    order_count is count()
}
`;

describe("dim-index non-public #(index) skip (integration)", () => {
   let ws: Workspace;

   beforeEach(async () => {
      ws = await writeWorkspace(
         [
            {
               relpath: "orders.csv",
               contents: "rep_id,channel_name\n1,web\n2,store\n",
            },
         ],
         "malloy-non-public-",
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

   test("skips an internal #(index) dimension: SUCCESS, skip recorded, no entity", async () => {
      const result = await compile(INTERNAL_INDEXED_DIM_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS; got ${result.compileStatus} (${result.errorMessage})`,
      );
      assert.deepEqual(result.dimIndexNonPublicSkips, [
         { sourceName: "orders", fieldName: "channel" },
      ]);
      // The skipped field never becomes an entity, so an indexer (driven by
      // entity tags) never sees it.
      const entityNames = (result.entities ?? []).map((e) => e.name);
      assert.ok(
         !entityNames.includes("channel"),
         `internal field must not be extracted as an entity; got: ${entityNames.join(", ")}`,
      );
   }, 30_000);

   test("inherited #(index) internalized in a child source: child skip, publish succeeds", async () => {
      const result = await compile(INHERITED_INTERNALIZED_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS; got ${result.compileStatus} (${result.errorMessage})`,
      );
      assert.deepEqual(result.dimIndexNonPublicSkips, [
         { sourceName: "orders_discovery", fieldName: "channel" },
      ]);
   }, 30_000);

   test("skips an internal #(index) measure (access check precedes the dimension check)", async () => {
      const result = await compile(INTERNAL_INDEXED_MEASURE_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS (skip, not a not_a_dimension rejection); got ${result.compileStatus} (${result.errorMessage})`,
      );
      assert.deepEqual(result.dimIndexNonPublicSkips, [
         { sourceName: "orders", fieldName: "order_count" },
      ]);
   }, 30_000);
});
