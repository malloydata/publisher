// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @fileoverview Integration test for the non-public ``#(index)`` skip,
 * compiling REAL Malloy models against an in-process DuckDB connection.
 *
 * These pin the parts that can only be proven against the real compiler:
 *
 *  - an ``internal:`` ``#(index)`` dimension compiles SUCCESS (no load
 *    failure) and produces NO entity (so an indexer never sees it);
 *  - a child source that internalizes an inherited indexed dimension via
 *    ``include { internal: ... }`` (the tag can't be stripped in Malloy)
 *    gets no fingerprint on the child while the load still succeeds;
 *  - an ``#(index)`` tag on an *internal measure* is skipped too, not
 *    rejected as ``not_a_dimension`` (the access check runs first: the tag is
 *    inert on any non-public field).
 *
 * No external services — DuckDB runs in-process over a CSV written to a tmpdir.
 */

import { describe, test } from "bun:test";
import assert from "node:assert";
import { compileFixture } from "./test_support";
import type { ModelCompileResult } from "./internal_types";

/** The (source, field) pairs that carry an `#(index)` fingerprint. */
function fingerprinted(result: ModelCompileResult): string[] {
   return (result.entities ?? [])
      .filter((e) => e.dim_index_fingerprint)
      .map((e) => `${e.source}.${e.name}`);
}

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
   const compile = async (model: string) =>
      (
         await compileFixture({
            "orders.csv": "rep_id,channel_name\n1,web\n2,store\n",
            "model.malloy": model,
         })
      )[0];

   test("skips an internal #(index) dimension: SUCCESS, no entity", async () => {
      const result = await compile(INTERNAL_INDEXED_DIM_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS; got ${result.compileStatus} (${result.errorMessage})`,
      );
      // The skipped field never becomes an entity, so an indexer (driven by
      // entity tags) never sees it.
      const entityNames = (result.entities ?? []).map((e) => e.name);
      assert.ok(
         !entityNames.includes("channel"),
         `internal field must not be extracted as an entity; got: ${entityNames.join(", ")}`,
      );
   }, 30_000);

   test("inherited #(index) internalized in a child source: no child fingerprint, load succeeds", async () => {
      const result = await compile(INHERITED_INTERNALIZED_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS; got ${result.compileStatus} (${result.errorMessage})`,
      );
      assert.ok(
         !fingerprinted(result).includes("orders_discovery.channel"),
         `got: ${fingerprinted(result).join(", ")}`,
      );
   }, 30_000);

   test("skips an internal #(index) measure (access check precedes the dimension check)", async () => {
      const result = await compile(INTERNAL_INDEXED_MEASURE_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS (skip, not a not_a_dimension rejection); got ${result.compileStatus} (${result.errorMessage})`,
      );
      assert.deepEqual(fingerprinted(result), []);
   }, 30_000);
});
