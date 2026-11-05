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
 * ``collectDimIndexCandidates`` flags it and extraction reports it as a finding — and that a *defaulted* parameter
 * (``value`` present) does NOT trip the guard, so the supported indexable-
 * parameterized-source pattern still compiles.
 *
 * No external services — DuckDB runs in-process over a CSV written to a tmpdir.
 */

import { describe, test } from "bun:test";
import assert from "node:assert";
import { compileFixture, extractFixture } from "./test_support";

// `report_id::number` is the required (no-default) parameter; `channel` is an
// #(index) dimension on that parameterized source.
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
   const compile = async (model: string) =>
      (
         await compileFixture({
            "orders.csv": "rep_id,channel_name\n1,web\n2,store\n",
            "model.malloy": model,
         })
      )[0];

   const findings = async (model: string) =>
      (
         await extractFixture({
            "orders.csv": "rep_id,channel_name\n1,web\n2,store\n",
            "model.malloy": model,
         })
      ).findings;

   test("reports an #(index) dimension on a source with a REQUIRED parameter", async () => {
      const [finding] = await findings(REQUIRED_PARAM_MODEL);
      assert.strictEqual(finding?.reason, "parameterized_source");
      assert.match(finding!.message, /required parameter\(s\) \(report_id\)/);
   }, 30_000);

   test("allows an #(index) dimension when the parameter is DEFAULTED", async () => {
      const result = await compile(DEFAULTED_PARAM_MODEL);

      assert.strictEqual(
         result.compileStatus,
         "SUCCESS",
         `expected SUCCESS for a defaulted parameter; got ${result.compileStatus} (${result.errorMessage})`,
      );
   }, 30_000);

   test("reports an #(index) tag on a measure (non-dimension)", async () => {
      const [finding] = await findings(INDEX_ON_MEASURE_MODEL);
      assert.strictEqual(finding?.reason, "not_a_dimension");
      assert.match(finding!.message, /is not a dimension/);
   }, 30_000);
});
