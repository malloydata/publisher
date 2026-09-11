// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import {
   buildQueryEnvelope,
   serializeEnvelope,
   MAX_RESULT_CHARS,
} from "./query_envelope";

const rows = (n: number) =>
   Array.from({ length: n }, (_, i) => ({ id: i, name: `row ${i}` }));

/** A minimal Malloy.Result: only the metadata the envelope reads. */
const result = (extra: Partial<Malloy.Result> = {}): Malloy.Result =>
   ({
      schema: { fields: [{ kind: "dimension", name: "id" }] },
      connection_name: "duckdb",
      ...extra,
   }) as Malloy.Result;

/**
 * The field names here are Credible's, on purpose: a data app authored against
 * Publisher is served through Credible, and an agent that sees one shape while
 * developing and another in production has to learn both. These assertions are
 * the contract that keeps them the same, so renaming a field should fail here.
 */
describe("buildQueryEnvelope", () => {
   it("returns flat rows plus Credible's metadata block", () => {
      const e = buildQueryEnvelope(rows(3), 1000, result());
      expect(e.rows).toHaveLength(3);
      expect(e._meta.connection_name).toBe("duckdb");
      expect(e._meta.schema.fields).toHaveLength(1);
      expect(e._meta.annotations).toEqual([]);
   });

   it("carries the Malloy metadata that flat rows drop", () => {
      // Field types and render tags live in the schema, not the rows, so an
      // agent would otherwise need a second verbose call to reach them.
      const e = buildQueryEnvelope(rows(1), 1000, {
         ...result(),
         query_timezone: "UTC",
         model_annotations: [{ value: "# dashboard" }],
      } as Malloy.Result);
      expect(e._meta.query_timezone).toBe("UTC");
      expect(e._meta.model_annotations).toEqual([{ value: "# dashboard" }]);
   });

   it("omits metadata keys the result did not carry", () => {
      const e = buildQueryEnvelope(rows(1), 1000, result());
      expect("query_timezone" in e._meta).toBe(false);
      expect("source_annotations" in e._meta).toBe(false);
   });

   /**
    * The correctness fix, and the one field a client cannot compute for itself:
    * the cap depends on server config and on the query's own LIMIT.
    */
   describe("_limit_hit", () => {
      it("is true when the row count lands exactly on the cap", () => {
         const e = buildQueryEnvelope(rows(1000), 1000, result());
         expect(e._limit_hit).toBe(true);
         expect(e._query_row_limit).toBe(1000);
         expect(e.warning).toContain("not a complete result");
      });

      it("is false when the result came in under the cap", () => {
         const e = buildQueryEnvelope(rows(999), 1000, result());
         expect(e._limit_hit).toBe(false);
         expect(e.warning).toBeUndefined();
      });

      /**
       * The false positive this guards. resolveModelQueryRowLimit folds the
       * query's own limit:/top: into the same cap, so equality holds every time
       * an author limited the query deliberately. Three of the eight views in
       * the bundled storefront example use `top:`, so a modelled top-N would
       * otherwise report its complete answer as cut off, and the Contract rule
       * would tell an agent that a top-N is "not the answer".
       */
      it("is false when the author's own limit: produced the cap", () => {
         const e = buildQueryEnvelope(
            rows(10),
            10,
            result(),
            [],
            undefined,
            "query",
         );
         expect(e._limit_hit).toBe(false);
         expect(e._limit_source).toBe("query");
         expect(e.warning).toBeUndefined();
      });

      it("still fires when the silent server default produced the cap", () => {
         const e = buildQueryEnvelope(
            rows(1000),
            1000,
            result(),
            [],
            undefined,
            "server_default",
         );
         expect(e._limit_hit).toBe(true);
         expect(e._limit_source).toBe("server_default");
         expect(e.warning).toContain("not a complete result");
      });

      it("reports the source even when the cap was not reached", () => {
         const e = buildQueryEnvelope(
            rows(3),
            10,
            result(),
            [],
            undefined,
            "query",
         );
         expect(e._limit_hit).toBe(false);
         expect(e._limit_source).toBe("query");
      });

      it("is false when no cap was applied", () => {
         // rowLimit 0 means uncapped; equality against 0 would otherwise call
         // an empty result "limited".
         expect(buildQueryEnvelope([], 0, result())._limit_hit).toBe(false);
      });

      it("does not call an empty result limited", () => {
         expect(buildQueryEnvelope([], 1000, result())._limit_hit).toBe(false);
      });
   });

   /**
    * compactResult is raw driver output and DuckDB returns count() as a BigInt,
    * so a plain JSON.stringify throws on the most common query anyone writes.
    */
   it("serializes BigInt values instead of throwing", () => {
      const e = buildQueryEnvelope([{ c: 150930n }], 1000, result());
      expect(() => serializeEnvelope(e)).not.toThrow();
      expect(JSON.parse(serializeEnvelope(e)).rows[0].c).toBe(150930);
   });

   describe("payload truncation", () => {
      it("drops rows to fit and reports both counts", () => {
         const e = buildQueryEnvelope(rows(200), 100_000, result(), [], 3_000);
         expect(e._rows_truncated).toBe(true);
         expect(e._total_rows).toBe(200);
         expect(e._returned_rows).toBe((e.rows as unknown[]).length);
         expect(e._returned_rows).toBeGreaterThan(0);
         expect(e._returned_rows).toBeLessThan(200);
         expect(e.warning).toContain("of 200 rows");
      });

      it("keeps the finished payload under the cap", () => {
         // The bug this pins: setting the marker fields and the warning AFTER
         // the search pushes the payload back over the limit the truncation
         // existed to respect.
         const e = buildQueryEnvelope(rows(400), 100_000, result(), [], 3_000);
         expect(serializeEnvelope(e).length).toBeLessThanOrEqual(3_000);
      });

      /**
       * Reachable because the hard ceiling is maxBytes (50MB), so one row with a
       * large text column passes assertWithinModelByteLimit and lands here.
       * "Showing 0 of N rows" reads as an empty result set, and an agent will
       * report "no rows matched" for a row that was merely too big to send.
       */
      it("says no rows fit rather than implying nothing matched", () => {
         const big = [{ doc: "z".repeat(5_000) }, { doc: "small" }];
         const e = buildQueryEnvelope(big, 1000, result(), [], 2_000);
         expect((e.rows as unknown[]).length).toBe(0);
         expect(e._returned_rows).toBe(0);
         expect(e._total_rows).toBe(2);
         expect(e.warning).toContain("No rows fit the result size limit");
         expect(e.warning).toContain("NOT an empty result");
         expect(e.warning).not.toContain("Showing 0 of");
      });

      it("keeps the zero-row payload under the cap too", () => {
         // The longer of the two wordings is what the search measures, so
         // swapping it in afterwards must not push the payload back over.
         const big = [{ doc: "z".repeat(5_000) }, { doc: "small" }];
         const e = buildQueryEnvelope(big, 1000, result(), [], 2_000);
         expect(serializeEnvelope(e).length).toBeLessThanOrEqual(2_000);
      });

      it("still reports the row cap when no rows fit", () => {
         // Both shortenings apply at once; neither may swallow the other.
         const big = [{ doc: "z".repeat(5_000) }, { doc: "y".repeat(5_000) }];
         const e = buildQueryEnvelope(big, 2, result(), [], 2_000);
         expect(e._limit_hit).toBe(true);
         expect(e.warning).toContain("not a complete result");
         expect(e.warning).toContain("No rows fit the result size limit");
      });

      it("omits the truncation fields entirely when nothing was dropped", () => {
         // Credible's shape: absent rather than false.
         const e = buildQueryEnvelope(rows(3), 1000, result());
         expect("_rows_truncated" in e).toBe(false);
         expect("_total_rows" in e).toBe(false);
         expect("_returned_rows" in e).toBe(false);
      });

      it("reports both shortenings when they happen together", () => {
         const e = buildQueryEnvelope(rows(200), 200, result(), [], 3_000);
         expect(e._limit_hit).toBe(true);
         expect(e._rows_truncated).toBe(true);
         // One says the query was capped, the other that the payload was.
         // Reporting only one would understate the loss.
         expect(e.warning).toContain("not a complete result");
         expect(e.warning).toContain("result size limit");
      });

      it("leaves an ordinary result well inside the default budget", () => {
         const e = buildQueryEnvelope(rows(500), 1000, result());
         expect("_rows_truncated" in e).toBe(false);
         expect(serializeEnvelope(e).length).toBeLessThan(MAX_RESULT_CHARS);
      });
   });

   it("passes render-tag messages through under Credible's key", () => {
      const e = buildQueryEnvelope(rows(1), 1000, result(), ["bad tag on x"]);
      expect(e.renderLogErrors).toEqual(["bad tag on x"]);
   });

   it("omits optional keys when they do not apply", () => {
      const e = buildQueryEnvelope(rows(1), 1000, result());
      expect("warning" in e).toBe(false);
      expect("renderLogErrors" in e).toBe(false);
   });
});
