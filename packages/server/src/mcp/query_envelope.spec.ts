import { describe, expect, it } from "bun:test";
import {
   buildQueryEnvelope,
   serializeEnvelope,
   MAX_RESULT_CHARS,
} from "./query_envelope";

const rows = (n: number) =>
   Array.from({ length: n }, (_, i) => ({ id: i, name: `row ${i}` }));

describe("buildQueryEnvelope", () => {
   it("reports flat rows with a count", () => {
      const e = buildQueryEnvelope(rows(3), 1000);
      expect(e.rows).toHaveLength(3);
      expect(e.row_count).toBe(3);
      expect(e.query_row_limit).toBe(1000);
      expect(e.truncated_for_size).toBe(false);
   });

   /**
    * The correctness fix. A query with no `limit:` of its own gets the server
    * default pushed into the SQL, and that is under PUBLISHER_MAX_QUERY_ROWS,
    * so nothing throws and nothing warns. Landing exactly on the cap is the
    * only evidence available that rows were left behind.
    */
   describe("limit_hit", () => {
      it("is true when the row count lands exactly on the cap", () => {
         const e = buildQueryEnvelope(rows(1000), 1000);
         expect(e.limit_hit).toBe(true);
         expect(e.warning).toContain("1000");
         expect(e.warning).toContain("not a complete result");
      });

      it("is false when the result came in under the cap", () => {
         const e = buildQueryEnvelope(rows(999), 1000);
         expect(e.limit_hit).toBe(false);
         expect(e.warning).toBeUndefined();
      });

      it("is false when no cap was applied", () => {
         // rowLimit 0 means uncapped; equality against 0 would otherwise call
         // an empty result "limited".
         const e = buildQueryEnvelope([], 0);
         expect(e.limit_hit).toBe(false);
      });

      it("does not call an empty result limited", () => {
         expect(buildQueryEnvelope([], 1000).limit_hit).toBe(false);
      });
   });

   /**
    * compactResult is raw driver output and DuckDB returns count() as a BigInt,
    * so a plain JSON.stringify throws on the most common query anyone writes.
    */
   it("serializes BigInt values instead of throwing", () => {
      const e = buildQueryEnvelope([{ wine_count: 150930n }], 1000);
      expect(() => serializeEnvelope(e)).not.toThrow();
      expect(JSON.parse(serializeEnvelope(e)).rows[0].wine_count).toBe(150930);
   });

   describe("truncated_for_size", () => {
      it("drops rows to fit and says how many it kept", () => {
         const e = buildQueryEnvelope(rows(200), 100_000, [], 2_000);
         expect(e.truncated_for_size).toBe(true);
         expect((e.rows as unknown[]).length).toBeLessThan(200);
         expect((e.rows as unknown[]).length).toBeGreaterThan(0);
         expect(serializeEnvelope(e).length).toBeLessThanOrEqual(2_000);
         // row_count describes the rows actually present, and the warning
         // carries the original count so the loss is quantified.
         expect(e.row_count).toBe((e.rows as unknown[]).length);
         expect(e.warning).toContain("of 200 rows");
      });

      it("leaves a result that already fits alone", () => {
         const e = buildQueryEnvelope(rows(3), 1000);
         expect(e.truncated_for_size).toBe(false);
         expect(e.rows).toHaveLength(3);
      });

      it("reports both shortenings when they happen together", () => {
         const e = buildQueryEnvelope(rows(50), 50, [], 2_000);
         expect(e.limit_hit).toBe(true);
         expect(e.truncated_for_size).toBe(true);
         // Both matter: one says the query was capped, the other says the
         // payload was. Reporting only one would understate the loss.
         expect(e.warning).toContain("not a complete result");
         expect(e.warning).toContain("result size limit");
      });

      it("keeps the default budget generous enough for ordinary results", () => {
         const e = buildQueryEnvelope(rows(500), 1000);
         expect(e.truncated_for_size).toBe(false);
         expect(serializeEnvelope(e).length).toBeLessThan(MAX_RESULT_CHARS);
      });
   });

   it("passes render-tag messages through", () => {
      const e = buildQueryEnvelope(rows(1), 1000, ["bad tag on field x"]);
      expect(e.renderLogErrors).toEqual(["bad tag on field x"]);
   });

   it("omits optional keys when they do not apply", () => {
      const e = buildQueryEnvelope(rows(1), 1000);
      expect("warning" in e).toBe(false);
      expect("renderLogErrors" in e).toBe(false);
   });
});
