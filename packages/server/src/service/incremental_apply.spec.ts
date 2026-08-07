// The delta query generator and its literal rendering. The compiler behaviors
// these encode (a query stage applies the range; a date column cannot be compared
// to a timestamp literal) are pinned in incremental_compiler_contract.spec.ts;
// here we check the text we generate, and that it really compiles.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelMaterializer,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   canonicalBoundValue,
   deltaQueryText,
   placeholderBounds,
   renderMalloyBound,
   renderSqlBound,
   snapshotBound,
   trialCompileDeltaQuery,
} from "./incremental_apply";

const ROOT = "file:///apply/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

function materialize(model: string): ModelMaterializer {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   return runtime.loadModel(new URL(`${ROOT}m.malloy`), {
      importBaseURL: new URL(ROOT),
   });
}

const MODEL = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date,
         TIMESTAMP '2024-01-01 00:00:00' AS ts, 'US' AS region
""")

#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date, ts, region
  aggregate: revenue is amount.sum()
}`;

describe("renderMalloyBound", () => {
   it("spells each type as its own Malloy literal", () => {
      expect(
         renderMalloyBound({ malloyType: "date", value: "2024-06-01" }),
      ).toBe("@2024-06-01");
      expect(
         renderMalloyBound({
            malloyType: "timestamp",
            value: "2024-06-01 12:30:00",
         }),
      ).toBe("@2024-06-01 12:30:00");
      expect(renderMalloyBound({ malloyType: "number", value: "42" })).toBe(
         "42",
      );
      expect(
         renderMalloyBound({ malloyType: "string", value: "us-east" }),
      ).toBe("'us-east'");
   });

   it("truncates a timestamp to whole seconds", () => {
      // Safe because the range is half-open and the ledger records the value
      // used: the trailing sub-second rows are picked up by the next run, which
      // starts exactly where this one ended.
      expect(
         renderMalloyBound({
            malloyType: "timestamp",
            value: "2024-06-01T12:30:00.512Z",
         }),
      ).toBe("@2024-06-01 12:30:00");
   });

   it("escapes a string bound rather than letting it break out of the literal", () => {
      expect(
         renderMalloyBound({ malloyType: "string", value: "o'brien\\x" }),
      ).toBe("'o\\'brien\\\\x'");
   });

   it("refuses a type it cannot render, instead of guessing", () => {
      expect(() =>
         renderMalloyBound({ malloyType: "boolean", value: "true" }),
      ).toThrow(/no literal rendering/);
   });

   it("refuses a malformed value rather than pasting it into a query", () => {
      expect(() =>
         renderMalloyBound({ malloyType: "date", value: "yesterday" }),
      ).toThrow(/ISO-8601/);
      expect(() =>
         renderMalloyBound({ malloyType: "number", value: "1; DROP TABLE t" }),
      ).toThrow(/finite number/);
   });
});

describe("renderSqlBound", () => {
   it("spells each type as its own SQL literal", () => {
      expect(renderSqlBound({ malloyType: "date", value: "2024-06-01" })).toBe(
         "DATE '2024-06-01'",
      );
      expect(
         renderSqlBound({
            malloyType: "timestamp",
            value: "2024-06-01T12:30:00.000Z",
         }),
      ).toBe("TIMESTAMP '2024-06-01 12:30:00'");
      expect(renderSqlBound({ malloyType: "number", value: "42" })).toBe("42");
   });

   it("doubles a quote in a string bound (SQL escaping, not Malloy's)", () => {
      expect(renderSqlBound({ malloyType: "string", value: "o'brien" })).toBe(
         "'o''brien'",
      );
   });
});

describe("canonicalBoundValue", () => {
   it("collapses the shapes a driver hands back", () => {
      const date = new Date("2024-06-01T12:30:00.512Z");
      expect(canonicalBoundValue("date", date)).toEqual({
         malloyType: "date",
         value: "2024-06-01",
      });
      expect(canonicalBoundValue("timestamp", date)).toEqual({
         malloyType: "timestamp",
         value: "2024-06-01T12:30:00",
      });
      // BigQuery wraps temporal values.
      expect(canonicalBoundValue("date", { value: "2024-06-01" })).toEqual({
         malloyType: "date",
         value: "2024-06-01",
      });
      expect(canonicalBoundValue("number", 17)).toEqual({
         malloyType: "number",
         value: "17",
      });
      expect(canonicalBoundValue("string", "us-east")).toEqual({
         malloyType: "string",
         value: "us-east",
      });
   });

   it("refuses a null watermark instead of producing an unbounded range", () => {
      expect(() => canonicalBoundValue("date", null)).toThrow(/null/);
   });

   it("round-trips a snapshot time through the literal renderer", () => {
      const bound = snapshotBound(
         "timestamp",
         new Date("2024-06-01T12:30:00Z"),
      );
      expect(renderMalloyBound(bound)).toBe("@2024-06-01 12:30:00");
   });
});

describe("deltaQueryText", () => {
   it("generates a half-open range over a query stage, with quoted identifiers", () => {
      const query = deltaQueryText({
         sourceName: "daily",
         watermarkName: "order_date",
         start: { malloyType: "date", value: "2024-06-01" },
         end: { malloyType: "date", value: "2024-07-01" },
      });
      expect(query).toBe(
         "run: `daily` -> {\n" +
            "  where: `order_date` >= @2024-06-01 and `order_date` < @2024-07-01\n" +
            "  select: *\n" +
            "}",
      );
   });

   it("compiles, and the compiled SQL carries both bounds", async () => {
      const sql = await materialize(MODEL)
         .loadQuery(
            deltaQueryText({
               sourceName: "daily",
               watermarkName: "order_date",
               start: { malloyType: "date", value: "2024-06-01" },
               end: { malloyType: "date", value: "2024-07-01" },
            }),
         )
         .getSQL();
      expect(sql).toContain("DATE '2024-06-01'");
      expect(sql).toContain("DATE '2024-07-01'");
      // The range filters the source's OUTPUT rows, above its GROUP BY.
      expect(sql.indexOf("WHERE")).toBeGreaterThan(sql.indexOf("GROUP BY"));
   });
});

describe("trialCompileDeltaQuery", () => {
   it("returns undefined for a source that can produce a delta", async () => {
      expect(
         await trialCompileDeltaQuery({
            materializer: materialize(MODEL),
            sourceName: "daily",
            watermarkName: "order_date",
            watermarkType: "date",
         }),
      ).toBeUndefined();
   });

   it("returns the compiler's message when the bound type does not match", async () => {
      // The publish-time value of the trial compile in one case: everything
      // static about `order_date` checks out (a real, orderable, non-aggregate
      // output column), and only a compile catches a bound spelled for the wrong
      // type. Note the asymmetry — a timestamp column accepts a date bound, but
      // not the reverse — which is why the renderer follows the column's type
      // rather than picking one temporal spelling.
      const error = await trialCompileDeltaQuery({
         materializer: materialize(MODEL),
         sourceName: "daily",
         watermarkName: "order_date",
         watermarkType: "timestamp",
      });
      expect(error).toMatch(/compare a date to a timestamp/i);
   });

   it("returns a message for a source name that does not exist", async () => {
      const error = await trialCompileDeltaQuery({
         materializer: materialize(MODEL),
         sourceName: "not_a_source",
         watermarkName: "order_date",
         watermarkType: "date",
      });
      expect(error).toBeDefined();
   });

   it("never executes anything: placeholder bounds are outside any real data", () => {
      expect(placeholderBounds("date")).toEqual({
         start: { malloyType: "date", value: "2000-01-01" },
         end: { malloyType: "date", value: "2000-01-02" },
      });
   });
});
