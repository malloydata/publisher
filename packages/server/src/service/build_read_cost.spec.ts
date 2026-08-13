import { describe, expect, it } from "bun:test";

import {
   bigQueryReadCost,
   pickSnowflakeReadRow,
   snowflakeCostSQL,
   snowflakeReadCost,
} from "./build_read_cost";

describe("bigQueryReadCost", () => {
   it("keeps billed and scanned as separate numbers", () => {
      // BigQuery rounds up to a 10MB floor, so a small read bills an order of
      // magnitude above what it scanned. Measured live: 5,114,816 -> 10,485,760.
      const cost = bigQueryReadCost(
         {
            bytes_processed: "5114816",
            bytes_billed: "10485760",
            total_slot_time_ms: 26,
            cache_hit: "false",
         },
         "script_job_x_0",
         "job_parent",
      );
      expect(cost.bytesScanned).toBe(5114816);
      expect(cost.bytesBilled).toBe(10485760);
      expect(cost.cacheHit).toBe(false);
   });

   it("carries the parent, which is the id the child can be found BY", () => {
      const cost = bigQueryReadCost({}, "child", "parent");
      expect(cost.jobId).toBe("child");
      expect(cost.parentJobId).toBe("parent");
   });
});

describe("snowflakeCostSQL", () => {
   it("scopes by tag rather than by LAST_QUERY_ID", () => {
      // The session is not exclusively the build's: the ADBC driver issues its own
      // connection probes, and measured live LAST_QUERY_ID() after a build-shaped
      // read returned a probe rather than the read.
      const sql = snowflakeCostSQL('{"cred_run":"r1"}');
      expect(sql).toContain("QUERY_TAG =");
      expect(sql).not.toContain("LAST_QUERY_ID");
   });

   it("excludes its own shape, which carries the same tag", () => {
      expect(snowflakeCostSQL("{}")).toContain(
         "QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA.QUERY_HISTORY%'",
      );
   });

   it("escapes a backslash in the tag, or it matches NOTHING", () => {
      // The tag is JSON, so a value containing a backslash arrives doubled.
      // Snowflake consumes one as an escape, so quote-doubling alone compares
      // against a string that cannot equal the stored tag — measured live, it
      // matched zero rows and reported no cost rather than an error.
      const tag = JSON.stringify({ cred_org: "o'brien\\co" });
      const sql = snowflakeCostSQL(tag);
      // Computed rather than hand-written: the levels are exactly what is easy to
      // get wrong, so spelling them out in a literal would test the spelling.
      const snowflakeEscaped = tag.replace(/\\/g, "\\\\").replace(/'/g, "''");
      expect(sql).toContain(`QUERY_TAG = '${snowflakeEscaped}'`);
      // And specifically NOT the quote-only form, which matched zero rows live.
      expect(sql).not.toContain(`QUERY_TAG = '${tag.replace(/'/g, "''")}'`);
   });

   it("reads the low-latency history view, not the lagging one", () => {
      // ACCOUNT_USAGE lags by up to three hours, so it cannot answer a question
      // asked immediately after a build.
      const sql = snowflakeCostSQL("{}");
      expect(sql).toContain("INFORMATION_SCHEMA.QUERY_HISTORY");
      expect(sql).not.toContain("ACCOUNT_USAGE");
   });
});

describe("pickSnowflakeReadRow", () => {
   const read = {
      query_text: "SELECT a FROM t",
      rows_produced: 4321,
      bytes_scanned: 900,
   };
   const probe = { query_text: "SELECT 1", rows_produced: 0, bytes_scanned: 0 };

   it("picks the build's read out of the driver's probes", () => {
      // Measured live: one build-shaped read left the read plus two `SELECT 1`
      // connection probes under the same tag.
      expect(
         pickSnowflakeReadRow([probe, read, probe], "SELECT a FROM t"),
      ).toBe(read);
   });

   it("tolerates surrounding whitespace on either side", () => {
      expect(
         pickSnowflakeReadRow([{ query_text: " SELECT a " }], "SELECT a"),
      ).toEqual({ query_text: " SELECT a " });
   });

   it("reports nothing when the read is not among them", () => {
      expect(
         pickSnowflakeReadRow([probe, probe], "SELECT a FROM t"),
      ).toBeNull();
   });

   it("reports nothing rather than guessing between duplicates", () => {
      expect(
         pickSnowflakeReadRow([read, { ...read }], "SELECT a FROM t"),
      ).toBeNull();
   });

   it("reads a row whose columns came back UPPERCASE", () => {
      // What Snowflake returns if the quoted aliases are ever lost.
      expect(
         pickSnowflakeReadRow([{ QUERY_TEXT: "SELECT a" }], "SELECT a"),
      ).toEqual({ QUERY_TEXT: "SELECT a" });
   });
});

describe("snowflakeReadCost", () => {
   it("reports scanned bytes and execution time, and no billed or slot figure", () => {
      // Snowflake bills warehouse-seconds, so there is no per-query billed
      // quantity, and EXECUTION_TIME is wall clock — a different thing from
      // BigQuery's aggregate slot time. Neither is invented.
      const cost = snowflakeReadCost({
         job_id: "01b2-c3",
         bytes_scanned: 4_500_000,
         execution_time_ms: 812,
         rows_produced: 66,
      });
      expect(cost).toEqual({
         engine: "snowflake",
         jobId: "01b2-c3",
         parentJobId: null,
         bytesScanned: 4_500_000,
         bytesBilled: null,
         slotTimeMs: null,
         executionTimeMs: 812,
         cacheHit: false,
      });
   });

   it("leaves cacheHit null rather than false when an input is missing", () => {
      const cost = snowflakeReadCost({
         job_id: "q",
         bytes_scanned: null,
         rows_produced: 66,
      });
      expect(cost.cacheHit).toBeNull();
   });

   it("over-reports a cache hit for anything that produces rows without scanning", () => {
      // Documented rather than fixed: measured live, a GENERATOR returning 50,000
      // rows reports BYTES_SCANNED 0 and is indistinguishable from a cached read.
      // A build's read scans real tables, so the shape barely arises there.
      const cost = snowflakeReadCost({
         job_id: "q",
         bytes_scanned: 0,
         rows_produced: 50_000,
      });
      expect(cost.cacheHit).toBe(true);
   });
});
