// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The bound that keeps a WIDE DuckLake write from buffering a whole row group per
// column. Asserted on the SQL actually issued, against a recording stub, so an
// unset value proving to be a no-op is a real assertion rather than an absence of
// one -- and so the catalog-scoped CALL cannot silently become a session SET.
import { afterEach, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { applyDuckLakeRowGroupBound } from "./connection";
import { assertDuckDBResourceConfig } from "../config";

function recorder(): { conn: DuckDBConnection; sql: string[] } {
   const sql: string[] = [];
   const conn = {
      runSQL: async (q: string) => {
         sql.push(q);
         return { rows: [], totalRows: 0 };
      },
   } as unknown as DuckDBConnection;
   return { conn, sql };
}

describe("applyDuckLakeRowGroupBound", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES;
   });

   it("issues nothing when unset, so an existing deployment attaches exactly as before", async () => {
      const { conn, sql } = recorder();
      await applyDuckLakeRowGroupBound(conn, "lake");
      expect(sql).toEqual([]);
   });

   it("clears preserve_insertion_order BEFORE the option, which DuckDB refuses without it", async () => {
      process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES = "16MB";
      const { conn, sql } = recorder();
      await applyDuckLakeRowGroupBound(conn, "lake");
      expect(sql).toEqual([
         "SET preserve_insertion_order=false",
         "CALL lake.set_option('parquet_row_group_size_bytes', '16MB')",
      ]);
   });

   it("does not fail the attach when the catalog refuses the option", async () => {
      // A lake this deployment may read but not re-configure must still serve.
      process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES = "16MB";
      const conn = {
         runSQL: async () => {
            throw new Error("permission denied for table ducklake_metadata");
         },
      } as unknown as DuckDBConnection;
      await applyDuckLakeRowGroupBound(conn, "lake");
   });
});

describe("row group bound configuration", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES;
   });

   it("rejects a byte size with no unit, which DuckDB cannot read", () => {
      process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES = "16000000";
      expect(() => assertDuckDBResourceConfig()).toThrow(
         /PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES/,
      );
   });
});
