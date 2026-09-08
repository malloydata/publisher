// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The bound that keeps a LARGE DuckLake write from holding a whole Parquet file
// resident while it uploads. Asserted on the SQL actually issued, against a
// recording stub, for the same reasons as the row group bound beside it: an unset
// value proving to be a no-op is a real assertion, and the catalog-scoped CALL
// cannot silently become a session SET.
import { afterEach, describe, expect, it } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   applyDuckLakeRowGroupBound,
   applyDuckLakeTargetFileSize,
} from "./connection";
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

describe("applyDuckLakeTargetFileSize", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES;
      delete process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES;
   });

   it("issues nothing when unset, so an existing deployment attaches exactly as before", async () => {
      const { conn, sql } = recorder();
      await applyDuckLakeTargetFileSize(conn, "lake");
      expect(sql).toEqual([]);
   });

   it("scopes the option to the catalog, not the session", async () => {
      process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = "256MB";
      const { conn, sql } = recorder();
      await applyDuckLakeTargetFileSize(conn, "lake");
      expect(sql).toEqual([
         "CALL lake.set_option('target_file_size', '256MB')",
      ]);
   });

   it("does not fail the attach when the catalog refuses the option", async () => {
      // A lake this deployment may read but not re-configure must still serve.
      process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = "256MB";
      const conn = {
         runSQL: async () => {
            throw new Error("permission denied for table ducklake_metadata");
         },
      } as unknown as DuckDBConnection;
      await applyDuckLakeTargetFileSize(conn, "lake");
   });

   // The two bounds are separate terms -- the row group bounds the per-column
   // buffer within a file, this bounds the file -- so setting one must not be
   // read as setting the other. Pins them as independent in BOTH directions,
   // because a future refactor that folds one into the other would still pass a
   // test that only checked they can be set together.
   it("is independent of the row group bound in both directions", async () => {
      process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES = "32MB";
      const rowGroupOnly = recorder();
      await applyDuckLakeRowGroupBound(rowGroupOnly.conn, "lake");
      await applyDuckLakeTargetFileSize(rowGroupOnly.conn, "lake");
      expect(rowGroupOnly.sql).toEqual([
         "SET preserve_insertion_order=false",
         "CALL lake.set_option('parquet_row_group_size_bytes', '32MB')",
      ]);

      delete process.env.PUBLISHER_DUCKLAKE_ROW_GROUP_SIZE_BYTES;
      process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = "256MB";
      const fileSizeOnly = recorder();
      await applyDuckLakeRowGroupBound(fileSizeOnly.conn, "lake");
      await applyDuckLakeTargetFileSize(fileSizeOnly.conn, "lake");
      expect(fileSizeOnly.sql).toEqual([
         "CALL lake.set_option('target_file_size', '256MB')",
      ]);
   });
});

describe("target file size configuration", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES;
   });

   it("rejects a byte size with no unit, which DuckDB cannot read", () => {
      process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = "268435456";
      expect(() => assertDuckDBResourceConfig()).toThrow(
         /PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES/,
      );
   });

   // A validator narrower than the engine fails the pod on a value DuckDB would
   // have taken, and says "expected a size like 256MB" while doing it -- which
   // reads as the operator's typo rather than ours. Probed against v1.5.5: it
   // takes TB and TiB here and refuses PB.
   it.each(["1TB", "1TIB", "1tb", "512MB", "1.5GB", "0.5GiB"])(
      "accepts %s, which DuckDB accepts",
      (value) => {
         process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = value;
         expect(() => assertDuckDBResourceConfig()).not.toThrow();
      },
   );

   it.each(["1PB", "268435456", "1GBB", "MB", "0MB", "0"])(
      "rejects %s, which DuckDB refuses or stores uselessly",
      (value) => {
         process.env.PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES = value;
         expect(() => assertDuckDBResourceConfig()).toThrow(
            /PUBLISHER_DUCKLAKE_TARGET_FILE_SIZE_BYTES/,
         );
      },
   );
});
