// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import {
   loadStorageTable,
   readerRefusal,
   readerSql,
} from "./storage_table_load";

type ApiConnection = components["schemas"]["Connection"];

const dest = {
   name: "sales",
   type: "ducklake",
   ducklakeConnection: {
      storage: { bucketUrl: "gs://bucket/ds_x" },
      catalog: {
         postgresConnection: { host: "h", databaseName: "d", userName: "u" },
         metadataSchema: "ds_x",
      },
   },
} as ApiConnection;

describe("readerSql renders only allowlisted, typed options", () => {
   it("reads each format with its own reader", () => {
      expect(readerSql("gs://b/x.parquet", "parquet")).toBe(
         "read_parquet('gs://b/x.parquet')",
      );
      expect(readerSql("gs://b/x.json", "json")).toBe(
         "read_json_auto('gs://b/x.json')",
      );
      expect(
         readerSql("gs://b/x.csv", "csv", {
            header: true,
            delimiter: ";",
            skip: 2,
         }),
      ).toBe("read_csv('gs://b/x.csv', header = true, delim = ';', skip = 2)");
      expect(readerSql("gs://b/x.xlsx", "xlsx", { sheet: "Q3 '24" })).toBe(
         "read_xlsx('gs://b/x.xlsx', sheet = 'Q3 ''24')",
      );
   });

   it("refuses an option that could carry text into SQL", () => {
      expect(() =>
         readerSql("u", "csv", { delimiter: "', x := 1, '" }),
      ).toThrow(BadRequestError);
      expect(() =>
         readerSql("u", "csv", { skip: "1; DROP" as unknown as number }),
      ).toThrow(BadRequestError);
   });

   it("escapes a quote in the object path", () => {
      expect(readerSql("gs://b/o'brien.csv", "csv")).toBe(
         "read_csv('gs://b/o''brien.csv')",
      );
   });
});

describe("loadStorageTable refuses before attaching", () => {
   const base = {
      destinationName: "sales",
      destinationConnection: dest,
      schemaName: "main",
      tableName: "orders",
      format: "csv" as const,
      mode: "create" as const,
   };

   it("reads nothing outside the destination's own prefix, a sibling prefix included", async () => {
      for (const uri of [
         "gs://bucket/other/x.csv",
         "gs://bucket/ds_x_evil/x.csv",
         "gs://bucket/ds_x",
      ]) {
         await expect(
            loadStorageTable({ ...base, uri }),
         ).rejects.toBeInstanceOf(BadRequestError);
      }
   });

   it("takes plain identifiers only", async () => {
      await expect(
         loadStorageTable({
            ...base,
            tableName: "x; DROP",
            uri: "gs://bucket/ds_x/a.csv",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("loads only into DuckLake", async () => {
      await expect(
         loadStorageTable({
            ...base,
            destinationConnection: {
               name: "sales",
               type: "duckdb",
            } as ApiConnection,
            uri: "gs://bucket/ds_x/a.csv",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });
});

describe("readerRefusal: what a person sent and the reader refused", () => {
   const uri = "s3://lake/org_1/db_2/staging/orders/0e1f/broken.parquet";

   it("is a 400 in the reader's own words, naming the file rather than the staging path, without the SQL", () => {
      const refusal = readerRefusal(
         new Error(
            `Invalid Input Error: No magic bytes found at end of file '${uri}'\n\nLINE 1: ...CREATE OR REPLACE TABLE "db_2"."main"."orders" AS SELECT * FROM read_parquet('${uri}')`,
         ),
         uri,
      );
      expect(refusal).toBeInstanceOf(BadRequestError);
      expect(refusal?.message).toBe(
         "Invalid Input Error: No magic bytes found at end of file 'broken.parquet'",
      );
   });

   it("covers a value the reader cannot convert", () => {
      expect(
         readerRefusal(
            new Error(
               "Conversion Error: Could not convert string 'N/A' to DOUBLE",
            ),
            uri,
         )?.message,
      ).toBe("Conversion Error: Could not convert string 'N/A' to DOUBLE");
   });

   it("leaves a storage or catalog failure alone, to answer 500", () => {
      expect(
         readerRefusal(
            new Error("IO Error: Connection error for HTTP HEAD"),
            uri,
         ),
      ).toBeNull();
      expect(readerRefusal("not an error", uri)).toBeNull();
   });

   // The message shape the patterns above are written against, read from DuckDB itself.
   it("matches what DuckDB says for a file that is not Parquet", async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "reader-refusal-"));
      const file = path.join(dir, "broken.parquet");
      fs.writeFileSync(file, "this is not a parquet file\n".repeat(64));
      const db = new DuckDBConnection("reader-refusal");
      try {
         let thrown: unknown;
         try {
            await db.runSQL(
               `CREATE TABLE t AS SELECT * FROM read_parquet('${file}')`,
            );
         } catch (error) {
            thrown = error;
         }
         const refusal = readerRefusal(thrown, file);
         expect(refusal?.message).toMatch(
            /^Invalid Input Error: No magic bytes found/,
         );
         expect(refusal?.message).not.toContain(dir);
      } finally {
         await db.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
