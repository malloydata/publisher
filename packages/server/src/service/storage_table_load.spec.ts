// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, describe, expect, it } from "bun:test";
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

   it("leaves a storage or catalog failure alone, to answer 500", () => {
      expect(
         readerRefusal(
            new Error("IO Error: Connection error for HTTP HEAD"),
            uri,
         ),
      ).toBeNull();
      // Arithmetic, which a `SELECT *` from a reader never does.
      expect(
         readerRefusal(
            new Error(
               "Out of Range Error: Overflow in multiplication of INT8 (100 * 2)!",
            ),
            uri,
         ),
      ).toBeNull();
      expect(readerRefusal("not an error", uri)).toBeNull();
   });

   // The message shapes the patterns above are written against, read from DuckDB
   // itself, so a DuckDB upgrade that rewords one fails here rather than quietly
   // answering 500 again.
   describe("matches what DuckDB says", () => {
      /**
       * The error DuckDB throws for the load's own statement shape, `CREATE TABLE ... AS SELECT *
       * FROM <reader>`. Every default in-memory DuckDBConnection in this process shares one
       * instance (the driver pools them by path and options, not by name), so the table is
       * named for this spec: a plain `t` collides with one another spec leaves behind, in an
       * order that differs by platform, and the collision's Catalog Error is not the reader's.
       */
      async function readerError(reader: string): Promise<Error> {
         const db = new DuckDBConnection("reader-refusal");
         try {
            await db.runSQL(
               `CREATE TABLE reader_refusal_probe AS SELECT * FROM ${reader}`,
            );
         } catch (error) {
            expect(error).toBeInstanceOf(Error);
            return error as Error;
         } finally {
            try {
               await db.runSQL("DROP TABLE IF EXISTS reader_refusal_probe");
            } finally {
               await db.close();
            }
         }
         throw new Error(`DuckDB read ${reader} without complaint`);
      }

      // Forward slashes, as an object URI has: DuckDB reads them on Windows too.
      const dir = fs
         .mkdtempSync(path.join(os.tmpdir(), "reader-refusal-"))
         .replaceAll(path.sep, "/");
      afterAll(() => fs.rmSync(dir, { recursive: true, force: true }));

      it("for a file that is not Parquet", async () => {
         const file = `${dir}/broken.parquet`;
         fs.writeFileSync(file, "this is not a parquet file\n".repeat(64));
         const thrown = await readerError(`read_parquet('${file}')`);
         // On the error itself first, so a reworded message is shown rather than a null refusal.
         expect(thrown.message).toMatch(
            /^Invalid Input Error: No magic bytes found/,
         );
         const refusal = readerRefusal(thrown, file);
         expect(refusal).toBeInstanceOf(BadRequestError);
         expect(refusal?.message).toContain("'broken.parquet'");
         expect(refusal?.message).not.toContain(dir);
         // The SQL DuckDB echoes after `LINE 1:` names the staging path again.
         expect(refusal?.message).not.toContain("CREATE TABLE");
      });

      it("for a CSV value the column's type cannot hold", async () => {
         const file = `${dir}/orders.csv`;
         fs.writeFileSync(file, "id,amount\n1,10.5\n2,N/A\n3,7\n");
         // A type the reader is told, as a column already in the table is on a
         // replace; a sniffed three-line file would read the column as text.
         const thrown = await readerError(
            `read_csv('${file}', types = {'amount': 'DOUBLE'})`,
         );
         expect(thrown.message).toMatch(
            /^Conversion Error: CSV Error on Line: 3\n/,
         );
         const refusal = readerRefusal(thrown, file);
         expect(refusal).toBeInstanceOf(BadRequestError);
         expect(refusal?.message).toContain(
            `Could not convert string "N/A" to 'DOUBLE'`,
         );
         // The reader's own report names the file: by name, not by staging path.
         expect(refusal?.message).toContain("file = orders.csv");
         expect(refusal?.message).not.toContain(dir);
         expect(refusal?.message).not.toContain("CREATE TABLE");
      });
   });
});
