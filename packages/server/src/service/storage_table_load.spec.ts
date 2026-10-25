// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import { loadStorageTable, readerSql } from "./storage_table_load";

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
      expect(readerSql("gs://b/x.parquet", "parquet")).toBe("read_parquet('gs://b/x.parquet')");
      expect(readerSql("gs://b/x.json", "json")).toBe("read_json_auto('gs://b/x.json')");
      expect(readerSql("gs://b/x.csv", "csv", { header: true, delimiter: ";", skip: 2 })).toBe(
         "read_csv('gs://b/x.csv', header = true, delim = ';', skip = 2)",
      );
      expect(readerSql("gs://b/x.xlsx", "xlsx", { sheet: "Q3 '24" })).toBe(
         "read_xlsx('gs://b/x.xlsx', sheet = 'Q3 ''24')",
      );
   });

   it("refuses an option that could carry text into SQL", () => {
      expect(() => readerSql("u", "csv", { delimiter: "', x := 1, '" })).toThrow(BadRequestError);
      expect(() => readerSql("u", "csv", { skip: "1; DROP" as unknown as number })).toThrow(
         BadRequestError,
      );
   });

   it("escapes a quote in the object path", () => {
      expect(readerSql("gs://b/o'brien.csv", "csv")).toBe("read_csv('gs://b/o''brien.csv')");
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
      for (const uri of ["gs://bucket/other/x.csv", "gs://bucket/ds_x_evil/x.csv", "gs://bucket/ds_x"]) {
         await expect(loadStorageTable({ ...base, uri })).rejects.toBeInstanceOf(BadRequestError);
      }
   });

   it("takes plain identifiers only", async () => {
      await expect(
         loadStorageTable({ ...base, tableName: "x; DROP", uri: "gs://bucket/ds_x/a.csv" }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });

   it("loads only into DuckLake", async () => {
      await expect(
         loadStorageTable({
            ...base,
            destinationConnection: { name: "sales", type: "duckdb" } as ApiConnection,
            uri: "gs://bucket/ds_x/a.csv",
         }),
      ).rejects.toBeInstanceOf(BadRequestError);
   });
});
