// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { MalloyConfig } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { fileInScope, sameCatalogTablesReadable } from "./same_catalog_tables";

const ROOT = "s3://bucket/lake/";
const SCOPE = [`${ROOT}main/`, `${ROOT}sales/`];

describe("fileInScope", () => {
   it("admits a file under a listed schema's prefix", () => {
      expect(fileInScope(`${ROOT}main/c/x.parquet`, SCOPE)).toBe(true);
      expect(fileInScope(`${ROOT}sales/o/x.parquet`, SCOPE)).toBe(true);
   });

   it("refuses another schema's file, and a prefix that only starts the same", () => {
      expect(fileInScope(`${ROOT}hr/salaries/x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope(`${ROOT}main_evil/x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope(`${ROOT}main`, SCOPE)).toBe(false);
   });

   it("refuses a path that could resolve elsewhere than it reads", () => {
      expect(fileInScope(`${ROOT}main/../hr/x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope(`${ROOT}main/./x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope(`${ROOT}main/%2e%2e/hr/x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope(`${ROOT}main\\..\\hr\\x.parquet`, SCOPE)).toBe(false);
   });

   it("compares byte for byte, as a secret's SCOPE does", () => {
      expect(fileInScope(`${ROOT}MAIN/x.parquet`, SCOPE)).toBe(false);
      expect(fileInScope("S3://bucket/lake/main/x.parquet", SCOPE)).toBe(false);
   });
});

/**
 * A destination whose catalog `credible` is at `state.snapshot`, holding
 * `state.types`, with `state.files` per table, under a data path of `ROOT`.
 */
function destination(state: {
   snapshot: number;
   types: Record<string, string>;
   files: Record<string, string[]>;
   fail?: boolean;
}) {
   const sql: string[] = [];
   const rowsFor = (text: string): Record<string, unknown>[] => {
      if (text.includes("information_schema")) {
         const rows = Object.entries(state.types)
            .filter(([name]) => text.includes(`'${name.split(".")[1]}'`))
            .map(([name, type]) => ({
               snapshot_id: state.snapshot,
               table_schema: name.split(".")[0],
               table_name: name.split(".")[1],
               table_type: type,
            }));
         return rows.length ? rows : [{ snapshot_id: state.snapshot }];
      }
      if (text.includes(".ducklake_metadata ")) return [{ value: ROOT }];
      if (text.includes("ducklake_schema")) {
         return ["main", "sales", "hr"].map((schema_name) => ({
            schema_name,
            path: `${schema_name}/`,
            path_is_relative: true,
         }));
      }
      if (text.includes("ducklake_list_files")) {
         const table = Object.keys(state.files).find((t) =>
            text.includes(`'${t}'`),
         );
         return (state.files[table ?? ""] ?? []).map((data_file) => ({
            data_file,
            delete_file: null,
         }));
      }
      return [{ id: state.snapshot }];
   };
   const connection = {
      runSQL: async (text: string) => {
         sql.push(text);
         if (state.fail) throw new Error("catalog unreachable");
         return { rows: rowsFor(text) };
      },
   };
   const config = {
      connections: { lookupConnection: async () => connection },
   } as unknown as MalloyConfig;
   return { config, sql };
}

const READ = {
   alias: "a",
   connection: "credible",
   schemas: new Set(["main"]),
   tables: [["credible", "main", "customers"]] as const,
};

describe("sameCatalogTablesReadable", () => {
   it("probes once per catalog snapshot, and again once the catalog changes", async () => {
      const state = {
         snapshot: 7,
         types: { "main.customers": "BASE TABLE" },
         files: { customers: [`${ROOT}main/customers/a.parquet`] },
      };
      const { config, sql } = destination(state);
      const probes = () =>
         sql.filter((s) => s.includes("information_schema")).length;
      expect(await sameCatalogTablesReadable(config, READ)).toBe(true);
      expect(await sameCatalogTablesReadable(config, READ)).toBe(true);
      // The second read is the snapshot id alone.
      expect(probes()).toBe(1);
      // A writer swaps the table for a view: a new snapshot, probed again.
      state.snapshot = 8;
      state.types = { "main.customers": "VIEW" };
      expect(await sameCatalogTablesReadable(config, READ)).toBe(false);
      expect(await sameCatalogTablesReadable(config, READ)).toBe(false);
      expect(probes()).toBe(2);
   });

   it("is false for a base table holding another schema's file", async () => {
      const { config } = destination({
         snapshot: 3,
         types: { "main.customers": "BASE TABLE" },
         files: {
            customers: [
               `${ROOT}main/customers/a.parquet`,
               `${ROOT}hr/salaries/b.parquet`,
            ],
         },
      });
      expect(await sameCatalogTablesReadable(config, READ)).toBe(false);
   });

   it("admits a file of a readable schema the connection lists", async () => {
      const { config } = destination({
         snapshot: 3,
         types: { "main.customers": "BASE TABLE" },
         files: { customers: [`${ROOT}sales/orders/b.parquet`] },
      });
      expect(
         await sameCatalogTablesReadable(config, {
            ...READ,
            schemas: new Set(["main", "sales"]),
         }),
      ).toBe(true);
      expect(await sameCatalogTablesReadable(config, READ)).toBe(false);
   });

   it("is false for a table it does not find", async () => {
      const { config } = destination({ snapshot: 1, types: {}, files: {} });
      expect(await sameCatalogTablesReadable(config, READ)).toBe(false);
   });

   it("throws when the catalog cannot be read, which the caller takes as no", async () => {
      const { config } = destination({
         snapshot: 1,
         types: {},
         files: {},
         fail: true,
      });
      await expect(sameCatalogTablesReadable(config, READ)).rejects.toThrow();
   });
});
