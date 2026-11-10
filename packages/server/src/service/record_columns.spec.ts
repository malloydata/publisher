// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { capColumns, rankLexically } from "../retrieval/index/schema_index";
import {
   columnsOfSchema,
   listTablesForSchema,
   schemaReadLimits,
} from "./db_utils";
import type { ApiConnection } from "./model";

describe("columnsOfSchema", () => {
   it("lists a BigQuery RECORD's members, a repeated record's members, and no members for a plain array", () => {
      // The shapes the BigQuery connector builds (addFieldsToStructDef).
      const columns = columnsOfSchema([
         { name: "order_id", type: "number" },
         {
            name: "delivery",
            type: "record",
            fields: [
               { name: "city", type: "string" },
               {
                  name: "address",
                  type: "record",
                  fields: [{ name: "zip", type: "string" }],
               },
            ],
         },
         {
            name: "lines",
            type: "array",
            elementTypeDef: { type: "record_element" },
            fields: [{ name: "sku", type: "string" }],
         },
         {
            name: "tags",
            type: "array",
            elementTypeDef: { type: "string" },
            fields: [{ name: "each", type: "string" }],
         },
      ]);
      expect(columns).toEqual([
         { name: "order_id", type: "number" },
         { name: "delivery", type: "record" },
         { name: "delivery.city", type: "string" },
         { name: "delivery.address", type: "record" },
         { name: "delivery.address.zip", type: "string" },
         { name: "lines", type: "array" },
         { name: "lines.sku", type: "string" },
         { name: "tags", type: "array" },
      ]);
   });

   it("leaves out a member whose name is not a plain identifier, and everything under it", () => {
      const columns = columnsOfSchema([
         { name: "my col", type: "string" },
         {
            name: "odd",
            type: "record",
            fields: [
               { name: "ok", type: "number" },
               { name: "a.b", type: "number" },
               { name: "two words", type: "number" },
               {
                  name: "x y",
                  type: "record",
                  fields: [{ name: "z", type: "number" }],
               },
            ],
         },
      ]);
      // `odd.a.b` would read as a nested record. A top-level column is the
      // database's own name, so it stays.
      expect(columns.map((c) => c.name)).toEqual(["my col", "odd", "odd.ok"]);
   });
});

describe("a DuckDB schema with STRUCT columns", () => {
   const malloy = new DuckDBConnection("duckdb", ":memory:");
   const conn: ApiConnection = {
      name: "duckdb",
      type: "duckdb",
      duckdbConnection: {},
   };
   beforeAll(async () => {
      await malloy.runSQL(
         "create table orders as select 1 as order_id, {'city': 'Denver', 'address': {'zip': '80202'}} as delivery, [{'sku': 'a', 'qty': 2}] as lines, ['gift'] as tags",
      );
      await malloy.runSQL(
         "create table shipments as select 1 as shipment_id, 'Denver' as warehouse_town",
      );
   });
   afterAll(() => malloy.close());

   it("lists each STRUCT's members as dotted columns, and leaves a flat table as it was", async () => {
      const tables = await listTablesForSchema(conn, "memory.main", malloy);
      const columnsOf = (table: string) =>
         tables
            .find((t) => t.resource?.endsWith(`.${table}`))
            ?.columns?.map((c) => `${c.name}:${c.type}`);
      // Top-level columns keep the type DuckDB reported; the members, which
      // it did not report, carry Malloy's.
      expect(columnsOf("orders")).toEqual([
         "order_id:integer",
         "delivery:struct(city varchar, address struct(zip varchar))",
         "delivery.city:string",
         "delivery.address:record",
         "delivery.address.zip:string",
         "lines:struct(sku varchar, qty integer)[]",
         "lines.sku:string",
         "lines.qty:number",
         "tags:varchar[]",
      ]);
      // No STRUCT column: INFORMATION_SCHEMA's columns, untouched.
      expect(columnsOf("shipments")).toEqual([
         "shipment_id:integer",
         "warehouse_town:varchar",
      ]);
      expect(tables.some((t) => t.membersUnread)).toBe(false);
   });

   it("follows the database when a table changes after a listing, never a cached schema", async () => {
      // Every step in one millisecond: Malloy re-reads its cached schema only
      // for a strictly newer refresh timestamp, so the listing must not take
      // its timestamp from the clock alone.
      const now = spyOn(Date, "now").mockReturnValue(Date.now());
      try {
         await changesAfterAListing();
      } finally {
         now.mockRestore();
      }
   });

   async function changesAfterAListing() {
      await malloy.runSQL(
         "create table changing as select 1 as id, {'city': 'Denver'} as place",
      );
      const columnsOf = async () =>
         (await listTablesForSchema(conn, "memory.main", malloy, ["changing"]))
            .find((t) => t.resource?.endsWith(".changing"))
            ?.columns?.map((c) => c.name);
      expect(await columnsOf()).toEqual(["id", "place", "place.city"]);
      await malloy.runSQL("alter table changing add column note varchar");
      expect(await columnsOf()).toEqual(["id", "place", "place.city", "note"]);
      await malloy.runSQL("drop table changing");
      await malloy.runSQL(
         "create table changing as select 1 as id, {'town': 'Boulder'} as place",
      );
      expect(await columnsOf()).toEqual(["id", "place", "place.town"]);
      await malloy.runSQL("drop table changing");
   }

   it("reads a table's schema again only when a record column's type changed", async () => {
      await malloy.runSQL(
         "create table steady as select 1 as id, {'city': 'Denver'} as place",
      );
      const reads = spyOn(malloy, "fetchSchemaForTables");
      try {
         const list = () =>
            listTablesForSchema(conn, "memory.main", malloy, ["steady"]);
         await list();
         expect(reads).toHaveBeenCalledTimes(1);
         // A new plain column leaves `place`'s type as it was: no read.
         await malloy.runSQL("alter table steady add column note varchar");
         const again = await list();
         expect(reads).toHaveBeenCalledTimes(1);
         expect(again[0].columns?.map((c) => c.name)).toEqual([
            "id",
            "place",
            "place.city",
            "note",
         ]);
         await malloy.runSQL("alter table steady drop column place");
         await malloy.runSQL(
            "alter table steady add column place struct(town varchar)",
         );
         await list();
         expect(reads).toHaveBeenCalledTimes(2);
      } finally {
         reads.mockRestore();
         await malloy.runSQL("drop table steady");
      }
   });

   it("marks a table whose record members could not be read, and lists its own columns as reported", async () => {
      await malloy.runSQL(
         "create table unread as select 1 as id, {'city': 'Denver'} as place",
      );
      try {
         const reads = spyOn(malloy, "fetchSchemaForTables").mockRejectedValue(
            new Error("warehouse unavailable"),
         );
         try {
            const [table] = await listTablesForSchema(
               conn,
               "memory.main",
               malloy,
               ["unread"],
            );
            expect(table.membersUnread).toBe(true);
            expect(table.columns?.map((c) => c.name)).toEqual(["id", "place"]);
         } finally {
            reads.mockRestore();
         }
         // Read once it can be, and no longer marked.
         const [table] = await listTablesForSchema(
            conn,
            "memory.main",
            malloy,
            ["unread"],
         );
         expect(table.membersUnread).toBeUndefined();
         expect(table.columns?.map((c) => c.name)).toEqual([
            "id",
            "place",
            "place.city",
         ]);
      } finally {
         await malloy.runSQL("drop table unread");
      }
   });

   it("starts each listing's reads with the tables tried longest ago, so the time limit cannot starve the rest", async () => {
      const names = Array.from({ length: 40 }, (_, i) => `slow${i}`);
      for (const name of names) {
         await malloy.runSQL(
            `create table ${name} as select 1 as id, {'city': 'Denver'} as place`,
         );
      }
      const tried = new Set<string>();
      // Every read fails, slowly, so nothing is ever cached and each listing
      // reads again: only the order the reads start in decides coverage.
      const reads = spyOn(malloy, "fetchSchemaForTables").mockImplementation(
         async (tables: Record<string, string>) => {
            for (const key of Object.keys(tables)) tried.add(key);
            await new Promise((r) => setTimeout(r, 40));
            throw new Error("warehouse unavailable");
         },
      );
      const deadline = schemaReadLimits.deadlineMs;
      schemaReadLimits.deadlineMs = 60;
      try {
         const list = () =>
            listTablesForSchema(conn, "memory.main", malloy, names);
         await list();
         // One listing reaches only some of the 40 within the limit...
         expect(tried.size).toBeLessThan(names.length);
         await list();
         await list();
         // ...and the next ones start with those not yet tried.
         expect(tried.size).toBe(names.length);
      } finally {
         schemaReadLimits.deadlineMs = deadline;
         reads.mockRestore();
         for (const name of names) await malloy.runSQL(`drop table ${name}`);
      }
   });

   it("reads a table once when listings overlap, and gives each its members", async () => {
      await malloy.runSQL(
         "create table shared as select 1 as id, {'city': 'Denver'} as place",
      );
      const reads = spyOn(malloy, "fetchSchemaForTables");
      try {
         const listings = await Promise.all(
            [1, 2, 3].map(() =>
               listTablesForSchema(conn, "memory.main", malloy, ["shared"]),
            ),
         );
         expect(reads).toHaveBeenCalledTimes(1);
         for (const [table] of listings) {
            expect(table.columns?.map((c) => c.name)).toEqual([
               "id",
               "place",
               "place.city",
            ]);
         }
      } finally {
         reads.mockRestore();
         await malloy.runSQL("drop table shared");
      }
   });

   it("neither reads nor marks a table whose name Malloy will not read unquoted", async () => {
      await malloy.runSQL(
         `create table "my-tbl" as select 1 as id, {'city': 'Denver'} as place`,
      );
      const reads = spyOn(malloy, "fetchSchemaForTables");
      try {
         const [table] = await listTablesForSchema(
            conn,
            "memory.main",
            malloy,
            ["my-tbl"],
         );
         expect(reads).not.toHaveBeenCalled();
         expect(table.membersUnread).toBeUndefined();
         expect(table.columns?.map((c) => c.name)).toEqual(["id", "place"]);
      } finally {
         reads.mockRestore();
         await malloy.runSQL(`drop table "my-tbl"`);
      }
   });

   it("ranks the table by a member's name, which matched nothing before", async () => {
      const tables = (
         await listTablesForSchema(conn, "memory.main", malloy)
      ).map((t) => ({
         connectionName: "duckdb",
         schemaName: "memory.main",
         tableName: t.resource!.split(".").at(-1)!,
         resource: t.resource!,
         columns: (t.columns ?? []).map((c) => ({
            name: c.name!,
            type: c.type,
         })),
      }));
      for (const query of ["city", "zip code", "sku"]) {
         expect([
            query,
            rankLexically(tables, query, 5).hits.map((h) => h.tableName),
         ]).toEqual([query, ["orders"]]);
      }
   });
});

describe("a Databricks schema with struct columns", () => {
   const conn: ApiConnection = {
      name: "dbx",
      type: "databricks",
      databricksConnection: { defaultCatalog: "main" },
   };
   // What information_schema.columns answers once the listing asks for
   // full_data_type on STRUCT and ARRAY columns. Databricks itself cannot run
   // here, so the query's text is checked and its answer is faked.
   function fakeDatabricks(placeType: string) {
      const sql: string[] = [];
      const rows = [
         { table_name: "orders", column_name: "id", data_type: "INT" },
         { table_name: "orders", column_name: "place", data_type: placeType },
         {
            table_name: "orders",
            column_name: "lines",
            data_type: "array<struct<sku:string>>",
         },
         {
            table_name: "orders",
            column_name: "tags",
            data_type: "array<string>",
         },
      ];
      const fields = [
         { name: "id", type: "number" },
         {
            name: "place",
            type: "record",
            fields: [{ name: "city", type: "string" }],
         },
         {
            name: "lines",
            type: "array",
            elementTypeDef: { type: "record_element" },
            fields: [{ name: "sku", type: "string" }],
         },
         {
            name: "tags",
            type: "array",
            elementTypeDef: { type: "string" },
         },
      ];
      const connection = {
         dialectName: "databricks",
         runSQL: async (text: string) => {
            sql.push(text);
            return { rows };
         },
         fetchSchemaForTables: async (tables: Record<string, string>) => ({
            schemas: Object.fromEntries(
               Object.keys(tables).map((k) => [k, { fields }]),
            ),
            errors: {},
         }),
      };
      return { connection, sql };
   }

   it("reads full_data_type, so an array of structs is expanded and an unchanged struct is not read again", async () => {
      const { connection, sql } = fakeDatabricks("struct<city:string>");
      const malloyConnection = connection as unknown as Parameters<
         typeof listTablesForSchema
      >[2];
      const reads = spyOn(connection, "fetchSchemaForTables");
      const [table] = await listTablesForSchema(
         conn,
         "sales",
         malloyConnection,
      );
      expect(sql[0]).toContain(
         "CASE WHEN upper(data_type) IN ('STRUCT', 'ARRAY') THEN coalesce(full_data_type, data_type) ELSE data_type END AS data_type",
      );
      expect(table.columns).toEqual([
         { name: "id", type: "int" },
         { name: "place", type: "struct<city:string>" },
         { name: "place.city", type: "string" },
         { name: "lines", type: "array<struct<sku:string>>" },
         { name: "lines.sku", type: "string" },
         { name: "tags", type: "array<string>" },
      ]);
      expect(reads).toHaveBeenCalledTimes(1);
      // The struct's type spells its members, so an unchanged one is cached.
      await listTablesForSchema(conn, "sales", malloyConnection);
      expect(reads).toHaveBeenCalledTimes(1);
   });

   it("reads a bare struct on every listing and never serves it from the cache", async () => {
      const { connection } = fakeDatabricks("struct");
      const malloyConnection = connection as unknown as Parameters<
         typeof listTablesForSchema
      >[2];
      const reads = spyOn(connection, "fetchSchemaForTables");
      await listTablesForSchema(conn, "sales", malloyConnection);
      await listTablesForSchema(conn, "sales", malloyConnection);
      expect(reads).toHaveBeenCalledTimes(2);
      // A failed read is not covered by the last good one: it is marked.
      reads.mockRejectedValueOnce(new Error("warehouse unavailable"));
      const [table] = await listTablesForSchema(
         conn,
         "sales",
         malloyConnection,
      );
      expect(table.membersUnread).toBe(true);
   });
});

describe("capColumns", () => {
   it("keeps a table's own columns before record members, in listed order", () => {
      const columns = [
         { name: "id", type: "integer" },
         { name: "delivery", type: "struct(m0 varchar, m1 varchar)" },
         ...Array.from({ length: 5 }, (_, i) => ({
            name: `delivery.m${i}`,
            type: "string",
         })),
         { name: "a", type: "integer" },
         // A top-level column whose own name holds a dot is not a member,
         // even when its prefix is another column's name, as long as that
         // column is not a record.
         { name: "a.zip_code", type: "varchar" },
      ];
      expect(capColumns(columns, 5).map((c) => c.name)).toEqual([
         "id",
         "delivery",
         "delivery.m0",
         "a",
         "a.zip_code",
      ]);
   });
});
