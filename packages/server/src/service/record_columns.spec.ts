// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { capColumns, rankLexically } from "../mcp/tools/schema_index";
import { columnsOfSchema, listTablesForSchema } from "./db_utils";
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
