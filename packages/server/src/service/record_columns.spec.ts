// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { rankLexically } from "../mcp/tools/schema_index";
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
