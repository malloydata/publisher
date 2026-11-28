// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The warehouse conformance fixture's tables, generated in the warehouse
 * itself: the conformance fixture's own store (`orders`, `customers`,
 * `lines`, the same rows as its CSVs) so the DuckDB shapes run unchanged
 * over it, and two tables of the types only a warehouse has:
 *
 *  - `typed`: integers past 2^53 (and their negatives), a NUMERIC with nine
 *    decimal places, a zoned timestamp across offsets, a quoted mixed-case
 *    column, and NULLs in every column but the key;
 *  - `nested`: an array of strings and a struct, empty and NULL among them.
 *
 * Every value comes from a fixed seed, so two runs build the same tables.
 * A few hundred rows in all: the point is the types, not the volume.
 */

import type { Connection } from "@malloydata/malloy";
import { DATA_FILES } from "../data";
import type { Dialect, Scratch } from "./targets";

export type ColumnType =
   | "int"
   | "float"
   | "numeric"
   | "string"
   | "date"
   | "timestamp"
   | "timestamptz"
   | "string_array"
   | "struct";

export interface FixtureColumn {
   name: string;
   type: ColumnType;
}

export type Value =
   | string
   | number
   | bigint
   | null
   | string[]
   | { k: number; label: string };

export interface FixtureTable {
   name: string;
   columns: FixtureColumn[];
   rows: Value[][];
}

/** mulberry32: a small seeded PRNG, so the typed rows are the same every run. */
function seeded(seed: number): () => number {
   let a = seed >>> 0;
   return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
   };
}

/** The conformance fixture's CSVs, read back with the types its raw model casts them to. */
function fromCsv(
   file: string,
   name: string,
   columns: FixtureColumn[],
): FixtureTable {
   const [header, ...lines] = DATA_FILES[file].split("\n");
   const names = header.split(",");
   if (names.join() !== columns.map((c) => c.name).join()) {
      throw new Error(`${file}: columns ${names.join()} are not the fixture's`);
   }
   const rows = lines.map((line) =>
      line.split(",").map((cell, i): Value => {
         if (cell === "") return null;
         const text = cell.replace(/^"|"$/g, "");
         const type = columns[i].type;
         return type === "int" || type === "float" ? Number(text) : text;
      }),
   );
   return { name, columns, rows };
}

const ORDERS = fromCsv("data/orders.csv", "orders", [
   { name: "id", type: "int" },
   { name: "customer_id", type: "int" },
   { name: "region", type: "string" },
   { name: "status", type: "string" },
   { name: "amount", type: "float" },
   { name: "discount", type: "float" },
   { name: "order_date", type: "date" },
   { name: "created_at", type: "timestamp" },
   { name: "order", type: "int" },
]);

const CUSTOMERS = fromCsv("data/customers.csv", "customers", [
   { name: "id", type: "int" },
   { name: "name", type: "string" },
   { name: "segment", type: "string" },
   { name: "home_region", type: "string" },
   { name: "signup_date", type: "date" },
]);

const LINES = fromCsv("data/lines.csv", "lines", [
   { name: "line_id", type: "int" },
   { name: "order_id", type: "int" },
   { name: "sku", type: "string" },
   { name: "qty", type: "int" },
]);

const OFFSETS = ["+00:00", "+05:30", "-08:00", "+09:00", "-03:30"];
const KINDS = ["Alpha", "beta", "GAMMA"];

/** 240 rows of the types a warehouse has and the CSVs do not. */
function typed(): FixtureTable {
   const rand = seeded(7098);
   const rows: Value[][] = [];
   for (let id = 1; id <= 240; id++) {
      const nullRow = id % 17 === 0;
      // Past 2^53 = 9007199254740992, where a JavaScript number rounds.
      const big =
         9007199254740993n + BigInt(Math.floor(rand() * 1_000_000)) * 7n;
      const signedBig = id % 5 === 0 ? -big : big;
      const whole = Math.floor(rand() * 10_000_000_000_000);
      const frac = String(Math.floor(rand() * 1_000_000_000)).padStart(9, "0");
      const day = 1 + Math.floor(rand() * 28);
      const hour = Math.floor(rand() * 24);
      const month = 1 + (id % 12);
      const offset = OFFSETS[id % OFFSETS.length];
      rows.push([
         id,
         nullRow ? null : signedBig,
         nullRow ? null : `${id % 3 === 0 ? "-" : ""}${whole}.${frac}`,
         nullRow
            ? null
            : `2024-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")} ${String(hour).padStart(2, "0")}:30:00${offset}`,
         id % 11 === 0 ? null : KINDS[id % KINDS.length],
         `g${id % 4}`,
      ]);
   }
   return {
      name: "typed",
      columns: [
         { name: "id", type: "int" },
         { name: "big", type: "int" },
         { name: "dec", type: "numeric" },
         { name: "ts", type: "timestamptz" },
         { name: "MixedCase", type: "string" },
         { name: "g", type: "string" },
      ],
      rows,
   };
}

const TAGS = ["red", "green", "blue", "cyan"];

/** 60 rows with an array and a struct, empty and NULL among them. */
function nested(): FixtureTable {
   const rand = seeded(1859);
   const rows: Value[][] = [];
   for (let id = 1; id <= 60; id++) {
      const n = id % 7 === 0 ? 0 : 1 + Math.floor(rand() * 3);
      const tags = Array.from(
         { length: n },
         (_, i) => TAGS[(id + i) % TAGS.length],
      );
      rows.push([
         id,
         id % 13 === 0 ? null : tags,
         id % 9 === 0 ? null : { k: id % 5, label: `L${id % 3}` },
      ]);
   }
   return {
      name: "nested",
      columns: [
         { name: "id", type: "int" },
         { name: "tags", type: "string_array" },
         { name: "attrs", type: "struct" },
      ],
      rows,
   };
}

/** Every table the fixture creates, in creation order. */
export const FIXTURE_TABLES: FixtureTable[] = [
   ORDERS,
   CUSTOMERS,
   LINES,
   typed(),
   nested(),
];

function sqlType(dialect: Dialect, type: ColumnType): string {
   const types: Record<Dialect, Record<ColumnType, string>> = {
      bigquery: {
         int: "INT64",
         float: "FLOAT64",
         numeric: "NUMERIC",
         string: "STRING",
         date: "DATE",
         timestamp: "TIMESTAMP",
         timestamptz: "TIMESTAMP",
         string_array: "ARRAY<STRING>",
         struct: "STRUCT<k INT64, label STRING>",
      },
      snowflake: {
         int: "NUMBER(38,0)",
         float: "FLOAT",
         numeric: "NUMBER(38,9)",
         string: "VARCHAR",
         date: "DATE",
         timestamp: "TIMESTAMP_NTZ",
         timestamptz: "TIMESTAMP_TZ",
         string_array: "ARRAY",
         struct: "OBJECT",
      },
      postgres: {
         int: "BIGINT",
         float: "DOUBLE PRECISION",
         numeric: "NUMERIC(38,9)",
         string: "TEXT",
         date: "DATE",
         timestamp: "TIMESTAMP",
         timestamptz: "TIMESTAMPTZ",
         string_array: "TEXT[]",
         struct: "JSONB",
      },
   };
   return types[dialect][type];
}

/** A column name, quoted so its case is kept (`MixedCase`, and the reserved `order`). */
function quoteColumn(dialect: Dialect, name: string): string {
   return dialect === "bigquery" ? `\`${name}\`` : `"${name}"`;
}

function str(value: string): string {
   if (/['\\]/.test(value))
      throw new Error(`unquotable fixture value ${value}`);
   return `'${value}'`;
}

function literal(dialect: Dialect, type: ColumnType, value: Value): string {
   if (value === null) {
      // BigQuery types an untyped NULL in VALUES by the column it lands in;
      // a NULL array or struct needs the type spelled.
      return dialect === "bigquery" &&
         (type === "string_array" || type === "struct")
         ? `CAST(NULL AS ${sqlType(dialect, type)})`
         : "NULL";
   }
   switch (type) {
      case "int":
      case "float":
         return String(value);
      case "numeric":
         return `CAST(${str(String(value))} AS ${sqlType(dialect, type)})`;
      case "string":
         return str(String(value));
      case "date":
         return `CAST(${str(String(value))} AS DATE)`;
      case "timestamp":
         return `CAST(${str(String(value))} AS ${sqlType(dialect, type)})`;
      case "timestamptz":
         return dialect === "snowflake"
            ? `TO_TIMESTAMP_TZ(${str(String(value))}, 'YYYY-MM-DD HH24:MI:SSTZH:TZM')`
            : `CAST(${str(String(value))} AS ${sqlType(dialect, type)})`;
      case "string_array": {
         const items = (value as string[]).map(str);
         return dialect === "bigquery"
            ? `CAST([${items.join(", ")}] AS ARRAY<STRING>)`
            : dialect === "snowflake"
              ? `ARRAY_CONSTRUCT(${items.join(", ")})`
              : `ARRAY[${items.join(", ")}]::TEXT[]`;
      }
      case "struct": {
         const { k, label } = value as { k: number; label: string };
         return dialect === "bigquery"
            ? `STRUCT<k INT64, label STRING>(${k}, ${str(label)})`
            : dialect === "snowflake"
              ? `OBJECT_CONSTRUCT('k', ${k}, 'label', ${str(label)})`
              : `jsonb_build_object('k', ${k}, 'label', ${str(label)})`;
      }
   }
}

/**
 * The statements that create one table and fill it. Snowflake refuses a
 * function call (ARRAY_CONSTRUCT) in VALUES, so it inserts from a UNION ALL
 * of SELECTs; the others insert VALUES.
 */
export function tableStatements(
   dialect: Dialect,
   scratch: Scratch,
   table: FixtureTable,
): string[] {
   const path = sqlTable(dialect, scratch, table.name);
   const cols = table.columns.map((c) => quoteColumn(dialect, c.name));
   const statements = [
      `CREATE TABLE ${path} (${table.columns
         .map((c, i) => `${cols[i]} ${sqlType(dialect, c.type)}`)
         .join(", ")})`,
   ];
   const rowSQL = (row: Value[]) =>
      row.map((v, i) => literal(dialect, table.columns[i].type, v));
   // One statement per 100 rows keeps each well under every statement limit.
   for (let at = 0; at < table.rows.length; at += 100) {
      const chunk = table.rows.slice(at, at + 100);
      statements.push(
         dialect === "snowflake"
            ? `INSERT INTO ${path} (${cols.join(", ")}) ${chunk
                 .map((r) => `SELECT ${rowSQL(r).join(", ")}`)
                 .join(" UNION ALL ")}`
            : `INSERT INTO ${path} (${cols.join(", ")}) VALUES ${chunk
                 .map((r) => `(${rowSQL(r).join(", ")})`)
                 .join(", ")}`,
      );
   }
   return statements;
}

/** A fixture table's path in SQL: created unquoted on Snowflake, so upper case. */
export function sqlTable(
   dialect: Dialect,
   scratch: Scratch,
   table: string,
): string {
   const path = scratch.tablePath(table);
   if (dialect === "bigquery") return `\`${path}\``;
   if (dialect === "postgres") {
      return path
         .split(".")
         .map((p) => `"${p}"`)
         .join(".");
   }
   return path;
}

/** Create and fill every fixture table in the scratch area. */
export async function createFixtureTables(
   connection: Connection,
   scratch: Scratch,
): Promise<void> {
   for (const table of FIXTURE_TABLES) {
      for (const sql of tableStatements(scratch.dialect, scratch, table)) {
         await connection.runSQL(sql, { rowLimit: 10 });
      }
   }
}

/**
 * The raw sources every shape starts from (the DuckDB fixture's `raw.malloy`),
 * over the warehouse's tables, plus the warehouse-typed ones.
 */
export function warehouseRawModel(scratch: Scratch): string {
   const t = (name: string) => `wh.table('${scratch.tablePath(name)}')`;
   // The store's tables through SQL, as the DuckDB fixture reads its CSVs
   // (duckdb.sql(...)): v1 #@ persist builds an extension of a SQL source
   // but refuses one of a bare table (persist-not-materializable).
   const q = (name: string) =>
      `wh.sql("""\n  SELECT * FROM ${sqlTable(scratch.dialect, scratch, name)}\n""")`;
   return `source: orders_raw is ${q("orders")} extend { primary_key: id }

source: customers_raw is ${q("customers")} extend { primary_key: id }

source: lines_raw is ${q("lines")} extend { primary_key: line_id }

source: typed_raw is ${t("typed")} extend { primary_key: id }

source: nested_raw is ${t("nested")} extend { primary_key: id }
`;
}
