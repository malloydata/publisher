// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   nativeTypeCatalogBatches,
   nativeTypeCatalogRead,
} from "./warehouse_sql";

/**
 * The catalog reads put a table's name parts in string literals; a bound
 * name comes from a host's manifest or the run store, so a part holding `\`
 * or `'` must never end its literal early. Each statement's literals are
 * read back as the dialect reads them: every literal decodes to a value the
 * read asked for, and nothing outside a literal is the payload.
 */

/** Dialects whose literals read `\` as an escape character. */
const BACKSLASH = new Set(["standardsql", "databricks", "snowflake", "mysql"]);

const DIALECTS = [
   "duckdb",
   "postgres",
   "standardsql",
   "snowflake",
   "databricks",
   "trino",
   "presto",
   "mysql",
   "sqlserver",
   "some_unknown_dialect",
];

const MYSQL_CONCAT = "CONCAT(";
const MYSQL_BACKSLASH = ", CHAR(92 USING utf8mb4), ";

/** The index just past the doubled-quote literal that starts at `sql[i]`. */
function literalEnd(sql: string, i: number): number {
   let j = i + 1;
   for (;;) {
      if (j >= sql.length) throw new Error(`unclosed literal in ${sql}`);
      if (sql[j] === "'") {
         if (sql[j + 1] === "'") {
            j += 2;
            continue;
         }
         return j + 1;
      }
      j++;
   }
}

/**
 * The statement with each string literal taken out, as `dialect` reads it,
 * and the literals' values. Identifier quotes (`"`, `` ` ``) are skipped
 * whole, a doubled quote inside one kept.
 */
function splitLiterals(
   sql: string,
   dialect: string,
): { outside: string; literals: string[] } {
   let outside = "";
   const literals: string[] = [];
   let i = 0;
   while (i < sql.length) {
      const ch = sql[i];
      if (ch === '"' || ch === "`") {
         let j = i + 1;
         for (;;) {
            if (j >= sql.length) throw new Error(`unclosed ${ch} in ${sql}`);
            if (sql[j] === ch) {
               if (sql[j + 1] === ch) {
                  j += 2;
                  continue;
               }
               break;
            }
            j++;
         }
         outside += "<id>";
         i = j + 1;
         continue;
      }
      // MySQL writes a value holding `\\` as CONCAT('…', CHAR(92 USING
      // utf8mb4), '…') (warehouse_sql.ts mysqlString): one value, read back
      // whole, whose pieces hold no backslash.
      if (dialect === "mysql" && sql.startsWith(MYSQL_CONCAT, i)) {
         const pieces: string[] = [];
         let j = i + MYSQL_CONCAT.length;
         for (;;) {
            if (sql[j] !== "'") {
               throw new Error(`a CONCAT piece is not a literal in ${sql}`);
            }
            const end = literalEnd(sql, j);
            pieces.push(sql.slice(j + 1, end - 1).replace(/''/g, "'"));
            j = end;
            if (sql.startsWith(MYSQL_BACKSLASH, j)) {
               j += MYSQL_BACKSLASH.length;
               continue;
            }
            if (sql[j] !== ")") throw new Error(`unclosed CONCAT in ${sql}`);
            break;
         }
         for (const p of pieces) expect(p).not.toContain("\\");
         literals.push(pieces.join("\\"));
         outside += "<lit>";
         i = j + 1;
         continue;
      }
      if (ch === "'") {
         let value = "";
         let j = i + 1;
         for (;;) {
            if (j >= sql.length) throw new Error(`unclosed literal in ${sql}`);
            const c = sql[j];
            // A MySQL literal holding `\\` reads one way with
            // NO_BACKSLASH_ESCAPES and another without: never written.
            if (c === "\\" && dialect === "mysql") {
               throw new Error(`a MySQL literal holds a backslash in ${sql}`);
            }
            if (c === "\\" && BACKSLASH.has(dialect)) {
               value += sql[j + 1];
               j += 2;
               continue;
            }
            if (c === "'") {
               if (sql[j + 1] === "'") {
                  value += "'";
                  j += 2;
                  continue;
               }
               break;
            }
            value += c;
            j++;
         }
         literals.push(value);
         outside += "<lit>";
         i = j + 1;
         continue;
      }
      outside += ch;
      i++;
   }
   return { outside, literals };
}

const HOSTILE = [
   "t\\'; DROP TABLE x --",
   "t'; DROP TABLE x --",
   "t\\",
   "t\\\\'' DROP TABLE x --",
];

describe("the catalog reads escape a name part's literal for each dialect", () => {
   for (const dialect of DIALECTS) {
      for (const hostile of HOSTILE) {
         it(`${dialect}: ${JSON.stringify(hostile)}`, () => {
            const parts = ["sch", hostile];
            const statements = [
               nativeTypeCatalogRead(parts, dialect)?.sql,
               ...nativeTypeCatalogBatches(
                  [
                     { key: "k'\\1", parts },
                     { key: "k2", parts: ["sch2", hostile] },
                  ],
                  dialect,
               ).map((b) => b.sql),
            ].filter((s): s is string => s !== undefined);
            expect(statements.length).toBeGreaterThan(0);
            for (const sql of statements) {
               const { outside, literals } = splitLiterals(sql, dialect);
               expect(outside).not.toContain("DROP");
               // Each literal is a value the read asked for: the name part,
               // a schema, a batch key, or a quoted path holding the part.
               for (const v of literals) {
                  expect(
                     v === hostile ||
                        v === "sch" ||
                        v === "sch2" ||
                        v === "k'\\1" ||
                        v === "k2" ||
                        v.includes(hostile.replace(/"/g, '""')),
                  ).toBe(true);
               }
               // DuckDB's DESCRIBE names the table by quoted identifiers
               // alone; every other read filters by literals.
               if (dialect !== "duckdb") {
                  expect(literals.length).toBeGreaterThan(0);
               }
            }
         });
      }
   }
});
