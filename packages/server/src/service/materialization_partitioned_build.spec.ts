// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A partitioned storage build is three statements rather than a CTAS, and the
// ORDER is the whole point: DuckLake applies a partition layout only to files
// written after it is set, so a layout declared after the rows have landed lays
// out nothing. Asserted twice — on the SQL issued, which is where the order
// lives, and against a real DuckLake, which is the only thing that can show the
// files actually separated.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { describe, expect, it } from "bun:test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { createTableAndDescribe } from "./materialization_build_session";

const ROWS = `SELECT * FROM (VALUES (1,7,'a'),(2,9,'b'),(1,8,'c')) AS t(org_id, user_id, s)`;

function recorder(): { conn: DuckDBConnection; sql: string[] } {
   const sql: string[] = [];
   const conn = {
      runSQL: async (q: string) => {
         sql.push(q);
         return q.startsWith("DESCRIBE")
            ? { rows: [{ column_name: "org_id", column_type: "INTEGER" }] }
            : { rows: [], totalRows: 0 };
      },
   } as unknown as DuckDBConnection;
   return { conn, sql };
}

describe("createTableAndDescribe: statements issued", () => {
   it("issues the single CTAS when nothing is partitioned", async () => {
      // An unpartitioned build must be byte-identical to what it was, so
      // adopting this parameter changes nothing for the sources already built.
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS);
      expect(sql).toEqual([
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS})`,
         'DESCRIBE "lake"."t"',
      ]);
   });

   it("lays the table out before inserting, never after", async () => {
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"]);
      expect(sql).toEqual([
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS}) WITH NO DATA`,
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id")',
         `INSERT INTO "lake"."t" (${ROWS})`,
         'DESCRIBE "lake"."t"',
      ]);
   });

   it("keeps the author's column order, which is the directory nesting", async () => {
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id", "s"]);
      expect(sql[1]).toBe(
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id", "s")',
      );
   });

   it("drops the empty table when the layout fails, leaving nothing stranded", async () => {
      // Unlike the CTAS path, the table is already committed by the time the
      // ALTER runs — and the caller records no manifest entry until this
      // function returns, so a table left here is reachable by neither the
      // failed-run reclaim nor manifest-driven GC.
      const issued: string[] = [];
      const conn = {
         runSQL: async (q: string) => {
            issued.push(q);
            if (q.startsWith("ALTER TABLE")) throw new Error("no such column");
            return { rows: [], totalRows: 0 };
         },
      } as unknown as DuckDBConnection;

      await expect(
         createTableAndDescribe(conn, '"lake"."t"', ROWS, ["nope"]),
      ).rejects.toThrow("no such column");
      expect(issued).toContain('DROP TABLE IF EXISTS "lake"."t"');
      // The failure the caller sees is the layout error, not a drop error.
      expect(issued.filter((q) => q.startsWith("INSERT"))).toEqual([]);
   });
});

describe("createTableAndDescribe: against a real DuckLake", () => {
   it("writes one directory per partition value", async () => {
      const dir = mkdtempSync(join(tmpdir(), "ducklake-partition-"));
      const conn = new DuckDBConnection("duckdb");
      await conn.runSQL("INSTALL ducklake");
      await conn.runSQL("LOAD ducklake");
      await conn.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}/')`,
      );
      // Without this a table this small lands in the catalog's own rows rather
      // than in files, and there would be nothing to lay out — the production
      // build session sets it on attach for the same reason.
      await conn.runSQL("SET ducklake_default_data_inlining_row_limit=0");

      const schema = await createTableAndDescribe(conn, "lake.t", ROWS, [
         "org_id",
      ]);
      // The read-back still answers, so the manifest entry a partitioned build
      // records declares the same authoritative schema an unpartitioned one does.
      expect(schema.map((c) => c.name).sort()).toEqual([
         "org_id",
         "s",
         "user_id",
      ]);

      const files = await conn.runSQL(
         `SELECT data_file FROM ducklake_list_files('lake', 't') ORDER BY data_file`,
      );
      const paths = (files.rows as { data_file: string }[]).map(
         (r) => r.data_file,
      );
      expect(paths).toHaveLength(2);
      expect(paths[0]).toContain("/t/org_id=1/");
      expect(paths[1]).toContain("/t/org_id=2/");

      // Every row is present: the layout separates the files, it does not filter.
      const count = await conn.runSQL(`SELECT count(*) AS n FROM lake.t`);
      expect(Number((count.rows as { n: unknown }[])[0].n)).toBe(3);
   }, 120000);
});
