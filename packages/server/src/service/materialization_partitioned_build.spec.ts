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
         "BEGIN TRANSACTION",
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS}) WITH NO DATA`,
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id")',
         `INSERT INTO "lake"."t" (${ROWS})`,
         // The read-back is INSIDE, before COMMIT: a failed DESCRIBE drops the
         // table, and after a commit that would delete the generation the
         // previous manifest still names.
         'DESCRIBE "lake"."t"',
         "COMMIT",
      ]);
   });

   it("keeps the author's column order, which is the directory nesting", async () => {
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id", "s"]);
      expect(sql[2]).toBe(
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id", "s")',
      );
   });

   it("rolls back rather than dropping when the layout fails", async () => {
      // The physical name is self-assigned and STABLE across generations, so a
      // rebuild targets the table currently being served. Dropping it on failure
      // would delete the generation the manifest still names; rolling back
      // restores it. This is why the three statements are one transaction —
      // see the real-DuckLake case below, which proves the rows survive.
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
      expect(issued).toContain("ROLLBACK");
      // Never a drop: that is what destroyed the served generation.
      expect(issued.filter((q) => q.startsWith("DROP"))).toEqual([]);
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
      // Separators normalized because DuckLake reports the platform's own —
      // `…\t\org_id=1\…` on Windows. The assertions below still read as paths
      // rather than as bare substrings, which is the point: `org_id=1` has to be
      // a DIRECTORY under the table, not merely a run of characters somewhere in
      // the name.
      const paths = (files.rows as { data_file: string }[]).map((r) =>
         r.data_file.replaceAll("\\", "/"),
      );
      expect(paths).toHaveLength(2);
      expect(paths[0]).toContain("/t/org_id=1/");
      expect(paths[1]).toContain("/t/org_id=2/");

      // Every row is present: the layout separates the files, it does not filter.
      const count = await conn.runSQL(`SELECT count(*) AS n FROM lake.t`);
      expect(Number((count.rows as { n: unknown }[])[0].n)).toBe(3);
   }, 120000);

   it("leaves the previous generation serving when the build fails", async () => {
      // The property the transaction exists for, proved on a real catalog rather
      // than on issued SQL.
      //
      // The physical name is self-assigned and stable across generations, so a
      // rebuild REPLACES the table being served. Unwrapped, `WITH NO DATA` empties
      // it — routed queries then answer zero rows reporting `servedFrom: storage`,
      // which is not even a visible fallback — and a failed INSERT leaves the drop
      // to delete a table the manifest still names. A source-warehouse timeout mid
      // refresh is routine, so this is a reachable path, not a hypothetical.
      const dir = mkdtempSync(join(tmpdir(), "ducklake-partition-fail-"));
      const conn = new DuckDBConnection("duckdb");
      await conn.runSQL("INSTALL ducklake");
      await conn.runSQL("LOAD ducklake");
      await conn.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake2 ` +
            `(DATA_PATH '${join(dir, "data")}/')`,
      );
      await conn.runSQL("SET ducklake_default_data_inlining_row_limit=0");

      // The generation currently being served.
      await conn.runSQL(`CREATE OR REPLACE TABLE lake2.t AS (${ROWS})`);
      const rows = async () => {
         const r = await conn.runSQL("SELECT count(*) AS n FROM lake2.t");
         return Number((r.rows as { n: unknown }[])[0].n);
      };
      expect(await rows()).toBe(3);

      // The select must BIND and then fail while running, which is the whole
      // point: a select that fails to bind takes the first statement with it,
      // and a failed `CREATE OR REPLACE` leaves the old table alone whether or
      // not there is a transaction — so it would assert nothing. This one types
      // cleanly and raises a conversion error partway through, once the INSERT
      // has already written files.
      const FAILS_MIDWAY =
         "SELECT i AS org_id, " +
         "CASE WHEN i < 900000 THEN 'a' ELSE CAST(CAST('zz' AS INT) AS VARCHAR) END AS s " +
         "FROM range(1000000) t(i)";
      await expect(
         createTableAndDescribe(conn, "lake2.t", FAILS_MIDWAY, ["org_id"]),
      ).rejects.toThrow();

      // Still serving the previous generation's rows, not zero and not absent.
      expect(await rows()).toBe(3);

      // And the LAYOUT reverted with them: a later write goes back to flat
      // files, so the rollback did not leave the table partitioned by a column
      // the surviving generation was never written under.
      await conn.runSQL(`INSERT INTO lake2.t (${ROWS})`);
      const after = await conn.runSQL(
         `SELECT data_file FROM ducklake_list_files('lake2', 't')`,
      );
      expect(
         (after.rows as { data_file: string }[]).every(
            (r) => !r.data_file.replaceAll("\\", "/").includes("/org_id="),
         ),
      ).toBe(true);
   }, 120000);
});
