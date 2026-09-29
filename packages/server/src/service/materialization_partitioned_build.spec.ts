// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A partitioned storage build is three statements rather than a CTAS, and the
// ORDER is the whole point: DuckLake applies a partition layout only to files
// written after it is set, so a layout declared after the rows have landed lays
// out nothing. Asserted twice — on the SQL issued, which is where the order
// lives, and against a real DuckLake, which is the only thing that can show the
// files actually separated.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
   createIsolatedBuildSession,
   createTableAndDescribe,
   orderByPartitionColumns,
} from "./materialization_build_session";

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
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"], {
         sourceType: "postgres",
      });
      expect(sql).toEqual([
         // Before the transaction, on this path only: the rows each appender
         // thread holds before flushing to the partition files. DuckDB's own
         // 524,288 is what a wide, many-partition insert dies on.
         "SET partitioned_write_flush_threshold = 8192",
         // One appender: a DuckDB-side sort is read in parallel, and several
         // appenders each meet every partition again.
         "SET threads = 1",
         "BEGIN TRANSACTION",
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS}) WITH NO DATA`,
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id")',
         // Ordered by the partition columns at the top of the INSERT and nowhere
         // else: the SELECT handed in is what the CTAS reads, unchanged.
         `INSERT INTO "lake"."t" (SELECT * FROM (${ROWS}) AS partitioned_build ORDER BY "org_id")`,
         // The read-back is INSIDE, before COMMIT: a failed DESCRIBE drops the
         // table, and after a commit that would delete the generation the
         // previous manifest still names.
         'DESCRIBE "lake"."t"',
         "COMMIT",
         "RESET threads",
      ]);
   });

   it("keeps the author's column order, which is the directory nesting", async () => {
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id", "s"], {
         sourceType: "postgres",
      });
      expect(sql[4]).toBe(
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id", "s")',
      );
      expect(sql[5]).toBe(
         `INSERT INTO "lake"."t" (SELECT * FROM (${ROWS}) AS partitioned_build ORDER BY "org_id", "s")`,
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
         createTableAndDescribe(conn, '"lake"."t"', ROWS, ["nope"], {
            sourceType: "postgres",
         }),
      ).rejects.toThrow("no such column");
      expect(issued).toContain("ROLLBACK");
      // And the thread count is given back on this path too.
      expect(issued.at(-1)).toBe("RESET threads");
      // Never a drop: that is what destroyed the served generation.
      expect(issued.filter((q) => q.startsWith("DROP"))).toEqual([]);
      expect(issued.filter((q) => q.startsWith("INSERT"))).toEqual([]);
   });
});

describe("createTableAndDescribe: the flush threshold", () => {
   afterEach(() => {
      delete process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD;
   });

   it("issues the configured value", async () => {
      process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD = "2048";
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"], {
         sourceType: "postgres",
      });
      expect(sql[0]).toBe("SET partitioned_write_flush_threshold = 2048");
   });

   it("`off` turns the whole treatment off: no threshold, no ordering, no single thread", async () => {
      // One switch, because the three only work together: a source that built
      // fine before is better served by none of them than by the sort alone.
      process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD = "off";
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"], {
         sourceType: "postgres",
      });
      expect(sql).toEqual([
         "BEGIN TRANSACTION",
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS}) WITH NO DATA`,
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id")',
         `INSERT INTO "lake"."t" (${ROWS})`,
         'DESCRIBE "lake"."t"',
         "COMMIT",
      ]);
   });

   it("never reaches an unpartitioned build, nor does the ordering or the thread count", async () => {
      // The setting only governs a partitioned COPY, and the unpartitioned
      // CTAS is promised byte-identical to what it was.
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, [], {
         sourceType: "postgres",
      });
      expect(sql).toEqual([
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS})`,
         'DESCRIBE "lake"."t"',
      ]);
   });

   it("reaches every passthrough source, and a chained build keeps the insert it had", async () => {
      // The failure is the writer's, measured on Postgres and BigQuery alike,
      // so every passthrough source is bounded the same way; a chained build
      // issues exactly the sequence it did before any of this existed.
      for (const sourceType of ["bigquery", "snowflake"] as const) {
         const { conn, sql } = recorder();
         await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"], {
            sourceType,
         });
         expect(sql[0]).toBe("SET partitioned_write_flush_threshold = 8192");
         expect(sql[1]).toBe("SET threads = 1");
         expect(sql[5]).toBe(
            `INSERT INTO "lake"."t" (SELECT * FROM (${ROWS}) AS partitioned_build ORDER BY "org_id")`,
         );
         expect(sql.at(-1)).toBe("RESET threads");
      }
      const before = [
         "BEGIN TRANSACTION",
         `CREATE OR REPLACE TABLE "lake"."t" AS (${ROWS}) WITH NO DATA`,
         'ALTER TABLE "lake"."t" SET PARTITIONED BY ("org_id")',
         `INSERT INTO "lake"."t" (${ROWS})`,
         'DESCRIBE "lake"."t"',
         "COMMIT",
      ];
      const { conn, sql } = recorder();
      await createTableAndDescribe(conn, '"lake"."t"', ROWS, ["org_id"]);
      expect(sql).toEqual(before);
   });

   it("gives nothing back on the failure path of a build it never bounded", async () => {
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
      expect(issued.at(-1)).toBe("ROLLBACK");
   });
});

describe("orderByPartitionColumns: the insert reads its SELECT in partition order", () => {
   it("leaves an unpartitioned build's SQL untouched", () => {
      expect(orderByPartitionColumns("SELECT 1", [], "postgres")).toBe(
         "SELECT 1",
      );
   });

   it("wraps rather than parses, ordered by the columns in the author's order", () => {
      expect(
         orderByPartitionColumns(
            "SELECT a, b FROM t",
            ["org_id", "day"],
            "postgres",
         ),
      ).toBe(
         'SELECT * FROM (SELECT a, b FROM t) AS partitioned_build ORDER BY "org_id", "day"',
      );
   });

   it("quotes for the given dialect: backticks on BigQuery, double quotes elsewhere", () => {
      expect(
         orderByPartitionColumns("SELECT 1", ["org_id"], "standardsql"),
      ).toBe("SELECT * FROM (SELECT 1) AS partitioned_build ORDER BY `org_id`");
      expect(orderByPartitionColumns("SELECT 1", ["org_id"], "snowflake")).toBe(
         'SELECT * FROM (SELECT 1) AS partitioned_build ORDER BY "org_id"',
      );
   });

   it("drops a trailing terminator, which would end the subselect early", () => {
      expect(orderByPartitionColumns("SELECT 1;\n", ["org_id"], "duckdb")).toBe(
         'SELECT * FROM (SELECT 1) AS partitioned_build ORDER BY "org_id"',
      );
   });

   it("escapes a quote inside a column name rather than breaking the statement", () => {
      expect(orderByPartitionColumns("SELECT 1", ['a"b'], "postgres")).toBe(
         'SELECT * FROM (SELECT 1) AS partitioned_build ORDER BY "a""b"',
      );
   });
});

describe("a wide, many-partition insert at a low memory limit", () => {
   // The failure this change exists for, reproduced on a real DuckLake at a
   // memory limit small enough to fail in seconds: DuckDB's partitioned COPY
   // charges each appender thread one vector per column for every partition it
   // has met, at first sight, and flushes nothing until it has appended
   // partitioned_write_flush_threshold rows. The SELECT handed in interleaves
   // its partitions, as a warehouse result does. Each assertion below fails on
   // the behaviour before this change: without the ORDER BY the insert dies
   // before a row is flushed at ANY threshold, and with it, it dies at DuckDB's
   // default threshold.
   const PARTITIONS = 300;
   const ROWS_PER_PARTITION = 2000;
   const COLUMNS = 40;
   const columns = Array.from({ length: COLUMNS }, (_, i) =>
      i % 2 === 0
         ? `md5((r + ${i})::VARCHAR) AS c${i}`
         : `(r * ${i + 1})::BIGINT AS c${i}`,
   ).join(", ");
   const interleaved =
      `SELECT (r % ${PARTITIONS})::BIGINT AS org_id, ${columns} ` +
      `FROM range(${PARTITIONS * ROWS_PER_PARTITION}) t(r)`;

   // Each case on its OWN instance, as a production build is: the memory limit
   // and thread count below must not leak into the pooled in-memory instance the
   // other suites share, and the lake alias must not collide with theirs.
   async function lake(): Promise<{
      conn: DuckDBConnection;
      dispose: () => Promise<void>;
   }> {
      const dir = mkdtempSync(join(tmpdir(), "ducklake-partition-memory-"));
      const { session: conn, dispose } = createIsolatedBuildSession(
         "partition_memory_test",
      );
      await conn.runSQL("INSTALL ducklake");
      await conn.runSQL("LOAD ducklake");
      await conn.runSQL(
         `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
            `(DATA_PATH '${join(dir, "data")}/')`,
      );
      await conn.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      await conn.runSQL("SET preserve_insertion_order=false");
      await conn.runSQL("SET memory_limit='192MB'");
      return { conn, dispose };
   }

   afterEach(() => {
      delete process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD;
   });

   it("without the ORDER BY, the insert fails before a row is flushed, whatever the threshold", async () => {
      // The statements createTableAndDescribe issues, minus the ordering: the
      // control that shows the ORDER BY is load-bearing, not the threshold alone.
      const { conn, dispose } = await lake();
      try {
         await conn.runSQL("SET partitioned_write_flush_threshold = 2048");
         await conn.runSQL("SET threads = 1");
         await conn.runSQL(
            `CREATE OR REPLACE TABLE lake.t AS (${interleaved}) WITH NO DATA`,
         );
         await conn.runSQL("ALTER TABLE lake.t SET PARTITIONED BY (org_id)");
         await expect(
            conn.runSQL(`INSERT INTO lake.t (${interleaved})`),
         ).rejects.toThrow(/Out of Memory/);
      } finally {
         await dispose();
      }
   }, 120000);

   it("with the ORDER BY but DuckDB's own threshold, the insert still fails", async () => {
      process.env.PUBLISHER_PARTITIONED_WRITE_FLUSH_THRESHOLD = "off";
      const { conn, dispose } = await lake();
      try {
         await expect(
            createTableAndDescribe(conn, "lake.t", interleaved, ["org_id"], {
               sourceType: "postgres",
            }),
         ).rejects.toThrow(/Out of Memory/);
      } finally {
         await dispose();
      }
   }, 120000);

   it("as issued, the insert completes with one directory per partition", async () => {
      const { conn, dispose } = await lake();
      try {
         const schema = await createTableAndDescribe(
            conn,
            "lake.t",
            interleaved,
            ["org_id"],
            { sourceType: "postgres" },
         );
         expect(schema).toHaveLength(COLUMNS + 1);
         const count = await conn.runSQL(`SELECT count(*) AS n FROM lake.t`);
         expect(Number((count.rows as { n: unknown }[])[0].n)).toBe(
            PARTITIONS * ROWS_PER_PARTITION,
         );
         const files = await conn.runSQL(
            `SELECT count(DISTINCT data_file) AS n FROM ducklake_list_files('lake', 't')`,
         );
         expect(Number((files.rows as { n: unknown }[])[0].n)).toBe(PARTITIONS);
      } finally {
         await dispose();
      }
   }, 120000);
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
      // has already written files. A few partition values rather than one per
      // row: the build flushes every partitioned_write_flush_threshold rows, and
      // a partition per row turns each flush into thousands of file opens, which
      // is the many-small-files layout the docs warn off, not this test's point.
      const FAILS_MIDWAY =
         "SELECT i % 3 AS org_id, " +
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
