// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// What the generated delta DML actually DOES, proven by executing it.
//
// The statement TEXT is pinned in incremental_apply.spec.ts; this file runs it.
// Every claim the design rests on — that a repeated range converges, that
// merge_key= replaces a restated row while a range replace duplicates it, that
// the two-statement form is atomic — is a claim about a database's behavior, and
// the only way to hold it is to execute against one.
//
// Executed on DuckDB, which is NOT on the dialect allowlist: it is a stand-in
// here for the transactional DML the allowlisted dialects provide, and it
// accepts the Postgres spelling verbatim (double-quoted identifiers,
// `BEGIN`/`COMMIT`, `MERGE INTO … WHEN MATCHED`). That makes these semantics
// checkable in a unit test; it does NOT make DuckDB supported, and it is not a
// substitute for running the real dialects (see the Postgres integration path
// in the docs). Snowflake's range replace — a Snowflake Scripting block — has
// no local stand-in at all: its text is pinned in incremental_apply.spec.ts and
// its semantics are provable only against a real Snowflake account.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { beforeAll, describe, expect, it } from "bun:test";
import { applyDeltaScript, deltaStatements } from "./incremental_apply";

let duckdb: DuckDBConnection;
// One `:memory:` database is shared by every connection in a process (see
// duckdb_instance_isolation.spec.ts), so each test takes a fresh table name
// rather than a fresh database.
let table = 0;

beforeAll(() => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
});

/** Everything goes through the same runner the build path uses. */
const runner = (sql: string) => duckdb.runSQL(sql);

const COLUMNS = ["order_id", "order_date", "revenue"];

/** `(id, 'YYYY-MM-DD', revenue)` rows as a VALUES-backed SELECT. */
function rowsSQL(rows: [number, string, number][]): string {
   return rows
      .map(
         ([id, date, revenue]) =>
            `SELECT ${id} AS "order_id", DATE '${date}' AS "order_date", ${revenue} AS "revenue"`,
      )
      .join(" UNION ALL ");
}

async function seed(rows: [number, string, number][]): Promise<string> {
   const name = `daily_${table++}`;
   await duckdb.runSQL(`CREATE TABLE "${name}" AS ${rowsSQL(rows)}`);
   return name;
}

async function contents(name: string): Promise<string[]> {
   const result = await duckdb.runSQL(
      `SELECT "order_id", strftime("order_date", '%Y-%m-%d') AS d, "revenue"
       FROM "${name}" ORDER BY "order_id", d, "revenue"`,
   );
   return result.rows.map((r) => {
      const row = r as Record<string, unknown>;
      return `${row.order_id}|${row.d}|${row.revenue}`;
   });
}

/** Apply a delta of `rows` over the range [start, end). */
async function applyDelta(params: {
   table: string;
   rows: [number, string, number][];
   mergeKeys: string[];
   start: string;
   end: string;
}): Promise<void> {
   const statements = deltaStatements({
      dialect: "postgres",
      quotedTablePath: `"${params.table}"`,
      deltaSQL: rowsSQL(params.rows),
      columns: COLUMNS,
      mergeKeys: params.mergeKeys,
      watermarkName: "order_date",
      start: { malloyType: "date", value: params.start },
      end: { malloyType: "date", value: params.end },
   });
   await applyDeltaScript(runner, "postgres", statements);
}

describe("range replace (no merge_key=)", () => {
   it("replaces the range and leaves everything below it alone", async () => {
      const name = await seed([
         [1, "2024-05-01", 10],
         [2, "2024-06-01", 20],
      ]);
      await applyDelta({
         table: name,
         rows: [
            [2, "2024-06-01", 25],
            [3, "2024-06-15", 30],
         ],
         mergeKeys: [],
         start: "2024-06-01",
         end: "2024-07-01",
      });
      expect(await contents(name)).toEqual([
         "1|2024-05-01|10", // below the range: untouched
         "2|2024-06-01|25", // inside: recomputed, not duplicated
         "3|2024-06-15|30", // new
      ]);
   });

   it("converges when the SAME range is applied twice", async () => {
      // The retry-safety property: a crash between the DML and the ledger
      // advance costs a repeat, not a corruption.
      const name = await seed([[1, "2024-06-01", 10]]);
      const delta = {
         table: name,
         rows: [[1, "2024-06-01", 99]] as [number, string, number][],
         mergeKeys: [],
         start: "2024-06-01",
         end: "2024-07-01",
      };
      await applyDelta(delta);
      const once = await contents(name);
      await applyDelta(delta);
      expect(await contents(name)).toEqual(once);
      expect(once).toEqual(["1|2024-06-01|99"]);
   });

   it("DUPLICATES a row restated with an advanced watermark", async () => {
      // The documented weakness of range replace, and the reason the load-time
      // advisory tells an author to declare merge_key= when rows are restated:
      // the older copy sits BELOW the new range, so nothing deletes it.
      const name = await seed([[1, "2024-06-01", 10]]);
      await applyDelta({
         table: name,
         rows: [[1, "2024-07-15", 10]],
         mergeKeys: [],
         start: "2024-07-01",
         end: "2024-08-01",
      });
      expect(await contents(name)).toEqual([
         "1|2024-06-01|10",
         "1|2024-07-15|10",
      ]);
   });

   it("is atomic: a failed insert leaves the deleted range in place", async () => {
      // The claim the dialect allowlist exists to guarantee. Without the
      // transaction the DELETE would autocommit on its own and the range would
      // be empty in a table that is already serving.
      const name = await seed([[1, "2024-06-01", 10]]);
      const statements = deltaStatements({
         dialect: "postgres",
         quotedTablePath: `"${name}"`,
         deltaSQL: `SELECT 1 AS "order_id", DATE '2024-06-02' AS "order_date", 'not a number' AS "revenue"`,
         columns: COLUMNS,
         mergeKeys: [],
         watermarkName: "order_date",
         start: { malloyType: "date", value: "2024-06-01" },
         end: { malloyType: "date", value: "2024-07-01" },
      });
      await expect(
         applyDeltaScript(runner, "postgres", statements),
      ).rejects.toThrow();
      expect(await contents(name)).toEqual(["1|2024-06-01|10"]);
   });
});

describe("merge (merge_key= declared)", () => {
   it("replaces a row restated with an advanced watermark, in place", async () => {
      // Exactly the case range replace duplicates above: identity is
      // independent of the watermark, so the older copy is MATCHED and updated.
      const name = await seed([[1, "2024-06-01", 10]]);
      await applyDelta({
         table: name,
         rows: [[1, "2024-07-15", 10]],
         mergeKeys: ["order_id"],
         start: "2024-07-01",
         end: "2024-08-01",
      });
      expect(await contents(name)).toEqual(["1|2024-07-15|10"]);
   });

   it("inserts what it does not match, and converges on a repeat", async () => {
      const name = await seed([[1, "2024-06-01", 10]]);
      const delta = {
         table: name,
         rows: [
            [1, "2024-06-01", 11],
            [2, "2024-06-02", 22],
         ] as [number, string, number][],
         mergeKeys: ["order_id"],
         start: "2024-06-01",
         end: "2024-07-01",
      };
      await applyDelta(delta);
      const once = await contents(name);
      await applyDelta(delta);
      expect(await contents(name)).toEqual(once);
      expect(once).toEqual(["1|2024-06-01|11", "2|2024-06-02|22"]);
   });

   it("matches a NULL identity value instead of duplicating the row", async () => {
      // SQL `=` is not NULL-safe, so the plain form would insert this row again
      // on every single run. The generated ON clause is NULL-safe for that
      // reason.
      const name = `nulls_${table++}`;
      await duckdb.runSQL(
         `CREATE TABLE "${name}" AS SELECT CAST(NULL AS INTEGER) AS "order_id",
             DATE '2024-06-01' AS "order_date", 10 AS "revenue"`,
      );
      const statements = deltaStatements({
         dialect: "postgres",
         quotedTablePath: `"${name}"`,
         deltaSQL: `SELECT CAST(NULL AS INTEGER) AS "order_id", DATE '2024-06-02' AS "order_date", 20 AS "revenue"`,
         columns: COLUMNS,
         mergeKeys: ["order_id"],
         watermarkName: "order_date",
         start: { malloyType: "date", value: "2024-06-01" },
         end: { malloyType: "date", value: "2024-07-01" },
      });
      await applyDeltaScript(runner, "postgres", statements);
      expect(await contents(name)).toEqual(["null|2024-06-02|20"]);
   });

   it("leaves a row outside the delta untouched, whatever its watermark", async () => {
      // A merge is bounded by what the delta CONTAINS, not by a range predicate:
      // nothing deletes, so a row the delta does not mention cannot be lost.
      const name = await seed([
         [1, "2020-01-01", 1],
         [2, "2024-06-01", 2],
      ]);
      await applyDelta({
         table: name,
         rows: [[2, "2024-06-01", 22]],
         mergeKeys: ["order_id"],
         start: "2024-06-01",
         end: "2024-07-01",
      });
      expect(await contents(name)).toEqual([
         "1|2020-01-01|1",
         "2|2024-06-01|22",
      ]);
   });
});
