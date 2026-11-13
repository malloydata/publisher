// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A pair table's delta on a real DuckLake, the production destination: the
// staged delta replaces whole parents by key in one transaction, and the
// parent-row check rolls the whole delta back, leaving the table and the
// session as they were. The R4 spec (served_copy_pair_incremental) proves the
// answers on plain DuckDB; this pins the engine behaviour that spec cannot:
// a session temporary beside a DuckLake catalog, written in one transaction
// that writes only the lake, and `error()` aborting it.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import {
   applyDeltaScript,
   deltaStatements,
   isPairGuardFailure,
   pairStagingTable,
} from "./incremental_apply";

let open: { conn: DuckDBConnection; dir: string } | undefined;

afterEach(async () => {
   if (!open) return;
   await open.conn.runSQL("DETACH lake");
   await open.conn.close();
   rmSync(open.dir, { recursive: true, force: true });
   open = undefined;
});

async function lake(): Promise<DuckDBConnection> {
   const dir = mkdtempSync(join(tmpdir(), "ducklake-pair-"));
   const conn = new DuckDBConnection("ducklake_pair", join(dir, "host.duckdb"));
   open = { conn, dir };
   await conn.runSQL("INSTALL ducklake");
   await conn.runSQL("LOAD ducklake");
   await conn.runSQL(
      `ATTACH 'ducklake:${join(dir, "catalog.ducklake")}' AS lake ` +
         `(DATA_PATH '${join(dir, "data")}/')`,
   );
   await conn.runSQL("SET ducklake_default_data_inlining_row_limit=0");
   // Parent 1 with two children, childless parent 2, parent 3 with one.
   await conn.runSQL(`CREATE TABLE lake.pairs AS SELECT * FROM (VALUES
      (1, 10, true, 5), (1, 11, false, 5), (2, NULL, true, 6), (3, 30, true, 7)
   ) AS t(id, child, __parent_row, __pair_watermark)`);
   return conn;
}

const statements = (deltaSQL: string) =>
   deltaStatements({
      dialect: "duckdb",
      quotedTablePath: "lake.pairs",
      deltaSQL,
      columns: ["id", "child", "__parent_row", "__pair_watermark"],
      mergeKeys: ["id"],
      watermarkName: "__pair_watermark",
      start: { malloyType: "number", value: "8" },
      end: { malloyType: "number", value: "20" },
      pair: {
         parentKey: "id",
         parentRow: "__parent_row",
         stagingTable: pairStagingTable("lake.pairs"),
      },
   });

async function rows(conn: DuckDBConnection): Promise<unknown[][]> {
   const result = await conn.runSQL(
      "SELECT id, child, __parent_row FROM lake.pairs ORDER BY id, child NULLS FIRST",
      { rowLimit: 1000 },
   );
   return (result.rows as Record<string, unknown>[]).map((r) => [
      r.id === null ? null : Number(r.id),
      r.child === null ? null : Number(r.child),
      r.__parent_row,
   ]);
}

describe("a pair table's delta on DuckLake", () => {
   it("replaces each staged parent whole: one losing a child, a childless one gaining two, a new one", async () => {
      const conn = await lake();
      const run = (sql: string) => conn.runSQL(sql);
      await applyDeltaScript(
         run,
         "duckdb",
         statements(`SELECT * FROM (VALUES
            (1, 10, true, 9),
            (2, 20, true, 9), (2, 21, false, 9),
            (4, NULL, true, 9)
         ) AS t(id, child, __parent_row, __pair_watermark)`),
      );
      expect(await rows(conn)).toEqual([
         [1, 10, true],
         [2, 20, true],
         [2, 21, false],
         [3, 30, true],
         [4, null, true],
      ]);
      // The staging table is gone.
      const left = await conn.runSQL(
         "SELECT count(*) AS n FROM duckdb_tables() WHERE table_name LIKE '__pair_delta%'",
      );
      expect(Number((left.rows as { n: unknown }[])[0].n)).toBe(0);
   });

   it("rolls the whole delta back when a parent would keep two flags, and stays usable", async () => {
      const conn = await lake();
      const run = (sql: string) => conn.runSQL(sql);
      const before = await rows(conn);
      // A delta that flags parent 3 twice: the check refuses it.
      const err = await applyDeltaScript(
         run,
         "duckdb",
         statements(`SELECT * FROM (VALUES
            (1, 12, true, 9), (3, 31, true, 9), (3, 32, true, 9)
         ) AS t(id, child, __parent_row, __pair_watermark)`),
      ).catch((e: unknown) => e);
      expect(isPairGuardFailure(err)).toBe(true);
      expect(await rows(conn)).toEqual(before);
   });
});
