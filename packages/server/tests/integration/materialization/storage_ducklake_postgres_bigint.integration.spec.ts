// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P4-01 on a DuckLake whose catalog is Postgres, the production shape: a
 * 128-bit integer written to Parquet is stored as a DOUBLE while the catalog
 * still says HUGEINT, and a same-catalog build stores a DuckDB-computed sum
 * as DECIMAL(38,0), every digit kept. Run with `PUBLISHER_TEST_POSTGRES_URL`.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { createTableWithDeclaredTypes } from "../../../src/service/materialization/run/materialization_build_session";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = "p4_01_lake";

/** libpq key=value form of a postgres:// URL, as DuckLake's ATTACH takes it. */
function libpq(url: string, schema: string): string {
   const u = new URL(url);
   return [
      `host=${u.hostname}`,
      `port=${u.port || "5432"}`,
      `user=${decodeURIComponent(u.username)}`,
      `password=${decodeURIComponent(u.password)}`,
      `dbname=${u.pathname.replace(/^\//, "")}`,
      `options=-csearch_path=${schema}`,
   ].join(" ");
}

describe.skipIf(!POSTGRES_URL)(
   "P4-01: a 128-bit integer in a Postgres-catalog DuckLake",
   () => {
      let dir: string;
      let pg: PostgresConnection;
      let conn: DuckDBConnection;
      beforeAll(async () => {
         pg = new PostgresConnection({
            name: "pg",
            connectionString: POSTGRES_URL,
         });
         await pg.runSQL(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
         await pg.runSQL(`CREATE SCHEMA ${SCHEMA}`);
         dir = mkdtempSync(join(tmpdir(), "p4-01-pg-lake-"));
         conn = new DuckDBConnection("p4_01_pg_lake", join(dir, "h.duckdb"));
         for (const sql of [
            "INSTALL ducklake",
            "LOAD ducklake",
            "INSTALL postgres",
            "LOAD postgres",
            `ATTACH 'ducklake:postgres:${libpq(POSTGRES_URL!, SCHEMA)}' AS lake (DATA_PATH '${join(dir, "data")}/', METADATA_SCHEMA '${SCHEMA}')`,
            "SET ducklake_default_data_inlining_row_limit=0",
         ]) {
            await conn.runSQL(sql);
         }
      }, 120000);
      afterAll(async () => {
         await conn?.close();
         await pg?.runSQL(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
         await pg?.close();
         rmSync(dir, { recursive: true, force: true });
      });

      const SUM = `SELECT SUM(v) AS total FROM (VALUES
         (4530000000000225186::BIGINT), (1::BIGINT)) AS w(v)`;

      it("stores a HUGEINT rounded while the catalog says HUGEINT (the defect)", async () => {
         await conn.runSQL(`CREATE TABLE lake.raw_t AS ${SUM}`);
         const read = await conn.runSQL(
            "SELECT total::VARCHAR AS v, typeof(total) AS t FROM lake.raw_t",
         );
         expect(read.rows).toEqual([
            { v: "4530000000000225280", t: "HUGEINT" },
         ]);
      }, 120000);

      it("a same-catalog build stores the sum as DECIMAL(38,0), every digit kept", async () => {
         const schema = await createTableWithDeclaredTypes(
            conn,
            "lake.built_t",
            SUM,
            { columns: [{ name: "total", type: "number" }] },
         );
         expect(schema).toEqual([{ name: "total", type: "DECIMAL(38,0)" }]);
         const read = await conn.runSQL(
            "SELECT total::VARCHAR AS v FROM lake.built_t",
         );
         expect(read.rows).toEqual([{ v: "4530000000000225187" }]);
      }, 120000);
   },
);
