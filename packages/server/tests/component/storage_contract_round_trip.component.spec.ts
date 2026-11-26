// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` table straight out of its build matches the column contract
 * the build recorded, at the next bind, for every column type a build
 * writes. The build records its contract from a `DESCRIBE` inside its CTAS
 * transaction (`createTableAndDescribe`); the bind reads the table on a fresh
 * read-only session after the commit. In a DuckLake destination a GEOMETRY
 * with a CRS reads `GEOMETRY('OGC:CRS84')` inside the transaction and
 * `GEOMETRY` after it, since DuckLake's catalog keeps no CRS: before 0.18.3
 * such a table failed its own contract (`column_retyped`) at every bind.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   compatible,
   parseContract,
} from "../../src/service/materialization/domain/column_contract";
import type { ObservedColumns } from "../../src/service/materialization/domain/types";
import { storageContract } from "../../src/service/materialization/run/column_contract_capture";
import {
   readTableCollations,
   readTableColumns,
   withCollations,
} from "../../src/service/materialization/run/database_column_read";
import { createTableAndDescribe } from "../../src/service/materialization/run/materialization_build_session";

/** Each column a build may write, by the expression that writes it. */
const COLUMNS: Record<string, string> = {
   b: "true",
   i8: "1::TINYINT",
   i16: "1::SMALLINT",
   i32: "1::INTEGER",
   i64: "1::BIGINT",
   u64: "1::UBIGINT",
   f32: "1.5::FLOAT",
   f64: "1.5::DOUBLE",
   dec: "1.5::DECIMAL(18,3)",
   wide_dec: "1.5::DECIMAL(38,10)",
   s: "'a'",
   d: "DATE '2026-01-01'",
   tod: "TIME '12:00:00'",
   ts: "TIMESTAMP '2026-01-01 12:00:00'",
   tstz: "TIMESTAMPTZ '2026-01-01 12:00:00+00'",
   iv: "INTERVAL 1 DAY",
   bl: "'\\x01'::BLOB",
   id: "'00000000-0000-0000-0000-000000000001'::UUID",
   list: "['a', 'b']",
   rec: "{'a': 1, 'b': 'x'}",
   m: "MAP {'k': 1}",
   geo: "'POINT(1 2)'::GEOMETRY",
   geo_crs84: "'POINT(1 2)'::GEOMETRY('OGC:CRS84')",
   geo_crs83: "'POINT(1 2)'::GEOMETRY('OGC:CRS83')",
   rec_geo: "{'g': 'POINT(1 2)'::GEOMETRY('OGC:CRS84')}",
};

const TABLE = '"lake"."t"';

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by two in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "a storage= table's contract, at the bind after its build",
   () => {
      let dir: string;
      let build: DuckDBConnection;
      let described: { name?: string; type?: string }[];
      let contract: Awaited<ReturnType<typeof storageContract>>;
      let observed: ObservedColumns;

      const attach = async (c: DuckDBConnection, readOnly: boolean) => {
         await c.runSQL("INSTALL ducklake");
         await c.runSQL("LOAD ducklake");
         await c.runSQL(
            `ATTACH 'ducklake:${path.join(dir, "catalog.ducklake")}' AS lake ` +
               `(DATA_PATH '${path.join(dir, "data")}/'${readOnly ? ", READ_ONLY" : ""})`,
         );
      };

      /** A session as a build's or a bind's: db-duckdb's row limit of 10 lifted. */
      const session = (name: string) =>
         new DuckDBConnection(name, path.join(dir, `${name}.duckdb`), dir, {
            rowLimit: Number.MAX_SAFE_INTEGER,
         });

      beforeAll(async () => {
         dir = fs.mkdtempSync(path.join(os.tmpdir(), "contract-round-trip-"));
         build = session("build");
         await attach(build, false);
         // The build: CTAS, then its DESCRIBE and collations, in one transaction.
         const captured: { collations?: never } = {};
         described = await createTableAndDescribe(
            build,
            TABLE,
            `SELECT ${Object.entries(COLUMNS)
               .map(([name, expr]) => `${expr} AS "${name}"`)
               .join(", ")}`,
            { captured },
         );
         contract = await storageContract({
            table: TABLE,
            schema: described,
            collations: captured.collations,
         });
         // The bind: a fresh read-only session, the catalog as committed.
         const bind = session("bind");
         try {
            await attach(bind, true);
            const tables = [
               { key: "t", connectionName: "d", tableName: TABLE },
            ];
            const open = async () => bind;
            const read = withCollations(
               tables,
               await readTableColumns({
                  tables,
                  connection: open,
                  asDescribed: true,
               }),
               await readTableCollations({ tables, connection: open }),
            );
            observed = read.get("t") ?? { unread: "not_read" };
         } finally {
            await bind.close();
         }
      });

      afterAll(async () => {
         await build?.close();
         fs.rmSync(dir, { recursive: true, force: true });
      });

      it("reads a GEOMETRY's CRS inside the build and not after it", () => {
         // The premise: what the build read is not what the bind reads.
         const inBuild = described.find((c) => c.name === "geo_crs84")?.type;
         expect(inBuild).toBe("GEOMETRY('OGC:CRS84')");
         expect("columns" in observed).toBe(true);
         if (!("columns" in observed)) return;
         const atBind = observed.columns.find((c) => c.name === "geo_crs84");
         expect(atBind?.nativeType).toBe("GEOMETRY");
      });

      it("records every column, and the table passes its own contract at the next bind", () => {
         expect(contract.columns.map((c) => c.name)).toEqual(
            Object.keys(COLUMNS),
         );
         expect(
            contract.columns.find((c) => c.name === "geo_crs84")?.nativeType,
         ).toBe("GEOMETRY");
         expect("columns" in observed).toBe(true);
         if (!("columns" in observed)) return;
         expect(compatible(contract, observed)).toEqual({ ok: true });
      });

      it("holds a contract an earlier release recorded with the CRS compatible too", () => {
         if (!("columns" in observed)) throw new Error("unread");
         const legacy = parseContract({
            ...contract,
            columns: contract.columns.map((c) => ({
               ...c,
               nativeType:
                  described.find((d) => d.name === c.name)?.type ??
                  c.nativeType,
            })),
         })!;
         expect(
            legacy.columns.find((c) => c.name === "geo_crs83")?.nativeType,
         ).toBe("GEOMETRY('OGC:CRS83')");
         expect(compatible(legacy, observed)).toEqual({ ok: true });
      });
   },
);
