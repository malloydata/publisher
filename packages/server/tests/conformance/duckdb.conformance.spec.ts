// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The DuckDB row of the capability table (`domain/dialect.ts`), observed on
 * the engine Publisher embeds: each test runs what the capability promises,
 * through the statements Publisher sends where they exist, and asserts the
 * observation matches `CAPABILITIES.duckdb`. A test name here is a probe
 * reference in `domain/dialect_evidence.ts`; renaming one fails
 * `dialect_evidence.spec.ts`. In-process, so it runs everywhere.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import {
   CAPABILITIES,
   isBinary64Type,
} from "../../src/service/materialization/domain/dialect";
import {
   applyDeltaScript,
   deltaStatements,
   pairDeltaStatements,
   probeTargetColumns,
} from "../../src/service/materialization/run/incremental_apply";
import { DUCKDB_COLLATION } from "../../src/service/materialization/serve/collation/duckdb";
import { probeIO } from "../../src/service/materialization/serve/gate_collation";
import {
   ownedTableProbeSql,
   relationProbeSql,
   tableCommentSql,
   transactionalSwapSql,
} from "../../src/service/materialization/run/warehouse_sql";

const CAN = CAPABILITIES.duckdb;

describe("DuckDB conformance", () => {
   // Files of its own, not `:memory:`: in-memory instances can be shared
   // within a process, and a setting made here must not reach another spec.
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "duckdb-conformance-"));
   let duck: DuckDBConnection;
   const runner = (sql: string) => duck.runSQL(sql);
   async function rows(sql: string): Promise<Record<string, unknown>[]> {
      return (await duck.runSQL(sql)).rows as Record<string, unknown>[];
   }
   async function attempt(sql: string): Promise<string | undefined> {
      try {
         await duck.runSQL(sql);
         return undefined;
      } catch (err) {
         return err instanceof Error ? err.message : String(err);
      }
   }
   async function exists(name: string): Promise<boolean> {
      const [row] = await rows(
         `SELECT count(*)::INT AS n FROM duckdb_tables() WHERE table_name = '${name}'`,
      );
      return row?.["n"] === 1;
   }

   beforeAll(() => {
      duck = new DuckDBConnection(
         "conformance",
         path.join(dir, "conformance.duckdb"),
      );
   });
   afterAll(async () => {
      await duck?.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("transactionalSwap: the swap's drop and rename roll back together", async () => {
      await duck.runSQL(`CREATE TABLE phys AS SELECT 1 AS v`);
      await duck.runSQL(`CREATE TABLE stage AS SELECT 2 AS v`);
      // Publisher's swap text, failing at its rename after the drop ran.
      const failing = transactionalSwapSql("duckdb", {
         before: [],
         quotedPhysical: `"phys"`,
         quotedStaging: `"no_such_stage"`,
         renameTarget: `"phys"`,
      });
      expect(await attempt(failing.sql)).toBeDefined();
      await duck.runSQL(failing.rollback!);
      const keptOnFailure = await exists("phys");
      const swap = transactionalSwapSql("duckdb", {
         before: [],
         quotedPhysical: `"phys"`,
         quotedStaging: `"stage"`,
         renameTarget: `"phys"`,
      });
      await duck.runSQL(swap.sql);
      expect(await rows(`SELECT v FROM phys`)).toEqual([{ v: 2 }]);
      expect(await exists("stage")).toBe(false);
      expect(keptOnFailure).toBe(CAN.transactionalSwap);
   });

   it("incrementalTargetDml: a delete and insert apply in one transaction and roll back together", async () => {
      await duck.runSQL(`CREATE TABLE d (w INT, v TEXT)`);
      await duck.runSQL(`INSERT INTO d VALUES (1, 'old'), (5, 'old')`);
      const delta = (deltaSQL: string) =>
         deltaStatements({
            dialect: "duckdb",
            quotedTablePath: `"d"`,
            deltaSQL,
            columns: ["w", "v"],
            mergeKeys: [],
            watermarkName: "w",
            start: { malloyType: "number", value: "5" },
            end: { malloyType: "number", value: "10" },
         });
      const failed = await applyDeltaScript(
         runner,
         "duckdb",
         delta(`SELECT 5 AS w, error('boom')::TEXT AS v`),
      ).then(
         () => false,
         () => true,
      );
      const afterFailure = await rows(`SELECT w, v FROM d ORDER BY w`);
      await applyDeltaScript(
         runner,
         "duckdb",
         delta(`SELECT 6 AS w, 'new' AS v`),
      );
      const applied = await rows(`SELECT w, v FROM d ORDER BY w`);
      expect(failed).toBe(true);
      expect(afterFailure).toEqual([
         { w: 1, v: "old" },
         { w: 5, v: "old" },
      ]);
      expect(applied).toEqual([
         { w: 1, v: "old" },
         { w: 6, v: "new" },
      ]);
      expect(CAN.incrementalTargetDml).toBe(true);
   });

   it("merge: MERGE INTO updates and inserts", async () => {
      await duck.runSQL(`CREATE TABLE m (k INT, v TEXT)`);
      await duck.runSQL(`INSERT INTO m VALUES (1, 'a'), (NULL, 'n')`);
      await applyDeltaScript(
         runner,
         "duckdb",
         deltaStatements({
            dialect: "duckdb",
            quotedTablePath: `"m"`,
            deltaSQL: `SELECT * FROM (VALUES (1, 'b'), (2, 'c'), (NULL, 'm')) AS d(k, v)`,
            columns: ["k", "v"],
            mergeKeys: ["k"],
            watermarkName: "k",
            start: { malloyType: "number", value: "0" },
            end: { malloyType: "number", value: "9" },
         }),
      );
      expect(await rows(`SELECT k, v FROM m ORDER BY k NULLS FIRST`)).toEqual([
         { k: null, v: "m" },
         { k: 1, v: "b" },
         { k: 2, v: "c" },
      ]);
      expect(CAN.merge).toBe(true);
   });

   it("pairDelta: a parent's rows are replaced by key in one transaction", async () => {
      await duck.runSQL(`CREATE TABLE p (k INT, c TEXT, parent BOOLEAN)`);
      await duck.runSQL(
         `INSERT INTO p VALUES (1, 'x', true), (1, 'y', false), (2, 'z', true)`,
      );
      const delta = (deltaSQL: string) =>
         pairDeltaStatements({
            dialect: "duckdb",
            quotedTablePath: `"p"`,
            deltaSQL,
            columns: ["k", "c", "parent"],
            pair: {
               parentKey: "k",
               parentRow: "parent",
               stagingTable: "p_stage",
            },
         });
      // A delta that leaves parent 1 with no parent row fails its check, and
      // the delete rolls back with it.
      const refused = await applyDeltaScript(
         runner,
         "duckdb",
         delta(`SELECT 1 AS k, 'w' AS c, false AS parent`),
      ).then(
         () => false,
         () => true,
      );
      const afterRefusal = await rows(`SELECT k, c FROM p ORDER BY k, c`);
      await applyDeltaScript(
         runner,
         "duckdb",
         delta(`SELECT 1 AS k, 'w' AS c, true AS parent`),
      );
      expect(refused).toBe(true);
      expect(afterRefusal).toEqual([
         { k: 1, c: "x" },
         { k: 1, c: "y" },
         { k: 2, c: "z" },
      ]);
      expect(await rows(`SELECT k, c FROM p ORDER BY k, c`)).toEqual([
         { k: 1, c: "w" },
         { k: 2, c: "z" },
      ]);
      expect(CAN.pairDelta).toBe(true);
   });

   it("relationProbe: a table's comment reads back through the relation probe", async () => {
      await duck.runSQL(`CREATE TABLE c AS SELECT 1 AS v`);
      await duck.runSQL(tableCommentSql(["c"], "it's marked", "duckdb"));
      expect(await rows(relationProbeSql(["c"], "duckdb"))).toEqual([
         expect.objectContaining({ kind: "TABLE", comment: "it's marked" }),
      ]);
      expect(await rows(relationProbeSql(["nope"], "duckdb"))).toEqual([]);
      expect(CAN.relationProbe).toBe(true);
   });

   it("tableOwnership: the ownership mark's comment reads back with its location", async () => {
      await duck.runSQL(`CREATE TABLE o AS SELECT 1 AS v`);
      await duck.runSQL(tableCommentSql(["o"], "owner=e/p", "duckdb"));
      expect(await rows(ownedTableProbeSql(["o"], "duckdb"))).toEqual([
         expect.objectContaining({
            kind: "TABLE",
            comment: "owner=e/p",
            location_schema: "main",
            location_name: "o",
         }),
      ]);
      expect(CAN.tableOwnership).toBe(true);
   });

   it("nestedColumns: a stored list and struct read back as built", async () => {
      await duck.runSQL(
         `CREATE TABLE n AS SELECT [1, 2] AS l, {'a': 1, 'b': 'x'} AS s, [{'a': 1}] AS ls`,
      );
      expect(await rows(`SELECT l, s, ls FROM n`)).toEqual([
         { l: [1, 2], s: { a: 1, b: "x" }, ls: [{ a: 1 }] },
      ]);
      expect(CAN.nestedColumns).toBe(true);
   });

   it("driverKeepsBigIntegers: a BIGINT past 2^53 keeps every digit", async () => {
      const [row] = await rows(
         `SELECT 9007199254740993::BIGINT AS a, 9223372036854775807::BIGINT AS b`,
      );
      expect(String(row?.["a"])).toBe("9007199254740993");
      expect(String(row?.["b"])).toBe("9223372036854775807");
      expect(CAN.driverKeepsBigIntegers).toBe(true);
   });

   it("castsToIntegerType: CAST AS BIGINT", async () => {
      const [row] = await rows(`SELECT CAST(41.9 AS BIGINT) AS i`);
      expect(String(row?.["i"])).toBe("42");
      expect(CAN.castsToIntegerType).toBe(true);
   });

   it("foldsTableNameCase: a quoted name resolves case-insensitively", async () => {
      await duck.runSQL(`CREATE TABLE "Folded" AS SELECT 1 AS v`);
      const found = await attempt(`SELECT v FROM "FOLDED"`);
      expect(found === undefined).toBe(CAN.foldsTableNameCase);
   });

   it("decodedTablePath: a bare and a quoted path find the table's columns through the target probe", async () => {
      // A storage destination's path: `<destination>.[<schema>.]<name>`.
      await duck.runSQL(`CREATE TABLE "MiXed" AS SELECT 1 AS a, 2 AS "B"`);
      await duck.runSQL(`CREATE TABLE plain AS SELECT 1 AS v`);
      const quoted = await probeTargetColumns(
         runner,
         "duckdb",
         `conformance."MiXed"`,
      );
      const bare = await probeTargetColumns(
         runner,
         "duckdb",
         `conformance.main.plain`,
      );
      const found = quoted.columns.length === 2 && bare.columns.length === 1;
      expect({ quoted, bare }).toEqual({
         quoted: {
            columns: ["a", "B"],
            types: new Map([
               ["a", "INTEGER"],
               ["B", "INTEGER"],
            ]),
         },
         bare: { columns: ["v"], types: new Map([["v", "INTEGER"]]) },
      });
      expect(found).toBe(CAN.decodedTablePath);
   });

   it("utcSessions: the session's TimeZone is UTC", async () => {
      const [row] = await rows(`SELECT current_setting('TimeZone') AS tz`);
      expect(row?.["tz"] === "UTC").toBe(CAN.utcSessions);
   });

   it("leafBudget: an identifier of the budget plus the build's suffix is kept whole", async () => {
      const name = "y".repeat(CAN.leafBudget + 13);
      await duck.runSQL(`CREATE TABLE "${name}" AS SELECT 1 AS v`);
      const [row] = await rows(
         `SELECT max(length(table_name))::INT AS n FROM duckdb_tables() WHERE table_name LIKE 'yyyy%'`,
      );
      expect(row?.["n"]).toBe(CAN.leafBudget + 13);
   });

   it("stringCollation: comparisons are byte-exact unless default_collation is set", async () => {
      const byteExact = await rows(`SELECT 'a' = 'A' AS eq, 'a' < 'B' AS lt`);
      const other = new DuckDBConnection(
         "collated",
         path.join(dir, "collated.duckdb"),
      );
      try {
         await other.runSQL(`SET default_collation = 'nocase'`);
         const collated = (
            await other.runSQL(`SELECT 'a' = 'A' AS eq, 'a' < 'B' AS lt`)
         ).rows;
         expect(byteExact).toEqual([{ eq: false, lt: false }]);
         expect(collated).toEqual([{ eq: true, lt: true }]);
      } finally {
         await other.close();
      }
      expect(CAN.stringCollation).toBe("instance");
   });

   it("binary64Types: DOUBLE and FLOAT8 are binary64 with NaN equal to itself and above all", async () => {
      await duck.runSQL(
         `CREATE TABLE f (a DOUBLE, b FLOAT8, c REAL, d FLOAT4)`,
      );
      const types = await rows(
         `SELECT column_name AS name, data_type AS type FROM duckdb_columns() WHERE table_name = 'f' ORDER BY column_index`,
      );
      expect(
         Object.fromEntries(
            types.map((r) => [
               r["name"],
               isBinary64Type("duckdb", r["type"] as string),
            ]),
         ),
      ).toEqual({ a: true, b: true, c: false, d: false });
      // The catalog spells FLOAT8 as DOUBLE; FLOAT8 is listed for a native
      // type an author writes.
      expect(isBinary64Type("duckdb", "float8")).toBe(true);
      const [nan] = await rows(
         `SELECT 'NaN'::DOUBLE = 'NaN'::DOUBLE AS eq, 'NaN'::DOUBLE > 'Infinity'::DOUBLE AS above, 0.1::DOUBLE = 0.1 AS exact`,
      );
      expect(nan).toEqual({ eq: true, above: true, exact: true });
   });

   it("the collation reader reads the instance: binary until a COLLATE or a default_collation", async () => {
      const other = new DuckDBConnection(
         "reader",
         path.join(dir, "reader.duckdb"),
      );
      try {
         const io = probeIO(other as never);
         await other.runSQL(`CREATE TABLE plain (s VARCHAR)`);
         expect(
            await DUCKDB_COLLATION.sourceColumns(
               io,
               "SELECT 'a' AS probe_value",
            ),
         ).toBe("binary");
         expect(await DUCKDB_COLLATION.storedTable(io, "plain")).toBe("binary");
         expect(
            await DUCKDB_COLLATION.sourceColumns(
               io,
               "SELECT 'a' COLLATE NOCASE AS probe_value",
            ),
         ).toBe("unknown");
         await other.runSQL(`CREATE TABLE ci (s VARCHAR COLLATE NOCASE)`);
         expect(await DUCKDB_COLLATION.storedTable(io, "ci")).toBe("unknown");
         expect(await DUCKDB_COLLATION.storedTable(io, "plain")).toBe("binary");
         expect(
            await DUCKDB_COLLATION.sourceColumns(
               io,
               "SELECT 'a' AS probe_value",
            ),
         ).toBe("unknown");
         await other.runSQL(`SET default_collation = 'nocase'`);
         expect(await DUCKDB_COLLATION.storedTable(io, "plain")).toBe(
            "unknown",
         );
      } finally {
         await other.close();
      }
   });
});
