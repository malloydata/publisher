// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The Postgres row of the capability table (`domain/dialect.ts`), observed on
 * a real server: each test runs what the capability promises against the
 * engine, through the statements Publisher sends where they exist, and
 * asserts the observation matches `CAPABILITIES.postgres`. A test name here
 * is a probe reference in `domain/dialect_evidence.ts`; renaming one fails
 * `dialect_evidence.spec.ts`.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop a schema and a collation on (CI's connection-integration job).
 * `passthroughNumericAsDouble` also loads DuckDB's postgres extension.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
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
   probeServerVersion,
   probeTargetColumns,
   renderSqlBound,
} from "../../src/service/materialization/run/incremental_apply";
import { POSTGRES_COLLATION } from "../../src/service/materialization/serve/collation/postgres";
import { catalogIO } from "../../src/service/materialization/serve/collation/catalog_io";
import {
   relationProbeSql,
   tableCommentSql,
   transactionalSwapSql,
   viewReadsSql,
   ownedTableProbeSql,
} from "../../src/service/materialization/run/warehouse_sql";

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(120_000);

import { readTargetContainer } from "../../src/service/materialization/run/target_container_read";

const CAN = CAPABILITIES.postgres;

describe.skipIf(!url)("Postgres conformance", () => {
   const schema = `conf_${Math.random().toString(36).slice(2, 8)}`;
   let pg: PostgresConnection;

   /** Plain rows of a select (Malloy's runSQL unwraps one `row` column). */
   async function rows(sql: string): Promise<Record<string, unknown>[]> {
      const result = await pg.runSQL(
         `SELECT row_to_json(q) AS "row" FROM (${sql}) AS q`,
      );
      return result.rows as Record<string, unknown>[];
   }
   /** A statement (or several, one implicit transaction) for its effect. */
   async function run(sql: string): Promise<void> {
      await pg.runSQL(sql);
   }
   /** Whether `sql` runs, and the error it fails with when it does not. */
   async function attempt(sql: string): Promise<string | undefined> {
      try {
         await run(sql);
         return undefined;
      } catch (err) {
         return err instanceof Error ? err.message : String(err);
      }
   }
   const t = (name: string) => `"${schema}"."${name}"`;
   async function exists(name: string): Promise<boolean> {
      const found = await rows(
         `SELECT to_regclass('${t(name).replace(/'/g, "''")}') IS NOT NULL AS e`,
      );
      return found[0]?.["e"] === true;
   }

   beforeAll(async () => {
      pg = new PostgresConnection({ name: "pg", connectionString: url! });
      await run(`CREATE SCHEMA "${schema}"`);
   });

   afterAll(async () => {
      await attempt(`DROP SCHEMA "${schema}" CASCADE`);
      await pg?.close();
   });

   it("transactionalSwap: the swap's drop and rename roll back together", async () => {
      await run(`CREATE TABLE ${t("phys")} AS SELECT 1 AS v`);
      await run(`CREATE TABLE ${t("stage")} AS SELECT 2 AS v`);
      // The swap's DDL inside a transaction that is rolled back: nothing moved.
      await run(
         `BEGIN; DROP TABLE ${t("phys")}; ALTER TABLE ${t("stage")} RENAME TO "phys"; ROLLBACK;`,
      );
      const rolledBack = (await exists("phys")) && (await exists("stage"));
      // Publisher's own swap text, failing after its drop: the implicit
      // transaction keeps the physical table.
      const failing = transactionalSwapSql("postgres", {
         before: [],
         quotedPhysical: t("phys"),
         quotedStaging: t("no_such_stage"),
         renameTarget: `"phys"`,
      });
      expect(failing.rollback).toBeUndefined();
      expect(await attempt(failing.sql)).toBeDefined();
      const keptOnFailure = await exists("phys");
      // And the same text swaps when it succeeds.
      await run(
         transactionalSwapSql("postgres", {
            before: [],
            quotedPhysical: t("phys"),
            quotedStaging: t("stage"),
            renameTarget: `"phys"`,
         }).sql,
      );
      const swapped = await rows(`SELECT v FROM ${t("phys")}`);
      expect(swapped).toEqual([{ v: 2 }]);
      expect(rolledBack && keptOnFailure).toBe(CAN.transactionalSwap);
   });

   it("mergeGatedByServerVersion: server_version_num is read, and MERGE exists from 15", async () => {
      const version = await probeServerVersion(
         (sql) => pg.runSQL(sql),
         "postgres",
      );
      expect(typeof version).toBe("number");
      // MERGE arrived in 15; a server below it refuses the statement, so the
      // capability is gated by the version read here.
      const merge = await attempt(
         `MERGE INTO ${t("phys")} AS __t USING (SELECT 1 AS v) AS __s ON __t.v = __s.v WHEN MATCHED THEN DO NOTHING`,
      );
      expect(merge === undefined).toBe(version! >= 150000);
      expect(CAN.mergeGatedByServerVersion).toBe(true);
   });

   it("merge: MERGE INTO updates and inserts", async () => {
      await run(`CREATE TABLE ${t("m")} (k int, v text)`);
      await run(`INSERT INTO ${t("m")} VALUES (1, 'a'), (NULL, 'n')`);
      const statements = deltaStatements({
         dialect: "postgres",
         quotedTablePath: t("m"),
         deltaSQL: `SELECT * FROM (VALUES (1, 'b'), (2, 'c'), (NULL, 'm')) AS d(k, v)`,
         columns: ["k", "v"],
         mergeKeys: ["k"],
         watermarkName: "k",
         start: { malloyType: "number", value: "0" },
         end: { malloyType: "number", value: "9" },
      });
      await applyDeltaScript((sql) => pg.runSQL(sql), "postgres", statements);
      const merged = await rows(
         `SELECT k, v FROM ${t("m")} ORDER BY k NULLS FIRST`,
      );
      expect(merged).toEqual([
         { k: null, v: "m" },
         { k: 1, v: "b" },
         { k: 2, v: "c" },
      ]);
      expect(CAN.merge).toBe(true);
   });

   it("incrementalTargetDml: a delete and insert apply in one transaction and roll back together", async () => {
      await run(`CREATE TABLE ${t("d")} (w int, v text)`);
      await run(`INSERT INTO ${t("d")} VALUES (1, 'old'), (5, 'old')`);
      const delta = (deltaSQL: string) =>
         deltaStatements({
            dialect: "postgres",
            quotedTablePath: t("d"),
            deltaSQL,
            columns: ["w", "v"],
            mergeKeys: [],
            watermarkName: "w",
            start: { malloyType: "number", value: "5" },
            end: { malloyType: "number", value: "10" },
         });
      const runner = (sql: string) => pg.runSQL(sql);
      // A delta whose insert fails: the delete before it rolls back too.
      const failed = await applyDeltaScript(
         runner,
         "postgres",
         delta(`SELECT 5 AS w, (1 / 0)::text AS v`),
      ).then(
         () => false,
         () => true,
      );
      const afterFailure = await rows(`SELECT w, v FROM ${t("d")} ORDER BY w`);
      await applyDeltaScript(
         runner,
         "postgres",
         delta(`SELECT 6 AS w, 'new' AS v`),
      );
      const applied = await rows(`SELECT w, v FROM ${t("d")} ORDER BY w`);
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

   it("incrementalSourceRange: a bounded range over every watermark type's literal", async () => {
      await run(
         `CREATE TABLE ${t("r")} (d date, ts timestamp, tz timestamptz, n numeric, s text)`,
      );
      await run(`INSERT INTO ${t("r")} VALUES
         ('2024-01-01', '2024-01-01 00:00:00', '2024-01-01 00:00:00+00', 1, 'a'),
         ('2024-01-02', '2024-01-02 12:00:00', '2024-01-02 12:00:00+00', 2, 'b'),
         ('2024-01-03', '2024-01-03 00:00:00', '2024-01-03 00:00:00+00', 3, 'c')`);
      const counts: Record<string, unknown> = {};
      for (const [column, malloyType, start, end] of [
         ["d", "date", "2024-01-02", "2024-01-03"],
         ["ts", "timestamp", "2024-01-02T00:00:00", "2024-01-03T00:00:00"],
         ["tz", "timestamp", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"],
         ["n", "number", "2", "3"],
         ["s", "string", "b", "c"],
      ] as const) {
         const lo = renderSqlBound({ malloyType, value: start }, "postgres");
         const hi = renderSqlBound({ malloyType, value: end }, "postgres");
         const [row] = await rows(
            `SELECT count(*)::int AS n FROM ${t("r")} WHERE ${column} >= ${lo} AND ${column} < ${hi}`,
         );
         counts[column] = row?.["n"];
      }
      expect(counts).toEqual({ d: 1, ts: 1, tz: 1, n: 1, s: 1 });
      expect(CAN.incrementalSourceRange).toBe(true);
   });

   it("dependencyGuard: a view's dependency on its table is read from the catalog", async () => {
      await run(`CREATE TABLE ${t("base")} AS SELECT 1 AS v`);
      await run(`CREATE TABLE ${t("other")} AS SELECT 1 AS v`);
      await run(`CREATE VIEW ${t("vw")} AS SELECT v FROM ${t("base")}`);
      const reads = async (table: string) => {
         const probe = viewReadsSql(
            [schema, "vw"],
            [schema, table],
            "postgres",
         );
         expect(probe.answer).toBe("reads");
         const result = await pg.runSQL(probe.sql);
         return (result.rows[0] as { reads?: unknown } | undefined)?.reads;
      };
      const readsBase = await reads("base");
      const readsOther = await reads("other");
      // Repointed with CREATE OR REPLACE, which keeps a comment: the catalog
      // still tells the tables apart.
      await run(
         `CREATE OR REPLACE VIEW ${t("vw")} AS SELECT v FROM ${t("other")}`,
      );
      expect([readsBase, readsOther, await reads("other")]).toEqual([
         true,
         false,
         true,
      ]);
      expect(CAN.dependencyGuard).toBe(true);
   });

   it("decodedTablePath: a bare and a quoted path find the table's columns through the target probe", async () => {
      // A bare segment folds to lower case, a quoted one is kept.
      await run(`CREATE TABLE ${t("MiXed")} AS SELECT 1 AS a, 2 AS "B"`);
      await run(`CREATE TABLE ${t("plain")} AS SELECT 1 AS v`);
      const runner = async (sql: string) => ({
         rows: (await pg.runSQL(sql)).rows,
      });
      const quoted = await probeTargetColumns(
         runner,
         "postgres",
         `"${schema}"."MiXed"`,
      );
      const bare = await probeTargetColumns(
         runner,
         "postgres",
         `${schema}.PLAIN`,
      );
      const found = quoted.columns.length === 2 && bare.columns.length === 1;
      expect({
         quoted: { ...quoted, columns: [...quoted.columns].sort() },
         bare,
      }).toEqual({
         quoted: {
            columns: ["B", "a"],
            types: new Map([
               ["a", "integer"],
               ["B", "integer"],
            ]),
         },
         bare: { columns: ["v"], types: new Map([["v", "integer"]]) },
      });
      expect(found).toBe(CAN.decodedTablePath);
   });

   it("relationProbe: a table's comment reads back through the relation probe", async () => {
      await run(`CREATE TABLE ${t("c")} AS SELECT 1 AS v`);
      await run(tableCommentSql([schema, "c"], "it's marked", "postgres"));
      const result = await pg.runSQL(
         relationProbeSql([schema, "c"], "postgres"),
      );
      expect(result.rows).toEqual([
         expect.objectContaining({ kind: "TABLE", comment: "it's marked" }),
      ]);
      const none = await pg.runSQL(
         relationProbeSql([schema, "nope"], "postgres"),
      );
      expect(none.rows).toEqual([]);
      expect(CAN.relationProbe).toBe(true);
   });

   it("tableOwnership: the ownership mark's comment reads back with its location", async () => {
      await run(`CREATE TABLE ${t("o")} AS SELECT 1 AS v`);
      await run(tableCommentSql([schema, "o"], "owner=e/p", "postgres"));
      const result = await pg.runSQL(
         ownedTableProbeSql([schema, "o"], "postgres"),
      );
      expect(result.rows).toEqual([
         expect.objectContaining({
            kind: "TABLE",
            comment: "owner=e/p",
            location_schema: schema,
            location_name: "o",
         }),
      ]);
      expect(CAN.tableOwnership).toBe(true);
   });

   it("castsToIntegerType: CAST AS BIGINT", async () => {
      const [row] = await rows(`SELECT CAST(41.9 AS bigint) AS i`);
      expect(row?.["i"]).toBe(42);
      expect(CAN.castsToIntegerType).toBe(true);
   });

   it("leafBudget: an identifier past 63 bytes is truncated, so a leaf has 63 less the build's 13", async () => {
      const long = "x".repeat(70);
      await run(`CREATE TABLE ${t(long)} AS SELECT 1 AS v`);
      const [row] = await rows(
         `SELECT max(length(relname))::int AS n FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace WHERE s.nspname = '${schema}' AND relname LIKE 'xxxx%'`,
      );
      const limit = row?.["n"] as number;
      expect(limit).toBe(63);
      expect(CAN.leafBudget).toBe(limit - 13);
   });

   it("liveRoundsBigIntegersMeasured: the driver's JSON stage rounds a bigint past 2^53 to the nearest double", async () => {
      // Live reads Postgres through row_to_json, as Malloy's final stage does.
      const [row] = await rows(
         `SELECT 9007199254740993::int8 AS a, 9223372036854775807::int8 AS b, 1800000000000000000001::numeric AS c`,
      );
      expect(row).toEqual({
         a: 9007199254740992,
         b: 2 ** 63,
         c: 1.8e21,
      });
      expect(String(row?.["b"])).toBe("9223372036854776000");
      expect(CAN.liveRoundsBigIntegersMeasured).toBe(true);
      expect(CAN.driverKeepsBigIntegers).toBe(false);
   });

   it("liveWritesFloat4Shortest: the driver's JSON stage writes a float4 as its shortest text", async () => {
      const [row] = await rows(
         `SELECT 0.1::float4 AS f, 16777217::float4 AS g, 0.1::float8 AS d`,
      );
      // float4out writes 0.1, not the widened 0.10000000149011612.
      expect(row).toEqual({ f: 0.1, g: 16777216, d: 0.1 });
      expect(Math.fround(0.1)).not.toBe(0.1);
      expect(CAN.liveWritesFloat4Shortest).toBe(true);
   });

   it("binary64Types: format_type spells float8 and float(25..53) as double precision", async () => {
      await run(
         `CREATE TABLE ${t("f")} (a float8, b float(25), c float(53), d double precision, e real, g float(24), h numeric)`,
      );
      const types = await rows(
         `SELECT attname AS name, format_type(atttypid, atttypmod) AS type FROM pg_attribute WHERE attrelid = to_regclass('"${schema}"."f"') AND attnum > 0 ORDER BY attnum`,
      );
      const binary64 = Object.fromEntries(
         types.map((r) => [
            r["name"],
            isBinary64Type("postgres", r["type"] as string),
         ]),
      );
      expect(binary64).toEqual({
         a: true,
         b: true,
         c: true,
         d: true,
         e: false,
         g: false,
         h: false,
      });
      // NaN is equal to itself and above every other value, as DuckDB's.
      const [nan] = await rows(
         `SELECT 'NaN'::float8 = 'NaN'::float8 AS eq, 'NaN'::float8 > 'Infinity'::float8 AS above`,
      );
      expect(nan).toEqual({ eq: true, above: true });
   });

   it("ungroupableTypes: each listed native type fails GROUP BY, and jsonb groups", async () => {
      const native = CAN.ungroupableTypes?.native ?? [];
      expect(native.length).toBeGreaterThan(0);
      const failures: Record<string, boolean> = {};
      for (const type of native) {
         for (const spelled of [type, `${type}[]`]) {
            const error = await attempt(
               `SELECT x FROM (SELECT NULL::${spelled} AS x) AS v GROUP BY x`,
            );
            failures[spelled] = error !== undefined;
         }
      }
      expect(Object.values(failures).every((f) => f)).toBe(true);
      expect(
         await attempt(
            `SELECT x FROM (SELECT '{"a":1}'::jsonb AS x) AS v GROUP BY x`,
         ),
      ).toBeUndefined();
      expect(CAN.ungroupableTypes?.malloy).toEqual([]);
   });

   it("containerCatalog: pg_namespace finds a schema exactly as quoted, and not a missing one", async () => {
      const read = (c: string) => readTargetContainer(pg, c);
      const [db] = await rows(`SELECT current_database() AS db`);
      expect({
         schema: await read(schema),
         qualified: await read(`${String(db?.["db"])}.${schema}`),
         otherCase: await read(schema.toUpperCase()),
         missing: await read(`${schema}_missing`),
      }).toEqual({
         schema: { found: true },
         qualified: { found: true },
         otherCase: { notFound: true },
         missing: { notFound: true },
      });
      expect(CAN.containerCatalog).toBe("pg_namespace");
   });

   it("stringCollation: pg_collation_for reads each column's collation", async () => {
      await run(
         `CREATE COLLATION "${schema}".ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`,
      );
      await run(
         `CREATE TABLE ${t("s")} (d text, c text COLLATE "C", n text COLLATE "${schema}".ci)`,
      );
      await run(`INSERT INTO ${t("s")} VALUES ('a', 'a', 'a')`);
      const [read] = await rows(
         `SELECT pg_collation_for(d) AS d, pg_collation_for(c) AS c, pg_collation_for(n) AS n FROM ${t("s")}`,
      );
      expect(read).toEqual({
         d: '"default"',
         c: '"C"',
         n: `${schema}.ci`,
      });
      // The column's collation decides equality: the nondeterministic one
      // finds 'A' equal to 'a', the others do not.
      const [eq] = await rows(
         `SELECT d = 'A' AS d, c = 'A' AS c, n = 'A' AS n FROM ${t("s")}`,
      );
      expect(eq).toEqual({ d: false, c: false, n: true });
      expect(CAN.stringCollation).toBe("column");
   });

   it("the collation reader judges columns under C, an ICU nondeterministic collation and the default", async () => {
      // The fixture from the stringCollation probe above: d default, c "C",
      // n nondeterministic ICU.
      const io = catalogIO(pg as never);
      const column = (name: string) =>
         POSTGRES_COLLATION.sourceColumns(
            io,
            `SELECT ${name} AS probe_value FROM ${t("s")}`,
         );
      const fallback = await POSTGRES_COLLATION.defaultCollation(io);
      expect(["binary", "deterministic"]).toContain(fallback);
      expect({
         c: await column("c"),
         n: await column("n"),
         d: await column("d"),
      }).toEqual({ c: "binary", n: "unknown", d: fallback });
      // A table holding the nondeterministic column is unknown; one of C
      // columns only is binary.
      expect(await POSTGRES_COLLATION.storedTable(io, `"${schema}"."s"`)).toBe(
         "unknown",
      );
      await run(`CREATE TABLE ${t("sc")} (c text COLLATE "C", i int)`);
      expect(await POSTGRES_COLLATION.storedTable(io, `"${schema}"."sc"`)).toBe(
         "binary",
      );
      // Identities tell the three apart, and the default column reads as
      // the database default's.
      const ids = await POSTGRES_COLLATION.columnIdentities(
         io,
         `"${schema}"."s"`,
      );
      expect(new Set(ids.values()).size).toBe(3);
      expect(ids.get("d")).toBe(await POSTGRES_COLLATION.defaultIdentity(io));
   });

   it("passthroughNumericAsDouble: DuckDB's postgres_query reads an unconstrained numeric as DOUBLE", async () => {
      // A file of its own: in-memory instances can be shared within a process.
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pg-conformance-"));
      const duck = new DuckDBConnection(
         "pg_conformance",
         path.join(dir, "passthrough.duckdb"),
      );
      try {
         await duck.runSQL("INSTALL postgres; LOAD postgres;");
         const dsn = url!.replace(/'/g, "''");
         await duck.runSQL(
            `ATTACH '${dsn}' AS pgdb (TYPE postgres, READ_ONLY)`,
         );
         const result = await duck.runSQL(
            `SELECT typeof(u) AS u, typeof(e) AS e, typeof(w) AS w, typeof(m) AS m, typeof(f) AS f
             FROM postgres_query('pgdb', 'SELECT 0.1::numeric AS u, 0.1::numeric + 0.2::numeric AS e, 0.1::numeric(50,10) AS w, 0.1::numeric(10,2) AS m, 0.1::float8 AS f')`,
         );
         const types = result.rows[0] as Record<string, string>;
         expect(types).toEqual({
            u: "DOUBLE",
            e: "DOUBLE",
            w: "DOUBLE",
            m: "DECIMAL(10,2)",
            f: "DOUBLE",
         });
         expect(types["u"] === "DOUBLE").toBe(CAN.passthroughNumericAsDouble);
      } finally {
         await duck.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
