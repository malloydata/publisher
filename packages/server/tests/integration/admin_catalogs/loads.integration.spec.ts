// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The admin `loads` endpoint (CatalogAdmin.loadTable) against a real Postgres
 * catalog and Parquet objects on a local data path: what a keyed load writes,
 * what it leaves alone, and what it refuses. Same environment as the admin
 * integration spec; skipped without it.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { spawn } from "child_process";
import { createHash } from "crypto";
import {
   copyFileSync,
   mkdirSync,
   mkdtempSync,
   rmSync,
   writeFileSync,
} from "fs";
import { tmpdir } from "os";
import { dirname, join } from "path";
import type { components } from "../../../src/api";
import { logger } from "../../../src/logger";
import { loadSlots } from "../../../src/query_concurrency";
import { resetInstrumentsForTesting } from "../../../src/telemetry";
import { startMetricsHarness } from "../../../src/test_helpers/metrics_harness";
import {
   CatalogAdmin,
   CatalogAdminError,
   type Load,
   type LoadNoOp,
} from "../../../src/service/catalog_admin";
import {
   attachDuckLakeReadOnly,
   attachDuckLakeReadWrite,
} from "../../../src/service/connection";
import {
   createIsolatedBuildSession,
   inTransaction,
} from "../../../src/service/materialization/run/materialization_build_session";
import { canonicalSha } from "../../../src/service/package_revision";
import {
   CommittedLoadError,
   type LoadRefusalError,
   mergeStatements,
   mergeStorageTable,
} from "../../../src/service/materialization/run/storage_table_load";
import { SUPERUSER, superuser } from "./superuser";

const DB = `loads_it_${process.pid}`;
const CATALOG = "lake";

async function refusal(p: Promise<unknown>): Promise<CatalogAdminError> {
   const error = await p.then(
      () => undefined,
      (e) => e,
   );
   expect(error).toBeInstanceOf(CatalogAdminError);
   return error as CatalogAdminError;
}

/** A type the load must round-trip without rewriting its rows on a retry. */
const TYPES: [string, string][] = [
   ["bigint_beyond_2_53", "9007199254740993::BIGINT + i"],
   ["integer", "i::INTEGER"],
   ["smallint", "i::SMALLINT"],
   ["utinyint", "i::UTINYINT"],
   ["ubigint", "18446744073709551612::UBIGINT + i::UBIGINT"],
   ["float", "i::FLOAT / 3"],
   ["double", "i::DOUBLE / 3"],
   ["nan", "CASE WHEN i = 1 THEN 'NaN'::DOUBLE ELSE i::DOUBLE END"],
   ["decimal", "(i / 7.0)::DECIMAL(18, 3)"],
   ["varchar", "'é' || i"],
   ["boolean", "i % 2 = 0"],
   ["date", "DATE '2026-01-01' + i::INTEGER"],
   ["timestamp", "TIMESTAMP '2026-01-01 00:00:00.123456' + to_hours(i)"],
   ["timestamptz", "TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_hours(i)"],
   ["time", "TIME '01:02:03.456' + to_minutes(i)"],
   ["blob", "('\\x01\\xAB' || i)::BLOB"],
   ["uuid", "uuid()"],
   ["list", "[i, i + 1]"],
   ["struct", "{'a': i, 'b': 'x' || i}"],
   ["map", "MAP {'k': i}"],
   ["nullable", "CASE WHEN i % 2 = 0 THEN NULL ELSE i END"],
];

describe.skipIf(!SUPERUSER)("loads", () => {
   const root = mkdtempSync(join(tmpdir(), "loads-it-"));
   const dataPath = join(root, "data");
   const conninfo = `${SUPERUSER} dbname=${DB}`;
   const disposers: (() => Promise<void>)[] = [];
   let admin: CatalogAdmin;
   let fixtures: DuckDBConnection;
   let reader: DuckDBConnection;
   let writer: DuckDBConnection;
   let counter = 0;

   /** Writes a Parquet object under `base` and returns its URI. */
   async function parquet(
      select: string,
      base = dataPath,
      name = `o${counter++}`,
   ): Promise<string> {
      const file = join(base, "staging", `${name}.parquet`);
      mkdirSync(dirname(file), { recursive: true });
      await fixtures.runSQL(`COPY (${select}) TO '${file}' (FORMAT parquet)`);
      return file;
   }

   /** Copies a fixture Parquet object under the data path and returns its URI. */
   function fixture(name: string): string {
      const file = join(dataPath, "staging", `f${counter++}_${name}`);
      mkdirSync(dirname(file), { recursive: true });
      copyFileSync(join(__dirname, "fixtures", name), file);
      return file;
   }

   async function sql(
      session: DuckDBConnection,
      text: string,
   ): Promise<Record<string, unknown>[]> {
      return (
         (await session.runSQL(text)) as unknown as {
            rows: Record<string, unknown>[];
         }
      ).rows;
   }

   /** A DuckLake session on a catalog database (the suite's by default), with its own disposer. */
   async function openSession(
      name: string,
      mode: "read" | "write",
      catalog = CATALOG,
      connectionString = conninfo,
   ) {
      const { session, dispose } = createIsolatedBuildSession(name);
      disposers.push(dispose);
      const config = {
         catalog: {
            postgresConnection: { connectionString },
            metadataSchema: catalog,
         },
      } as Parameters<typeof attachDuckLakeReadOnly>[2];
      await (
         mode === "read" ? attachDuckLakeReadOnly : attachDuckLakeReadWrite
      )(session, catalog, config);
      return { session, dispose };
   }

   /** A DuckLake session on the catalog database, as a replica's would be. */
   async function attached(
      name: string,
      mode: "read" | "write",
      catalog = CATALOG,
   ): Promise<DuckDBConnection> {
      return (await openSession(name, mode, catalog)).session;
   }

   const load = async (
      table: string,
      body: Record<string, unknown>,
      catalog = CATALOG,
   ) => (await admin.loadTable(catalog, "s", table, body)).load;

   const merge = (
      table: string,
      objects: string[],
      extra: Record<string, unknown> = {},
   ) => load(table, { mode: "merge", key: ["id"], objects, ...extra });

   const rowsOf = (table: string, columns = "id::INTEGER AS id, v") =>
      sql(reader, `SELECT ${columns} FROM ${CATALOG}.s.${table} ORDER BY id`);

   const snapshotId = async () => (await admin.listSnapshots(CATALOG, 1))[0].id;

   /** The ids of a table's live data files. */
   async function dataFileIds(table: string): Promise<number[]> {
      return (
         await superuser(
            `select f.data_file_id::int as id from ${CATALOG}.ducklake_data_file f join ${CATALOG}.ducklake_table t ` +
               `on f.table_id = t.table_id where t.table_name = '${table}' and t.end_snapshot is null and f.end_snapshot is null order by 1`,
            DB,
         )
      ).map((row) => Number(row.id));
   }

   const dataFiles = async (table: string) => (await dataFileIds(table)).length;

   async function inlinedRows(catalog: string, table: string): Promise<number> {
      let rows = 0;
      for (const { table_name } of await superuser(
         `select i.table_name from ${catalog}.ducklake_inlined_data_tables i join ${catalog}.ducklake_table t ` +
            `on i.table_id = t.table_id where t.table_name = '${table}' and t.end_snapshot is null`,
         DB,
      )) {
         const [row] = await superuser(
            `select count(*)::int as n from ${catalog}."${String(table_name)}" where end_snapshot is null`,
            DB,
         );
         rows += Number(row.n);
      }
      return rows;
   }

   /** The statement_usage records the callback's loads emit. */
   async function metered(run: () => Promise<unknown>) {
      const spy = spyOn(logger, "info");
      try {
         await run().catch(() => undefined);
         return (
            spy.mock.calls as unknown as [unknown, Record<string, unknown>][]
         )
            .filter((c) => c[0] === "statement_usage")
            .map((c) => c[1]);
      } finally {
         spy.mockRestore();
      }
   }

   /** What `run` answers, and the statements every DuckDB session ran meanwhile. */
   async function recorded<T>(run: () => Promise<T>) {
      const original = DuckDBConnection.prototype.runSQL;
      const seen: string[] = [];
      const spy = spyOn(
         DuckDBConnection.prototype,
         "runSQL",
      ).mockImplementation(function (
         this: DuckDBConnection,
         ...args: Parameters<typeof original>
      ) {
         seen.push(args[0]);
         return original.apply(this, args);
      });
      try {
         return { value: await run(), seen };
      } finally {
         spy.mockRestore();
      }
   }

   beforeAll(async () => {
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
      await superuser(`CREATE DATABASE ${DB}`);
      admin = new CatalogAdmin({ catalogDatabase: conninfo });
      await admin.createCatalog({
         name: CATALOG,
         dataPath,
         encrypted: true,
         schemas: ["s"],
      });
      fixtures = new DuckDBConnection(
         "fixtures",
         ":memory:",
         mkdtempSync(join(tmpdir(), "fx-")),
      );
      reader = await attached("loads_reader", "read");
      writer = await attached("loads_writer", "write");
   });

   afterAll(async () => {
      for (const dispose of disposers) await dispose();
      await fixtures?.close();
      rmSync(root, { recursive: true, force: true });
      await superuser(`DROP DATABASE IF EXISTS ${DB} WITH (FORCE)`);
   });

   describe("merge", () => {
      it("creates the table, then inserts new keys, replaces changed ones and leaves the rest", async () => {
         const first = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         expect(await merge("basic", [first])).toMatchObject({
            rowsInserted: 5,
            rowsReplaced: 0,
            rowsUnchanged: 0,
            rowsDeleted: 0,
            rowCount: 5,
            cursorMax: null,
         });
         const second = await parquet(
            "SELECT * FROM (VALUES (2, 'changed'), (5, 'v5'), (6, 'new')) t(id, v)",
         );
         expect(await dataFiles("basic")).toBeGreaterThan(0);
         expect(await merge("basic", [second])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
            rowsUnchanged: 1,
            rowsDeleted: 0,
            rowCount: 6,
            cursorMax: null,
         });
         expect(await rowsOf("basic")).toEqual([
            { id: 1, v: "v1" },
            { id: 2, v: "changed" },
            { id: 3, v: "v3" },
            { id: 4, v: "v4" },
            { id: 5, v: "v5" },
            { id: 6, v: "new" },
         ]);
         expect(await merge("basic", [second])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 3,
            rowsDeleted: 0,
            rowCount: 6,
            cursorMax: null,
         });
      });

      it("keeps the latest row of a key, across objects by their order and within one by its row", async () => {
         const a = await parquet(
            "SELECT * FROM (VALUES (1, 'a1'), (1, 'a1-later'), (2, 'a2')) t(id, v)",
         );
         const b = await parquet("SELECT * FROM (VALUES (2, 'b2')) t(id, v)");
         const result = await merge("dedupe", [a, b]);
         expect([result.rowsInserted, result.rowCount]).toEqual([2, 2]);
         expect(await rowsOf("dedupe")).toEqual([
            { id: 1, v: "a1-later" },
            { id: 2, v: "b2" },
         ]);
         await merge("dedupe", [b, a]);
         expect(await rowsOf("dedupe")).toEqual([
            { id: 1, v: "a1-later" },
            { id: 2, v: "a2" },
         ]);
      });

      it("refuses a URI listed twice, before anything is written", async () => {
         const a = await parquet("SELECT 1 AS id, 'x' AS v");
         const error = await refusal(merge("dupe_uri", [a, a]));
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(await admin.listTables(CATALOG, "s")).not.toContainEqual(
            expect.objectContaining({ name: "dupe_uri" }),
         );
      });

      it("heals a key stored twice by rewriting it once", async () => {
         const a = await parquet("SELECT 1 AS id, 'x' AS v");
         await merge("heal", [a]);
         await sql(writer, `INSERT INTO ${CATALOG}.s.heal VALUES (1, 'x')`);
         expect(await rowsOf("heal")).toHaveLength(2);
         expect(await merge("heal", [a])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 1,
            rowsUnchanged: 0,
            rowsDeleted: 1,
            rowCount: 1,
            cursorMax: null,
         });
         expect(await rowsOf("heal")).toEqual([{ id: 1, v: "x" }]);
      });

      it("matches a composite key", async () => {
         const a = await parquet(
            "SELECT * FROM (VALUES (1, 'a', 'x'), (1, 'b', 'y')) t(id, region, v)",
         );
         await load("composite", {
            mode: "merge",
            key: ["id", "region"],
            objects: [a],
         });
         const b = await parquet(
            "SELECT * FROM (VALUES (1, 'b', 'z'), (2, 'a', 'w')) t(id, region, v)",
         );
         const result = await load("composite", {
            mode: "merge",
            key: ["id", "region"],
            objects: [b],
         });
         expect([
            result.rowsInserted,
            result.rowsReplaced,
            result.rowsUnchanged,
            result.rowCount,
         ]).toEqual([1, 1, 0, 3]);
      });

      it("reads nested columns that sit before a plain one, and objects whose schemas differ", async () => {
         const a = await parquet(
            "SELECT 1 AS id, {'x': 1, 'y': {'z': 2}} AS st, [1, 2] AS l, MAP {'k': 1} AS m, 'tail' AS last",
         );
         const b = await parquet(
            "SELECT 2 AS id, {'x': 3, 'y': {'z': 4}} AS st, [3] AS l, MAP {'k': 2} AS m, 'tail2' AS last, 'extra' AS only_b",
         );
         expect(await merge("nested", [a, b])).toMatchObject({
            rowsInserted: 2,
            rowCount: 2,
         });
         const table = await admin.getTable(CATALOG, "s", "nested");
         expect(table.columns.map((c) => c.name)).toEqual([
            "id",
            "st",
            "l",
            "m",
            "last",
            "only_b",
         ]);
         expect(await merge("nested", [a, b])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 2,
         });
      });

      it("writes an object with no row into a table that exists, and creates an empty table", async () => {
         const empty = await parquet("SELECT 1 AS id, 'x' AS v WHERE false");
         expect(await merge("empty_new", [empty])).toMatchObject({
            rowsInserted: 0,
            rowCount: 0,
         });
         const filled = await parquet("SELECT 1 AS id, 'x' AS v");
         await merge("empty_new", [filled]);
         expect(await merge("empty_new", [empty])).toMatchObject({
            rowsInserted: 0,
            rowCount: 1,
         });
      });
   });

   describe("idempotency", () => {
      for (const [type, expr] of TYPES) {
         it(`an identical second call changes nothing and takes no snapshot (${type})`, async () => {
            const object = await parquet(
               `SELECT i AS id, ${expr} AS v FROM range(1, 4) t(i)`,
            );
            const table = `idem_${type}`;
            const first = await merge(table, [object]);
            expect(first.rowsInserted).toBe(3);
            const before = await snapshotId();
            expect(await merge(table, [object])).toMatchObject({
               rowsInserted: 0,
               rowsReplaced: 0,
               rowsUnchanged: 3,
               rowsDeleted: 0,
               rowCount: 3,
               cursorMax: null,
            });
            expect(await snapshotId()).toBe(before);
         });
      }

      it("adds no data file when nothing changed, and one when a row did", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 100) t(i)",
         );
         await merge("files", [object]);
         const files = await dataFiles("files");
         await merge("files", [object]);
         expect(await dataFiles("files")).toBe(files);
         const changed = await parquet("SELECT 7 AS id, 'edited' AS v");
         await merge("files", [changed]);
         expect(await dataFiles("files")).toBe(files + 1);
      });

      it("flushes rows a catalog's own inlining limit keeps in the catalog database, even when nothing changed", async () => {
         await admin.createCatalog({
            name: "inl",
            dataPath: join(root, "inl"),
            encrypted: false,
            dataInliningRowLimit: 100,
            schemas: ["s"],
         });
         const object = await parquet(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, v)",
            join(root, "inl"),
         );
         const body = { mode: "merge", key: ["id"], objects: [object] };
         await load("flush", body, "inl");
         expect(await inlinedRows("inl", "flush")).toBe(0);
         const inl = await attached("loads_inl", "write", "inl");
         await sql(inl, "INSERT INTO inl.s.flush VALUES (3, 'c')");
         expect(await inlinedRows("inl", "flush")).toBe(1);
         const third = await parquet(
            "SELECT * FROM (VALUES (3, 'c')) t(id, v)",
            join(root, "inl"),
         );
         expect(
            await load(
               "flush",
               { mode: "merge", key: ["id"], objects: [third] },
               "inl",
            ),
         ).toMatchObject({
            rowsUnchanged: 1,
            rowsInserted: 0,
            rowsReplaced: 0,
         });
         expect(await inlinedRows("inl", "flush")).toBe(0);
      });
   });

   describe("the commit message", () => {
      const newest = async () => (await admin.listSnapshots(CATALOG, 1))[0];

      it("is the caller's commitMessage verbatim, listed with the author publisher-admin", async () => {
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         await load("msg_given", {
            mode: "merge",
            key: ["id"],
            objects: [object],
            commitMessage: "nightly 'orders' load",
         });
         expect(await newest()).toMatchObject({
            author: "publisher-admin",
            commitMessage: "nightly 'orders' load",
         });
      });

      it("is `load <schema>.<table>` when the caller gives none", async () => {
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         await merge("msg_default", [object]);
         expect(await newest()).toMatchObject({
            author: "publisher-admin",
            commitMessage: "load s.msg_default",
         });
      });

      it("is recorded by no snapshot when the load changes nothing", async () => {
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         await merge("msg_noop", [object]);
         const before = await newest();
         await load("msg_noop", {
            mode: "merge",
            key: ["id"],
            objects: [object],
            commitMessage: "a retry",
         });
         expect(await newest()).toEqual(before);
      });
   });

   describe("the Load resource", () => {
      const UUID =
         /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
      const body = (
         objects: string[],
         extra: Record<string, unknown> = {},
      ) => ({ mode: "merge", key: ["id"], objects, ...extra });
      /** A load with an optional `Idempotency-Key`, answered as the route sees it: the Load, and whether it was created. Typed as a commit: the specs that read an id assert `created` first, and a no-op's compare its counts alone. */
      const outcome = (
         table: string,
         objects: string[],
         key?: string,
         extra: Record<string, unknown> = {},
         catalog = CATALOG,
      ) =>
         admin.loadTable(
            catalog,
            "s",
            table,
            body(objects, extra),
            key,
         ) as Promise<{
            load: Load;
            created: boolean;
         }>;
      /** An object holding the rows `1..n`, valued `<tag>1..<tag>n`. */
      const rowsObject = (tag: string, n = 3) =>
         parquet(
            `SELECT i AS id, '${tag}' || i AS v FROM range(1, ${n + 1}) t(i)`,
         );
      const snapshotRow = async (id: number) =>
         (
            await superuser(
               `select author, commit_message, commit_extra_info from ${CATALOG}.ducklake_snapshot_changes where snapshot_id = ${id}`,
               DB,
            )
         )[0];

      it("answers a commit created, with the load's id, snapshot and time, and a no-op with its counts alone", async () => {
         const object = await rowsObject("v");
         const first = await outcome("res_basic", [object]);
         expect(first.created).toBe(true);
         expect(first.load).toMatchObject({
            mode: "merge",
            rowsInserted: 3,
            rowsReplaced: 0,
            rowsUnchanged: 0,
            rowsDeleted: 0,
            rowCount: 3,
            cursorMax: null,
         });
         expect(first.load.id).toMatch(UUID);
         const [newest] = await admin.listSnapshots(CATALOG, 1);
         expect(first.load.snapshotId).toBe(newest.id);
         expect(first.load.committedAt).toBe(newest.time);
         const retry = await outcome("res_basic", [object]);
         expect(retry.created).toBe(false);
         expect(retry.load as LoadNoOp).toStrictEqual({
            mode: "merge",
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 3,
            rowsDeleted: 0,
            rowCount: 3,
            cursorMax: null,
         });
      });

      it("answers a load that only adds a column created, though no row changed", async () => {
         await merge("res_column", [await parquet("SELECT 1 AS id, 'a' AS v")]);
         const widened = await outcome("res_column", [
            await parquet("SELECT 1 AS id, 'a' AS v, NULL::VARCHAR AS extra"),
         ]);
         expect(widened.created).toBe(true);
         expect(widened.load).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 1,
         });
         expect(widened.load.id).toMatch(UUID);
         expect(widened.load.snapshotId).toBe(await snapshotId());
      });

      it("records the load in its own snapshot, with the key's digest and never the key", async () => {
         const request = body([await rowsObject("v")], {
            cursor: "id",
            commitMessage: "orders run 7",
         });
         const { load } = (await admin.loadTable(
            CATALOG,
            "s",
            "res_record",
            request,
            "order-run-7",
         )) as { load: Load };
         const row = await snapshotRow(load.snapshotId);
         expect([row.author, row.commit_message]).toEqual([
            "publisher-admin",
            "orders run 7",
         ]);
         const extra = String(row.commit_extra_info);
         expect(extra).not.toContain("order-run-7");
         expect(JSON.parse(extra)).toEqual({
            publisherLoad: 1,
            id: load.id,
            table: "s.res_record",
            keyDigest: createHash("sha256").update("order-run-7").digest("hex"),
            requestDigest: canonicalSha(request),
            mode: "merge",
            result: {
               rowsInserted: 3,
               rowsReplaced: 0,
               rowsUnchanged: 0,
               rowsDeleted: 0,
               rowCount: 3,
               cursorMax: "3",
               cursorType: "BIGINT",
            },
         });
         const unkeyed = await outcome("res_record", [
            await parquet("SELECT 9 AS id, 'z' AS v"),
         ]);
         const unkeyedRow = await snapshotRow(unkeyed.load.snapshotId!);
         expect(
            JSON.parse(String(unkeyedRow.commit_extra_info)),
         ).not.toHaveProperty("keyDigest");
      });

      it("replays a keyed load that committed: created, the same Load, nothing written", async () => {
         const object = await rowsObject("v");
         const first = await outcome("rp_basic", [object], "k1");
         const before = await snapshotId();
         const replay = await outcome("rp_basic", [object], "k1");
         expect(replay).toStrictEqual(first);
         expect(await snapshotId()).toBe(before);
      });

      it("flushes the rows a replayed load left inlined, as the retry of a load that failed after its commit must", async () => {
         await admin.createCatalog({
            name: "idem_inl",
            dataPath: join(root, "idem_inl"),
            encrypted: false,
            dataInliningRowLimit: 100,
            schemas: ["s"],
         });
         const object = await parquet(
            "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, v)",
            join(root, "idem_inl"),
         );
         const first = await outcome("flushed", [object], "k", {}, "idem_inl");
         const inl = await attached("loads_idem_inl", "write", "idem_inl");
         await sql(inl, "INSERT INTO idem_inl.s.flushed VALUES (3, 'c')");
         expect(await inlinedRows("idem_inl", "flushed")).toBe(1);
         expect(
            await outcome("flushed", [object], "k", {}, "idem_inl"),
         ).toEqual(first);
         expect(await inlinedRows("idem_inl", "flushed")).toBe(0);
      });

      it("does not revert a later load when an earlier one is retried", async () => {
         const a = await parquet("SELECT 1 AS id, 'from_a' AS v");
         const b = await parquet("SELECT 1 AS id, 'from_b' AS v");
         const first = await outcome("rp_revert", [a], "key-a");
         await outcome("rp_revert", [b], "key-b");
         expect(await outcome("rp_revert", [a], "key-a")).toStrictEqual(first);
         expect(await rowsOf("rp_revert")).toEqual([{ id: 1, v: "from_b" }]);
      });

      it("still replays after a later load added a column and widened another", async () => {
         const a = await parquet("SELECT 1 AS id, 5::INTEGER AS n");
         const first = await outcome("rp_alter", [a], "k");
         const later = await outcome("rp_alter", [
            await parquet("SELECT 1 AS id, 5::BIGINT AS n, 'x' AS extra"),
         ]);
         expect(later.created).toBe(true);
         expect(
            (await admin.getTable(CATALOG, "s", "rp_alter")).columns.map(
               (c) => `${c.name} ${c.type}`,
            ),
         ).toEqual(["id INTEGER", "n BIGINT", "extra VARCHAR"]);
         expect(await outcome("rp_alter", [a], "k")).toStrictEqual(first);
      });

      it("refuses a key reused with a different body as 409 IDEMPOTENCY_KEY_REUSED, and writes nothing", async () => {
         await outcome("rp_reuse", [await rowsObject("a")], "k");
         const before = await snapshotId();
         const error = await refusal(
            outcome("rp_reuse", [await rowsObject("b")], "k"),
         );
         expect([error.status, error.reason]).toEqual([
            409,
            "IDEMPOTENCY_KEY_REUSED",
         ]);
         expect(await snapshotId()).toBe(before);
      });

      it("scopes a key to its table", async () => {
         const object = await rowsObject("v");
         const first = await outcome("rp_scope_a", [object], "shared");
         const other = await outcome("rp_scope_b", [object], "shared");
         expect(other.created).toBe(true);
         expect(other.load.id).not.toBe(first.load.id);
      });

      it("does not outlive the table it was recorded for: a DELETE or a PUT replace ends it", async () => {
         const a = await rowsObject("a");
         const first = await outcome("rp_reset", [a], "k");
         await admin.deleteTable(CATALOG, "s", "rp_reset");
         const afterDelete = await outcome("rp_reset", [a], "k");
         expect(afterDelete.created).toBe(true);
         expect(afterDelete.load.id).not.toBe(first.load.id);
         expect(afterDelete.load.rowsInserted).toBe(3);
         await admin.putTable(
            CATALOG,
            "s",
            "rp_reset",
            { source: { uri: await rowsObject("b"), format: "parquet" } },
            false,
         );
         const afterReplace = await outcome("rp_reset", [a], "k");
         expect(afterReplace.created).toBe(true);
         expect(afterReplace.load.id).not.toBe(afterDelete.load.id);
         expect(afterReplace.load.rowsReplaced).toBe(3);
      });

      it("stores nothing for a no-op, so its key runs again", async () => {
         const a = await rowsObject("a");
         await merge("rp_noop", [a]);
         expect((await outcome("rp_noop", [a], "k")).created).toBe(false);
         await merge("rp_noop", [await rowsObject("b")]);
         expect((await outcome("rp_noop", [a], "k")).created).toBe(true);
         expect(await rowsOf("rp_noop")).toEqual([
            { id: 1, v: "a1" },
            { id: 2, v: "a2" },
            { id: 3, v: "a3" },
         ]);
      });

      it("keeps a record only as long as its snapshot", async () => {
         await admin.createCatalog({
            name: "idem_exp",
            dataPath: join(root, "idem_exp"),
            encrypted: false,
            schemas: ["s"],
         });
         const [a, b] = [
            await parquet("SELECT 1 AS id, 'a' AS v", join(root, "idem_exp")),
            await parquet("SELECT 1 AS id, 'b' AS v", join(root, "idem_exp")),
         ];
         const first = await outcome("aged", [a], "k", {}, "idem_exp");
         await outcome("aged", [b], undefined, {}, "idem_exp");
         await admin.deleteSnapshots("idem_exp", new Date(Date.now() + 60_000));
         await expect(
            admin.getLoad("idem_exp", "s", "aged", first.load.id!),
         ).rejects.toMatchObject({ reason: "LOAD_NOT_FOUND" });
         const rerun = await outcome("aged", [a], "k", {}, "idem_exp");
         expect(rerun.created).toBe(true);
         expect(rerun.load.id).not.toBe(first.load.id);
      });

      describe("reads", () => {
         it("answers a load by id, with who committed it and the message", async () => {
            const { load } = await outcome("rd_one", [await rowsObject("v")]);
            expect(
               await admin.getLoad(CATALOG, "s", "rd_one", load.id!),
            ).toStrictEqual({
               ...load,
               author: "publisher-admin",
               commitMessage: "load s.rd_one",
            });
         });

         it("answers 404 LOAD_NOT_FOUND for an id it has not, one that is not an id and another table's, and 404 CATALOG_NOT_FOUND for a catalog it has not", async () => {
            const mine = await outcome("rd_miss", [await rowsObject("v")]);
            const theirs = await outcome("rd_other", [await rowsObject("v")]);
            for (const id of [
               "00000000-0000-4000-8000-000000000000",
               "not-an-id",
               theirs.load.id!,
            ]) {
               const error = await refusal(
                  admin.getLoad(CATALOG, "s", "rd_miss", id),
               );
               expect([id, error.status, error.reason]).toEqual([
                  id,
                  404,
                  "LOAD_NOT_FOUND",
               ]);
            }
            const missing = await refusal(
               admin.getLoad("no_such_catalog", "s", "rd_miss", mine.load.id!),
            );
            expect([missing.status, missing.reason]).toEqual([
               404,
               "CATALOG_NOT_FOUND",
            ]);
         });

         it("lists a table's loads newest first in pages, without no-ops or other tables' loads, across a drop and recreate", async () => {
            const ids: string[] = [];
            for (const tag of ["a", "b", "c", "d", "e"]) {
               const { load } = await outcome("rd_list", [
                  await parquet(`SELECT 1 AS id, '${tag}' AS v`),
               ]);
               ids.push(load.id!);
            }
            await outcome("rd_list", [
               await parquet("SELECT 1 AS id, 'e' AS v"),
            ]);
            await outcome("rd_list_other", [await rowsObject("v")]);
            const first = await admin.listLoads(CATALOG, "s", "rd_list", 2);
            expect(first.items.map((l) => l.id)).toEqual([ids[4], ids[3]]);
            expect(first.items[0]).toMatchObject({
               author: "publisher-admin",
               commitMessage: "load s.rd_list",
               rowsReplaced: 1,
            });
            const second = await admin.listLoads(
               CATALOG,
               "s",
               "rd_list",
               2,
               first.nextCursor,
            );
            expect(second.items.map((l) => l.id)).toEqual([ids[2], ids[1]]);
            const third = await admin.listLoads(
               CATALOG,
               "s",
               "rd_list",
               2,
               second.nextCursor,
            );
            expect(third.items.map((l) => l.id)).toEqual([ids[0]]);
            expect(third).not.toHaveProperty("nextCursor");
            await admin.deleteTable(CATALOG, "s", "rd_list");
            const recreated = await outcome("rd_list", [await rowsObject("v")]);
            expect(
               (await admin.listLoads(CATALOG, "s", "rd_list", 100)).items.map(
                  (l) => l.id,
               ),
            ).toEqual([recreated.load.id, ...ids.slice().reverse()]);
         });
      });

      describe("the reads it makes of the catalog database", () => {
         /** How many queries `run` makes on the table lock's own connection. */
         async function lockReads(run: () => Promise<unknown>) {
            const lock = spyOn(admin, "withTableLock");
            let reads = 0;
            lock.mockImplementation(
               (catalog, schema, table, fn) =>
                  CatalogAdmin.prototype.withTableLock.call(
                     admin,
                     catalog,
                     schema,
                     table,
                     (held) =>
                        fn({
                           ...held,
                           pg: async (query) => {
                              reads++;
                              return held.pg(query);
                           },
                           pgAfterCommit: async (query) => {
                              reads++;
                              return held.pgAfterCommit(query);
                           },
                        }),
                  ) as ReturnType<typeof fn>,
            );
            try {
               await run();
               return reads;
            } finally {
               lock.mockRestore();
            }
         }

         it("adds one read to a load that commits and two to a keyed one, one to a keyed no-op or replay, and none to a no-op", async () => {
            const a = await rowsObject("a");
            const b = await rowsObject("b");
            await merge("rd_count", [a]);
            const noop = await lockReads(() => outcome("rd_count", [a]));
            expect(noop).toBe(3);
            expect(await lockReads(() => outcome("rd_count", [b]))).toBe(
               noop + 1,
            );
            expect(await lockReads(() => outcome("rd_count", [a], "k"))).toBe(
               noop + 2,
            );
            expect(await lockReads(() => outcome("rd_count", [a], "k"))).toBe(
               noop + 1,
            );
            expect(await lockReads(() => outcome("rd_count", [a], "j"))).toBe(
               noop + 1,
            );
            expect(
               await lockReads(() => refusal(outcome("rd_count", [b], "k"))),
            ).toBe(noop);
         });

         it("makes one read for a load's record on a GET, beside the catalog's existence check", async () => {
            const { load } = await outcome("rd_get", [await rowsObject("v")]);
            const reads: (() => Promise<unknown>)[] = [
               () => admin.getLoad(CATALOG, "s", "rd_get", load.id!),
               () => admin.listLoads(CATALOG, "s", "rd_get", 10),
            ];
            for (const read of reads) {
               const { seen } = await recorded(read);
               const queries = seen.filter((s) =>
                  s.includes("postgres_query('admin_pg'"),
               );
               expect(queries).toHaveLength(2);
               expect(
                  queries.filter((s) =>
                     s.includes("ducklake_snapshot_changes"),
                  ),
               ).toHaveLength(1);
            }
         });
      });
   });

   describe("replace", () => {
      async function seeded(table: string): Promise<void> {
         await merge(table, [
            await parquet(
               "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
            ),
         ]);
      }

      it("deletes the keys the objects lack, and counts them", async () => {
         await seeded("rep_keyed");
         const object = await parquet(
            "SELECT * FROM (VALUES (2, 'v2'), (3, 'v3'), (6, 'v6')) t(id, v)",
         );
         expect(
            await load("rep_keyed", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 3,
               objects: [object],
            }),
         ).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 0,
            rowsUnchanged: 2,
            rowsDeleted: 3,
            rowCount: 3,
            cursorMax: null,
         });
         expect(await rowsOf("rep_keyed")).toEqual([
            { id: 2, v: "v2" },
            { id: 3, v: "v3" },
            { id: 6, v: "v6" },
         ]);
      });

      it("refuses a delete past maxDeletedRows and writes nothing", async () => {
         await seeded("rep_guard");
         const object = await parquet("SELECT 2 AS id, 'v2' AS v");
         const before = await snapshotId();
         const error = await refusal(
            load("rep_guard", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 3,
               objects: [object],
            }),
         );
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_DELETE_LIMIT_EXCEEDED",
         ]);
         expect(error.detail).toEqual({
            wouldDelete: 4,
            rowsBefore: 5,
            maxDeletedRows: 3,
         });
         expect(await snapshotId()).toBe(before);
         expect(await rowsOf("rep_guard")).toHaveLength(5);
      });

      it("deletes the stored rows of a NULL key when the objects lack them", async () => {
         await seeded("rep_null");
         await sql(
            writer,
            `INSERT INTO ${CATALOG}.s.rep_null VALUES (NULL, 'orphan'), (NULL, 'orphan2')`,
         );
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         const result = await load("rep_null", {
            mode: "replace",
            key: ["id"],
            maxDeletedRows: 2,
            objects: [object],
         });
         expect([result.rowsDeleted, result.rowCount]).toEqual([2, 5]);
      });

      it("keyless: writes nothing when the table already equals the objects", async () => {
         await seeded("rep_keyless");
         const same = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
         );
         const before = await snapshotId();
         expect(
            await load("rep_keyless", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [same],
            }),
         ).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 5,
            rowsDeleted: 0,
            rowCount: 5,
            cursorMax: null,
         });
         expect(await snapshotId()).toBe(before);
      });

      it("keyless: counts the rows that differ as deleted and inserted, and guards the deleted", async () => {
         await seeded("rep_keyless2");
         const keyless = async (select: string, maxDeletedRows: number) =>
            load("rep_keyless2", {
               mode: "replace",
               maxDeletedRows,
               objects: [await parquet(select)],
            });
         const edited =
            "SELECT i AS id, CASE WHEN i = 3 THEN 'edited' ELSE 'v' || i END AS v FROM range(1, 6) t(i)";
         // An edited row is one deleted and one inserted, so the guard sees it.
         const error = await refusal(keyless(edited, 0));
         expect([error.reason, error.detail]).toEqual([
            "LOAD_DELETE_LIMIT_EXCEEDED",
            { wouldDelete: 1, rowsBefore: 5, maxDeletedRows: 0 },
         ]);
         expect(await rowsOf("rep_keyless2")).toHaveLength(5);
         expect(await keyless(edited, 1)).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 0,
            rowsUnchanged: 4,
            rowsDeleted: 1,
            rowCount: 5,
         });
         expect(
            await keyless(
               "SELECT i AS id, 'v' || i AS v FROM range(1, 8) t(i)",
               1,
            ),
         ).toMatchObject({
            rowsInserted: 3,
            rowsReplaced: 0,
            rowsUnchanged: 4,
            rowsDeleted: 1,
            rowCount: 7,
         });
         const smaller = "SELECT 1 AS id, 'v1' AS v";
         expect((await refusal(keyless(smaller, 5))).detail).toEqual({
            wouldDelete: 6,
            rowsBefore: 7,
            maxDeletedRows: 5,
         });
         expect(await keyless(smaller, 6)).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 1,
            rowsDeleted: 6,
            rowCount: 1,
         });
         expect(await rowsOf("rep_keyless2")).toEqual([{ id: 1, v: "v1" }]);
      });

      it("keyless: compares rows as a multiset, so a repeated row counts once per copy", async () => {
         const rowsOfV = async () =>
            (
               await sql(
                  reader,
                  `SELECT v FROM ${CATALOG}.s.rep_bag ORDER BY v`,
               )
            ).map((r) => r.v);
         const keyless = async (values: string) =>
            load("rep_bag", {
               mode: "replace",
               maxDeletedRows: 10,
               objects: [
                  await parquet(`SELECT * FROM (VALUES ${values}) t(v)`),
               ],
            });
         await keyless("('a'), ('a'), ('b')");
         expect(await keyless("('a'), ('b'), ('b')")).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 0,
            rowsUnchanged: 2,
            rowsDeleted: 1,
            rowCount: 3,
         });
         expect(await rowsOfV()).toEqual(["a", "b", "b"]);
      });

      it("keyless: an object with no rows cannot empty the table past the guard", async () => {
         await seeded("rep_empty");
         const empty = await parquet("SELECT 1 AS id, 'x' AS v WHERE false");
         const error = await refusal(
            load("rep_empty", {
               mode: "replace",
               maxDeletedRows: 0,
               objects: [empty],
            }),
         );
         expect(error.reason).toBe("LOAD_DELETE_LIMIT_EXCEEDED");
         expect(await rowsOf("rep_empty")).toHaveLength(5);
      });
   });

   describe("the schema contract", () => {
      async function seed(table: string, select: string): Promise<void> {
         await merge(table, [await parquet(select)]);
      }

      async function contract(
         promise: Promise<unknown>,
      ): Promise<Record<string, unknown>> {
         const error = await refusal(promise);
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_COLUMN_MISMATCH",
         ]);
         return error.detail!;
      }

      it("adds a column the table lacks, and writes NULL for one the objects lack", async () => {
         await seed("c_add", "SELECT 1 AS id, 'a' AS v, 10 AS n");
         const wider = await parquet(
            "SELECT * FROM (VALUES (1, 'a', 10, 'new'), (2, 'b', 20, 'x')) t(id, v, n, w)",
         );
         expect(await merge("c_add", [wider])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
         });
         expect(
            await rowsOf("c_add", "id::INTEGER AS id, v, n::INTEGER AS n, w"),
         ).toEqual([
            { id: 1, v: "a", n: 10, w: "new" },
            { id: 2, v: "b", n: 20, w: "x" },
         ]);
         const narrower = await parquet("SELECT 2 AS id, 'b' AS v");
         expect(await merge("c_add", [narrower])).toMatchObject({
            rowsReplaced: 1,
         });
         expect(
            await rowsOf("c_add", "id::INTEGER AS id, n::INTEGER AS n, w"),
         ).toEqual([
            { id: 1, n: 10, w: "new" },
            { id: 2, n: null, w: null },
         ]);
      });

      it("accepts a narrower type into the table's, keeping the table's type", async () => {
         await seed(
            "c_keep",
            "SELECT 1 AS id, 5::BIGINT AS big, 1.5::DOUBLE AS d, 1.50::DECIMAL(7,2) AS dec, 5::DOUBLE AS x",
         );
         const narrow = await parquet(
            "SELECT 1 AS id, 5::INTEGER AS big, 1.5::FLOAT AS d, 1.50::DECIMAL(3,2) AS dec, 5::INTEGER AS x",
         );
         expect(await merge("c_keep", [narrow])).toMatchObject({
            rowsUnchanged: 1,
            rowsReplaced: 0,
         });
         expect(
            (await admin.getTable(CATALOG, "s", "c_keep")).columns.map(
               (c) => c.type,
            ),
         ).toEqual(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL(7,2)", "DOUBLE"]);
      });

      it("widens the table's column to the objects' wider type in one snapshot, rewriting no file, and a retry changes nothing", async () => {
         await seed(
            "c_widen",
            "SELECT i AS id, i::INTEGER AS n, i::FLOAT / 2 AS f FROM range(1, 4) t(i)",
         );
         const filesBefore = await dataFileIds("c_widen");
         const before = await snapshotId();
         const wider = await parquet(
            "SELECT * FROM (VALUES (1, 1::BIGINT, 0.5::DOUBLE), (4, 9007199254740993::BIGINT, 1.1::DOUBLE)) t(id, n, f)",
         );
         expect(await merge("c_widen", [wider])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 0,
            rowsUnchanged: 1,
         });
         expect(await snapshotId()).toBe(before + 1);
         const filesAfter = await dataFileIds("c_widen");
         expect(filesAfter).toEqual(expect.arrayContaining(filesBefore));
         expect(filesAfter).toHaveLength(filesBefore.length + 1);
         expect(
            (await admin.getTable(CATALOG, "s", "c_widen")).columns.map((c) => [
               c.name,
               c.type,
            ]),
         ).toEqual([
            ["id", "BIGINT"],
            ["n", "BIGINT"],
            ["f", "DOUBLE"],
         ]);
         expect(
            await rowsOf("c_widen", "id::INTEGER AS id, n::VARCHAR AS n, f"),
         ).toEqual([
            { id: 1, n: "1", f: 0.5 },
            { id: 2, n: "2", f: 1 },
            { id: 3, n: "3", f: 1.5 },
            { id: 4, n: "9007199254740993", f: 1.1 },
         ]);
         const settled = await snapshotId();
         expect(await merge("c_widen", [wider])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 2,
         });
         expect(await snapshotId()).toBe(settled);
      });

      it("refuses a type the table's neither holds nor widens into without loss, and writes nothing", async () => {
         await seed(
            "c_narrow",
            "SELECT 1 AS id, 5::INTEGER AS n, 5::DOUBLE AS x",
         );
         const before = await snapshotId();
         expect(
            await contract(
               merge("c_narrow", [
                  await parquet("SELECT 1 AS id, 'x' AS n, 5::DOUBLE AS x"),
               ]),
            ),
         ).toEqual({
            column: "n",
            tableType: "INTEGER",
            loadType: "VARCHAR",
            violation: "type_change",
         });
         // BIGINT into DOUBLE rounds past 2^53, and DOUBLE into BIGINT drops fractions.
         expect(
            await contract(
               merge("c_narrow", [
                  await parquet(
                     "SELECT 1 AS id, 5::INTEGER AS n, 5::BIGINT AS x",
                  ),
               ]),
            ),
         ).toMatchObject({ column: "x", violation: "type_change" });
         expect(
            await contract(
               merge("c_narrow", [
                  await parquet(
                     "SELECT 1 AS id, 5::INTEGER AS n, 1.50::DECIMAL(10,2) AS x",
                  ),
               ]),
            ),
         ).toMatchObject({ column: "x", violation: "type_change" });
         expect(await snapshotId()).toBe(before);
      });

      it("refuses a table that has a column named like the load's own helper", async () => {
         await sql(
            writer,
            `CREATE TABLE ${CATALOG}.s.c_shadow (id INTEGER, v VARCHAR, __publisher_present INTEGER)`,
         );
         expect(
            await contract(
               merge("c_shadow", [await parquet("SELECT 1 AS id, 'a' AS v")]),
            ),
         ).toMatchObject({
            column: "__publisher_present",
            violation: "reserved_name",
         });
      });

      it("refuses a column the objects type differently when they do not widen into one, and takes one that does", async () => {
         const mixed = async (a: string, b: string) => [
            await parquet(`SELECT 1 AS id, 1::${a} AS k`),
            await parquet(`SELECT 2 AS id, 2::${b} AS k`),
         ];
         // Read together BIGINT and DOUBLE would be DOUBLE: past 2^53 distinct keys collapse.
         expect(
            await contract(merge("c_mixed", await mixed("BIGINT", "DOUBLE"))),
         ).toEqual({
            column: "k",
            tableType: null,
            loadType: "BIGINT",
            violation: "type_change",
         });
         expect(
            await refusal(
               merge("c_mixed", await mixed("INTEGER", "VARCHAR")),
            ).then((error) => error.detail),
         ).toMatchObject({ column: "k", violation: "type_change" });
         expect(
            (await admin.listTables(CATALOG, "s")).map((t) => t.name),
         ).not.toContain("c_mixed");
         expect(
            await merge("c_widened", await mixed("INTEGER", "BIGINT")),
         ).toMatchObject({ rowsInserted: 2 });
         expect(
            (await admin.getTable(CATALOG, "s", "c_widened")).columns[1].type,
         ).toBe("BIGINT");
      });

      it("refuses a key the objects lack, and a NULL key", async () => {
         await seed("c_key", "SELECT 1 AS id, 'a' AS v");
         expect(
            await contract(merge("c_key", [await parquet("SELECT 'a' AS v")])),
         ).toMatchObject({ column: "id", violation: "missing_key" });
         expect(
            await contract(
               merge("c_key", [
                  await parquet("SELECT NULL::INTEGER AS id, 'a' AS v"),
               ]),
            ),
         ).toMatchObject({ violation: "null_key" });
         expect(await rowsOf("c_key")).toEqual([{ id: 1, v: "a" }]);
      });

      it("refuses names that differ only by case in two objects", async () => {
         expect(
            await contract(
               merge("c_case", [
                  await parquet("SELECT 1 AS id, 'x' AS v"),
                  await parquet("SELECT 2 AS id, 'y' AS V"),
               ]),
            ),
         ).toMatchObject({ violation: "case_collision" });
      });

      it("refuses a column name the load reserves", async () => {
         for (const name of ["__publisher_x", "file_row_number", "rowid"]) {
            expect(
               await contract(
                  merge("c_reserved", [
                     await parquet(`SELECT 1 AS id, 1 AS ${name}`),
                  ]),
               ),
            ).toMatchObject({ column: name, violation: "reserved_name" });
         }
      });

      it("never splices a hostile struct field name into SQL", async () => {
         await seed("c_hostile", "SELECT 1 AS id, 'a' AS v");
         const hostile = `x"), "evil`;
         const object = await parquet(
            `SELECT 1 AS id, 'a' AS v, {'${hostile}': 1, 'it''s': 2} AS st`,
         );
         expect(await merge("c_hostile", [object])).toMatchObject({
            rowsReplaced: 1,
         });
         const table = await admin.getTable(CATALOG, "s", "c_hostile");
         expect(table.columns.map((c) => c.name)).toEqual(["id", "v", "st"]);
         expect(table.columns[2].type).toContain(`"x""), ""evil"`);
         expect(await merge("c_hostile", [object])).toMatchObject({
            rowsUnchanged: 1,
         });
      });

      it("refuses a cursor that is missing or not an integer, date or timestamp", async () => {
         await seed("c_cursor", "SELECT 1 AS id, 'a' AS v");
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         expect(
            await contract(merge("c_cursor", [object], { cursor: "v" })),
         ).toEqual({
            column: "v",
            tableType: "VARCHAR",
            loadType: "VARCHAR",
            violation: "cursor_type",
         });
         expect(
            await contract(merge("c_cursor", [object], { cursor: "nope" })),
         ).toMatchObject({ violation: "missing_cursor" });
      });
   });

   describe("DEFAULT and NOT NULL columns", () => {
      const create = (table: string, columns: string) =>
         sql(writer, `CREATE TABLE ${CATALOG}.s.${table} (${columns})`);
      const contract = async (promise: Promise<unknown>) => {
         const error = await refusal(promise);
         expect([error.status, error.reason]).toEqual([
            422,
            "LOAD_COLUMN_MISMATCH",
         ]);
         return error.detail!;
      };

      it("writes NULL, not the DEFAULT, into a column the objects lack, so a retry changes nothing", async () => {
         await create(
            "df_merge",
            "id INTEGER, v VARCHAR, extra INTEGER DEFAULT 7",
         );
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         expect(await merge("df_merge", [object])).toMatchObject({
            rowsInserted: 1,
         });
         expect(
            await rowsOf("df_merge", "id::INTEGER AS id, v, extra"),
         ).toEqual([{ id: 1, v: "a", extra: null }]);
         expect(await merge("df_merge", [object])).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 1,
         });
         await create("df_keyless", "v VARCHAR, extra INTEGER DEFAULT 7");
         const first = await load("df_keyless", {
            mode: "replace",
            maxDeletedRows: 10,
            objects: [await parquet("SELECT 'a' AS v")],
         });
         expect(first).toMatchObject({ rowsInserted: 1 });
         expect(
            await sql(reader, `SELECT extra FROM ${CATALOG}.s.df_keyless`),
         ).toEqual([{ extra: null }]);
         expect(
            await load("df_keyless", {
               mode: "replace",
               maxDeletedRows: 10,
               objects: [await parquet("SELECT 'a' AS v")],
            }),
         ).toMatchObject({ rowsUnchanged: 1, rowsInserted: 0, rowsDeleted: 0 });
      });

      it("refuses a NOT NULL column the objects lack, and writes nothing", async () => {
         await create(
            "nn_missing",
            "id INTEGER, v VARCHAR, req VARCHAR NOT NULL",
         );
         await sql(
            writer,
            `INSERT INTO ${CATALOG}.s.nn_missing VALUES (1, 'a', 'r')`,
         );
         const before = await snapshotId();
         expect(
            await contract(
               merge("nn_missing", [await parquet("SELECT 2 AS id, 'b' AS v")]),
            ),
         ).toEqual({
            column: "req",
            tableType: "VARCHAR",
            loadType: null,
            violation: "not_null",
         });
         expect(await snapshotId()).toBe(before);
      });

      it("refuses a NULL in a NOT NULL column the objects carry, unless a later row of its key replaces it", async () => {
         await create("nn_value", "id INTEGER, req VARCHAR NOT NULL");
         const before = await snapshotId();
         expect(
            await contract(
               merge("nn_value", [
                  await parquet(
                     "SELECT * FROM (VALUES (1, 'a'), (2, NULL::VARCHAR)) t(id, req)",
                  ),
               ]),
            ),
         ).toMatchObject({
            column: "req",
            tableType: "VARCHAR",
            violation: "not_null",
         });
         expect(await snapshotId()).toBe(before);
         expect(
            await merge("nn_value", [
               await parquet(
                  "SELECT * FROM (VALUES (1, NULL::VARCHAR), (1, 'later'), (2, 'b')) t(id, req)",
               ),
            ]),
         ).toMatchObject({ rowsInserted: 2 });
      });
   });

   describe("a column its writer typed NULL", () => {
      // fixtures/*_null_column.parquet: pyarrow 26.0.1, pq.write_table(pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"], "junk": pa.nulls(3)}))
      // and, for only_null_column, pa.table({"junk": pa.nulls(3)}). DuckDB reads `junk` as INTEGER; parquet_schema calls it NullType().
      const columnsOf = async (table: string) =>
         (await admin.getTable(CATALOG, "s", table)).columns.map(
            (c) => `${c.name} ${c.type}`,
         );

      it("is ignored against a table that has the column typed otherwise", async () => {
         await merge("nulls_existing", [
            await parquet(
               "SELECT * FROM (VALUES (1::BIGINT, 'a', NULL::VARCHAR), (2::BIGINT, 'b', NULL::VARCHAR), (3::BIGINT, 'c', NULL::VARCHAR)) t(id, v, junk)",
            ),
         ]);
         expect(
            await merge("nulls_existing", [fixture("all_null_column.parquet")]),
         ).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 3,
         });
         expect(await columnsOf("nulls_existing")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk VARCHAR",
         ]);
      });

      it("is not created as INTEGER on a first load, and a later batch with values for it adds it", async () => {
         expect(
            await merge("nulls_first", [fixture("all_null_column.parquet")]),
         ).toMatchObject({ rowsInserted: 3, rowCount: 3 });
         expect(await columnsOf("nulls_first")).toEqual([
            "id BIGINT",
            "v VARCHAR",
         ]);
         const later = await parquet(
            "SELECT * FROM (VALUES (1::BIGINT, 'a', 'real'), (4::BIGINT, 'd', 'x')) t(id, v, junk)",
         );
         expect(await merge("nulls_first", [later])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
            rowsUnchanged: 0,
         });
         expect(await columnsOf("nulls_first")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk VARCHAR",
         ]);
         expect(
            await rowsOf("nulls_first", "id::INTEGER AS id, junk AS v"),
         ).toEqual([
            { id: 1, v: "real" },
            { id: 2, v: null },
            { id: 3, v: null },
            { id: 4, v: "x" },
         ]);
      });

      it("takes the type of another object that has values for it", async () => {
         const typed = await parquet(
            "SELECT * FROM (VALUES (4::BIGINT, 'd', 7::BIGINT)) t(id, v, junk)",
         );
         expect(
            await merge("nulls_mixed", [
               fixture("all_null_column.parquet"),
               typed,
            ]),
         ).toMatchObject({ rowsInserted: 4 });
         expect(await columnsOf("nulls_mixed")).toEqual([
            "id BIGINT",
            "v VARCHAR",
            "junk BIGINT",
         ]);
      });

      it("reads a cursor typed NULL as no value: on a first load it has no type, and on a table it has the column's", async () => {
         const first = await merge(
            "nulls_cursor",
            [fixture("all_null_column.parquet")],
            { cursor: "junk" },
         );
         expect(first).toMatchObject({ rowsInserted: 3, cursorMax: null });
         expect(first).not.toHaveProperty("cursorType");
         await merge("nulls_cursor_typed", [
            await parquet(
               "SELECT * FROM (VALUES (1::BIGINT, 'a', 5::BIGINT), (2::BIGINT, 'b', 9::BIGINT)) t(id, v, junk)",
            ),
         ]);
         expect(
            await merge(
               "nulls_cursor_typed",
               [fixture("all_null_column.parquet")],
               { cursor: "junk" },
            ),
         ).toMatchObject({ cursorMax: null, cursorType: "BIGINT" });
         const error = await refusal(
            merge("nulls_existing", [fixture("all_null_column.parquet")], {
               cursor: "junk",
            }),
         );
         expect(error.detail).toMatchObject({
            column: "junk",
            tableType: "VARCHAR",
            violation: "cursor_type",
         });
      });

      it("answers a key typed NULL as a NULL key", async () => {
         for (const table of ["nulls_key_new", "nulls_existing"]) {
            const error = await refusal(
               merge(table, [fixture("all_null_column.parquet")], {
                  key: ["junk"],
               }),
            );
            expect([error.reason, error.detail]).toMatchObject([
               "LOAD_COLUMN_MISMATCH",
               { violation: "null_key" },
            ]);
         }
      });

      it("refuses objects whose every column is typed NULL, as having no column", async () => {
         const error = await refusal(
            merge("nulls_only", [fixture("only_null_column.parquet")]),
         );
         expect([error.status, error.reason, error.detail]).toEqual([
            422,
            "LOAD_COLUMN_MISMATCH",
            {
               column: "",
               tableType: null,
               loadType: null,
               violation: "no_columns",
            },
         ]);
      });

      it("adds a typed column whose every value is NULL, though no row changed", async () => {
         await merge("nulls_added", [
            await parquet("SELECT 1 AS id, 'a' AS v"),
         ]);
         const before = await snapshotId();
         expect(
            await merge("nulls_added", [
               await parquet(
                  "SELECT 1 AS id, 'a' AS v, NULL::VARCHAR AS extra",
               ),
            ]),
         ).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 1,
         });
         expect(await columnsOf("nulls_added")).toEqual([
            "id INTEGER",
            "v VARCHAR",
            "extra VARCHAR",
         ]);
         expect(await snapshotId()).toBeGreaterThan(before);
         const settled = await snapshotId();
         await merge("nulls_added", [
            await parquet("SELECT 1 AS id, 'a' AS v, NULL::VARCHAR AS extra"),
         ]);
         expect(await snapshotId()).toBe(settled);
      });
   });

   describe("the cursor", () => {
      it.each([
         [
            "BIGINT beyond 2^53",
            "9007199254740993::BIGINT + i",
            "9007199254740996",
            "BIGINT",
         ],
         [
            "UBIGINT beyond 2^63",
            "18446744073709551612::UBIGINT + i::UBIGINT",
            "18446744073709551615",
            "UBIGINT",
         ],
         ["DATE", "DATE '2026-01-01' + i::INTEGER", "2026-01-04", "DATE"],
         [
            "TIMESTAMP",
            "TIMESTAMP '2026-01-01 00:00:00.5' + to_hours(i)",
            "2026-01-01T03:00:00.500000",
            "TIMESTAMP",
         ],
         [
            "TIMESTAMPTZ",
            "TIMESTAMPTZ '2026-01-01 00:00:00+02' + to_hours(i)",
            "2026-01-01T01:00:00.000000Z",
            "TIMESTAMP WITH TIME ZONE",
         ],
         ["only NULL", "NULL::BIGINT", null, "BIGINT"],
      ])("reports the greatest %s as text", async (name, expr, max, type) => {
         const object = await parquet(
            `SELECT i AS id, ${expr} AS c FROM range(1, 4) t(i)`,
         );
         const table = `cur_${name.replace(/\W+/g, "_")}`;
         const result = await merge(table, [object], { cursor: "c" });
         expect(result.cursorMax).toBe(max);
         expect(result.cursorType).toBe(type);
         // The same on a call that changes nothing.
         expect(await merge(table, [object], { cursor: "c" })).toMatchObject({
            cursorMax: max,
            rowsUnchanged: 3,
         });
      });

      it("takes the greatest over the rows a load keeps, not the ones a later row replaces", async () => {
         const object = await parquet(
            "SELECT * FROM (VALUES (1, 100), (1, 5), (2, 7)) t(id, c)",
         );
         expect(
            (await merge("cur_dedupe", [object], { cursor: "c" })).cursorMax,
         ).toBe("7");
      });
   });

   describe("objects", () => {
      it("refuses a missing object as unreadable, naming no path", async () => {
         const good = await parquet("SELECT 1 AS id, 'x' AS v");
         const missing = join(dataPath, "staging", "missing.parquet");
         const error = await refusal(merge("unread", [good, missing]));
         expect([error.status, error.reason]).toEqual([
            422,
            "OBJECT_UNREADABLE",
         ]);
         expect(error.message).not.toContain("missing.parquet");
         expect(error.message).not.toContain(root);
         expect(error.detail).toEqual({ object: 1 });
      });

      it("refuses a file that is not Parquet as unreadable", async () => {
         const file = join(dataPath, "staging", "not_parquet.parquet");
         mkdirSync(dirname(file), { recursive: true });
         writeFileSync(file, "this is not parquet");
         const error = await refusal(merge("unread2", [file]));
         expect([error.status, error.reason]).toEqual([
            422,
            "OBJECT_UNREADABLE",
         ]);
         expect(error.message).not.toContain(root);
      });

      it("reads a column from the object, not from a k=v directory above it", async () => {
         const dir = `hive_${counter++}`;
         const objects = [
            await parquet(
               "SELECT 1 AS id, 5::INTEGER AS x",
               dataPath,
               `${dir}/x=1/a`,
            ),
            await parquet(
               "SELECT 2 AS id, 6::INTEGER AS x",
               dataPath,
               `${dir}/x=2/b`,
            ),
         ];
         expect(await merge("hive", objects)).toMatchObject({
            rowsInserted: 2,
         });
         expect(await rowsOf("hive", "id::INTEGER AS id, x")).toEqual([
            { id: 1, x: 5 },
            { id: 2, x: 6 },
         ]);
         expect(
            (await admin.getTable(CATALOG, "s", "hive")).columns.find(
               (c) => c.name === "x",
            )?.type,
         ).toBe("INTEGER");
         // Objects that type the column differently are described one by one, too.
         const mixed = [
            await parquet(
               "SELECT 3 AS id, 7::SMALLINT AS x",
               dataPath,
               `${dir}/x=3/c`,
            ),
            await parquet(
               "SELECT 4 AS id, 8::INTEGER AS x",
               dataPath,
               `${dir}/x=4/d`,
            ),
         ];
         expect(await merge("hive", mixed)).toMatchObject({ rowsInserted: 2 });
         expect(await rowsOf("hive", "id::INTEGER AS id, x")).toEqual([
            { id: 1, x: 5 },
            { id: 2, x: 6 },
            { id: 3, x: 7 },
            { id: 4, x: 8 },
         ]);
      });

      it("refuses an object outside the data path", async () => {
         const outside = join(root, "elsewhere.parquet");
         const error = await refusal(merge("outside", [outside]));
         expect([error.status, error.reason]).toEqual([
            400,
            "OBJECT_OUTSIDE_DATA_PATH",
         ]);
      });

      it("answers 404 for a catalog or a schema that does not exist", async () => {
         const object = await parquet("SELECT 1 AS id");
         expect(
            (
               await refusal(
                  admin.loadTable("nowhere", "s", "t", {
                     mode: "merge",
                     key: ["id"],
                     objects: [object],
                  }),
               )
            ).reason,
         ).toBe("CATALOG_NOT_FOUND");
         expect(
            (
               await refusal(
                  admin.loadTable(CATALOG, "nowhere", "t", {
                     mode: "merge",
                     key: ["id"],
                     objects: [object],
                  }),
               )
            ).reason,
         ).toBe("SCHEMA_NOT_FOUND");
      });
   });

   describe("admission", () => {
      /** Runs `run` with `hook` called while its load holds the table's lock, after it planned and right before it confirms the lock and commits. */
      async function beforeCommit<T>(
         hook: () => Promise<void>,
         run: () => Promise<T>,
      ): Promise<T> {
         const lock = spyOn(admin, "withTableLock");
         lock.mockImplementation(
            (catalog, schema, table, fn) =>
               CatalogAdmin.prototype.withTableLock.call(
                  admin,
                  catalog,
                  schema,
                  table,
                  (held) =>
                     fn({
                        ...held,
                        assertLockHeld: async () => {
                           await hook();
                           await held.assertLockHeld();
                        },
                     }),
               ) as ReturnType<typeof fn>,
         );
         try {
            return await run();
         } finally {
            lock.mockRestore();
         }
      }

      it("refuses an overlapping load of the same table from another replica, and loads it once the first finishes", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         const other = new CatalogAdmin({ catalogDatabase: conninfo });
         let busy: CatalogAdminError | undefined;
         const first = await beforeCommit(
            async () => {
               // Another replica has a load slot of its own: only the table's lock refuses it.
               const slot = spyOn(loadSlots, "tryAcquire").mockReturnValue({
                  release: () => undefined,
               });
               try {
                  busy = await refusal(
                     other.loadTable(CATALOG, "s", "overlap", {
                        mode: "merge",
                        key: ["id"],
                        objects: [object],
                     }),
                  );
               } finally {
                  slot.mockRestore();
               }
            },
            () => merge("overlap", [object]),
         );
         expect([busy?.status, busy?.reason]).toEqual([503, "LOAD_BUSY"]);
         expect(busy?.message).toContain("is already running");
         expect(first.rowsInserted).toBe(3);
         expect(
            (
               await other.loadTable(CATALOG, "s", "overlap", {
                  mode: "merge",
                  key: ["id"],
                  objects: [object],
               })
            ).load,
         ).toMatchObject({ rowsUnchanged: 3, rowsInserted: 0 });
      });

      it("rolls back and answers 503 when the lock's connection dies before the commit", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         const before = await snapshotId();
         const error = await refusal(
            beforeCommit(
               async () => {
                  const [row] = await superuser(
                     "select pid from pg_locks where locktype = 'advisory' and granted and database = (select oid from pg_database where datname = current_database())",
                     DB,
                  );
                  await superuser(
                     `select pg_terminate_backend(${Number(row.pid)})`,
                  );
               },
               () => merge("killed", [object]),
            ),
         );
         expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         expect(await snapshotId()).toBe(before);
         expect(await admin.listTables(CATALOG, "s")).not.toContainEqual(
            expect.objectContaining({ name: "killed" }),
         );
         // The lock went with its connection: the retry loads.
         expect((await merge("killed", [object])).rowsInserted).toBe(3);
      });

      it("answers a plain error, not a 503 LOAD_BUSY or a lost-lock count, when the lock's connection dies after the commit", async () => {
         const harness = await startMetricsHarness();
         resetInstrumentsForTesting();
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         const lock = spyOn(admin, "withTableLock");
         lock.mockImplementation(
            (catalog, schema, table, fn) =>
               CatalogAdmin.prototype.withTableLock.call(
                  admin,
                  catalog,
                  schema,
                  table,
                  (held) => {
                     // Kills the connection right before the load's record is read back.
                     const killing =
                        (read: typeof held.pg): typeof held.pg =>
                        async (query) => {
                           if (query.includes("ducklake_snapshot_changes")) {
                              const [{ pid }] = await held.pg(
                                 "SELECT pg_backend_pid() AS pid",
                              );
                              await superuser(
                                 `select pg_terminate_backend(${Number(pid)})`,
                              );
                           }
                           return read(query);
                        };
                     return fn({
                        ...held,
                        pg: killing(held.pg),
                        pgAfterCommit: killing(held.pgAfterCommit),
                     });
                  },
               ) as ReturnType<typeof fn>,
         );
         try {
            const error = await merge("committed_then_lost", [object]).then(
               () => undefined,
               (e) => e,
            );
            expect(error).toBeInstanceOf(Error);
            expect(error).not.toBeInstanceOf(CatalogAdminError);
            expect((error as Error).message).toContain(
               "committed, but its record could not be read back; a retry runs the load again",
            );
            expect(
               await harness.collectCounter("publisher_load_rejections_total", {
                  cause: "lost",
               }),
            ).toBe(0);
         } finally {
            lock.mockRestore();
            resetInstrumentsForTesting();
            await harness.shutdown();
         }
         // The load did commit: its retry finds nothing to do.
         expect(
            (await merge("committed_then_lost", [object])).rowsUnchanged,
         ).toBe(3);
      });

      it("rolls back and answers 409 when another replica replaces the table before the commit", async () => {
         const first = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         await merge("raced", [first]);
         const replacer = await attached("loads_replacer", "write");
         const changed = await parquet(
            "SELECT * FROM (VALUES (1, 'changed'), (7, 'new')) t(id, v)",
         );
         const error = await refusal(
            beforeCommit(
               () =>
                  sql(
                     replacer,
                     `CREATE OR REPLACE TABLE ${CATALOG}.s.raced AS SELECT 99 AS id, 'replaced' AS v`,
                  ).then(() => undefined),
               () => merge("raced", [changed]),
            ),
         );
         expect([error.status, error.reason]).toEqual([409, "LOAD_CONFLICT"]);
         // Only the replace landed; the load's rows did not.
         expect(await rowsOf("raced")).toEqual([{ id: 99, v: "replaced" }]);
         // The retry loads against the new table.
         expect(
            await merge("raced", [
               await parquet("SELECT 99 AS id, 'replaced' AS v"),
            ]),
         ).toMatchObject({ rowsUnchanged: 1 });
      });

      it.each(["merge into", "create"] as const)(
         "rolls back and answers 409 when another replica drops the schema before a load would %s its table",
         async (into) => {
            const schema = `dropped_${counter++}`;
            await admin.createSchema(CATALOG, schema, undefined, false);
            const loadInto = (objects: string[]) =>
               admin.loadTable(CATALOG, schema, "t", {
                  mode: "merge",
                  key: ["id"],
                  objects,
               });
            if (into === "merge into") {
               await loadInto([await parquet("SELECT 1 AS id, 'a' AS v")]);
            }
            const dropper = await attached(
               `loads_dropper_${counter++}`,
               "write",
            );
            const object = await parquet(
               "SELECT * FROM (VALUES (1, 'changed'), (2, 'new')) t(id, v)",
            );
            let dropped = 0;
            const error = await refusal(
               beforeCommit(
                  async () => {
                     await sql(
                        dropper,
                        `DROP SCHEMA ${CATALOG}.${schema} CASCADE`,
                     );
                     dropped = await snapshotId();
                  },
                  () => loadInto([object]),
               ),
            );
            expect([error.status, error.reason]).toEqual([
               409,
               "LOAD_CONFLICT",
            ]);
            // The drop is the last commit: the load wrote nothing after it.
            expect(await snapshotId()).toBe(dropped);
            expect(
               await sql(
                  reader,
                  `SELECT schema_name FROM duckdb_schemas() WHERE database_name = '${CATALOG}' AND schema_name = '${schema}'`,
               ),
            ).toEqual([]);
         },
      );

      it("answers 404 SCHEMA_NOT_FOUND when another replica drops the schema before the load's transaction begins", async () => {
         const schema = `dropped_${counter++}`;
         await admin.createSchema(CATALOG, schema, undefined, false);
         const dropper = await attached(`loads_dropper_${counter++}`, "write");
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         // The load has read the schema's owner, so the schema is past the 404 check on the catalog database.
         const lock = spyOn(admin, "withTableLock");
         lock.mockImplementation(
            (catalog, schemaName, table, fn) =>
               CatalogAdmin.prototype.withTableLock.call(
                  admin,
                  catalog,
                  schemaName,
                  table,
                  (held) =>
                     fn({
                        ...held,
                        pg: async (query) => {
                           const rows = await held.pg(query);
                           if (query.includes("pg_get_userbyid")) {
                              await sql(
                                 dropper,
                                 `DROP SCHEMA ${CATALOG}.${schema} CASCADE`,
                              );
                           }
                           return rows;
                        },
                     }),
               ) as ReturnType<typeof fn>,
         );
         let error: CatalogAdminError;
         try {
            error = await refusal(
               admin.loadTable(CATALOG, schema, "t", {
                  mode: "merge",
                  key: ["id"],
                  objects: [object],
               }),
            );
         } finally {
            lock.mockRestore();
         }
         expect([error.status, error.reason]).toEqual([
            404,
            "SCHEMA_NOT_FOUND",
         ]);
      });

      it("refuses a load while this process's load slot is held, and releases the table's lock", async () => {
         const object = await parquet("SELECT 1 AS id, 'x' AS v");
         const slot = loadSlots.tryAcquire()!;
         try {
            const error = await refusal(merge("slot", [object]));
            expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
         } finally {
            slot.release();
         }
         expect((await merge("slot", [object])).rowsInserted).toBe(1);
         expect(loadSlots.active).toBe(0);
      });
   });

   describe("metering", () => {
      it("meters the rows a load wrote, none for a no-op, and an error for a refusal", async () => {
         const object = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         const wrote = await metered(() => merge("meter", [object]));
         expect(wrote).toHaveLength(1);
         expect(wrote[0]).toMatchObject({
            surface: "load",
            connection: CATALOG,
            catalog: CATALOG,
            rows: 3,
            status: "ok",
         });
         const changed = await parquet(
            "SELECT * FROM (VALUES (1, 'x'), (9, 'y')) t(id, v)",
         );
         expect((await metered(() => merge("meter", [changed])))[0].rows).toBe(
            2,
         );
         expect(
            (await metered(() => merge("meter", [changed])))[0],
         ).toMatchObject({
            rows: 0,
            status: "ok",
         });
         const refused = await metered(() =>
            load("meter", {
               mode: "replace",
               key: ["id"],
               maxDeletedRows: 0,
               objects: [changed],
            }),
         );
         expect(refused).toHaveLength(1);
         expect(refused[0]).toMatchObject({ status: "error" });
         expect(refused[0].rows).toBeUndefined();
         // A request refused before it ran is not metered.
         expect(
            await metered(() =>
               load("meter", { mode: "merge", objects: [object] }),
            ),
         ).toEqual([]);
      });
   });

   describe("the plan", () => {
      it.each(["merge", "replace"] as const)(
         "plans every %s statement as hash joins, never a nested loop, and a group pass reads only its own columns",
         async (mode) => {
            const table = `plan_${mode}`;
            await merge(table, [
               await parquet(
                  "SELECT i AS id, 'v' || i AS v, 'w' || i AS w FROM range(1, 5001) t(i)",
               ),
            ]);
            const target = `"${CATALOG}"."s"."${table}"`;
            await sql(
               writer,
               "CREATE OR REPLACE TEMP TABLE \"__publisher_stage\" AS SELECT i AS id, 'x' || i AS v, 'y' || i AS w, 0 AS __publisher_pos, 0 AS __publisher_row FROM range(1, 100) t(i)",
            );
            await sql(writer, 'DROP TABLE IF EXISTS "__publisher_changes"');
            const column = (name: string) => ({
               name,
               type: "VARCHAR",
               inStage: true,
               inTable: true,
            });
            const statements = mergeStatements({
               target,
               mode,
               keys: ["id"],
               columns: [column("v"), column("w")],
               groups: [[column("v")], [column("w")]],
            });
            const plan = async (statement: string) =>
               (await sql(writer, `EXPLAIN ${statement}`))
                  .map((r) => String(r.explain_value))
                  .join("\n");
            const planned: string[] = [];
            // The keys pass, then a pass per group straight into the changes.
            expect(statements.changes).toHaveLength(3);
            for (const statement of statements.changes) {
               planned.push(await plan(statement));
               await sql(writer, statement);
            }
            planned.push(
               await plan(statements.delete),
               await plan(statements.insert),
            );
            for (const text of planned) {
               expect(text).toContain("HASH_JOIN");
               expect(text).not.toContain("NL_JOIN");
               expect(text).not.toContain("CROSS_PRODUCT");
            }
            const [keysPass, vPass, wPass, , insert] = planned;
            // A delim join holds every column of its left side; the delete's is over the table's row ids alone.
            for (const text of [keysPass, vPass, wPass, insert]) {
               expect(text).not.toContain("DELIM_JOIN");
            }
            // A group pass anti joins the changes, hashing their keys, so a key several groups change lands once.
            for (const text of [vPass, wPass]) {
               expect(text).toMatch(/Join Type:\s+(RIGHT_)?ANTI/);
            }
            expect(keysPass).not.toMatch(/\b[vw]\b/);
            expect(vPass).toMatch(/\bv\b/);
            expect(vPass).not.toMatch(/\bw\b/);
            expect(wPass).toMatch(/\bw\b/);
            expect(wPass).not.toMatch(/\bv\b/);
         },
      );
   });

   describe("a session that cannot finish its transaction", () => {
      /** The first rejection of `run`, or undefined when it resolves. */
      const failure = (run: Promise<unknown>) =>
         run.then(
            () => undefined,
            (e: unknown) => e as Error,
         );

      /**
       * Runs the Postgres scenario of the fatal-session fixture in a child
       * process, which DuckDB aborts when a session is not contained, so a
       * regression fails this case alone. `whenOpen` runs once the child
       * reports its transaction open, and the child proceeds after it.
       */
      function fixture(
         scenario: "fatal" | "dead",
         connectionString: string,
         table: string,
         whenOpen?: () => Promise<void>,
      ) {
         return new Promise<{
            status: number | null;
            signal: NodeJS.Signals | null;
            stdout: string;
            stderr: string;
            result?: Record<string, unknown>;
         }>((resolve, reject) => {
            const child = spawn(
               process.execPath,
               [
                  join(
                     __dirname,
                     "../../../src/test_helpers/fatal_session_fixture.ts",
                  ),
                  "postgres",
                  scenario,
                  table,
               ],
               {
                  cwd: join(__dirname, "../../.."),
                  env: {
                     ...process.env,
                     FATAL_FIXTURE_CONNINFO: connectionString,
                  },
               },
            );
            let stdout = "";
            let stderr = "";
            let opened = false;
            const timer = setTimeout(() => {
               child.kill("SIGKILL");
               reject(new Error(`The fixture timed out:\n${stderr}`));
            }, 120_000);
            child.stderr.on("data", (chunk) => (stderr += chunk));
            child.stdout.on("data", (chunk) => {
               stdout += chunk;
               if (!whenOpen || opened || !stdout.includes("OPEN\n")) return;
               opened = true;
               whenOpen().then(
                  () => child.stdin.write("go\n"),
                  (error) => {
                     child.kill("SIGKILL");
                     reject(error);
                  },
               );
            });
            child.on("close", (status, signal) => {
               clearTimeout(timer);
               const line = stdout
                  .split("\n")
                  .find((l) => l.startsWith("RESULT "));
               resolve({
                  status,
                  signal,
                  stdout,
                  stderr,
                  result: line && JSON.parse(line.slice("RESULT ".length)),
               });
            });
         });
      }

      /**
       * A catalog on its own database holding `s.dead` (1, 2, 3), dropped after
       * `run`: a case that kills the metadata connections cannot share the suite's.
       */
      async function withDeadCatalog(
         suffix: string,
         run: (catalog: {
            conninfo: string;
            database: string;
            dataPath: string;
            admin: CatalogAdmin;
            rowsAfter: () => Promise<Record<string, unknown>[]>;
         }) => Promise<void>,
      ) {
         const database = `loads_dead${suffix}_${process.pid}`;
         const deadConninfo = `${SUPERUSER} dbname=${database}`;
         const deadPath = join(root, `dead${suffix}-data`);
         await superuser(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
         await superuser(`CREATE DATABASE ${database}`);
         try {
            const deadAdmin = new CatalogAdmin({
               catalogDatabase: deadConninfo,
            });
            await deadAdmin.createCatalog({
               name: CATALOG,
               dataPath: deadPath,
               encrypted: true,
               schemas: ["s"],
            });
            await deadAdmin.loadTable(CATALOG, "s", "dead", {
               mode: "merge",
               key: ["id"],
               objects: [
                  await parquet(
                     "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
                     deadPath,
                  ),
               ],
            });
            await run({
               conninfo: deadConninfo,
               database,
               dataPath: deadPath,
               admin: deadAdmin,
               rowsAfter: async () =>
                  sql(
                     (
                        await openSession(
                           `loads_dead${suffix}_read`,
                           "read",
                           CATALOG,
                           deadConninfo,
                        )
                     ).session,
                     `SELECT id::INTEGER AS id FROM ${CATALOG}.s.dead ORDER BY id`,
                  ),
            });
         } finally {
            await superuser(`DROP DATABASE IF EXISTS ${database} WITH (FORCE)`);
         }
      }

      it("fails on a fatal DuckDB error, rolls back through Postgres, and leaves the table to the next merge", async () => {
         const first = await parquet(
            "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
         );
         await merge("fatal", [first]);
         expect(await dataFiles("fatal")).toBeGreaterThan(0);
         const run = await fixture("fatal", conninfo, `${CATALOG}.s.fatal`);
         // Exit 134 is the abort of DuckDB 1.5's destructor, which the session contains.
         expect({ status: run.status, stderr: run.stderr }).toMatchObject({
            status: 0,
         });
         expect(
            run.result?.transaction,
            "The second read no longer fails: DuckDB fixed duckdb#24311 (v2.0.0), so this case needs another fatal trigger",
         ).toContain("INTERNAL Error");
         expect(await rowsOf("fatal")).toEqual([
            { id: 1, v: "v1" },
            { id: 2, v: "v2" },
            { id: 3, v: "v3" },
         ]);
         const second = await parquet(
            "SELECT * FROM (VALUES (3, 'changed'), (4, 'v4')) t(id, v)",
         );
         expect(await merge("fatal", [second])).toMatchObject({
            rowsInserted: 1,
            rowsReplaced: 1,
            rowCount: 4,
         });
      });

      it("fails when the catalog's metadata connection dies mid-transaction, and the catalog serves the next operation", async () => {
         await withDeadCatalog("", async (dead) => {
            const { session, dispose } = await openSession(
               "loads_dead",
               "write",
               CATALOG,
               dead.conninfo,
            );
            const table = `${CATALOG}.s.dead`;
            const error = await failure(
               inTransaction(session, table, async () => {
                  await session.runSQL(`INSERT INTO ${table} VALUES (99, 'x')`);
                  await superuser(
                     "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                        `WHERE datname = '${dead.database}' AND pid <> pg_backend_pid()`,
                     dead.database,
                  );
                  await session.runSQL(
                     `INSERT INTO ${table} VALUES (100, 'y')`,
                  );
               }),
            );
            expect(error).toBeInstanceOf(Error);
            await dispose();
            expect(await dead.rowsAfter()).toEqual([
               { id: 1 },
               { id: 2 },
               { id: 3 },
            ]);
            const next = await dead.admin.loadTable(CATALOG, "s", "dead", {
               mode: "merge",
               key: ["id"],
               objects: [
                  await parquet(
                     "SELECT * FROM (VALUES (4, 'v4')) t(id, v)",
                     dead.dataPath,
                  ),
               ],
            });
            expect(next.load).toMatchObject({ rowsInserted: 1 });
         });
      });

      it("disposes a session whose transaction is still open when its metadata connection is dead, without aborting the process", async () => {
         await withDeadCatalog("_open", async (dead) => {
            const run = await fixture(
               "dead",
               dead.conninfo,
               `${CATALOG}.s.dead`,
               async () => {
                  await superuser(
                     "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
                        `WHERE datname = '${dead.database}' AND pid <> pg_backend_pid()`,
                     dead.database,
                  );
               },
            );
            // Exit 134 is DuckLake's destructor rollback throwing past the disconnect.
            expect({
               status: run.status,
               signal: run.signal,
               stderr: run.stderr,
            }).toMatchObject({ status: 0 });
            expect(run.result).toEqual({ disposed: true });
            expect(await dead.rowsAfter()).toEqual([
               { id: 1 },
               { id: 2 },
               { id: 3 },
            ]);
            const next = await dead.admin.loadTable(CATALOG, "s", "dead", {
               mode: "merge",
               key: ["id"],
               objects: [
                  await parquet(
                     "SELECT * FROM (VALUES (4, 'v4')) t(id, v)",
                     dead.dataPath,
                  ),
               ],
            });
            expect(next.load).toMatchObject({ rowsInserted: 1 });
         });
      });
   });

   describe("the compare passes", () => {
      const destination = {
         name: CATALOG,
         type: "ducklake",
         ducklakeConnection: {
            catalog: {
               postgresConnection: { connectionString: conninfo },
               metadataSchema: CATALOG,
            },
         },
      } as components["schemas"]["Connection"];

      /** A load that skips the endpoint's lock and slot, so a test can set the compare budget. */
      const direct = (
         table: string,
         body: {
            mode: "merge" | "replace";
            key?: string[];
            objects: string[];
            maxDeletedRows?: number;
         },
         columnGroupBudget?: number,
      ) =>
         mergeStorageTable({
            destinationName: CATALOG,
            destinationConnection: destination,
            schemaName: "s",
            tableName: table,
            key: [],
            record: { id: table, table: `s.${table}`, requestDigest: "" },
            assertLockHeld: async () => undefined,
            columnGroupBudget,
            ...body,
         }).then((outcome) => outcome.result);

      const passesOf = (seen: string[]) =>
         seen.filter((s) => s.startsWith('INSERT INTO "__publisher_changes"'))
            .length;

      // A budget of one byte puts every compared column in a group of its own.
      describe.each([
         ["one group", undefined, 1],
         ["a group a column", 1, 4],
      ] as const)("%s", (_, budget, passes) => {
         const tag = budget === undefined ? "one" : "each";

         it("merges changes that land in different groups, counting a row changed in two once", async () => {
            const table = `grp_merge_${tag}`;
            await direct(
               table,
               {
                  mode: "merge",
                  key: ["id"],
                  objects: [
                     await parquet(
                        "SELECT i AS id, i AS a, CASE WHEN i = 6 THEN NULL ELSE 'b' || i END AS b, " +
                           "i::DOUBLE / 2 AS c, DATE '2026-01-01' + i::INTEGER AS d FROM range(1, 7) t(i)",
                     ),
                  ],
               },
               budget,
            );
            const object = await parquet(
               "SELECT i AS id, CASE WHEN i IN (2, 5) THEN i + 100 ELSE i END AS a, " +
                  "CASE WHEN i IN (3, 6) THEN NULL ELSE 'b' || i END AS b, " +
                  "CASE WHEN i = 4 THEN 99.5 ELSE i::DOUBLE / 2 END AS c, " +
                  "CASE WHEN i = 5 THEN DATE '2030-01-01' ELSE DATE '2026-01-01' + i::INTEGER END AS d FROM range(1, 9) t(i)",
            );
            const { value, seen } = await recorded(() =>
               direct(
                  table,
                  { mode: "merge", key: ["id"], objects: [object] },
                  budget,
               ),
            );
            expect(value).toMatchObject({
               rowsInserted: 2,
               rowsReplaced: 4,
               rowsUnchanged: 2,
               rowsDeleted: 0,
               rowCount: 8,
               cursorMax: null,
            });
            expect(passesOf(seen)).toBe(passes);
            expect(
               await rowsOf(
                  table,
                  "id::INTEGER AS id, a::INTEGER AS a, b, c, d::VARCHAR AS d",
               ),
            ).toEqual([
               { id: 1, a: 1, b: "b1", c: 0.5, d: "2026-01-02" },
               { id: 2, a: 102, b: "b2", c: 1, d: "2026-01-03" },
               { id: 3, a: 3, b: null, c: 1.5, d: "2026-01-04" },
               { id: 4, a: 4, b: "b4", c: 99.5, d: "2026-01-05" },
               { id: 5, a: 105, b: "b5", c: 2.5, d: "2030-01-01" },
               { id: 6, a: 6, b: null, c: 3, d: "2026-01-07" },
               { id: 7, a: 7, b: "b7", c: 3.5, d: "2026-01-08" },
               { id: 8, a: 8, b: "b8", c: 4, d: "2026-01-09" },
            ]);
         });

         it("heals a key stored twice that a column also changes, compares a column the objects lack as NULL, widens, and adds a column", async () => {
            const table = `grp_shape_${tag}`;
            await direct(
               table,
               {
                  mode: "merge",
                  key: ["id"],
                  objects: [
                     await parquet(
                        "SELECT i AS id, i AS a, 'b' || i AS b, CASE WHEN i = 3 THEN 7.5 END AS c, " +
                           "CASE WHEN i = 4 THEN 'x' END AS d FROM range(1, 6) t(i)",
                     ),
                  ],
               },
               budget,
            );
            await sql(
               writer,
               `INSERT INTO ${CATALOG}.s.${table} VALUES (1, 1, 'b1', NULL, NULL)`,
            );
            const object = await parquet(
               "SELECT i AS id, (CASE WHEN i = 1 THEN 11 ELSE i END)::INTEGER AS a, 'b' || i AS b, " +
                  "CASE WHEN i = 5 THEN 'new' END AS e FROM range(1, 7) t(i)",
            );
            expect(
               await direct(
                  table,
                  { mode: "merge", key: ["id"], objects: [object] },
                  budget,
               ),
            ).toMatchObject({
               rowsInserted: 1,
               rowsReplaced: 4,
               rowsUnchanged: 1,
               rowsDeleted: 1,
               rowCount: 6,
               cursorMax: null,
            });
            expect(
               await rowsOf(
                  table,
                  "id::INTEGER AS id, a::INTEGER AS a, b, c, d, e",
               ),
            ).toEqual([
               { id: 1, a: 11, b: "b1", c: null, d: null, e: null },
               { id: 2, a: 2, b: "b2", c: null, d: null, e: null },
               { id: 3, a: 3, b: "b3", c: null, d: null, e: null },
               { id: 4, a: 4, b: "b4", c: null, d: null, e: null },
               { id: 5, a: 5, b: "b5", c: null, d: null, e: "new" },
               { id: 6, a: 6, b: "b6", c: null, d: null, e: null },
            ]);
         });

         it("replaces by key when the absent keys and the changed columns land in different groups", async () => {
            const table = `grp_replace_${tag}`;
            await direct(
               table,
               {
                  mode: "merge",
                  key: ["id"],
                  objects: [
                     await parquet(
                        "SELECT i AS id, i AS a, 'b' || i AS b, i::DOUBLE AS c FROM range(1, 9) t(i)",
                     ),
                  ],
               },
               budget,
            );
            await sql(
               writer,
               `INSERT INTO ${CATALOG}.s.${table} VALUES (NULL, 0, 'orphan', 0)`,
            );
            const object = await parquet(
               "SELECT i AS id, CASE WHEN i = 2 THEN 200 ELSE i END AS a, 'b' || i AS b, " +
                  "CASE WHEN i = 4 THEN 44.5 ELSE i::DOUBLE END AS c FROM range(1, 10) t(i) WHERE i IN (1, 2, 3, 4, 5, 9)",
            );
            expect(
               await direct(
                  table,
                  {
                     mode: "replace",
                     key: ["id"],
                     maxDeletedRows: 4,
                     objects: [object],
                  },
                  budget,
               ),
            ).toMatchObject({
               rowsInserted: 1,
               rowsReplaced: 2,
               rowsUnchanged: 3,
               rowsDeleted: 4,
               rowCount: 6,
               cursorMax: null,
            });
            expect(
               await rowsOf(table, "id::INTEGER AS id, a::INTEGER AS a, c"),
            ).toEqual([
               { id: 1, a: 1, c: 1 },
               { id: 2, a: 200, c: 2 },
               { id: 3, a: 3, c: 3 },
               { id: 4, a: 4, c: 44.5 },
               { id: 5, a: 5, c: 5 },
               { id: 9, a: 9, c: 9 },
            ]);
         });
      });

      it.each(["merge", "replace"] as const)(
         "builds the %s changes the single wide statement built, whatever the groups",
         async (mode) => {
            const table = `grp_equal_${mode}`;
            await direct(table, {
               mode: "merge",
               key: ["id"],
               objects: [
                  await parquet(
                     "SELECT i AS id, i AS a, CASE WHEN i % 3 = 0 THEN NULL ELSE 'b' || i END AS b, i::DOUBLE AS c " +
                        "FROM range(1, 13) t(i)",
                  ),
               ],
            });
            await sql(
               writer,
               `INSERT INTO ${CATALOG}.s.${table} VALUES (2, 2, 'b2', 2), (3, 3, NULL, 3), (NULL, 0, 'orphan', 0)`,
            );
            // Against the stored rows: 1 and 6 unchanged; 4 (a), 5 and 7 (b), 8 (c) and 9 (a and c) changed; 2 and 3 stored twice; 10 to 12 and the NULL key absent; 13 and 14 new.
            await sql(
               writer,
               'CREATE OR REPLACE TEMP TABLE "__publisher_stage" AS SELECT i AS id, ' +
                  "CASE WHEN i IN (4, 9) THEN i + 100 ELSE i END AS a, " +
                  "CASE WHEN i % 3 = 0 THEN NULL WHEN i IN (5, 7) THEN 'changed' ELSE 'b' || i END AS b, " +
                  "CASE WHEN i IN (8, 9) THEN -1.0 ELSE i::DOUBLE END AS c, " +
                  "0 AS __publisher_pos, 0 AS __publisher_row FROM range(1, 15) t(i) WHERE i < 10 OR i > 12",
            );
            const target = `"${CATALOG}"."s"."${table}"`;
            const replace = mode === "replace";
            const key = replace ? 'coalesce(__s."id", __t."id")' : '__s."id"';
            const touched = [
               replace ? "count(__s.__publisher_pos) = 0" : undefined,
               "count(__t.__publisher_present) <> 1",
               'bool_or(__t."a" IS DISTINCT FROM __s."a" OR __t."b" IS DISTINCT FROM __s."b" OR __t."c" IS DISTINCT FROM __s."c")',
            ]
               .filter(Boolean)
               .join(" OR ");
            await sql(writer, 'DROP TABLE IF EXISTS "__reference"');
            await sql(
               writer,
               `CREATE TEMP TABLE "__reference" AS SELECT ${key} AS "id", ` +
                  (replace
                     ? "CASE WHEN count(__s.__publisher_pos) = 0 THEN 'deleted' WHEN count(__t.__publisher_present) = 0 THEN 'new' ELSE 'changed' END"
                     : "CASE WHEN count(__t.__publisher_present) = 0 THEN 'new' ELSE 'changed' END") +
                  " AS __publisher_status, count(__t.__publisher_present) AS __publisher_stored " +
                  `FROM "__publisher_stage" AS __s ${replace ? "FULL" : "LEFT"} JOIN (SELECT *, TRUE AS __publisher_present FROM ${target}) AS __t ` +
                  `ON __s."id" = __t."id" GROUP BY ${key} HAVING ${touched}`,
            );
            const kinds = (
               await sql(
                  writer,
                  'SELECT __publisher_status AS status, __publisher_stored AS stored, count(*) AS n FROM "__reference" GROUP BY ALL ORDER BY ALL',
               )
            ).map((r) => [r.status, Number(r.stored), Number(r.n)]);
            expect(kinds).toEqual([
               ["changed", 1, 5],
               ["changed", 2, 2],
               ...(replace ? [["deleted", 1, 4]] : []),
               ["new", 0, 2],
            ]);
            const column = (name: string) => ({
               name,
               type: "VARCHAR",
               inStage: true,
               inTable: true,
            });
            const [a, b, c] = ["a", "b", "c"].map(column);
            for (const groups of [
               [[a, b, c]],
               [[a], [b], [c]],
               [[b, c], [a]],
            ]) {
               await sql(writer, 'DROP TABLE IF EXISTS "__publisher_changes"');
               for (const statement of mergeStatements({
                  target,
                  mode,
                  keys: ["id"],
                  columns: [a, b, c],
                  groups,
               }).changes) {
                  await sql(writer, statement);
               }
               for (const [left, right] of [
                  ['"__publisher_changes"', '"__reference"'],
                  ['"__reference"', '"__publisher_changes"'],
               ]) {
                  expect(
                     await sql(
                        writer,
                        `SELECT count(*)::INTEGER AS n FROM (SELECT * FROM ${left} EXCEPT SELECT * FROM ${right})`,
                     ),
                  ).toEqual([{ n: 0 }]);
               }
               expect(
                  await sql(
                     writer,
                     'SELECT count(*)::INTEGER AS n FROM "__publisher_changes"',
                  ),
               ).toEqual([
                  { n: kinds.reduce((sum, k) => sum + Number(k[2]), 0) },
               ]);
            }
         },
      );

      it("sizes the groups by the smaller of the stage and the table, the side a hash join builds on", async () => {
         const table = "grp_smaller_side";
         await direct(table, {
            mode: "merge",
            key: ["id"],
            objects: [
               await parquet(
                  "SELECT i AS id, i + 100 AS a, 2 AS b, 3 AS c FROM range(1, 2) t(i)",
               ),
            ],
         });
         // Over budget at the stage's 2000 rows, within it at the table's one.
         const { value, seen } = await recorded(async () =>
            direct(
               table,
               {
                  mode: "merge",
                  key: ["id"],
                  objects: [
                     await parquet(
                        "SELECT i AS id, i AS a, 2 AS b, 3 AS c FROM range(1, 2001) t(i)",
                     ),
                  ],
               },
               10_000,
            ),
         );
         expect(value).toMatchObject({ rowsInserted: 1999, rowsReplaced: 1 });
         expect(passesOf(seen)).toBe(1);
      });

      it("keyless: always compares the bags, and guards the rows that leave", async () => {
         const replace = async (
            rows: number,
            maxDeletedRows: number,
            edit = false,
         ) =>
            load("kl_shortcut", {
               mode: "replace",
               maxDeletedRows,
               objects: [
                  await parquet(
                     `SELECT i AS id, CASE WHEN i = 3 AND ${edit} THEN 'edited' ELSE 'v' || i END AS v FROM range(1, ${rows + 1}) t(i)`,
                  ),
               ],
            });
         const compares = (seen: string[]) =>
            seen.filter((s) => s.includes("__publisher_side")).length;
         await merge("kl_shortcut", [
            await parquet(
               "SELECT i AS id, 'v' || i AS v FROM range(1, 6) t(i)",
            ),
         ]);
         const snapshot = await snapshotId();
         const same = await recorded(() => replace(5, 0));
         expect(same.value.rowsUnchanged).toBe(5);
         expect(compares(same.seen)).toBe(1);
         expect(await snapshotId()).toBe(snapshot);
         const edited = await recorded(() => replace(5, 1, true));
         expect([
            edited.value.rowsInserted,
            edited.value.rowsDeleted,
            edited.value.rowsUnchanged,
            edited.value.rowsReplaced,
            compares(edited.seen),
         ]).toEqual([1, 1, 4, 0, 1]);
         const grown = await recorded(() => replace(7, 1));
         expect(grown.value).toMatchObject({
            rowsInserted: 3,
            rowsReplaced: 0,
            rowsUnchanged: 4,
            rowsDeleted: 1,
            rowCount: 7,
         });
         expect(compares(grown.seen)).toBe(1);
         const shrunk = await recorded(() => replace(2, 6));
         expect(shrunk.value).toMatchObject({
            rowsInserted: 0,
            rowsReplaced: 0,
            rowsUnchanged: 2,
            rowsDeleted: 5,
            rowCount: 2,
         });
         expect(compares(shrunk.seen)).toBe(1);
         const refused = await recorded(() => refusal(replace(1, 0)));
         expect(refused.value.reason).toBe("LOAD_DELETE_LIMIT_EXCEEDED");
         expect(refused.value.detail).toEqual({
            wouldDelete: 1,
            rowsBefore: 2,
            maxDeletedRows: 0,
         });
         expect(compares(refused.seen)).toBe(1);
         expect(await rowsOf("kl_shortcut")).toEqual([
            { id: 1, v: "v1" },
            { id: 2, v: "v2" },
         ]);
      });

      describe("a keyless replace", () => {
         const compares = (seen: string[]) =>
            seen.filter((s) => s.includes("__publisher_side"));
         const attempt = async (
            table: string,
            select: string,
            budget?: number,
            maxDeletedRows = 100_000,
         ) =>
            direct(
               table,
               {
                  mode: "replace",
                  maxDeletedRows,
                  objects: [await parquet(select)],
               },
               budget,
            );
         const keyless = (...args: Parameters<typeof attempt>) =>
            recorded(() => attempt(...args));

         it("compares in as many passes as its bytes need, and counts what one pass counts", async () => {
            const seed =
               "SELECT i AS id, 'v' || i AS v FROM range(1, 201) t(i) UNION ALL SELECT 1, 'v1'";
            const next =
               "SELECT i AS id, CASE WHEN i % 10 = 0 THEN 'edited' ELSE 'v' || i END AS v FROM range(1, 221) t(i)";
            const counts = {
               rowsInserted: 40,
               rowsReplaced: 0,
               rowsUnchanged: 180,
               rowsDeleted: 21,
               rowCount: 220,
            };
            await keyless("kp_one", seed);
            await keyless("kp_many", seed);
            const one = await keyless("kp_one", next);
            expect(one.value).toMatchObject(counts);
            expect(compares(one.seen)).toHaveLength(1);
            expect(compares(one.seen)[0]).not.toContain("hash(");
            const many = await keyless("kp_many", next, 64);
            expect(many.value).toEqual(one.value);
            expect(compares(many.seen).length).toBeGreaterThan(1);
            for (const pass of compares(many.seen)) {
               expect(pass).toContain("hash(");
            }
            expect(await rowsOf("kp_many")).toEqual(await rowsOf("kp_one"));
         });

         it("counts exactly across passes when the stage and the table type a column differently, whatever its NaN and -0.0", async () => {
            const row = (stage: boolean, from: number, to: number) => {
               const zero = stage ? "'-0.0'::DOUBLE" : "0.0::DOUBLE";
               const nan = stage ? "'nan'::DOUBLE" : "-('nan'::DOUBLE)";
               const d = stage ? "DECIMAL(6,1)" : "DECIMAL(9,2)";
               const f = stage ? "FLOAT" : "DOUBLE";
               // The load widens a scalar column, not a list: both sides type it alike.
               const l = "INT[]";
               return (
                  `SELECT (i % 1000)::${d} AS d, CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 13)::${f} END AS f, ` +
                  `[i % 5, NULL]::${l} AS l, {'a': CASE WHEN i % 11 = 0 THEN ${zero} ELSE (i % 3)::DOUBLE END, 'b': 'x' || (i % 17)} AS s, ` +
                  `CASE WHEN i % 19 = 0 THEN ${nan} ELSE (i % 23)::DOUBLE END AS n FROM range(${from}, ${to}) t(i)`
               );
            };
            for (const table of ["mixed_one", "mixed_many"]) {
               await sql(
                  writer,
                  `CREATE TABLE ${CATALOG}.s.${table} AS ${row(false, 300, 2300)}`,
               );
            }
            const stage = row(true, 0, 2000);
            const one = await keyless("mixed_one", stage);
            const many = await keyless("mixed_many", stage, 50_000);
            expect(compares(one.seen)).toHaveLength(1);
            expect(compares(many.seen).length).toBeGreaterThan(1);
            expect(many.value).toEqual(one.value);
            // Rows 300..1999 are on both sides; -0.0 equals 0.0 and NaN equals NaN, as an EXCEPT ALL of the two bags counts them.
            expect(one.value).toMatchObject({
               rowsInserted: 300,
               rowsReplaced: 0,
               rowsUnchanged: 1700,
               rowsDeleted: 300,
               rowCount: 2000,
            });
         });

         it("deletes everything for an empty stage and inserts everything into an empty table, with no compare", async () => {
            await keyless(
               "kp_empty",
               "SELECT i AS id, 'v' AS v FROM range(1, 6) t(i)",
            );
            const before = await snapshotId();
            const refused = await recorded(() =>
               attempt(
                  "kp_empty",
                  "SELECT 1 AS id, 'x' AS v WHERE false",
                  undefined,
                  4,
               ).catch((e: unknown) => e),
            );
            const error = refused.value as LoadRefusalError;
            expect(error.reason).toBe("LOAD_DELETE_LIMIT_EXCEEDED");
            expect(error.detail).toEqual({
               wouldDelete: 5,
               rowsBefore: 5,
               maxDeletedRows: 4,
            });
            expect(compares(refused.seen)).toHaveLength(0);
            expect(await snapshotId()).toBe(before);
            const emptied = await keyless(
               "kp_empty",
               "SELECT 1 AS id, 'x' AS v WHERE false",
               undefined,
               5,
            );
            expect(emptied.value).toMatchObject({
               rowsInserted: 0,
               rowsReplaced: 0,
               rowsUnchanged: 0,
               rowsDeleted: 5,
               rowCount: 0,
            });
            expect(compares(emptied.seen)).toHaveLength(0);
            const filled = await keyless(
               "kp_empty",
               "SELECT i AS id, 'v' AS v FROM range(1, 4) t(i)",
               undefined,
               0,
            );
            expect(filled.value).toMatchObject({
               rowsInserted: 3,
               rowsReplaced: 0,
               rowsUnchanged: 0,
               rowsDeleted: 0,
               rowCount: 3,
            });
            expect(compares(filled.seen)).toHaveLength(0);
         });
      });

      it("answers an out-of-memory in the flush after the commit as a 500, not as a load that wrote nothing", async () => {
         const original = DuckDBConnection.prototype.runSQL;
         const spy = spyOn(
            DuckDBConnection.prototype,
            "runSQL",
         ).mockImplementation(function (
            this: DuckDBConnection,
            ...args: Parameters<typeof original>
         ) {
            return args[0].startsWith("CALL ducklake_flush_inlined_data")
               ? Promise.reject(
                    new Error(
                       "Out of Memory Error: failed to pin block of size 256.0 KiB",
                    ),
                 )
               : original.apply(this, args);
         });
         const error = await merge("oom_flush", [
            await parquet("SELECT 1 AS id, 'a' AS v"),
         ]).then(
            () => undefined,
            (e) => e,
         );
         spy.mockRestore();
         expect(error).toBeInstanceOf(CommittedLoadError);
         expect(error).not.toBeInstanceOf(CatalogAdminError);
         expect(error.message).toContain("failed to pin block");
         expect(await rowsOf("oom_flush")).toEqual([{ id: 1, v: "a" }]);
      });

      it("does not call a flush failure after a load that changed nothing a failure after its commit, and still fails it", async () => {
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         await merge("noop_flush", [object]);
         const original = DuckDBConnection.prototype.runSQL;
         const spy = spyOn(
            DuckDBConnection.prototype,
            "runSQL",
         ).mockImplementation(function (
            this: DuckDBConnection,
            ...args: Parameters<typeof original>
         ) {
            return args[0].startsWith("CALL ducklake_flush_inlined_data")
               ? Promise.reject(new Error("IO Error: flush failed"))
               : original.apply(this, args);
         });
         const snapshot = await snapshotId();
         const error = await merge("noop_flush", [object]).then(
            () => undefined,
            (e) => e,
         );
         spy.mockRestore();
         expect(error).toBeInstanceOf(Error);
         expect(error).not.toBeInstanceOf(CommittedLoadError);
         expect(error).not.toBeInstanceOf(CatalogAdminError);
         expect(error.message).toContain("flush failed");
         expect(await snapshotId()).toBe(snapshot);
      });

      it("answers an out-of-memory in the flush after a load that changed nothing as a 500, not a 422 whose advice is for the load", async () => {
         const object = await parquet("SELECT 1 AS id, 'a' AS v");
         await merge("noop_oom_flush", [object]);
         const original = DuckDBConnection.prototype.runSQL;
         const spy = spyOn(
            DuckDBConnection.prototype,
            "runSQL",
         ).mockImplementation(function (
            this: DuckDBConnection,
            ...args: Parameters<typeof original>
         ) {
            return args[0].startsWith("CALL ducklake_flush_inlined_data")
               ? Promise.reject(
                    new Error(
                       "Out of Memory Error: failed to pin block of size 256.0 KiB",
                    ),
                 )
               : original.apply(this, args);
         });
         const error = await merge("noop_oom_flush", [object]).then(
            () => undefined,
            (e) => e,
         );
         spy.mockRestore();
         expect(error).toBeInstanceOf(Error);
         expect(error).not.toBeInstanceOf(CatalogAdminError);
         expect(error.message).toContain("failed to pin block");
      });

      it("answers a load that runs out of memory or spill with a 422 LOAD_TOO_LARGE, and writes nothing", async () => {
         await merge("oom", [
            await parquet(
               "SELECT i AS id, 'v' || i AS v FROM range(1, 4) t(i)",
            ),
         ]);
         const changed = await parquet("SELECT 1 AS id, 'changed' AS v");
         const original = DuckDBConnection.prototype.runSQL;
         const spy = spyOn(
            DuckDBConnection.prototype,
            "runSQL",
         ).mockImplementation(function (
            this: DuckDBConnection,
            ...args: Parameters<typeof original>
         ) {
            return args[0].startsWith('INSERT INTO "__publisher_changes"')
               ? Promise.reject(
                    new Error(
                       "Out of Memory Error: failed to offload data block of size 256.0 KiB (5.9 GiB/6.0 GiB used).",
                    ),
                 )
               : original.apply(this, args);
         });
         const snapshot = await snapshotId();
         const failed = await refusal(
            merge("oom", [changed]).finally(() => spy.mockRestore()),
         );
         expect([failed.status, failed.reason]).toEqual([
            422,
            "LOAD_TOO_LARGE",
         ]);
         expect(failed.message).toContain("failed to offload");
         expect(await snapshotId()).toBe(snapshot);
         expect(await rowsOf("oom")).toHaveLength(3);
      });
   });
});
