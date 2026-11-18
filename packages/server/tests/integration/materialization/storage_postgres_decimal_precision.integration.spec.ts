// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

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
import path from "path";
import {
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";
import { pollFor, TERMINAL_STATUSES } from "./polling";

/**
 * A `storage=` table built from Postgres keeps a `numeric`'s exact value.
 *
 * DuckDB's Postgres passthrough reads an unconstrained `numeric`, a
 * `numeric(p,s)` with p > 38 and every numeric expression as DOUBLE, so `sum`
 * of 0.1 and 0.2 served `0.30000000000000004` where live answers `0.3`. The
 * build reads such a column as Postgres's text and stores the DECIMAL(38, s)
 * that holds every value it read; one no DECIMAL holds is stored as that text
 * and answered live. Through a real server, for a `#@ persist storage=` source
 * and a `#(rollup) storage=` rollup.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const COLUMNS = "id, g, u, big, m, e is u + m, w is u * 3, hs, tz, dm";

const MODEL = [
   "##! experimental.persistence",
   "source: t is pg.table('public.t')",
   `source: live is t -> { select: ${COLUMNS} }`,
   '#@ persist name="t_decimal" storage="lake"',
   `source: persisted is t -> { select: ${COLUMNS}, huge }`,
   `source: live_huge is t -> { select: ${COLUMNS}, huge }`,
   "source: rolled is t extend {",
   '  #(rollup) grain=[g] storage="lake"',
   "  measure: total is sum(u + m)",
   "}",
   '#@ persist name="t_incremental" storage="lake" refresh="incremental" watermark="id"',
   "source: incremental is t -> { select: id, u }",
   "source: live_incremental is t -> { select: id, u }",
   '#(materialize) storage="lake" name="t_materialized"',
   "source: materialized is t extend { primary_key: id }",
   "source: live_rolled is t extend {",
   "  measure: total is sum(u + m)",
   "}",
   "",
].join("\n");

interface Answer {
   servedBy?: string;
   via?: string;
   rows: unknown;
}

describe.skipIf(!url)(
   "a storage= table built from Postgres numerics keeps their precision",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `dec_wh_${token}`;
      const catalog = `dec_lake_${token}`;
      let admin: PostgresConnection;
      let wh: PostgresConnection;
      let lakeCatalog: PostgresConnection;
      let server: SpawnedServer;
      let catalogConnection = "";
      const dirs: { cleanup(): void }[] = [];
      const api = () => `${server.baseUrl}/api/v0/environments/e1/packages/pa`;

      async function query(malloy: string): Promise<Answer> {
         const res = await fetch(`${api()}/models/model.malloy/query`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ query: malloy, compactJson: true }),
         });
         const body = (await res.json()) as {
            result?: string;
            message?: string;
            path?: { servedBy?: { kind?: string; via?: string } };
         };
         if (!body.result) throw new Error(`${malloy}: ${body.message}`);
         // Plain rows: each value as the JavaScript number live answers.
         const result = { data: JSON.parse(body.result) as unknown };
         return {
            servedBy: body.path?.servedBy?.kind,
            via: body.path?.servedBy?.via,
            rows: result.data,
         };
      }

      /** Run a materialization of the package and wait for it to succeed. */
      async function build(): Promise<void> {
         const created = (await (
            await fetch(`${api()}/materializations`, {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: "{}",
            })
         ).json()) as { id?: string; message?: string };
         if (!created.id) throw new Error(`no run: ${created.message}`);
         const run = await pollFor(
            async () =>
               (await (
                  await fetch(`${api()}/materializations/${created.id}`)
               ).json()) as {
                  status: string;
                  error?: string | null;
                  manifest?: { failures?: Record<string, unknown> };
               },
            (m) => TERMINAL_STATUSES.includes(m.status),
            "the build did not settle",
            120_000,
         );
         expect(run.error ?? null).toBeNull();
         expect(run.manifest?.failures ?? {}).toEqual({});
      }

      /** Each stored column's type in the DuckLake catalog, by column name. */
      async function storedTypes(): Promise<Map<string, string[]>> {
         const rows = (
            await lakeCatalog.runSQL(
               `SELECT row_to_json(q) AS "row" FROM (
                  SELECT t.table_name, c.column_name, c.column_type
                  FROM ducklake_column c JOIN ducklake_table t ON t.table_id = c.table_id
                  WHERE c.end_snapshot IS NULL AND t.end_snapshot IS NULL) q`,
            )
         ).rows as {
            table_name: string;
            column_name: string;
            column_type: string;
         }[];
         const out = new Map<string, string[]>();
         for (const r of rows) {
            const key = `${r.table_name}.${r.column_name}`;
            out.set(key, [...(out.get(key) ?? []), r.column_type]);
         }
         return out;
      }

      beforeAll(async () => {
         admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         await admin.runSQL(`CREATE DATABASE ${warehouse}`);
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         const parsed = new URL(url!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const database = (name: string) =>
            `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${name}`;
         wh = new PostgresConnection({
            name: "wh",
            connectionString: database(warehouse),
         });
         lakeCatalog = new PostgresConnection({
            name: "lake",
            connectionString: database(catalog),
         });
         catalogConnection =
            `dbname=${catalog} host=${pg.host} port=${pg.port} ` +
            `user=${pg.userName} password=${pg.password}`;
         // u: unconstrained; big: wider than DECIMAL(38); m: a DECIMAL
         // already; huge: no DECIMAL(38) holds 10^45.
         await wh.runSQL(`CREATE DOMAIN amount AS numeric`);
         // hs: a scale of 20, as Postgres's avg and division write;
         // tz: trailing zeros; dm: a domain over numeric.
         await wh.runSQL(
            `CREATE TABLE t (id int, g text, u numeric, big numeric(50,10),
               m numeric(10,2), huge numeric, hs numeric, tz numeric, dm amount)`,
         );
         await wh.runSQL(`INSERT INTO t VALUES
            (1, 'a', 0.1, 0.1, 0.10, 1000000000000000000000000000000000000000000000.5,
               1::numeric / 3, 2.50, 0.1),
            (2, 'a', 0.2, 0.2, 0.20, 1, 2::numeric / 3, 1.100, 0.2),
            (3, 'b', 1.005, 12345678901234567890.0123456789, 3.33, 2,
               1234567.5::numeric / 7, 3.000, 1.25),
            (4, 'b', NULL, NULL, NULL, NULL, NULL, NULL, NULL)`);
         const lake = tempDir("decimal-lake-");
         dirs.push(lake);
         const dir = tempDir("decimal-pkg-");
         dirs.push(dir);
         fs.writeFileSync(
            path.join(dir.dir, "publisher.json"),
            JSON.stringify({ name: "pa", version: "1.0.0", description: "x" }),
         );
         fs.writeFileSync(path.join(dir.dir, "model.malloy"), MODEL);
         server = await spawnServer({
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: "e1",
                     packages: [{ name: "pa", location: dir.dir }],
                     connections: [
                        {
                           name: "pg",
                           type: "postgres",
                           postgresConnection: {
                              ...pg,
                              databaseName: warehouse,
                           },
                        },
                     ],
                     storageDestinations: [
                        {
                           name: "lake",
                           type: "ducklake",
                           ducklakeConnection: {
                              catalog: {
                                 postgresConnection: {
                                    ...pg,
                                    databaseName: catalog,
                                 },
                              },
                              storage: { bucketUrl: lake.dir },
                           },
                        },
                     ],
                  },
               ],
            },
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
            },
         });
         await server.waitUntilServing();
         await build();
      });

      afterAll(async () => {
         await server?.stop();
         await wh?.close().catch(() => undefined);
         await lakeCatalog?.close().catch(() => undefined);
         for (const db of [warehouse, catalog]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         for (const dir of dirs) dir.cleanup();
      });

      it("stores unconstrained, wide and computed numerics as DECIMAL, and the overflow as text", async () => {
         const types = await storedTypes();
         const of = (column: string) =>
            [...types.entries()]
               .filter(
                  ([key]) =>
                     key.startsWith("t_decimal") && key.endsWith(`.${column}`),
               )
               .flatMap(([, t]) => t);
         expect(of("u")).toEqual(["decimal(38,3)"]);
         expect(of("big")).toEqual(["decimal(38,10)"]);
         expect(of("m")).toEqual(["decimal(10,2)"]);
         expect(of("e")).toEqual(["decimal(38,3)"]);
         expect(of("w")).toEqual(["decimal(38,3)"]);
         expect(of("huge")).toEqual(["varchar"]);
         expect(of("hs")).toEqual(["decimal(38,20)"]);
         // Trailing zeros are not digits the column must hold.
         expect(of("tz")).toEqual(["decimal(38,1)"]);
         // A domain is read as its base type.
         expect(of("dm")).toEqual(["decimal(38,2)"]);
         // The rollup's partial is a numeric expression too.
         const partials = [...types.entries()].filter(
            ([key]) =>
               !key.startsWith("t_decimal") &&
               !key.startsWith("t_incremental") &&
               !key.startsWith("t_materialized") &&
               !key.endsWith(".g"),
         );
         expect(partials.length).toBeGreaterThan(0);
         for (const [, t] of partials) {
            expect(t.every((x) => x.startsWith("decimal(38,"))).toBe(true);
         }
      });

      it("serves sum and avg from the table, equal to live exactly", async () => {
         const read = (s: string) =>
            query(
               `run: ${s} -> { group_by: g; aggregate:
                  su is u.sum(), au is u.avg(), sb is big.sum(), ab is big.avg(),
                  se is e.sum(), ae is e.avg(), sw is w.sum(), sm is m.sum(),
                  sh is hs.sum(), st is tz.sum(), sd is dm.sum(), ad is dm.avg()
                  order_by: g }`,
            );
         const live = await read("live");
         const served = await read("persisted");
         expect(served.servedBy).toBe("materialized");
         expect(served.rows).toEqual(live.rows);
         const a = (live.rows as Record<string, number>[])[0];
         expect(a.su).toBe(0.3);
         expect(a.se).toBe(0.6);
      });

      it("answers a query reading the column no DECIMAL holds live, equal to live", async () => {
         const read = (s: string) =>
            query(`run: ${s} -> { select: id, huge; order_by: id }`);
         const live = await read("live_huge");
         const served = await read("persisted");
         expect(served.servedBy).toBe("live");
         expect(served.rows).toEqual(live.rows);
      });

      it("answers arithmetic past DECIMAL(38)'s digits live, equal to live", async () => {
         // DECIMAL(38,20) * DECIMAL(38,20) needs scale 40, which DuckDB
         // refuses: the stored answer degrades to live although the source
         // declares no freshnessFallback.
         const read = (s: string) =>
            query(
               `run: ${s} -> { group_by: g; aggregate: sq is sum(hs * hs); order_by: g }`,
            );
         const live = await read("live");
         const served = await read("persisted");
         expect(served.servedBy).not.toBe("materialized");
         expect(served.rows).toEqual(live.rows);
      });

      it("answers a #(materialize) query reading the column no DECIMAL holds live, equal to live", async () => {
         const read = (s: string) =>
            query(`run: ${s} -> { select: id, huge; order_by: id }`);
         const live = await read("live_huge");
         const served = await read("materialized");
         expect(served.servedBy).toBe("live");
         expect(served.rows).toEqual(live.rows);
      });

      it("serves a rollup's numeric partial, equal to live exactly", async () => {
         const read = (s: string) =>
            query(
               `run: ${s} -> { group_by: g; aggregate: total; order_by: g }`,
            );
         const live = await read("live_rolled");
         const served = await read("rolled");
         expect(served.servedBy).toBe("rollup");
         expect(served.rows).toEqual(live.rows);
         expect((live.rows as Record<string, number>[])[0].total).toBe(0.6);
      });

      it("refreshes an incremental table exactly, and re-seeds when a delta needs a wider scale", async () => {
         const uType = async () =>
            [...(await storedTypes()).entries()]
               .filter(
                  ([key]) =>
                     key.startsWith("t_incremental") && key.endsWith(".u"),
               )
               .flatMap(([, t]) => t);
         // A delta reads [covered, frontier), so each step adds a row past
         // the one it checks, and compares the rows the table covers.
         const sums = async (through: number) => {
            const read = (s: string) =>
               query(
                  `run: ${s} -> { where: id <= ${through}; aggregate: su is u.sum(), c is count() }`,
               );
            const live = await read("live_incremental");
            const served = await read("incremental");
            expect(served.servedBy).toBe("materialized");
            expect(served.rows).toEqual(live.rows);
            return live.rows;
         };
         expect(await uType()).toEqual(["decimal(38,3)"]);

         // A delta whose values fit the seed's scale is inserted exactly.
         await wh.runSQL(
            `INSERT INTO t (id, g, u) VALUES (5, 'c', 0.25), (6, 'c', NULL)`,
         );
         await build();
         expect(await uType()).toEqual(["decimal(38,3)"]);
         expect(await sums(5)).toEqual([{ su: 1.555, c: 5 }]);

         // One that needs more digits is not rounded into the column: the
         // table re-seeds at the wider scale.
         await wh.runSQL(
            `INSERT INTO t (id, g, u) VALUES (7, 'c', 0.00001), (8, 'c', NULL)`,
         );
         await build();
         expect(await uType()).toEqual(["decimal(38,5)"]);
         expect(await sums(7)).toEqual([{ su: 1.55501, c: 7 }]);
      });

      it("re-seeds an incremental table a build before the precision fix stored as DOUBLE, to DECIMAL, equal to live", async () => {
         const [table] = (
            await lakeCatalog.runSQL(
               `SELECT row_to_json(q) AS "row" FROM (
                  SELECT s.schema_name, t.table_name
                  FROM ducklake_table t JOIN ducklake_schema s ON s.schema_id = t.schema_id
                  WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL
                    AND t.table_name LIKE 't_incremental%') q`,
            )
         ).rows as { schema_name: string; table_name: string }[];
         expect(table).toBeDefined();
         const uType = async () =>
            (await storedTypes()).get(`${table!.table_name}.u`) ?? [];
         // The table as a build before the fix left it: `u` as the
         // passthrough's DOUBLE, its boundary and rows otherwise unchanged.
         const duck = new DuckDBConnection("rewrite", ":memory:");
         try {
            for (const ext of ["ducklake", "postgres"]) {
               await duck.runSQL(`INSTALL ${ext}`);
               await duck.runSQL(`LOAD ${ext}`);
            }
            await duck.runSQL(
               `ATTACH 'ducklake:postgres:${catalogConnection}' AS old_build`,
            );
            const path = `old_build."${table!.schema_name}"."${table!.table_name}"`;
            await duck.runSQL(
               `CREATE OR REPLACE TABLE ${path} AS ` +
                  `SELECT * REPLACE (CAST(u AS DOUBLE) AS u) FROM ${path}`,
            );
         } finally {
            await duck.close();
         }
         expect(await uType()).toEqual(["float64"]);

         await wh.runSQL(
            `INSERT INTO t (id, g, u) VALUES (9, 'c', 0.1), (10, 'c', NULL)`,
         );
         await build();
         // The refresh re-seeded rather than inserting into the DOUBLE.
         expect(await uType()).toEqual(["decimal(38,5)"]);
         // The build log names the cause beside the seed's reason code.
         expect(server.output()).toContain(
            "Re-seeding to store Postgres numerics exactly",
         );
         const read = (s: string) =>
            query(
               `run: ${s} -> { where: id <= 9; aggregate: su is u.sum(), c is count() }`,
            );
         const live = await read("live_incremental");
         const served = await read("incremental");
         expect(served.servedBy).toBe("materialized");
         expect(served.rows).toEqual(live.rows);
         expect(live.rows).toEqual([{ su: 1.65501, c: 9 }]);

         // Re-seeded once: the next refresh advances by delta, with no
         // second re-seed and no Postgres probe for a DOUBLE it no longer has.
         const seeds = (out: string) =>
            out.split("Re-seeding to store Postgres numerics exactly").length;
         const before = server.output();
         await wh.runSQL(`INSERT INTO t (id, g, u) VALUES (11, 'c', 0.5)`);
         await build();
         const after = server.output().slice(before.length);
         expect(seeds(after)).toBe(1);
         expect(after).not.toContain(
            "Rebuilding an incremental source in full",
         );
         expect(after).toMatch(
            /materialization\.build\.finished[^\n]*"method":"delta"[^\n]*"physicalTable":"t_incremental/,
         );
         expect(await uType()).toEqual(["decimal(38,5)"]);
      });
   },
);
