// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

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
 * R4, served against live, for a `storage=` table built from Postgres columns
 * no stored declaration reads back as live does (known limitation 42):
 *
 * - `json`, `jsonb`, a composite type and `uuid`, which live types `sql
 *   native`: the destination stores them as text (a `uuid` as UUID), which
 *   the served copy could only declare `string`.
 * - `bytea` and `interval`, which Postgres's dialect types `string`, answered
 *   live in Postgres's own text (`\x0102`, `3 mons`): the build keeps the
 *   BLOB and INTERVAL the destination's session reads them as, rather than
 *   casting them to text DuckDB writes differently (`\x01\x02`, `3 months`).
 *
 * Each is left out of the served copy, so a query reading it is answered live
 * and equal to live (rows, text and field types), `select: *` is answered
 * live, and a query reading none of them is served from the table. Through a
 * real server, for a `#@ persist storage=` and a `#(materialize) storage=`
 * source.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

/** Columns left out: a query reading one answers live. */
const LEFT_OUT = ["j", "jb", "bin", "iv", "comp", "uu"];
/** Columns served as before: a query reading one reads the table. */
const SERVED = ["g", "n", "amount", "d", "ts"];
/** The persisted sources, each beside `live`, the same table unpersisted. */
const STORED_SOURCES = ["persisted", "materialized"];

const MODEL = [
   "##! experimental.persistence",
   "source: live is pg.table('public.t')",
   '#@ persist name="t_persisted" storage="lake"',
   "source: persisted is pg.table('public.t') -> { select: * }",
   '#(materialize) storage="lake" name="t_materialized"',
   "source: materialized is pg.table('public.t') extend { primary_key: id }",
   "",
].join("\n");

interface Answer {
   servedBy?: string;
   via?: string;
   fields: unknown;
   rows: unknown;
}

describe.skipIf(!url)(
   "a storage= table built from Postgres native-typed columns",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `native_wh_${token}`;
      const catalog = `native_lake_${token}`;
      let admin: PostgresConnection;
      let wh: PostgresConnection;
      let lakeCatalog: PostgresConnection;
      let server: SpawnedServer;
      const dirs: { cleanup(): void }[] = [];
      const api = () => `${server.baseUrl}/api/v0/environments/e1/packages/pa`;

      /**
       * A query's answer, with number subtypes (known limitation 12) and each
       * compile's own field annotations (a `reference_id`) left out.
       */
      async function query(malloy: string): Promise<Answer> {
         const res = await fetch(`${api()}/models/model.malloy/query`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ query: malloy }),
         });
         const body = (await res.json()) as {
            result?: string;
            message?: string;
            path?: { servedBy?: { kind?: string; via?: string } };
         };
         if (!body.result) throw new Error(`${malloy}: ${body.message}`);
         const result = JSON.parse(body.result, (key, value) =>
            key === "subtype" || key === "annotations" ? undefined : value,
         ) as { schema: { fields: unknown }; data: unknown };
         return {
            servedBy: body.path?.servedBy?.kind,
            via: body.path?.servedBy?.via,
            fields: result.schema.fields,
            rows: result.data,
         };
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
         await wh.runSQL(`CREATE TYPE pair AS (a int, b text)`);
         await wh.runSQL(
            `CREATE TABLE t (id int, g text, n int, amount numeric(10,2), d date, ts timestamptz,
               j json, jb jsonb, bin bytea, iv interval, comp pair, uu uuid)`,
         );
         await wh.runSQL(`INSERT INTO t VALUES
            (1, 'a', 10, 1.25, '2024-01-02', '2024-01-02 03:04:05+00',
               '{"n": 1}', '{"n": 1, "s": "x"}', '\\x0102', interval '1 day 2 hours',
               ROW(1,'p')::pair, '11111111-2222-3333-4444-555555555555'),
            (2, 'b', 20, 2.5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            (3, 'a', 30, 3.75, '2024-12-31', '2024-06-30 23:00:00+00',
               '[1,2]', '"str"', '\\x', interval '3 months',
               ROW(NULL,NULL)::pair, '21111111-2222-3333-4444-555555555555')`);
         const lake = tempDir("native-lake-");
         dirs.push(lake);
         const dir = tempDir("native-pkg-");
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
         const created = (await (
            await fetch(`${api()}/materializations`, {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: "{}",
            })
         ).json()) as { id: string };
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

      it("stores a bytea and an interval as the destination reads them, not as text", async () => {
         const stored = (
            await lakeCatalog.runSQL(
               `SELECT row_to_json(q) AS "row" FROM (
                  SELECT t.table_name, c.column_name, c.column_type
                  FROM ducklake_column c JOIN ducklake_table t ON t.table_id = c.table_id
                  WHERE c.end_snapshot IS NULL AND t.end_snapshot IS NULL
                    AND c.column_name IN ('bin', 'iv', 'g')) q`,
            )
         ).rows as {
            table_name: string;
            column_name: string;
            column_type: string;
         }[];
         const types = (column: string) =>
            [
               ...new Set(
                  stored
                     .filter((r) => r.column_name === column)
                     .map((r) => r.column_type),
               ),
            ].sort();
         expect(stored.length).toBe(6);
         expect(types("bin")).toEqual(["blob"]);
         expect(types("iv")).toEqual(["interval"]);
         expect(types("g")).toEqual(["varchar"]);
      });

      for (const source of STORED_SOURCES) {
         describe(source, () => {
            for (const column of LEFT_OUT) {
               it(`answers a query reading ${column} live, equal to live`, async () => {
                  const read = (s: string) =>
                     query(
                        `run: ${s} -> { select: id, ${column}; order_by: id }`,
                     );
                  const live = await read("live");
                  const served = await read(source);
                  expect(served.servedBy).toBe("live");
                  expect(served.fields).toEqual(live.fields);
                  expect(served.rows).toEqual(live.rows);
               });
            }

            it("answers select: * live, equal to live", async () => {
               const read = (s: string) =>
                  query(`run: ${s} -> { select: *; order_by: id }`);
               const live = await read("live");
               const served = await read(source);
               expect(served.servedBy).toBe("live");
               expect(served.fields).toEqual(live.fields);
               expect(served.rows).toEqual(live.rows);
            });

            it("serves a query reading none of them, equal to live", async () => {
               const read = (s: string) =>
                  query(
                     `run: ${s} -> { select: id, ${SERVED.join(", ")}; order_by: id }`,
                  );
               const live = await read("live");
               const served = await read(source);
               expect(served.servedBy).toBe("materialized");
               expect(served.via).toBe("served_copy");
               expect(served.fields).toEqual(live.fields);
               expect(served.rows).toEqual(live.rows);
               const grouped = (s: string) =>
                  query(
                     `run: ${s} -> { group_by: g; aggregate: c is count(), total is amount.sum(); order_by: g }`,
                  );
               const servedGroups = await grouped(source);
               expect(servedGroups.servedBy).toBe("materialized");
               expect(servedGroups.rows).toEqual((await grouped("live")).rows);
            });
         });
      }
   },
);
