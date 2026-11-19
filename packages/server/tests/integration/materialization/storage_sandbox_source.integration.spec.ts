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
 * A `storage=` source on the package's DuckDB sandbox (ms2data/service#7640).
 * A `storage=` build passes a query through to the source's warehouse, and
 * cannot read the sandbox at all. Every run used to fail such a source with
 * "Connection duckdb not found", and a run with nothing else to build failed
 * whole. Now it is refused at plan time on every route: the package loads,
 * the source is not built and is answered live, the route's `*-not-built`
 * warning names the connection, and a run builds its Postgres siblings.
 *
 * Through a real server, with a Postgres warehouse and a DuckLake destination
 * whose catalog is on the same server.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const SANDBOX = `duckdb.sql("""
  SELECT 1 AS id, 'a' AS g, 10 AS amount
  UNION ALL SELECT 2, 'b', 20
  UNION ALL SELECT 3, 'a', 30
""")`;

const MODEL = `##! experimental.persistence
source: pg_orders is pg.table('public.orders')
source: sb_orders is ${SANDBOX} extend { primary_key: id }

#@ persist name="pg_stored" storage="lake"
source: pg_stored is pg_orders -> { group_by: g; aggregate: total is amount.sum() }

#@ persist name="sb_stored" storage="lake"
source: sb_stored is sb_orders -> { group_by: g; aggregate: total is amount.sum() }

#(materialize) storage="lake" name="sb_materialized"
source: sb_materialized is sb_orders extend { measure: total is amount.sum() }

source: sb_viewed is sb_orders extend {
  measure: total is amount.sum()
  #@ persist storage="lake"
  view: by_g is { group_by: g; aggregate: total }
}

#@ persist storage="lake"
query: sb_query is sb_orders -> { group_by: g; aggregate: total is amount.sum() }

source: sb_rolled is sb_orders extend {
  #(rollup) grain=[g] storage="lake"
  measure: total is amount.sum()
}

#@ persist name="sb_database"
source: sb_database is sb_orders -> { group_by: g; aggregate: total is amount.sum() }
`;

interface Warning {
   code?: string;
   subject?: string;
   severity?: string;
   message: string;
}

interface Answer {
   servedBy?: string;
   rows: unknown;
}

describe.skipIf(!url)(
   "a storage= source on the package's DuckDB sandbox",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `sbx_wh_${token}`;
      const catalog = `sbx_lake_${token}`;
      let admin: PostgresConnection;
      let wh: PostgresConnection;
      let server: SpawnedServer;
      const dirs: { cleanup(): void }[] = [];
      let warnings: Warning[] = [];
      let run: {
         status: string;
         error?: string | null;
         manifest?: {
            entries?: Record<string, { sourceName?: string }>;
            failures?: Record<string, unknown>;
         };
         metadata?: {
            refusedSources?: { name: string; reason?: string }[];
         };
      };
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
            path?: { servedBy?: { kind?: string } };
         };
         if (!body.result) throw new Error(`${malloy}: ${body.message}`);
         return {
            servedBy: body.path?.servedBy?.kind,
            rows: JSON.parse(body.result),
         };
      }

      beforeAll(async () => {
         admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         // A binary (`C`) warehouse: a `storage=` copy orders and ranges a
         // string as live does only under a binary collation, so a string
         // order here serves (the server default, often `en_US`, answers it live).
         await admin.runSQL(
            `CREATE DATABASE ${warehouse} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
         );
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         const parsed = new URL(url!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         wh = new PostgresConnection({
            name: "wh",
            connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
         });
         await wh.runSQL(`CREATE TABLE orders (id int, g text, amount int)`);
         await wh.runSQL(
            `INSERT INTO orders VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)`,
         );
         const lake = tempDir("sbx-lake-");
         dirs.push(lake);
         const dir = tempDir("sbx-pkg-");
         dirs.push(dir);
         fs.writeFileSync(
            path.join(dir.dir, "publisher.json"),
            JSON.stringify({ name: "pa", version: "1.0.0", description: "x" }),
         );
         fs.writeFileSync(path.join(dir.dir, "model.malloy"), MODEL);
         // A second package whose `storage=` source is on a DuckLake
         // connection to the destination's own catalog: admitted at plan.
         const lakePkg = tempDir("sbx-lakepkg-");
         dirs.push(lakePkg);
         fs.writeFileSync(
            path.join(lakePkg.dir, "publisher.json"),
            JSON.stringify({ name: "pl", version: "1.0.0", description: "x" }),
         );
         fs.writeFileSync(
            path.join(lakePkg.dir, "model.malloy"),
            [
               "##! experimental.persistence",
               '#@ persist storage="lake"',
               'source: lk_stored is lk.sql("SELECT 1 AS x") -> { select: x }',
               "",
            ].join("\n"),
         );
         const lakeCatalog = {
            catalog: {
               postgresConnection: { ...pg, databaseName: catalog },
            },
            storage: { bucketUrl: lake.dir },
         };
         server = await spawnServer({
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: "e1",
                     packages: [
                        { name: "pa", location: dir.dir },
                        { name: "pl", location: lakePkg.dir },
                     ],
                     connections: [
                        {
                           name: "pg",
                           type: "postgres",
                           postgresConnection: {
                              ...pg,
                              databaseName: warehouse,
                           },
                        },
                        {
                           name: "lk",
                           type: "ducklake",
                           ducklakeConnection: {
                              ...lakeCatalog,
                              readOnly: false,
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
         const pkg = (await (await fetch(api())).json()) as {
            warnings?: Warning[];
         };
         warnings = pkg.warnings ?? [];
         const created = (await (
            await fetch(`${api()}/materializations`, {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: "{}",
            })
         ).json()) as { id: string };
         run = await pollFor(
            async () =>
               (await (
                  await fetch(`${api()}/materializations/${created.id}`)
               ).json()) as typeof run,
            (m) => TERMINAL_STATUSES.includes(m.status),
            "the build did not settle",
            120_000,
         );
      });

      afterAll(async () => {
         await server?.stop();
         await wh?.close().catch(() => undefined);
         for (const db of [warehouse, catalog]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         for (const d of dirs) d.cleanup();
      });

      it("loads, with each route's not-built warning naming the connection", () => {
         const refused = (code: string, subject: RegExp) =>
            warnings.find(
               (w) =>
                  w.code === code &&
                  subject.test(w.subject ?? "") &&
                  w.message.includes("cannot be passed through"),
            );
         for (const [code, subject] of [
            ["persist-not-built", /^sb_stored$/],
            ["materialize-not-built", /^sb_materialized$/],
            ["persist-view-not-built", /by_g/],
            ["persist-query-not-built", /sb_query/],
            ["rollup-not-built", /sb_rolled/],
         ] as const) {
            const warning = refused(code, subject);
            expect(warning, `${code} ${subject}`).toBeDefined();
            expect(warning?.severity).toBe("warn");
            expect(warning?.message).toContain("connection 'duckdb'");
            expect(warning?.message).toContain("Drop 'storage='");
         }
         // The Postgres sibling and the database sandbox source are not refused.
         expect(
            warnings.filter(
               (w) =>
                  /pg_stored|sb_database/.test(w.subject ?? "") &&
                  w.code?.endsWith("-not-built"),
            ),
         ).toEqual([]);
      });

      it("admits a storage= source on a DuckLake connection to the destination's own catalog", async () => {
         const plan = (
            (await (
               await fetch(
                  `${server.baseUrl}/api/v0/environments/e1/packages/pl`,
               )
            ).json()) as {
               buildPlan?: {
                  sources?: Record<string, { name: string }>;
                  refusedSources?: Record<string, { reason?: string }>;
               };
            }
         ).buildPlan;
         expect(Object.values(plan?.sources ?? {}).map((s) => s.name)).toEqual([
            "lk_stored",
         ]);
         expect(Object.values(plan?.refusedSources ?? {})).toEqual([]);
      });

      it("builds the Postgres sibling and the database source, and fails nothing", () => {
         expect(run.status).toBe("MANIFEST_FILE_READY");
         expect(run.error ?? null).toBeNull();
         expect(run.manifest?.failures ?? {}).toEqual({});
         const built = Object.values(run.manifest?.entries ?? {}).map(
            (e) => e.sourceName,
         );
         expect(built).toContain("pg_stored");
         expect(built).toContain("sb_database");
         expect(built.some((name) => name?.startsWith("sb_stored"))).toBe(
            false,
         );
      });

      it("serves the Postgres sibling from its stored table", async () => {
         const served = await query(
            "run: pg_stored -> { select: *; order_by: g }",
         );
         expect(served.servedBy).toBe("materialized");
         expect(served.rows).toEqual([
            { g: "a", total: 40 },
            { g: "b", total: 20 },
         ]);
      });

      it("answers each refused sandbox source live", async () => {
         const expected = [
            { g: "a", total: 40 },
            { g: "b", total: 20 },
         ];
         for (const malloy of [
            "run: sb_stored -> { select: *; order_by: g }",
            "run: sb_materialized -> { group_by: g; aggregate: total; order_by: g }",
            "run: sb_viewed -> by_g -> { select: *; order_by: g }",
            "run: sb_rolled -> { group_by: g; aggregate: total; order_by: g }",
         ]) {
            const answer = await query(malloy);
            expect(answer.servedBy, malloy).toBe("live");
            expect(answer.rows, malloy).toEqual(expected);
         }
      });
   },
);
