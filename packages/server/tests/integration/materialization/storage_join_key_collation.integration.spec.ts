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
 * A row scope that tests a string for null through a join on a string key,
 * served from `storage=` tables in a DuckLake destination, from Postgres.
 *
 * Under the warehouse's deterministic default collation the stored copy
 * matches the join's rows as live does (equality is byte equality), so the
 * null test is served from the tables: over a query-shaped source (gtme's
 * `pipeline_now`, formerly refused for collation) and a table-backed one.
 * Under a nondeterministic key collation (case-insensitive ICU), live
 * matches `D1` to `d1` and a byte-wise copy would not, so it is answered
 * live, equal to live (the table-backed shape used to be served wrong).
 *
 * Opt-in: PUBLISHER_TEST_POSTGRES_URL, a server the test may create and drop
 * databases on.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const DATA = `(VALUES
    ('{"id":"d1"}'::jsonb, 'won'),
    ('{"id":"D1"}'::jsonb, 'open'),
    ('{"id":"d2"}'::jsonb, 'open'),
    ('{"id":"d3"}'::jsonb, 'lost')
  ) AS t(data, stage)`;

const model = (ci: boolean, shape: "query" | "table") => {
   const key = ci ? "(data->>'id') COLLATE ci" : "data->>'id'";
   const base =
      shape === "query"
         ? `events -> { group_by: deal_id }`
         : `pg.sql("""SELECT ${ci ? "id COLLATE ci" : "id"} AS deal_id FROM (VALUES ('D1'), ('d2'), ('d3')) AS t(id)""")`;
   return `##! experimental.persistence
source: events is pg.sql("""SELECT ${key} AS deal_id, stage FROM ${DATA}""")

#(materialize) name="jk_first_close" storage="lake"
source: deal_first_close is events -> {
  where: stage = 'won' or stage = 'lost'
  group_by: deal_id, close_type is stage
}

#(materialize) name="jk_deals" storage="lake"
source: deals is ${base} extend {
  join_one: deal_first_close on deal_id = deal_first_close.deal_id
  dimension: is_open is deal_first_close.close_type is null
}

source: pipeline_now is deals extend { where: is_open }
source: funnel is deals extend { measure: c is count() }
`;
};

const QUERIES = {
   open: "run: pipeline_now -> { aggregate: n is count() }",
   all: "run: funnel -> { aggregate: c }",
};

for (const shape of ["query", "table"] as const) {
   for (const ci of [false, true]) {
      describe.skipIf(!url)(
         `a null test through a string join key, ${shape}-shaped, ${ci ? "case-insensitive" : "deterministic"} key`,
         () => {
            const token = Math.random().toString(36).slice(2, 8);
            const warehouse = `jk_wh_${token}`;
            const catalog = `jk_lake_${token}`;
            let admin: PostgresConnection;
            let server: SpawnedServer;
            const dirs: { cleanup(): void }[] = [];
            const live: Record<string, { kind?: string; rows?: string }> = {};
            const api = () =>
               `${server.baseUrl}/api/v0/environments/e1/packages/pa`;
            async function query(malloy: string) {
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
               return { kind: body.path?.servedBy?.kind, rows: body.result };
            }

            beforeAll(async () => {
               admin = new PostgresConnection({
                  name: "admin",
                  connectionString: url!,
               });
               await admin.runSQL(
                  `CREATE DATABASE ${warehouse} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
               );
               await admin.runSQL(`CREATE DATABASE ${catalog}`);
               const parsed = new URL(url!);
               const pg = {
                  host: parsed.hostname,
                  port: Number(parsed.port || 5432),
                  userName: decodeURIComponent(parsed.username),
                  password: decodeURIComponent(parsed.password),
               };
               if (ci) {
                  const c = new PostgresConnection({
                     name: "c",
                     connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
                  });
                  await c.runSQL(
                     "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
                  );
                  await c.close();
               }
               const lake = tempDir("jk-lake-");
               dirs.push(lake);
               const dir = tempDir("jk-pkg-");
               dirs.push(dir);
               fs.writeFileSync(
                  path.join(dir.dir, "publisher.json"),
                  JSON.stringify({
                     name: "pa",
                     version: "1.0.0",
                     description: "x",
                  }),
               );
               fs.writeFileSync(
                  path.join(dir.dir, "model.malloy"),
                  model(ci, shape),
               );
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
               for (const [n, q] of Object.entries(QUERIES))
                  live[n] = await query(q);
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
               for (const db of [warehouse, catalog]) {
                  await admin
                     ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                     .catch(() => undefined);
               }
               await admin?.close().catch(() => undefined);
               for (const d of dirs) d.cleanup();
            });

            it(`answers the null test ${ci ? "live" : "from the tables"}, equal to live`, async () => {
               const served = await query(QUERIES.open);
               expect(served.rows).toBe(live.open.rows);
               expect(served.kind).toBe(ci ? "live" : "materialized");
            });

            it("serves a query that reads no join from the table", async () => {
               const served = await query(QUERIES.all);
               expect(served.rows).toBe(live.all.rows);
               expect(served.kind).toBe("materialized");
            });
         },
      );
   }
}
