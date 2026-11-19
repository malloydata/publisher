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
 * A database `#@ persist` source with its own row filter over a column its
 * own query stage computes (`tenant is concat(category, '-x')`, filtered by
 * `tenant in $GROUPS`), through a real server on a real Postgres: the shape
 * 0.15.0 answered live, with nothing in the path report, while 0.14.0 served
 * it from its table. The stage ran in the build, in the source's own
 * warehouse as live runs it, so the read compares only the stored `tenant`
 * column, by equality: served under the database's (deterministic) default
 * collation, equal to live for every caller.
 *
 * The controls answer live, and say why: the same shape over a column whose
 * collation is case-insensitive (an ICU nondeterministic collation, which
 * the stored column keeps), and a stage computed when the table is read (a
 * query-shaped source over a stored one), over that column.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop a database on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(300_000);

const MODEL = `##! experimental.persistence
##! experimental.givens

given:
  GROUPS :: string[]

source: raw_orders is pg.table('public.orders')
source: raw_ci is pg.table('public.orders_ci')

// The regression: an own row filter over a column the stage computes.
#(access_filter) tenant in $GROUPS
#@ persist name="gated_orders"
source: gated_orders is raw_orders -> {
  group_by:
    category
    tenant is concat(category, '-x')
  aggregate: total_orders is count()
}

// Its live twin: the same text, never persisted.
#(access_filter) tenant in $GROUPS
source: live_orders is raw_orders -> {
  group_by:
    category
    tenant is concat(category, '-x')
  aggregate: total_orders is count()
}

// The same shape over a case-insensitive column: the stored tenant keeps it.
#(access_filter) tenant in $GROUPS
#@ persist name="gated_ci"
source: gated_ci is raw_ci -> {
  group_by:
    category
    tenant is concat(category, '-x')
  aggregate: total_orders is count()
}

#(access_filter) tenant in $GROUPS
source: live_ci is raw_ci -> {
  group_by:
    category
    tenant is concat(category, '-x')
  aggregate: total_orders is count()
}

// A stage computed when the table is read: over a stored base.
#@ persist name="base_ci"
source: base_ci is raw_ci -> { group_by: category; aggregate: n is count() }

#(access_filter) tenant in $GROUPS
source: over_ci is base_ci -> {
  group_by: tenant is concat(category, '-x')
  aggregate: total is n.sum()
}

#(access_filter) tenant in $GROUPS
source: over_ci_live is raw_ci -> { group_by: category; aggregate: n is count() } -> {
  group_by: tenant is concat(category, '-x')
  aggregate: total is n.sum()
}
`;

const CALLERS: string[][] = [
   ["books-x"],
   ["electronics-x", "books-x"],
   ["Books-x"],
   ["nobody-x"],
];

interface QueryAnswer {
   rows: Record<string, unknown>[];
   path: {
      servedBy: { kind: string; tier?: string; tableId?: string };
      considered: { tableId?: string; outcome: string; reason?: string }[];
   };
}

describe.skipIf(!url)(
   "a gated database #@ persist over a column its own stage computes, on Postgres",
   () => {
      const db = `gps_${Math.random().toString(36).slice(2, 8)}`;
      let admin: PostgresConnection;
      let server: SpawnedServer;
      const dirs: { cleanup(): void }[] = [];
      const api = () => `${server.baseUrl}/api/v0/environments/e1/packages/pa`;

      async function query(q: string, groups: string[]): Promise<QueryAnswer> {
         const res = await fetch(`${api()}/models/model.malloy/query`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               query: q,
               compactJson: true,
               givens: { GROUPS: groups },
            }),
         });
         const body = (await res.json()) as {
            result: string;
            path: QueryAnswer["path"];
         };
         expect(res.status).toBe(200);
         return { rows: JSON.parse(body.result), path: body.path };
      }

      beforeAll(async () => {
         admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         await admin.runSQL(`CREATE DATABASE ${db}`);
         const parsed = new URL(url!);
         const wh = new PostgresConnection({
            name: "wh",
            connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${db}`,
         });
         const rows =
            "(1,'electronics'),(2,'books'),(3,'books'),(4,'Books'),(5,'toys')";
         await wh.runSQL(
            `CREATE TABLE orders AS SELECT * FROM (VALUES ${rows}) AS t(id, category)`,
         );
         await wh.runSQL(
            "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
         );
         await wh.runSQL(
            `CREATE TABLE orders_ci AS SELECT id, category COLLATE ci AS category FROM (VALUES ${rows}) AS t(id, category)`,
         );
         await wh.close();
         const pkg = tempDir("gps-pa-");
         dirs.push(pkg);
         fs.writeFileSync(
            path.join(pkg.dir, "publisher.json"),
            JSON.stringify({ name: "pa", version: "1.0.0", description: "x" }),
         );
         fs.writeFileSync(path.join(pkg.dir, "model.malloy"), MODEL);
         const root = tempDir("gps-root-");
         dirs.push(root);
         server = await spawnServer({
            serverRoot: root.dir,
            config: {
               frozenConfig: false,
               environments: [
                  {
                     name: "e1",
                     packages: [{ name: "pa", location: pkg.dir }],
                     connections: [
                        {
                           name: "pg",
                           type: "postgres",
                           postgresConnection: {
                              host: parsed.hostname,
                              port: Number(parsed.port || 5432),
                              userName: decodeURIComponent(parsed.username),
                              password: decodeURIComponent(parsed.password),
                              databaseName: db,
                           },
                        },
                     ],
                  },
               ],
            },
            env: { PUBLISHER_MATERIALIZE: "true" },
         });
         await server.waitUntilServing();
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
                  metadata?: Record<string, unknown> | null;
               },
            (m) =>
               TERMINAL_STATUSES.includes(m.status) &&
               m.metadata?.autoLoad !== undefined,
            "the build did not settle",
            120_000,
         );
         expect(`${run.status} ${run.error ?? ""}`).toBe(
            "MANIFEST_FILE_READY ",
         );
      });

      afterAll(async () => {
         await server?.stop();
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
            .catch(() => undefined);
         await admin?.close().catch(() => undefined);
         for (const dir of dirs) dir.cleanup();
      });

      const sel = (s: string, cols: string) =>
         `run: ${s} -> { select: ${cols}; order_by: tenant }`;

      it("serves the gated source from its table, equal to live, for every caller", async () => {
         for (const groups of CALLERS) {
            const served = await query(
               sel("gated_orders", "tenant, total_orders"),
               groups,
            );
            const live = await query(
               sel("live_orders", "tenant, total_orders"),
               groups,
            );
            expect(served.rows, JSON.stringify(groups)).toEqual(live.rows);
            expect(served.path.servedBy).toMatchObject({
               kind: "materialized",
               tier: "database",
            });
            expect(served.path.considered).toEqual([
               expect.objectContaining({
                  tableId: served.path.servedBy.tableId,
                  outcome: "used",
               }),
            ]);
         }
         // The filter applied: one caller sees only its tenant.
         const books = await query(
            sel("gated_orders", "tenant, total_orders"),
            ["books-x"],
         );
         expect(books.rows).toEqual([{ tenant: "books-x", total_orders: 2 }]);
      });

      it("serves from a case-insensitive stored column that keeps its source's collation", async () => {
         // The table was built in the source's own warehouse, so its column
         // keeps the source's case-insensitive collation: the gate compares
         // there as live does.
         for (const groups of CALLERS) {
            const got = await query(
               sel("gated_ci", "tenant, total_orders"),
               groups,
            );
            const live = await query(
               sel("live_ci", "tenant, total_orders"),
               groups,
            );
            expect(got.rows, JSON.stringify(groups)).toEqual(live.rows);
            expect(got.path.servedBy.kind).toBe("materialized");
         }
      });

      it("answers live for a stage computed when the table is read", async () => {
         for (const groups of CALLERS) {
            const got = await query(sel("over_ci", "tenant, total"), groups);
            const live = await query(
               sel("over_ci_live", "tenant, total"),
               groups,
            );
            expect(got.rows, JSON.stringify(groups)).toEqual(live.rows);
            expect(got.path.servedBy.kind).toBe("live");
         }
      });
   },
);
