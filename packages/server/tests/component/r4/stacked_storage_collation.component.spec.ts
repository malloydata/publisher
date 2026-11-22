// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` source stacked on a stored upstream in the same DuckLake
 * destination is built in DuckDB, which compares strings byte-exact; live
 * computes the same source on Postgres, under each column's collation. Where
 * the downstream groups, filters, counts distinct or orders a string whose
 * collation is not binary, a build over the upstream's table writes other
 * rows than live computes, and a query over the downstream that reads none
 * of those strings is served from that table. So such a build is taken from
 * the warehouse instead: every answer over the downstream equals live.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create and
 * drop databases on.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../src/test_helpers/temp_storage";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

setDefaultTimeout(600_000);

/** Each value, and each case-insensitive group, has its own count. */
const VALUES: [string, number][] = [
   ["apple", 3],
   ["Apple", 1],
   ["banana", 2],
   ["Banana", 4],
   ["cherry", 5],
];

function seed(): string[] {
   let id = 0;
   const rows = VALUES.flatMap(([v, n]) =>
      Array.from({ length: n }, () => `(${++id}, '${v}', ${id * 10})`),
   );
   return [
      `CREATE COLLATION ci_l2 (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`,
      `CREATE TABLE raw_items (id int, v text, amt int)`,
      `INSERT INTO raw_items VALUES ${rows.join(", ")}`,
      `CREATE TABLE items (id int, cat text, ci text COLLATE ci_l2, cc text COLLATE "C", amt int)`,
      `INSERT INTO items SELECT id, v, v, v, amt FROM raw_items`,
   ];
}

/**
 * `a` is the stored upstream; each `b_*` stacks on it, and so does each
 * rollup of `a` and `a2`. `cat` is under the database's `en_US` default,
 * `ci` a case-insensitive ICU collation, `cc` under `C`.
 */
const MODEL = `##! experimental { persistence }
#@ persist storage="lake"
source: a is pg.table('public.items') -> { select: id, cat, ci, cc, amt } extend {
  #@ preaggregate grain="ci" storage=lake
  measure: rn is count()
  #@ preaggregate grain="ci" storage=lake
  measure: apple_n is count() { where: ci = 'apple' }
  #@ preaggregate grain="ci" storage=lake
  measure: mn is min(cat)
  #@ preaggregate grain="ci" storage=lake
  measure: amt_sum is amt.sum()
}

#@ persist storage="lake"
source: a2 is pg.table('public.items') -> { select: id, cc, amt } extend {
  #@ preaggregate grain="cc" storage=lake
  measure: amt_total is amt.sum()
}

#@ persist storage="lake"
source: a3 is pg.table('public.items') -> { select: id, cc, ci } extend {
  #@ preaggregate grain="cc" storage=lake
  measure: apple3 is count() { where: ci = 'apple' }
}

#@ persist storage="lake"
source: b_mf is a -> { aggregate: n is count() { where: ci = 'apple' } }

#@ persist storage="lake"
source: b_having is a -> { group_by: cc; aggregate: n is count(); having: max(ci) = 'cherry' }

source: a_apple is a extend { where: ci = 'apple' }

source: a_joined is a extend {
  join_one: k is a_apple on id = k.id
}

#@ persist storage="lake"
source: b_join is a_joined -> { aggregate: n is k.count() }

#@ persist storage="lake"
source: b_nest is a -> {
  aggregate: total is count()
  nest: apples is { where: ci = 'apple'; aggregate: n is count() }
}

#@ persist storage="lake"
source: b_ext is a_apple -> { group_by: big is amt > 50; aggregate: n is count() }

#@ persist storage="lake"
source: b_group is a -> { group_by: ci; aggregate: n is count() }

#@ persist storage="lake"
source: b_where is a -> { where: ci = 'apple'; select: id, amt }

#@ persist storage="lake"
source: b_distinct is a -> { aggregate: d is count(ci) }

#@ persist storage="lake"
source: b_top is a -> { group_by: cat; aggregate: n is count(); order_by: cat; limit: 2 }

#@ persist storage="lake"
source: b_lower is a -> { group_by: l is lower(ci); aggregate: n is count() }

#@ persist storage="lake"
source: b_binary is a -> { group_by: cc; aggregate: n is count(); order_by: cc; limit: 2 }

// Unpersisted: answered by a served copy over the table of a, or live.
source: x_mf is a -> { aggregate: n is count() { where: ci = 'apple' } }
source: x_where is a -> { where: ci = 'apple'; aggregate: n is count() }
source: x_cat is a -> { aggregate: n is count() { where: cat > 'B' } }

#@ persist storage="lake"
source: b_number is a -> { group_by: big is amt > 50; aggregate: n is count() }
`;

type ServedBy = "materialized" | "rollup" | "live";

interface Shape {
   query: string;
   /** Who answers after the build, with live's rows. */
   servedBy: ServedBy;
}

const SHAPES: Shape[] = [
   // A query, or an unpersisted query-shaped source, comparing `ci` over
   // `a`'s table: live, each filtered measure's `{ where: }` judged as a
   // `where:` is (a served copy over the table compared it byte-exact).
   {
      query: "run: a -> { aggregate: n is count() { where: ci = 'apple' } }",
      servedBy: "live",
   },
   {
      query: "run: a -> { where: ci = 'apple'; aggregate: n is count() }",
      servedBy: "live",
   },
   { query: "run: x_mf -> { aggregate: t is sum(n) }", servedBy: "live" },
   { query: "run: x_where -> { aggregate: t is sum(n) }", servedBy: "live" },
   { query: "run: x_cat -> { aggregate: t is sum(n) }", servedBy: "live" },
   // Stacked builds that compare a non-binary string: built from the
   // warehouse instead, so the table holds live's rows and answers.
   {
      query: "run: b_group -> { aggregate: g is count() }",
      servedBy: "materialized",
   },
   {
      query: "run: b_group -> { group_by: n; aggregate: g is count(); order_by: n }",
      servedBy: "materialized",
   },
   {
      query: "run: b_where -> { aggregate: c is count() }",
      servedBy: "materialized",
   },
   {
      query: "run: b_distinct -> { aggregate: d is max(d) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_top -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_lower -> { aggregate: g is count() }",
      servedBy: "materialized",
   },
   {
      query: "run: b_mf -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_having -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_ext -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_join -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_nest -> { select: total, apples.n }",
      servedBy: "live",
   },
   // Stacked builds that compare a binary string, or none: still stacked.
   {
      query: "run: b_binary -> { aggregate: t is sum(n) }",
      servedBy: "materialized",
   },
   {
      query: "run: b_number -> { aggregate: t is count() }",
      servedBy: "materialized",
   },
   // Rollups: of `a` at grain `ci` built from the warehouse, of `a2` at `cc`
   // stacked.
   { query: "run: a -> { aggregate: rn }", servedBy: "rollup" },
   { query: "run: a -> { aggregate: apple_n }", servedBy: "rollup" },
   { query: "run: a -> { aggregate: amt_sum }", servedBy: "rollup" },
   { query: "run: a2 -> { aggregate: amt_total }", servedBy: "rollup" },
   { query: "run: a3 -> { aggregate: apple3 }", servedBy: "rollup" },
   { query: "run: a -> { aggregate: mn }", servedBy: "live" },
   { query: "run: a -> { group_by: ci; aggregate: rn }", servedBy: "live" },
];

/** Each stored table's build: stacked on its upstream, or from the warehouse. */
const BUILT: Record<string, "reused" | "warehouse"> = {
   b_group: "warehouse",
   b_where: "warehouse",
   b_distinct: "warehouse",
   b_top: "warehouse",
   b_lower: "warehouse",
   b_binary: "reused",
   b_number: "reused",
   b_mf: "warehouse",
   b_having: "warehouse",
   b_ext: "warehouse",
   b_join: "warehouse",
   b_nest: "warehouse",
   a3__rollup__cc: "warehouse",
   a__rollup__ci: "warehouse",
   a2__rollup__cc: "reused",
};

interface Answer {
   rows: unknown;
   servedBy: string;
}

type Outcome = { answer: Answer } | { error: string };

describe.skipIf(!POSTGRES_URL)(
   "a storage= source stacked on a stored upstream answers as live under the origin's collation",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `stack_coll_wh_${token}`;
      const catalog = `stack_coll_lake_${token}`;
      let admin: PostgresConnection;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment | undefined;
      let service: MaterializationService;
      let restore = () => {};
      let run: Materialization | undefined;
      const live = new Map<Shape, Outcome>();
      const served = new Map<Shape, Outcome>();

      async function ask({ query }: Shape): Promise<Outcome> {
         const pkg = await env!.getPackage(parsePackageName(PKG), false);
         try {
            const result = await pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            return {
               answer: {
                  rows: result.compactResult,
                  servedBy: result.path?.servedBy?.kind ?? "live",
               },
            };
         } catch (error) {
            return {
               error: error instanceof Error ? error.message : String(error),
            };
         }
      }

      async function build(
         options: Record<string, unknown> = {},
      ): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, options);
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         const deadline = Date.now() + 300_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return (await repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 50));
         }
      }

      /**
       * Each stored table's entry in the last run, by source name; a
       * rollup's by its name up to the grain (its digest varies).
       */
      function entries(): Record<string, Record<string, unknown>> {
         const manifest = run?.manifest as {
            entries?: Record<string, Record<string, unknown>>;
         };
         const out: Record<string, Record<string, unknown>> = {};
         for (const e of Object.values(manifest?.entries ?? {})) {
            const name = String(e.sourceName).replace(/__[0-9a-f]{8}$/, "");
            out[name] = e;
         }
         return out;
      }

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
            },
         }).restore;
         await freshLoadPool();
         ({ root, db, repo } = await openTempStorage("stack-coll-", {
            id: ENV_ID,
            name: ENV,
         }));
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         await admin.runSQL(
            `CREATE DATABASE ${warehouse} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
         );
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const seeder = new PostgresConnection({
            name: "seed",
            connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
         });
         for (const sql of seed()) await seeder.runSQL(sql);
         await seeder.close();

         const envPath = path.join(root, "env");
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
            {
               manifest: {
                  name: PKG,
                  description: "stacked storage collation",
               },
            },
         );
         const bucket = path.join(root, "bucket");
         fs.mkdirSync(bucket);
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "pg",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: warehouse },
               } as never,
            ],
            [
               {
                  name: "lake",
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: { ...pg, databaseName: catalog },
                     },
                     storage: { bucketUrl: bucket },
                  },
               } as never,
            ],
         );
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env!,
         );
         for (const s of SHAPES) live.set(s, await ask(s));
         run = await build();
         for (const s of SHAPES) served.set(s, await ask(s));
      });

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         if (db) await disposeTempStorage({ db, root });
         for (const name of [warehouse, catalog]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         restore();
         await freshLoadPool();
      });

      it("builds every stored table", () => {
         expect([run?.status, run?.error ?? null]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const manifest = run?.manifest as {
            failures?: Record<string, unknown>;
         };
         expect(manifest.failures ?? {}).toEqual({});
      });

      it("stacks only a build that compares no string its source compares otherwise", () => {
         const got = entries();
         const built: Record<string, string> = {};
         for (const name of Object.keys(BUILT)) {
            const entry = got[name];
            if (entry === undefined) throw new Error(`no entry for ${name}`);
            built[name] =
               entry.upstreamReuse === "reused" ? "reused" : "warehouse";
         }
         expect(built).toEqual(BUILT);
      });

      for (const s of SHAPES) {
         it(`${s.query}: ${s.servedBy}, equal to live as strings`, () => {
            const l = live.get(s)!;
            const got = served.get(s)!;
            if ("error" in l) throw new Error(`live failed: ${l.error}`);
            if ("error" in got) throw new Error(`served failed: ${got.error}`);
            expect(l.answer.servedBy).toBe("live");
            expect(JSON.stringify(got.answer.rows)).toBe(
               JSON.stringify(l.answer.rows),
            );
            expect(got.answer.servedBy).toBe(s.servedBy);
         });
      }
   },
);
