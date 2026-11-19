// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` copy of a Postgres source compares strings byte-exact; the
 * source compares each string column under its own collation. Under any
 * collation but a binary one (`C`, `POSIX`, the Postgres 17+ builtin
 * provider) an ordering differs: an `order_by` of a string, Malloy's
 * implicit ordering of a `group_by` with no measure, a top-N, a window
 * ordered by a string, a `min`, a range comparison, and each of those over
 * a string derived from the column. Under a nondeterministic collation (a
 * case-insensitive ICU one) grouping, distinct counting, equality and a
 * join on the string differ too. Under a deterministic one (the `en_US`
 * default, an ICU locale) they do not: equality is byte equality.
 *
 * Through the real package path (Environment, Package, the
 * MaterializationService's build into a DuckLake destination on a Postgres
 * catalog, and its bind), each query is answered live before the build and
 * again after it: every answer must equal live's exactly, and must have been
 * answered as the shape says: from the served copy, a rollup, a persisted
 * view or a persisted named query, or declined (with the path report's
 * code) and answered live.
 *
 * The Postgres half is opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server
 * the test may create and drop databases on, e.g.
 * `postgresql://postgres:pw@127.0.0.1:5432/postgres`. The DuckDB half always
 * runs: a DuckDB source's column under `NOCASE` is judged by its instance,
 * as its filters are, and a byte-exact one is served.
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
import {
   freshLoadPool,
   LAKE,
   loadFixture,
   type Outcome as FixtureOutcome,
} from "../../../src/test_helpers/conformance/package_runner";
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

/**
 * Each string column holds the same values: `cat` under the database's
 * `en_US` (libc) default, `icu` under a deterministic ICU collation, `ci`
 * under a nondeterministic case-insensitive one, `cc` under `C`, and (on
 * Postgres 17+) `bc` under the builtin `pg_c_utf8`. Byte order puts every
 * capital first (`Apple`, `Banana`, `apple`, …); each collation but `C` and
 * the builtin interleaves them (`apple`, `Apple`, `banana`, …). Each value
 * has its own count, and so has each case-insensitive group, so an order by
 * a count has no ties.
 */
const VALUES: [string, number][] = [
   ["apple", 3],
   ["Apple", 1],
   ["banana", 2],
   ["Banana", 4],
   ["cherry", 5],
];

function seed(builtin: boolean): string[] {
   let id = 0;
   const rows = VALUES.flatMap(([v, n]) =>
      Array.from({ length: n }, () => `(${++id}, '${v}', ${id * 10})`),
   );
   return [
      `CREATE COLLATION ci_l2 (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`,
      `CREATE TABLE raw_items (id int, v text, amt int)`,
      `INSERT INTO raw_items VALUES ${rows.join(", ")}`,
      `CREATE TABLE items (id int, cat text, icu text COLLATE "und-x-icu", ci text COLLATE ci_l2, cc text COLLATE "C",${builtin ? ` bc text COLLATE pg_c_utf8,` : ""} amt int)`,
      `INSERT INTO items SELECT id, v, v, v, v,${builtin ? " v," : ""} amt FROM raw_items`,
      `ALTER TABLE items ADD COLUMN cu text COLLATE "C.utf8"`,
      `UPDATE items SET cu = cat`,
      `CREATE TABLE kinds2 (kname2 text, w2 int)`,
      `INSERT INTO kinds2 VALUES ('apple', 1), ('Apple', 2), ('banana', 3), ('Banana', 4), ('cherry', 5)`,
      `CREATE TABLE kinds (kname text COLLATE ci_l2, w int)`,
      `INSERT INTO kinds VALUES ('apple', 10), ('banana', 20), ('cherry', 30)`,
   ];
}

const MODEL = `##! experimental { persistence }
source: items_src is pg.table('public.items') extend {
  primary_key: id
  measure: n is count()
}

#(materialize) storage="lake"
source: kinds is pg.table('public.kinds') extend { primary_key: kname }

#(materialize) storage="lake"
source: kinds2 is pg.table('public.kinds2') extend { primary_key: kname2 }

#(materialize) storage="lake"
source: m is items_src extend {
  join_one: k is kinds on ci = k.kname
  join_one: k2 is kinds2 on cat = k2.kname2
  dimension: lc is concat(cat, '!')
  dimension: side is pick 'hi' when cat > 'b' else 'lo'
  dimension: cst is cast(amt - 75 as string)
  #@ persist name="v_by_cat_ordered" storage="lake"
  view: by_cat_ordered is { group_by: cat; aggregate: n; order_by: cat }
  #@ persist name="v_by_cat_counted" storage="lake"
  view: by_cat_counted is { group_by: cat; aggregate: n }
}

source: r is items_src extend {
  #(rollup) grain=["cat"] storage="lake"
  measure: rn is count()
}

#@ persist name="q_by_cat" storage="lake"
query: q_by_cat is items_src -> { group_by: cat; aggregate: n }
`;

type Served = "materialized" | "rollup" | "view" | "query";

interface Shape {
   name: string;
   query: string;
   /**
    * Served from this leg, declined with this code and answered live, or
    * refused by live and by the copy alike.
    */
   want: { served: Served } | { live: string } | { error: true };
   /** Only on a server with the builtin provider (Postgres 17+). */
   builtin?: true;
}

const DECLINED = "NOT_SERVED";

const SHAPES: Shape[] = [
   // Under en_US (libc), the managed-Postgres default: the order differs.
   {
      name: "en_US: a group_by alone, in Malloy's implicit order",
      query: "run: m -> { group_by: cat }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: an explicit order_by",
      query: "run: m -> { group_by: cat; aggregate: n; order_by: cat }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a select ordered descending",
      query: "run: m -> { select: cat, id; order_by: cat desc, id }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a top-N by the string",
      query: "run: m -> { group_by: cat; aggregate: n; order_by: cat; limit: 2 }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: min of the string",
      query: "run: m -> { aggregate: lo is min(cat) }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a window ordered by the string",
      query: "run: m -> { group_by: cat; aggregate: n; calculate: r is rank() { order_by: cat } }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a range comparison inside a dimension",
      query: "run: m -> { group_by: side; aggregate: n }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a derived string, ordered",
      query: "run: m -> { group_by: lc }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a nest's rows, ordered",
      query: "run: m -> { aggregate: n; nest: cats is { group_by: cat } }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a later stage ordered by the string",
      query: "run: m -> { group_by: cat; aggregate: n } -> { select: cat, n; order_by: cat }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a window min over the string",
      query: "run: m -> { group_by: cat; aggregate: n; calculate: w is min_window(cat) }",
      want: { live: DECLINED },
   },
   {
      name: "en_US: a cumulative min over the string",
      query: "run: m -> { group_by: cat; aggregate: n; calculate: w is min_cumulative(cat) }",
      want: { live: DECLINED },
   },
   {
      name: "en_US default: a string made from a number, ordered",
      query: "run: m -> { group_by: cst }",
      want: { live: DECLINED },
   },
   // A value made from literals takes the database's default collation,
   // whatever column a `when:`, a join or a length reads (Postgres's
   // `pg_collation_for` says "default" of each).
   {
      name: "default: max of a pick of literals chosen by a C column",
      query: "run: m -> { aggregate: x is max(concat(pick 'a' when cc > 'b' else 'B', '')) }",
      want: { live: DECLINED },
   },
   {
      name: "default: max of a pick of literals chosen through a join",
      query: "run: m -> { aggregate: x is max(concat(pick 'a' when k2.w2 > 2 else 'B', '')) }",
      want: { live: DECLINED },
   },
   {
      name: "default: a range over a pick of literals, grouped",
      query: "run: m -> { group_by: g is concat(pick 'a' when cc > 'b' else 'B', '') > 'M'; aggregate: n }",
      want: { live: DECLINED },
   },
   {
      name: "default: a range over a coalesce of literals, grouped",
      query: "run: m -> { group_by: g is coalesce(pick 'a' when cc = 'apple' else null, 'B') > 'M'; aggregate: n }",
      want: { live: DECLINED },
   },
   {
      name: "default: a C column's length, as a string, ordered",
      query: "run: m -> { group_by: s is cast(length(cc) - 6 as string) }",
      want: { live: DECLINED },
   },
   {
      name: "default: a C column's strpos, as a string, ordered",
      query: "run: m -> { select: id, s is cast(strpos(cc, 'a') - 2 as string); order_by: s, id }",
      want: { live: DECLINED },
   },
   // Two columns' collations in one value: live refuses, and so does the copy.
   {
      name: "C and C.utf8 compared",
      query: "run: m -> { group_by: e is cc = cu; aggregate: n }",
      want: { error: true },
   },
   {
      name: "C and C.utf8 concatenated, ordered",
      query: "run: m -> { group_by: s is concat(cc, cu) }",
      want: { error: true },
   },
   // ...and what still serves under en_US: equality is byte equality.
   {
      name: "C: a C column with a literal, ordered by the column's collation",
      query: "run: m -> { group_by: c is concat(cc, '!') }",
      want: { served: "materialized" },
   },
   {
      name: "C.utf8: a group_by alone, in Malloy's implicit order",
      query: "run: m -> { group_by: cu }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: the longest string's length",
      query: "run: m -> { aggregate: m is max(length(cat)) }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: a group_by ordered by its measure",
      query: "run: m -> { group_by: cat; aggregate: n }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: a top-N by its measure",
      query: "run: m -> { group_by: cat; aggregate: n; limit: 2 }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: a count",
      query: "run: m -> { aggregate: n }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: a distinct count",
      query: "run: m -> { aggregate: d is count(cat) }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: an equality, grouped",
      query: "run: m -> { group_by: is_apple is cat = 'apple'; aggregate: n }",
      want: { served: "materialized" },
   },
   {
      name: "en_US: a numeric order",
      query: "run: m -> { select: id, cat, amt; order_by: amt desc }",
      want: { served: "materialized" },
   },
   // Under a deterministic ICU collation: the order differs, groups do not.
   {
      name: "ICU: a group_by alone, in Malloy's implicit order",
      query: "run: m -> { group_by: icu }",
      want: { live: DECLINED },
   },
   {
      name: "ICU: a group_by ordered by its measure",
      query: "run: m -> { group_by: icu; aggregate: n }",
      want: { served: "materialized" },
   },
   // Under a nondeterministic collation: groups and equality differ too.
   {
      name: "nondeterministic: a group_by ordered by its measure",
      query: "run: m -> { group_by: ci; aggregate: n }",
      want: { live: DECLINED },
   },
   {
      name: "nondeterministic: a distinct count",
      query: "run: m -> { aggregate: d is count(ci) }",
      want: { live: DECLINED },
   },
   {
      name: "nondeterministic: an equality, grouped",
      query: "run: m -> { group_by: is_apple is ci = 'apple'; aggregate: n }",
      want: { live: DECLINED },
   },
   {
      name: "nondeterministic: nullif, an equality",
      query: "run: m -> { aggregate: x is sum(pick 1 when nullif(ci, 'apple') = null else 0) }",
      want: { live: DECLINED },
   },
   {
      name: "nondeterministic: grouped by the string's length",
      query: "run: m -> { group_by: l is length(ci); aggregate: n }",
      want: { served: "materialized" },
   },
   {
      name: "nondeterministic: a join on the string",
      query: "run: m -> { group_by: w is k.w; aggregate: n }",
      want: { live: DECLINED },
   },
   // Binary collations: everything serves.
   {
      name: "C: a group_by alone, in Malloy's implicit order",
      query: "run: m -> { group_by: cc }",
      want: { served: "materialized" },
   },
   {
      name: "C: min of the string",
      query: "run: m -> { aggregate: lo is min(cc) }",
      want: { served: "materialized" },
   },
   {
      name: "builtin pg_c_utf8: a group_by alone, in Malloy's implicit order",
      query: "run: m -> { group_by: bc }",
      want: { served: "materialized" },
      builtin: true,
   },
   // A rollup, a persisted view and a persisted named query.
   {
      name: "rollup: ordered by its measure",
      query: "run: r -> { group_by: cat; aggregate: rn }",
      want: { served: "rollup" },
   },
   {
      name: "rollup: ordered by the string",
      query: "run: r -> { group_by: cat; aggregate: rn; order_by: cat }",
      want: { live: "ROW_LEVEL_GATE" },
   },
   {
      name: "persisted view: ordered by its measure",
      query: "run: m -> by_cat_counted",
      want: { served: "view" },
   },
   {
      name: "persisted view: ordered by the string",
      query: "run: m -> by_cat_ordered",
      want: { live: "MISSING_FIELD" },
   },
   {
      name: "persisted view: a later stage ordered by the string",
      query: "run: m -> by_cat_counted -> { select: cat, n; order_by: cat }",
      want: { live: "MISSING_FIELD" },
   },
   {
      name: "persisted query: ordered by its measure",
      query: "run: q_by_cat",
      want: { served: "query" },
   },
   {
      name: "persisted query: a later stage ordered by the string",
      query: "run: q_by_cat -> { select: cat, n; order_by: cat }",
      want: { live: "MISSING_FIELD" },
   },
];

interface Answer {
   rows: unknown;
   servedBy: string;
   /** Each candidate the router declined, by its code. */
   declined: string[];
}

type Outcome = { answer: Answer } | { error: string };

describe.skipIf(!POSTGRES_URL)(
   "a storage= copy answers a string's order, groups and equality as its Postgres source does",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `str_order_wh_${token}`;
      const catalog = `str_order_lake_${token}`;
      let admin: PostgresConnection;
      let builtin = false;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment | undefined;
      let service: MaterializationService;
      let restore = () => {};
      let run: Materialization | undefined;
      /** How the warehouse orders each column, against byte order. */
      const orders: Record<string, { collated: string[]; bytes: string[] }> =
         {};
      const live = new Map<Shape, Outcome>();
      const served = new Map<Shape, Outcome>();
      const shapes = () => SHAPES.filter((s) => !s.builtin || builtin);

      async function ask(query: string): Promise<Outcome> {
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
                  declined: (result.path?.considered ?? [])
                     .filter((c) => c.reason)
                     .map((c) => String(c.reason)),
               },
            };
         } catch (error) {
            return {
               error: error instanceof Error ? error.message : String(error),
            };
         }
      }

      /** One run of the package, to its settled record. */
      async function build(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
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

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
            },
         }).restore;
         await freshLoadPool();
         ({ root, db, repo } = await openTempStorage("str-order-", {
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
         const version = (
            (await seeder.runSQL(
               "SELECT row_to_json(t) AS row FROM (SELECT current_setting('server_version_num')::int AS v) t",
            )) as unknown as { rows: { v?: number }[] }
         ).rows[0];
         builtin = Number(version?.v ?? 0) >= 170000;
         for (const sql of seed(builtin)) await seeder.runSQL(sql);
         for (const col of ["cat", "icu", "cc", ...(builtin ? ["bc"] : [])]) {
            const read = async (order: string) =>
               (
                  (await seeder.runSQL(
                     `SELECT row_to_json(t) AS row FROM (SELECT v FROM (SELECT DISTINCT ${col} AS v FROM items) d ORDER BY ${order}) t`,
                  )) as unknown as {
                     rows: { v?: string; row?: { v?: string } }[];
                  }
               ).rows.map((r) => String(r.row?.v ?? r.v));
            orders[col] = {
               collated: await read("v"),
               bytes: await read('v COLLATE "C"'),
            };
         }
         await seeder.close();

         const envPath = path.join(root, "env");
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": MODEL },
            { manifest: { name: PKG, description: "string order collation" } },
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
         for (const s of shapes()) live.set(s, await ask(s.query));
         run = await build();
         for (const s of shapes()) served.set(s, await ask(s.query));
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

      it("orders each non-binary column otherwise than byte order, and C and the builtin as it", () => {
         for (const col of ["cat", "icu"]) {
            expect(orders[col].collated, col).not.toEqual(orders[col].bytes);
         }
         for (const col of ["cc", ...(builtin ? ["bc"] : [])]) {
            expect(orders[col].collated, col).toEqual(orders[col].bytes);
         }
      });

      for (const s of SHAPES) {
         const how =
            "served" in s.want
               ? `served from ${s.want.served}`
               : "live" in s.want
                 ? `declined (${s.want.live}), answered live`
                 : "refused as live refuses it";
         it(`${s.name}: ${how}, equal to live`, () => {
            if (s.builtin && !builtin) return;
            const l = live.get(s)!;
            const got = served.get(s)!;
            if ("error" in s.want) {
               expect("error" in l, JSON.stringify(l)).toBe(true);
               expect("error" in got, JSON.stringify(got)).toBe(true);
               return;
            }
            if ("error" in l) throw new Error(`live failed: ${l.error}`);
            if ("error" in got) throw new Error(`served failed: ${got.error}`);
            expect(l.answer.servedBy).toBe("live");
            expect(JSON.stringify(got.answer.rows)).toBe(
               JSON.stringify(l.answer.rows),
            );
            if ("served" in s.want) {
               expect(got.answer.servedBy).toBe(s.want.served);
            } else {
               expect(got.answer.servedBy).toBe("live");
               expect(got.answer.declined).toContain(s.want.live);
            }
         });
      }
   },
);

/** Each value, and each case-insensitive group, has its own count. */
const DUCKDB_VALUES: [string, number][] = [
   ["apple", 1],
   ["Apple", 2],
   ["banana", 3],
   ["Banana", 5],
   ["cherry", 4],
];

const duckdbRows = (collate: string) => {
   let id = 0;
   const rows = DUCKDB_VALUES.flatMap(([v, n]) =>
      Array.from({ length: n }, () => `(${++id}, '${v}')`),
   );
   return `duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, v${collate} AS v
  FROM (VALUES ${rows.join(", ")}) AS t(id, v)
""")`;
};

const DUCKDB_MODEL = `##! experimental { persistence }
#(materialize) name="nocase_m" storage="${LAKE}"
source: nocase_m is ${duckdbRows(" COLLATE NOCASE")} extend {
  primary_key: id
  measure: n is count()
}

#(materialize) name="bytes_m" storage="${LAKE}"
source: bytes_m is ${duckdbRows("")} extend {
  primary_key: id
  measure: n is count()
}
`;

const DUCKDB_SHAPES: { query: string; served: boolean }[] = [
   { query: "run: nocase_m -> { group_by: v }", served: false },
   { query: "run: nocase_m -> { group_by: v; aggregate: n }", served: false },
   { query: "run: nocase_m -> { aggregate: d is count(v) }", served: false },
   { query: "run: nocase_m -> { aggregate: n }", served: true },
   { query: "run: bytes_m -> { group_by: v }", served: true },
   { query: "run: bytes_m -> { group_by: v; aggregate: n }", served: true },
   { query: "run: bytes_m -> { aggregate: lo is min(v) }", served: true },
];

describe("a storage= copy of a DuckDB source answers a NOCASE string as its source does", () => {
   let restore = () => {};
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: Awaited<ReturnType<typeof loader.bind>>;
   const live = new Map<string, FixtureOutcome>();
   const served = new Map<string, FixtureOutcome>();

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": DUCKDB_MODEL });
      for (const s of DUCKDB_SHAPES) {
         live.set(s.query, await loader.run("m.malloy", s.query));
      }
      fixture = await loader.bind();
      for (const s of DUCKDB_SHAPES) {
         served.set(s.query, await fixture.run("m.malloy", s.query));
      }
   });

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore();
      await freshLoadPool();
   });

   it("builds every stored table", () => {
      expect(fixture.buildErrors).toEqual({});
   });

   for (const s of DUCKDB_SHAPES) {
      it(`${s.query}: ${s.served ? "served from the copy" : "answered live"}, equal to live`, () => {
         const l = live.get(s.query)!;
         const got = served.get(s.query)!;
         if ("error" in l) throw new Error(`live failed: ${l.error}`);
         if ("error" in got) throw new Error(`served failed: ${got.error}`);
         expect(JSON.stringify(got.answer.rows)).toBe(
            JSON.stringify(l.answer.rows),
         );
         if (s.served) {
            expect(got.answer.via).toBe("served_copy");
         } else {
            expect(got.answer.servedBy).toBe("live");
            expect(got.answer.servedSkip?.reason).toBe(DECLINED);
         }
      });
   }
});
