// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Campaign P2-26 (coverage gap G3): colocated tables on a Postgres origin
 * whose columns are the types a stored read most easily gets wrong: a BIGINT
 * past 2^53, an unconstrained NUMERIC, a wide NUMERIC(50,10), a TIMESTAMPTZ,
 * BIGINT and TEXT arrays, and JSONB. One source of each mechanism over the
 * same table, each built in Postgres beside it (database tier):
 *
 * - a database `#(materialize)` over the table;
 * - a `#@ persist` `-> { select: * }` over it.
 *
 * Every query is answered live before the build and served after it, through
 * the real production path (Environment, Package, MaterializationService's
 * build and bind). The served answer must equal live, every value compared as
 * a string (never through a double), with the same field types, and must be
 * either read from the source's table or answered live with a coded reason.
 * Then each table is tampered with its own suffix, so a source read from
 * another source's table, or from live while claiming a table, is told apart.
 *
 * Each mechanism runs twice: over the whole table, and over a copy without
 * the one column that keeps it from being served at all on this branch. Both
 * are bugs (equal to live, but never served), held as `it.todo` below:
 *
 * - Bug A: a database `#(materialize)` over a TIMESTAMPTZ column is never
 *   served. Every query, one not reading the column included, answers live
 *   with `NOT_SERVED` ("column `ts` (declared `timestamptz`) is not in the
 *   stored table"); the column is in the table.
 * - Bug B: a database `#@ persist` over a JSONB column is never served, and
 *   its path report gives no reason: nothing is considered ("no stored table
 *   is re-rooted in this model's served copy").
 * - Bug C: the database `#@ persist` it does serve answers `select: *` (whose
 *   arrays the served copy leaves out) live with no reason in its path
 *   report either, where the `#(materialize)` one reports `NOT_SERVED` and a
 *   query naming an array column reports `MISSING_FIELD`.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create and
 * drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
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
import type { Row } from "../../../src/test_helpers/conformance/answer_difference";
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

/** 2^53 + 1, the first integer a JavaScript number cannot hold. */
const ABOVE = "9007199254740993";

const SEED = [
   `CREATE TABLE t (
      id int8 PRIMARY KEY, g text, big int8, n numeric, w numeric(50,10),
      ts timestamptz, ia int8[], ta text[], jb jsonb
    )`,
   `INSERT INTO t VALUES
      (1, 'a', ${ABOVE}, 1.5, 12345678901234567890.1234567890,
       '2024-01-02 03:04:05+00', '{1,${ABOVE}}', '{x,y}', '{"k": 1, "s": "v"}'),
      (2, 'a', 9007199254740991, 0.000000000000000000001, 1.0000000001,
       '2024-06-30 23:00:00+00', '{}', '{z}', '{"k": [1, 2]}'),
      (3, 'b', -${ABOVE}, 123456789012345678901234567890.123, -5.5,
       NULL, NULL, NULL, NULL)`,
   // The same rows without the column of bug A, and without that of bug B.
   `CREATE TABLE t_no_ts AS SELECT id, g, big, n, w, ia, ta, jb FROM t`,
   `CREATE TABLE t_no_jb AS SELECT id, g, big, n, w, ts, ia, ta FROM t`,
];

const ALL = ["id", "g", "big", "n", "w", "ts", "ia", "ta", "jb"];

interface Source {
   source: string;
   model: string;
   /** The suffix its tampered table carries. */
   tag: string;
   columns: string[];
   /** The bug that keeps it from being served on this branch, if any. */
   bug?: "A" | "B";
}

/** Each mechanism over the whole table, and over a copy it serves. */
const SOURCES: Source[] = [
   {
      source: "mat",
      model: "mat.malloy",
      tag: "_mat",
      columns: ALL.filter((c) => c !== "ts"),
   },
   {
      source: "per",
      model: "per.malloy",
      tag: "_per",
      columns: ALL.filter((c) => c !== "jb"),
   },
   {
      source: "mat_ts",
      model: "mat.malloy",
      tag: "_mts",
      columns: ALL,
      bug: "A",
   },
   {
      source: "per_jb",
      model: "per.malloy",
      tag: "_pjb",
      columns: ALL,
      bug: "B",
   },
];

/** The two mechanisms, each in its own model. */
const FILES: Record<string, string> = {
   "mat.malloy": `#(materialize) name="g3_mat"
source: mat is pg.table('public.t_no_ts') extend { primary_key: id }

#(materialize) name="g3_mat_ts"
source: mat_ts is pg.table('public.t') extend { primary_key: id }
`,
   "per.malloy": `##! experimental.persistence
#@ persist name="g3_per"
source: per is pg.table('public.t_no_jb') -> { select: * }

#@ persist name="g3_per_jb"
source: per_jb is pg.table('public.t') -> { select: * }
`,
};

interface Query {
   name: string;
   body: string;
   /** The columns it reads; it runs on a source holding them all. */
   reads: string[];
}

/** One query per column, plus `select: *`, each ordered so rows line up. */
const SCALARS: Query = {
   name: "aggregates over the integer and numeric columns",
   body: `group_by: g
  aggregate:
    n_rows is count()
    big_sum is big.sum()
    big_max is big.max()
    n_sum is n.sum()
    n_max is n.max()
    w_sum is w.sum()
    w_max is w.max()
  order_by: g`,
   reads: ["g", "big", "n", "w"],
};

const QUERIES: Query[] = [
   SCALARS,
   {
      name: "a group-by on the big integer",
      body: "group_by: big; order_by: big",
      reads: ["big"],
   },
   {
      name: "a group-by on the unconstrained numeric",
      body: "group_by: n; order_by: n",
      reads: ["n"],
   },
   {
      name: "a group-by on the wide numeric",
      body: "group_by: w; order_by: w",
      reads: ["w"],
   },
   {
      name: "a max of the zoned timestamp",
      body: "group_by: g; aggregate: ts_max is ts.max(); order_by: g",
      reads: ["g", "ts"],
   },
   {
      name: "a group-by on the zoned timestamp",
      body: "group_by: ts; order_by: ts",
      reads: ["ts"],
   },
   {
      name: "the bigint array",
      body: "select: id, ia; order_by: id",
      reads: ["id", "ia"],
   },
   {
      name: "the text array",
      body: "select: id, ta; order_by: id",
      reads: ["id", "ta"],
   },
   {
      name: "the jsonb column",
      body: "select: id, jb; order_by: id",
      reads: ["id", "jb"],
   },
   { name: "select: *", body: "select: *; order_by: id", reads: ["id"] },
];

/** Bug C: a declined query the path report gives no reason for. */
const silentDecline = (s: Source, q: Query) =>
   s.source === "per" && q.name === "select: *";

const queriesOf = (s: Source) =>
   QUERIES.filter((q) => q.reads.every((c) => s.columns.includes(c)));

/** A value as an exact string: never read back through a double. */
function exact(value: unknown): string {
   if (value === null || value === undefined) return String(value);
   if (value instanceof Date) return value.toISOString();
   if (typeof value === "object") {
      return JSON.stringify(value, (_k, v) =>
         typeof v === "bigint" ? `${v}n` : v,
      );
   }
   return String(value);
}

/** Rows with every value an exact string, in the order they came. */
function asStrings(rows: Row[]): Record<string, string>[] {
   return rows.map((r) =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, exact(v)])),
   );
}

/** Field types without what a stored table cannot carry (KL 12). */
function comparableFields(fields: unknown): unknown {
   return JSON.parse(
      JSON.stringify(fields ?? [], (key, value) =>
         key === "subtype" || key === "annotations" ? undefined : value,
      ),
   );
}

interface Answer {
   rows: Row[];
   fields: unknown;
   servedBy: string;
   servedFrom: string | null;
   /** The coded reasons the router gave for each candidate it declined. */
   declined: string[];
}

type Outcome = { answer: Answer } | { error: string };

describe.skipIf(!POSTGRES_URL)(
   "P2-26: colocated Postgres types served equal to live",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const warehouse = `g3_pg_wh_${token}`;
      let admin: PostgresConnection;
      let seed: PostgresConnection | undefined;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment | undefined;
      let service: MaterializationService;
      let restore = () => {};
      let run: Materialization | undefined;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();
      const key = (source: string, q: string) => `${source}|${q}`;

      async function ask(model: string, malloy: string): Promise<Outcome> {
         const pkg = await env!.getPackage(parsePackageName(PKG), false);
         const m = pkg.getModel(model);
         if (!m) return { error: `${model} did not load` };
         try {
            const result = await m.runQuery(
               unrecordedAudit(),
               { query: malloy } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
            return {
               answer: {
                  rows: result.compactResult as unknown as Row[],
                  fields: (result.result?.schema?.fields ?? []).map((f) => ({
                     name: f.name,
                     type: (f as { type?: unknown }).type,
                  })),
                  servedBy: result.path?.servedBy?.kind ?? "live",
                  servedFrom: result.servedFrom ?? null,
                  declined: (result.path?.considered ?? [])
                     .filter((c) => c.reason || c.outcome === "skipped")
                     .map((c) => `${c.kind}:${c.reason ?? c.outcome}`),
               },
            };
         } catch (error) {
            return {
               error: error instanceof Error ? error.message : String(error),
            };
         }
      }

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

      const queryOf = (source: string, body: string) =>
         `run: ${source} -> {\n  ${body}\n}`;

      beforeAll(async () => {
         restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
         }).restore;
         await freshLoadPool();
         ({ root, db, repo } = await openTempStorage("g3-pg-", {
            id: ENV_ID,
            name: ENV,
         }));
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         // A binary collation, so a string order is served as live orders it.
         await admin.runSQL(
            `CREATE DATABASE ${warehouse} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
         );
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         seed = new PostgresConnection({
            name: "seed",
            connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
         });
         for (const sql of SEED) await seed.runSQL(sql);

         const envPath = path.join(root, "env");
         writePackageFiles(path.join(envPath, PKG), FILES, {
            manifest: { name: PKG, description: "colocated Postgres types" },
         });
         env = await Environment.create(ENV, envPath, [
            {
               name: "pg",
               type: "postgres",
               postgresConnection: { ...pg, databaseName: warehouse },
            } as never,
         ]);
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env!,
         );
         for (const s of SOURCES) {
            for (const q of queriesOf(s)) {
               live.set(
                  key(s.source, q.name),
                  await ask(s.model, queryOf(s.source, q.body)),
               );
            }
         }
         run = await build();
         for (const s of SOURCES) {
            for (const q of queriesOf(s)) {
               served.set(
                  key(s.source, q.name),
                  await ask(s.model, queryOf(s.source, q.body)),
               );
            }
         }
      });

      afterAll(async () => {
         await seed?.close().catch(() => undefined);
         await env?.closeAllConnections().catch(() => undefined);
         if (db) await disposeTempStorage({ db, root });
         await admin
            ?.runSQL(`DROP DATABASE IF EXISTS ${warehouse} WITH (FORCE)`)
            .catch(() => undefined);
         await admin?.close().catch(() => undefined);
         restore();
         await freshLoadPool();
      });

      /** The table the build wrote for `source`. */
      function tableOf(source: string): string {
         const entries = Object.values(run?.manifest?.entries ?? {}).filter(
            (e) => e.sourceName === source,
         );
         expect(entries).toHaveLength(1);
         return entries[0].physicalTableName;
      }

      it("builds every source in Postgres, each its own table, with no failure", () => {
         expect([run?.status, run?.error ?? null]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(
            (run?.manifest as { failures?: Record<string, unknown> })
               .failures ?? {},
         ).toEqual({});
         const tables = SOURCES.map((s) => tableOf(s.source));
         expect(new Set(tables).size).toBe(SOURCES.length);
      });

      /** The live and served outcomes of one query, both answered. */
      function answers(s: Source, q: Query): { l: Answer; v: Answer } {
         const l = live.get(key(s.source, q.name))!;
         const v = served.get(key(s.source, q.name))!;
         if ("error" in l) throw new Error(`live failed: ${l.error}`);
         if ("error" in v) throw new Error(`served failed: ${v.error}`);
         return { l: l.answer, v: v.answer };
      }

      for (const s of SOURCES) {
         describe(s.source, () => {
            for (const q of queriesOf(s)) {
               it(`${q.name}: equal to live, value for value`, () => {
                  const { l, v } = answers(s, q);
                  expect(l.servedBy).toBe("live");
                  expect(l.rows.length).toBeGreaterThan(0);
                  expect(asStrings(v.rows)).toEqual(asStrings(l.rows));
                  expect(comparableFields(v.fields)).toEqual(
                     comparableFields(l.fields),
                  );
               });

               // Bugs B and C: answered live with nothing considered.
               (s.bug === "B" || silentDecline(s, q) ? it.todo : it)(
                  `${q.name}: from its table, or live with a coded reason`,
                  () => {
                     const { v } = answers(s, q);
                     if (v.servedBy === "live") {
                        expect(v.declined.length).toBeGreaterThan(0);
                     } else {
                        expect(v.servedBy).toBe("materialized");
                        expect(v.servedFrom).toBe("database");
                     }
                  },
               );
            }

            // Bugs A and B: the whole source is answered live.
            (s.bug ? it.todo : it)(
               "serves a query reading only integer and numeric columns from its table",
               () => {
                  const { v } = answers(s, SCALARS);
                  expect(v.servedBy).toBe("materialized");
                  expect(v.servedFrom).toBe("database");
               },
            );
         });
      }

      it("reads each source's own table: tampered apart, a served answer carries its own suffix", async () => {
         for (const s of SOURCES) {
            await seed!.runSQL(
               `UPDATE ${tableOf(s.source)} SET g = g || '${s.tag}'`,
            );
         }
         for (const s of SOURCES) {
            const { l } = answers(s, SCALARS);
            const v = await ask(s.model, queryOf(s.source, SCALARS.body));
            if ("error" in v) throw new Error(`served failed: ${v.error}`);
            // A source a bug answers live must still be live's rows, never
            // another source's table.
            const tag = v.answer.servedBy === "live" ? "" : s.tag;
            if (!s.bug) expect(tag).toBe(s.tag);
            expect(asStrings(v.answer.rows)).toEqual(
               asStrings(l.rows.map((r) => ({ ...r, g: `${r.g}${tag}` }))),
            );
         }
      });
   },
);
