// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * End to end, through the real package and environment: a database table
 * read by Malloy's build-manifest substitution (a rollup, a persisted view, a
 * persisted query), built by a real run and then altered in its warehouse,
 * no longer matches the column contract its build recorded, once the package
 * binds again (`Package.reloadAllModels`, the path a run's commit, a manifest
 * bind and a restart take). Whatever the drift (a wider integer, a zone, a
 * dropped column, a type Malloy cannot read, a boolean now an integer), the
 * table is left out, never cast or narrowed: every query on it answers live,
 * equal to live's answer read before the first build, and the serve verdict
 * says `incompatible`, naming the table and the columns that moved.
 *
 * DuckDB, and Postgres when `PUBLISHER_TEST_POSTGRES_URL` names one.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/** Each drift kind, by the suffix its view and query are named with. */
const KINDS = ["num", "tz", "drop", "odd", "cast"] as const;
type Kind = (typeof KINDS)[number];

/** Each kind's grouping: the column its table's drift moves. */
const GROUPING: Record<Kind, string> = {
   num: "cat",
   tz: "h is ts.hour",
   drop: "note",
   odd: "d",
   cast: "flag",
};

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', CAST(1 AS INTEGER), CAST(9.0 AS DECIMAL(10,1)), true, DATE '2026-01-15', TIMESTAMP '2026-01-15 10:00:00', 'x'),
    ('a', CAST(2 AS INTEGER), CAST(10.0 AS DECIMAL(10,1)), false, DATE '2026-02-03', TIMESTAMP '2026-02-03 01:00:00', 'y'),
    ('b', CAST(3 AS INTEGER), CAST(2.5 AS DECIMAL(10,1)), true, DATE '2026-02-28', TIMESTAMP '2026-02-28 12:00:00', 'x')
  ) AS t(cat, n, amt, flag, d, ts, note)
""") extend {
  measure: total is n.max()
  measure: top is amt.max()
}

source: viewed is raw extend {
${KINDS.map(
   (k) => `  #@ persist
  view: v_${k} is { group_by: ${GROUPING[k]}; aggregate: total; order_by: 1 }`,
).join("\n")}
}

${KINDS.map(
   (k) => `#@ persist
query: q_${k} is raw -> { group_by: ${GROUPING[k]}; aggregate: total; order_by: 1 }`,
).join("\n")}

source: rolled is raw extend {
  #(rollup) grain=["cat", "flag", "d", "ts", "note"]
  measure: rtotal is n.max()
  #(rollup) grain=["cat", "flag", "d", "ts", "note"]
  measure: rtop is amt.max()
}
`;

/** The queries, by mechanism and drift kind; `untouched` reads no moved column. */
const QUERIES: Record<string, string> = {
   ...Object.fromEntries(
      KINDS.flatMap((k) => [
         [`view ${k}`, `run: viewed -> v_${k}`],
         [`query ${k}`, `run: q_${k}`],
         [
            `rollup ${k}`,
            `run: rolled -> { group_by: ${GROUPING[k]}; aggregate: rtotal; order_by: 1 }`,
         ],
      ]),
   ),
   "rollup untouched":
      "run: rolled -> { group_by: cat; aggregate: rtop; order_by: cat }",
};

interface Tier {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   /** Each drift's statement on `table`'s `column`. */
   alter: Record<Kind, (table: string, column: string) => string>;
   columnsSql: (table: string) => string;
   /** The drift kinds a query reading them answers live after. */
   live: readonly Kind[];
}

const tiers: Tier[] = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
      alter: {
         num: (t, c) => `ALTER TABLE ${t} ALTER "${c}" TYPE BIGINT`,
         tz: (t, c) => `ALTER TABLE ${t} ALTER "${c}" TYPE TIMESTAMPTZ`,
         drop: (t, c) => `ALTER TABLE ${t} DROP COLUMN "${c}"`,
         odd: (t, c) =>
            `ALTER TABLE ${t} ALTER "${c}" TYPE DATE[] USING ["${c}"]`,
         cast: (t, c) => `ALTER TABLE ${t} ALTER "${c}" TYPE INTEGER`,
      },
      columnsSql: (table) =>
         `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}'`,
      // DuckDB's serving sessions run in UTC: a zone gained reads alike.
      live: ["drop", "odd"],
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              warehouse: async () => {
                 const db = `drift_sub_${process.pid}_${Date.now()}`;
                 const admin = new PostgresConnection({
                    name: "admin",
                    connectionString: POSTGRES_URL,
                 });
                 await admin.runSQL(`CREATE DATABASE ${db}`);
                 const parsed = new URL(POSTGRES_URL);
                 return {
                    config: {
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: parsed.hostname,
                          port: Number(parsed.port || 5432),
                          userName: decodeURIComponent(parsed.username),
                          password: decodeURIComponent(parsed.password),
                          databaseName: db,
                       },
                    },
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
              alter: {
                 num: (t: string, c: string) =>
                    `ALTER TABLE ${t} ALTER COLUMN "${c}" TYPE bigint`,
                 tz: (t: string, c: string) =>
                    `ALTER TABLE ${t} ALTER COLUMN "${c}" TYPE timestamptz`,
                 drop: (t: string, c: string) =>
                    `ALTER TABLE ${t} DROP COLUMN "${c}"`,
                 odd: (t: string, c: string) =>
                    `ALTER TABLE ${t} ALTER COLUMN "${c}" TYPE date[] USING ARRAY["${c}"]`,
                 cast: (t: string, c: string) =>
                    `ALTER TABLE ${t} ALTER COLUMN "${c}" TYPE integer USING "${c}"::int`,
              },
              columnsSql: (table: string) =>
                 `SELECT row_to_json(c) AS row FROM (SELECT column_name FROM information_schema.columns WHERE table_name = '${table}') c`,
              live: ["tz", "drop", "odd"] as const,
           } satisfies Tier,
        ]
      : []),
];

/** The column each drift kind moves, in a view's or query's own table. */
const RESULT_COLUMN: Record<Kind, string> = {
   num: "total",
   tz: "h",
   drop: "note",
   odd: "d",
   cast: "flag",
};

/** The column each drift kind moves in the rollup's table. */
const ROLLUP_COLUMN: Record<Kind, (names: string[]) => string | undefined> = {
   num: (names) =>
      names.find((n) => n.startsWith("rtotal") && n.includes("partial")),
   tz: (names) => names.find((n) => n === "ts"),
   drop: (names) => names.find((n) => n === "note"),
   odd: (names) => names.find((n) => n === "d"),
   cast: (names) => names.find((n) => n === "flag"),
};

interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(warehouse: unknown): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "drift-substituted-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": MODEL },
      { manifest: { name: PKG, description: "fixture" } },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   const repo = createDuckDBRepository(db);
   const env = await Environment.create(ENV, envPath, [warehouse as never], []);
   await env.addPackage(PKG);
   const pkg = await env.getPackage(parsePackageName(PKG), false);
   const service = serviceOverEnvironment(
      () => repo,
      () => env,
   );
   return {
      root,
      env,
      pkg,
      repo,
      db,
      service,
      cleanup: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
      },
   };
}

/** One auto-run, to its settled record. */
async function build(f: Fixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 120_000;
   for (;;) {
      const m = await f.repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await f.repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

interface Answer {
   rows: Record<string, unknown>[];
   kind?: string;
   skipped?: unknown;
}

/** The answer to `query` through the package, with what served it. */
async function ask(f: Fixture, query: string): Promise<Answer> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   const report = result.path as
      | { servedBy?: { kind?: string }; skipped?: unknown }
      | undefined;
   return {
      rows: normalized(result.compactResult as Record<string, unknown>[]),
      kind: report?.servedBy?.kind,
      skipped: report?.skipped,
   };
}

/**
 * Rows with each value as JSON writes it, a date or timestamp as its ISO
 * text: the comparison is of what a caller receives.
 */
function normalized(
   rows: Record<string, unknown>[],
): Record<string, unknown>[] {
   return JSON.parse(JSON.stringify(rows)) as Record<string, unknown>[];
}

/** The column names of `table`, unqualified, as its warehouse lists them. */
async function columnsOf(
   warehouse: { runSQL(sql: string): Promise<{ rows: unknown[] }> },
   tier: Tier,
   table: string,
): Promise<string[]> {
   const unqualified = table.split(".").pop()!.replace(/"/g, "");
   const rows = (await warehouse.runSQL(tier.columnsSql(unqualified)))
      .rows as Record<string, unknown>[];
   return rows.map((c) => {
      const row = (c.row ?? c) as Record<string, unknown>;
      return String(row.column_name);
   });
}

for (const tier of tiers) {
   describe(`a rollup, persisted view or query table altered after its build reads each column as the served copy does (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const live: Record<string, Answer> = {};
      const before: Record<string, Answer> = {};
      const altered: string[] = [];
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await freshLoadPool();
         const w = await tier.warehouse();
         drop = w.drop;
         f = await fixture(w.config);
         // Live's answers: nothing is built yet.
         for (const [name, query] of Object.entries(QUERIES)) {
            live[name] = await ask(f, query);
         }
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         for (const [name, query] of Object.entries(QUERIES)) {
            before[name] = await ask(f, query);
         }
         // Out of band: the warehouse's own ALTER, after the build, of each
         // table's column its drift kind names.
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const tables = Object.values(
            (
               run.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         ).map((e) => e.physicalTableName);
         for (const table of tables) {
            const names = await columnsOf(warehouse, tier, table);
            const kind = KINDS.find(
               (k) => table.includes(`v_${k}`) || table.includes(`q_${k}`),
            );
            if (kind) {
               await warehouse.runSQL(
                  tier.alter[kind](table, RESULT_COLUMN[kind]),
               );
               altered.push(`${table}.${RESULT_COLUMN[kind]}`);
               continue;
            }
            // A rollup's table: every drift at once, but on the rollup of
            // `rtop`, whose partial stays as built.
            for (const k of KINDS) {
               const column = ROLLUP_COLUMN[k](names);
               if (!column) continue;
               await warehouse.runSQL(tier.alter[k](table, column));
               altered.push(`${table}.${column}`);
            }
         }
         // The bind every run, manifest and restart takes.
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      it("built and served every mechanism from its table, equal to live, before the alter", () => {
         // A view and the query of its text share one table, so five
         // tables for both; and the rollup's (both measures share it), one
         // column per drift (the partial of `rtotal` for a number's).
         expect(altered.length).toBe(5 + 5);
         for (const [name, answer] of Object.entries(before)) {
            expect({ name, rows: answer.rows }).toEqual({
               name,
               rows: live[name].rows,
            });
            expect({ name, kind: answer.kind }).not.toEqual({
               name,
               kind: "live",
            });
         }
      });

      for (const name of Object.keys(QUERIES)) {
         it(`${name}: answered live, its table incompatible with its contract, equal to live`, async () => {
            const after = await ask(f, QUERIES[name]);
            expect(after.rows).toEqual(live[name].rows);
            expect(after.kind).toBe("live");
         });
      }

      it("says why on each table's verdict: incompatible, never a cast", () => {
         const incompatible = f.pkg
            .materialization()
            .serveVerdicts()
            .filter((v) => v.reason === "incompatible");
         expect(incompatible.length).toBeGreaterThan(0);
         for (const v of incompatible) {
            expect(v.table?.physicalTableName).toBeTruthy();
            expect(v.incompatibility?.columns.length).toBeGreaterThan(0);
         }
         // The old drift warning, which cast or left a column out, is gone.
         expect(
            (f.pkg.getPackageMetadata().warnings ?? []).filter(
               (w) => w.code === "database-column-type-mismatch",
            ),
         ).toEqual([]);
      });
   });
}

describe("a stored read that finds its table gone since the bind (the drift handler's failed-read trigger)", () => {
   let restore: () => void;
   let f: Fixture;
   let drop: (() => Promise<void>) | undefined;
   const live: Record<string, Answer> = {};
   let dropped = "";
   let staleMarks = 0;
   const QUERY = QUERIES["view num"];
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      const w = await tiers[0].warehouse();
      drop = w.drop;
      f = await fixture(w.config);
      live.before = await ask(f, QUERY);
      const run = await build(f);
      expect(run.status).toBe("MANIFEST_FILE_READY");
      // Out of band, with no bind after it: the view's table is dropped.
      const warehouse = await f.env.getMalloyConnection("warehouse");
      dropped = Object.values(
         (
            run.manifest as {
               entries: Record<string, { physicalTableName: string }>;
            }
         ).entries,
      )
         .map((e) => e.physicalTableName)
         .find((t) => t.includes("v_num") || t.includes("q_num"))!;
      await warehouse.runSQL(`DROP TABLE ${dropped}`);
      // Every drift report marks the build plan stale and derives it again.
      const plan = (
         f.pkg.materialization() as unknown as {
            plan: { markStale(changed?: Iterable<string>): void };
         }
      ).plan;
      const markStale = plan.markStale.bind(plan);
      plan.markStale = (changed?: Iterable<string>) => {
         staleMarks++;
         markStale(changed);
      };
   }, 300_000);
   afterAll(async () => {
      await f?.cleanup();
      await drop?.();
      restore();
      await freshLoadPool();
   });

   it("answers that query live, equal to live, rather than failing it", async () => {
      expect(dropped).not.toBe("");
      const answer = await ask(f, QUERY);
      expect(answer.rows).toEqual(live.before.rows);
      expect(answer.kind).toBe("live");
   });

   it("fails the table closed, then the rebind judges it: never served again until it is rebuilt", async () => {
      const deadline = Date.now() + 20_000;
      let verdict: { reason?: string; table?: { physicalTableName: string } } =
         {};
      while (Date.now() < deadline) {
         verdict =
            f.pkg
               .materialization()
               .serveVerdicts()
               .find((v) => v.table?.physicalTableName === dropped) ?? {};
         if (verdict.reason !== undefined) break;
         await new Promise((r) => setTimeout(r, 100));
      }
      // Gone at the rebind's read: unread (not_found), or failed closed.
      expect(["incompatible", "stale", undefined]).toContain(verdict.reason);
      // And the build plan was derived again, from what the rebind bound.
      expect(staleMarks).toBeGreaterThan(0);
      await f.pkg.materialization().whenBuildPlanCurrent(20_000);
      expect(f.pkg.materialization().isBuildPlanStale()).toBe(false);
      const again = await ask(f, QUERY);
      expect(again.rows).toEqual(live.before.rows);
      expect(again.kind).toBe("live");
   });
});
