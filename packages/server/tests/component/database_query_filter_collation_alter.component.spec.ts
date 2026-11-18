// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The caller's own query filters over database tables, through the real
 * package bind: a `where:`, a `having:`, a filtered measure and a view's
 * filter on the served copy, a `where:` on a rollup, and a later stage's
 * `where:` on a persisted view and a persisted named query.
 *
 * Each is applied to the stored rows, so each compares the stored column
 * under the collation it holds now. As built, every stored column keeps its
 * source's collation and each answer is served, equal to live. Once a
 * stored `category` column is altered to compare case-insensitively (which
 * changes no type), a filter `category = 'books'` would admit `Books` from
 * the table while live admits only `books`: each such answer is then live.
 * A filter that compares no string (`id > 2`) is still served. Postgres
 * under a `C` and an `en_US` default; DuckDB by its tables' definitions.
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

const MODEL = `##! experimental { persistence }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    (1, 'electronics'), (2, 'books'), (3, 'books'), (4, 'Books'), (5, 'toys'),
    (6, 'apple')
  ) AS t(id, category)
""")

#(materialize) name="qf_tab"
source: m_tab is raw extend {
  primary_key: id
  measure: c is count()
  measure: nb is count() { where: category = 'books' }
  view: v_books is { where: category = 'books'; select: id; order_by: id }
  #@ persist name="qf_view"
  view: by_cat is { group_by: category; aggregate: c }
}

source: r_src is raw extend {
  #(rollup) grain=["category"]
  measure: rc is count()
}

#@ persist name="qf_query"
query: q_cat is raw -> { group_by: category; aggregate: n is count() }
`;

/** Each query, and whether its filter compares a string (else it is served after the alter too). */
const CASES: { name: string; query: string; leg: string; strings: boolean }[] =
   [
      {
         name: "a query's where:",
         query: "run: m_tab -> { where: category = 'books'; select: id; order_by: id }",
         leg: "materialized",
         strings: true,
      },
      {
         name: "a query's having:",
         query: "run: m_tab -> { group_by: id; aggregate: mx is max(category); having: mx = 'books'; order_by: id }",
         leg: "materialized",
         strings: true,
      },
      {
         name: "a filtered measure",
         query: "run: m_tab -> { group_by: id; aggregate: nb; order_by: id }",
         leg: "materialized",
         strings: true,
      },
      {
         name: "a view's filter",
         query: "run: m_tab -> v_books",
         leg: "materialized",
         strings: true,
      },
      {
         name: "a rollup's where:",
         query: "run: r_src -> { where: category = 'books'; group_by: category; aggregate: rc; order_by: category }",
         leg: "rollup",
         strings: true,
      },
      {
         name: "a persisted view's later stage",
         query: "run: m_tab -> by_cat -> { where: category = 'books'; select: category, c; order_by: category }",
         leg: "view",
         strings: true,
      },
      {
         name: "a persisted query's later stage",
         query: "run: q_cat -> { where: category = 'books'; select: category, n; order_by: category }",
         leg: "query",
         strings: true,
      },
      {
         name: "a numeric where: (the control)",
         query: "run: m_tab -> { where: id > 2; select: id, category; order_by: id }",
         leg: "materialized",
         strings: false,
      },
   ];

interface Fixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
   cleanup: () => Promise<void>;
}

async function fixture(warehouse: unknown, model = MODEL): Promise<Fixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "query-filter-collation-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": model },
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
   const deadline = Date.now() + 60_000;
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

/** The answer to `query` through the package, with what served it. */
async function ask(
   f: Fixture,
   query: string,
   givens?: Record<string, string>,
): Promise<{
   rows: Record<string, unknown>[];
   kind?: string;
   considered?: unknown;
}> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query, ...(givens ? { givens } : {}) } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      rows: result.compactResult as Record<string, unknown>[],
      kind: result.path?.servedBy?.kind,
      considered: (result.path as { considered?: unknown } | undefined)
         ?.considered,
   };
}

const tiers: {
   label: string;
   warehouse: () => Promise<{ config: unknown; drop?: () => Promise<void> }>;
   /** Make `column` of `table` compare case-insensitively. */
   alter: (table: string, column: string) => string[];
   /** Lists a table's column names, as the dialect's connection returns rows. */
   columnsSql: (table: string) => string;
}[] = [
   {
      label: "DuckDB",
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         },
      }),
      alter: (table, column) => [
         `ALTER TABLE ${table} ALTER "${column}" SET DATA TYPE VARCHAR COLLATE NOCASE`,
      ],
      columnsSql: (table) =>
         `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}'`,
   },
   ...(POSTGRES_URL
      ? (["C", "en_US"] as const).map((locale) => ({
           label: `Postgres, ${locale} default`,
           warehouse: async () => {
              const db = `qf_coll_${locale.toLowerCase()}_${process.pid}_${Date.now()}`;
              const admin = new PostgresConnection({
                 name: "admin",
                 connectionString: POSTGRES_URL,
              });
              await admin.runSQL(
                 `CREATE DATABASE ${db} LOCALE '${locale === "C" ? "C" : "en_US.utf8"}' ENCODING 'UTF8' TEMPLATE template0`,
              );
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
           alter: (table: string, column: string) => [
              `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS qf_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
              `ALTER TABLE ${table} ALTER COLUMN "${column}" TYPE text COLLATE qf_ci`,
           ],
           columnsSql: (table: string) =>
              `SELECT row_to_json(c) AS row FROM (SELECT column_name FROM information_schema.columns WHERE table_name = '${table}') c`,
        }))
      : []),
];

for (const tier of tiers) {
   describe(`a query's own filter over a stored table compares as live (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const live: Record<string, unknown> = {};
      const served: Record<string, { rows: unknown; kind?: string }> = {};
      const after: Record<
         string,
         { rows: unknown; kind?: string; considered?: unknown }
      > = {};
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
         // Before any build, every answer is live's.
         for (const c of CASES) live[c.name] = (await ask(f, c.query)).rows;
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         for (const c of CASES) served[c.name] = await ask(f, c.query);
         // Out of band, after the build: every stored table's `category`
         // column altered to compare case-insensitively, its type unchanged.
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         );
         for (const entry of entries) {
            const unqualified = entry.physicalTableName.split(".").pop()!;
            const held = (
               (await warehouse.runSQL(tier.columnsSql(unqualified)))
                  .rows as Record<string, unknown>[]
            ).map((c) =>
               String(((c.row ?? c) as Record<string, unknown>).column_name),
            );
            if (!held.includes("category")) continue;
            for (const sql of tier.alter(entry.physicalTableName, "category")) {
               await warehouse.runSQL(sql);
            }
            altered.push(unqualified);
         }
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         for (const c of CASES) after[c.name] = await ask(f, c.query);
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      it("alters the source copy, the rollup, the persisted view and the persisted query", () => {
         for (const fragment of ["qf_tab", "qf_view", "qf_query"]) {
            expect(
               altered.some((t) => t.includes(fragment)),
               fragment,
            ).toBe(true);
         }
         // The rollup's table, named after its source.
         expect(altered.length).toBeGreaterThanOrEqual(4);
      });

      for (const c of CASES) {
         it(`${c.name}: is served from its table as built, equal to live`, () => {
            expect(served[c.name].rows).toEqual(live[c.name] as never);
            expect(served[c.name].kind).toBe(c.leg);
         });

         it(
            c.strings
               ? `${c.name}: answers live, equal to live, once its column compares case-insensitively`
               : `${c.name}: is still served, equal to live, once a column it does not compare is altered`,
            () => {
               expect(after[c.name].rows).toEqual(live[c.name] as never);
               expect(after[c.name].kind).toBe(c.strings ? "live" : c.leg);
            },
         );
      }
   });
}
