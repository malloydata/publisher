// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An ungated `where:` over a database table, through the real package bind.
 *
 * A database table sits in its source's own warehouse and keeps its
 * columns' collations, so a scope over it compares as live does while every
 * stored column it reads keeps its source's collation: an equality, an
 * ordering, a function, a pattern, a dimension computed when the table is
 * read, a renamed column and a composite's field are all served, under a Postgres `C` default and an `en_US` one. A
 * column whose collation was changed after the build (here, to a
 * case-insensitive one) changes no type, but admits other rows than live:
 * every such scope then answers live. On DuckDB, which keeps no per-column
 * collation catalog, the table's definition is read as before.
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

const MODEL = `##! experimental { persistence composite_sources }
source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    (1, 'electronics'), (2, 'books'), (3, 'books'), (4, 'Books'), (5, 'toys'),
    (6, 'apple')
  ) AS t(id, category)
""")

#@ persist name="dbs_plain"
source: p_plain is raw -> {
  group_by: category, tenant is concat(category, '-x')
  aggregate: n is count()
}
#(materialize) name="dbs_tab"
source: m_tab is raw extend { primary_key: id }

source: s_ord_stage is p_plain extend { where: category > 'M' }
source: s_stage_out is p_plain extend { where: tenant = 'books-x' }
source: s_comp is compose(p_plain, p_plain extend { except: n }) extend {
  where: tenant = 'books-x'
}
source: s_eq is m_tab extend { where: category = 'books' }
source: s_ord is m_tab extend { where: category > 'M' }
source: s_fn is m_tab extend { where: upper(category) = 'BOOKS' }
source: s_like is m_tab extend { where: category ~ 'b%' }
source: s_dim is m_tab extend {
  dimension: t3 is concat(category, '-z')
  where: t3 = 'books-z'
}
source: s_ren is m_tab extend {
  rename: cat is category
  where: cat > 'M'
}
`;

/** Each scope, by the source that carries it. */
const SCOPES = [
   "s_ord_stage",
   "s_stage_out",
   "s_comp",
   "s_eq",
   "s_ord",
   "s_fn",
   "s_like",
   "s_dim",
   "s_ren",
] as const;
const query = (s: string) =>
   `run: ${s} -> { select: id_or_cat is ${s === "s_ord_stage" || s === "s_stage_out" || s === "s_comp" ? "category" : "id"}; order_by: id_or_cat }`;

/** The stored columns to alter, by table name fragment. */
const ALTER: [string, string[]][] = [
   ["dbs_plain", ["category", "tenant"]],
   ["dbs_tab", ["category"]],
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
      fs.mkdtempSync(path.join(os.tmpdir(), "database-alter-")),
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
              const db = `dbs_coll_${locale.toLowerCase()}_${process.pid}_${Date.now()}`;
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
              `DO $$ BEGIN CREATE COLLATION IF NOT EXISTS dbs_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); EXCEPTION WHEN others THEN NULL; END $$`,
              `ALTER TABLE ${table} ALTER COLUMN "${column}" TYPE text COLLATE dbs_ci`,
           ],
           columnsSql: (table: string) =>
              `SELECT row_to_json(c) AS row FROM (SELECT column_name FROM information_schema.columns WHERE table_name = '${table}') c`,
        }))
      : []),
];

for (const tier of tiers) {
   describe(`a scope over a database table compares as live while its columns keep their collation (${tier.label})`, () => {
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const live: Record<string, unknown> = {};
      const served: Record<string, { rows: unknown; kind?: string }> = {};
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
         for (const s of SCOPES) live[s] = (await ask(f, query(s))).rows;
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
         for (const s of SCOPES) served[s] = await ask(f, query(s));
         // Out of band, after the build: the warehouse's own ALTER of the
         // tables' string columns, which leaves their types as they were.
         const warehouse = await f.env.getMalloyConnection("warehouse");
         const entries = Object.values(
            (
               run.manifest as {
                  entries: Record<string, { physicalTableName: string }>;
               }
            ).entries,
         );
         for (const [fragment, columns] of ALTER) {
            const entry = entries.find((e) =>
               e.physicalTableName.includes(fragment),
            );
            expect(entry, fragment).toBeDefined();
            const unqualified = entry!.physicalTableName.split(".").pop()!;
            const held = (
               (await warehouse.runSQL(tier.columnsSql(unqualified)))
                  .rows as Record<string, unknown>[]
            ).map((c) =>
               String(((c.row ?? c) as Record<string, unknown>).column_name),
            );
            for (const column of columns) {
               expect(
                  held,
                  `${fragment}.${column} in ${held.join(",")}`,
               ).toContain(column);
               for (const sql of tier.alter(entry!.physicalTableName, column)) {
                  await warehouse.runSQL(sql);
               }
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

      for (const s of SCOPES) {
         it(`${s}: is served from its table as built, equal to live`, () => {
            expect(served[s].rows).toEqual(live[s] as never);
            // A composite's field names no one column (its member depends on
            // the whole query): a Postgres probe cannot read it, so live.
            const composite =
               s === "s_comp" && tier.label.startsWith("Postgres");
            expect(served[s].kind).toBe(composite ? "live" : "materialized");
         });

         it(`${s}: answers live, equal to live, once its column compares case-insensitively`, async () => {
            const after = await ask(f, query(s));
            expect(after.rows).toEqual(live[s] as never);
            expect(after.kind).toBe("live");
            // The path report says why its table was not read. (Its chain
            // walk does not follow a composite to its members' tables.)
            if (s === "s_comp") return;
            expect(after.considered).toContainEqual(
               expect.objectContaining({
                  outcome: "skipped",
                  reason: "NOT_SERVED",
               }),
            );
         });
      }
   });
}

/** A Postgres database for one fixture, with `setup` run in it first. */
async function postgresWarehouse(
   setup: string[],
): Promise<{ config: unknown; drop: () => Promise<void> }> {
   const db = `dbs_more_${process.pid}_${Date.now()}`;
   const admin = new PostgresConnection({
      name: "admin",
      connectionString: POSTGRES_URL,
   });
   await admin.runSQL(
      `CREATE DATABASE ${db} LOCALE 'en_US.utf8' ENCODING 'UTF8' TEMPLATE template0`,
   );
   const parsed = new URL(POSTGRES_URL!);
   const inDb = new PostgresConnection({
      name: "setup",
      connectionString: POSTGRES_URL!.replace(/\/[^/]*$/, `/${db}`),
   });
   for (const sql of setup) await inDb.runSQL(sql);
   await inDb.close();
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
}

const CI = `CREATE COLLATION dbs_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)`;
const ROWS = `(1, 'electronics'), (2, 'books'), (3, 'books'), (4, 'Books'), (5, 'toys')`;

/**
 * A gated `#(materialize)` table on an en_US default answers a caller's
 * inline `extend` that adds no `where:` and no gate from its table, as the
 * named source does: it reads that source's rows under its gates.
 *
 * A composite's scope reads whichever member the whole query resolves to,
 * which a probe of one field cannot tell: a composite over a case-insensitive
 * member and a byte-exact one answers live, also once the second member's
 * stored column is altered to the first's collation (where the identities
 * would match and the stored member would admit `Books`).
 */
describe.skipIf(!POSTGRES_URL)(
   "an inline extend and a composite over Postgres database tables",
   () => {
      const MORE = `##! experimental { persistence composite_sources givens access_modifiers }
given: G :: string
source: raw is warehouse.sql("""SELECT * FROM (VALUES ${ROWS}) AS t(id, category)""")
source: raw_ci is warehouse.sql("""
  SELECT id, category COLLATE dbs_ci AS category FROM (VALUES ${ROWS}) AS t(id, category)
""")
#(access_filter) category = $G
#(materialize) name="dbs_tg"
source: t_g is raw extend { primary_key: id }
#(materialize) name="dbs_cma"
source: cm_a is raw_ci extend { primary_key: id }
#(materialize) name="dbs_cmb"
source: cm_b is raw extend {
  primary_key: id
  dimension: bonus is 1
}
source: comp_x is compose(cm_a, cm_b) extend { where: category = 'books' }
`;
      const GATED: [string, string][] = [
         ["named", "run: t_g -> { select: id; order_by: id }"],
         [
            "inline extend",
            "run: t_g extend { dimension: z is 1 } -> { select: id, z; order_by: id }",
         ],
      ];
      const CALLERS = [{ G: "books" }, { G: "Books" }];
      // Member B answers it: only B has `bonus`.
      const COMPOSITE =
         "run: comp_x -> { select: id, category, bonus; order_by: id }";
      let restore: () => void;
      let f: Fixture;
      let drop: (() => Promise<void>) | undefined;
      const live = new Map<string, unknown>();
      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "off",
            },
         }).restore;
         await freshLoadPool();
         const w = await postgresWarehouse([CI]);
         drop = w.drop;
         f = await fixture(w.config, MORE);
         for (const [name, q] of GATED) {
            for (const [i, g] of CALLERS.entries()) {
               live.set(`${name}${i}`, (await ask(f, q, g)).rows);
            }
         }
         live.set("composite", (await ask(f, COMPOSITE)).rows);
         const run = await build(f);
         expect(run.status).toBe("MANIFEST_FILE_READY");
      }, 300_000);
      afterAll(async () => {
         await f?.cleanup();
         await drop?.();
         restore();
         await freshLoadPool();
      });

      for (const [name, q] of GATED) {
         it(`a gated table's ${name} query is served from its table, equal to live`, async () => {
            for (const [i, g] of CALLERS.entries()) {
               const got = await ask(f, q, g);
               expect(got.rows).toEqual(live.get(`${name}${i}`) as never);
               expect(got.kind).toBe("materialized");
            }
         });
      }

      it("answers a composite's scope live, before and after its member is altered to the other's collation", async () => {
         const before = await ask(f, COMPOSITE);
         expect(before.rows).toEqual(live.get("composite") as never);
         expect(before.kind).toBe("live");
         const warehouse = await f.env.getMalloyConnection("warehouse");
         await warehouse.runSQL(
            `ALTER TABLE dbs_cmb ALTER COLUMN category TYPE text COLLATE dbs_ci`,
         );
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         const after = await ask(f, COMPOSITE);
         expect(after.rows).toEqual(live.get("composite") as never);
         expect(after.kind).toBe("live");
      });
   },
);
