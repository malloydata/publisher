// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` extension of a pair-table parent (an inlined join_many,
 * a path of two, or two inlined paths as blocks) is built over the parent's
 * table read back one row per parent (its join put back, plan/pair_reroot.ts),
 * never by recomputing the parent from raw.
 *
 *  - The drop-raw proof: the parent is built alone, every raw table is then
 *    emptied out of band, and the extension, built next, still builds with
 *    `upstreamReuse: "reused"`, names the parent's table in `builtFrom`, and
 *    holds one row per parent (a pair-row read would count a parent once per
 *    child), equal to live before the raw rows went.
 *  - A parent whose leaf key check failed: built in the same run as the
 *    extension, the extension is refused at build (`schema_incompatible`);
 *    once that table is bound, the plan records why, and a run that does
 *    not rebuild the parent refuses it (`upstream_table_unusable`) before
 *    building anything, so that table never fails it again; a run that
 *    rebuilds the fixed parent builds it.
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
import { compilePackageBuildPlan } from "../../src/service/materialization/plan/build_plan_compile";
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
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/**
 * The raw tables. Order 1 has two items, order 3 none (a childless parent);
 * item 100 has two options.
 */
const RAW = (duplicateLeaf: boolean): Record<string, string> => ({
   xp_orders: `SELECT * FROM (VALUES
    (1, 'x', CAST(10.0 AS DECIMAL(10,1))),
    (2, 'x', CAST(20.0 AS DECIMAL(10,1))),
    (3, 'y', CAST(5.0 AS DECIMAL(10,1))),
    (4, 'z', CAST(7.0 AS DECIMAL(10,1)))
  ) AS t(id, cust, amount)`,
   xp_items: `SELECT * FROM (VALUES
    (100, 1, CAST(3.00 AS DECIMAL(10,2))),
    (101, 1, CAST(4.50 AS DECIMAL(10,2))),
    (102, 2, CAST(6.00 AS DECIMAL(10,2))),
    (${duplicateLeaf ? 102 : 103}, 4, CAST(1.25 AS DECIMAL(10,2)))
  ) AS t(item_id, order_id, price)`,
   xp_opts: `SELECT * FROM (VALUES
    (1000, 100, CAST(0.50 AS DECIMAL(10,2))),
    (1001, 100, CAST(0.25 AS DECIMAL(10,2))),
    (1002, 102, CAST(1.00 AS DECIMAL(10,2)))
  ) AS t(option_id, item_id, extra)`,
   xp_notes: `SELECT * FROM (VALUES
    (7, 1, 'late'), (8, 2, 'gift'), (9, 2, 'late'), (10, 2, 'late')
  ) AS t(note_id, order_id, topic)`,
});

const HEAD = `##! experimental { persistence }
source: items is warehouse.table('__T__xp_items') extend {
  primary_key: item_id
}
`;

/** The extension every mechanism declares over its pair-table parent. */
const EXT = `
#(materialize)
source: ext is orders extend {
  dimension: big is amount > 6
}
`;

interface Mechanism {
   label: string;
   model: string;
   /** The parent's leaf key check, as its contract records it. */
   leaf: string;
}

const MECHANISMS: Mechanism[] = [
   {
      label: "through one join",
      model: `${HEAD}
#(materialize) inline=["items"]
source: orders is warehouse.table('__T__xp_orders') extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  measure: total is amount.sum()
}
${EXT}`,
      leaf: "items.item_id",
   },
   {
      label: "through a path of two joins",
      model: `${HEAD}
source: opts is warehouse.table('__T__xp_opts') extend {
  primary_key: option_id
}

source: items_opts is items extend {
  join_many: options is opts on options.item_id = item_id
}

#(materialize) inline=["items.options"]
source: orders is warehouse.table('__T__xp_orders') extend {
  primary_key: id
  join_many: items is items_opts on items.order_id = id
  measure: total is amount.sum()
}
${EXT}`,
      leaf: "items.options.option_id",
   },
   {
      label: "of two inlined paths (blocks)",
      model: `${HEAD}
source: notes is warehouse.table('__T__xp_notes') extend {
  primary_key: note_id
}

#(materialize) inline=["items", "notes"]
source: orders is warehouse.table('__T__xp_orders') extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  join_many: notes is notes on notes.order_id = id
  measure: total is amount.sum()
}
${EXT}`,
      leaf: "notes.note_id",
   },
];

/** The extension's rows, per customer, as text: one row per parent. */
const QUERY = `run: ext -> {
  group_by: cust
  aggregate: n is count(), amount_sum is total, big_n is count() { where: big }
} -> {
  select: cust, n_text is n::string, amount_text is amount_sum::string, big_text is big_n::string
  order_by: cust
}`;

interface Answer {
   rows: unknown;
   kind?: string;
}

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   upstreamReuse?: string;
   builtAt?: string;
   error?: string;
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
   }[];
   contract?: { keys?: Record<string, { unique: boolean; level?: string }> };
}

const entriesOf = (m: Materialization): Record<string, Entry> =>
   (m.manifest as { entries?: Record<string, Entry> }).entries ?? {};

const failuresOf = (
   m: Materialization,
): { sourceName?: string; code?: string; reason?: string }[] =>
   Object.values(
      (
         m.manifest as {
            failures?: Record<
               string,
               { sourceName?: string; code?: string; reason?: string }
            >;
         }
      ).failures ?? {},
   );

const tiers = [
   {
      label: "DuckDB",
      tablePrefix: "main.",
      rowsSQL: (sql: string) => sql,
      warehouse: async () => ({
         config: {
            name: "warehouse",
            type: "duckdb",
            duckdbConnection: { setupSQL: "SELECT 1" },
         } as unknown,
         drop: undefined as undefined | (() => Promise<void>),
      }),
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              tablePrefix: "public.",
              // Its runSQL reads each row from a `row` JSON column.
              rowsSQL: (sql: string) =>
                 `SELECT row_to_json(t) AS row FROM (${sql}) t`,
              warehouse: async () => {
                 const db = `xp_${process.pid}_${Date.now()}`;
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
                    } as unknown,
                    drop: async () => {
                       await admin
                          .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                          .catch(() => undefined);
                       await admin.close();
                    },
                 };
              },
           },
        ]
      : []),
];

type Tier = (typeof tiers)[number];

/** One package over its own warehouse, its raw tables created first. */
class Fixture {
   restore!: () => void;
   root!: string;
   env!: Environment;
   db!: DuckDBConnection;
   repo!: ResourceRepository;
   service!: MaterializationService;
   pkg!: Package;
   drop: (() => Promise<void>) | undefined;

   constructor(
      private readonly tier: Tier,
      private readonly model: string,
      private readonly duplicateLeaf = false,
   ) {}

   async open(): Promise<void> {
      this.restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      this.root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "xp-")),
      );
      const envPath = path.join(this.root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": this.model.replace(/__T__/g, this.tier.tablePrefix) },
         { manifest: { name: PKG, description: "fixture" } },
      );
      this.db = new DuckDBConnection(":memory:");
      await this.db.initialize();
      await initializeSchema(this.db);
      await this.db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         ["env-1", ENV, envPath],
      );
      this.repo = createDuckDBRepository(this.db);
      const w = await this.tier.warehouse();
      this.drop = w.drop;
      this.env = await Environment.create(
         ENV,
         envPath,
         [w.config as never],
         [],
      );
      const warehouse = await this.warehouse();
      for (const [table, rows] of Object.entries(RAW(this.duplicateLeaf))) {
         await warehouse.runSQL(
            `CREATE TABLE ${this.tier.tablePrefix}${table} AS ${rows}`,
         );
      }
      await this.env.addPackage(PKG);
      this.pkg = await this.env.getPackage(parsePackageName(PKG), false);
      this.service = serviceOverEnvironment(
         () => this.repo,
         () => this.env,
      );
   }

   warehouse() {
      return this.env.getMalloyConnection("warehouse");
   }

   /** Empties every raw table out of band. */
   async emptyRaw(): Promise<void> {
      const warehouse = await this.warehouse();
      for (const table of Object.keys(RAW(false))) {
         await warehouse.runSQL(`DELETE FROM ${this.tier.tablePrefix}${table}`);
      }
   }

   async ask(query: string): Promise<Answer> {
      const result = await this.pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      return {
         rows: JSON.parse(JSON.stringify(result.compactResult)),
         kind: (result.path as { servedBy?: { kind?: string } } | undefined)
            ?.servedBy?.kind,
      };
   }

   async build(opts: object): Promise<Materialization> {
      const created = await this.service.createMaterialization(ENV, PKG, opts);
      const running = (
         this.service as unknown as {
            runningAbortControllers: Map<string, unknown>;
         }
      ).runningAbortControllers;
      const deadline = Date.now() + 120_000;
      for (;;) {
         const m = await this.repo.getMaterializationById(created.id);
         if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
            while (running.has(created.id)) {
               await new Promise((r) => setTimeout(r, 10));
            }
            return (await this.repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
   }

   /** The extension's table, per customer, as text. */
   async storedRows(table: string): Promise<unknown[]> {
      const warehouse = await this.warehouse();
      const result = await warehouse.runSQL(
         this.tier.rowsSQL(
            `SELECT cust, CAST(COUNT(*) AS VARCHAR) AS n_text, ` +
               `CAST(SUM(amount) AS VARCHAR) AS amount_text, ` +
               `CAST(SUM(CASE WHEN big THEN 1 ELSE 0 END) AS VARCHAR) AS big_text ` +
               `FROM ${table} GROUP BY cust ORDER BY cust`,
         ),
      );
      return (result.rows as Record<string, unknown>[]).map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               v === null ? null : String(v),
            ]),
         ),
      );
   }

   async close(): Promise<void> {
      await this.env?.closeAllConnections().catch(() => undefined);
      await this.db?.close();
      await this.drop?.();
      fs.rmSync(this.root, { recursive: true, force: true });
      this.restore();
      await freshLoadPool();
   }
}

for (const [tier, mechanism] of tiers.flatMap((t) =>
   MECHANISMS.map((m) => [t, m] as const),
)) {
   describe(`an extension builds over its pair-table parent's table with every raw table emptied (${mechanism.label}, ${tier.label})`, () => {
      const f = new Fixture(tier, mechanism.model);
      let first: Materialization;
      let second: Materialization;
      let liveBefore: Answer;
      let served: Answer;

      beforeAll(async () => {
         await f.open();
         liveBefore = await f.ask(QUERY);
         first = await f.build({ sourceNames: ["orders"] });
         // Out of band: an extension that recomputed its parent from raw
         // would hold no rows.
         await f.emptyRaw();
         second = await f.build({ strictUpstreams: true });
         served = await f.ask(QUERY);
      }, 300_000);
      afterAll(() => f.close());

      it("records the parent's leaf key check as passed", () => {
         expect(first.status).toBe("MANIFEST_FILE_READY");
         const parent = Object.values(entriesOf(first)).filter(
            (e) => e.sourceName === "orders",
         );
         expect(parent).toHaveLength(1);
         expect(parent[0].contract?.keys?.[mechanism.leaf]).toEqual({
            unique: true,
            level: "leaf",
         });
      });

      it("builds the extension, reusing the parent's table, and names it in builtFrom", () => {
         expect([second.status, (second as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const [address, upEntry] = Object.entries(entriesOf(first)).find(
            ([, e]) => e.sourceName === "orders",
         )!;
         const ext = Object.values(entriesOf(second)).filter(
            (e) => e.sourceName === "ext",
         );
         expect(ext).toHaveLength(1);
         expect(ext[0].upstreamReuse).toBe("reused");
         expect(ext[0].builtFrom).toEqual([
            {
               sourceEntityId: address,
               physicalTableName: upEntry.physicalTableName,
               builtAt: upEntry.builtAt,
            },
         ]);
      });

      it("holds one row per parent, equal to live, in its own table read directly", async () => {
         const ext = Object.values(entriesOf(second)).find(
            (e) => e.sourceName === "ext",
         )!;
         expect(await f.storedRows(ext.physicalTableName)).toEqual(
            liveBefore.rows as unknown[],
         );
         // Order 1 has two children and order 3 none: each counted once.
         expect(liveBefore.rows).toEqual([
            { cust: "x", n_text: "2", amount_text: "30.0", big_text: "2" },
            { cust: "y", n_text: "1", amount_text: "5.0", big_text: "0" },
            { cust: "z", n_text: "1", amount_text: "7.0", big_text: "1" },
         ]);
      });

      it("serves its table, equal to live before the raw rows went", () => {
         expect(served.kind).toBe("materialized");
         expect(served.rows).toEqual(liveBefore.rows);
      });
   });
}

for (const tier of tiers) {
   describe(`an extension of a pair table whose leaf key check failed (${tier.label})`, () => {
      // Item 102 under two orders: the leaf's key is held twice.
      const f = new Fixture(tier, MECHANISMS[0].model, true);
      let first: Materialization;
      let second: Materialization;
      let replanned: Awaited<ReturnType<typeof compilePackageBuildPlan>>;

      beforeAll(async () => {
         await f.open();
         first = await f.build({});
         // The bind every run's commit, a manifest bind and a restart take.
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         replanned = await compilePackageBuildPlan(f.pkg);
         second = await f.build({});
      }, 300_000);
      afterAll(() => f.close());

      it("builds the parent, recording the failed check", () => {
         const parent = Object.values(entriesOf(first)).find(
            (e) => e.sourceName === "orders",
         );
         expect(parent?.contract?.keys?.["items.item_id"]).toEqual({
            unique: false,
            level: "leaf",
         });
      });

      it("refuses the extension at build in the run that built the parent, as schema_incompatible, never from raw", () => {
         const ext = Object.values(entriesOf(first)).filter(
            (e) => e.sourceName === "ext",
         );
         expect(ext).toHaveLength(1);
         expect(ext[0].error).toEqual(expect.any(String));
         expect(ext[0].builtAt).toBeUndefined();
         const failures = failuresOf(first).filter(
            (x) => x.sourceName === "ext",
         );
         expect(failures.map((x) => x.code)).toEqual(["schema_incompatible"]);
         expect(failures[0].reason).toMatch(
            /upstream 'orders''s pair table: the leaf key check of its inlined join_many.*found the key 'items\.item_id' held more than once or NULL/,
         );
      });

      it("records at plan, once the parent's table is bound, why the extension cannot build over it", () => {
         const ext = Object.values(replanned.tables).find(
            (t) => t.source.name === "ext",
         );
         expect(ext?.unusableUpstream).toMatch(
            /extends 'orders'.*leaf key check.*found the key 'items\.item_id' held more than once or NULL/,
         );
      });

      it("never fails it again in a run that does not rebuild the parent: it is refused, not attempted", () => {
         const failures = failuresOf(second).filter(
            (x) => x.sourceName === "ext",
         );
         expect(failures.map((x) => x.code)).not.toContain(
            "schema_incompatible",
         );
         expect(
            Object.values(entriesOf(second)).filter(
               (e) => e.sourceName === "ext" && e.builtAt !== undefined,
            ),
         ).toEqual([]);
      });
   });
}

for (const tier of tiers) {
   describe(`an extension of a pair table whose parent is rebuilt after its leaf key check failed (${tier.label})`, () => {
      const f = new Fixture(tier, MECHANISMS[0].model, true);
      let replanned: Awaited<ReturnType<typeof compilePackageBuildPlan>>;
      let fixedRun: Materialization;

      beforeAll(async () => {
         await f.open();
         await f.build({});
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         // The parent's raw rows fixed out of band, its definition unchanged:
         // its bound contract still records the failed check.
         const warehouse = await f.warehouse();
         await warehouse.runSQL(
            `DELETE FROM ${tier.tablePrefix}xp_items WHERE item_id = 102 AND order_id = 4`,
         );
         replanned = await compilePackageBuildPlan(f.pkg);
         // A run that rebuilds the parent.
         fixedRun = await f.build({ forceRefresh: true });
      }, 300_000);
      afterAll(() => f.close());

      it("keeps the extension planned, its stale reason recorded for a run that does not rebuild the parent", () => {
         expect(
            (replanned.annotationFindings ?? []).filter(
               (x) => x.code === "materialize-not-built" && x.subject === "ext",
            ),
         ).toEqual([]);
         expect(
            Object.values(replanned.tables).find((t) => t.source.name === "ext")
               ?.unusableUpstream,
         ).toEqual(expect.any(String));
      });

      it("builds the extension in the run that rebuilds the fixed parent, judged by its fresh check", () => {
         expect(failuresOf(fixedRun)).toEqual([]);
         const ext = Object.values(entriesOf(fixedRun)).find(
            (e) => e.sourceName === "ext",
         );
         expect(ext?.builtAt).toEqual(expect.any(String));
         expect(ext?.error).toBeUndefined();
      });
   });
}
