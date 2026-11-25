// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `#(materialize)` extension of a parent whose build leaves a current-time
 * `where:` holding an upper bound out is built over the parent's table, the
 * parent's re-root leaving that term out too, so the extension's table holds
 * every row the parent's does and each read applies the window again: never
 * by recomputing the parent from raw. The drop-raw proof: the parent is built
 * alone, its raw table is then emptied out of band, and the extension, built
 * next, still builds with `upstreamReuse: "reused"`, names the parent's table
 * in `builtFrom`, holds every row the parent holds, and serves live's rows.
 *
 * An extension of a parent whose `where:` reads a given is refused at plan
 * (`upstream_table_unusable`): the extension's own rows depend on the
 * caller's given, so one shared table cannot serve every caller. With the raw table emptied, callers with different
 * givens, under an access filter on the extension (one filtered to nothing),
 * are each served exactly the rows live gave them, from the parent's table.
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

/** Two regions, each with one row older than the window and one far ahead. */
const RAW = `SELECT * FROM (VALUES
    (1, 'east', CAST(10.5 AS DECIMAL(10,1)), TIMESTAMP '2020-01-01 00:00:00'),
    (2, 'west', CAST(20.0 AS DECIMAL(10,1)), TIMESTAMP '2020-02-01 00:00:00'),
    (3, 'east', CAST(5.5 AS DECIMAL(10,1)), TIMESTAMP '2100-01-01 00:00:00'),
    (4, 'west', CAST(3.0 AS DECIMAL(10,1)), TIMESTAMP '2100-02-01 00:00:00'),
    (5, 'east', CAST(8.0 AS DECIMAL(10,1)), TIMESTAMP '2020-03-01 00:00:00')
  ) AS t(id, region, amount, ts)`;

/** The extension's rows, per region, as text. */
const QUERY = `run: ext -> {
  group_by: region
  aggregate: n is count(), amount_sum is amount.sum(), big_n is count() { where: big }
} -> {
  select: region, n_text is n::string, amount_text is amount_sum::string, big_text is big_n::string
  order_by: region
}`;

interface Caller {
   label: string;
   givens?: Record<string, unknown>;
   /** Live's rows for this caller, as text. */
   live: unknown[];
}

interface Mechanism {
   label: string;
   model: string;
   /**
    * The extension is refused at plan, as `upstream_table_unusable` (a
    * given-reading parent: its own rows depend on the caller's given): it
    * has no table, and each caller is served from the parent's table under
    * the term and the extension's access filter.
    */
   refused?: string;
   callers: Caller[];
   /** Every row the parent's table holds, per region: the extension's too. */
   stored: { region: string; n_text: string }[];
}

const ALL_ROWS = [
   { region: "east", n_text: "3" },
   { region: "west", n_text: "2" },
];

const MECHANISMS: Mechanism[] = [
   {
      label: "a parent's upper-bound window",
      model: `##! experimental { persistence }
source: raw is warehouse.table('__T__xt_raw')

#(materialize)
source: older is raw extend {
  where: ts < now - 30 days
}

#(materialize)
source: ext is older extend {
  dimension: big is amount > 6
}
`,
      callers: [
         {
            label: "any caller",
            live: [
               {
                  region: "east",
                  n_text: "2",
                  amount_text: "18.5",
                  big_text: "2",
               },
               {
                  region: "west",
                  n_text: "1",
                  amount_text: "20.0",
                  big_text: "1",
               },
            ],
         },
      ],
      stored: ALL_ROWS,
   },
   {
      label: "a parent's given-reading where:, under an access filter",
      refused: "one shared table cannot serve every caller",
      model: `##! experimental { persistence givens }
given: REGION :: string
given: REGIONS :: string[]
source: raw is warehouse.table('__T__xt_raw')

#(materialize)
source: orders is raw extend {
  where: region = $REGION
}

#(access_filter) region in $REGIONS
#(materialize)
source: ext is orders extend {
  dimension: big is amount > 6
}
`,
      callers: [
         {
            label: "east",
            givens: { REGION: "east", REGIONS: ["east"] },
            live: [
               {
                  region: "east",
                  n_text: "3",
                  amount_text: "24.0",
                  big_text: "2",
               },
            ],
         },
         {
            label: "west",
            givens: { REGION: "west", REGIONS: ["east", "west"] },
            live: [
               {
                  region: "west",
                  n_text: "2",
                  amount_text: "23.0",
                  big_text: "1",
               },
            ],
         },
         {
            label: "east, admitted to west only",
            givens: { REGION: "east", REGIONS: ["west"] },
            live: [],
         },
      ],
      stored: ALL_ROWS,
   },
];

interface Answer {
   rows: unknown;
   kind?: string;
}

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   upstreamReuse?: string;
   builtAt?: string;
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
   }[];
}

const entriesOf = (m: Materialization): Record<string, Entry> =>
   (m.manifest as { entries?: Record<string, Entry> }).entries ?? {};

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
                 const db = `xt_${process.pid}_${Date.now()}`;
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
         fs.mkdtempSync(path.join(os.tmpdir(), "xt-")),
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
      await warehouse.runSQL(
         `CREATE TABLE ${this.tier.tablePrefix}xt_raw AS ${RAW}`,
      );
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

   /** Empties the raw table out of band. */
   async emptyRaw(): Promise<void> {
      const warehouse = await this.warehouse();
      await warehouse.runSQL(`DELETE FROM ${this.tier.tablePrefix}xt_raw`);
   }

   async ask(query: string, givens?: Record<string, unknown>): Promise<Answer> {
      const result = await this.pkg
         .getModel("m.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query, ...(givens ? { givens } : {}) } as never,
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

   /** The extension's table, per region, as text: every caller's rows. */
   async storedRows(table: string): Promise<unknown[]> {
      const warehouse = await this.warehouse();
      const result = await warehouse.runSQL(
         this.tier.rowsSQL(
            `SELECT region, CAST(COUNT(*) AS VARCHAR) AS n_text ` +
               `FROM ${table} GROUP BY region ORDER BY region`,
         ),
      );
      return (result.rows as Record<string, unknown>[]).map((r) => ({
         region: String(r["region"]),
         n_text: String(r["n_text"]),
      }));
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
   describe(`an extension builds over its parent's table, the parent's left-out term left out, with the raw table emptied (${mechanism.label}, ${tier.label})`, () => {
      const f = new Fixture(tier, mechanism.model);
      let first: Materialization;
      let second: Materialization;
      let planned: Awaited<ReturnType<typeof compilePackageBuildPlan>>;
      const liveBefore = new Map<string, Answer>();
      const served = new Map<string, Answer>();

      beforeAll(async () => {
         await f.open();
         for (const c of mechanism.callers) {
            liveBefore.set(c.label, await f.ask(QUERY, c.givens));
         }
         const parent = mechanism.model.includes("source: older")
            ? "older"
            : "orders";
         first = await f.build({ sourceNames: [parent] });
         // Out of band: an extension that recomputed its parent from raw
         // would hold no rows.
         await f.emptyRaw();
         second = await f.build({});
         planned = await compilePackageBuildPlan(f.pkg);
         for (const c of mechanism.callers) {
            served.set(c.label, await f.ask(QUERY, c.givens));
         }
      }, 300_000);
      afterAll(() => f.close());

      if (mechanism.refused) {
         it("refuses the extension at plan, and never builds it over the raw chain", () => {
            expect(first.status).toBe("MANIFEST_FILE_READY");
            expect(second.status).toBe("MANIFEST_FILE_READY");
            expect(
               Object.values(entriesOf(second)).filter(
                  (e) => e.sourceName === "ext",
               ),
            ).toEqual([]);
            const finding = (planned.annotationFindings ?? []).find(
               (x) => x.code === "materialize-not-built" && x.subject === "ext",
            );
            expect(finding?.refused?.reason).toBe("upstream_table_unusable");
            expect(finding?.message).toContain(mechanism.refused!);
         });
      } else {
         it("builds the extension, reusing the parent's table, and names it in builtFrom", () => {
            expect(first.status).toBe("MANIFEST_FILE_READY");
            expect([
               second.status,
               (second as { error?: unknown }).error,
            ]).toEqual(["MANIFEST_FILE_READY", null]);
            const [address, upEntry] = Object.entries(entriesOf(first)).find(
               ([, e]) => e.sourceName !== "ext",
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

         it("holds every row the parent's table holds, the left-out term not applied", async () => {
            const ext = Object.values(entriesOf(second)).find(
               (e) => e.sourceName === "ext",
            )!;
            expect(await f.storedRows(ext.physicalTableName)).toEqual(
               mechanism.stored,
            );
         });
      }

      for (const c of mechanism.callers) {
         it(`serves ${c.label} exactly live's rows for that caller, from the extension's table`, () => {
            expect(liveBefore.get(c.label)?.rows).toEqual(c.live);
            expect(served.get(c.label)?.rows).toEqual(c.live);
            if (c.live.length > 0) {
               // The extension's table, or, refused, its parent's.
               expect(served.get(c.label)?.kind).toBe("materialized");
            }
         });
      }
   });
}
