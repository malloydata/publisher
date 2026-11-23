// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted view or named query over a `#(materialize)` pair table (an
 * inlined join_many) is built from that table with its join put back
 * (plan/pair_reroot.ts), never by recomputing its upstream from raw.
 *
 *  - The drop-raw proof: the pair table is built alone, every raw table is
 *    then emptied out of band, and the downstream, built next, still builds
 *    with `upstreamReuse: "reused"`, names the pair table in `builtFrom`, and
 *    holds exactly what live answered before the raw rows went: childless
 *    parents, model and inline aggregates, `count`, `sum`, `avg`, `min` and
 *    `max`, through one join and through a path of two.
 *  - A fresh package builds the pair table and the view in one run.
 *  - A pair table whose leaf key check failed refuses the view
 *    (`schema_incompatible`), naming the upstream and the check.
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
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/**
 * The raw tables, by name. Order 3 has no item (a childless parent); item
 * 101 has no option (a childless middle row).
 */
const RAW = (duplicateLeaf: boolean): Record<string, string> => ({
   pv_orders: `SELECT * FROM (VALUES
    (1, 'x', CAST(10.0 AS DECIMAL(10,1))),
    (2, 'x', CAST(20.0 AS DECIMAL(10,1))),
    (3, 'y', CAST(5.0 AS DECIMAL(10,1))),
    (4, 'z', CAST(7.0 AS DECIMAL(10,1)))
  ) AS t(id, cust, amount)`,
   pv_items: `SELECT * FROM (VALUES
    (100, 1, CAST(3.00 AS DECIMAL(10,2))),
    (101, 1, CAST(4.50 AS DECIMAL(10,2))),
    (102, 2, CAST(6.00 AS DECIMAL(10,2))),
    (${duplicateLeaf ? 102 : 103}, 4, CAST(1.25 AS DECIMAL(10,2)))
  ) AS t(item_id, order_id, price)`,
   pv_opts: `SELECT * FROM (VALUES
    (1000, 100, CAST(0.50 AS DECIMAL(10,2))),
    (1001, 100, CAST(0.25 AS DECIMAL(10,2))),
    (1002, 102, CAST(1.00 AS DECIMAL(10,2)))
  ) AS t(option_id, item_id, extra)`,
});

const HEAD = `##! experimental { persistence }
`;

/** One join: orders through items. */
const ONE_LEVEL = `${HEAD}source: items is warehouse.table('__T__pv_items') extend {
  primary_key: item_id
}

#(materialize) inline=["items"]
source: orders is warehouse.table('__T__pv_orders') extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  measure: total is amount.sum()
  measure: item_count is items.count()
  measure: price_sum is items.price.sum()
}
`;

/** The measures every one-join downstream groups, model and inline alike. */
const ONE_LEVEL_AGGREGATES = `group_by: cust
    aggregate:
      total
      n is count()
      item_count
      price_sum
      avg_price is items.price.avg()
      min_price is items.price.min()
      max_price is items.price.max()
      avg_amount is amount.avg()
      inline_items is items.count()`;

const ONE_LEVEL_COLUMNS = [
   "total",
   "n",
   "item_count",
   "price_sum",
   "avg_price",
   "min_price",
   "max_price",
   "avg_amount",
   "inline_items",
];

/** A path of two joins: orders through items through options. */
const TWO_LEVEL = `${HEAD}source: opts is warehouse.table('__T__pv_opts') extend {
  primary_key: option_id
}

source: items is warehouse.table('__T__pv_items') extend {
  primary_key: item_id
  join_many: options is opts on options.item_id = item_id
}

#(materialize) inline=["items.options"]
source: orders is warehouse.table('__T__pv_orders') extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  measure: total is amount.sum()
}

source: s is orders extend {
  #@ persist name="__T__pv_v2"
  view: by_cust is {
    group_by: cust
    aggregate:
      total
      n is count()
      item_count is items.count()
      option_count is items.options.count()
      price_sum is items.price.sum()
      extra_sum is items.options.extra.sum()
      avg_extra is items.options.extra.avg()
      min_extra is items.options.extra.min()
      max_extra is items.options.extra.max()
  }
}
`;

const TWO_LEVEL_COLUMNS = [
   "total",
   "n",
   "item_count",
   "option_count",
   "price_sum",
   "extra_sum",
   "avg_extra",
   "min_extra",
   "max_extra",
];

interface Mechanism {
   label: string;
   model: string;
   downstream: RegExp;
   /** The downstream's result, before the text stage. */
   run: string;
   servedBy: string;
   columns: string[];
}

const MECHANISMS: Mechanism[] = [
   {
      label: "persisted view through one join",
      model: `${ONE_LEVEL}
source: s is orders extend {
  #@ persist name="__T__pv_v"
  view: by_cust is {
    ${ONE_LEVEL_AGGREGATES}
  }
}
`,
      downstream: /^s__by_cust__view$/,
      run: "s -> by_cust",
      servedBy: "view",
      columns: ONE_LEVEL_COLUMNS,
   },
   {
      label: "persisted query through one join",
      model: `${ONE_LEVEL}
#@ persist name="__T__pv_q"
query: q is orders -> {
    ${ONE_LEVEL_AGGREGATES}
}
`,
      downstream: /^q__query$/,
      run: "q",
      servedBy: "query",
      columns: ONE_LEVEL_COLUMNS,
   },
   {
      label: "persisted view through a path of two joins",
      model: TWO_LEVEL,
      downstream: /^s__by_cust__view$/,
      run: "s -> by_cust",
      servedBy: "view",
      columns: TWO_LEVEL_COLUMNS,
   },
];

/** `run` with every column rendered as the warehouse renders it as text. */
const asText = (m: Pick<Mechanism, "run" | "columns">) =>
   `run: ${m.run} -> { group_by: cust, ${m.columns
      .map((c) => `${c}_text is ${c}::string`)
      .join(", ")}; order_by: cust }`;

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
   contract?: { keys?: Record<string, { unique: boolean; level: string }> };
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
      namePrefix: "",
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
              // Postgres reads a table's columns only by a qualified name.
              namePrefix: "public.",
              // Its runSQL reads each row from a `row` JSON column.
              rowsSQL: (sql: string) =>
                 `SELECT row_to_json(t) AS row FROM (${sql}) t`,
              warehouse: async () => {
                 const db = `pv_${process.pid}_${Date.now()}`;
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
         fs.mkdtempSync(path.join(os.tmpdir(), "pv-")),
      );
      const envPath = path.join(this.root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         {
            "m.malloy": this.model
               .replace(
                  /__T__pv_(orders|items|opts)/g,
                  `${this.tier.tablePrefix}pv_$1`,
               )
               .replace(/__T__/g, this.tier.namePrefix),
         },
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

   /** A table's rows, each column cast to text, ordered by `cust`. */
   async storedRows(table: string, columns: string[]): Promise<unknown[]> {
      const warehouse = await this.warehouse();
      const result = await warehouse.runSQL(
         this.tier.rowsSQL(
            `SELECT cust, ${columns
               .map((c) => `CAST(${c} AS VARCHAR) AS ${c}_text`)
               .join(", ")} FROM ${table} ORDER BY cust`,
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
   describe(`a downstream builds from its upstream's pair table with every raw table emptied (${mechanism.label}, ${tier.label})`, () => {
      const f = new Fixture(tier, mechanism.model);
      let first: Materialization;
      let second: Materialization;
      let liveBefore: Answer;
      let served: Answer;

      beforeAll(async () => {
         await f.open();
         liveBefore = await f.ask(asText(mechanism));
         first = await f.build({ sourceNames: ["orders"] });
         // Out of band: a downstream that recomputed from raw would hold no
         // rows.
         await f.emptyRaw();
         second = await f.build({ strictUpstreams: true });
         served = await f.ask(asText(mechanism));
      }, 300_000);
      afterAll(() => f.close());

      it("records the pair table's leaf key check as passed", () => {
         expect(first.status).toBe("MANIFEST_FILE_READY");
         const pair = Object.values(entriesOf(first)).filter(
            (e) => e.sourceName?.startsWith("orders") ?? false,
         );
         expect(pair).toHaveLength(1);
         const leaf = mechanism.columns.includes("option_count")
            ? "items.options.option_id"
            : "items.item_id";
         expect(pair[0].contract?.keys?.[leaf]).toEqual({
            unique: true,
            level: "leaf",
         });
      });

      it("builds the downstream, reusing the pair table, and names it in builtFrom", () => {
         expect([second.status, (second as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const [address, upEntry] = Object.entries(entriesOf(first)).find(
            ([, e]) => e.sourceName?.startsWith("orders") ?? false,
         )!;
         const downstream = Object.values(entriesOf(second)).filter((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         );
         expect(downstream).toHaveLength(1);
         expect(downstream[0].upstreamReuse).toBe("reused");
         expect(downstream[0].builtFrom).toEqual([
            {
               sourceEntityId: address,
               physicalTableName: upEntry.physicalTableName,
               builtAt: upEntry.builtAt,
            },
         ]);
      });

      it("holds live's rows in its own table, read directly, as text", async () => {
         const downstream = Object.values(entriesOf(second)).find((e) =>
            mechanism.downstream.test(e.sourceName ?? ""),
         )!;
         expect(
            await f.storedRows(downstream.physicalTableName, mechanism.columns),
         ).toEqual(liveBefore.rows as unknown[]);
         // Childless parents included: one group has no child at all.
         expect((liveBefore.rows as unknown[]).length).toBe(3);
      });

      it("serves its table, equal to live before the raw rows went", () => {
         const kinds: (string | undefined)[] =
            tier.label === "DuckDB"
               ? [mechanism.servedBy]
               : [mechanism.servedBy, "materialized"];
         expect(kinds).toContain(served.kind);
         expect(served.rows).toEqual(liveBefore.rows);
      });
   });
}

for (const tier of tiers) {
   describe(`a fresh package builds a pair table and the view over it in one run (${tier.label})`, () => {
      const f = new Fixture(tier, MECHANISMS[0].model);
      let run: Materialization;
      let live: Answer;

      beforeAll(async () => {
         await f.open();
         live = await f.ask(asText(MECHANISMS[0]));
         run = await f.build({ strictUpstreams: true });
      }, 300_000);
      afterAll(() => f.close());

      it("builds the view after the pair table, over it, equal to live", async () => {
         expect([run.status, (run as { error?: unknown }).error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const entries = Object.entries(entriesOf(run));
         const [address] = entries.find(
            ([, e]) => e.sourceName?.startsWith("orders") ?? false,
         )!;
         const view = entries.find(([, e]) =>
            MECHANISMS[0].downstream.test(e.sourceName ?? ""),
         )![1];
         expect(view.upstreamReuse).toBe("reused");
         expect(view.builtFrom?.map((b) => b.sourceEntityId)).toEqual([
            address,
         ]);
         expect(
            await f.storedRows(view.physicalTableName, MECHANISMS[0].columns),
         ).toEqual(live.rows as unknown[]);
      });
   });

   describe(`a pair table whose leaf key check failed refuses the view over it (${tier.label})`, () => {
      // Item 102 under two orders: the leaf's key is held twice.
      const f = new Fixture(tier, MECHANISMS[0].model, true);
      let run: Materialization;

      beforeAll(async () => {
         await f.open();
         run = await f.build({});
      }, 300_000);
      afterAll(() => f.close());

      it("builds the pair table, recording the failed check", () => {
         const pair = Object.values(entriesOf(run)).find(
            (e) => e.sourceName?.startsWith("orders") ?? false,
         );
         expect(pair?.contract?.keys?.["items.item_id"]).toEqual({
            unique: false,
            level: "leaf",
         });
      });

      it("refuses the view as schema_incompatible, naming the upstream and the check, and never builds it from raw", () => {
         // Recorded as failed: an entry with the error and no build.
         const view = Object.values(entriesOf(run)).filter((e) =>
            MECHANISMS[0].downstream.test(e.sourceName ?? ""),
         ) as (Entry & { error?: string })[];
         expect(view).toHaveLength(1);
         expect(view[0].error).toEqual(expect.any(String));
         expect(view[0].builtAt).toBeUndefined();
         expect(view[0].contract).toBeUndefined();
         const failures = failuresOf(run).filter((x) =>
            MECHANISMS[0].downstream.test(x.sourceName ?? ""),
         );
         expect(failures).toHaveLength(1);
         expect(failures[0].code).toBe("schema_incompatible");
         expect(failures[0].reason).toMatch(
            /upstream 'orders''s pair table: the leaf key check of its inlined join_many.*found the key 'items\.item_id' held more than once or NULL/,
         );
      });
   });
}
