// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Extension chains, case 2: an incremental `#(materialize)` extension of a
 * `#(materialize)` parent is refreshed over the parent's stored table, never
 * by recomputing the parent from raw, and follows the parent's refresh:
 *  - drop-raw: the parent built, its raw rows emptied, the extension still
 *    builds over the parent's table, names it (and its boundary) in
 *    `builtFrom`, and holds the rows live held;
 *  - two runs: the parent advances by a delta that re-reads a late row below
 *    the extension's own lower bound, and each extension follows it: an
 *    incremental one by delta (range replace and merge), nothing missed or
 *    double counted; a full one rebuilt; one on another watermark rebuilt
 *    (`upstream_watermark_differs`);
 *  - orchestrated: a worker with no bound manifest deltas from the
 *    generation the echoed ledger carries; with it stripped, the extension
 *    rebuilds and the package warns; one that missed a parent refresh
 *    rebuilds (`upstream_generation_skipped`);
 *  - a `storage=` extension stacked on its parent in one DuckLake
 *    destination deltas there.
 * On Postgres, when `PUBLISHER_TEST_POSTGRES_URL` names one: incremental
 * refresh reads its range out of Postgres, Snowflake or BigQuery only.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { logger } from "../../src/logger";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const DAY = 86_400_000;
const NOW = Date.now();

/** A UTC timestamp `ms` before the suite started, as SQL text. */
const at = (ms: number) =>
   new Date(NOW - ms).toISOString().slice(0, 19).replace("T", " ");

/** One raw row. */
interface Row {
   id: number;
   ts: string;
   amt: number;
}

const FIRST: Row[] = [
   { id: 1, ts: at(10 * DAY), amt: 2 },
   { id: 2, ts: at(5 * DAY), amt: 7 },
   { id: 3, ts: at(3 * DAY), amt: 11 },
];
/** New, and late: stamped below every extension's newest stored row. */
const SECOND: Row[] = [
   { id: 4, ts: at(3_600_000), amt: 13 },
   { id: 5, ts: at(4 * DAY), amt: 17 },
];

const valuesOf = (rows: Row[]) =>
   rows
      .map(
         (r) =>
            `(${r.id}, CAST('${r.ts}' AS TIMESTAMP), CAST(${r.amt} AS DOUBLE PRECISION))`,
      )
      .join(", ");

/** The model; `__S__` is each table's `storage=`, empty on the database tier. */
const MODEL = `##! experimental { persistence }
source: raw is warehouse.table('__RAW__')

#(materialize) name="__P__ev" refresh="incremental" watermark="ts" merge_key="id" lookback="7 days"__S__
source: ev is raw

#(materialize) name="__P__ext_rr" refresh="incremental" watermark="ts"__S__
source: ext_rr is ev extend { dimension: big is amt > 5 }

#(materialize) name="__P__ext_mg" refresh="incremental" watermark="ts" merge_key="id"__S__
source: ext_mg is ev extend { dimension: small is amt <= 5 }

#(materialize) name="__P__ext_full"__S__
source: ext_full is ev extend { dimension: twice is amt * 2 }

#(materialize) name="__P__ext_wm" refresh="incremental" watermark="id"__S__
source: ext_wm is ev extend { dimension: neg is 0 - amt }
`;

const EXTENSIONS = ["ext_rr", "ext_mg", "ext_full", "ext_wm"];

interface Entry {
   sourceName?: string;
   physicalTableName: string;
   storageDestinationName?: string;
   refresh?: string;
   upstreamReuse?: string;
   builtAt?: string;
   ledger?: Record<string, unknown> & {
      parent?: {
         sourceEntityId: string;
         advancedBy?: string;
         builtAt?: string;
         coveredThrough?: string;
         coveredThroughType?: string;
      };
   };
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
      coveredThrough?: string;
   }[];
}

const entriesOf = (m: Materialization): Record<string, Entry> =>
   (m.manifest as { entries?: Record<string, Entry> }).entries ?? {};
const entryNamed = (m: Materialization, name: string) =>
   Object.entries(entriesOf(m)).find(([, e]) => e.sourceName === name);

interface Tier {
   label: string;
   raw: string;
   /** A database table's name prefix (Postgres reads qualified names). */
   prefix: string;
   storage?: string;
   warehouse: () => Promise<{
      config: unknown;
      destinations: unknown[];
      drop?: () => Promise<void>;
   }>;
}

const pgParts = () => {
   const parsed = new URL(POSTGRES_URL!);
   return {
      host: parsed.hostname,
      port: Number(parsed.port || 5432),
      userName: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
   };
};

const pgDatabases = async (names: string[]) => {
   const admin = new PostgresConnection({
      name: "admin",
      connectionString: POSTGRES_URL!,
   });
   for (const n of names) await admin.runSQL(`CREATE DATABASE ${n}`);
   return async () => {
      for (const n of names) {
         await admin
            .runSQL(`DROP DATABASE IF EXISTS ${n} WITH (FORCE)`)
            .catch(() => undefined);
      }
      await admin.close();
   };
};

const POSTGRES: Tier = {
   label: "Postgres",
   raw: "public.xc_raw",
   prefix: "public.",
   warehouse: async () => {
      const db = `xc_${process.pid}_${Date.now()}`;
      const drop = await pgDatabases([db]);
      return {
         config: {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { ...pgParts(), databaseName: db },
         },
         destinations: [],
         drop,
      };
   },
};

/** A `storage=` DuckLake destination over a Postgres warehouse. */
const DUCKLAKE = (root: () => string): Tier => ({
   label: "DuckLake storage=",
   raw: "public.xc_raw",
   prefix: "",
   storage: "lake",
   warehouse: async () => {
      const catalog = `xcl_${process.pid}_${Date.now()}`;
      const db = `xcw_${process.pid}_${Date.now()}`;
      const drop = await pgDatabases([catalog, db]);
      const bucket = path.join(root(), "bucket");
      fs.mkdirSync(bucket, { recursive: true });
      return {
         config: {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { ...pgParts(), databaseName: db },
         },
         destinations: [
            {
               name: "lake",
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        ...pgParts(),
                        databaseName: catalog,
                     },
                  },
                  storage: { bucketUrl: bucket },
               },
            },
         ],
         drop,
      };
   },
});

/** One package over one warehouse, and the service that builds it. */
class Fixture {
   root = "";
   env!: Environment;
   pkg!: Package;
   repo!: ResourceRepository;
   service!: MaterializationService;
   private db?: DuckDBConnection;
   private drop?: () => Promise<void>;
   private restore?: () => void;
   /** Each "rebuild in full" warn's reason, by table. */
   seeds: { physicalTableName?: string; reasonCode?: string }[] = [];
   private unwrap?: () => void;

   constructor(private readonly tier: Tier) {}

   async open(model: string): Promise<void> {
      this.restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: this.tier.storage ? "on" : "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      const storage = await openTempStorage("xc-", { id: "env-1", name: ENV });
      this.root = storage.root;
      this.db = storage.db;
      this.repo = storage.repo;
      writePackageFiles(
         path.join(storage.envPath, PKG),
         {
            "m.malloy": model
               .replace("__RAW__", this.tier.raw)
               .replace(/__P__/g, this.tier.storage ? "" : this.tier.prefix)
               .replace(
                  /__S__/g,
                  this.tier.storage ? ` storage="${this.tier.storage}"` : "",
               ),
         },
         { manifest: { name: PKG, description: "fixture" } },
      );
      const w = await this.tier.warehouse();
      this.drop = w.drop;
      this.env = await Environment.create(
         ENV,
         storage.envPath,
         [w.config as never],
         w.destinations as never[],
      );
      await this.sql(
         `CREATE TABLE ${this.tier.raw} AS SELECT * FROM (VALUES ${valuesOf(
            FIRST,
         )}) AS t(id, ts, amt)`,
      );
      await this.env.addPackage(PKG);
      this.pkg = await this.env.getPackage(parsePackageName(PKG), false);
      this.service = serviceOverEnvironment(
         () => this.repo,
         () => this.env,
      );
      const warn = logger.warn.bind(logger);
      (logger as { warn: unknown }).warn = (
         message: string,
         meta?: Record<string, unknown>,
      ) => {
         if (message === "Rebuilding an incremental source in full") {
            this.seeds.push({
               physicalTableName: meta?.physicalTableName as string,
               reasonCode: meta?.reasonCode as string,
            });
         }
         return warn(message, meta as never);
      };
      this.unwrap = () => {
         (logger as { warn: unknown }).warn = warn;
      };
   }

   async close(): Promise<void> {
      this.unwrap?.();
      await this.env?.closeAllConnections().catch(() => undefined);
      await this.db?.close();
      await this.drop?.();
      if (this.root) fs.rmSync(this.root, { recursive: true, force: true });
      this.restore?.();
      await freshLoadPool();
   }

   async sql(text: string): Promise<void> {
      const warehouse = await this.env.getMalloyConnection("warehouse");
      await warehouse.runSQL(text);
   }

   insert(rows: Row[]): Promise<void> {
      return this.sql(
         `INSERT INTO ${this.tier.raw} SELECT * FROM (VALUES ${valuesOf(
            rows,
         )}) AS t(id, ts, amt)`,
      );
   }

   async build(opts: object): Promise<Materialization> {
      const created = await this.service.createMaterialization(ENV, PKG, opts);
      const running = (
         this.service as unknown as {
            runningAbortControllers: Map<string, unknown>;
         }
      ).runningAbortControllers;
      const deadline = Date.now() + 180_000;
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

   /** Count and sum through the source, served from its table when bound. */
   async totals(
      source: string,
   ): Promise<{ n: number; total: number; kind?: string }> {
      const result = await this.pkg.getModel("m.malloy")!.runQuery(
         unrecordedAudit(),
         {
            query: `run: ${source} -> { aggregate: n is count(), total is amt.sum() }`,
         } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      const row = (result.compactResult as Record<string, unknown>[])[0];
      return {
         n: Number(row.n),
         total: Number(row.total),
         kind: (result.path as { servedBy?: { kind?: string } } | undefined)
            ?.servedBy?.kind,
      };
   }

   /** The rows a table holds, read directly where it lives. */
   async stored(
      entry: Entry,
   ): Promise<{ n: number; total: number; ids: number[] }> {
      if (entry.storageDestinationName) {
         throw new Error("read a storage= table through the served query");
      }
      const warehouse = await this.env.getMalloyConnection("warehouse");
      const sql = `SELECT id FROM ${entry.physicalTableName} ORDER BY id`;
      const result = await warehouse.runSQL(
         this.tier.label === "Postgres"
            ? `SELECT row_to_json(t) AS row FROM (${sql}) t`
            : sql,
      );
      const ids = (result.rows as Record<string, unknown>[]).map((r) =>
         Number(r.id),
      );
      const sum = await warehouse.runSQL(
         this.tier.label === "Postgres"
            ? `SELECT row_to_json(t) AS row FROM (SELECT SUM(amt) AS total FROM ${entry.physicalTableName}) t`
            : `SELECT SUM(amt) AS total FROM ${entry.physicalTableName}`,
      );
      return {
         n: ids.length,
         total: Number((sum.rows as Record<string, unknown>[])[0].total),
         ids,
      };
   }
}

const sumOf = (rows: Row[]) => rows.reduce((s, r) => s + r.amt, 0);

// Incremental refresh reads its range out of Postgres, Snowflake or BigQuery
// only (a DuckDB source's is refused at plan, and so is every extension over
// it: extension_chain_verdict.spec.ts), so these run on Postgres.
const databaseTiers = POSTGRES_URL ? [POSTGRES] : [];

for (const tier of databaseTiers) {
   describe(`an incremental extension over its parent's table (${tier.label})`, () => {
      const f = new Fixture(tier);
      let first: Materialization;
      let second: Materialization;
      let third: Materialization;
      const seedsInSecond: typeof f.seeds = [];
      const seedsInThird: typeof f.seeds = [];
      let judgedAfterThird: unknown[] = [];

      beforeAll(async () => {
         await f.open(MODEL);
         first = await f.build({});
         await f.insert(SECOND);
         const before = f.seeds.length;
         second = await f.build({});
         seedsInSecond.push(...f.seeds.slice(before));
         // Nothing new: the extension on another watermark still rebuilds
         // once, and binding what it wrote finds nothing for the drift
         // handler, so no run is asked for and nothing loops.
         const beforeThird = f.seeds.length;
         third = await f.build({});
         seedsInThird.push(...f.seeds.slice(beforeThird));
         await f.pkg.reloadAllModels(
            f.pkg.materialization().boundDatabaseEntries() ?? {},
         );
         judgedAfterThird = f.pkg
            .materialization()
            .serveVerdicts()
            .filter(
               (v) =>
                  v.reason === "incompatible" ||
                  v.reason === "contract_missing",
            );
      }, 400_000);
      afterAll(() => f.close());

      it("seeds every table on the first run, each extension over the parent's", () => {
         expect([first.status, (first as { error?: unknown }).error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const [parentAddress, parent] = entryNamed(first, "ev")!;
         for (const name of EXTENSIONS) {
            const ext = entryNamed(first, name)![1];
            expect([name, ext.builtFrom?.map((b) => b.sourceEntityId)]).toEqual(
               [name, [parentAddress]],
            );
            // The parent's boundary as it was read.
            expect(ext.builtFrom?.[0].coveredThrough).toBe(
               parent.ledger?.coveredThrough as string,
            );
         }
         // An incremental extension's boundary names the parent build it
         // was reached over.
         expect(entryNamed(first, "ext_rr")![1].ledger?.parent).toEqual({
            sourceEntityId: parentAddress,
            advancedBy: first.id,
            builtAt: parent.builtAt,
            coveredThrough: parent.ledger?.coveredThrough as string,
            coveredThroughType: "timestamp",
         });
      });

      it("advances the parent by delta, re-reading the late row", async () => {
         expect([second.status, (second as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const parent = entryNamed(second, "ev")![1];
         expect(parent.refresh).toBe("delta");
         expect(await f.stored(parent)).toEqual({
            n: 5,
            total: sumOf([...FIRST, ...SECOND]),
            ids: [1, 2, 3, 4, 5],
         });
      });

      it("deltas each incremental extension on the parent's watermark, from no later than the parent's start", async () => {
         for (const name of ["ext_rr", "ext_mg"]) {
            const ext = entryNamed(second, name)![1];
            expect([name, ext.refresh]).toEqual([name, "delta"]);
            // The late row (id 5, below the extension's own lower bound) is
            // there, and nothing is there twice.
            expect([name, await f.stored(ext)]).toEqual([
               name,
               {
                  n: 5,
                  total: sumOf([...FIRST, ...SECOND]),
                  ids: [1, 2, 3, 4, 5],
               },
            ]);
         }
      });

      it("standalone, reads which parent build an extension last read from the bound manifest", () => {
         // The local ledger store holds no parent; the run proved the
         // generation from the manifest the first run bound.
         expect(
            seedsInSecond.filter(
               (s) => s.reasonCode === "upstream_generation_skipped",
            ),
         ).toEqual([]);
      });

      it("rebuilds a full extension, and one on another watermark, over the new parent table", async () => {
         const full = entryNamed(second, "ext_full")![1];
         expect(full.refresh).toBeUndefined();
         expect((await f.stored(full)).n).toBe(5);
         const wm = entryNamed(second, "ext_wm")![1];
         expect(wm.refresh).toBe("full");
         expect((await f.stored(wm)).ids).toEqual([1, 2, 3, 4, 5]);
         expect(
            seedsInSecond.filter(
               (s) => s.physicalTableName === wm.physicalTableName,
            ),
         ).toEqual([
            {
               physicalTableName: wm.physicalTableName,
               reasonCode: "upstream_watermark_differs",
            },
         ]);
      });

      it("rebuilds the extension on another watermark once a run, which the bind accepts", () => {
         expect([third.status, (third as { error?: unknown }).error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const wm = entryNamed(third, "ext_wm")![1];
         expect(
            seedsInThird.filter(
               (s) => s.physicalTableName === wm.physicalTableName,
            ),
         ).toEqual([
            {
               physicalTableName: wm.physicalTableName,
               reasonCode: "upstream_watermark_differs",
            },
         ]);
         // A coordinated extension is not rebuilt by it.
         expect(entryNamed(third, "ext_rr")![1].refresh).not.toBe("full");
         expect(judgedAfterThird).toEqual([]);
      });

      it("serves every extension equal to live", async () => {
         for (const name of EXTENSIONS) {
            const served = await f.totals(name);
            expect([name, served.n, served.total]).toEqual([
               name,
               5,
               sumOf([...FIRST, ...SECOND]),
            ]);
         }
      });

      it("warns at plan that the extension on another watermark rebuilds in full", () => {
         const warned = f.pkg
            .annotationFindingWarnings()
            .filter((w) => w.code === "materialize-extension-full-refresh");
         expect(warned.map((w) => w.subject)).toEqual(["ext_wm"]);
      });
   });

   describe(`an incremental extension built with its parent's raw rows gone (${tier.label})`, () => {
      const f = new Fixture(tier);
      let parentRun: Materialization;
      let extRun: Materialization;

      beforeAll(async () => {
         await f.open(MODEL);
         parentRun = await f.build({ sourceNames: ["ev"] });
         await f.sql(`DELETE FROM ${tier.raw}`);
         extRun = await f.build({ sourceNames: ["ext_rr"] });
      }, 400_000);
      afterAll(() => f.close());

      it("builds over the parent's table, names it and its boundary, and holds live's rows", async () => {
         expect([extRun.status, (extRun as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const [parentAddress, parent] = entryNamed(parentRun, "ev")!;
         const ext = entryNamed(extRun, "ext_rr")![1];
         expect(ext.builtFrom).toEqual([
            expect.objectContaining({
               sourceEntityId: parentAddress,
               physicalTableName: parent.physicalTableName,
               coveredThrough: expect.any(String),
            }),
         ]);
         expect(await f.stored(ext)).toEqual({
            n: 3,
            total: sumOf(FIRST),
            ids: [1, 2, 3],
         });
         const served = await f.totals("ext_rr");
         expect([served.n, served.total]).toEqual([3, sumOf(FIRST)]);
      });
   });

   describe(`an orchestrated incremental extension (${tier.label})`, () => {
      const f = new Fixture(tier);
      const ORCH = `##! experimental { persistence }
source: raw is warehouse.table('__RAW__')

#(materialize) refresh="incremental" watermark="ts" merge_key="id" lookback="7 days"
source: ev is raw

#(materialize) refresh="incremental" watermark="ts"
source: ext_rr is ev extend { dimension: big is amt > 5 }
`;
      const runs: Materialization[] = [];
      const seedsAt: (typeof f.seeds)[] = [];
      let boundBeforeSecond: unknown;
      let heldAfterSecond: number[] = [];
      let warnedWhenUnproven: { subject?: string; code?: string }[] = [];

      const instruct = (
         names: string[],
         ledger: unknown[],
      ): Record<string, unknown> => {
         const sources = Object.values(f.pkg.getBuildPlan()!.sources).filter(
            (s) => names.includes(s.name),
         );
         return {
            buildInstructions: sources.map((s) => ({
               sourceEntityId: s.sourceEntityId,
               sourceID: s.sourceID,
               materializedTableId: `mt-${s.name}`,
               physicalTableName: `${tier.prefix}host_${s.name}`,
               realization: "COPY",
            })),
            ledger,
         };
      };
      /** Each table's ledger entry from the newest run that reported one. */
      const echoed = (strip?: string): unknown[] => {
         const byTable = new Map<string, Entry["ledger"]>();
         for (const run of runs) {
            for (const e of Object.values(entriesOf(run))) {
               if (e.ledger) byTable.set(e.physicalTableName, e.ledger);
            }
         }
         return [...byTable].map(([table, ledger]) => {
            if (!table.endsWith(strip ?? "\u0000")) return ledger;
            const { parent: _parent, ...rest } = ledger!;
            return rest;
         });
      };
      const step = async (names: string[], ledger: unknown[]) => {
         const before = f.seeds.length;
         // A host echoes only the entries of the tables it instructs.
         const tables = new Set(names.map((n) => `${tier.prefix}host_${n}`));
         runs.push(
            await f.build(
               instruct(
                  names,
                  ledger.filter((l) =>
                     tables.has(
                        (l as { physicalTableName: string }).physicalTableName,
                     ),
                  ),
               ),
            ),
         );
         seedsAt.push(f.seeds.slice(before));
      };

      beforeAll(async () => {
         await f.open(ORCH);
         await step(["ev", "ext_rr"], []);
         await f.insert(SECOND);
         // No manifest is bound on this worker: a host binds, not a run.
         boundBeforeSecond = f.pkg.materialization().boundDatabaseEntries();
         await step(["ev", "ext_rr"], echoed());
         heldAfterSecond = (await f.stored(entryNamed(runs[1], "ext_rr")![1]))
            .ids;
         await f.insert([{ id: 6, ts: at(1_800_000), amt: 19 }]);
         // A host that dropped the ledger's `parent`: nothing proves it.
         await step(["ev", "ext_rr"], echoed("host_ext_rr"));
         warnedWhenUnproven = f.pkg
            .materialization()
            .unprovenGenerationWarnings();
         await f.insert([{ id: 7, ts: at(900_000), amt: 23 }]);
         // The parent refreshed without the extension...
         await step(["ev"], echoed());
         await f.insert([{ id: 8, ts: at(600_000), amt: 29 }]);
         // ...which then missed a generation.
         await step(["ev", "ext_rr"], echoed());
      }, 600_000);
      afterAll(() => f.close());

      const ext = (i: number) => entryNamed(runs[i], "ext_rr")![1];
      const all = () => [
         ...FIRST,
         ...SECOND,
         { id: 6, ts: "", amt: 19 },
         { id: 7, ts: "", amt: 23 },
         { id: 8, ts: "", amt: 29 },
      ];

      it("deltas on a worker with no bound manifest, from the echoed ledger's parent", async () => {
         for (const run of runs) {
            expect([run.status, (run as { error?: unknown }).error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         }
         expect(Object.keys(boundBeforeSecond ?? {})).toEqual([]);
         expect(ext(1).refresh).toBe("delta");
         expect(seedsAt[1]).toEqual([]);
         expect(heldAfterSecond).toEqual([1, 2, 3, 4, 5]);
         // Which run last changed the parent's table, as the extension read it.
         expect(ext(1).ledger?.parent?.advancedBy).toBe(
            entryNamed(runs[1], "ev")![1].ledger?.advancedBy as string,
         );
      });

      it("rebuilds, loudly, when neither the ledger nor a bound manifest names the parent build", () => {
         expect(ext(2).refresh).toBe("full");
         expect(seedsAt[2].map((s) => s.reasonCode)).toEqual([
            "upstream_generation_skipped",
         ]);
         // The next run's ledger carries it again.
         expect(ext(2).ledger?.parent).toBeDefined();
      });

      it("rebuilds an extension that missed a parent refresh", async () => {
         expect(entryNamed(runs[3], "ext_rr")).toBeUndefined();
         expect(ext(4).refresh).toBe("full");
         expect(seedsAt[4].map((s) => s.reasonCode)).toEqual([
            "upstream_generation_skipped",
         ]);
         const held = await f.stored(ext(4));
         expect(held.ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
         expect(held.total).toBe(sumOf(all()));
      });

      it("warns, naming the extension, until a run proves it again", () => {
         expect(warnedWhenUnproven.map((w) => [w.subject, w.code])).toEqual([
            ["ext_rr", "materialize-extension-generation-unproven"],
         ]);
         // The newest run's reseed had a record (a missed generation), so
         // the package names no unproven extension now.
         expect(f.pkg.materialization().unprovenGenerationWarnings()).toEqual(
            [],
         );
      });
   });
}

if (POSTGRES_URL) {
   describe("an incremental storage= extension stacked on its parent in one DuckLake destination", () => {
      let root = "";
      const f = new Fixture(DUCKLAKE(() => root));
      let second: Materialization;

      beforeAll(async () => {
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(fs.realpathSync("/tmp"), "xcl-")),
         );
         await f.open(MODEL.replace(/name="__P__[a-z_]+" /g, ""));
         await f.build({});
         await f.insert(SECOND);
         second = await f.build({});
      }, 400_000);
      afterAll(async () => {
         await f.close();
         fs.rmSync(root, { recursive: true, force: true });
      });

      it("deltas in the destination over the parent's table, equal to live", async () => {
         expect([second.status, (second as { error?: unknown }).error]).toEqual(
            ["MANIFEST_FILE_READY", null],
         );
         const [parentAddress] = entryNamed(second, "ev")!;
         for (const name of ["ext_rr", "ext_mg"]) {
            const ext = entryNamed(second, name)![1];
            expect([name, ext.storageDestinationName, ext.refresh]).toEqual([
               name,
               "lake",
               "delta",
            ]);
            expect(ext.upstreamReuse).toBe("reused");
            expect(ext.builtFrom?.map((b) => b.sourceEntityId)).toEqual([
               parentAddress,
            ]);
         }
         for (const name of EXTENSIONS) {
            const served = await f.totals(name);
            expect([name, served.n, served.total]).toEqual([
               name,
               5,
               sumOf([...FIRST, ...SECOND]),
            ]);
         }
      });
   });
}
