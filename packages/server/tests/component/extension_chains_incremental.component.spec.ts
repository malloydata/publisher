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
 *  - the same orchestrated round trip through the HTTP controller: the
 *    ledger entry's `parent` key leaves on the wire and comes back in the
 *    next request's `buildInstructions.ledger`, and the extension deltas;
 *  - a `storage=` extension stacked on its parent in one DuckLake
 *    destination deltas there, and one in another tier or destination
 *    than its parent follows it there.
 * On Postgres, when `PUBLISHER_TEST_POSTGRES_URL` names one: incremental
 * refresh reads its range out of Postgres, Snowflake or BigQuery only.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { MaterializationController } from "../../src/controller/materialization.controller";
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
         seededBy?: string;
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

/**
 * `storage=` DuckLake destinations over a Postgres warehouse, one catalog
 * and bucket each: `lake`, and any `others` beside it.
 */
const DUCKLAKE = (root: () => string, others: string[] = []): Tier => ({
   label: "DuckLake storage=",
   raw: "public.xc_raw",
   prefix: "",
   storage: "lake",
   warehouse: async () => {
      const stamp = `${process.pid}_${Date.now()}`;
      const lakes = ["lake", ...others];
      const catalogs = lakes.map((l) => `xcl_${l}_${stamp}`);
      const db = `xcw_${stamp}`;
      const drop = await pgDatabases([...catalogs, db]);
      return {
         config: {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { ...pgParts(), databaseName: db },
         },
         destinations: lakes.map((name, i) => {
            const bucket = path.join(root(), `bucket_${name}`);
            fs.mkdirSync(bucket, { recursive: true });
            return {
               name,
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: {
                        ...pgParts(),
                        databaseName: catalogs[i],
                     },
                  },
                  storage: { bucketUrl: bucket },
               },
            };
         }),
         drop,
      };
   },
});

/** One package over one warehouse, and the service that builds it. */
class Fixture {
   root = "";
   envPath = "";
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
      this.envPath = storage.envPath;
      this.db = storage.db;
      this.repo = storage.repo;
      writePackageFiles(
         path.join(storage.envPath, PKG),
         { "m.malloy": this.modelText(model) },
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

   private modelText(model: string): string {
      return model
         .replace("__RAW__", this.tier.raw)
         .replace(/__P__/g, this.tier.storage ? "" : this.tier.prefix)
         .replace(
            /__S__/g,
            this.tier.storage ? ` storage="${this.tier.storage}"` : "",
         );
   }

   /** Replace the model and reload the package. */
   async rewrite(model: string): Promise<void> {
      fs.writeFileSync(
         path.join(this.envPath, PKG, "m.malloy"),
         this.modelText(model),
      );
      this.pkg = await this.env.getPackage(parsePackageName(PKG), true);
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

   /**
    * One run through the HTTP controller: the body as JSON bytes, validated
    * as a request's is, and the settled run read back as the wire shape.
    */
   async buildOverHttp(body: object): Promise<Materialization> {
      const controller = new MaterializationController(this.service);
      const wire = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;
      const created = await controller.createMaterialization(
         ENV,
         PKG,
         wire(body) as Record<string, unknown>,
      );
      await this.settle(created.id);
      return wire(
         await controller.getMaterialization(ENV, PKG, created.id),
      ) as Materialization;
   }

   async build(opts: object): Promise<Materialization> {
      const created = await this.service.createMaterialization(ENV, PKG, opts);
      return this.settle(created.id);
   }

   private async settle(id: string): Promise<Materialization> {
      const created = { id };
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
            seededBy: first.id,
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

   describe(`an incremental extension refreshed in its own runs (${tier.label})`, () => {
      const f = new Fixture(tier);
      const late = { id: 6, ts: at(2 * DAY), amt: 19 };
      const runs: Record<string, Materialization> = {};
      const seedsIn: Record<string, typeof f.seeds> = {};
      const build = async (label: string, opts: object) => {
         const before = f.seeds.length;
         runs[label] = await f.build(opts);
         seedsIn[label] = f.seeds.slice(before);
      };

      beforeAll(async () => {
         await f.open(MODEL);
         await build("first", {});
         // The extensions alone (their parent refreshed with them, nothing
         // new): then everything, whose parent delta they follow.
         await build("extOnly", { sourceNames: ["ext_rr", "ext_mg"] });
         await f.insert(SECOND);
         await build("all", {});
         // The parent alone on its schedule, re-reading a late row...
         await f.insert([late]);
         await build("parentOnly", { sourceNames: ["ev"] });
         // ...then the extensions alone, which follow it by delta.
         await build("extAfter", { sourceNames: ["ext_rr", "ext_mg"] });
      }, 600_000);
      afterAll(() => f.close());

      const rows = () => [...FIRST, ...SECOND, late];

      it("follows the parent after an extension-only run, with no reseed", () => {
         for (const run of Object.values(runs)) {
            expect([run.status, (run as { error?: unknown }).error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         }
         const parentLedger = entryNamed(runs.extOnly, "ev")![1].ledger!;
         expect(
            entryNamed(runs.extOnly, "ext_rr")![1].ledger?.parent,
         ).toMatchObject({
            advancedBy: parentLedger.advancedBy,
            coveredThrough: parentLedger.coveredThrough,
         });
         expect(entryNamed(runs.all, "ev")![1].refresh).toBe("delta");
         for (const name of ["ext_rr", "ext_mg"]) {
            expect([name, entryNamed(runs.all, name)![1].refresh]).toEqual([
               name,
               "delta",
            ]);
         }
         expect(
            seedsIn.all.filter(
               (s) => s.reasonCode === "upstream_generation_skipped",
            ),
         ).toEqual([]);
      });

      it("deltas an extension over a parent refreshed by an earlier run, equal to live", async () => {
         expect(entryNamed(runs.parentOnly, "ev")![1].refresh).toBe("delta");
         expect(seedsIn.extAfter).toEqual([]);
         for (const name of ["ext_rr", "ext_mg"]) {
            const ext = entryNamed(runs.extAfter, name)![1];
            expect([name, ext.refresh]).toEqual([name, "delta"]);
            // The late row, below the extension's own lower bound, is read
            // once; nothing is missed or counted twice.
            expect([name, await f.stored(ext)]).toEqual([
               name,
               { n: 6, total: sumOf(rows()), ids: [1, 2, 3, 4, 5, 6] },
            ]);
            const served = await f.totals(name);
            expect([name, served.n, served.total]).toEqual([
               name,
               6,
               sumOf(rows()),
            ]);
         }
      });

      it("records the parent's history since its seed on its ledger", () => {
         const ledger = entryNamed(runs.parentOnly, "ev")![1].ledger!;
         expect(ledger.seededBy).toBe(runs.first.id);
         expect(
            (ledger.advances as { from: string; advancedBy: string }[]).map(
               (a) => [a.from, a.advancedBy],
            ),
         ).toEqual([
            [runs.first.id, runs.extOnly.id],
            [runs.extOnly.id, runs.all.id],
            [runs.all.id, runs.parentOnly.id],
         ]);
      });
   });

   describe(`an incremental extension whose parent was rebuilt, refreshed often, or edited between its runs (${tier.label})`, () => {
      const f = new Fixture(tier);
      let n = 100;
      const fresh = () => {
         n += 1;
         return { id: n, ts: at(DAY - n * 1000), amt: 1 };
      };
      const added: Row[] = [];
      const seedsIn: Record<string, typeof f.seeds> = {};
      const runs: Record<string, Materialization> = {};
      const build = async (label: string, opts: object) => {
         const before = f.seeds.length;
         runs[label] = await f.build(opts);
         seedsIn[label] = f.seeds.slice(before);
      };
      const addRow = async () => {
         const row = fresh();
         added.push(row);
         await f.insert([row]);
      };
      const extSeeds = (label: string) =>
         seedsIn[label].filter(
            (s) =>
               s.physicalTableName ===
               entryNamed(runs[label], "ext_rr")?.[1].physicalTableName,
         );
      const overflowed: string[] = [];

      beforeAll(async () => {
         await f.open(MODEL);
         await build("first", {});
         // The parent rebuilt in full on its own, then the extension.
         await addRow();
         await build("parentReseed", { sourceNames: ["ev"], reseed: true });
         await build("afterReseed", { sourceNames: ["ext_rr"] });
         // Nine parent deltas, one more than its ledger lists.
         for (let i = 0; i < 9; i++) {
            await addRow();
            await build(`delta${i}`, { sourceNames: ["ev"] });
         }
         const warn = logger.warn;
         (logger as { warn: unknown }).warn = (
            message: string,
            meta?: Record<string, unknown>,
         ) => {
            if (message.includes("refreshed more often")) {
               overflowed.push(meta?.physicalTableName as string);
            }
            return warn(message, meta as never);
         };
         await build("afterNine", { sourceNames: ["ext_rr"] });
         (logger as { warn: unknown }).warn = warn;
         // The parent's lookback edited, then a late row it re-reads alone.
         await f.rewrite(
            MODEL.replace('lookback="7 days"', 'lookback="3 days"'),
         );
         const late = { id: 999, ts: at(2 * DAY), amt: 5 };
         added.push(late);
         await f.insert([late]);
         await build("afterEdit", { sourceNames: ["ev"] });
         await build("extAfterEdit", { sourceNames: ["ext_rr"] });
      }, 900_000);
      afterAll(() => f.close());

      const live = () => [...FIRST, ...added];

      it("rebuilds the extension when its parent was rebuilt in full since it read it", () => {
         expect(entryNamed(runs.parentReseed, "ev")![1].refresh).toBe("full");
         expect(entryNamed(runs.afterReseed, "ext_rr")![1].refresh).toBe(
            "full",
         );
         expect(extSeeds("afterReseed").map((s) => s.reasonCode)).toEqual([
            "upstream_generation_skipped",
         ]);
      });

      it("rebuilds it, naming the cap, after more parent deltas than the ledger lists", async () => {
         for (let i = 0; i < 9; i++) {
            expect([
               i,
               entryNamed(runs[`delta${i}`], "ev")![1].refresh,
            ]).toEqual([i, "delta"]);
         }
         const ext = entryNamed(runs.afterNine, "ext_rr")![1];
         expect(ext.refresh).toBe("full");
         expect(extSeeds("afterNine").map((s) => s.reasonCode)).toEqual([
            "upstream_generation_skipped",
         ]);
         expect(overflowed).toEqual([ext.physicalTableName]);
      });

      it("stays equal to live across a parent lookback edit", async () => {
         for (const label of ["afterEdit", "extAfterEdit"]) {
            const run = runs[label];
            expect([
               label,
               run.status,
               (run as { error?: unknown }).error,
            ]).toEqual([label, "MANIFEST_FILE_READY", null]);
         }
         const ext = entryNamed(runs.extAfterEdit, "ext_rr")![1];
         const want = live();
         expect(await f.stored(ext)).toEqual({
            n: want.length,
            total: sumOf(want),
            ids: want.map((r) => r.id).sort((a, b) => a - b),
         });
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
      let heldAfterFourth: Awaited<ReturnType<Fixture["stored"]>>;
      let warnedWhenUnproven: { subject?: string; code?: string }[] = [];

      const instruct = (
         names: string[],
         ledger: unknown[],
         reseed = false,
      ): Record<string, unknown> => {
         const sources = Object.values(f.pkg.getBuildPlan()!.sources).filter(
            (s) => names.includes(s.name),
         );
         // Every other source's table, as the newest run that built it
         // reported it: what a host references and does not rebuild.
         const referenceManifest = Object.values(f.pkg.getBuildPlan()!.sources)
            .filter((s) => !names.includes(s.name))
            .flatMap((s) => {
               const newest = [...runs]
                  .reverse()
                  .map((r) => entriesOf(r)[s.sourceEntityId])
                  .find((e) => e !== undefined);
               return newest
                  ? [
                       {
                          sourceEntityId: s.sourceEntityId,
                          physicalTableName: newest.physicalTableName,
                          connectionName: "warehouse",
                          ...(newest.builtAt
                             ? { builtAt: newest.builtAt }
                             : {}),
                       },
                    ]
                  : [];
            });
         return {
            ...(referenceManifest.length ? { referenceManifest } : {}),
            buildInstructions: sources.map((s) => ({
               sourceEntityId: s.sourceEntityId,
               sourceID: s.sourceID,
               materializedTableId: `mt-${s.name}`,
               physicalTableName: `${tier.prefix}host_${s.name}`,
               realization: "COPY",
               ...(reseed ? { reseed: true } : {}),
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
      const step = async (
         names: string[],
         ledger: unknown[],
         reseed = false,
      ) => {
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
                  reseed,
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
         // ...which then follows it by delta, from the parent history the
         // echoed parent entry lists.
         await step(["ev", "ext_rr"], echoed());
         heldAfterFourth = await f.stored(entryNamed(runs[4], "ext_rr")![1]);
         // The parent alone, then the extension alone over the parent's
         // table as referenced: its build and history come from that
         // table's newest entry.
         await f.insert([{ id: 9, ts: at(2 * DAY), amt: 31 }]);
         await step(["ev"], echoed());
         await step(["ext_rr"], echoed());
         // The parent rebuilt in full alone: the extension alone rebuilds.
         await step(["ev"], echoed(), true);
         await step(["ext_rr"], echoed());
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
      const allWithLate = () => [...all(), { id: 9, ts: "", amt: 31 }];

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

      it("deltas an extension over a parent refreshed in an earlier call", async () => {
         // Referenced, not rebuilt.
         expect(entryNamed(runs[3], "ext_rr")?.[1].builtAt).toBe(
            ext(2).builtAt,
         );
         expect(ext(4).refresh).toBe("delta");
         expect(seedsAt[4]).toEqual([]);
         const held = heldAfterFourth;
         expect(held.ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
         expect(held.total).toBe(sumOf(all()));
      });

      it("deltas an extension refreshed alone over a parent refreshed alone, equal to live", async () => {
         expect(entryNamed(runs[5], "ev")![1].refresh).toBe("delta");
         expect(entryNamed(runs[6], "ev")?.[1].builtAt).toBe(
            entryNamed(runs[5], "ev")![1].builtAt,
         );
         expect(ext(6).refresh).toBe("delta");
         expect(seedsAt[6]).toEqual([]);
         // The late row (two days old, below the extension's own lower
         // bound) once, nothing twice.
         const held = await f.stored(ext(6));
         expect(held.ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
         expect(held.total).toBe(sumOf(allWithLate()));
      });

      it("rebuilds an extension refreshed alone after its parent was rebuilt in full", async () => {
         expect(entryNamed(runs[7], "ev")![1].refresh).toBe("full");
         expect(ext(8).refresh).toBe("full");
         expect(seedsAt[8].map((s) => s.reasonCode)).toEqual([
            "upstream_generation_skipped",
         ]);
         expect((await f.stored(ext(8))).total).toBe(sumOf(allWithLate()));
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

   describe(`an orchestrated incremental extension through the HTTP controller (${tier.label})`, () => {
      const f = new Fixture(tier);
      const ORCH = `##! experimental { persistence }
source: raw is warehouse.table('__RAW__')

#(materialize) refresh="incremental" watermark="ts" merge_key="id" lookback="7 days"
source: ev is raw

#(materialize) refresh="incremental" watermark="ts"
source: ext_rr is ev extend { dimension: big is amt > 5 }
`;
      const host = (name: string) => `${tier.prefix}host_${name}`;
      /** A host's request body: each source's instruction, and the ledger. */
      const body = (ledger: unknown[], names?: string[]) => ({
         buildInstructions: {
            sources: Object.values(f.pkg.getBuildPlan()!.sources)
               .filter((s) => !names || names.includes(s.name))
               .map((s) => ({
                  sourceEntityId: s.sourceEntityId,
                  sourceID: s.sourceID,
                  materializedTableId: `mt-${s.name}`,
                  physicalTableName: host(s.name),
                  realization: "COPY",
               })),
            // A host echoes only the entries of the tables it instructs.
            ledger: ledger.filter(
               (l) =>
                  !names ||
                  names.some(
                     (n) =>
                        (l as { physicalTableName: string })
                           .physicalTableName === host(n),
                  ),
            ),
         },
      });
      let first: Materialization;
      let second: Materialization;
      let echoed: Entry["ledger"][] = [];
      let seedsInSecond: typeof f.seeds = [];
      let heldAfterSecond: Awaited<ReturnType<Fixture["stored"]>>;
      let third: Materialization;
      let fourth: Materialization;
      let seedsInFourth: typeof f.seeds = [];

      beforeAll(async () => {
         await f.open(ORCH);
         first = await f.buildOverHttp(body([]));
         await f.insert(SECOND);
         // The host stores each entry whole, as the wire returned it, and
         // echoes it back unchanged.
         echoed = Object.values(entriesOf(first))
            .map((e) => e.ledger)
            .filter((l) => l !== undefined);
         const before = f.seeds.length;
         second = await f.buildOverHttp(body(echoed));
         seedsInSecond = f.seeds.slice(before);
         heldAfterSecond = await f.stored(entryNamed(second, "ext_rr")![1]);
         // The parent alone, then both: each entry as the newest run that
         // reported it returned it, the only carrier of the parent's history.
         const newest = (runs: Materialization[]) => {
            const byTable = new Map<string, Entry["ledger"]>();
            for (const run of runs) {
               for (const e of Object.values(entriesOf(run))) {
                  if (e.ledger) byTable.set(e.physicalTableName, e.ledger);
               }
            }
            return [...byTable.values()];
         };
         await f.insert([{ id: 6, ts: at(2 * DAY), amt: 19 }]);
         third = await f.buildOverHttp(body(newest([first, second]), ["ev"]));
         const beforeFourth = f.seeds.length;
         fourth = await f.buildOverHttp(body(newest([first, second, third])));
         seedsInFourth = f.seeds.slice(beforeFourth);
      }, 400_000);
      afterAll(() => f.close());

      it("returns the extension's ledger entry with its parent key on the wire", () => {
         for (const run of [first, second]) {
            expect([run.status, (run as { error?: unknown }).error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         }
         const [parentAddress, parent] = entryNamed(first, "ev")!;
         const ext = echoed.find(
            (l) => l?.physicalTableName === host("ext_rr"),
         );
         expect(ext?.parent).toEqual({
            sourceEntityId: parentAddress,
            advancedBy: first.id,
            seededBy: first.id,
            builtAt: parent.builtAt,
            coveredThrough: parent.ledger?.coveredThrough as string,
            coveredThroughType: "timestamp",
         });
      });

      it("deltas the extension on the echoed parent key, with no reseed", async () => {
         // No manifest is bound on this worker: only the echoed key proves
         // which parent build the extension last read.
         expect(
            Object.keys(f.pkg.materialization().boundDatabaseEntries() ?? {}),
         ).toEqual([]);
         expect(entryNamed(second, "ev")![1].refresh).toBe("delta");
         const ext = entryNamed(second, "ext_rr")![1];
         expect(ext.refresh).toBe("delta");
         expect(seedsInSecond).toEqual([]);
         expect(ext.ledger?.parent?.advancedBy).toBe(
            entryNamed(second, "ev")![1].ledger?.advancedBy as string,
         );
         expect(heldAfterSecond).toEqual({
            n: 5,
            total: sumOf([...FIRST, ...SECOND]),
            ids: [1, 2, 3, 4, 5],
         });
      });

      it("deltas the extension after a parent-only call, from the echoed entries alone", async () => {
         for (const run of [third, fourth]) {
            expect([run.status, (run as { error?: unknown }).error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         }
         expect(entryNamed(third, "ext_rr")).toBeUndefined();
         expect(entryNamed(third, "ev")![1].ledger?.advances).toHaveLength(2);
         const ext = entryNamed(fourth, "ext_rr")![1];
         expect(ext.refresh).toBe("delta");
         expect(seedsInFourth).toEqual([]);
         const want = [...FIRST, ...SECOND, { id: 6, ts: "", amt: 19 }];
         expect(await f.stored(ext)).toEqual({
            n: 6,
            total: sumOf(want),
            ids: [1, 2, 3, 4, 5, 6],
         });
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

   describe("an incremental storage= extension stacked on its parent, refreshed after a parent-only run", () => {
      let root = "";
      const f = new Fixture(DUCKLAKE(() => root));
      const late = { id: 6, ts: at(2 * DAY), amt: 19 };
      let parentOnly: Materialization;
      let extAfter: Materialization;
      const seedsInExt: typeof f.seeds = [];

      beforeAll(async () => {
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(fs.realpathSync("/tmp"), "xcl-")),
         );
         await f.open(MODEL.replace(/name="__P__[a-z_]+" /g, ""));
         await f.build({});
         await f.insert([...SECOND, late]);
         parentOnly = await f.build({ sourceNames: ["ev"] });
         const before = f.seeds.length;
         extAfter = await f.build({ sourceNames: ["ext_rr", "ext_mg"] });
         seedsInExt.push(...f.seeds.slice(before));
      }, 400_000);
      afterAll(async () => {
         await f.close();
         fs.rmSync(root, { recursive: true, force: true });
      });

      it("deltas in the destination from the parent's recorded history, equal to live", async () => {
         for (const run of [parentOnly, extAfter]) {
            expect([run.status, (run as { error?: unknown }).error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
         }
         expect(entryNamed(parentOnly, "ev")![1].refresh).toBe("delta");
         expect(seedsInExt).toEqual([]);
         for (const name of ["ext_rr", "ext_mg"]) {
            const ext = entryNamed(extAfter, name)![1];
            expect([name, ext.storageDestinationName, ext.refresh]).toEqual([
               name,
               "lake",
               "delta",
            ]);
            const served = await f.totals(name);
            expect([name, served.n, served.total]).toEqual([
               name,
               6,
               sumOf([...FIRST, ...SECOND, late]),
            ]);
         }
      });
   });

   // Across tiers and destinations (extension chains, cases 1 and 2): the
   // parent advances by a delta that re-reads a late row, and each
   // extension follows it over the parent's table, in another tier or
   // destination than the parent's, nothing missed or counted twice.
   for (const [label, parentName, parentStorage, extStorage, others] of [
      [
         "a database parent, its extensions in a DuckLake destination",
         `name="public.xc_ev"`,
         "",
         ` storage="lake"`,
         [],
      ],
      [
         "a parent in one DuckLake destination, its extensions in another",
         "",
         ` storage="lake"`,
         ` storage="lake2"`,
         ["lake2"],
      ],
   ] as const) {
      describe(`an incremental extension across tiers: ${label}`, () => {
         let root = "";
         const f = new Fixture(DUCKLAKE(() => root, [...others]));
         let first: Materialization;
         let second: Materialization;
         const seedsInSecond: typeof f.seeds = [];
         const destination = extStorage.match(/"(.*)"/)![1];

         beforeAll(async () => {
            root = fs.realpathSync(
               fs.mkdtempSync(path.join(fs.realpathSync("/tmp"), "xct-")),
            );
            // The parent on its own tier, every extension on the other.
            const model = MODEL.replace(`name="__P__ev"`, parentName)
               .replace(/name="__P__[a-z_]+"/g, "")
               .replace(/(lookback="7 days")__S__/, `$1${parentStorage}`)
               .replace(/__S__/g, extStorage);
            await f.open(model);
            first = await f.build({});
            await f.insert(SECOND);
            const before = f.seeds.length;
            second = await f.build({});
            seedsInSecond.push(...f.seeds.slice(before));
         }, 400_000);
         afterAll(async () => {
            await f.close();
            fs.rmSync(root, { recursive: true, force: true });
         });

         it("seeds each extension over the parent's table, recording the parent build it read", () => {
            for (const run of [first, second]) {
               expect([run.status, (run as { error?: unknown }).error]).toEqual(
                  ["MANIFEST_FILE_READY", null],
               );
            }
            const [parentAddress, parent] = entryNamed(first, "ev")!;
            expect(parent.storageDestinationName ?? "").toBe(
               parentStorage.match(/"(.*)"/)?.[1] ?? "",
            );
            for (const name of EXTENSIONS) {
               const ext = entryNamed(first, name)![1];
               expect([
                  name,
                  ext.storageDestinationName,
                  ext.builtFrom?.map((b) => b.sourceEntityId),
               ]).toEqual([name, destination, [parentAddress]]);
            }
            expect(entryNamed(first, "ext_rr")![1].ledger?.parent).toEqual({
               sourceEntityId: parentAddress,
               advancedBy: first.id,
               seededBy: first.id,
               builtAt: parent.builtAt,
               coveredThrough: parent.ledger?.coveredThrough as string,
               coveredThroughType: "timestamp",
            });
         });

         it("deltas each extension on the parent's watermark after the parent's delta", () => {
            const [parentAddress, parent] = entryNamed(second, "ev")!;
            expect(parent.refresh).toBe("delta");
            for (const name of ["ext_rr", "ext_mg"]) {
               const ext = entryNamed(second, name)![1];
               expect([name, ext.refresh]).toEqual([name, "delta"]);
               expect(ext.ledger?.parent?.sourceEntityId).toBe(parentAddress);
               expect(ext.ledger?.parent?.advancedBy).toBe(second.id);
            }
            // Followed by delta, not reseeded; only the extension on another
            // watermark is rebuilt, for that reason.
            expect(
               seedsInSecond
                  .filter((s) => s.reasonCode !== undefined)
                  .map((s) => s.reasonCode),
            ).toEqual(["upstream_watermark_differs"]);
            expect(entryNamed(second, "ext_wm")![1].refresh).toBe("full");
         });

         it("serves every extension equal to live, the late row once", async () => {
            for (const name of ["ev", ...EXTENSIONS]) {
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
}
