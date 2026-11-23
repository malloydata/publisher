// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The key checks a build records beside its tables reach what a package
 * serves through the real publish, build, load and rebind path, and never
 * outlive the rows they were taken over. Each check rides on its table's
 * binding: a pair table's leaf key (known limitation 40), a rollup's own key
 * and a stored source's key for a join to a rollup in a destination (known
 * limitation 29, #1704).
 *
 * - On a published version, the run's own load binds each check's verdict,
 *   and the rebind of the version from the store after the run binds the
 *   same one. Before this, that rebind dropped the verdicts, and every query
 *   that needs one was answered live for as long as the version served.
 * - On an unversioned package, a rebind reads the store and binds under the
 *   package's lock, which a run is created under too. So a run that rebuilds
 *   a table after the read can never find a verdict over the table's old
 *   rows bound over its new ones.
 *
 * Every expected row is written out, not read from a live query: with
 * materialization off a query can still read a bound copy.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse and the DuckLake
 * destination's catalog (the only destination type a configuration may
 * declare).
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName, VersionRef } from "../../src/service/identity";
import { withRollupKeyRecords } from "../../src/service/materialization/run/inline_uniqueness";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import { storageBindingResolverFor } from "../../src/service/versions/materialization_scope";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "keys";
const VERSION = "1.0.0";

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

type Row = Record<string, unknown>;

interface Answer {
   rows: Row[];
   servedFrom: string | undefined;
   servedBy: string | undefined;
   reasons: (string | undefined)[];
}

interface Fixture {
   service: MaterializationService;
   repo: ResourceRepository;
   /** Run SQL on the warehouse the model reads. */
   warehouse(sql: string): Promise<void>;
   build(options?: { forceRefresh?: boolean }): Promise<Materialization>;
   query(malloy: string): Promise<Answer>;
   committer(): {
      rebindLoadedVersions(env: string, pkg: string): Promise<boolean>;
   };
   /**
    * Unversioned only: bind `entries` as a host's manifest
    * (`manifestLocation`), as an orchestrated deployment binds what its
    * builds reported: no local run's key check records come with it.
    */
   hostManifest(entries: Record<string, unknown>): Promise<void>;
   /**
    * Write `model` over the package's model and reload it in place, as an
    * edit does: the models recompile, and no `storage=` entry is bound again.
    */
   editModel(model: string): Promise<void>;
   dispose(): Promise<void>;
}

/**
 * A package of one model over a Postgres warehouse, with a DuckLake
 * destination `lake`, served from a published version or, `versioned`
 * false, from its one unversioned slot. Loaded before anything is built, as a
 * package that serves queries is.
 */
async function fixture(
   model: string,
   seed: string[],
   versioned: boolean,
): Promise<Fixture> {
   const restore = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_PACKAGE_VERSIONING: versioned ? "on" : "off",
      },
   }).restore;
   const token = Math.random().toString(36).slice(2, 8);
   const catalog = `key_rebind_lake_${token}`;
   const warehouseDb = `key_rebind_wh_${token}`;
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "key-check-rebind-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   const source = versioned
      ? path.join(root, "source")
      : path.join(envPath, PKG);
   writePackageFiles(
      source,
      { "m.malloy": model },
      {
         manifest: {
            name: PKG,
            description: "fixture",
            ...(versioned ? { version: VERSION } : {}),
         },
      },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   const repo = createDuckDBRepository(db);
   const admin = new PostgresConnection({
      name: "admin",
      connectionString: url!,
   });
   await admin.runSQL(`CREATE DATABASE ${catalog}`);
   // A binary (`C`) warehouse: a `storage=` copy orders and ranges a
   // string as live does only under a binary collation, so a string
   // order here serves (the server default, often `en_US`, answers it live).
   await admin.runSQL(
      `CREATE DATABASE ${warehouseDb} LOCALE 'C' ENCODING 'UTF8' TEMPLATE template0`,
   );
   const parsed = new URL(url!);
   const pg = {
      host: parsed.hostname,
      port: Number(parsed.port || 5432),
      userName: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
   };
   const warehouse = new PostgresConnection({
      name: "seed",
      connectionString: Object.assign(new URL(url!), {
         pathname: `/${warehouseDb}`,
      }).toString(),
   });
   for (const sql of seed) await warehouse.runSQL(sql);
   const bucket = path.join(root, "bucket");
   fs.mkdirSync(bucket);
   const env = await Environment.create(
      ENV,
      envPath,
      [
         {
            name: "warehouse",
            type: "postgres",
            postgresConnection: { ...pg, databaseName: warehouseDb },
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
   if (versioned) {
      // As EnvironmentStore.bindVersionRegistry wires a versioned environment.
      env.bindVersions(repo, ENV_ID, {
         downloaderFor: (_packageName, location) => async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         ensurePackageRecord: async (packageName, description) => {
            if (await repo.getPackageByName(ENV_ID, packageName)) return false;
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description,
               manifestPath: "",
            });
            return true;
         },
         removePackageRecord: async (packageName) => {
            const row = await repo.getPackageByName(ENV_ID, packageName);
            if (row) await repo.deletePackage(row.id);
         },
      });
   } else {
      await repo.createPackage({
         environmentId: ENV_ID,
         name: PKG,
         description: "fixture",
         manifestPath: "",
      });
   }
   env.setStorageBindingResolver(
      storageBindingResolverFor(repo, ENV_ID, withRollupKeyRecords),
   );
   if (versioned) {
      await env.getVersionService()!.publish(
         PKG,
         async (stagingPath) => {
            await fs.promises.cp(source, stagingPath, { recursive: true });
         },
         { sourceLocation: source, promotion: "on-publish" },
      );
   }
   const service = serviceOverEnvironment(
      () => repo,
      () => env,
   );

   const pkg = () =>
      env.getPackage(
         parsePackageName(PKG),
         false,
         versioned ? { versionId: VersionRef.parse(VERSION) } : {},
      );
   await pkg();

   return {
      service,
      repo,
      warehouse: async (sql) => {
         await warehouse.runSQL(sql);
      },
      build: async (options = {}) => {
         const created = await service.createMaterialization(ENV, PKG, options);
         const deadline = Date.now() + 90_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               // A run rebinds after its final status; done once the service
               // stops tracking it.
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return (await repo.getMaterializationById(created.id)) ?? m;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      },
      query: async (malloy) => {
         const model = (await pkg()).getModel("m.malloy");
         expect(model).toBeDefined();
         const result = await model!.runQuery(
            unrecordedAudit(),
            { query: malloy } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
         return {
            rows: (result.compactResult as unknown as Row[]).map((r) =>
               Object.fromEntries(
                  Object.entries(r).map(([k, v]) => [
                     k,
                     typeof v === "string" ? v : Number(v),
                  ]),
               ),
            ),
            servedFrom: result.servedFrom ?? undefined,
            servedBy: result.path?.servedBy?.kind,
            reasons: (result.path?.considered ?? []).map((c) => c.reason),
         };
      },
      committer: () =>
         (
            service as unknown as {
               committer: ReturnType<Fixture["committer"]>;
            }
         ).committer,
      editModel: async (edited) => {
         const loaded = await pkg();
         fs.writeFileSync(
            path.join(loaded.getPackagePath(), "m.malloy"),
            edited,
         );
         await loaded.reloadAllModels({});
      },
      hostManifest: async (entries) => {
         const location = path.join(root, `host-${Date.now()}.json`);
         fs.writeFileSync(
            location,
            JSON.stringify({ builtAt: new Date().toISOString(), entries }),
         );
         writePackageFiles(
            source,
            { "m.malloy": model },
            {
               manifest: {
                  name: PKG,
                  description: "fixture",
                  manifestLocation: location,
               } as never,
            },
         );
         await env.getPackage(parsePackageName(PKG), true);
      },
      dispose: async () => {
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         await warehouse.close().catch(() => undefined);
         for (const name of [catalog, warehouseDb]) {
            await admin
               .runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin.close().catch(() => undefined);
         fs.rmSync(root, { recursive: true, force: true });
         restore();
      },
   };
}

const byCarrier = (source: string, aggregates: string) =>
   `run: ${source} -> { group_by: carrier; aggregate: ${aggregates}; order_by: carrier }`;

const FLIGHTS = `CREATE TABLE flights AS SELECT * FROM (VALUES
   (1, 'AA'), (2, 'UA'), (3, 'AA'), (4, 'UA')) AS t(id, carrier)`;
// Flight 3 has no segment; segment 99 has no flight.
const SEGS = `CREATE TABLE segs AS SELECT * FROM (VALUES
   (10, 1, 100.0), (11, 1, 50.0), (20, 2, 200.0), (40, 4, 300.0),
   (99, 99, 7.0)) AS t(seg_id, flight_id, miles)`;

const pairMeasures = `  measure:
    flight_count is count()
    seg_n is segs.count()
    seg_ids is count(segs.seg_id)
    seg_total is segs.miles.sum()`;

describe.skipIf(!url)(
   "a published version serves a pair table's counts through the inlined join, after its run's rebind",
   () => {
      // `flights` is stored and `flights_co` database, over the same rows;
      // `flights_dup`'s leaf holds segment 20 twice, the same row, so its
      // check fails.
      const MODEL = `source: segs is warehouse.table('public.segs') extend {
  primary_key: seg_id
}
source: segs_dup is warehouse.table('public.segs_dup') extend {
  primary_key: seg_id
}

#(materialize) storage="lake" inline=["segs"]
source: flights is warehouse.table('public.flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
${pairMeasures}
}

#(materialize) name="key_rebind_flights_co" inline=["segs"]
source: flights_co is warehouse.table('public.flights') extend {
  primary_key: id
  where: id is not null
  join_many: segs on segs.flight_id = id
${pairMeasures}
}

#(materialize) storage="lake" inline=["segs"]
source: flights_dup is warehouse.table('public.flights') extend {
  primary_key: id
  join_many: segs is segs_dup on segs.flight_id = id
${pairMeasures}
}
`;
      const ALL = [
         {
            carrier: "AA",
            flight_count: 2,
            seg_n: 2,
            seg_ids: 2,
            seg_total: 150,
         },
         {
            carrier: "UA",
            flight_count: 2,
            seg_n: 2,
            seg_ids: 2,
            seg_total: 500,
         },
      ];
      let f: Fixture;
      let run: Materialization;

      beforeAll(async () => {
         f = await fixture(
            MODEL,
            [
               FLIGHTS,
               SEGS,
               `CREATE TABLE segs_dup AS SELECT * FROM segs
                  UNION ALL SELECT 20, 2, 200.0`,
            ],
            true,
         );
         run = await f.build();
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      it("builds every pair table, and records each leaf key check", () => {
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const records = Object.values(
            (
               run.metadata as {
                  inlineUniquenessChecks?: {
                     rollupKeys?: Record<
                        string,
                        { table: string; unique: boolean }
                     >;
                  };
               }
            ).inlineUniquenessChecks?.rollupKeys ?? {},
         )
            .map((r) => [JSON.parse(r.table)[0], r.unique])
            .sort();
         expect(records).toEqual([
            ["connection", true],
            ["destination", false],
            ["destination", true],
         ]);
      });

      it("serves counts and sums through the inlined join from the storage= pair table", async () => {
         const served = await f.query(
            byCarrier("flights", "flight_count, seg_n, seg_ids, seg_total"),
         );
         expect(served.rows).toEqual(ALL);
         expect(served.reasons).not.toContain("COUNT_SEMANTICS");
         expect(served.servedFrom).toBe("storage");
      });

      it("serves the same counts from the database pair table", async () => {
         const served = await f.query(
            byCarrier("flights_co", "flight_count, seg_n, seg_ids, seg_total"),
         );
         expect(served.rows).toEqual(ALL);
         expect(served.reasons).not.toContain("COUNT_SEMANTICS");
         expect(served.servedBy).toBeDefined();
         expect(served.servedBy).not.toBe("live");
      });

      it("answers live where the leaf's key is held twice", async () => {
         const cases: [string, Row[]][] = [
            [
               "seg_n",
               [
                  { carrier: "AA", seg_n: 2 },
                  { carrier: "UA", seg_n: 3 },
               ],
            ],
            [
               "seg_total",
               [
                  { carrier: "AA", seg_total: 150 },
                  { carrier: "UA", seg_total: 700 },
               ],
            ],
         ];
         for (const [aggregate, rows] of cases) {
            const served = await f.query(byCarrier("flights_dup", aggregate));
            expect(served.rows).toEqual(rows);
            expect(served.servedFrom).toBeUndefined();
            expect(served.reasons).toContain("COUNT_SEMANTICS");
         }
      });

      // Last: it leaves another run on record. A verdict is bound only from
      // a run nothing could have written after (uncontestedAutoRun); while
      // another run is active the rebind binds none, and the counts answer
      // live until the store vouches for the check again.
      it("binds no verdict while another run could be writing the table, and binds it again after", async () => {
         const malloy = byCarrier("flights", "seg_n, seg_total");
         const other = await f.repo.createMaterialization(
            ENV_ID,
            PKG,
            "PENDING",
            { mode: "auto" },
            { version: VERSION },
         );
         expect(await f.committer().rebindLoadedVersions(ENV, PKG)).toBe(true);
         const declined = await f.query(malloy);
         expect(declined.servedFrom).toBeUndefined();
         expect(declined.reasons).toContain("COUNT_SEMANTICS");
         expect(declined.rows).toEqual([
            { carrier: "AA", seg_n: 2, seg_total: 150 },
            { carrier: "UA", seg_n: 2, seg_total: 500 },
         ]);

         await f.repo.deleteMaterialization(other.id);
         expect(await f.committer().rebindLoadedVersions(ENV, PKG)).toBe(true);
         expect((await f.query(malloy)).servedFrom).toBe("storage");
      });
   },
);

describe.skipIf(!url)(
   "an unversioned rebind never binds a verdict over a table a newer run rebuilds",
   () => {
      const MODEL = `source: segs is warehouse.table('public.segs') extend {
  primary_key: seg_id
}

#(materialize) storage="lake" inline=["segs"]
source: flights is warehouse.table('public.flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
${pairMeasures}
}
`;
      let f: Fixture;
      beforeAll(async () => {
         f = await fixture(MODEL, [FLIGHTS, SEGS], false);
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      // A rebind reads the store while the first run's verdict (unique)
      // stands; then segment 20 arrives twice and a refresh rebuilds the
      // table, which stores the two equal segments as one row. Had the
      // rebind's bind landed after the refresh began writing, the first
      // run's verdict would serve UA 2 / 500 over the new rows, against
      // 3 / 700. The rebind holds the package's lock from its read to its
      // bind, and the refresh is created under it: it starts only after the
      // bind, and forgets that verdict before it writes.
      it("a refresh created while a rebind is between its read and its bind waits for the bind, and forgets its verdict", async () => {
         const first = await f.build();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         const malloy = byCarrier("flights", "seg_n, seg_total");
         expect((await f.query(malloy)).servedFrom).toBe("storage");
         await f.warehouse("INSERT INTO segs VALUES (20, 2, 200.0)");
         const truth = [
            { carrier: "AA", seg_n: 2, seg_total: 150 },
            { carrier: "UA", seg_n: 3, seg_total: 700 },
         ];

         // Hold the rebind once it has read the store.
         const committer = f.committer() as unknown as Record<
            string,
            (...args: unknown[]) => Promise<unknown>
         >;
         const read = committer.boundEntries;
         let readDone!: () => void;
         const hasRead = new Promise<void>((resolve) => (readDone = resolve));
         let bindGo!: () => void;
         const mayBind = new Promise<void>((resolve) => (bindGo = resolve));
         committer.boundEntries = async (...args: unknown[]) => {
            const out = await read.apply(committer, args);
            readDone();
            await mayBind;
            return out;
         };
         // Hold the refresh at its load, once its table is written.
         const service = f.service.collaborators;
         const autoLoad = service.autoLoadManifest;
         let loading!: () => void;
         const atLoad = new Promise<void>((resolve) => (loading = resolve));
         let loadGo!: () => void;
         const mayLoad = new Promise<void>((resolve) => (loadGo = resolve));
         service.autoLoadManifest = async (...args) => {
            loading();
            await mayLoad;
            return autoLoad(...args);
         };
         try {
            const rebinding = f.committer().rebindLoadedVersions(ENV, PKG);
            await hasRead;
            const refreshing = f.build({ forceRefresh: true });
            // Without the lock the refresh writes its table now.
            await Promise.race([
               atLoad,
               new Promise((resolve) => setTimeout(resolve, 3_000)),
            ]);
            bindGo();
            expect(await rebinding).toBe(true);
            await atLoad;
            // The refresh's table is written and not yet loaded.
            expect((await f.query(malloy)).rows).toEqual(truth);
            loadGo();
            const second = await refreshing;
            expect(second.status).toBe("MANIFEST_FILE_READY");
            // Its own check found the key held twice.
            const after = await f.query(malloy);
            expect(after.rows).toEqual(truth);
            expect(after.reasons).toContain("COUNT_SEMANTICS");
         } finally {
            bindGo();
            loadGo();
            committer.boundEntries = read;
            service.autoLoadManifest = autoLoad;
         }
      }, 120_000);
   },
);

describe.skipIf(!url)(
   "a published version keeps a join to a rollup in a destination served, after its run's rebind",
   () => {
      // The rollup's own key check: `crew` is read through its rollup at
      // flight_id, its key, which must be unique.
      const MODEL = `source: crew is warehouse.table('public.crew') extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"] storage="lake"
  measure: crew_size is size.sum()
}

#(materialize) storage="lake"
source: flights is warehouse.table('public.flights') extend {
  primary_key: id
  join_one: crew with id
  measure: flight_count is count()
}
`;
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, crew.crew_size; order_by: carrier }";
      let f: Fixture;
      beforeAll(async () => {
         f = await fixture(
            MODEL,
            [
               FLIGHTS,
               `CREATE TABLE crew AS SELECT * FROM (VALUES
                  (1, 3), (3, 2), (4, 5)) AS t(flight_id, size)`,
            ],
            true,
         );
         expect((await f.build()).status).toBe("MANIFEST_FILE_READY");
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      it("serves it from storage once the run's rebind has run", async () => {
         const served = await f.query(malloy);
         expect(served.rows).toEqual([
            { carrier: "AA", flight_count: 2, crew_size: 5 },
            { carrier: "UA", flight_count: 2, crew_size: 5 },
         ]);
         expect(served.servedFrom).toBe("storage");
      });

      it("answers right after a rebuild finds the rollup's key held twice, and after a rebind", async () => {
         await f.warehouse("INSERT INTO crew VALUES (1, 4)");
         expect((await f.build({ forceRefresh: true })).status).toBe(
            "MANIFEST_FILE_READY",
         );
         // A join_one whose key repeats has no right answer to write out
         // (live's symmetric aggregate reads the key as unique): what holds
         // is that the rollup is no longer read, before a rebind or after.
         const served = await f.query(malloy);
         expect(served.servedFrom).toBeUndefined();
         expect(await f.committer().rebindLoadedVersions(ENV, PKG)).toBe(true);
         const rebound = await f.query(malloy);
         expect(rebound.servedFrom).toBeUndefined();
         expect(rebound.rows).toEqual(served.rows);
      });
   },
);

describe.skipIf(!url)(
   "a published version keeps a stored source's key check, after its run's rebind",
   () => {
      // The stored source's own key: `flights` is stored, and read beside
      // `segs`'s rollup through a join keyed on its primary key.
      const MODEL = `source: segs is warehouse.table('public.segs') extend {
  primary_key: seg_id
  #(rollup) grain=["flight_id"] storage="lake"
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is warehouse.table('public.flights') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure: flight_count is count()
}
`;
      const malloy =
         "run: flights -> { group_by: carrier; aggregate: flight_count, segs.seg_miles; order_by: carrier }";
      let f: Fixture;
      beforeAll(async () => {
         f = await fixture(MODEL, [FLIGHTS, SEGS], true);
         expect((await f.build()).status).toBe("MANIFEST_FILE_READY");
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      it("serves it from storage once the run's rebind has run", async () => {
         const served = await f.query(malloy);
         expect(served.rows).toEqual([
            { carrier: "AA", flight_count: 2, seg_miles: 150 },
            { carrier: "UA", flight_count: 2, seg_miles: 500 },
         ]);
         expect(served.servedFrom).toBe("storage");
      });

      it("answers right after a rebuild finds the stored key held twice, and after a rebind", async () => {
         await f.warehouse("INSERT INTO flights VALUES (1, 'UA')");
         expect((await f.build({ forceRefresh: true })).status).toBe(
            "MANIFEST_FILE_READY",
         );
         // Flight 1 is held twice, as AA and as UA.
         const truth = [
            { carrier: "AA", flight_count: 2, seg_miles: 150 },
            { carrier: "UA", flight_count: 3, seg_miles: 650 },
         ];
         const served = await f.query(malloy);
         expect(served.servedFrom).toBeUndefined();
         expect(served.rows).toEqual(truth);
         expect(await f.committer().rebindLoadedVersions(ENV, PKG)).toBe(true);
         const rebound = await f.query(malloy);
         expect(rebound.servedFrom).toBeUndefined();
         expect(rebound.rows).toEqual(truth);
      });
   },
);

describe.skipIf(!url)(
   "an orchestrated join to a rollup serves by the key checks its builds recorded in their contracts",
   () => {
      // A join_one to `crew`'s rollup at its key (the rollup's own check),
      // and a join_many to `segs`'s rollup beside the stored `flights`
      // (the stored source's own check). Bound from a host's manifest, so no
      // local run's record is read: only each table's contract.
      const MODEL = `source: crew is warehouse.table('public.crew') extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"] storage="lake"
  measure: crew_size is size.sum()
}

source: segs is warehouse.table('public.segs') extend {
  primary_key: seg_id
  #(rollup) grain=["flight_id"] storage="lake"
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is warehouse.table('public.flights') extend {
  primary_key: id
  join_one: crew with id
  join_many: segs on segs.flight_id = id
  measure: flight_count is count()
}
`;
      const CREW =
         "run: flights -> { group_by: carrier; aggregate: flight_count, crew.crew_size; order_by: carrier }";
      const SEG =
         "run: flights -> { group_by: carrier; aggregate: flight_count, segs.seg_miles; order_by: carrier }";
      let f: Fixture;
      let entries: Record<
         string,
         {
            contract?: {
               keys?: Record<string, { unique: boolean; level?: string }>;
            };
         }
      >;
      beforeAll(async () => {
         f = await fixture(
            MODEL,
            [
               FLIGHTS,
               SEGS,
               `CREATE TABLE crew AS SELECT * FROM (VALUES
                  (1, 3), (3, 2), (4, 5)) AS t(flight_id, size)`,
            ],
            false,
         );
         const built = await f.build();
         expect(built.status).toBe("MANIFEST_FILE_READY");
         entries = (built.manifest as { entries: typeof entries }).entries;
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      /** `entries` with every recorded rollup and source key check set to `unique`, or dropped. */
      const withChecks = (unique: boolean | "none") =>
         Object.fromEntries(
            Object.entries(entries).map(([id, e]) => {
               const keys = Object.fromEntries(
                  Object.entries(e.contract?.keys ?? {}).flatMap(([k, v]) =>
                     v.level === "leaf"
                        ? [[k, v]]
                        : unique === "none"
                          ? []
                          : [[k, { ...v, unique }]],
                  ),
               );
               return [
                  id,
                  e.contract ? { ...e, contract: { ...e.contract, keys } } : e,
               ];
            }),
         );

      it("records the rollup's key and the stored source's key in the contracts, under the package-relative scheme", () => {
         const names = Object.values(entries).flatMap((e) =>
            Object.entries(e.contract?.keys ?? {}).map(([k, v]) => [
               k,
               v.level,
            ]),
         );
         expect(names).toContainEqual(["rollup:flight_id", "rollup"]);
         expect(names).toContainEqual(["stored:flights@m.malloy.id", "stored"]);
      });

      it("serves both joins from storage when the recorded checks passed", async () => {
         await f.hostManifest(withChecks(true));
         const crew = await f.query(CREW);
         expect(crew.rows).toEqual([
            { carrier: "AA", flight_count: 2, crew_size: 5 },
            { carrier: "UA", flight_count: 2, crew_size: 5 },
         ]);
         expect(crew.servedFrom).toBe("storage");
         const seg = await f.query(SEG);
         expect(seg.rows).toEqual([
            { carrier: "AA", flight_count: 2, seg_miles: 150 },
            { carrier: "UA", flight_count: 2, seg_miles: 500 },
         ]);
         expect(seg.servedFrom).toBe("storage");
      });

      it("answers both live when the recorded checks failed, or none was recorded", async () => {
         for (const checks of [false, "none"] as const) {
            await f.hostManifest(withChecks(checks));
            for (const q of [CREW, SEG]) {
               expect({
                  checks,
                  q,
                  from: (await f.query(q)).servedFrom,
               }).toEqual({
                  checks,
                  q,
                  from: undefined,
               });
            }
         }
      });
   },
);

describe.skipIf(!url)(
   "an orchestrated join to a rollup reads its stored source's key check under the primary key the model declares now",
   () => {
      // `flights` is stored and read beside `segs`'s rollup through a
      // join_many on its primary key. Its build checked `id`; an edit then
      // makes `alt_id`, which repeats, the key the join reads. The table and
      // its contract stay bound, but the check under `id` says nothing of
      // `alt_id`: read through the rollup, flight 1's segments would be met
      // once per flight sharing alt_id 1.
      const model = (
         key: string,
      ) => `source: segs is warehouse.table('public.segs') extend {
  primary_key: seg_id
  #(rollup) grain=["flight_id"] storage="lake"
  measure: seg_miles is miles.sum()
}

#(materialize) storage="lake"
source: flights is warehouse.table('public.flights') extend {
  primary_key: ${key}
  join_many: segs on segs.flight_id = ${key}
  measure: flight_count is count()
}
`;
      const SEG =
         "run: flights -> { group_by: carrier; aggregate: flight_count, segs.seg_miles; order_by: carrier }";
      let f: Fixture;
      beforeAll(async () => {
         f = await fixture(
            model("id"),
            [
               `CREATE TABLE flights AS SELECT * FROM (VALUES
                  (1, 1, 'AA'), (2, 2, 'UA'), (3, 1, 'AA'), (4, 4, 'UA'))
                  AS t(id, alt_id, carrier)`,
               SEGS,
            ],
            false,
         );
         const built = await f.build();
         expect(built.status).toBe("MANIFEST_FILE_READY");
         await f.hostManifest(
            (built.manifest as { entries: Record<string, unknown> }).entries,
         );
      }, 120_000);
      afterAll(async () => {
         await f?.dispose();
      });

      it("serves the join from storage while the checked key is the one it reads", async () => {
         const served = await f.query(SEG);
         expect(served.rows).toEqual([
            { carrier: "AA", flight_count: 2, seg_miles: 150 },
            { carrier: "UA", flight_count: 2, seg_miles: 500 },
         ]);
         expect(served.servedFrom).toBe("storage");
      });

      it("answers live once a reload moves the primary key, with no storage rebind", async () => {
         await f.editModel(model("alt_id"));
         const answer = await f.query(SEG);
         // Live's own answer over a key the data holds twice: it counts
         // AA's two flights, both alt_id 1, as one flight, and meets
         // flight 1's segments once per row. Read through the rollup, the
         // stale check would have answered 2 and 150.
         expect(answer.rows).toEqual([
            { carrier: "AA", flight_count: 1, seg_miles: 300 },
            { carrier: "UA", flight_count: 2, seg_miles: 500 },
         ]);
         expect(answer.servedFrom).toBeUndefined();
         expect(answer.reasons).toContain("COUNT_SEMANTICS");
      });
   },
);
