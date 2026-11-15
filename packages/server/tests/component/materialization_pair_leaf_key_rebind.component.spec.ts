// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
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

/**
 * A pair table's leaf key check (known limitation 40) reaches what a published
 * version serves, through the real publish, build, load and rebind path. The
 * run's own load binds the table with the check's verdict; the rebind of the
 * run's version from the store that follows must bind the same verdict, or a
 * count through the inlined join is answered live (`COUNT_SEMANTICS`) for as
 * long as that version serves.
 *
 * `flights` is stored (`storage="lake"`) and `flights_co` colocated, over the
 * same rows; `flights_dup`'s leaf holds a key twice, so its check fails and
 * its counts keep answering live. Every answer equals live.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse and the DuckLake
 * destination's catalog (the only destination type a configuration may
 * declare).
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pairs";
const VERSION = "1.0.0";

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;

const measures = `  measure:
    flight_count is count()
    seg_n is segs.count()
    seg_ids is count(segs.seg_id)
    seg_total is segs.miles.sum()`;

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
${measures}
}

#(materialize) name="pair_leaf_flights_co" inline=["segs"]
source: flights_co is warehouse.table('public.flights') extend {
  primary_key: id
  where: id is not null
  join_many: segs on segs.flight_id = id
${measures}
}

#(materialize) storage="lake" inline=["segs"]
source: flights_dup is warehouse.table('public.flights') extend {
  primary_key: id
  join_many: segs is segs_dup on segs.flight_id = id
${measures}
}
`;

// Flight 3 has no segment; segment 99 has no flight; `segs_dup` holds
// segment 20 twice, the same row, under flight 2.
const SEED = [
   `CREATE TABLE flights AS SELECT * FROM (VALUES
      (1, 'AA'), (2, 'UA'), (3, 'AA'), (4, 'UA')) AS t(id, carrier)`,
   `CREATE TABLE segs AS SELECT * FROM (VALUES
      (10, 1, 100.0), (11, 1, 50.0), (20, 2, 200.0), (40, 4, 300.0),
      (99, 99, 7.0)) AS t(seg_id, flight_id, miles)`,
   `CREATE TABLE segs_dup AS SELECT * FROM segs
      UNION ALL SELECT 20, 2, 200.0`,
];

describe.skipIf(!url)(
   "a published version serves a pair table's counts through the inlined join, after its run's rebind",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const catalog = `pair_leaf_lake_${token}`;
      const warehouse = `pair_leaf_wh_${token}`;
      let admin: PostgresConnection;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;
      let run: Materialization;

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_PACKAGE_VERSIONING: "on",
            },
         }).restore;
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "pair-leaf-rebind-")),
         );
         const envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         const source = path.join(root, "source");
         writePackageFiles(
            source,
            { "m.malloy": MODEL },
            {
               manifest: {
                  name: PKG,
                  description: "fixture",
                  version: VERSION,
               },
            },
         );
         db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [ENV_ID, ENV, envPath],
         );
         repo = createDuckDBRepository(db);
         admin = new PostgresConnection({
            name: "admin",
            connectionString: url!,
         });
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         await admin.runSQL(`CREATE DATABASE ${warehouse}`);
         const parsed = new URL(url!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const seed = new PostgresConnection({
            name: "seed",
            connectionString: Object.assign(new URL(url!), {
               pathname: `/${warehouse}`,
            }).toString(),
         });
         for (const sql of SEED) await seed.runSQL(sql);
         await seed.close();
         const bucket = path.join(root, "bucket");
         fs.mkdirSync(bucket);
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "postgres",
                  postgresConnection: { ...pg, databaseName: warehouse },
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
         // As EnvironmentStore.bindVersionRegistry wires a versioned
         // environment.
         env.bindVersions(repo, ENV_ID, {
            downloaderFor: (_packageName, location) => async (stagingPath) => {
               await fs.promises.cp(location, stagingPath, { recursive: true });
            },
            ensurePackageRecord: async (packageName, description) => {
               if (await repo.getPackageByName(ENV_ID, packageName)) {
                  return false;
               }
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
         env.setStorageBindingResolver(
            storageBindingResolverFor(repo, ENV_ID, withRollupKeyRecords),
         );
         await env.getVersionService()!.publish(
            PKG,
            async (stagingPath) => {
               await fs.promises.cp(source, stagingPath, { recursive: true });
            },
            { sourceLocation: source, promotion: "on-publish" },
         );
         service = new MaterializationService({
            storageManager: { getRepository: () => repo },
            getEnvironment: async () => env,
            getLoadedEnvironments: () => [env],
         } as unknown as EnvironmentStore);
         // Loaded before the run, as a version that serves queries is: the
         // run's load binds it, and its rebind from the store follows.
         await versionPackage();
         run = await build();
      }, 120_000);

      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         for (const name of [catalog, warehouse]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close().catch(() => undefined);
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      function versionPackage() {
         return env.getPackage(parsePackageName(PKG), false, {
            versionId: VersionRef.parse(VERSION),
         });
      }

      async function build(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
         const deadline = Date.now() + 90_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               // The run rebinds its version after its final status; done
               // once the service stops tracking it.
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
      }

      type Row = Record<string, unknown>;
      async function query(malloy: string, materialize: boolean) {
         const restoreSwitch = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: materialize ? "true" : "false",
               PUBLISHER_PACKAGE_VERSIONING: "on",
            },
         }).restore;
         try {
            const model = (await versionPackage()).getModel("m.malloy");
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
               servedFrom: result.servedFrom,
               servedBy: result.path?.servedBy?.kind,
               reasons: (result.path?.considered ?? []).map((c) => c.reason),
            };
         } finally {
            restoreSwitch();
         }
      }

      const byCarrier = (source: string, aggregates: string) =>
         `run: ${source} -> { group_by: carrier; aggregate: ${aggregates}; order_by: carrier }`;

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

      it("serves counts and sums through the inlined join from the storage= pair table, equal to live", async () => {
         const malloy = byCarrier(
            "flights",
            "flight_count, seg_n, seg_ids, seg_total",
         );
         const live = await query(malloy, false);
         const served = await query(malloy, true);
         expect(live.rows).toEqual([
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
         ]);
         expect(served.rows).toEqual(live.rows);
         expect(served.reasons).not.toContain("COUNT_SEMANTICS");
         expect(served.servedFrom).toBe("storage");
      });

      it("serves the same counts from the colocated pair table, equal to live", async () => {
         const malloy = byCarrier(
            "flights_co",
            "flight_count, seg_n, seg_ids, seg_total",
         );
         const live = await query(malloy, false);
         const served = await query(malloy, true);
         expect(served.rows).toEqual(live.rows);
         expect(served.reasons).not.toContain("COUNT_SEMANTICS");
         expect(served.servedBy).not.toBe("live");
         expect(served.servedBy).toBeDefined();
      });

      it("answers live where the leaf's key is held twice", async () => {
         for (const aggregate of ["seg_n", "seg_total"]) {
            const malloy = byCarrier("flights_dup", aggregate);
            const live = await query(malloy, false);
            const served = await query(malloy, true);
            expect(served.rows).toEqual(live.rows);
            expect(served.servedFrom ?? undefined).toBeUndefined();
            expect(served.reasons).toContain("COUNT_SEMANTICS");
         }
      });

      // Last: it leaves another run on record. A verdict is bound only from
      // a run nothing could have written after (uncontestedAutoRun); while
      // another run is active the rebind binds none, and the counts answer
      // live until the store vouches for the check again.
      it("binds no verdict while another run could be writing the table, and binds it again after", async () => {
         const committer = (
            service as unknown as {
               committer: {
                  rebindLoadedVersions(
                     env: string,
                     pkg: string,
                  ): Promise<boolean>;
               };
            }
         ).committer;
         const malloy = byCarrier("flights", "seg_n, seg_total");
         const other = await repo.createMaterialization(
            ENV_ID,
            PKG,
            "PENDING",
            { mode: "auto" },
            { version: VERSION },
         );
         expect(await committer.rebindLoadedVersions(ENV, PKG)).toBe(true);
         const declined = await query(malloy, true);
         expect(declined.servedFrom ?? undefined).toBeUndefined();
         expect(declined.reasons).toContain("COUNT_SEMANTICS");
         expect(declined.rows).toEqual((await query(malloy, false)).rows);

         await repo.deleteMaterialization(other.id);
         expect(await committer.rebindLoadedVersions(ENV, PKG)).toBe(true);
         expect((await query(malloy, true)).servedFrom).toBe("storage");
      });
   },
);
