// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A manifest this server writes still loads and serves on the last release's
 * worker (v0.17.2), during a rolling deploy where both read one manifest.
 * The new fields (`contract`, `builtAt`, `builtFrom`, and a reference's
 * `schema`, `dataAsOf`, `contract` and `builtAt`) must be ignored there, and
 * no field v0.17.2 reads may be dropped or change meaning.
 *
 * Each manifest is run through a frozen copy of v0.17.2's projection
 * (frozen/manifest_loader_0_17_2.ts):
 *
 *  - a manifest a real run writes now: DuckDB's database tier (a `#@ persist`
 *    source, a `#(materialize)` table, a pair table and a persisted view over
 *    one), and, when `PUBLISHER_TEST_POSTGRES_URL` names a Postgres, a
 *    `storage=` table in a DuckLake destination beside a Postgres one;
 *  - a frozen golden of every tier and origin this server writes
 *    (frozen/manifest_with_contract.golden.json), so the storage tier is
 *    covered on every run.
 *
 * Every entry binds, its projection is exactly that of the same entry with
 * the new fields taken out, and each database projection names a table the
 * warehouse reads, holding the rows the stored table holds. The real old
 * worker serving such a manifest is `hammer/mixed-version/run.ts`, run
 * nightly (nightly.yml, job `mixed-version`).
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import {
   type FrozenEntry,
   MANIFEST_ENTRY_FIELDS_0_17_2,
   MANIFEST_REFERENCE_FIELDS_0_17_2,
   referenceManifestToEntries,
   splitManifestEntries,
} from "./frozen/manifest_loader_0_17_2";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

const GOLDEN = path.join(
   __dirname,
   "frozen",
   "manifest_with_contract.golden.json",
);

/** The fields an older worker never reads: what this server added. */
const NEW_ENTRY_FIELDS = ["contract", "builtAt", "builtFrom"];

const withoutNewFields = (entry: FrozenEntry): FrozenEntry =>
   Object.fromEntries(
      Object.entries(entry).filter(([k]) => !NEW_ENTRY_FIELDS.includes(k)),
   );

/** The reference a host builds from an entry it stored, as this server asks. */
const referenceOf = (entry: FrozenEntry): FrozenEntry => ({
   sourceEntityId: entry.sourceEntityId,
   physicalTableName: entry.physicalTableName,
   connectionName: entry.connectionName,
   ...(entry.storageDestinationName
      ? { storageDestinationName: entry.storageDestinationName }
      : {}),
   ...(entry.nativeSchema ? { nativeSchema: entry.nativeSchema } : {}),
   ...(entry.schema ? { schema: entry.schema } : {}),
   ...(entry.dataAsOf ? { dataAsOf: entry.dataAsOf } : {}),
   ...(entry.builtAt ? { builtAt: entry.builtAt } : {}),
   ...(entry.contract ? { contract: entry.contract } : {}),
});

const pickOld = (ref: FrozenEntry): FrozenEntry =>
   Object.fromEntries(
      Object.entries(ref).filter(([k]) =>
         (MANIFEST_REFERENCE_FIELDS_0_17_2 as readonly string[]).includes(k),
      ),
   );

/** The checks every manifest this server writes must pass on v0.17.2. */
function expectServedByOldLoader(entries: Record<string, FrozenEntry>): void {
   const built = Object.entries(entries).filter(([, e]) => !e.error);
   expect(built.length).toBeGreaterThan(0);
   for (const [, e] of built) {
      // It is a manifest this server wrote: it carries what is new.
      expect(e.contract?.v).toBe(1);
      expect(e.builtAt).toEqual(expect.any(String));
      // Every field it carries that v0.17.2 declared keeps its meaning:
      // `schema` on a storage entry, `nativeSchema` on a database one only.
      if (e.storageDestinationName) {
         expect(e.schema?.length ?? 0).toBeGreaterThan(0);
         expect(e.nativeSchema).toBeUndefined();
      } else {
         expect(e.nativeSchema?.length ?? 0).toBeGreaterThan(0);
      }
      for (const key of Object.keys(withoutNewFields(e))) {
         expect(MANIFEST_ENTRY_FIELDS_0_17_2 as readonly string[]).toContain(
            key,
         );
      }
   }
   for (const from of ["host", "store"] as const) {
      const skipped: string[] = [];
      const loaded = splitManifestEntries(entries, from, skipped);
      // Only a failed source is skipped, as v0.17.2 skips one.
      expect(skipped.every((s) => s.endsWith("records a failed source"))).toBe(
         true,
      );
      const bound = {
         ...loaded.tableNameManifest,
         ...loaded.storageEntries,
      };
      expect(Object.keys(bound).sort()).toEqual(built.map(([id]) => id).sort());
      // The new fields change nothing it binds: a database entry's
      // projection, and a storage entry kept whole but for them.
      const plain = splitManifestEntries(
         Object.fromEntries(
            Object.entries(entries).map(([id, e]) => [id, withoutNewFields(e)]),
         ),
         from,
      );
      expect(loaded.tableNameManifest).toEqual(plain.tableNameManifest);
      expect(
         Object.fromEntries(
            Object.entries(loaded.storageEntries).map(([id, e]) => [
               id,
               withoutNewFields(e),
            ]),
         ),
      ).toEqual(plain.storageEntries);
   }
   // A host's references built from these entries, the new fields included,
   // seed v0.17.2's build exactly as references without them.
   const refs = built.map(([, e]) => referenceOf(e));
   expect(referenceManifestToEntries(refs)).toEqual(
      referenceManifestToEntries(refs.map(pickOld)),
   );
}

const MODEL = `##! experimental { persistence }
source: raw is warehouse.table('__T__mv_orders')
source: items is warehouse.table('__T__mv_items') extend { primary_key: item_id }

#@ persist name="__T__mv_daily"
source: daily is raw -> { group_by: cat; aggregate: total is amount.sum() }

#(materialize)
source: plain is raw extend { dimension: cat2 is cat }

#(materialize) inline=["items"]
source: orders is raw extend {
  primary_key: id
  join_many: items is items on items.order_id = id
}

source: s is daily extend {
  #@ persist name="__T__mv_view"
  view: top is { group_by: cat; aggregate: t is total.sum() }
}
`;

const RAW: Record<string, string> = {
   mv_orders: `SELECT * FROM (VALUES
    (1, 'a', CAST(10.0 AS DECIMAL(10,1))),
    (2, 'b', CAST(5.5 AS DECIMAL(10,1)))
  ) AS t(id, cat, amount)`,
   mv_items: `SELECT * FROM (VALUES (10, 1), (11, 1)) AS t(item_id, order_id)`,
};

async function waitForRun(
   service: MaterializationService,
   repo: ResourceRepository,
   opts: object,
): Promise<Materialization> {
   const created = await service.createMaterialization(ENV, PKG, opts);
   const running = (
      service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 120_000;
   for (;;) {
      const m = await repo.getMaterializationById(created.id);
      if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) {
         while (running.has(created.id)) {
            await new Promise((r) => setTimeout(r, 10));
         }
         return (await repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((r) => setTimeout(r, 20));
   }
}

describe("the frozen golden manifest, every tier and origin, on v0.17.2's loader", () => {
   it("binds every entry, ignoring the new fields, and seeds a build from its references as before", () => {
      const golden = JSON.parse(fs.readFileSync(GOLDEN, "utf8")) as {
         entries: Record<string, FrozenEntry>;
      };
      const tiers = new Set(
         Object.values(golden.entries).map((e) =>
            e.storageDestinationName ? "storage" : "database",
         ),
      );
      expect([...tiers].sort()).toEqual(["database", "storage"]);
      expectServedByOldLoader(golden.entries);
   });
});

describe("a manifest a DuckDB run writes now, on v0.17.2's loader", () => {
   let restore: () => void;
   let root: string;
   let db: DuckDBConnection;
   let env: Environment;
   let run: Materialization;

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      let envPath: string;
      let repo: ResourceRepository;
      ({ root, envPath, db, repo } = await openTempStorage("mv-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL.replace(/__T__/g, "main.") },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      const warehouse = await env.getMalloyConnection("warehouse");
      for (const [t, rows] of Object.entries(RAW)) {
         await warehouse.runSQL(`CREATE TABLE main.${t} AS ${rows}`);
      }
      await env.addPackage(PKG);
      run = await waitForRun(
         serviceOverEnvironment(
            () => repo,
            () => env,
         ),
         repo,
         {},
      );
   }, 300_000);
   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("binds every entry on v0.17.2's loader, as without the new fields", () => {
      expect(run.status).toBe("MANIFEST_FILE_READY");
      const entries = (run.manifest as { entries: Record<string, FrozenEntry> })
         .entries;
      expect(Object.keys(entries)).toHaveLength(4);
      expect(
         Object.values(entries).some((e) => (e.builtFrom ?? []).length > 0),
      ).toBe(true);
      expectServedByOldLoader(entries);
   });

   it("names, in each database projection, a table the warehouse reads with its stored rows", async () => {
      const entries = (run.manifest as { entries: Record<string, FrozenEntry> })
         .entries;
      const { tableNameManifest } = splitManifestEntries(entries);
      const warehouse = await env.getMalloyConnection("warehouse");
      for (const [id, bound] of Object.entries(tableNameManifest)) {
         const viaOld = await warehouse.runSQL(
            `SELECT COUNT(*) AS n FROM ${String(bound.tableName)}`,
         );
         const viaEntry = await warehouse.runSQL(
            `SELECT COUNT(*) AS n FROM ${entries[id].physicalTableName}`,
         );
         expect(Number(viaOld.rows[0]["n"])).toBeGreaterThan(0);
         expect(viaOld.rows).toEqual(viaEntry.rows);
      }
   });
});

describe.skipIf(!POSTGRES_URL)(
   "a manifest a Postgres run writes now, with a storage= table, on v0.17.2's loader",
   () => {
      const token = Math.random().toString(36).slice(2, 8);
      const catalog = `mv_lake_${token}`;
      const warehouseDb = `mv_wh_${token}`;
      let restore: () => void;
      let root: string;
      let db: DuckDBConnection;
      let env: Environment;
      let admin: PostgresConnection;
      let run: Materialization;

      beforeAll(async () => {
         restore = installSettings({
            env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
         }).restore;
         await freshLoadPool();
         let envPath: string;
         let repo: ResourceRepository;
         ({ root, envPath, db, repo } = await openTempStorage("mvs-", {
            id: ENV_ID,
            name: ENV,
         }));
         writePackageFiles(
            path.join(envPath, PKG),
            {
               "m.malloy": `##! experimental { persistence }
source: raw is warehouse.table('public.mv_orders')

#@ persist name="public.mv_daily"
source: daily is raw -> { group_by: cat; aggregate: total is amount.sum() }

#@ persist storage="lake" name="mv_lake_daily"
source: lake_daily is raw -> { group_by: cat; aggregate: total is amount.sum() }

#(materialize) storage="lake"
source: lake_plain is raw extend { dimension: cat2 is cat }
`,
            },
            { manifest: { name: PKG, description: "fixture" } },
         );
         admin = new PostgresConnection({
            name: "admin",
            connectionString: POSTGRES_URL!,
         });
         await admin.runSQL(`CREATE DATABASE ${catalog}`);
         await admin.runSQL(`CREATE DATABASE ${warehouseDb}`);
         const parsed = new URL(POSTGRES_URL!);
         const pg = {
            host: parsed.hostname,
            port: Number(parsed.port || 5432),
            userName: decodeURIComponent(parsed.username),
            password: decodeURIComponent(parsed.password),
         };
         const bucket = path.join(root, "bucket");
         fs.mkdirSync(bucket);
         env = await Environment.create(
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
         const warehouse = await env.getMalloyConnection("warehouse");
         await warehouse.runSQL(
            `CREATE TABLE public.mv_orders AS ${RAW["mv_orders"]}`,
         );
         await env.addPackage(PKG);
         run = await waitForRun(
            serviceOverEnvironment(
               () => repo,
               () => env,
            ),
            repo,
            {},
         );
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         for (const d of [catalog, warehouseDb]) {
            await admin
               ?.runSQL(`DROP DATABASE IF EXISTS ${d} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin?.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore();
         await freshLoadPool();
      });

      it("binds every entry on v0.17.2's loader, both tiers, as without the new fields", () => {
         expect(run.status).toBe("MANIFEST_FILE_READY");
         const entries = (
            run.manifest as { entries: Record<string, FrozenEntry> }
         ).entries;
         const storage = Object.values(entries).filter(
            (e) => e.storageDestinationName,
         );
         expect(storage).toHaveLength(2);
         expectServedByOldLoader(entries);
      });
   },
);
