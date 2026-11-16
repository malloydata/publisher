// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A colocated rollup over an incremental `#@ persist` base on Postgres builds
 * from the base's stored table, and stores the rows a build from the
 * warehouse stores: after the seed, and after a delta advanced the base in
 * place.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = `rollup_inc_${Math.random().toString(36).slice(2, 8)}`;
const q = (name: string) => `${SCHEMA}.${name}`;

function modelOf(grain: string): string {
   return `##! experimental.persistence
source: ev is warehouse.table('${q("ev")}')

#@ persist name="${q("inc")}" refresh="incremental" watermark="d"
source: daily is ev -> {
  group_by: d, region
  aggregate: amt_sum is amt.sum(), rows is count()
} extend {
  #@ preaggregate grain="${grain}"
  measure: total is amt_sum.sum()
  #@ preaggregate grain="${grain}"
  measure: n is rows.sum()
}
`;
}

type Row = Record<string, unknown>;
type Entry = {
   sourceName: string;
   sourceEntityId: string;
   physicalTableName: string;
   upstreamReuse?: string;
   origin?: string;
   refresh?: string;
};

function sorted(rows: Row[]): string[] {
   return rows
      .map((r) =>
         JSON.stringify(
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [
                  k,
                  typeof v === "bigint" || typeof v === "string"
                     ? isNaN(Number(v))
                        ? v
                        : Number(v)
                     : v,
               ]),
            ),
         ),
      )
      .sort();
}

describe.skipIf(!POSTGRES_URL)(
   "a colocated rollup over an incremental #@ persist base",
   () => {
      let root: string;
      let envPath: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;

      async function sql(text: string): Promise<Row[]> {
         const connection = await env.getMalloyConnection("warehouse");
         return (await connection.runSQL(text)).rows as Row[];
      }

      async function build(): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {});
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
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

      const entries = (m: Materialization): Entry[] =>
         Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         );

      /** The rollup's stored rows equal its warehouse build's (its planned SQL). */
      async function expectParity(m: Materialization): Promise<Entry> {
         const rollup = entries(m).find((e) => e.origin === "preaggregate")!;
         expect(rollup.upstreamReuse).toBe("reused");
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const planned = Object.values(pkg.getBuildPlan()!.sources).find(
            (s) => s.sourceEntityId === rollup.sourceEntityId,
         )!;
         // Each row as one JSON value: Malloy's Postgres connection returns
         // the column named `row`.
         const asJson = (relation: string) =>
            sql(`SELECT to_jsonb(t) AS row FROM (${relation}) AS t`);
         const stored = await asJson(
            `SELECT * FROM ${rollup.physicalTableName}`,
         );
         const warehouse = await asJson(planned.sql);
         expect(stored.length).toBeGreaterThan(0);
         expect(sorted(stored)).toEqual(sorted(warehouse));
         return rollup;
      }

      beforeEach(async () => {
         restore = installSettings({ env: {} }).restore;
         await __setPackageLoadPoolForTests(null);
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "rollup-inc-base-")),
         );
         envPath = path.join(root, "env");
         fs.mkdirSync(envPath);
         writePackageFiles(
            path.join(envPath, PKG),
            { "m.malloy": modelOf("region") },
            { manifest: { name: PKG, description: "fixture" } },
         );
         db = new DuckDBConnection(":memory:");
         await db.initialize();
         await initializeSchema(db);
         await db.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [ENV_ID, ENV, envPath],
         );
         repo = createDuckDBRepository(db);
         env = await Environment.create(ENV, envPath, [
            {
               name: "warehouse",
               type: "postgres",
               postgresConnection: { connectionString: POSTGRES_URL },
            } as never,
         ]);
         await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
         await sql(`CREATE SCHEMA ${SCHEMA}`);
         await sql(`CREATE TABLE ${q("ev")} AS SELECT * FROM (VALUES
           ('east', 10.5, TIMESTAMP '2026-01-03 10:00:00'),
           ('east', 20.0, TIMESTAMP '2026-01-31 23:30:00'),
           ('west', 5.5, TIMESTAMP '2026-02-01 00:00:00')
         ) AS t(region, amt, d)`);
         await env.addPackage(PKG);
         service = new MaterializationService({
            storageManager: { getRepository: () => repo },
            getEnvironment: async () => env,
            getLoadedEnvironments: () => [env],
         } as unknown as EnvironmentStore);
      });

      afterEach(async () => {
         await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`).catch(
            () => undefined,
         );
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore();
         await __setPackageLoadPoolForTests(null);
      });

      it("stores the warehouse build's rows after the seed and after a delta", async () => {
         const first = await build();
         expect([first.status, first.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         await expectParity(first);

         // New rows past the boundary, then a rollup at another grain: the
         // base advances by a delta, and the new rollup reads the advanced table.
         // Stamped now: after the seed's boundary (its run's start, in whole
         // seconds), and before the next run's.
         await sql(`INSERT INTO ${q("ev")} VALUES
           ('west', 7.25, LOCALTIMESTAMP(3)),
           ('north', 1.0, LOCALTIMESTAMP(3))`);
         // The boundary is whole seconds: the next run's frontier must pass them.
         await new Promise((resolve) => setTimeout(resolve, 2100));
         fs.writeFileSync(path.join(envPath, PKG, "m.malloy"), modelOf("d"));
         await env.getPackage(parsePackageName(PKG), true);
         const second = await build();
         expect([second.status, second.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const base = entries(second).find((e) => e.sourceName === "daily")!;
         expect(base.refresh).toBe("delta");
         await expectParity(second);
      });
   },
);
