// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { unrecordedAudit } from "../../access_record";
import { Environment } from "../../environment";
import type { EnvironmentStore } from "../../environment_store";
import { parsePackageName, VersionRef } from "../../identity";
import { QueryPrivileges } from "../../model_query_request";
import type { Package } from "../../package";
import { storageBindingResolverFor } from "../../versions/materialization_scope";
import { MaterializationService } from "./materialization_service";

/**
 * The v1 cutover on a real Postgres (the R4 subset the
 * fixture cannot reach, and G7): v1 colocated `#@ persist` tables, one of them
 * incremental, built through real builds and
 * answered from the served copy equal to live, before and after a delta. Date
 * truncations stored by Postgres as timestamps exercise G5's read cast there.
 */

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = "v1_cutover_pg";
const q = (name: string) => `${SCHEMA}.${name}`;

function modelText(flag: boolean): string {
   return `${flag ? "##! experimental.persistence\n" : ""}source: ev is warehouse.table('${q("ev")}') extend {
  dimension: tier is pick 'high' when amt > 10 else 'low'
}

#@ persist name="${q("v1_all")}"
source: ev_all is ev -> {
  group_by: region, tier, big is amt > 10, d_day is d.day, d_month is d.month
  aggregate: amt_sum is amt.sum(), rows is count()
} extend {
  measure: total is amt_sum.sum(), n is rows.sum()
}

#@ persist name="${q("v1_inc")}" refresh="incremental" watermark="d"
source: ev_inc is ev -> {
  group_by: d, d_day is d.day, region
  aggregate: amt_sum is amt.sum(), rows is count()
} extend {
  measure: total is amt_sum.sum(), n is rows.sum()
}
`;
}

const ALL = `ev -> {
  group_by: region, tier, big is amt > 10, d_day is d.day, d_month is d.month
  aggregate: amt_sum is amt.sum(), rows is count()
}`;

/** Each query on a stored source, and the same query against the live base. */
const QUERIES: { name: string; served: string; live: string }[] = [
   {
      name: "all dimensions, stored truncations included",
      served:
         "run: ev_all -> { group_by: region, big, tier, d_day, d_month; aggregate: total, n }",
      live: `run: ${ALL} -> { group_by: region, big, tier, d_day, d_month; aggregate: total is amt_sum.sum(), n is rows.sum() }`,
   },
   {
      name: "a filter and a re-truncation on a stored truncation",
      served:
         "run: ev_all -> { where: d_day >= @2026-02-01; group_by: y is d_month.year, d_month; aggregate: n }",
      live: `run: ${ALL} -> { where: d_day >= @2026-02-01; group_by: y is d_month.year, d_month; aggregate: n is rows.sum() }`,
   },
   {
      name: "the incremental source by day",
      served: "run: ev_inc -> { group_by: d_day; aggregate: total, n }",
      live: "run: ev -> { group_by: d_day is d.day; aggregate: total is amt.sum(), n is count() }",
   },
];

async function settledRun(
   repository: Pick<ResourceRepository, "getMaterializationById">,
   service: MaterializationService,
   id: string,
): Promise<Materialization> {
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await repository.getMaterializationById(id);
      if (
         m &&
         (m.status === "MANIFEST_FILE_READY" ||
            m.status === "FAILED" ||
            m.status === "CANCELLED")
      ) {
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         while (running.has(id)) {
            if (Date.now() > deadline) throw new Error(`run ${id} never ended`);
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await repository.getMaterializationById(id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error(`run ${id} never settled`);
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

type Row = Record<string, unknown>;

function sortRows(rows: Row[]): string {
   return JSON.stringify(
      rows
         .map((r) =>
            Object.fromEntries(
               Object.entries(r).map(([k, v]) => [
                  k,
                  typeof v === "bigint" ? Number(v) : v,
               ]),
            ),
         )
         .map((r) => JSON.stringify(r))
         .sort(),
   );
}

for (const flag of [true, false])
   describe.skipIf(!POSTGRES_URL)(
      `v1 colocated #@ persist on Postgres under the cutover (${flag ? "flagged" : "unflagged"} file)`,
      () => {
         let root: string;
         let envPath: string;
         let sourcesPath: string;
         let db: DuckDBConnection;
         let repo: ResourceRepository;
         let env: Environment;
         let service: MaterializationService;
         let restore: () => void;

         async function publish(version: string) {
            const location = path.join(sourcesPath, `${PKG}-${version}`);
            writePackageFiles(
               location,
               { "model.malloy": modelText(flag) },
               {
                  manifest: {
                     name: PKG,
                     version,
                     materialization: { scope: "package" },
                  },
               },
            );
            return env.getVersionService()!.publish(
               PKG,
               async (stagingPath) => {
                  await fs.promises.cp(location, stagingPath, {
                     recursive: true,
                  });
               },
               { sourceLocation: location, promotion: "on-publish" },
            );
         }

         async function build(): Promise<Materialization> {
            const created = await service.createMaterialization(ENV, PKG, {
               versionId: VersionRef.parse("1.0.0"),
            });
            return settledRun(repo, service, created.id);
         }

         async function sql(text: string): Promise<Row[]> {
            const connection = await env.getMalloyConnection("warehouse");
            return (await connection.runSQL(text)).rows as Row[];
         }

         async function pkg(): Promise<Package> {
            return env.getPackage(parsePackageName(PKG), false, {
               versionId: VersionRef.parse("1.0.0"),
            });
         }

         async function run(query: string) {
            const model = (await pkg()).getModel("model.malloy");
            if (!model) throw new Error("model.malloy did not load");
            return model.runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         }

         async function expectEqualsLive(
            query: (typeof QUERIES)[number],
            served = true,
         ) {
            const s = await run(query.served);
            const l = await run(query.live);
            expect(sortRows(s.compactResult as unknown as Row[])).toBe(
               sortRows(l.compactResult as unknown as Row[]),
            );
            expect(l.path?.servedBy.kind ?? "live").toBe("live");
            if (served) {
               expect(s.path?.servedBy).toMatchObject({
                  kind: "materialized",
                  via: "served_copy",
               });
            }
         }

         beforeEach(async () => {
            restore = installSettings({
               env: {
                  PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
                  PUBLISHER_MATERIALIZE: "true",
               },
            }).restore;
            await __setPackageLoadPoolForTests(null);
            root = fs.realpathSync(
               fs.mkdtempSync(path.join(os.tmpdir(), "v1-cutover-pg-")),
            );
            envPath = path.join(root, "env");
            sourcesPath = path.join(root, "sources");
            fs.mkdirSync(envPath);
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
            env.bindVersions(repo, ENV_ID, {
               downloaderFor: (_p, location) => async (stagingPath) => {
                  await fs.promises.cp(location, stagingPath, {
                     recursive: true,
                  });
               },
               ensurePackageRecord: async (packageName, description) => {
                  if (await repo.getPackageByName(ENV_ID, packageName))
                     return false;
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
               storageBindingResolverFor(repo, ENV_ID),
            );
            service = new MaterializationService({
               storageManager: { getRepository: () => repo },
               getEnvironment: async () => env,
               getLoadedEnvironments: () => [env],
            } as unknown as EnvironmentStore);
            await sql(`DROP SCHEMA IF EXISTS ${SCHEMA} CASCADE`);
            await sql(`CREATE SCHEMA ${SCHEMA}`);
            await sql(`CREATE TABLE ${q("ev")} AS SELECT * FROM (VALUES
              (1, 'east', 10.5, TIMESTAMP '2026-01-03 10:00:00'),
              (2, 'east', 20.0, TIMESTAMP '2026-01-31 23:30:00'),
              (3, 'west', 5.5, TIMESTAMP '2026-02-01 00:00:00'),
              (4, 'west', 11.0, TIMESTAMP '2026-03-15 12:00:00')
            ) AS t(id, region, amt, d)`);
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

         it(
            "serves each v1 table from the served copy equal to live, and after an incremental delta",
            async () => {
               await publish("1.0.0");
               const first = await build();
               expect(first.status).toBe("MANIFEST_FILE_READY");
               const verdicts = (await pkg())
                  .getServeVerdicts()
                  .map((v) => `${v.sourceName}:${v.verdict}`)
                  .sort();
               expect(verdicts).toEqual(["ev_all:serves", "ev_inc:serves"]);
               for (const query of QUERIES) await expectEqualsLive(query);
               const covered = (
                  Object.values(
                     (first.manifest as { entries?: Record<string, unknown> })
                        ?.entries ?? {},
                  ) as {
                     sourceName?: string;
                     ledger?: { coveredThrough?: string };
                  }[]
               ).find((e) => e.sourceName === "ev_inc")?.ledger?.coveredThrough;
               expect(covered).toBeDefined();
               const at = `TIMESTAMP '${covered!.replace("T", " ").replace("Z", "")}' + INTERVAL '1 second'`;
               await sql(`INSERT INTO ${q("ev")} VALUES
                 (5, 'east', 7.0, ${at}),
                 (6, 'west', 3.0, ${at})`);
               await new Promise((resolve) => setTimeout(resolve, 2100));
               const second = await build();
               expect(second.status).toBe("MANIFEST_FILE_READY");
               const entries = Object.values(
                  (second.manifest as { entries?: Record<string, unknown> })
                     ?.entries ?? {},
               ) as { sourceName?: string; refresh?: string }[];
               expect(
                  entries.find((e) => e.sourceName === "ev_inc")?.refresh,
               ).toBe("delta");
               await expectEqualsLive(QUERIES[2]);
               const n = await run("run: ev_inc -> { aggregate: n }");
               expect(Number((n.compactResult as Row[])[0].n)).toBe(6);
            },
            { timeout: 90_000 },
         );
      },
   );
