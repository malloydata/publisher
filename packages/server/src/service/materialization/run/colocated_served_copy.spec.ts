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
 * Colocated `#(materialize)` through real builds and the real router (phase 3
 * pull request 6): a source with no `storage=` is built into its own warehouse
 * (a DuckDB, and a real Postgres when `PUBLISHER_TEST_POSTGRES_URL` names one,
 * each the environment's `warehouse`), its table bound from the
 * colocated manifest, and the served copy answers from it on that connection,
 * equal to live: a full build, and on Postgres (where incremental refresh is
 * supported) an incremental refresh that appends a delta, and an incremental
 * rolling window, whose window the served copy applies again at read.
 */

const ENV = "testEnv";
const ENV_ID = "env-1";
const PKG = "sales";

/** A warehouse the spec runs on: DuckDB always, Postgres when one is given. */
interface Tier {
   label: string;
   config: Record<string, unknown>;
   /** Where the spec's tables live: the seed and every built table. */
   schema?: string;
   /** Incremental refresh is proven on Postgres, not DuckDB. */
   incremental: boolean;
}

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const SCHEMA = "colocated_served_spec";

const TIERS: Tier[] = [
   {
      label: "DuckDB",
      config: {
         name: "warehouse",
         type: "duckdb",
         duckdbConnection: { setupSQL: "SELECT 1" },
      },
      incremental: false,
   },
   ...(POSTGRES_URL
      ? [
           {
              label: "Postgres",
              config: {
                 name: "warehouse",
                 type: "postgres",
                 postgresConnection: { connectionString: POSTGRES_URL },
              },
              schema: SCHEMA,
              incremental: true,
           },
        ]
      : []),
];

/** A model of the test's own, in place of {@link modelFor}'s. */
let modelOverride: ((q: (name: string) => string) => string) | undefined;

function modelFor(tier: Tier): string {
   const q = (name: string) => (tier.schema ? `${tier.schema}.${name}` : name);
   const incremental = tier.incremental
      ? `
#(materialize) name="${q("ev_inc")}" refresh="incremental" watermark="d"
source: ev_inc is ev extend {
  measure: total is amt.sum(), n is count()
}

#(materialize) name="${q("ev_recent")}" refresh="incremental" watermark="d"
source: ev_recent is ev extend {
  where: d >= now - 30 days
  measure: total is amt.sum(), n is count()
}
`
      : "";
   return `source: ev is warehouse.table('${q("ev")}') extend {
  dimension: tier is pick 'high' when amt > 10 else 'low'
}

#(materialize) name="${q("ev_all")}"
source: ev_all is ev extend {
  dimension: big is amt > 10
  measure: total is amt.sum(), n is count()
}
${incremental}`;
}

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

let restoreSettings: () => void;

for (const tier of TIERS)
   describe(`colocated #(materialize) through real builds, served by the served copy (${tier.label})`, () => {
      let root: string;
      let envPath: string;
      let sourcesPath: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;

      async function publish(version: string) {
         const location = path.join(sourcesPath, `${PKG}-${version}`);
         writePackageFiles(
            location,
            {
               "model.malloy": modelOverride
                  ? modelOverride((name) =>
                       tier.schema ? `${tier.schema}.${name}` : name,
                    )
                  : modelFor(tier),
            },
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
               await fs.promises.cp(location, stagingPath, { recursive: true });
            },
            { sourceLocation: location, promotion: "on-publish" },
         );
      }

      async function build(forceRefresh = false): Promise<Materialization> {
         const created = await service.createMaterialization(ENV, PKG, {
            versionId: VersionRef.parse("1.0.0"),
            ...(forceRefresh ? { forceRefresh } : {}),
         });
         return settledRun(repo, service, created.id);
      }

      async function sql(text: string): Promise<Row[]> {
         const connection = await env.getMalloyConnection("warehouse");
         return (await connection.runSQL(text)).rows as Row[];
      }

      /** A row count, read back on either warehouse (Postgres's runSQL reads a `row` column). */
      async function countOf(from: string): Promise<number> {
         const select = `SELECT count(*) AS c FROM ${from}`;
         const rows = await sql(
            tier.schema
               ? `SELECT row_to_json(t) AS row FROM (${select}) t`
               : select,
         );
         return Number(rows[0].c);
      }

      async function pkg(): Promise<Package> {
         return env.getPackage(parsePackageName(PKG), false, {
            versionId: VersionRef.parse("1.0.0"),
         });
      }

      /** One query served (the switch on) and live (off), on the same package. */
      async function both(query: string) {
         const model = (await pkg()).getModel("model.malloy");
         if (!model) throw new Error("model.malloy did not load");
         const runQuery = () =>
            model.runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         const served = await runQuery();
         // Live: the same model with its served copy holding no table, so
         // no candidate reads one (the live compile reads none).
         const copy = (
            model as unknown as {
               servedCopy: {
                  bindings: unknown[];
                  setBindings(b: unknown[]): void;
               };
            }
         ).servedCopy;
         const kept = copy.bindings;
         copy.setBindings([]);
         let live: Awaited<ReturnType<typeof runQuery>>;
         try {
            live = await runQuery();
         } finally {
            copy.setBindings(kept);
         }
         return { served, live };
      }

      beforeEach(async () => {
         restoreSettings = installSettings({
            env: {
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
               PUBLISHER_MATERIALIZE: "true",
            },
         }).restore;
         await __setPackageLoadPoolForTests(null);
         root = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "colocated-served-")),
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
         env = await Environment.create(ENV, envPath, [tier.config as never]);
         env.bindVersions(repo, ENV_ID, {
            downloaderFor: (_packageName, location) => async (stagingPath) => {
               await fs.promises.cp(location, stagingPath, { recursive: true });
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
         env.setStorageBindingResolver(storageBindingResolverFor(repo, ENV_ID));
         service = new MaterializationService({
            storageManager: { getRepository: () => repo },
            getEnvironment: async () => env,
            getLoadedEnvironments: () => [env],
         } as unknown as EnvironmentStore);
         if (tier.schema) {
            await sql(`DROP SCHEMA IF EXISTS ${tier.schema} CASCADE`);
            await sql(`CREATE SCHEMA ${tier.schema}`);
         }
         // Rows up to two days ago, in the 30-day window and out of it.
         await sql(`CREATE TABLE ${table("ev")} AS SELECT * FROM (VALUES
         (1, 'east', 10.5, CAST(current_date - INTERVAL '40 days' AS TIMESTAMP)),
         (2, 'east', 20.0, CAST(current_date - INTERVAL '10 days' AS TIMESTAMP)),
         (3, 'west', 5.5, CAST(current_date - INTERVAL '5 days' AS TIMESTAMP)),
         (4, 'west', 11.0, CAST(current_date - INTERVAL '2 days' AS TIMESTAMP))
       ) AS t(id, region, amt, d)`);
      });

      const table = (name: string) =>
         tier.schema ? `${tier.schema}.${name}` : name;

      afterEach(async () => {
         if (tier.schema) {
            await sql(`DROP SCHEMA IF EXISTS ${tier.schema} CASCADE`).catch(
               () => undefined,
            );
         }
         await env.closeAllConnections().catch(() => undefined);
         await db.close();
         fs.rmSync(root, { recursive: true, force: true });
         modelOverride = undefined;
         restoreSettings();
         await __setPackageLoadPoolForTests(null);
      });

      const QUERIES = {
         ev_all:
            "run: ev_all -> { group_by: region, big, tier; aggregate: total, n }",
         ev_inc: "run: ev_inc -> { group_by: region; aggregate: total, n }",
         ev_recent:
            "run: ev_recent -> { group_by: region; aggregate: total, n }",
      } as const;
      const built: (keyof typeof QUERIES)[] = tier.incremental
         ? ["ev_all", "ev_inc", "ev_recent"]
         : ["ev_all"];

      async function expectServedEqualsLive(
         source: keyof typeof QUERIES,
      ): Promise<void> {
         const { served, live } = await both(QUERIES[source]);
         expect(sortRows(served.compactResult as unknown as Row[])).toBe(
            sortRows(live.compactResult as unknown as Row[]),
         );
         // Live read the warehouse's own table; served, the source's colocated
         // table through the served copy.
         expect(live.path?.servedBy.kind ?? "live").toBe("live");
         const address = Object.values(
            (await pkg()).getBuildPlan()!.sources,
         ).find((s) => s.name === source)!.sourceEntityId;
         expect(served.path?.servedBy).toMatchObject({
            kind: "materialized",
            tableId: address,
            via: "served_copy",
         });
         // The source's own warehouse: no storage hit.
         expect(served.servedFrom ?? null).toBeNull();
      }

      it(
         "builds each colocated source into its warehouse and answers it from there, equal to live",
         async () => {
            await publish("1.0.0");
            const run = await build();
            expect(run.status).toBe("MANIFEST_FILE_READY");
            const plan = (await pkg()).getBuildPlan()!;
            expect(
               Object.values(plan.sources)
                  .map((s) => [s.name, s.annotationFields?.storage])
                  .sort(),
            ).toEqual(built.map((name) => [name, undefined]));
            expect(
               (await pkg()).getServeVerdicts().map((v) => v.verdict),
            ).toEqual(built.map(() => "serves"));
            for (const source of built) await expectServedEqualsLive(source);
            // A query that reads no stored table, but names a result column
            // as the table is named, is not answered from that table.
            const { served } = await both(
               "run: ev -> { group_by: region; aggregate: ev_all is count() }",
            );
            expect(served.path?.servedBy.kind ?? "live").toBe("live");
         },
         { timeout: 60_000 },
      );

      it(
         "never builds over a table at name= that no run of the package wrote, and rebuilds its own",
         async () => {
            // An author's table in the warehouse the source reads, at the
            // name= one source asks for: the build swap would drop it.
            await sql(
               `CREATE TABLE ${table("customers")} AS SELECT 1 AS cid, 'alice' AS cname`,
            );
            modelOverride = (q) => `source: ev is warehouse.table('${q("ev")}')

#(materialize) name="${q("customers")}"
source: ev_taken is ev extend { measure: n is count() }

#(materialize) name="${q("ev_own")}"
source: ev_own is ev extend {
  where: region = 'east'
  measure: n is count()
}
`;
            await publish("1.0.0");
            const first = await build();
            // The run commits the source that built; the other fails alone.
            const failures = Object.values(
               (first.manifest as { failures?: Record<string, unknown> })
                  ?.failures ?? {},
            ) as { sourceName?: string; reason?: string }[];
            expect(failures.map((f) => f.sourceName)).toEqual(["ev_taken"]);
            // Refused by the ownership check (table_ownership.ts), which
            // runs before the colocated name= preflight.
            expect(failures[0].reason).toContain("table-owned-elsewhere");
            expect(failures[0].reason).toContain(
               "a table with no Publisher mark",
            );
            // The author's table is untouched, and the other source built.
            expect(
               await countOf(
                  `${table("customers")} WHERE cid = 1 AND cname = 'alice'`,
               ),
            ).toBe(1);
            expect(await countOf(table("ev_own"))).toBe(2);
            // A table this package's own run wrote at its name= is rebuilt.
            await sql(`INSERT INTO ${table("ev")} VALUES
            (9, 'east', 1.0, CAST(current_date AS TIMESTAMP))`);
            await build(true);
            expect(await countOf(table("ev_own"))).toBe(3);
         },
         { timeout: 60_000 },
      );

      it.skipIf(!tier.incremental)(
         "refreshes an incremental colocated source and an incremental rolling window with a delta, each still equal to live",
         async () => {
            await publish("1.0.0");
            const first = await build();
            expect(first.status).toBe("MANIFEST_FILE_READY");
            // New rows just above the boundary the first build covered
            // through (on the build's own clock, which need not be the
            // database's): the next delta's range starts there.
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
            const at = `TIMESTAMP '${covered!.replace("T", " ")}' + INTERVAL '1 second'`;
            await sql(`INSERT INTO ${table("ev")} VALUES
            (5, 'east', 7.0, ${at}),
            (6, 'west', 3.0, ${at})`);
            // The second build's clock passes them.
            await new Promise((resolve) => setTimeout(resolve, 2100));
            const second = await build();
            expect(second.status).toBe("MANIFEST_FILE_READY");
            const entries = Object.values(
               (second.manifest as { entries?: Record<string, unknown> })
                  ?.entries ?? {},
            ) as { sourceName?: string; refresh?: string }[];
            const refresh = (name: string) =>
               entries.find((e) => e.sourceName === name)?.refresh;
            expect(refresh("ev_inc")).toBe("delta");
            expect(refresh("ev_recent")).toBe("delta");
            for (const source of ["ev_inc", "ev_recent"] as const) {
               await expectServedEqualsLive(source);
            }
            // The served answers include the delta's rows.
            const { served } = await both("run: ev_inc -> { aggregate: n }");
            expect(Number((served.compactResult as Row[])[0].n)).toBe(6);
         },
         { timeout: 60_000 },
      );
   });
